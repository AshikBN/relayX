package relayxtopic

import (
	"context"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AshikBN/relayX/internal/infrastructure/logger"
	relayxcache "github.com/AshikBN/relayX/internal/relayXcache"
	relayxrecords "github.com/AshikBN/relayX/internal/replayXrecords"
	"github.com/AshikBN/relayX/relayxerr"
)

type File struct {
	Size int64
	Path string
}

type Storage interface {
	Writer(recordBatchPath string) (io.WriteCloser, error)
	Reader(recordBatchPath string) (io.ReadCloser, error)
	ListFiles(topicName string, extention string) ([]File, error)
}

type Compress interface {
	NewWriter(io.Writer) (io.WriteCloser, error)
	NewReader(io.Reader) (io.ReadCloser, error)
}

type Topic struct {
	log        logger.Logger
	topicName  string
	nextOffset atomic.Uint64

	mu                 sync.Mutex
	recordBatchOffsets []uint64

	backingStorage Storage
	cache          *relayxcache.Cache
	compression    Compress
	OffsetCond     *OffsetCond
}

type Opts struct {
	Compression Compress
}

func New(log logger.Logger, topicName string, backingStorage Storage, cache *relayxcache.Cache, optFuncs ...func(*Opts)) (*Topic, error) {
	opts := Opts{
		Compression: Gzip{},
	}
	for _, optFunc := range optFuncs {
		optFunc(&opts)
	}

	recordBatchOffsets, err := listRecordBatchOffsets(backingStorage, topicName)
	if err != nil {
		return nil, fmt.Errorf("listing record batches %w", err)
	}
	topic := &Topic{
		log:                log,
		topicName:          topicName,
		backingStorage:     backingStorage,
		recordBatchOffsets: recordBatchOffsets,
		cache:              cache,
		compression:        opts.Compression,
		OffsetCond:         NewOffsetCond(0),
	}

	if len(recordBatchOffsets) > 0 {
		newestRecordBatchOffset := recordBatchOffsets[len(recordBatchOffsets)-1]
		parser, err := topic.parseRecordBatch(newestRecordBatchOffset)
		if err != nil {
			return nil, fmt.Errorf("reading record batch header %w", err)
		}
		defer parser.Close()

		nextOffset := newestRecordBatchOffset + uint64(parser.Header.NumRecords)
		topic.nextOffset.Store(nextOffset)
		topic.OffsetCond = NewOffsetCond(nextOffset - 1)

	}
	return topic, nil

}

func (s *Topic) AddRecords(batch relayxrecords.Batch) ([]uint64, error) {
	recordBatchId := s.nextOffset.Load()

	rbPath := RecordBatchKey(s.topicName, recordBatchId)
	backingWriter, err := s.backingStorage.Writer(rbPath)
	if err != nil {
		return nil, fmt.Errorf("opening the writer %s:%w", rbPath, err)
	}
	w := backingWriter
	if s.compression != nil {
		w, err = s.compression.NewWriter(backingWriter)
		if err != nil {
			return nil, fmt.Errorf("creating the compression writer %w", err)
		}
	}
	t0 := time.Now()
	err = relayxrecords.Write(w, batch)
	if err != nil {
		return nil, fmt.Errorf("writing record batch:%w", err)
	}

	if s.compression != nil {
		err = w.Close()
		if err != nil {
			return nil, fmt.Errorf("closing the compression writer %w", err)
		}
	}
	err = backingWriter.Close()
	if err != nil {
		return nil, fmt.Errorf("closing backing writer:%w", err)
	}

	s.log.Infof("wrote %d records (%s bytes) to %s (%s)", batch.Len(), len(batch.Data), rbPath, time.Since(t0))
	nextOffset := recordBatchId + uint64(batch.Len())
	offsets := make([]uint64, 0, batch.Len())
	for i := recordBatchId; i < nextOffset; i++ {
		offsets = append(offsets, i)
	}

	s.mu.Lock()
	s.recordBatchOffsets = append(s.recordBatchOffsets, recordBatchId)
	s.mu.Unlock()
	s.nextOffset.Store(nextOffset)

	if s.cache != nil {
		cacheWtr, err := s.cache.Writer(rbPath)
		if err != nil {
			s.log.Errorf("creating cache writer to cache %s:%w", rbPath, err)
		}
		err = relayxrecords.Write(cacheWtr, batch)
		if err != nil {
			s.log.Errorf("writing to cache %s:%w", rbPath, err)
		}

		err = cacheWtr.Close()
		if err != nil {
			s.log.Errorf("closing the cache file %s:%w", rbPath, err)
		}

	}
	if len(offsets) > 0 {
		s.OffsetCond.Broadcast(offsets[len(offsets)-1])
	}
	return offsets, nil

}

//.record_batch file
//===================

// 1.Header
// 	num_records
// 2.index
// 	each record start index
// 3.data
// 	slice of bytes

//Topic(each topic will have multiple record batch files with the offset in the file name(location/00000012.record_batch))
// 1.next_offset
// 	next record number
// 2.record_batch_offsets
// 		list of start record number in every batch record
// 3. storage
// 4. cache

func (s *Topic) ReadRecords(ctx context.Context, batch *relayxrecords.Batch, offset uint64, maxRecords int, softMaxBytes int) error {
	if offset >= s.nextOffset.Load() {
		return fmt.Errorf("offset does not exist: %w", relayxerr.ErrOutOfBounds)
	}

	if maxRecords == 0 {
		maxRecords = 10
	}

	// make a local copy of recordBatchOffsets so that we don't have to hold the
	// lock for the rest of the function.
	s.mu.Lock()
	recordBatchOffsets := make([]uint64, len(s.recordBatchOffsets))
	copy(recordBatchOffsets, s.recordBatchOffsets)
	s.mu.Unlock()

	// find the batch that offset is located in
	var (
		batchOffset      uint64
		batchOffsetIndex int
	)
	for batchOffsetIndex = len(recordBatchOffsets) - 1; batchOffsetIndex >= 0; batchOffsetIndex-- {
		curBatchOffset := recordBatchOffsets[batchOffsetIndex]
		if curBatchOffset <= offset {
			batchOffset = curBatchOffset
			break
		}
	}

	trackByteSize := softMaxBytes != 0
	recordBatchBytes := uint32(0)
	batchRecordIndex := uint32(offset - batchOffset)
	firstRecord := true

	moreRecords := func() bool { return batch.Len() < maxRecords }
	moreBytes := func() bool { return (!trackByteSize || recordBatchBytes < uint32(softMaxBytes)) }
	moreBatches := func() bool { return batchOffsetIndex < len(recordBatchOffsets) }

	for moreRecords() && moreBytes() && moreBatches() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batchOffset = recordBatchOffsets[batchOffsetIndex]
		rb, err := s.parseRecordBatch(batchOffset)
		if err != nil {
			return fmt.Errorf("parsing record batch: %w", err)
		}

		batchMaxRecords := min(uint32(maxRecords-batch.Len()), rb.Header.NumRecords-batchRecordIndex)
		numRecords := batchMaxRecords
		if trackByteSize {
			numRecords = 0

			for _, recordSize := range rb.RecordSizes[batchRecordIndex : batchRecordIndex+batchMaxRecords] {
				if !firstRecord && recordBatchBytes+recordSize > uint32(softMaxBytes) {
					break
				}

				numRecords += 1
				recordBatchBytes += recordSize
				firstRecord = false
			}
		}

		// we read enough records to satisfy the request
		if numRecords == 0 {
			break
		}

		// TODO: pass batch into rb.Records to write to it directly
		err = rb.Records(batch, batchRecordIndex, batchRecordIndex+numRecords)
		if err != nil {
			return fmt.Errorf("record batch '%s': %w", s.recordBatchPath(batchOffset), err)
		}

		// no more relevant records in batch -> prepare to check next batch
		rb.Close()
		batchOffsetIndex += 1
		batchRecordIndex = 0
	}

	return nil
}

type Metadata struct {
	NextOffset     uint64
	LatestCommitAt time.Time
}

func (s *Topic) Metadata() (Metadata, error) {
	var latestCommitAt time.Time
	nextOffset := s.nextOffset.Load()

	if nextOffset > 0 {
		recordBatchId := s.offsetGetRecordBatchID(nextOffset - 1)
		p, err := s.parseRecordBatch(recordBatchId)
		if err != nil {
			return Metadata{}, fmt.Errorf("parsing record batch:%w", err)
		}

		latestCommitAt = time.UnixMicro(p.Header.UnixEpoch)

	}

	return Metadata{
		NextOffset:     nextOffset,
		LatestCommitAt: latestCommitAt,
	}, nil
}

func (s *Topic) offsetGetRecordBatchID(offset uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := len(s.recordBatchOffsets) - 1; i >= 0; i-- {
		if offset >= s.recordBatchOffsets[i] {
			return s.recordBatchOffsets[i]
		}
	}
	return 0
}

const recordBatchExtension = ".record_batch"

func listRecordBatchOffsets(backingStorage Storage, topicName string) ([]uint64, error) {
	files, err := backingStorage.ListFiles(topicName, recordBatchExtension)
	if err != nil {
		return nil, fmt.Errorf("listing files:%w", err)
	}

	offsets := make([]uint64, 0, len(files))
	for _, file := range files {
		fileName := path.Base(file.Path)
		offsetStr := fileName[:len(fileName)-len(recordBatchExtension)]
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			return nil, err
		}
		offsets = append(offsets, uint64(offset))
	}
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})
	return offsets, nil
}

func (s *Topic) parseRecordBatch(recordBatchId uint64) (*relayxrecords.Parser, error) {
	recordBatchPath := s.recordBatchPath(recordBatchId)

	f, err := s.cache.Reader(recordBatchPath)
	if err != nil {
		s.log.Infof("%s not found in cache", recordBatchPath)
	}
	if f == nil {
		//cache not found
		backingReader, err := s.backingStorage.Reader(recordBatchPath)
		if err != nil {
			return nil, fmt.Errorf("opening the reader %s:%w", recordBatchPath, err)
		}
		r := backingReader
		if s.compression != nil {
			r, err = s.compression.NewReader(r)
			if err != nil {
				return nil, fmt.Errorf("creating compression reader:%w", err)
			}
		}
		//writing to cache
		cacheFile, err := s.cache.Writer(recordBatchPath)
		if err != nil {
			return nil, fmt.Errorf("writing backing storage result to cache %w", err)
		}
		_, err = io.Copy(cacheFile, r)
		if err != nil {
			return nil, fmt.Errorf("copying backing storage result to cache %w", err)
		}
		if s.compression != nil {
			r.Close()
		}
		err = cacheFile.Close()
		if err != nil {
			return nil, fmt.Errorf("closing cache file %w", err)
		}
		err = backingReader.Close()
		if err != nil {
			return nil, fmt.Errorf("closing backing reader %w", err)
		}
		f, err = s.cache.Reader(recordBatchPath)
		if err != nil {
			return nil, fmt.Errorf("readind cache file just after reading:%w", err)
		}

	}
	rb, err := relayxrecords.Parse(f)
	if err != nil {
		return nil, fmt.Errorf("parsing record batch %s:%w", recordBatchPath, err)
	}
	return rb, nil

}

func (s *Topic) recordBatchPath(recordBatchId uint64) string {
	return RecordBatchKey(s.topicName, recordBatchId)
}

func RecordBatchKey(topicName string, recordBatchId uint64) string {
	return filepath.Join(topicName, fmt.Sprintf("%012d%s", recordBatchId, recordBatchExtension))
}

func WithCompress(c Compress) func(*Opts) {
	return func(o *Opts) {
		o.Compression = c
	}
}
