package relayxrecords

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/AshikBN/relayX/relayxerr"
)

var (
	fileFormatMagicBytes = [4]byte{'r', 'e', 'l', '!'}
	byteOrder            = binary.LittleEndian
)

const (
	FileFormatVersion = 1
	HeaderBytes       = 32
	recordIndexSize   = 4
)

type Header struct {
	MagicBytes [4]byte
	Version    int16
	UnixEpoch  int64
	NumRecords uint32
	Reserved   [14]byte
}

func (h Header) Size() uint32 {
	return HeaderBytes + h.NumRecords*recordIndexSize
}

var UnixEpoch = func() int64 {
	return time.Now().UnixMicro()
}

func Write(wtr io.Writer, batch Batch) error {
	header := Header{
		MagicBytes: fileFormatMagicBytes,
		Version:    FileFormatVersion,
		UnixEpoch:  UnixEpoch(),
		NumRecords: uint32(batch.Len()),
	}
	err := binary.Write(wtr, byteOrder, header)
	if err != nil {
		return fmt.Errorf("wriring header:%w", err)
	}
	indexes := make([]int32, len(batch.Sizes))
	index := int32(0)
	for i, recordSize := range batch.Sizes {
		indexes[i] = index
		index += int32(recordSize)
	}
	err = binary.Write(wtr, byteOrder, indexes)
	if err != nil {
		return fmt.Errorf("wriring record indexes:%w", err)
	}

	err = binary.Write(wtr, byteOrder, batch.Data)
	if err != nil {
		return fmt.Errorf("wriring record:%w", err)
	}
	return nil
}

type Parser struct {
	Header      Header
	recordIndex []uint32
	RecordSizes []uint32
	rdr         io.ReadSeekCloser
}

func Parse(rdr io.ReadSeekCloser) (*Parser, error) {
	header := Header{}
	err := binary.Read(rdr, byteOrder, &header)
	if err != nil {
		return nil, fmt.Errorf("reading header %w", err)
	}

	recordIndexes := make([]uint32, header.NumRecords, header.NumRecords+1)
	err = binary.Read(rdr, byteOrder, &recordIndexes)
	if err != nil {
		return nil, fmt.Errorf("reading record index %w", err)
	}
	fileSize, err := rdr.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seeking the file end %w", err)
	}

	recordIndexes = append(recordIndexes, uint32(fileSize)-header.Size())
	recordSizes := make([]uint32, header.NumRecords)
	for i := 0; i < len(recordIndexes)-1; i++ {
		recordSizes[i] = recordIndexes[i+1] - recordIndexes[i]
	}

	return &Parser{
		Header:      header,
		recordIndex: recordIndexes,
		RecordSizes: recordSizes,
		rdr:         rdr,
	}, nil

}

func (rb *Parser) Records(batch *Batch, recordIndexStart uint32, recordIndexEnd uint32) error {
	if recordIndexStart >= rb.Header.NumRecords {
		return fmt.Errorf("%d records available, start record index %d does not exist:%w", rb.Header.NumRecords, recordIndexStart, relayxerr.ErrOutOfBounds)
	}
	if recordIndexEnd > rb.Header.NumRecords {
		return fmt.Errorf("%d records available, end record index %d does not exist:%w", rb.Header.NumRecords, recordIndexEnd, relayxerr.ErrOutOfBounds)
	}
	if recordIndexStart >= recordIndexEnd {
		return fmt.Errorf("%w:start record index %d must be lower than end record index%d", relayxerr.ErrBadInput, recordIndexStart, recordIndexEnd)
	}
	requestedRecords := int(recordIndexEnd - recordIndexStart)
	recordsLeftInBatch := cap(batch.Sizes) - len(batch.Sizes)
	if requestedRecords > recordsLeftInBatch {
		return fmt.Errorf("%w:not enough records left in buffer to satisfy the read,%d required %d left", relayxerr.ErrBufferTooSmall, requestedRecords, recordsLeftInBatch)
	}

	recordOffsetStart := rb.recordIndex[recordIndexStart]
	recordOffsetEnd := rb.recordIndex[recordIndexEnd]
	requestedBytes := int(recordOffsetEnd - recordOffsetStart)

	bytesLeftInBatch := cap(batch.Data) - len(batch.Data)
	if requestedBytes > bytesLeftInBatch {
		return fmt.Errorf("%w:not enough bytes left in buffer to satisfy the read,%d required and %d left", relayxerr.ErrBufferTooSmall, requestedBytes, bytesLeftInBatch)
	}

	fileOffsetStart := rb.Header.Size() + recordOffsetStart
	_, err := rb.rdr.Seek(int64(fileOffsetStart), io.SeekStart)
	if err != nil {
		return fmt.Errorf("seeking for record %d/%d:%w", recordIndexStart, len(rb.recordIndex), err)
	}
	buf := batch.Data[len(batch.Data) : len(batch.Data)+requestedBytes]
	n, err := io.ReadFull(rb.rdr, buf)
	if err != nil {
		return fmt.Errorf("reading record indexes[%d:%d]:%w", recordIndexStart, recordIndexEnd, err)
	}
	if n != requestedBytes {
		return fmt.Errorf("reading record indexes[%d:%d]: expected read %d, read %d", recordIndexStart, recordIndexEnd, requestedBytes, n)
	}
	batch.Data = batch.Data[:len(batch.Data)+requestedBytes]
	batch.Sizes = append(batch.Sizes, rb.RecordSizes[recordIndexStart:recordIndexStart+uint32(requestedRecords)]...)
	return nil
}

func (rb *Parser) Close() error {
	return rb.rdr.Close()
}

// {
// header{}
// index[0,3,4,5,5]
// data{///////}
// }
