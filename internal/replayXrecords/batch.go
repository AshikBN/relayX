package replayxrecords

import (
	"fmt"

	"github.com/AshikBN/relayX/relayxerr"
)

type Batch struct {
	Sizes []uint32
	Data  []byte
}

func NewBatch(recordSizes []uint32, recordsData []byte) Batch {
	return Batch{
		Sizes: recordSizes,
		Data:  recordsData,
	}
}

func (b Batch) Len() int {
	return len(b.Sizes)
}

func (b *Batch) Reset() {
	b.Data = b.Data[:0]
	b.Sizes = b.Sizes[:0]
}

func (b Batch) Records(starIndex int, endIndex int) ([]byte, error) {
	if starIndex >= len(b.Sizes) || endIndex > len(b.Sizes) || starIndex < 0 || endIndex <= 0 {
		return nil, relayxerr.ErrOutOfBounds
	}
	if starIndex > endIndex {
		return nil, fmt.Errorf("%w:start %d must be less than end %d", relayxerr.ErrBadInput, starIndex, endIndex)
	}
	var totalSize uint32 = 0
	var startSize uint32 = 0
	var endSize uint32 = 0
	for i, size := range b.Sizes {
		if starIndex == i {
			startSize = totalSize
		}
		if endIndex == i {
			endSize = totalSize
			break
		}
		totalSize += size
	}
	return b.Data[startSize:endSize], nil
}

func (b Batch) IndividualRecords() [][]byte {
	if b.Len() == 0 {
		return nil
	}

	records, err := b.IndividualRecordsSubset(0, b.Len())
	if err != nil {
		panic(fmt.Sprintf("unexpected error for individual records %s", err))
	}
	return records
}

func (b Batch) IndividualRecordsSubset(startIndex int, endIndex int) ([][]byte, error) {
	recordsData, err := b.Records(startIndex, endIndex)
	if err != nil {
		return nil, err
	}

	recordSets := make([][]byte, endIndex-startIndex)
	start := uint32(0)
	end := b.Sizes[startIndex]
	for i := range recordSets {
		recordSets[i] = recordsData[start:end]
		start = end
		end += b.Sizes[startIndex+1]
	}
	return recordSets, nil

}
