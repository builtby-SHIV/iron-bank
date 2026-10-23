package storage

import (
	"encoding/binary"
	"io"
	"os"

	"iron-bank/proto"
)

func FindOffsetForKey(index []*proto.IndexEntry, key string) (bool, int64) {
	l := 0
	h := len(index) - 1

	for l <= h {
		mid := (l + h) / 2
		if index[mid].Key == key {
			return true, int64(index[mid].Offset)
		} else if index[mid].Key < key {
			l = mid + 1
		} else {
			h = mid - 1
		}
	}

	return false, 0
}

func FindOffsetForRangeKey(index []*proto.IndexEntry, key string) (bool, int64) {
	l := 0
	h := len(index) - 1

	for l <= h {
		mid := (l + h) / 2
		if index[mid].Key == key {
			return true, int64(index[mid].Offset)
		} else if index[mid].Key < key {
			l = mid + 1
		} else {
			h = mid - 1
		}
	}

	if l >= len(index) {
		return false, 0
	}

	return true, int64(index[l].Offset)
}

func ReadDataSize(file *os.File) (int64, error) {
	var size int64
	if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
		return 0, err
	}
	return size, nil
}

func ReadDataEntry(file *os.File, size int64) ([]byte, error) {
	data := make([]byte, size)
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, err
	}
	return data, nil
}

func ReadSSTableMetaData(file *os.File) (*BloomFilter, *proto.Index, int64, error) {
	var dataOffset int64 = 0

	bloomFilterSize, err := ReadDataSize(file)
	if err != nil {
		return nil, nil, 0, err
	}
	dataOffset += int64(binary.Size(bloomFilterSize))

	bloomFilterData := make([]byte, bloomFilterSize)
	if _, err := io.ReadFull(file, bloomFilterData); err != nil {
		return nil, nil, 0, err
	}
	dataOffset += int64(len(bloomFilterData))

	indexSize, err := ReadDataSize(file)
	if err != nil {
		return nil, nil, 0, err
	}
	dataOffset += int64(binary.Size(indexSize))

	indexData := make([]byte, indexSize)
	if _, err := io.ReadFull(file, indexData); err != nil {
		return nil, nil, 0, err
	}
	dataOffset += int64(len(indexData))

	serializedBloomFilter := &proto.BloomFilter{}
	proto.MustUnmarshal(bloomFilterData, serializedBloomFilter)
	bloomFilter := &BloomFilter{
		BitSet: serializedBloomFilter.BitSet,
		Size:   serializedBloomFilter.Size,
	}
	index := &proto.Index{}
	proto.MustUnmarshal(indexData, index)

	return bloomFilter, index, dataOffset, nil
}
