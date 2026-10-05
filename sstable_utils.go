package main

import (
	"encoding/binary"
	"fmt"
	"os"

	"google.golang.org/protobuf/proto"
)

func MustMarshal[T proto.Message](pb T) []byte {
	bytes, err := proto.Marshal(pb)
	if err != nil {
		panic(fmt.Sprintf("failed to marhshal proto %v", err))
	}
	return bytes
}

func MustUnmarshal [T proto.Message](bytes []byte, pb T) {
	err := proto.Unmarshal(bytes, pb)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarhshal proto %v", err))
	}
}

func FindOffsetForKey(index []*IndexEntry, key string) (bool, int64) {
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

func FindOffsetForRangeKey(index []*IndexEntry, key string) (bool, int64) {
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
	if _, err := file.Read(data); err != nil {
		return nil, err
	}
	return data, nil
}
