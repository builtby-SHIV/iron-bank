package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"google.golang.org/protobuf/proto"
)

type SSTable struct {
	bloomFilter *BloomFilter
	index       *Index
	file        *os.File
	dataOffSet  int64
}

func SerializeToSSTable(messages []*LSMEntry, filename string) (*SSTable, error) {
	bloomFilter, index, entriesBuffer, err := buildMetaDataAndEntriesBuffer(messages)

	if err != nil {
		return nil, err
	}

	indexData := mustMarshal(index)
	bloomFilterData := mustMarshal(bloomFilter)

	dataOffSet, err := writeSSTable(filename, bloomFilterData, indexData, entriesBuffer)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return &SSTable{bloomFilter: bloomFilter, index: index, file: file, dataOffSet: dataOffSet}, nil
}

func buildMetaDataAndEntriesBuffer(messages []*LSMEntry) (*BloomFilter, *Index, *bytes.Buffer, error) {
	var index []*IndexEntry
	var bloomFilter *BloomFilter = NewBloomFilter(1000000)
	var currentOffset int64 = 0
	entriesBuffer := &bytes.Buffer{}

	for _, msg := range messages {
		data := mustMarshal(msg)
		entrySize := int64(len(data))

		//dense index, 1M keys means 1M keys present in index
		index = append(index, &IndexEntry{Key: msg.Key, Offset: int64(currentOffset)})
		bloomFilter.Add([]byte(msg.Key))

		if err := binary.Write(entriesBuffer, binary.LittleEndian, int64(entrySize)); err != nil {
			return nil, nil, nil, err
		}
		if _, err := entriesBuffer.Write(data); err != nil {
			return nil, nil, nil, err
		}

		currentOffset += int64(binary.Size(entrySize)) + int64(entrySize)
	}

	return bloomFilter, &Index{Entries: index}, entriesBuffer, nil
}

func mustMarshal[T proto.Message](pb T) []byte {
	bytes, err := proto.Marshal(pb)
	if err != nil {
		panic(fmt.Sprintf("failed to marhshal proto %v", err))
	}
	return bytes
}

func mustUnmarshal [T proto.Message](bytes []byte, pb T) {
	err := proto.Unmarshal(bytes, pb)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarhshal proto %v", err))
	}
}

func writeSSTable(filename string, bloomfilterData, indexData []byte, entriesBuffer *bytes.Buffer) (int64, error) {
	file, err := os.Create(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var dataOffSet int64 = 0

	//bloom filter size
	if err := binary.Write(file, binary.LittleEndian, int64(len(bloomfilterData))); err != nil {
		return 0, err
	}
	dataOffSet += int64(binary.Size(int64(len(bloomfilterData))))

	//bloom filter data
	if _, err := file.Write(bloomfilterData); err != nil {
		return 0, err
	}
	dataOffSet += int64(len(bloomfilterData))

	//index size
	if err := binary.Write(file, binary.LittleEndian, int64(len(indexData))); err != nil {
		return 0, err
	}
	dataOffSet += int64(binary.Size(int64(len(indexData))))

	//index data
	if _, err := file.Write(indexData); err != nil {
		return 0, err
	}
	dataOffSet += int64(len(indexData))

	if _, err := io.Copy(file, entriesBuffer); err != nil {
		return 0, err
	}

	return dataOffSet, nil
}

func (s *SSTable) Get(key string) (*LSMEntry, error) {
	if !s.bloomFilter.Test([]byte(key)) {
		return nil, nil
	}

	exists, offSet := findOffsetForKey(s.index.Entries, key)
	if !exists {
		return nil, nil
	}

	if _, err := s.file.Seek(int64(s.dataOffSet + offSet), io.SeekStart); err != nil {
		return nil, err
	}

	size, err := readDataSize(s.file)
	if err != nil {
		return nil, err
	}

	data, err := readDataEntry(s.file, size)
	if err != nil {
		return nil, err
	}
	entry := LSMEntry{}
	mustUnmarshal(data, &entry)
	
	return &entry, nil
}

func findOffsetForKey(index []*IndexEntry, key string) (bool, int64) {
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

func readDataSize(file *os.File) (int64, error) {
	var size int64
	if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
		return 0, err
	}
	return size, nil
}

func readDataEntry(file *os.File, size int64) ([]byte, error) {
	data := make([]byte, size)
	if _, err := file.Read(data); err != nil {
		return nil, err
	}
	return data, nil
}