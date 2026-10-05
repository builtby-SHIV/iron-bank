package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

type SSTable struct {
	bloomFilter *BloomFilter
	index       *Index
	file        *os.File
	dataOffSet  int64
}

type SSTableIterator struct {
	s *SSTable
	file *os.File
	value *LSMEntry
}

func SerializeToSSTable(messages []*LSMEntry, filename string) (*SSTable, error) {
	bloomFilter, index, entriesBuffer, err := buildMetaDataAndEntriesBuffer(messages)

	if err != nil {
		return nil, err
	}

	indexData := MustMarshal(index)
	bloomFilterData := MustMarshal(bloomFilter)

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
		data := MustMarshal(msg)
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

	exists, offSet := FindOffsetForKey(s.index.Entries, key)
	if !exists {
		return nil, nil
	}

	if _, err := s.file.Seek(int64(s.dataOffSet + offSet), io.SeekStart); err != nil {
		return nil, err
	}

	size, err := ReadDataSize(s.file)
	if err != nil {
		return nil, err
	}

	data, err := ReadDataEntry(s.file, size)
	if err != nil {
		return nil, err
	}
	entry := LSMEntry{}
	MustUnmarshal(data, &entry)
	
	return &entry, nil
}

func (s *SSTable) RangeScan(startKey, endKey string) ([]*LSMEntry, error) {
	exists, offSet := FindOffsetForRangeKey(s.index.Entries, startKey)
	if !exists {
		return nil, nil
	}

	if _, err := s.file.Seek(int64(s.dataOffSet + offSet), io.SeekStart); err != nil {
		return nil, err
	}

	var res []*LSMEntry
	loop:
	for {
		size, err := ReadDataSize(s.file)
		if err != nil {
			return nil, err
		}

		data, err := ReadDataEntry(s.file, size)
		if err != nil {
			return nil, err
		}
		entry := LSMEntry{}
		MustUnmarshal(data, &entry)
		res = append(res, &entry)

		if entry.Key == endKey{
			break loop
		}
	}

	return res, nil
}

func (s *SSTable) Front() *SSTableIterator {
	file, err := os.Open(s.file.Name())
	if err != nil {
		return nil
	}

	i := &SSTableIterator{s: s, file: file, value: &LSMEntry{}}
	if _, err := s.file.Seek(int64(s.dataOffSet), io.SeekStart); err != nil {
		return nil
	}

	size, err := ReadDataSize(i.file)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		panic(err)
	}

	data, err := ReadDataEntry(i.file, size)
	if err != nil {
		return nil
	}

	MustUnmarshal(data, i.value)
	return i
}

func (i *SSTableIterator) Next() *SSTableIterator {
	size, err := ReadDataSize(i.file)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		panic(err)
	}

	data, err := ReadDataEntry(i.file, size)
	if err != nil {
		return nil
	}

	i.value = &LSMEntry{}
	MustUnmarshal(data, i.value)
	return i
}