package storage

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"iron-bank/proto"
)

type SSTable struct {
	bloomFilter *BloomFilter
	index       *proto.Index
	file        *os.File
	dataOffSet  int64
}

type SSTableIterator struct {
	s     *SSTable
	file  *os.File
	Value *proto.LSMEntry
	value *proto.LSMEntry
}

func SerializeToSSTable(messages []*proto.LSMEntry, filename string) (*SSTable, error) {
	bloomFilter, index, entriesBuffer, err := buildMetaDataAndEntriesBuffer(messages)

	if err != nil {
		return nil, err
	}

	indexData := proto.MustMarshal(index)
	bloomFilterData := proto.MustMarshal(&proto.BloomFilter{
		BitSet: bloomFilter.BitSet,
		Size:   bloomFilter.Size,
	})

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

func buildMetaDataAndEntriesBuffer(messages []*proto.LSMEntry) (*BloomFilter, *proto.Index, *bytes.Buffer, error) {
	var index []*proto.IndexEntry
	bloomFilter := NewBloomFilter(1000000)
	var currentOffset int64 = 0
	entriesBuffer := &bytes.Buffer{}

	for _, msg := range messages {
		data := proto.MustMarshal(msg)
		entrySize := int64(len(data))

		//dense index, 1M keys means 1M keys present in index
		index = append(index, &proto.IndexEntry{Key: msg.Key, Offset: int64(currentOffset)})
		bloomFilter.Add([]byte(msg.Key))

		if err := binary.Write(entriesBuffer, binary.LittleEndian, int64(entrySize)); err != nil {
			return nil, nil, nil, err
		}
		if _, err := entriesBuffer.Write(data); err != nil {
			return nil, nil, nil, err
		}

		currentOffset += int64(binary.Size(entrySize)) + int64(entrySize)
	}

	return bloomFilter, &proto.Index{Entries: index}, entriesBuffer, nil
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

	if err := file.Sync(); err != nil {
		return 0, err
	}

	return dataOffSet, nil
}

func (s *SSTable) Close() error {
	if s != nil && s.file != nil {
		return s.file.Close()
	}
	return nil
}

func (s *SSTable) Get(key string) (*proto.LSMEntry, error) {
	if s.bloomFilter != nil && !s.bloomFilter.Test([]byte(key)) {
		return nil, nil
	}

	exists, offSet := FindOffsetForKey(s.index.Entries, key)
	if !exists {
		return nil, nil
	}

	entryOffset := s.dataOffSet + offSet
	sizeBuf := make([]byte, 8)
	if _, err := s.file.ReadAt(sizeBuf, entryOffset); err != nil {
		return nil, err
	}
	size := int64(binary.LittleEndian.Uint64(sizeBuf))

	data := make([]byte, size)
	if _, err := s.file.ReadAt(data, entryOffset+8); err != nil {
		return nil, err
	}

	entry := proto.LSMEntry{}
	proto.MustUnmarshal(data, &entry)

	return &entry, nil
}

func (s *SSTable) RangeScan(startKey, endKey string) ([]*proto.LSMEntry, error) {
	exists, offSet := FindOffsetForRangeKey(s.index.Entries, startKey)
	if !exists {
		return []*proto.LSMEntry{}, nil
	}

	currentOffset := s.dataOffSet + offSet
	fileInfo, err := s.file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()

	var res = []*proto.LSMEntry{}
	sizeBuf := make([]byte, 8)

	for currentOffset < fileSize {
		if _, err := s.file.ReadAt(sizeBuf, currentOffset); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		size := int64(binary.LittleEndian.Uint64(sizeBuf))
		currentOffset += 8

		data := make([]byte, size)
		if _, err := s.file.ReadAt(data, currentOffset); err != nil {
			return nil, err
		}
		currentOffset += size

		entry := proto.LSMEntry{}
		proto.MustUnmarshal(data, &entry)

		if entry.Key > endKey {
			break
		}
		res = append(res, &entry)
		if entry.Key == endKey {
			break
		}
	}

	return res, nil
}

func (s *SSTable) GetEntries() ([]*proto.LSMEntry, error) {
	fileInfo, err := s.file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()
	currentOffset := s.dataOffSet
	var entries []*proto.LSMEntry
	sizeBuf := make([]byte, 8)

	for currentOffset < fileSize {
		if _, err := s.file.ReadAt(sizeBuf, currentOffset); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		size := int64(binary.LittleEndian.Uint64(sizeBuf))
		currentOffset += 8

		data := make([]byte, size)
		if _, err := s.file.ReadAt(data, currentOffset); err != nil {
			return nil, err
		}
		currentOffset += size

		entry := &proto.LSMEntry{}
		proto.MustUnmarshal(data, entry)
		entries = append(entries, entry)
	}

	return entries, nil
}

func (s *SSTable) Front() *SSTableIterator {
	if s.file == nil {
		return nil
	}
	file, err := os.Open(s.file.Name())
	if err != nil {
		return nil
	}

	if _, err := file.Seek(s.dataOffSet, io.SeekStart); err != nil {
		_ = file.Close()
		return nil
	}

	size, err := ReadDataSize(file)
	if err != nil {
		_ = file.Close()
		return nil
	}

	data, err := ReadDataEntry(file, size)
	if err != nil {
		_ = file.Close()
		return nil
	}

	entry := &proto.LSMEntry{}
	proto.MustUnmarshal(data, entry)
	return &SSTableIterator{s: s, file: file, Value: entry, value: entry}
}

func (i *SSTableIterator) Next() *SSTableIterator {
	if i == nil || i.file == nil {
		return nil
	}

	size, err := ReadDataSize(i.file)
	if err != nil {
		_ = i.file.Close()
		i.file = nil
		return nil
	}

	data, err := ReadDataEntry(i.file, size)
	if err != nil {
		_ = i.file.Close()
		i.file = nil
		return nil
	}

	i.Value = &proto.LSMEntry{}
	proto.MustUnmarshal(data, i.Value)
	i.value = i.Value
	return i
}

func (i *SSTableIterator) Close() error {
	if i != nil && i.file != nil {
		err := i.file.Close()
		i.file = nil
		return err
	}
	return nil
}
