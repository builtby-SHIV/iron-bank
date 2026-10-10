package main

import (
	"time"

	"github.com/huandu/skiplist"
)

type Memtable struct {
	data skiplist.SkipList
	size int64
}

func NewMemTable() *Memtable{
	return &Memtable{
		data: *skiplist.New(skiplist.String),
		size: 0,
	}
}

func (m *Memtable) Put(key string, val []byte) {
	sizeChange := int64(len(key))
	keyExists := m.data.Get(key)
	if keyExists != nil {
		m.size += int64(len(keyExists.Value.(*LSMEntry).Value))
	} else {
		sizeChange += int64(len(val))
	}
	entry := getLSMEntry(key, &val, Command_PUT)
	m.data.Set(key, entry)
	m.size += sizeChange
}

func (m *Memtable) Del(key string) {
	keyExists := m.data.Get(key)
	if keyExists != nil {
		m.size += int64(len(keyExists.Value.(*LSMEntry).Value))
	} else {
		m.size += int64(len(key))
	}
	m.data.Set(key, getLSMEntry(key, nil, Command_DELETE))
}

func (m *Memtable) Get(key string) *LSMEntry {
	keyExists := m.data.Get(key)
	if keyExists != nil {
		return keyExists.Value.(*LSMEntry)
	}
	return nil
}

func (m *Memtable) RangeScan(start, end string) [] *LSMEntry {
	var results [] *LSMEntry
	iter := m.data.Find(start)
	for iter != nil {
		if iter.Element().Key().(string) > end {
			break
		}

		results = append(results, iter.Value.(*LSMEntry))
		iter = iter.Next()
	}
	return  results
}

func (m *Memtable) SizeInbytes() int64 {
	return m.size
}

func (m *Memtable) GetEntries() [] *LSMEntry {
	var results []*LSMEntry
	iter := m.data.Front()
	for iter != nil {
		results = append(results, iter.Value.(*LSMEntry))
		iter = iter.Next()
	}
	return results
}

func getLSMEntry(key string, value *[]byte, command Command) *LSMEntry {
	entry := &LSMEntry{
		Key:       key,
		Command:   command,
		Timestamp: time.Now().UnixNano(),
	}
	if value != nil {
		entry.Value = *value
	}
	return entry
}