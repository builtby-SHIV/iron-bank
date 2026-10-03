package main

import "github.com/huandu/skiplist"

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

func (m *Memtable) Put(key, val string) {
	sizeChange := int64(len(key))
	keyExists := m.data.Get(key)
	if keyExists != nil {
		m.size += int64(len(keyExists.Value.(*LSMEntry).val))
	} else {
		sizeChange += int64(len(val))
	}
	entry := GetLSMEntry(key, val, "add")
	m.data.Set(key, entry)
	m.size += sizeChange
}

func (m *Memtable) Del(key string) {
	keyExists := m.data.Get(key)
	if keyExists != nil {
		m.size += int64(len(keyExists.Value.(*LSMEntry).val))
	} else {
		m.size += int64(len(key))
	}
	m.data.Set(key, GetLSMEntry(key, nil, "del"))
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
	// turn this into an iterator
	iter := m.data.Front()
	for iter != nil {
		results = append(results, iter.Value.(*LSMEntry))
		iter = iter.Next()
	}
	return results
}

