package storage

import (
	"iron-bank/proto"
	"time"

	"github.com/huandu/skiplist"
)

type Memtable struct {
	data skiplist.SkipList
	size int64
}

func NewMemTable() *Memtable {
	return &Memtable{
		data: *skiplist.New(skiplist.String),
		size: 0,
	}
}

func NewMemtable() *Memtable {
	return NewMemTable()
}

func (m *Memtable) Clear() {
	m.data = *skiplist.New(skiplist.String)
	m.size = 0
}

func (m *Memtable) Len() int {
	return m.data.Len()
}

func (m *Memtable) Delete(key string) {
	m.Del(key)
}

func (m *Memtable) SizeInBytes() int64 {
	return m.SizeInbytes()
}

func (m *Memtable) Put(key string, val []byte) {
	keyExists := m.data.Get(key)
	if keyExists != nil {
		oldEntry := keyExists.Value.(*proto.LSMEntry)
		m.size += int64(len(val)) - int64(len(oldEntry.Value))
	} else {
		m.size += int64(len(key)) + int64(len(val))
	}
	entry := getLSMEntry(key, &val, proto.Command_PUT)
	m.data.Set(key, entry)
}

func (m *Memtable) Del(key string) {
	keyExists := m.data.Get(key)
	if keyExists != nil {
		oldEntry := keyExists.Value.(*proto.LSMEntry)
		m.size -= int64(len(oldEntry.Value))
	} else {
		m.size += int64(len(key))
	}
	m.data.Set(key, getLSMEntry(key, nil, proto.Command_DELETE))
}

func (m *Memtable) Get(key string) *proto.LSMEntry {
	keyExists := m.data.Get(key)
	if keyExists != nil {
		return keyExists.Value.(*proto.LSMEntry)
	}
	return nil
}

func (m *Memtable) RangeScan(start, end string) []*proto.LSMEntry {
	var results []*proto.LSMEntry
	iter := m.data.Find(start)
	for iter != nil {
		if iter.Key().(string) > end {
			break
		}

		results = append(results, iter.Value.(*proto.LSMEntry))
		iter = iter.Next()
	}
	return results
}

func (m *Memtable) SizeInbytes() int64 {
	return m.size
}

func (m *Memtable) GetEntries() []*proto.LSMEntry {
	var results []*proto.LSMEntry
	iter := m.data.Front()
	for iter != nil {
		results = append(results, iter.Value.(*proto.LSMEntry))
		iter = iter.Next()
	}
	return results
}

func getLSMEntry(key string, value *[]byte, command proto.Command) *proto.LSMEntry {
	entry := &proto.LSMEntry{
		Key:       key,
		Command:   command,
		Timestamp: time.Now().UnixNano(),
	}
	if value != nil {
		entry.Value = *value
	}
	return entry
}
