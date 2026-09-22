package kvstore

import "sync"

type KVStore struct {
	data map[string]string
	mu sync.RWMutex
}

func NewKVStore() *KVStore {
	return &KVStore{
		data : make(map[string]string),
	}
}

func (kv *KVStore) Set(k, v string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[k] = v
}