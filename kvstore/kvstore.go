package kvstore

import (
	"fmt"
	"sync"
)

type KVStore struct {
	data map[string]string
	mu sync.RWMutex
}

type NotFoundError struct {
	key string
}

func (e *NotFoundError) Error() string {
	return  fmt.Sprintf("key %s not found", e.key)
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

func (kv *KVStore) Get(k string) (string, error) {
	v, ok := kv.data[k]
	if !ok {
		return "", &NotFoundError{ key: k }
	}
	return v, nil
}