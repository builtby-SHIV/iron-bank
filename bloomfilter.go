package main

import "github.com/spaolacci/murmur3"


func NewBloomFilter(size int64) *BloomFilter {
	return &BloomFilter{
		BitSet: make([]bool, size),
		Size:   size,
	}
}

func (bf *BloomFilter) Add(item []byte) {
	h1 := murmur3.Sum64(item)
	h2 := murmur3.Sum64WithSeed(item, 1)
	h3 := murmur3.Sum64WithSeed(item, 2)

	bf.BitSet[h1 % uint64(bf.Size)] = true
	bf.BitSet[h2 % uint64(bf.Size)] = true
	bf.BitSet[h3 % uint64(bf.Size)] = true
}

func (bf *BloomFilter) Test(item []byte) bool {
	h1 := murmur3.Sum64(item)
	h2 := murmur3.Sum64WithSeed(item, 1)
	h3 := murmur3.Sum64WithSeed(item, 2)

	return bf.BitSet[h1 % uint64(bf.Size)] &&
	bf.BitSet[h2 % uint64(bf.Size)] &&
	bf.BitSet[h3 % uint64(bf.Size)] 
}