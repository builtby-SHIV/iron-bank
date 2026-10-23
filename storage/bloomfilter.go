package storage

import "github.com/spaolacci/murmur3"

type BloomFilter struct {
	BitSet []bool
	Size   int64
}

func NewBloomFilter(size int64) *BloomFilter {
	return &BloomFilter{
		BitSet: make([]bool, size),
		Size:   size,
	}
}

func (bf *BloomFilter) Add(item []byte) {
	if bf == nil || bf.Size <= 0 || len(bf.BitSet) == 0 {
		return
	}
	h1 := murmur3.Sum64(item)
	h2 := murmur3.Sum64WithSeed(item, 1)
	h3 := murmur3.Sum64WithSeed(item, 2)

	bf.BitSet[h1%uint64(bf.Size)] = true
	bf.BitSet[h2%uint64(bf.Size)] = true
	bf.BitSet[h3%uint64(bf.Size)] = true
}

func (bf *BloomFilter) Test(item []byte) bool {
	if bf == nil || bf.Size <= 0 || len(bf.BitSet) == 0 {
		return true
	}
	h1 := murmur3.Sum64(item)
	h2 := murmur3.Sum64WithSeed(item, 1)
	h3 := murmur3.Sum64WithSeed(item, 2)

	return bf.BitSet[h1%uint64(bf.Size)] &&
		bf.BitSet[h2%uint64(bf.Size)] &&
		bf.BitSet[h3%uint64(bf.Size)]
}
