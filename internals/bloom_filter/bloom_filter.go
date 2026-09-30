package bloomfilter

import "github.com/spaolacci/murmur3"

type BloomFilter struct {
	Bitset []bool
	Size   int64
}

func NewBloomFilter(size int64) *BloomFilter {
	return &BloomFilter{
		Bitset: make([]bool, size),
		Size:   size,
	}
}

func (bf *BloomFilter) Add(item []byte) {
	h1 := murmur3.Sum64(item)
	h2 := murmur3.Sum64WithSeed(item, 1)
	h3 := murmur3.Sum64WithSeed(item, 2)

	bf.Bitset[h1 % uint64(bf.Size)] = true
	bf.Bitset[h2 % uint64(bf.Size)] = true
	bf.Bitset[h3 % uint64(bf.Size)] = true
}

func (bf *BloomFilter) Test(item []byte) bool {
	h1 := murmur3.Sum64(item)
	h2 := murmur3.Sum64WithSeed(item, 1)
	h3 := murmur3.Sum64WithSeed(item, 2)

	return bf.Bitset[h1 % uint64(bf.Size)] &&
	bf.Bitset[h2 % uint64(bf.Size)] &&
	bf.Bitset[h3 % uint64(bf.Size)] 
}