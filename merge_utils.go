package main

import (
	"container/heap"
)

type heapEntry struct {
	entry     *LSMEntry
	listIndex int
	idx       int
	iterator  *SSTableIterator
}

type mergeHeap []heapEntry

func (h mergeHeap) Len() int {
	return len(h)
}

func (h mergeHeap) Less(i, j int) bool {
	if h[i].entry.Key != h[j].entry.Key {
		return h[i].entry.Key < h[j].entry.Key
	}
	if h[i].entry.Timestamp != h[j].entry.Timestamp {
		return h[i].entry.Timestamp > h[j].entry.Timestamp // newer timestamp wins
	}
	return h[i].listIndex > h[j].listIndex // higher list index (newer SSTable) wins
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(heapEntry))
}

func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type rangeMergeHeap []heapEntry

func (h rangeMergeHeap) Len() int {
	return len(h)
}

func (h rangeMergeHeap) Less(i, j int) bool {
	if h[i].entry.Key != h[j].entry.Key {
		return h[i].entry.Key < h[j].entry.Key
	}
	if h[i].entry.Timestamp != h[j].entry.Timestamp {
		return h[i].entry.Timestamp > h[j].entry.Timestamp
	}
	return h[i].listIndex < h[j].listIndex // lower listIndex (memtable / newer queue) wins
}

func (h rangeMergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *rangeMergeHeap) Push(x interface{}) {
	*h = append(*h, x.(heapEntry))
}

func (h *rangeMergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func mergeRanges(ranges [][]*LSMEntry) []KVPair {
	minHeap := &rangeMergeHeap{}
	heap.Init(minHeap)

	for i, entries := range ranges {
		if len(entries) > 0 {
			heap.Push(minHeap, heapEntry{entry: entries[0], listIndex: i, idx: 0})
		}
	}

	var results []KVPair

	for minHeap.Len() > 0 {
		top := heap.Pop(minHeap).(heapEntry)
		currentKey := top.entry.Key
		winner := top.entry

		if top.idx+1 < len(ranges[top.listIndex]) {
			nextEntry := ranges[top.listIndex][top.idx+1]
			heap.Push(minHeap, heapEntry{entry: nextEntry, listIndex: top.listIndex, idx: top.idx + 1})
		}

		for minHeap.Len() > 0 && (*minHeap)[0].entry.Key == currentKey {
			dup := heap.Pop(minHeap).(heapEntry)
			if dup.entry.Timestamp > winner.Timestamp {
				winner = dup.entry
			}
			if dup.idx+1 < len(ranges[dup.listIndex]) {
				nextEntry := ranges[dup.listIndex][dup.idx+1]
				heap.Push(minHeap, heapEntry{entry: nextEntry, listIndex: dup.listIndex, idx: dup.idx + 1})
			}
		}

		results = append(results, KVPair{Key: winner.Key, Val: string(winner.Value)})
	}

	return results
}

func mergeIterators(iterators []*SSTableIterator, dropTombstones bool) []*LSMEntry {
	minHeap := &mergeHeap{}
	heap.Init(minHeap)

	for i, iterator := range iterators {
		if iterator == nil || iterator.value == nil {
			continue
		}
		heap.Push(minHeap, heapEntry{entry: iterator.value, iterator: iterator, listIndex: i})
	}

	var results []*LSMEntry

	for minHeap.Len() > 0 {
		top := heap.Pop(minHeap).(heapEntry)
		currentKey := top.entry.Key
		winner := top.entry

		if top.iterator.Next() != nil {
			heap.Push(minHeap, heapEntry{entry: top.iterator.value, iterator: top.iterator, listIndex: top.listIndex})
		}

		// Drain duplicates for the same key
		for minHeap.Len() > 0 && (*minHeap)[0].entry.Key == currentKey {
			dup := heap.Pop(minHeap).(heapEntry)
			if dup.entry.Timestamp > winner.Timestamp {
				winner = dup.entry
			}
			if dup.iterator.Next() != nil {
				heap.Push(minHeap, heapEntry{entry: dup.iterator.value, iterator: dup.iterator, listIndex: dup.listIndex})
			}
		}

		results = append(results, winner)
	}

	return results
}