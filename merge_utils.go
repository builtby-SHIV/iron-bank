package main

import (
	"container/heap"

	"github.com/huandu/skiplist"
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
	return h[i].entry.Timestamp < h[j].entry.Timestamp
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

func mergeRanges(ranges [][]*LSMEntry) []KVPair {
	minHeap := &mergeHeap{}
	heap.Init(minHeap)

	var results []KVPair

	seen := skiplist.New(skiplist.String)

	for i, entries := range ranges {
		if len(entries) > 0 {
			heap.Push(minHeap, heapEntry{entry: entries[0], listIndex: i, idx: 0})
		}
	}

	for minHeap.Len() > 0 {
		minEntry := heap.Pop(minHeap).(heapEntry)
		previousValue := seen.Get(minEntry.entry.Key)

		if previousValue != nil {

			if previousValue.Value.(heapEntry).entry.Timestamp < minEntry.entry.Timestamp {
				seen.Set(minEntry.entry.Key, minEntry)
			}
		} else {
			seen.Set(minEntry.entry.Key, minEntry)
		}

		if minEntry.idx+1 < len(ranges[minEntry.listIndex]) {
			nextEntry := ranges[minEntry.listIndex][minEntry.idx+1]
			heap.Push(minHeap, heapEntry{entry: nextEntry, listIndex: minEntry.listIndex, idx: minEntry.idx + 1})
		}
	}

	iter := seen.Front()
	for iter != nil {
		entry := iter.Value.(heapEntry)
		if entry.entry.Command == Command_DELETE {
			iter = iter.Next()
			continue
		}
		results = append(results, KVPair{key: entry.entry.Key, val: string(entry.entry.Value)})
		iter = iter.Next()
	}

	return results
}

func mergeIterators(iterators []*SSTableIterator) []*LSMEntry {
	minHeap := &mergeHeap{}
	heap.Init(minHeap)

	var results []*LSMEntry

	seen := skiplist.New(skiplist.String)

	for _, iterator := range iterators {
		if iterator == nil {
			continue
		}
		heap.Push(minHeap, heapEntry{entry: iterator.value, iterator: iterator})
	}

	for minHeap.Len() > 0 {
		minEntry := heap.Pop(minHeap).(heapEntry)
		previousValue := seen.Get(minEntry.entry.Key)

		if previousValue != nil {
			if previousValue.Value.(heapEntry).entry.Timestamp < minEntry.entry.Timestamp {
				seen.Set(minEntry.entry.Key, minEntry)
			}
		} else {
			seen.Set(minEntry.entry.Key, minEntry)
		}

		if minEntry.iterator.Next() != nil {
			nextEntry := minEntry.iterator.value
			heap.Push(minHeap, heapEntry{entry: nextEntry, iterator: minEntry.iterator})
		}
	}

	iter := seen.Front()
	for iter != nil {
		entry := iter.Value.(heapEntry)
		if entry.entry.Command == Command_DELETE {
			iter = iter.Next()
			continue
		}
		results = append(results, entry.entry)
		iter = iter.Next()
	}

	return results
}