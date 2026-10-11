package main

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
)

var maxSSTables = map[int]int{
	0: 4,
	1: 8,
	2: 16,
	3: 32,
	4: 64,
	5: 128,
	6: 256,
}

const (
	SSTableFilePrefix  = "sstable_"
	WALDirectorySuffix = "_wal"
	maxLevel = 6
)

type level struct {
	ssTables []*SSTable
	mu       sync.RWMutex
}

type KVPair struct {
	key, val string
}

type LSMTree struct {
	memtable             *Memtable
	mu                   sync.RWMutex   
	maxMemtableSize      int64          
	directory            string         
	wal                  *WAL        
	inRecovery           bool           
	levels               []*level       
	current_sst_sequence uint64         
	compactionChan       chan int       
	flushingQueue        []*Memtable    
	flushingQueueMu      sync.RWMutex   
	flushingChan         chan *Memtable 
	ctx                  context.Context
	cancel               context.CancelFunc
	wg                   sync.WaitGroup
}

func Open(dir string, maxMemtableSize int64, recoverFromWAL bool) (*LSMTree, error) {
	ctx, cancel := context.WithCancel(context.Background())

	wal, err := OpenWal(dir+WALDirectorySuffix, true, 128000, 1000)
	if err != nil {
		cancel()
		return  nil, err
	}

	levels := make([]*level, maxLevel)
	for i := 0; i < maxLevel; i++ {
		levels[i] = &level{ssTables: make([]*SSTable, 0)}
	}

	lsm := &LSMTree{
		memtable:             NewMemTable(),
		maxMemtableSize:      maxMemtableSize,
		directory:            dir,
		wal:                  wal,
		inRecovery:           false,
		current_sst_sequence: 0,
		levels:               levels,
		compactionChan:       make(chan int, 100),
		flushingQueue:        make([]*Memtable, 0),
		flushingChan:         make(chan *Memtable, 100),
		ctx:                  ctx,
		cancel:               cancel,
	}

	if err := lsm.loadSSTables(); err != nil {
		return nil, err
	}

	lsm.wg.Add(2)
	go lsm.backgroundCompaction()
	go lsm.backgroundMemTableflushing()

	if recoverFromWAL {
		if err := lsm.recoverFromWAL(); err != nil {
			return nil, err
		}
	}

	return lsm, nil
}

func (l *LSMTree) backgroundCompaction() error {
	defer l.wg.Done()
	for {
		select{
		case <- l.ctx.Done():
			if readToExit := l.checkAndTriggerCompaction(); readToExit {
				return nil
			}
		case compactionCandidate := <- l.compactionChan:
			l.compactLevel(compactionCandidate)
		}
	}
}

func (l *LSMTree) checkAndTriggerCompaction() bool {
	var readyToExit bool = true

	for idx, level := range l.levels {
		level.mu.RLock()
		if len(level.ssTables) > maxSSTables[idx] {
			l.compactionChan <- idx
			readyToExit = false
		}
		level.mu.RUnlock()
	}
	return readyToExit
}

func (l *LSMTree) compactLevel(compactionCandidate int) error {
	if compactionCandidate == maxLevel - 1 {
		return nil
	}

	l.levels[compactionCandidate].mu.RLock()
	if len(l.levels[compactionCandidate].ssTables) < maxSSTables[compactionCandidate] {
		l.levels[compactionCandidate].mu.RUnlock()
		return nil
	}

	_, iterators := l.getSSTableHandlesAtLevel(compactionCandidate)
	l.levels[compactionCandidate].mu.RUnlock()

	mergedSSTable, err := l.mergeSSTables(iterators, compactionCandidate + 1)
	if err != nil {
		return err
	}

	l.levels[compactionCandidate].mu.Lock()
	l.levels[compactionCandidate + 1].mu.Lock()

	l.deleteSSTableAtLevel(compactionCandidate, iterators)
	l.addSSTableToLevel(mergedSSTable, compactionCandidate + 1)

	l.levels[compactionCandidate].mu.Unlock()
	l.levels[compactionCandidate + 1].mu.Unlock()

	return nil
}

func (l *LSMTree) getSSTableHandlesAtLevel(compactionCandidate int) ([]*SSTable, []*SSTableIterator){
	sstables := l.levels[compactionCandidate].ssTables
	iterators := make([]*SSTableIterator, len(sstables))

	for i, sstable := range sstables {
		iterators[i] = sstable.Front()
	}

	return sstables, iterators
}

func (l *LSMTree) mergeSSTables(iterators []*SSTableIterator, targetLevel int) (*SSTable, error) {
	mergedEntries := mergeIterators(iterators)

	sstableFileName := l.getSSTableFileName(targetLevel)

	sst, err := SerializeToSSTable(mergedEntries, sstableFileName)
	if err != nil {
		return nil, err
	}
	return sst, nil
}

func (l *LSMTree) deleteSSTableAtLevel(level int, iterators []*SSTableIterator) {
	l.levels[level].ssTables = l.levels[level].ssTables[len(iterators):]
	for _, it := range iterators {
		_ = it.s.file.Close()
		if err := os.Remove(it.s.file.Name()); err != nil {
			panic(err)
		}
	}
}

func (l *LSMTree) addSSTableToLevel(sst *SSTable, level int) {
	l.levels[level].ssTables = append(l.levels[level].ssTables, sst)
	l.compactionChan <- level
}

func (l *LSMTree) loadSSTables() error {
	if err := os.MkdirAll(l.directory, 0755); err != nil {
		return err
	}

	if err := l.loadSSTablesFromDisk(); err != nil {
		return err
	}

	l.sortSSTablesBySequenceNumber()
	l.initializeCurrentSequenceNumber()

	return nil
}

func (l *LSMTree) loadSSTablesFromDisk() error {
	files, err := os.ReadDir(l.directory)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() || !isSSTableFile(file.Name()) {
			continue
		}

		ssTable, err := OpenSSTable(l.directory + "/" + file.Name())
		if err != nil {
			return err
		}
		
		level := l.getLevelFromSSTableFileName(ssTable.file.Name())
		l.levels[level].ssTables = append(l.levels[level].ssTables, ssTable)
	}

	return nil
}

func (l *LSMTree) PUT(key string, val []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	l.memtable.Put(key, val)
	if l.memtable.SizeInbytes() > l.maxMemtableSize {
		l.flushingQueueMu.Lock()
		l.flushingQueue = append(l.flushingQueue, l.memtable)
		l.flushingQueueMu.Unlock()
		l.flushingChan <- l.memtable
		l.memtable = NewMemTable()
	}
	
	return nil
}

func (l *LSMTree) GET(key string) (string, error) {
	l.mu.RLock()
	val := l.memtable.Get(key)
	if val != nil {
		if val.Command == Command_DELETE {
			l.mu.RUnlock()
			return "", nil
		}
		return string(val.Value), nil
	}
	l.mu.RUnlock()
	
	l.flushingQueueMu.RLock()
	for i := len(l.flushingQueue) - 1; i>= 0; i-- {
		val := l.flushingQueue[i].Get(key)
		if val != nil {
			if val.Command == Command_DELETE {
				return "", nil
			}
			return string(val.Value), nil
		}
	}
	l.flushingQueueMu.RUnlock()
	
	for i := range l.levels {
		l.levels[i].mu.RLock()
		for i := len(l.levels[i].ssTables) - 1; i>= 0; i-- {
			val, err := l.levels[i].ssTables[i].Get(key)
			if err != nil {
				l.levels[i].mu.RUnlock()
				return "", err
			}
			if val != nil {
				l.levels[i].mu.RUnlock()
				if val.Command == Command_DELETE {
					return "", nil
				}
				return string(val.Value), nil
			}
		}
		l.levels[i].mu.RUnlock()
	}
	
	return "", nil
}

func (l *LSMTree) DELETE(key, val string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	l.memtable.Del(key)
	if l.memtable.SizeInbytes() > l.maxMemtableSize {
		l.flushingQueueMu.Lock()
		l.flushingQueue = append(l.flushingQueue, l.memtable)
		l.flushingQueueMu.Unlock()
		l.flushingChan <- l.memtable
		l.memtable = NewMemTable()
	}
	
	return nil
}

func (l *LSMTree) RangeScan(startKey, endKey string) ([]KVPair, error) {
	ranges := [][]*LSMEntry{}
	
	l.mu.RLock()
	defer l.mu.RUnlock()

	for _, level := range l.levels {
		level.mu.RLock()
		defer level.mu.RUnlock()
	}

	l.flushingQueueMu.RLock()
	defer l.flushingQueueMu.RUnlock()

	ranges = append(ranges, l.memtable.RangeScan(startKey, endKey))

	for i := len(l.flushingQueue) - 1; i >= 0; i-- {
		entries := l.flushingQueue[i].RangeScan(startKey, endKey)
		ranges = append(ranges, entries)
	}

	for _, level := range l.levels {
		for i := len(level.ssTables) - 1; i >= 0; i-- {
			entries, err := level.ssTables[i].RangeScan(startKey, endKey)
			if err != nil {
				return nil, err
			}
			ranges = append(ranges, entries)
		}
	}

	return mergeRanges(ranges), nil
}

func OpenSSTable(filename string) (*SSTable, error) {
	file, err := os.Open(filename)
	if err != nil{
		return nil, err
	}
	
	bloomFilter, index, dataOffset, err := ReadSSTableMetaData(file)
	return &SSTable{bloomFilter: bloomFilter, index: index, dataOffSet: dataOffset}, nil
}

func (l *LSMTree) backgroundMemTableflushing() error {
	defer l.wg.Done()
	for {
		select {
		case <- l.ctx.Done():
			if len(l.flushingChan) == 0 {
				return nil
			}
		case memtable := <- l.flushingChan:
			l.flushMemTable(memtable)
		}
	}
}

func (l *LSMTree) flushMemTable(memtable *Memtable) {
	if memtable.size == 0 {
		return
	}
	
	atomic.AddUint64(&l.current_sst_sequence, 1)
	sstableFileName := l.getSSTableFileName(0)
	sst, err := SerializeToSSTable(memtable.GetEntries(), sstableFileName)
	if err != nil {
		panic(err)
	}
	
	l.levels[0].mu.Lock()
	l.flushingQueueMu.Lock()
	
	l.levels[0].ssTables = append(l.levels[0].ssTables, sst)
	l.flushingQueue[0] = nil
	l.flushingQueue = l.flushingQueue[1:]
	
	l.flushingQueueMu.Unlock()
	l.levels[0].mu.Unlock()
	
	l.compactionChan <- 0
}

func (l *LSMTree) sortSSTablesBySequenceNumber() {
	for _, level := range l.levels {
		slices.SortFunc(level.ssTables, func(i, j *SSTable) int {
			seq1 := l.getSequenceNumber(i.file.Name())
			seq2 := l.getSequenceNumber(j.file.Name())
			return cmp.Compare(seq1, seq2)
		})
	}
}

func (l *LSMTree) getSequenceNumber(filename string) uint64 {
	sequenceStr := filename[len(l.directory)+1+2+len(SSTableFilePrefix):]
	sequence, err := strconv.ParseUint(sequenceStr, 10, 64)
	if err != nil {
		panic(err)
	}
	return sequence
}

func (l *LSMTree) getLevelFromSSTableFileName(filename string) int {
	levelStr := filename[len(l.directory) + 1 + len(SSTableFilePrefix) : len(l.directory) + 2 + len(SSTableFilePrefix)]
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		panic(err)
	}
	return level
}

func (l *LSMTree) getSSTableFileName(level int) string {
	return fmt.Sprintf("%s/%s%d_%d", l.directory, SSTableFilePrefix, level, atomic.LoadUint64(&l.current_sst_sequence))
}

func isSSTableFile(filename string) bool {
	return filename[:len(SSTableFilePrefix)] == SSTableFilePrefix
}

func (l *LSMTree) initializeCurrentSequenceNumber() error {
	var maxSequence uint64
	for _, level := range l.levels {
		if len(level.ssTables) > 0 {
			lastSSTable := level.ssTables[len(level.ssTables)-1]
			sequence := l.getSequenceNumber(lastSSTable.file.Name())
			if sequence > maxSequence {
				maxSequence = sequence
			}
		}
	}

	atomic.StoreUint64(&l.current_sst_sequence, maxSequence)

	return nil
}
