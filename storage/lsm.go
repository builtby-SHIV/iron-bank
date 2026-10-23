package storage

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iron-bank/proto"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
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
	maxLevel           = 7
)

type level struct {
	ssTables []*SSTable
	mu       sync.RWMutex
}

type KVPair struct {
	Key, Val string
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
	flushingWg           sync.WaitGroup
	compactionWg         sync.WaitGroup
	closed               bool
}

func Open(dir string, maxMemtableSize int64, recoverFromWAL bool) (*LSMTree, error) {
	ctx, cancel := context.WithCancel(context.Background())

	wal, err := OpenWAL(dir+WALDirectorySuffix, true, 128000, 1000)
	if err != nil {
		cancel()
		return nil, err
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
		_ = wal.Close()
		cancel()
		return nil, err
	}

	lsm.compactionWg.Add(1)
	go lsm.backgroundCompaction()

	lsm.flushingWg.Add(1)
	go lsm.backgroundMemTableflushing()

	if recoverFromWAL {
		if err := lsm.recoverFromWAL(); err != nil {
			_ = lsm.Close()
			return nil, err
		}
	}

	return lsm, nil
}

func (l *LSMTree) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true

	if l.memtable != nil && l.memtable.size > 0 {
		l.flushingQueueMu.Lock()
		l.flushingQueue = append(l.flushingQueue, l.memtable)
		l.flushingQueueMu.Unlock()

		l.flushingChan <- l.memtable
		l.memtable = NewMemTable()
	}
	close(l.flushingChan)
	l.mu.Unlock()

	l.flushingWg.Wait()

	l.cancel()
	l.compactionWg.Wait()

	if l.wal != nil {
		_ = l.wal.Close()
	}

	for _, lvl := range l.levels {
		lvl.mu.Lock()
		for _, sst := range lvl.ssTables {
			if sst != nil {
				_ = sst.Close()
			}
		}
		lvl.mu.Unlock()
	}

	return nil
}

func (l *LSMTree) LastLogEntry() (*proto.LogEntry, error) {
	return l.wal.getLastEntryInLog()
}

func (l *LSMTree) backgroundCompaction() {
	defer l.compactionWg.Done()
	for {
		select {
		case <-l.ctx.Done():
			return
		case compactionCandidate, ok := <-l.compactionChan:
			if !ok {
				return
			}
			_ = l.compactLevel(compactionCandidate)
		}
	}
}

func (l *LSMTree) checkAndTriggerCompaction() bool {
	readyToExit := true

	for idx, lvl := range l.levels {
		if idx >= maxLevel-1 {
			continue
		}
		lvl.mu.RLock()
		needsCompact := len(lvl.ssTables) > maxSSTables[idx]
		lvl.mu.RUnlock()

		if needsCompact {
			readyToExit = false
			select {
			case l.compactionChan <- idx:
			default:
			}
		}
	}
	return readyToExit
}

func (l *LSMTree) compactLevel(compactionCandidate int) error {
	if compactionCandidate >= maxLevel-1 {
		return nil
	}

	l.levels[compactionCandidate].mu.RLock()
	if len(l.levels[compactionCandidate].ssTables) <= maxSSTables[compactionCandidate] {
		l.levels[compactionCandidate].mu.RUnlock()
		return nil
	}

	sstables, iterators := l.getSSTableHandlesAtLevel(compactionCandidate)
	l.levels[compactionCandidate].mu.RUnlock()

	targetLevel := compactionCandidate + 1
	mergedSSTable, err := l.mergeSSTables(iterators, targetLevel)
	if err != nil {
		for _, it := range iterators {
			if it != nil {
				_ = it.Close()
			}
		}
		return err
	}

	l.levels[compactionCandidate].mu.Lock()
	l.levels[targetLevel].mu.Lock()

	l.deleteSSTablesAtLevel(compactionCandidate, sstables, iterators)
	l.levels[targetLevel].ssTables = append(l.levels[targetLevel].ssTables, mergedSSTable)

	checkTarget := len(l.levels[targetLevel].ssTables) > maxSSTables[targetLevel]
	checkCurrent := len(l.levels[compactionCandidate].ssTables) > maxSSTables[compactionCandidate]

	l.levels[targetLevel].mu.Unlock()
	l.levels[compactionCandidate].mu.Unlock()

	if checkTarget && targetLevel < maxLevel-1 {
		select {
		case l.compactionChan <- targetLevel:
		default:
		}
	}
	if checkCurrent {
		select {
		case l.compactionChan <- compactionCandidate:
		default:
		}
	}

	return nil
}

func (l *LSMTree) getSSTableHandlesAtLevel(compactionCandidate int) ([]*SSTable, []*SSTableIterator) {
	sstables := l.levels[compactionCandidate].ssTables
	iterators := make([]*SSTableIterator, len(sstables))

	for i, sstable := range sstables {
		iterators[i] = sstable.Front()
	}

	return sstables, iterators
}

func (l *LSMTree) mergeSSTables(iterators []*SSTableIterator, targetLevel int) (*SSTable, error) {
	dropTombstones := targetLevel >= maxLevel-1
	mergedEntries := mergeIterators(iterators, dropTombstones)

	atomic.AddUint64(&l.current_sst_sequence, 1)
	sstableFileName := l.getSSTableFileName(targetLevel)

	sst, err := SerializeToSSTable(mergedEntries, sstableFileName)
	if err != nil {
		return nil, err
	}
	return sst, nil
}

func (l *LSMTree) deleteSSTablesAtLevel(level int, sstables []*SSTable, iterators []*SSTableIterator) {
	for _, it := range iterators {
		if it != nil {
			_ = it.Close()
		}
	}

	if len(l.levels[level].ssTables) >= len(sstables) {
		l.levels[level].ssTables = l.levels[level].ssTables[len(sstables):]
	} else {
		l.levels[level].ssTables = nil
	}

	for _, sst := range sstables {
		if sst != nil && sst.file != nil {
			fileName := sst.file.Name()
			_ = sst.file.Close()
			_ = os.Remove(fileName)
		}
	}
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

		fullPath := filepath.Join(l.directory, file.Name())
		ssTable, err := OpenSSTable(fullPath)
		if err != nil {
			return err
		}

		lvl := l.getLevelFromSSTableFileName(file.Name())
		if lvl < 0 || lvl >= maxLevel {
			_ = ssTable.Close()
			continue
		}
		l.levels[lvl].ssTables = append(l.levels[lvl].ssTables, ssTable)
	}

	return nil
}

func (l *LSMTree) PUT(key string, val []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.inRecovery {
		if err := l.wal.WriteEntry(proto.Command_PUT, key, val); err != nil {
			return err
		}
	}

	l.memtable.Put(key, val)
	if l.memtable.SizeInbytes() > l.maxMemtableSize {
		l.enqueueMemtableFlush()
	}

	return nil
}

func (l *LSMTree) Put(key string, val []byte) error {
	return l.PUT(key, val)
}

func (l *LSMTree) GET(key string) (string, error) {
	l.mu.RLock()
	val := l.memtable.Get(key)
	l.mu.RUnlock()

	if val != nil {
		if val.Command == proto.Command_DELETE {
			return "", &NotFoundError{key: key}
		}
		return string(val.Value), nil
	}

	l.flushingQueueMu.RLock()
	for i := len(l.flushingQueue) - 1; i >= 0; i-- {
		entry := l.flushingQueue[i].Get(key)
		if entry != nil {
			l.flushingQueueMu.RUnlock()
			if entry.Command == proto.Command_DELETE {
				return "", &NotFoundError{key: key}
			}
			return string(entry.Value), nil
		}
	}
	l.flushingQueueMu.RUnlock()

	for i := range l.levels {
		l.levels[i].mu.RLock()
		for j := len(l.levels[i].ssTables) - 1; j >= 0; j-- {
			entry, err := l.levels[i].ssTables[j].Get(key)
			if err != nil {
				l.levels[i].mu.RUnlock()
				return "", err
			}
			if entry != nil {
				l.levels[i].mu.RUnlock()
				if entry.Command == proto.Command_DELETE {
					return "", &NotFoundError{key: key}
				}
				return string(entry.Value), nil
			}
		}
		l.levels[i].mu.RUnlock()
	}

	return "", &NotFoundError{key: key}
}

func (l *LSMTree) Get(key string) (string, error) {
	return l.GET(key)
}

func (l *LSMTree) DELETE(key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.inRecovery {
		if err := l.wal.WriteEntry(proto.Command_DELETE, key, nil); err != nil {
			return err
		}
	}

	l.memtable.Del(key)
	if l.memtable.SizeInbytes() > l.maxMemtableSize {
		l.enqueueMemtableFlush()
	}

	return nil
}

func (l *LSMTree) Del(key string) error {
	return l.DELETE(key)
}

func (l *LSMTree) enqueueMemtableFlush() {
	l.flushingQueueMu.Lock()
	l.flushingQueue = append(l.flushingQueue, l.memtable)
	l.flushingQueueMu.Unlock()
	l.flushingChan <- l.memtable
	l.memtable = NewMemTable()
}

func (l *LSMTree) RangeScan(startKey, endKey string) ([]KVPair, error) {
	if startKey > endKey {
		return []KVPair{}, nil
	}

	var ranges [][]*proto.LSMEntry

	l.mu.RLock()
	defer l.mu.RUnlock()

	l.flushingQueueMu.RLock()
	defer l.flushingQueueMu.RUnlock()

	for _, lvl := range l.levels {
		lvl.mu.RLock()
		defer lvl.mu.RUnlock()
	}

	ranges = append(ranges, l.memtable.RangeScan(startKey, endKey))

	for i := len(l.flushingQueue) - 1; i >= 0; i-- {
		entries := l.flushingQueue[i].RangeScan(startKey, endKey)
		ranges = append(ranges, entries)
	}

	for _, lvl := range l.levels {
		for i := len(lvl.ssTables) - 1; i >= 0; i-- {
			entries, err := lvl.ssTables[i].RangeScan(startKey, endKey)
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
	if err != nil {
		return nil, err
	}

	bloomFilter, index, dataOffset, err := ReadSSTableMetaData(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &SSTable{bloomFilter: bloomFilter, index: index, file: file, dataOffSet: dataOffset}, nil
}

func (l *LSMTree) backgroundMemTableflushing() {
	defer l.flushingWg.Done()
	for memtable := range l.flushingChan {
		l.flushMemTable(memtable)
	}
}

func (l *LSMTree) flushMemTable(memtable *Memtable) {
	if memtable == nil || memtable.size == 0 {
		l.flushingQueueMu.Lock()
		if len(l.flushingQueue) > 0 && l.flushingQueue[0] == memtable {
			l.flushingQueue[0] = nil
			l.flushingQueue = l.flushingQueue[1:]
		}
		l.flushingQueueMu.Unlock()
		return
	}

	atomic.AddUint64(&l.current_sst_sequence, 1)
	sstableFileName := l.getSSTableFileName(0)
	sst, err := SerializeToSSTable(memtable.GetEntries(), sstableFileName)
	if err != nil {
		panic(err)
	}

	l.flushingQueueMu.Lock()
	l.levels[0].mu.Lock()

	l.levels[0].ssTables = append(l.levels[0].ssTables, sst)
	if len(l.flushingQueue) > 0 {
		l.flushingQueue[0] = nil
		l.flushingQueue = l.flushingQueue[1:]
	}

	l.levels[0].mu.Unlock()
	l.flushingQueueMu.Unlock()

	select {
	case l.compactionChan <- 0:
	default:
	}
}

func parseSSTableFileName(filename string) (int, uint64, error) {
	base := filepath.Base(filename)
	if !strings.HasPrefix(base, SSTableFilePrefix) {
		return 0, 0, fmt.Errorf("not an sstable file: %s", filename)
	}
	trimmed := strings.TrimPrefix(base, SSTableFilePrefix)
	parts := strings.Split(trimmed, "_")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid sstable filename format: %s", filename)
	}
	level, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, err
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return level, seq, nil
}

func (l *LSMTree) sortSSTablesBySequenceNumber() {
	for _, lvl := range l.levels {
		slices.SortFunc(lvl.ssTables, func(i, j *SSTable) int {
			if i.file == nil || j.file == nil {
				return 0
			}
			seq1 := l.getSequenceNumber(i.file.Name())
			seq2 := l.getSequenceNumber(j.file.Name())
			return cmp.Compare(seq1, seq2)
		})
	}
}

func (l *LSMTree) getSequenceNumber(filename string) uint64 {
	_, seq, err := parseSSTableFileName(filename)
	if err != nil {
		return 0
	}
	return seq
}

func (l *LSMTree) getLevelFromSSTableFileName(filename string) int {
	lvl, _, err := parseSSTableFileName(filename)
	if err != nil {
		return 0
	}
	return lvl
}

func (l *LSMTree) getSSTableFileName(level int) string {
	return filepath.Join(l.directory, fmt.Sprintf("%s%d_%d", SSTableFilePrefix, level, atomic.LoadUint64(&l.current_sst_sequence)))
}

func isSSTableFile(filename string) bool {
	return strings.HasPrefix(filepath.Base(filename), SSTableFilePrefix)
}

func (l *LSMTree) initializeCurrentSequenceNumber() error {
	var maxSequence uint64
	for _, lvl := range l.levels {
		for _, sst := range lvl.ssTables {
			if sst != nil && sst.file != nil {
				seq := l.getSequenceNumber(sst.file.Name())
				if seq > maxSequence {
					maxSequence = seq
				}
			}
		}
	}

	atomic.StoreUint64(&l.current_sst_sequence, maxSequence)
	return nil
}

func (l *LSMTree) recoverFromWAL() error {
	l.inRecovery = true
	defer func() {
		l.inRecovery = false
	}()
	entries, err := l.readEntriesFromWAL()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if err := l.processWALEntry(entry); err != nil {
			return err
		}
	}

	return nil
}

func (l *LSMTree) readEntriesFromWAL() ([]*proto.LogEntry, error) {
	entries, err := l.wal.ReadAll()
	if err != nil {
		repairedEntries, repErr := l.wal.Repair()
		if repErr != nil {
			return nil, repErr
		}
		return repairedEntries, nil
	}
	return entries, nil
}

func (l *LSMTree) processWALEntry(entry *proto.LogEntry) error {
	switch entry.Command {
	case proto.Command_PUT:
		return l.PUT(entry.Key, entry.Value)
	case proto.Command_DELETE:
		return l.DELETE(entry.Key)
	case proto.Command_WRITE_SST:
		return errors.New("unexpected write sst command in WAL")
	default:
		return errors.New("unknown command in WAL")
	}
}
