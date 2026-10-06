package main

import (
	"cmp"
	"context"
	"os"
	"slices"
	"strconv"
	"sync"
)

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
	wal                  *gw.WAL        
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

func Open(dir string, maxMemtableSize int64) (*LSMEntry, error) {
	ctx, cancel := context.WithCancel(context.Background())

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

	lsm.wg.Add(1)
	go lsm.backgroundMemTableflushing()

	return lsm, nil
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
		
		level := getLevelFromSSTableFileName(ssTable.file.Name())
		l.levels[level].ssTables = append(l.levels[level].ssTables, ssTable)
	}

	return nil
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

func (l *LSMTree) getLevelFromSSTableFilename(filename string) int {
	levelStr := filename[len(l.directory) + 1 + len(SSTableFilePrefix) : len(l.directory) + 2 + len(SSTableFilePrefix)]
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		panic(err)
	}
	return level
}

func (l *LSMTree) PUT(key, val string) error {
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