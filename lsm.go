package main

import (
	"context"
	"sync"
)

const (
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