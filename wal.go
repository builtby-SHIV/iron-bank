package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"internal/strconv"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type WAL struct {
	directory           string
	currentSegment      *os.File
	lock                sync.Mutex
	lastSequenceNo      uint64
	bufWriter           *bufio.Writer
	syncTimer           *time.Timer
	shouldFsync         bool
	maxFileSize         int64
	maxSegments         int64
	currentSegmentIndex int64
	ctx                 context.Context
	cancel              context.CancelFunc
}

var ( 
	ErrOpenAndCreateWAL = errors.New("cannot open or create log file")
	FileNotFound = errors.New("log file not found")
)

const (
	syncInterval  = 200 * time.Millisecond
	segmentPrefix = "segment-"
)

func OpenWAL(directory string, enableFsync bool, maxFileSize int64, maxSegments int64) (*WAL, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	files, err := filepath.Glob(filepath.Join(directory, segmentPrefix + "*"))
	if err != nil {
		return nil, err
	}

	var lastSegmentID int64
	if len(files) > 0 {
		lastSegmentID, err = findLastSegmentIndexinFiles(files)
		if err != nil {
			return nil, err
		}
	} else {
		file, err := createSegmentFile(directory, 0)
		if err != nil {
			return nil, err
		}

		if err := file.Close(); err != nil {
			return nil, err
		}
	}

	filePath := filepath.Join(directory, fmt.Sprintf("%s%d", segmentPrefix, lastSegmentID))
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	wal := &WAL{
		directory:           directory,
		currentSegment:      file,
		lastSequenceNo:      0,
		bufWriter:           bufio.NewWriter(file),
		syncTimer:           time.NewTimer(syncInterval), // syncInterval is a predefined duration
		shouldFsync:         enableFsync,
		maxFileSize:         maxFileSize,
		maxSegments:         maxSegments,
		currentSegmentIndex: lastSegmentID,
		ctx:                 ctx,
		cancel:              cancel,
	}

	if wal.lastSequenceNo, err = wal.getLastSequenceNo(); err != nil {
		return nil, err
	}

	go wal.keepSyncing()

	return wal, nil
}

func findLastSegmentIndexinFiles(files []string) (int64, error) {
	var lastSegmentId int64
	for _, file := range files {
		_, fileName := filepath.Split(file)
		segmentId, err := strconv.Atoi(strings.TrimPrefix(fileName, segmentPrefix))
		if err != nil {
			return 0, err
		}
		if lastSegmentId < int64(segmentId) {
			lastSegmentId = int64(segmentId)
		}
	}
	return lastSegmentId, nil
}

func createSegmentFile(directory string, segmentID int) (*os.File, error) {
	filePath := filepath.Join(directory, fmt.Sprintf("segment-%d", segmentID))
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (wal *WAL) getLastSequenceNo() (uint64, error) {
	entry, err := wal.getLastEntryInLog()
	if err != nil {
		return 0, err
	}

	if entry != nil {
		return entry.GetLogSequenceNumber(), nil
	}

	return 0, nil
}