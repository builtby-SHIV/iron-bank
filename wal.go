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
	maxSegments         int
	currentSegmentIndex int
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

func OpenWAL(directory string, enableFsync bool, maxFileSize int64, maxSegments int) (*WAL, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	files, err := filepath.Glob(filepath.Join(directory, segmentPrefix + "*"))
	if err != nil {
		return nil, err
	}

	var lastSegmentID int
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

// func (w *WAL) WriteToWal(op byte, kv *KVStore, key, val []byte) error {
// 	w.mu.Lock()
// 	defer w.mu.Unlock()
// 	keyLen := len(key)
// 	valLen := len(val)

// 	totalSize := Header + int(keyLen) + int(valLen)
// 	if totalSize > cap(w.buf) {
// 		w.buf = make([]byte, totalSize)
// 	} else {
// 		w.buf = w.buf[:totalSize]
// 	}

// 	w.buf[4] = op
// 	binary.BigEndian.PutUint64(w.buf[5:13], uint64(keyLen))
// 	binary.BigEndian.PutUint64(w.buf[13:21], uint64(valLen))
// 	copy(w.buf[21:21+keyLen], key)
// 	copy(w.buf[21+keyLen:], val)

// 	checksum := crc32.ChecksumIEEE(w.buf[4:])
// 	binary.BigEndian.PutUint32(w.buf[0:4], checksum)

// 	if _, err := w.file.Write(w.buf); err != nil {
// 		return err
// 	}
// 	w.file.Sync()
// 	return nil
// }

// func (w *WAL) ReplayLog(kv *KVStore) error {
// 	st, err := w.file.Stat()
// 	if err != nil {
// 		return err
// 	}
// 	size := st.Size()
// 	var off int64
// 	for off < size {
// 		if size - off < Header {
// 			break
// 		}
// 		buf := make([]byte, 21)
// 		_, err := w.file.ReadAt(buf, off)
// 		if err == io.EOF {
// 			return nil
// 		}
// 		if err != nil {
// 			fmt.Println("no error is from here")
// 			return err
// 		}

// 		checksum := binary.BigEndian.Uint32(buf[0:4])

// 		crc := crc32.NewIEEE()
// 		crc.Write(buf[4:21])

// 		op := buf[4]
// 		key_len := binary.BigEndian.Uint64(buf[5:13])
// 		val_len := binary.BigEndian.Uint64(buf[13:21])
// 		buf = make([]byte, key_len + val_len)
// 		if _, err := w.file.ReadAt(buf, off + Header); err != nil {
// 			return err
// 		}
// 		crc.Write(buf)
// 		key, val := buf[:key_len], buf[key_len:]

// 		recEnd := off + Header + int64(key_len + val_len)
// 		if checksum != crc.Sum32() {
// 			if recEnd < size {
// 				return fmt.Errorf("corrupt record at offset %d, %d bytes follow", off, size-recEnd)
// 			}
// 			break
// 		} else {
// 			if op == 1 {
// 				kv.Set(string(key), string(val))
// 			} else {
// 				kv.Del(string(key))
// 			}
// 		}
// 		off = recEnd
// 	}

// 	if off < size {
// 		if err = w.file.Truncate(off); err != nil { return err }
// 		if err = w.file.Sync(); err != nil { return err }
// 	}

// 	w.offset = off

// 	return nil
// }


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