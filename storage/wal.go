package storage

import (
	"bufio"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	googleproto "google.golang.org/protobuf/proto"
	"iron-bank/proto"
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
	FileNotFound        = errors.New("log file not found")
)

const (
	syncInterval  = 200 * time.Millisecond
	segmentPrefix = "segment-"
)

func OpenWAL(directory string, enableFsync bool, maxFileSize int64, maxSegments int64) (*WAL, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	files, err := filepath.Glob(filepath.Join(directory, segmentPrefix+"*"))
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
		lastSegmentID = 0
	}

	filePath := filepath.Join(directory, fmt.Sprintf("%s%d", segmentPrefix, lastSegmentID))
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	wal := &WAL{
		directory:           directory,
		currentSegment:      file,
		lastSequenceNo:      0,
		bufWriter:           bufio.NewWriter(file),
		syncTimer:           time.NewTimer(syncInterval),
		shouldFsync:         enableFsync,
		maxFileSize:         maxFileSize,
		maxSegments:         maxSegments,
		currentSegmentIndex: lastSegmentID,
		ctx:                 ctx,
		cancel:              cancel,
	}

	if wal.lastSequenceNo, err = wal.getLastSequenceNo(); err != nil {
		_ = wal.Close()
		return nil, err
	}

	go wal.keepSyncing()

	return wal, nil
}

func findLastSegmentIndexinFiles(files []string) (int64, error) {
	var lastSegmentId int64 = -1
	for _, file := range files {
		fileName := filepath.Base(file)
		if !strings.HasPrefix(fileName, segmentPrefix) || strings.HasSuffix(fileName, ".tmp") {
			continue
		}
		segmentId, err := strconv.ParseInt(strings.TrimPrefix(fileName, segmentPrefix), 10, 64)
		if err != nil {
			continue
		}
		if segmentId > lastSegmentId {
			lastSegmentId = segmentId
		}
	}
	if lastSegmentId == -1 {
		return 0, nil
	}
	return lastSegmentId, nil
}

func createSegmentFile(directory string, segmentID int) (*os.File, error) {
	filePath := filepath.Join(directory, fmt.Sprintf("%s%d", segmentPrefix, segmentID))
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func calculateCRC(val []byte, seq uint64) uint32 {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, seq)
	return crc32.ChecksumIEEE(append(val, buf...))
}

func verifyCRC(entry *proto.LogEntry) bool {
	return entry.CRC == calculateCRC(entry.GetValue(), entry.GetIndex())
}

func (w *WAL) WriteEntry(cmd proto.Command, key string, val []byte) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if err := w.rotateIfNeeded(); err != nil {
		return err
	}

	nextSeq := w.lastSequenceNo + 1

	entry := &proto.LogEntry{
		Index:    nextSeq,
		Key:      key,
		Value:    val,
		Command:  cmd,
		CRC:      calculateCRC(val, nextSeq),
	}

	data := proto.MustMarshal(entry)

	if err := binary.Write(w.bufWriter, binary.LittleEndian, int64(len(data))); err != nil {
		return err
	}
	if _, err := w.bufWriter.Write(data); err != nil {
		return err
	}

	if err := w.bufWriter.Flush(); err != nil {
		return err
	}

	if w.shouldFsync {
		if err := w.currentSegment.Sync(); err != nil {
			return err
		}
	}

	w.lastSequenceNo = nextSeq
	return nil
}

func (w *WAL) rotateIfNeeded() error {
	fileInfo, err := w.currentSegment.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size()+int64(w.bufWriter.Buffered()) >= w.maxFileSize {
		if err := w.rotateLog(); err != nil {
			return err
		}
	}

	return nil
}

func (w *WAL) rotateLog() error {
	if err := w.sync(); err != nil {
		return err
	}

	if err := w.currentSegment.Close(); err != nil {
		return err
	}

	w.currentSegmentIndex++
	newFile, err := createSegmentFile(w.directory, int(w.currentSegmentIndex))
	if err != nil {
		return err
	}
	w.currentSegment = newFile
	w.bufWriter = bufio.NewWriter(newFile)

	files, err := filepath.Glob(filepath.Join(w.directory, segmentPrefix+"*"))
	if err == nil && int64(len(files)) > w.maxSegments {
		_ = w.deleteOldestSegment()
	}

	return nil
}

func (w *WAL) roatateLog() error {
	return w.rotateLog()
}

func (w *WAL) deleteOldestSegment() error {
	files, err := filepath.Glob(filepath.Join(w.directory, segmentPrefix+"*"))
	if err != nil || len(files) == 0 {
		return err
	}

	oldestSegmentFilePath, err := w.findOldestSegmentFile(files)
	if err != nil {
		return err
	}

	if oldestSegmentFilePath == w.currentSegment.Name() {
		return nil
	}

	return os.Remove(oldestSegmentFilePath)
}

func (w *WAL) findOldestSegmentFile(files []string) (string, error) {
	var oldestSegment string
	oldestSegmentID := int64(math.MaxInt64)

	for _, file := range files {
		fileName := filepath.Base(file)
		if !strings.HasPrefix(fileName, segmentPrefix) || strings.HasSuffix(fileName, ".tmp") {
			continue
		}
		segmentID, err := strconv.ParseInt(strings.TrimPrefix(fileName, segmentPrefix), 10, 64)
		if err != nil {
			continue
		}
		if segmentID < oldestSegmentID {
			oldestSegmentID = segmentID
			oldestSegment = file
		}
	}

	if oldestSegment == "" {
		return "", errors.New("no valid segment found")
	}

	return oldestSegment, nil
}

func (w *WAL) Close() error {
	w.cancel()
	w.syncTimer.Stop()

	w.lock.Lock()
	defer w.lock.Unlock()

	if err := w.bufWriter.Flush(); err != nil {
		_ = w.currentSegment.Close()
		return err
	}
	if w.shouldFsync {
		_ = w.currentSegment.Sync()
	}
	return w.currentSegment.Close()
}

func (wal *WAL) getLastSequenceNo() (uint64, error) {
	files, err := filepath.Glob(filepath.Join(wal.directory, segmentPrefix+"*"))
	if err != nil || len(files) == 0 {
		return 0, nil
	}

	type segInfo struct {
		id   int64
		path string
	}
	var segs []segInfo
	for _, f := range files {
		base := filepath.Base(f)
		if !strings.HasPrefix(base, segmentPrefix) || strings.HasSuffix(base, ".tmp") {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimPrefix(base, segmentPrefix), 10, 64)
		if err != nil {
			continue
		}
		segs = append(segs, segInfo{id: id, path: f})
	}

	slices.SortFunc(segs, func(a, b segInfo) int {
		return cmp.Compare(b.id, a.id) // descending order
	})

	for _, seg := range segs {
		file, err := os.OpenFile(seg.path, os.O_RDONLY, 0644)
		if err != nil {
			continue
		}
		entry, err := wal.getLastEntryInFile(file)
		_ = file.Close()
		if err == nil && entry != nil {
			return entry.GetIndex(), nil
		}
	}

	return 0, nil
}

func (wal *WAL) getLastEntryInFile(file *os.File) (*proto.LogEntry, error) {
	var previousSize int64
	var offset int64

	for {
		var size int64
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				if offset == 0 && previousSize == 0 {
					return nil, nil
				}

				if _, err := file.Seek(offset, io.SeekStart); err != nil {
					return nil, err
				}

				data := make([]byte, previousSize)
				if _, err := io.ReadFull(file, data); err != nil {
					return nil, err
				}

				return unmarshalAndVerify(data)
			}
			return nil, err
		}

		curOffset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		offset = curOffset
		previousSize = size

		if _, err := file.Seek(size, io.SeekCurrent); err != nil {
			return nil, err
		}
	}
}

func (wal *WAL) GetLastEntryInLog() (*proto.LogEntry, error) {
	file, err := os.OpenFile(wal.currentSegment.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return wal.getLastEntryInFile(file)
}

func (w *WAL) ReadAllEntries(file *os.File) ([]*proto.LogEntry, error) {
	var entries []*proto.LogEntry

	var size int64
	for {
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				break
			}
			return entries, err
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return entries, fmt.Errorf("unexpected end of file reading entry")
			}
			return entries, err
		}

		entry, err := unmarshalAndVerify(data)
		if err != nil {
			return entries, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func unmarshalAndVerify(data []byte) (*proto.LogEntry, error) {
	var entry proto.LogEntry
	if err := googleproto.Unmarshal(data, &entry); err != nil {
		return nil, err
	}

	if !verifyCRC(&entry) {
		return nil, fmt.Errorf("CRC mismatch, data might be corrupted")
	}
	return &entry, nil
}

func (w *WAL) ReadAll() ([]*proto.LogEntry, error) {
	return w.readAllFromOffset(-1)
}

func (w *WAL) readAllFromOffset(offset int) ([]*proto.LogEntry, error) {
	files, err := filepath.Glob(filepath.Join(w.directory, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	type segInfo struct {
		id   int64
		path string
	}
	var segments []segInfo
	for _, file := range files {
		fileName := filepath.Base(file)
		if !strings.HasPrefix(fileName, segmentPrefix) || strings.HasSuffix(fileName, ".tmp") {
			continue
		}
		segmentIndex, err := strconv.ParseInt(strings.TrimPrefix(fileName, segmentPrefix), 10, 64)
		if err != nil {
			continue
		}
		if segmentIndex < int64(offset) {
			continue
		}
		segments = append(segments, segInfo{id: segmentIndex, path: file})
	}

	slices.SortFunc(segments, func(a, b segInfo) int {
		return cmp.Compare(a.id, b.id)
	})

	var entries []*proto.LogEntry

	for _, seg := range segments {
		file, err := os.OpenFile(seg.path, os.O_RDONLY, 0644)
		if err != nil {
			return nil, err
		}

		entries_from_segment, err := w.readAllEntries(file)
		_ = file.Close()
		if err != nil {
			return entries, err
		}

		entries = append(entries, entries_from_segment...)
	}

	return entries, nil
}

func (wal *WAL) sync() error {
	if err := wal.bufWriter.Flush(); err != nil {
		return err
	}
	if wal.shouldFsync {
		if err := wal.currentSegment.Sync(); err != nil {
			return err
		}
	}

	wal.resetTimer()
	return nil
}

func (wal *WAL) resetTimer() {
	wal.syncTimer.Reset(syncInterval)
}

func (wal *WAL) keepSyncing() {
	for {
		select {
		case <-wal.syncTimer.C:
			wal.lock.Lock()
			err := wal.sync()
			wal.lock.Unlock()
			if err != nil {
				log.Printf("Error while performing sync: %v", err)
			}

		case <-wal.ctx.Done():
			return
		}
	}
}

func (wal *WAL) Repair() ([]*proto.LogEntry, error) {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	_ = wal.bufWriter.Flush()

	files, err := filepath.Glob(filepath.Join(wal.directory, segmentPrefix+"*"))
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
		return nil, errors.New("no log segments found, nothing to repair")
	}

	filePath := filepath.Join(wal.directory, fmt.Sprintf("%s%d", segmentPrefix, lastSegmentID))
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	var entries []*proto.LogEntry
	needsRepair := false

	for {
		var size int64
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				break
			}
			needsRepair = true
			break
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			needsRepair = true
			break
		}

		var entry proto.LogEntry
		if err := googleproto.Unmarshal(data, &entry); err != nil {
			needsRepair = true
			break
		}

		if !verifyCRC(&entry) {
			needsRepair = true
			break
		}

		entries = append(entries, &entry)
	}
	_ = file.Close()

	if needsRepair {
		if err := wal.replaceWithFixedFile(filePath, entries); err != nil {
			return entries, err
		}
	}

	return entries, nil
}

func (wal *WAL) replaceWithFixedFile(targetFilePath string, entries []*proto.LogEntry) error {
	tempFilePath := fmt.Sprintf("%s.tmp", targetFilePath)
	tempFile, err := os.OpenFile(tempFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		marshaledEntry := proto.MustMarshal(entry)
		size := int64(len(marshaledEntry))
		if err := binary.Write(tempFile, binary.LittleEndian, size); err != nil {
			_ = tempFile.Close()
			return err
		}
		if _, err := tempFile.Write(marshaledEntry); err != nil {
			_ = tempFile.Close()
			return err
		}
	}

	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return err
	}

	if err := tempFile.Close(); err != nil {
		return err
	}

	if wal.currentSegment != nil {
		_ = wal.currentSegment.Close()
	}

	if err := os.Rename(tempFilePath, targetFilePath); err != nil {
		return err
	}

	reopened, err := os.OpenFile(targetFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	wal.currentSegment = reopened
	wal.bufWriter = bufio.NewWriter(reopened)

	return nil
}
