package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"iron-bank/internals/kvstore"
	"os"
	"sync"
)

type WAL struct {
	buf [] byte
	file *os.File
	mu sync.Mutex
	offset int64
	lastseqnum int
	maxfilesize int
}

var ( 
	ErrOpenAndCreateWAL = errors.New("cannot open or create log file")
	FileNotFound = errors.New("log file not found")
)

const (
	OpSet byte = 1
	OpDel byte = 2
	Header = 21
)

func StartLogger() (*WAL, error) {
	f, err := os.OpenFile("WAL.log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, ErrOpenAndCreateWAL
	}
	return &WAL{ file: f, buf: make([]byte, 1024) }, nil
}

func (w *WAL) WriteToWal(op byte, kv *kvstore.KVStore, key, val []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	keyLen := len(key)
	valLen := len(val)

	totalSize := Header + int(keyLen) + int(valLen)
	if totalSize > cap(w.buf) {
		w.buf = make([]byte, totalSize)
	} else {
		w.buf = w.buf[:totalSize]
	}

	w.buf[4] = op
	binary.BigEndian.PutUint64(w.buf[5:13], uint64(keyLen))
	binary.BigEndian.PutUint64(w.buf[13:21], uint64(valLen))
	copy(w.buf[21:21+keyLen], key)
	copy(w.buf[21+keyLen:], val)

	checksum := crc32.ChecksumIEEE(w.buf[4:])
	binary.BigEndian.PutUint32(w.buf[0:4], checksum)

	if _, err := w.file.Write(w.buf); err != nil {
		return err
	}
	w.file.Sync()
	return nil
}

func (w *WAL) ReplayLog(kv *kvstore.KVStore) error {
	st, err := w.file.Stat()
	if err != nil {
		return err
	}
	size := st.Size()
	var off int64
	for off < size {
		if size - off < Header {
			break
		}
		buf := make([]byte, 21)
		_, err := w.file.ReadAt(buf, off)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			fmt.Println("no error is from here")
			return err
		}

		checksum := binary.BigEndian.Uint32(buf[0:4])

		crc := crc32.NewIEEE()
		crc.Write(buf[4:21])

		op := buf[4]
		key_len := binary.BigEndian.Uint64(buf[5:13])
		val_len := binary.BigEndian.Uint64(buf[13:21])
		buf = make([]byte, key_len + val_len)
		if _, err := w.file.ReadAt(buf, off + Header); err != nil {
			return err
		}
		crc.Write(buf)
		key, val := buf[:key_len], buf[key_len:]

		recEnd := off + Header + int64(key_len + val_len)
		if checksum != crc.Sum32() {
			if recEnd < size {
				return fmt.Errorf("corrupt record at offset %d, %d bytes follow", off, size-recEnd)
			}
			break
		} else {
			if op == 1 {
				kv.Set(string(key), string(val))
			} else {
				kv.Del(string(key))
			}
		}
		off = recEnd
	}

	if off < size {
		if err = w.file.Truncate(off); err != nil { return err }
		if err = w.file.Sync(); err != nil { return err }
	}

	w.offset = off

	return nil
}
