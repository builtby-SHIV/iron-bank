package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"hash/crc32"
	"iron-bank/internals/kvstore"
	"os"
	"sync"
)

type WAL struct {
	buf [] byte
	file *os.File
	mu sync.Mutex
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

func (w *WAL) WriteToWal(key, val []byte, op byte) error {
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
	file, err := os.Open("WAL.log")
	if err != nil {
		return FileNotFound
	}
	defer file.Close()

	for  {
		buf := make([]byte, 21)
		_, err := io.ReadFull(file, buf)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		checksum := binary.BigEndian.Uint32(buf[0:4])

		crc := crc32.NewIEEE()
		crc.Write(buf[4:21])

		op := buf[4]
		key_len := binary.BigEndian.Uint64(buf[5:13])
		val_len :=binary.BigEndian.Uint64(buf[13:21])
		buf = make([]byte, key_len)
		if _, err := io.ReadFull(file, buf); err != nil {
			return err
		}
		key := buf
		crc.Write(key)
		buf = make([]byte, val_len)
		if _, err := io.ReadFull(file, buf); err != nil {
			return err
		}
		val := buf
		crc.Write(val)

		if checksum != crc.Sum32() {
			//apply corrupt WAL
		} else {
			if op == 1 {
				kv.Set(string(key), string(val))
			} else {
				kv.Del(string(key))
			}
		}
	}
}
