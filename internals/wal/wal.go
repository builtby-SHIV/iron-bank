package wal

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"iron-bank/internals/kvstore"
	"log"
	"os"
	"strings"
	"sync"
)

type WAL struct {
	logger *log.Logger
	file *os.File
	mu sync.Mutex
}

var( 
	ErrOpenAndCreateWAL = errors.New("cannot open or create log file")
	FileNotFound = errors.New("log file not found")
)

func StartLogger() (*WAL, error) {
	f, err := os.OpenFile("WAL.log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, ErrOpenAndCreateWAL
	}
	logger := log.New(f, "", log.Ltime)
	return &WAL{ logger: logger, file: f }, nil
}

func (w *WAL) WriteToWal(k, v string) error{
	w.mu.Lock()
	defer w.mu.Unlock()

	h := sha256.Sum256([]byte(k + v))
	hash := hex.EncodeToString(h[:])
	w.logger.Println(k, v, hash)
	if err := w.file.Sync(); err != nil {
		return err
	}
	return nil
}

func (w *WAL) ReplayLog(kv *kvstore.KVStore) error {
	file, err := os.Open("WAL.log")
	if err != nil {
		return FileNotFound
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		res := strings.Split(scanner.Text(), " ")
		if hash := sha256.Sum256([]byte(res[1] + res[2])); hex.EncodeToString(hash[:]) != strings.Join(res[3:], " ") {
			return nil
		}
		kv.Set(res[1], res[2])
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return nil
}