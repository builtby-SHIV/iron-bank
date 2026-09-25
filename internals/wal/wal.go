package wal

import (
	"bufio"
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
	mu sync.RWMutex
}

var ErrOpenAndCreateWAL = errors.New("cannot open or create log file")
var FileNotFound = errors.New("log file not found")

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

	w.logger.Println(k, v)
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
		kv.Set(res[1], res[2])
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return nil
}