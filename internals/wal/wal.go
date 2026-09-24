package wal

import (
	"errors"
	"log"
	"os"
	"sync"
)

type WAL struct {
	logger *log.Logger
	mu sync.RWMutex
}

var ErrOpenAndCreateWAL = errors.New("cannot open or create log file")

func StartLogger() (*WAL, error) {
	f, err := os.OpenFile("WAL.log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, ErrOpenAndCreateWAL
	}
	logger := log.New(f, "", log.Ltime)
	return &WAL{ logger: logger }, nil
}

func (w *WAL) WriteToWal(k, v string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.logger.Println(k, v)
}