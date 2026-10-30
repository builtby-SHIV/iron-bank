package storage

import (
	"errors"
	"fmt"
	"iron-bank/proto"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBloomFilter(t *testing.T) {
	bf := NewBloomFilter(1000)
	bf.Add([]byte("hello"))
	bf.Add([]byte("world"))

	if !bf.Test([]byte("hello")) {
		t.Fatal("expected hello to be in bloom filter")
	}
	if !bf.Test([]byte("world")) {
		t.Fatal("expected world to be in bloom filter")
	}
	// Test nil or empty safety
	var nilBf *BloomFilter
	if !nilBf.Test([]byte("anything")) {
		t.Fatal("nil bloomfilter should return true safely")
	}
}

func TestKVStoreConcurrent(t *testing.T) {
	kv := NewKVStore()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(id int) {
			defer wg.Done()
			kv.Set(fmt.Sprintf("key-%d", id), fmt.Sprintf("val-%d", id))
		}(i)
		go func(id int) {
			defer wg.Done()
			_, _ = kv.Get(fmt.Sprintf("key-%d", id))
		}(i)
	}
	wg.Wait()
}

func TestMemtable(t *testing.T) {
	m := NewMemTable()
	m.Put("k1", []byte("v1"))
	m.Put("k2", []byte("v2"))
	m.Put("k3", []byte("v3"))

	if m.SizeInbytes() != int64(len("k1")+len("v1")+len("k2")+len("v2")+len("k3")+len("v3")) {
		t.Fatalf("unexpected size: %d", m.SizeInbytes())
	}

	entry := m.Get("k2")
	if entry == nil || string(entry.Value) != "v2" {
		t.Fatalf("expected v2, got %v", entry)
	}

	// Update existing key
	m.Put("k2", []byte("updated_v2"))
	entry = m.Get("k2")
	if entry == nil || string(entry.Value) != "updated_v2" {
		t.Fatalf("expected updated_v2, got %v", entry)
	}

	// Delete key
	m.Del("k2")
	entry = m.Get("k2")
	if entry == nil || entry.Command != proto.Command_DELETE {
		t.Fatalf("expected delete tombstone, got %v", entry)
	}

	// Range scan
	m.Put("a", []byte("1"))
	m.Put("b", []byte("2"))
	m.Put("c", []byte("3"))
	m.Put("d", []byte("4"))

	results := m.RangeScan("b", "c")
	if len(results) != 2 || results[0].Key != "b" || results[1].Key != "c" {
		t.Fatalf("range scan failed: %+v", results)
	}
}

func TestSSTableBasic(t *testing.T) {
	dir := t.TempDir()
	sstPath := filepath.Join(dir, "sstable_0_1")

	entries := []*proto.LSMEntry{
		{Key: "apple", Value: []byte("red"), Command: proto.Command_PUT, Timestamp: 100},
		{Key: "banana", Value: []byte("yellow"), Command: proto.Command_PUT, Timestamp: 101},
		{Key: "cherry", Value: []byte("darkred"), Command: proto.Command_PUT, Timestamp: 102},
	}

	sst, err := SerializeToSSTable(entries, sstPath)
	if err != nil {
		t.Fatalf("serialize failed: %v", err)
	}
	defer sst.Close()

	// Test Get
	e, err := sst.Get("banana")
	if err != nil || e == nil || string(e.Value) != "yellow" {
		t.Fatalf("expected yellow, got %v, err %v", e, err)
	}

	e, err = sst.Get("nonexistent")
	if err != nil || e != nil {
		t.Fatalf("expected nil for nonexistent, got %v, err %v", e, err)
	}

	// Test RangeScan
	scan, err := sst.RangeScan("apple", "banana")
	if err != nil || len(scan) != 2 {
		t.Fatalf("expected 2 entries, got %d, err %v", len(scan), err)
	}

	// Test RangeScan past end
	scan, err = sst.RangeScan("banana", "donut")
	if err != nil || len(scan) != 2 { // banana, cherry
		t.Fatalf("expected 2 entries for banana..donut, got %d, err %v", len(scan), err)
	}

	// Test Iterator
	it := sst.Front()
	if it == nil || it.value.Key != "apple" {
		t.Fatalf("iterator front failed: %v", it)
	}
	it = it.Next()
	if it == nil || it.value.Key != "banana" {
		t.Fatalf("iterator next 1 failed: %v", it)
	}
	it = it.Next()
	if it == nil || it.value.Key != "cherry" {
		t.Fatalf("iterator next 2 failed: %v", it)
	}
	it = it.Next()
	if it != nil {
		t.Fatalf("expected EOF nil iterator, got %v", it)
	}

	// Test concurrent Get
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := sst.Get("banana")
			if err != nil || res == nil || string(res.Value) != "yellow" {
				t.Errorf("concurrent get failed: %v, %v", res, err)
			}
		}()
	}
	wg.Wait()
}

func TestWAL(t *testing.T) {
	dir := t.TempDir()

	wal, err := OpenWAL(dir, true, 500, 5)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	// Write entries triggering rotation
	for i := 0; i < 20; i++ {
		err := wal.WriteEntry(proto.Command_PUT, fmt.Sprintf("k-%03d", i), []byte(fmt.Sprintf("v-%03d", i)), 0, false)
		if err != nil {
			t.Fatalf("failed to write wal entry: %v", err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("failed to close wal: %v", err)
	}

	// Re-open and verify entries
	wal2, err := OpenWAL(dir, true, 500, 5)
	if err != nil {
		t.Fatalf("failed to reopen wal: %v", err)
	}
	defer wal2.Close()

	entries, err := wal2.ReadAll()
	if err != nil {
		t.Fatalf("failed to read all wal entries: %v", err)
	}
	if len(entries) == 0 {
		t.Fatalf("expected entries from wal, got 0")
	}

	// Test Repair on corrupted file
	corruptPath := wal2.currentSegment.Name()
	_ = wal2.Close()

	// Append corrupt bytes
	f, err := os.OpenFile(corruptPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open file for corrupting: %v", err)
	}
	_, _ = f.Write([]byte{0xFF, 0xFE, 0x01, 0x02, 0x03, 0x04})
	_ = f.Close()

	wal3, err := OpenWAL(dir, true, 500, 5)
	if err != nil {
		t.Fatalf("failed to open wal for repair: %v", err)
	}
	defer wal3.Close()

	repaired, err := wal3.Repair()
	if err != nil {
		t.Fatalf("repair failed: %v", err)
	}
	if len(repaired) == 0 {
		t.Fatalf("expected repaired entries, got 0")
	}
}

func TestLSMTreeBasicAndPersistence(t *testing.T) {
	dir := t.TempDir()

	lsm, err := Open(dir, 100, true)
	if err != nil {
		t.Fatalf("failed to open lsm: %v", err)
	}

	// Write enough entries to cause multiple memtable flushes
	for i := 0; i < 50; i++ {
		err := lsm.PUT(fmt.Sprintf("user_%03d", i), []byte(fmt.Sprintf("alice_%03d", i)))
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}
	}

	// Give a little time for background flush
	time.Sleep(100 * time.Millisecond)

	// Verify reads
	for i := 0; i < 50; i++ {
		val, err := lsm.GET(fmt.Sprintf("user_%03d", i))
		if err != nil {
			t.Fatalf("GET user_%03d failed: %v", i, err)
		}
		expected := fmt.Sprintf("alice_%03d", i)
		if val != expected {
			t.Fatalf("expected %s, got %s", expected, val)
		}
	}

	// Delete some keys
	for i := 0; i < 10; i++ {
		err := lsm.DELETE(fmt.Sprintf("user_%03d", i))
		if err != nil {
			t.Fatalf("DELETE user_%03d failed: %v", i, err)
		}
	}

	// Verify deleted keys return NotFoundError
	for i := 0; i < 10; i++ {
		_, err := lsm.GET(fmt.Sprintf("user_%03d", i))
		if err == nil {
			t.Fatalf("expected NotFoundError for user_%03d, got nil", i)
		}
		var nf *NotFoundError
		if !strings.Contains(err.Error(), "not found") && !errors.As(err, &nf) {
			t.Fatalf("expected NotFoundError, got %v", err)
		}
	}

	// Range scan
	pairs, err := lsm.RangeScan("user_020", "user_025")
	if err != nil {
		t.Fatalf("RangeScan failed: %v", err)
	}
	if len(pairs) != 6 {
		t.Fatalf("expected 6 pairs in range scan, got %d", len(pairs))
	}
	if pairs[0].Key != "user_020" || pairs[5].Key != "user_025" {
		t.Fatalf("unexpected range scan bounds: %+v", pairs)
	}

	// Close cleanly
	if err := lsm.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen with WAL recovery and verify data survived!
	lsm2, err := Open(dir, 100, true)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer lsm2.Close()

	for i := 10; i < 50; i++ {
		val, err := lsm2.GET(fmt.Sprintf("user_%03d", i))
		if err != nil {
			t.Fatalf("after reopen, GET user_%03d failed: %v", i, err)
		}
		expected := fmt.Sprintf("alice_%03d", i)
		if val != expected {
			t.Fatalf("after reopen, expected %s, got %s", expected, val)
		}
	}
}

func TestLSMTreeCompactionAndShutdown(t *testing.T) {
	dir := t.TempDir()

	// Small memtable size so flushes happen frequently
	lsm, err := Open(dir, 50, true)
	if err != nil {
		t.Fatalf("failed to open lsm: %v", err)
	}

	// Write 200 keys to trigger multiple flushes and compaction
	for i := 0; i < 200; i++ {
		err := lsm.PUT(fmt.Sprintf("key_%04d", i), []byte(fmt.Sprintf("val_%04d", i)))
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}
	}

	// Close must not deadlock!
	done := make(chan struct{})
	go func() {
		_ = lsm.Close()
		close(done)
	}()

	select {
	case <-done:
		// Success!
	case <-time.After(5 * time.Second):
		t.Fatal("LSMTree.Close() DEADLOCKED!")
	}
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	dir := t.TempDir()
	lsm, err := Open(dir, 100, true)
	if err != nil {
		t.Fatalf("failed to open lsm: %v", err)
	}
	defer lsm.Close()

	var wg sync.WaitGroup
	numWriters := 8
	numReaders := 8
	opsPerWorker := 50

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("k_%d_%d", workerID, i)
				val := fmt.Sprintf("v_%d_%d", workerID, i)
				_ = lsm.PUT(key, []byte(val))
			}
		}(w)
	}

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				targetWriter := rand.Intn(numWriters)
				key := fmt.Sprintf("k_%d_%d", targetWriter, rand.Intn(opsPerWorker))
				_, _ = lsm.GET(key)
			}
		}(r)
	}

	wg.Wait()
}

func TestTombstonePreservedDuringCompaction(t *testing.T) {
	dir := t.TempDir()

	lsm, err := Open(dir, 50, true)
	if err != nil {
		t.Fatalf("failed to open lsm: %v", err)
	}

	// 1. Put key "target" with "val_old"
	if err := lsm.PUT("target", []byte("val_old")); err != nil {
		t.Fatal(err)
	}

	// 2. Put enough keys to flush "target" into an SSTable
	for i := 0; i < 20; i++ {
		_ = lsm.PUT(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i)))
	}

	// Wait for flush to finish
	time.Sleep(100 * time.Millisecond)

	// 3. Delete "target"
	if err := lsm.DELETE("target"); err != nil {
		t.Fatal(err)
	}

	// 4. Put more keys to trigger another flush and compaction
	for i := 20; i < 100; i++ {
		_ = lsm.PUT(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i)))
	}

	time.Sleep(200 * time.Millisecond)

	// 5. Ensure "target" is NOT found (did not resurrect)
	val, err := lsm.GET("target")
	if err == nil {
		t.Fatalf("expected target to be deleted, but got value: %s", val)
	}

	_ = lsm.Close()
}
