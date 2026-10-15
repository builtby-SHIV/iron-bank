# 🏦 Iron Bank

<p align="center">
  <img src="mascot.png" alt="Iron Bank Mascot" width="250">
</p>

A **Log-Structured Merge-Tree (LSM) based key-value storage engine** written in Go — built from scratch as a learning project, evolving from a simple in-memory hashmap to a full-fledged LSM storage engine with write-ahead logging, SSTables, bloom filters, and multi-level compaction.

## Why I Built This

I first read about LSM trees in [*Designing Data-Intensive Applications* (DDIA)](https://dataintensive.net/) by Martin Kleppmann, and the explanation just clicked. The whole architecture how writes go to an in-memory table, get flushed to sorted on-disk files, and are periodically merged together made complete sense to me. It felt elegant.

That understanding led me to build Iron Bank: a project where I could take the concepts from the book and turn them into working code. I started with the simplest possible thing an in-memory `map[string]string` behind an HTTP server and then progressively added each layer of the LSM architecture, one commit at a time. The goal was never to build a production database, but to deeply understand how storage engines actually work under the hood.

## How It Evolved

The git history tells the story of a bottom-up build:

1. **Simple KV Store** — Started as a basic in-memory hashmap (`map[string]string`) with `GET`, `PUT`, and `DELETE` endpoints over HTTP.
2. **Write-Ahead Log (WAL)** — Added durability with a binary WAL: every mutation is logged to disk before being applied, so data survives crashes.
3. **Memtable** — Replaced the raw hashmap with a skip list–backed memtable for ordered iteration and efficient range scans.
4. **SSTables** — When the memtable fills up, it gets flushed to disk as a Sorted String Table with a dense index and bloom filter.
5. **LSM Tree** — Tied everything together: the memtable, flushing queue, multi-level SSTable management, background compaction, and WAL recovery.

## Architecture

```
                    ┌─────────────┐
    PUT/GET/DEL ──► │  HTTP API   │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐     ┌──────────────┐
                    │   LSM Tree  │────►│  WAL (disk)  │
                    └──────┬──────┘     └──────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────────┐
        │ Memtable │ │ Flushing │ │   SSTables   │
        │(skiplist)│ │  Queue   │ │ (L0 → L6)    │
        └──────────┘ └──────────┘ └──────────────┘
```

**Read path:** Memtable → Flushing Queue (newest first) → SSTables (L0 → L6, newest first). Bloom filters are checked before scanning an SSTable.

**Write path:** WAL append → Memtable insert → (if full) enqueue for background flush → SSTable on disk.

## Features

### LSM Tree Storage Engine
- **Multi-level architecture** — 7 levels (L0–L6) with configurable SSTable limits per level (4, 8, 16, 32, 64, 128, 256).
- **Background compaction** — A dedicated goroutine monitors levels and merges SSTables when a level exceeds its capacity, pushing merged results to the next level.
- **Background memtable flushing** — Memtables are flushed to L0 SSTables asynchronously via a flushing queue, so writes aren't blocked by disk I/O.
- **Graceful shutdown** — The `Close()` method flushes the active memtable, drains all background workers, and syncs the WAL before exiting.

### Write-Ahead Log (WAL)
- **Crash recovery** — On startup, WAL entries are replayed to rebuild the memtable, ensuring no acknowledged write is lost.
- **Segmented log files** — The WAL is split into fixed-size segments with automatic rotation, preventing unbounded file growth.
- **Buffered writes with periodic fsync** — Writes go to a `bufio.Writer` and are fsynced to disk every 200ms, balancing throughput and durability.
- **CRC32 checksums** — Every WAL entry includes a CRC32 checksum (IEEE) to detect corruption.
- **Log repair** — If a corrupted entry is detected, the WAL can repair itself by truncating the log at the last valid entry and rewriting a clean file.

### SSTables (Sorted String Tables)
- **Dense index** — Every key in the SSTable has a corresponding offset in the index, enabling O(log n) binary search lookups.
- **Bloom filters** — Each SSTable embeds a bloom filter (murmur3 hashing, 3 hash functions) to skip disk reads for keys that definitely don't exist.
- **Protobuf serialization** — All on-disk structures (entries, index, bloom filter, WAL entries) are serialized with Protocol Buffers for compact, schema-evolvable encoding.
- **Iterator-based merging** — SSTable compaction uses iterators and a min-heap for efficient k-way merge, deduplicating by timestamp.

### Memtable
- **Skip list–backed** — Uses a skip list (`github.com/huandu/skiplist`) for O(log n) inserts, lookups, and in-order iteration.
- **Range scans** — Supports scanning a range of keys `[start, end]` across the memtable, flushing queue, and all SSTables with merged, deduplicated results.
- **Tombstone deletes** — Deletes don't remove entries; they write a `DELETE` tombstone that shadows the key until compaction removes it.

### HTTP API
Iron Bank exposes a simple REST-like API:

| Method   | Endpoint | Description           |
|----------|----------|-----------------------|
| `POST`   | `/put`   | Set a key-value pair  |
| `GET`    | `/get`   | Retrieve a value by key |
| `DELETE` | `/del`   | Delete a key          |

**Examples:**
```bash
# Set a key
curl -X POST http://localhost:8080/put \
  -H "Content-Type: application/json" \
  -d '{"key": "name", "val": "tyrion"}'

# Get a key
curl http://localhost:8080/get \
  -H "Content-Type: application/json" \
  -d '{"key": "name"}'

# Delete a key
curl -X DELETE http://localhost:8080/del \
  -H "Content-Type: application/json" \
  -d '{"key": "name"}'
```

## Benchmarks & Amplification Analysis

Benchmarks were conducted on an **AMD Ryzen 7 7435HS (16 threads)** running **Go 1.26.4** on Windows x86_64 to evaluate **Write Amplification (WAF)**, **Read Amplification (RAF)**, and raw throughput under strict durability guarantees.

### 1. Write Amplification Factor (WAF) — Synchronous `fsync` on Every Write

Iron Bank enforces high consistency: every write flushes the userspace buffer and issues an operating system `fsync` (`file.Sync()`) directly to storage media before returning success. SSTable creations during memtable flushes and compaction also execute `file.Sync()` prior to closing.

$$\text{WAF} = \frac{\text{Total Bytes Written to Storage (WAL + SSTables)}}{\text{Total Logical Bytes Written by User}}$$

| Workload | Operations | Value Size | Logical User Data | Storage Written | WAF | Level Distribution on Disk | Durable Throughput (`fsync` / write) |
|---|---|---|---|---|---|---|---|
| **Sequential Writes** | 5,000 keys | 128 B | 693.36 KB | 12.45 MB | **17.96x** | **L0: 2, L1: 8, L2: 1** | **~990 durable ops/sec** (~1.01 ms / write) |
| **Random Writes** | 5,000 keys | 128 B | 708.01 KB | 13.48 MB | **19.03x** | **L0: 3, L1: 8, L2: 1** | **~974 durable ops/sec** (~1.03 ms / write) |
| **Heavy Overwrites** (500 distinct keys) | 5,000 keys | 128 B | 664.06 KB | 11.01 MB | **16.57x** | **L0: 2, L1: 7, L2: 1** | **~976 durable ops/sec** (~1.03 ms / write) |

> **Analysis & Verification of Multi-Level Compaction:**
> - **Proof of SSTable Writes:** Memtables flush to SSTable files when reaching `maxMemtableSize` (8 KB). 5,000 writes trigger repeated flushes into the data directory with disk-persisted `sstable_0_*` files.
> - **Proof of Merging & Compaction Across Levels:** 
>   - SSTables are generated originally *only* at Level 0 (`sstable_0_*`).
>   - When $L_0$ exceeds `maxSSTables[0] = 4`, the background compaction worker merges them and writes into Level 1 (`sstable_1_*`), deleting the old $L_0$ files.
>   - When $L_1$ exceeds `maxSSTables[1] = 8`, compaction cascades: $L_1$ tables are merged and promoted into Level 2 (`sstable_2_*`).
>   - The resulting disk state (**L0: 2, L1: 8, L2: 1**) proves that data was physically written into SSTables, merged across levels, and compacted up to Level 2.
> - **Durable Hardware Throughput:** Synchronous physical `fsync` on every write commits directly to disk platters/NAND flash, achieving ~970 durable transactions/sec on consumer NVMe/SSD storage without buffering data in volatile memory.
> - **WAL Overhead:** Every mutation in the WAL accounts for ~1.15x logical user payload due to Little-Endian length headers, sequence numbers, protobuf tags, and CRC32 checksums.
> - **Compaction Overhead:** Compaction rewrites SSTables across multiple levels ($O(\text{levels})$). Heavy overwrites exhibit lower WAF (16.57x vs 19.03x) because compaction deduplicates superseded versions of identical keys.

---

### 2. Read Amplification Factor (RAF) & Why Range RAF > Point RAF

$$\text{RAF} = \frac{\text{Bytes Read from Storage Media}}{\text{Logical Payload Bytes Retrieved by User}}$$

| Query Type | Pattern | Disk Probes / Query | Disk Bytes Read | User Bytes Read | RAF | Throughput |
|---|---|---|---|---|---|---|
| **Point Read (Hit)** | Random Key in SSTable | **1.00** | 154.30 KB | 132.81 KB | **1.16x** | ~123,800 reads/sec |
| **Point Read (Cold Miss)** | Non-Existent Key | **0.00** | **0 B** (100% Bloom Filtered) | 0 B | **0.00x** | ~922,000 reads/sec |
| **Range Scan (10 keys)** | Sequential Range | Multi-SSTable scan | 89.44 KB | 57.35 KB | **1.56x** | ~4,400 scans/sec |
| **Range Scan (50 keys)** | Sequential Range | Multi-SSTable scan | 409.35 KB | 286.28 KB | **1.43x** | ~1,700 scans/sec |
| **Range Scan (100 keys)** | Sequential Range | Multi-SSTable scan | 809.25 KB | 572.56 KB | **1.41x** | ~850 scans/sec |

---
### 3. Throughput & Microbenchmarks

Microbenchmarks measured via `go test -bench . -benchmem`:

| Benchmark | Iterations | Latency (ns/op) | Memory (B/op) | Allocs/op | Throughput (ops/sec) |
|---|---|---|---|---|---|
| `BenchmarkPutSequential` | 46,144 | 27,941 ns/op | 14,176 B/op | 35 | ~35,789 ops/sec (async) |
| `BenchmarkPutRandom` | 45,146 | 26,132 ns/op | 14,287 B/op | 35 | ~38,267 ops/sec (async) |
| `BenchmarkGetHit` | 187,068 | 6,324 ns/op | 958 B/op | 15 | ~158,127 reads/sec |
| `BenchmarkGetMissBloomFilter` | 1,735,495 | 800.9 ns/op | 186 B/op | 8 | ~1,248,595 reads/sec |
| `BenchmarkRangeScan` (50 keys) | 3,992 | 285,505 ns/op | 45,407 B/op | 405 | ~3,502 scans/sec |

### Running the Benchmarks

```bash
# Run write and read amplification tests with full metric reports:
go test -v -run "TestBenchmark"

# Run microbenchmarks for throughput and allocation profiles:
go test -bench . -benchmem -run "^$"
```

## Getting Started

### Prerequisites
- [Go 1.26+](https://go.dev/dl/)
- [Protocol Buffers compiler](https://protobuf.dev/) (only if modifying `.proto` files)

### Run
```bash
go run .
```
The server starts on `http://localhost:8001` with data stored in `./data/8001`.

### Running Multiple Processes
You can run multiple instances concurrently on different ports without data collisions. Each process automatically provisions its own storage directory based on its port number (e.g. `./data/<port>` and `./data/<port>_wal`):

```bash
# Node 1 (port 8001 -> data stored in ./data/8001)
go run . -port 8001
# Or using -addr
go run . -addr :8001

# Node 2 (port 8002 -> data stored in ./data/8002)
go run . -port 8002

# Node 3 (port 8003 -> data stored in ./data/8003)
go run . -port 8003
```

Flags available:
- `-port <number>`: HTTP listen port (e.g. `8001`, `8002`).
- `-addr <address>`: HTTP listen address (e.g. `:8001`, `127.0.0.1:8001`). Port is automatically extracted.
- `-data <path>`: Optional override for the storage directory (defaults to `./data/<port>`).
- `-id <identifier>`: Optional node identifier (defaults to `node-<port>`).

### Build
```bash
go build -o iron-bank .
./iron-bank -port 8001
```

## Project Structure

```
iron-bank/
├── main.go              # HTTP server and API handlers
├── kvstore.go           # Original in-memory KV store (map-backed)
├── lsm.go              # LSM tree: memtable management, compaction, flush, recovery
├── memtable.go          # Skip list–backed memtable with range scan support
├── sstable.go           # SSTable read/write, serialization, and iteration
├── sstable_utils.go     # Binary search on index, protobuf helpers, I/O utilities
├── bloomfilter.go       # Bloom filter (murmur3, 3 hash functions)
├── wal.go               # Write-ahead log: segments, rotation, repair, fsync
├── merge_utils.go       # Min-heap based k-way merge for compaction and range scans
├── serialization.proto  # Protobuf definitions for all on-disk structures
├── serialization.pb.go  # Generated protobuf Go code
├── go.mod               # Go module and dependencies
└── go.sum               # Dependency checksums
```

## Dependencies

| Package | Purpose |
|---------|---------|
| [`huandu/skiplist`](https://github.com/huandu/skiplist) | Skip list for the memtable |
| [`spaolacci/murmur3`](https://github.com/spaolacci/murmur3) | MurmurHash3 for bloom filters |
| [`google.golang.org/protobuf`](https://pkg.go.dev/google.golang.org/protobuf) | Protocol Buffers serialization |

## References

- [*Designing Data-Intensive Applications*](https://dataintensive.net/) by Martin Kleppmann — the book that started it all
- [LSM Tree - Wikipedia](https://en.wikipedia.org/wiki/Log-structured_merge-tree)

## License

This project is for learning purposes.
