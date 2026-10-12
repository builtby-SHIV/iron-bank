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

## Getting Started

### Prerequisites
- [Go 1.26+](https://go.dev/dl/)
- [Protocol Buffers compiler](https://protobuf.dev/) (only if modifying `.proto` files)

### Run
```bash
go run .
```
The server starts on `http://localhost:8080`.

### Build
```bash
go build -o iron-bank .
./iron-bank
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
