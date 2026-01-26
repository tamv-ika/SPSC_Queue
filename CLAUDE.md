# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SPSC_Queue is a high-performance, lock-free Single Producer Single Consumer queue library for ultra-low latency inter-thread and shared-memory IPC communication in C++. Message latency is within 50-100ns for 10-200B messages. Zero-copy design: messages are allocated in queue memory, written by producer, and read directly by consumer.

**Author:** Meng Rao | **License:** MIT | **Language:** C++11/C++20

## Build Commands

### Building Tests/Benchmarks

The core library is header-only. For tests in `/test`:

```bash
cd test
g++ -std=c++20 -O3 -march=native -pthread -lrt <source>.cc -o <binary>
```

Example commands:
```bash
# Comprehensive benchmark
g++ -std=c++20 -O3 -march=native -o comp_bench comprehensive_bench.cc -pthread

# Latency analysis with percentiles
g++ -std=c++20 -O3 -march=native -o latency_bench latency_analysis_v2.cc -pthread

# Basic multi-thread test
g++ -std=c++20 -O3 -march=native -o mt_test multhread_q.cc -pthread
```

### Subdirectory Builds (CMake)

```bash
# rigtorp_spsc/
cd rigtorp_spsc && mkdir build && cd build && cmake .. && make

# moodycamel_rwq/
cd moodycamel_rwq && mkdir build && cd build && cmake .. && make install

# disruptor_cpp/
cd disruptor_cpp && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=release -DDISRUPTOR_BUILD_TESTS=true && make
```

## Architecture

### Core Queue Implementations (Root Level Headers)

| Header | Description | Use Case |
|--------|-------------|----------|
| `SPSCQueue.h` | Atomic, crash-safe | Shared memory IPC, crash safety required |
| `SPSCQueueOPT.h` | Optimized, non-atomic | Single-process, highest speed (62.6ns p50) |
| `SPSCVarQueue.h` | Variable-sized messages, atomic | Variable-length IPC messages |
| `SPSCVarQueueOPT.h` | Variable-sized, non-atomic | Variable-sized, single process |
| `SPSCDisruptor.h` | LMAX Disruptor-style | Batch processing, event streaming |
| `SPSCQueueAsymmetric.h` | Consumer batching | Best p50/p90/p99 with batching |
| `SPSCQueueBatch.h` | Batch operations | Batch message processing |

### API Pattern (alloc/push/front/pop)

```cpp
#include "SPSCQueueOPT.h"
SPSCQueueOPT<int, 4096> queue;  // 4K slots (must be power of 2)

// Producer
int* slot = queue.alloc();  // Get slot
if (slot) { *slot = 42; queue.push(); }  // Write and publish

// Consumer
int* slot = queue.front();  // Get next item
if (slot) { int val = *slot; queue.pop(); }  // Read and consume
```

### Design Principles

- **False sharing prevention:** Cache-line alignment (128 bytes for indices, 64 bytes per slot)
- **Two-sided caching:** Producer caches read_idx, consumer caches write_idx to reduce cache coherency traffic
- **Power-of-2 sizing:** Enables fast modulo via bitmasking
- **Memory ordering:** `memory_order_relaxed` where safe, `acquire/release` for synchronization

## Key Files

### Test Utilities (`/test`)
- `cpupin.h` - CPU affinity pinning
- `rdtsc.h` - RDTSC performance counter
- `shmmap.h` - Shared memory mapping
- `multhread_q.cc` - Reference benchmark (correct measurement methodology)
- `comprehensive_bench.cc` - Multi-queue comparison

### Disruptor Components
- `WaitStrategy.h` - BusySpinWait, YieldingWait, SleepingWait, BlockingWait
- `ConsumerSequence.h` - Consumer sequence tracking
- `MultiConsumerDisruptor.h` - Multi-consumer variant

## Performance Notes

**Critical:** See `MEASUREMENT_METHODOLOGY.md` - correct measurement uses throttling (1000 cycles between messages) to measure real queue latency, not alloc() overhead.

### Correct Results (with throttling)
- **SPSCQueueOPT:** p50=62.6ns, p90=71.3ns (lowest latency)
- **SPSCQueue:** p50=68.7ns, p99=95.7ns (best p99, crash-safe)
- **SPSCQueueAsymmetric:** p50=58.3ns with batch=16 (best overall p50)

### Quick Optimizations
1. Use `SPSCQueueOPT` instead of `SPSCQueue` (+15% throughput)
2. Queue size 4K-16K slots (-85% p99 latency)
3. Run `sudo ./optimize_system.sh` for system-level tuning
4. Pin producer/consumer to separate CPUs: `cpupin(6)`, `cpupin(7)`

## Subdirectories

- `disruptor_cpp/` - LMAX Disruptor C++ port (full implementation, Google Test)
- `rigtorp_spsc/` - Erik Rigtorp's SPSC queue (comparison implementation)
- `moodycamel_rwq/` - Cameron Desrochers' ReaderWriterQueue (comparison)

Each is an independent git repository.
