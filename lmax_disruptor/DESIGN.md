# High-Performance LMAX Disruptor for C++

## Goals

1. **Sub-100ns latency** (match your SPSCQueue performance)
2. **Zero virtual calls** in hot path
3. **Template-based** design for compile-time polymorphism
4. **LMAX-style API** (sequences, batching, dependency chains)
5. **Header-only** for easy integration

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         RingBuffer<T, SIZE>                      │
│  ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐             │
│  │  0  │  1  │  2  │  3  │  4  │  5  │  6  │  7  │  ...        │
│  └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘             │
│                            ▲                                     │
│                     seq & MASK                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
   ┌─────────┐          ┌─────────┐          ┌─────────┐
   │ cursor  │          │ gating  │          │consumer │
   │  (pub)  │          │sequences│          │sequences│
   └─────────┘          └─────────┘          └─────────┘
```

## Key Components

### 1. Sequence (sequence.h)
Cache-line padded atomic counter.

```cpp
class Sequence {
    alignas(64) std::atomic<int64_t> value_{-1};
    char padding_[64 - sizeof(std::atomic<int64_t>)];
};
```

### 2. WaitStrategy (wait_strategy.h)
Template-based, no virtual calls.

```cpp
struct BusySpinWait {
    void wait() { _mm_pause(); }
    void signal() {}
};

struct YieldingWait {
    void wait() {
        if (++counter_ > 100) std::this_thread::yield();
        else _mm_pause();
    }
};
```

### 3. SingleProducerSequencer (single_producer_sequencer.h)
Claims sequences for single producer.

```cpp
template<typename WaitStrategy = BusySpinWait>
class SingleProducerSequencer {
    int64_t next();           // Claim next sequence (blocking)
    int64_t tryNext();        // Non-blocking claim
    int64_t next(int n);      // Batch claim
    void publish(int64_t);    // Make visible to consumers
};
```

### 4. SequenceBarrier (sequence_barrier.h)
Consumers wait on this.

```cpp
template<typename WaitStrategy = BusySpinWait>
class SequenceBarrier {
    int64_t waitFor(int64_t seq);  // Wait for sequence
    int64_t available();           // Highest available
};
```

### 5. RingBuffer (ring_buffer.h)
Main data structure combining all components.

```cpp
template<typename T, size_t SIZE, typename WaitStrategy = BusySpinWait>
class RingBuffer {
    T* get(int64_t seq);
    const T* get(int64_t seq) const;

    // Producer API
    int64_t next();
    void publish(int64_t seq);

    // Consumer API
    SequenceBarrier<WaitStrategy> newBarrier();
};
```

## File Structure

```
lmax_disruptor/
├── DESIGN.md                    # This file
├── include/
│   └── lmax/
│       ├── sequence.h           # Padded atomic sequence
│       ├── wait_strategy.h      # BusySpin, Yielding, Sleeping, Blocking
│       ├── single_producer_sequencer.h
│       ├── multi_producer_sequencer.h
│       ├── sequence_barrier.h   # Consumer wait point
│       ├── ring_buffer.h        # Main container
│       ├── event_processor.h    # Batch event handler
│       └── disruptor.h          # All-in-one include
├── test/
│   ├── sequence_test.cc
│   ├── spsc_test.cc             # Single producer single consumer
│   ├── spmc_test.cc             # Single producer multi consumer
│   ├── pipeline_test.cc         # WAL -> Replicator -> ME pattern
│   └── benchmark.cc             # Latency & throughput comparison
└── CMakeLists.txt
```

## Implementation Plan

### Phase 1: Core Components
1. [ ] `sequence.h` - Padded atomic sequence counter
2. [ ] `wait_strategy.h` - Template-based wait strategies
3. [ ] `single_producer_sequencer.h` - SPSC sequencer

### Phase 2: Ring Buffer
4. [ ] `ring_buffer.h` - Main data structure
5. [ ] `sequence_barrier.h` - Consumer wait mechanism

### Phase 3: Multi-Consumer Support
6. [ ] `sequence_group.h` - Track minimum of multiple sequences
7. [ ] `event_processor.h` - Batch processing helper

### Phase 4: Multi-Producer (Optional)
8. [ ] `multi_producer_sequencer.h` - CAS-based multi-producer

### Phase 5: Testing & Benchmarking
9. [ ] Unit tests for correctness
10. [ ] Latency benchmark (target: <100ns p50)
11. [ ] Throughput benchmark (target: >30M ops/sec)
12. [ ] Pipeline test for WAL -> Replicator -> ME

## Design Decisions

### Why int64_t instead of uint64_t?
- LMAX uses signed to start at -1 (nothing published)
- Avoids unsigned underflow issues in wrap calculations
- `seq & MASK` works correctly for positive values

### Why templates instead of virtual?
- Virtual call: ~10-20 cycles overhead per call
- Template: Zero overhead, fully inlined
- Critical for sub-100ns latency

### Memory Ordering
- Producer publish: `release`
- Consumer read cursor: `acquire`
- CAS operations: `acq_rel`
- Cached values: `relaxed` (local to thread)

### Cache Line Padding
- 64-byte alignment for all atomics
- Prevents false sharing between producer/consumer
- Critical for multi-threaded performance

## API Comparison

| Operation | Java Disruptor | Our C++ Version |
|-----------|---------------|-----------------|
| Claim | `sequencer.next()` | `ring.next()` |
| Get slot | `ring.get(seq)` | `ring.get(seq)` |
| Publish | `sequencer.publish(seq)` | `ring.publish(seq)` |
| Wait | `barrier.waitFor(seq)` | `barrier.waitFor(seq)` |
| Batch claim | `sequencer.next(n)` | `ring.next(n)` |

## Usage Example

```cpp
#include "lmax/disruptor.h"

struct Event {
    uint64_t id;
    double price;
};

// Create ring buffer
lmax::RingBuffer<Event, 65536> ring;

// Producer
void producer() {
    for (uint64_t i = 0; i < 1000000; i++) {
        int64_t seq = ring.next();      // Claim
        Event* e = ring.get(seq);       // Get slot
        e->id = i;
        e->price = 100.0;
        ring.publish(seq);              // Make visible
    }
}

// Consumer with batching
void consumer() {
    auto barrier = ring.newBarrier();
    int64_t nextSeq = 0;

    while (running) {
        int64_t available = barrier.waitFor(nextSeq);

        // Process batch
        while (nextSeq <= available) {
            const Event* e = ring.get(nextSeq);
            process(e);
            nextSeq++;
        }

        barrier.markConsumed(nextSeq - 1);
    }
}
```

## Pipeline Example (Your Use Case)

```cpp
// WAL -> Replicator -> ME pipeline
lmax::RingBuffer<Entry, 65536> ring;

lmax::Sequence walSeq;      // WAL consumer progress
lmax::Sequence replSeq;     // Replicator progress
lmax::Sequence meSeq;       // Matching Engine progress

// ME is the gating sequence (slowest consumer gates producer)
ring.addGatingSequence(&meSeq);

// WAL waits on cursor (first in chain)
void walHandler() {
    auto barrier = ring.newBarrier();  // Waits on cursor
    // ...
    walSeq.set(processed);
}

// Replicator waits on WAL
void replicator() {
    auto barrier = ring.newBarrier(&walSeq);  // Waits on walSeq
    // ...
    replSeq.set(processed);
}

// ME waits on Replicator (or backup ACK)
void matchingEngine() {
    auto barrier = ring.newBarrier(&replSeq);  // Waits on replSeq
    // ...
    meSeq.set(processed);  // Gates producer
}
```

## Performance Targets

| Metric | disruptor-cpp | Our Target |
|--------|---------------|------------|
| P50 Latency | 3,800 ns | **< 100 ns** |
| P99 Latency | 9,000 ns | **< 500 ns** |
| Throughput | 32 M ops/sec | **> 30 M ops/sec** |
| Virtual calls | Many | **Zero** |
