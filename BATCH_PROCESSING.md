# Batch Processing for SPSCQueue

## Vấn Đề với Single-Item Operations

**Mỗi push/pop đều cần 1 atomic operation:**
```cpp
// Producer: N messages = N atomic stores
for (int i = 0; i < N; i++) {
    T* msg = queue.alloc();
    *msg = data[i];
    queue.push();  // ← Atomic store #i
}

// Consumer: N messages = N atomic stores
for (int i = 0; i < N; i++) {
    T* msg = queue.front();
    process(*msg);
    queue.pop();   // ← Atomic store #i
}

// Total: 2N atomic operations cho N messages
```

**Vấn đề:**
- Atomic operations rất đắt: ~40-100 cycles
- Memory barrier overhead
- Cache coherency traffic
- Không tận dụng spatial locality

## Giải Pháp: Batch Processing

**Key insight**: Chỉ cần 1 atomic operation cho cả batch!

```cpp
// Producer: N messages = N/B atomic stores (B = batch size)
for (int i = 0; i < N; i += B) {
    // Allocate batch
    T** slots = queue.allocBatch(B, allocated);

    // Fill batch (no atomic ops)
    for (int j = 0; j < allocated; j++) {
        *slots[j] = data[i + j];
    }

    // Push entire batch with ONE atomic store
    queue.pushBatch(allocated);  // ← ONE atomic store for B messages!
}

// Consumer tương tự
// Total: 2N/B atomic operations (giảm B lần!)
```

## Implementation Details

### API Design

```cpp
// Allocate batch of slots
size_t allocated;
T** slots = queue.allocBatch(max_count, allocated);

// Write to slots (no synchronization needed)
for (size_t i = 0; i < allocated; i++) {
    *slots[i] = my_data[i];
}

// Push with ONE atomic operation
queue.pushBatch(allocated);

// Similar for consumer
T** slots = queue.frontBatch(max_count, available);
// Process slots...
queue.popBatch(available);
```

### Internal Implementation

```cpp
void pushBatch(size_t count) {
    write_idx += count;  // Local increment (no atomic)

    // Only ONE atomic store for entire batch!
    ((std::atomic<uint32_t>*)&write_idx)->store(
        write_idx,
        std::memory_order_release
    );
}

void popBatch(size_t count) {
    read_idx += count;   // Local increment

    // Only ONE atomic store!
    ((std::atomic<uint32_t>*)&read_idx)->store(
        read_idx,
        std::memory_order_release
    );
}
```

**Chìa khóa:**
1. Increment local variable nhiều lần (cheap)
2. Chỉ atomic store 1 lần cuối (expensive)
3. Memory ordering vẫn đảm bảo correctness

## Benchmark Results

### Throughput Improvement

| Batch Size | Throughput | Speedup vs Single | Atomic Ops | Reduction |
|------------|------------|-------------------|------------|-----------|
| 1 (single) | 30.55 Mops/sec | 1.00x | 2,000,000 | 0% |
| 4 | 34.73 Mops/sec | 1.14x | 500,000 | 75% |
| 8 | 71.98 Mops/sec | 2.36x | 250,000 | 87.5% |
| 16 | 105.11 Mops/sec | 3.44x | 125,000 | 93.8% |
| 32 | 138.20 Mops/sec | 4.52x | 62,500 | 96.9% |
| **64** | **148.07 Mops/sec** | **4.85x** | **31,250** | **98.4%** |

### Key Insights

**1. Atomic Operation Reduction**
- Batch 64: **98.4% ít atomic ops hơn**
- Từ 2M atomic ops → 31K atomic ops
- Mỗi atomic op saved ≈ 50-100 cycles

**2. Throughput Scaling**
- Near-linear scaling up to batch 16
- Diminishing returns after batch 32-64
- Sweet spot: **batch 16-32** (3-4x speedup)

**3. Why It Works**
```
Single-item:
  [alloc] → [write] → [ATOMIC PUSH] → [alloc] → [write] → [ATOMIC PUSH] ...
  ↑ 50 cyc            ↑ 100 cyc        ↑ 50 cyc            ↑ 100 cyc

Batch:
  [alloc batch] → [write slot 1] → [write slot 2] → ... → [ATOMIC PUSH BATCH]
  ↑ 100 cyc       ↑ 10 cyc          ↑ 10 cyc              ↑ 100 cyc for N msgs!
```

## Trade-offs

### Pros ✅
- **Massive throughput improvement** (4-5x)
- **Dramatically fewer atomic operations** (98% reduction)
- Better cache utilization (spatial locality)
- Lower bus traffic (fewer cache coherency messages)
- Amortized overhead

### Cons ⚠️
- **Increased latency per message** (messages wait for batch to fill)
- More complex API
- Requires buffering on producer/consumer side
- May not fill batch if traffic is bursty
- Not suitable for strict real-time requirements

## Use Cases

### ✅ GOOD for Batch Processing

**High throughput data pipelines:**
```cpp
// Network packet processing
queue.tryPushBatch(packets.size(), [&](Msg** slots, size_t count) {
    for (size_t i = 0; i < count; i++) {
        *slots[i] = packets[i];
    }
});
```

**Log/event aggregation:**
```cpp
// Buffer logs and flush in batches
std::vector<LogEvent> buffer;
if (buffer.size() >= BATCH_SIZE) {
    queue.tryPushBatch(buffer.size(), [&](Msg** slots, size_t count) {
        for (size_t i = 0; i < count; i++) {
            *slots[i] = buffer[i];
        }
    });
    buffer.clear();
}
```

**Streaming analytics:**
```cpp
// Process events in batches
queue.tryPopBatch(32, [&](Msg** slots, size_t count) {
    // SIMD-friendly batch processing
    for (size_t i = 0; i < count; i++) {
        results[i] = process(slots[i]);
    }
});
```

### ❌ BAD for Batch Processing

**Ultra-low latency trading:**
- Cannot wait for batch to fill
- Need immediate processing
- Use single-item operations

**Interactive applications:**
- User input must be responsive
- Batching adds latency
- Use single-item operations

**Control systems:**
- Real-time deadlines
- Predictable latency required
- Use single-item operations

## Hybrid Approach: Adaptive Batching

**Best of both worlds:**
```cpp
void smart_push(std::vector<Msg>& msgs) {
    if (msgs.size() >= MIN_BATCH_SIZE) {
        // Use batch if we have enough messages
        queue.tryPushBatch(msgs.size(), [&](Msg** slots, size_t count) {
            for (size_t i = 0; i < count; i++) {
                *slots[i] = msgs[i];
            }
        });
    } else {
        // Fall back to single-item for low traffic
        for (auto& msg : msgs) {
            queue.tryPush([&](Msg* slot) {
                *slot = msg;
            });
        }
    }
}
```

**Or time-based flushing:**
```cpp
std::vector<Msg> buffer;
auto last_flush = std::chrono::steady_clock::now();

void push_with_timeout(Msg msg) {
    buffer.push_back(msg);

    auto now = std::chrono::steady_clock::now();
    bool timeout = (now - last_flush) > MAX_BATCH_DELAY;
    bool full = buffer.size() >= MAX_BATCH_SIZE;

    if (timeout || full) {
        flush_batch();
        last_flush = now;
    }
}
```

## Performance Recommendations

### For Maximum Throughput
- **Use batch size 32-64**
- Buffer messages on producer side
- Process in batches on consumer side
- **Expected**: 4-5x throughput improvement

### For Low Latency
- **Use batch size 4-8**
- Small batches still give 2x improvement
- Latency impact minimal
- **Expected**: 2x throughput, <2x latency increase

### For Balanced Performance
- **Use adaptive batching**
- Batch when traffic is high
- Single-item when traffic is low
- Time-based flush for latency guarantee

## Code Example: Complete Implementation

```cpp
#include "SPSCQueueBatch.h"

SPSCQueueBatch<Msg, 4096> queue;

// Producer thread
void producer() {
    const size_t BATCH = 16;
    std::vector<Msg> buffer;
    buffer.reserve(BATCH);

    while (running) {
        // Collect messages
        collect_messages(buffer, BATCH);

        // Push batch
        size_t pushed = queue.tryPushBatch(buffer.size(),
            [&](Msg** slots, size_t count) {
                for (size_t i = 0; i < count; i++) {
                    *slots[i] = buffer[i];
                }
            }
        );

        buffer.erase(buffer.begin(), buffer.begin() + pushed);
    }
}

// Consumer thread
void consumer() {
    const size_t BATCH = 16;

    while (running) {
        size_t processed = queue.tryPopBatch(BATCH,
            [&](Msg** slots, size_t count) {
                // Process entire batch
                for (size_t i = 0; i < count; i++) {
                    process(*slots[i]);
                }
            }
        );

        if (processed == 0) {
            // Queue empty, can yield or spin
            std::this_thread::yield();
        }
    }
}
```

## Summary

**Batch processing is a powerful optimization for SPSCQueue:**

| Metric | Improvement |
|--------|-------------|
| Throughput | **Up to 4.85x** |
| Atomic operations | **98.4% reduction** |
| Best batch size | **16-32 for balanced, 64 for max throughput** |
| Use case | High throughput, can tolerate latency |

**When to use:**
- ✅ High message rate (>1M msgs/sec)
- ✅ Batch-friendly workload (logs, packets, events)
- ✅ Throughput > latency priority

**When NOT to use:**
- ❌ Ultra-low latency requirements (<1μs)
- ❌ Real-time deadlines
- ❌ Interactive/responsive systems

**Implementation:** See [SPSCQueueBatch.h](SPSCQueueBatch.h)
**Benchmark:** See [test/batch_benchmark.cc](test/batch_benchmark.cc)
