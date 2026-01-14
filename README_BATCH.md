# SPSCQueueBatch - Batch Processing Extension

## Quick Summary

**SPSCQueueBatch** extends SPSCQueue with batch operations that achieve **4-17x throughput improvement** by reducing atomic operations by up to **98.4%**.

## Performance Results

| Batch Size | Throughput | Speedup | Atomic Ops | Reduction |
|------------|------------|---------|------------|-----------|
| 1 (single) | 6.65 - 30 Mops/sec | 1.0x | N×2 | 0% |
| 4 | 27.57 - 35 Mops/sec | 4.1x | N×2÷4 | 75% |
| 16 | 80.48 - 105 Mops/sec | 12.1x | N×2÷16 | 93.8% |
| **64** | **111 - 148 Mops/sec** | **16.8x** | **N×2÷64** | **98.4%** |

## How It Works

### Problem with Single-Item Operations
```cpp
// N messages = N atomic stores (expensive!)
for (int i = 0; i < N; i++) {
    msg = queue.alloc();
    *msg = data[i];
    queue.push();  // ← Atomic store (50-100 cycles)
}
```

### Solution: Batch Processing
```cpp
// N messages = N/B atomic stores (B = batch size)
queue.tryPushBatch(N, [&](T** slots, size_t count) {
    for (size_t i = 0; i < count; i++) {
        *slots[i] = data[i];
    }
});  // ← ONE atomic store for entire batch!
```

**Key insight**: Amortize atomic operation cost over multiple messages.

## API Reference

### Producer API

```cpp
// Method 1: Manual allocation
size_t allocated;
T** slots = queue.allocBatch(max_count, allocated);
// Fill slots...
queue.pushBatch(allocated);

// Method 2: Callback-based (recommended)
size_t pushed = queue.tryPushBatch(max_count,
    [](T** slots, size_t count) {
        for (size_t i = 0; i < count; i++) {
            *slots[i] = my_data[i];
        }
    }
);
```

### Consumer API

```cpp
// Method 1: Manual access
size_t available;
T** slots = queue.frontBatch(max_count, available);
// Process slots...
queue.popBatch(available);

// Method 2: Callback-based (recommended)
size_t popped = queue.tryPopBatch(max_count,
    [](T** slots, size_t count) {
        for (size_t i = 0; i < count; i++) {
            process(*slots[i]);
        }
    }
);
```

## Usage Examples

### Example 1: Basic Batch Operations
```cpp
#include "SPSCQueueBatch.h"

SPSCQueueBatch<int, 1024> queue;

// Producer: Push 100 messages in batches of 10
for (int batch = 0; batch < 10; batch++) {
    queue.tryPushBatch(10, [&](int** slots, size_t count) {
        for (size_t i = 0; i < count; i++) {
            *slots[i] = batch * 10 + i;
        }
    });
}

// Consumer: Pop in batches of 20
size_t total = 0;
while (total < 100) {
    total += queue.tryPopBatch(20, [](int** slots, size_t count) {
        for (size_t i = 0; i < count; i++) {
            std::cout << *slots[i] << " ";
        }
    });
}
```

### Example 2: Adaptive Batching (Best Practice)
```cpp
const size_t MIN_BATCH = 4;
const size_t MAX_BATCH = 32;
const auto MAX_DELAY = std::chrono::microseconds(100);

std::vector<Msg> buffer;
auto last_flush = std::chrono::steady_clock::now();

void producer_loop() {
    while (running) {
        // Collect message
        buffer.push_back(get_next_message());

        auto now = std::chrono::steady_clock::now();
        bool timeout = (now - last_flush) > MAX_DELAY;
        bool full = buffer.size() >= MAX_BATCH;

        if ((timeout && buffer.size() >= MIN_BATCH) || full) {
            queue.tryPushBatch(buffer.size(),
                [&](Msg** slots, size_t count) {
                    for (size_t i = 0; i < count; i++) {
                        *slots[i] = buffer[i];
                    }
                }
            );
            buffer.clear();
            last_flush = now;
        }
    }
}
```

## When to Use Batch Processing

### ✅ USE Batching When:
- **High throughput required** (>1M msgs/sec)
- **Batch-friendly workload**: logs, packets, events, analytics
- **Can tolerate latency**: batch filling adds delay
- **Bursty traffic**: messages arrive in groups
- **CPU-bound**: atomic operations are bottleneck

**Best batch sizes:**
- **4-8**: Low latency, 2-4x speedup
- **16-32**: Balanced, 3-12x speedup (recommended)
- **64+**: Max throughput, 15-17x speedup

### ❌ DON'T Use Batching When:
- **Ultra-low latency required** (<1μs per message)
- **Real-time deadlines**: need predictable latency
- **Interactive applications**: user-facing, need responsiveness
- **Single message at a time**: no benefit from batching
- **Memory constrained**: batching requires buffering

## Comparison: SPSCQueue vs SPSCQueueBatch

| Feature | SPSCQueue | SPSCQueueBatch |
|---------|-----------|----------------|
| **Throughput** | 2-30 Mops/sec | 30-150 Mops/sec |
| **Latency** | 70-200 ns | 70 ns - 10 μs |
| **API** | Simple | More complex |
| **Use case** | General purpose | High throughput |
| **Atomic ops** | 2 per message | 2 per batch |

## Building and Testing

### Compile Examples
```bash
cd examples
g++ -std=c++20 -O3 -march=native -o batch_example batch_example.cc -pthread
./batch_example
```

### Run Benchmarks
```bash
cd test
g++ -std=c++20 -O3 -march=native -o batch_benchmark batch_benchmark.cc -pthread
./batch_benchmark
```

## Implementation Notes

### Memory Layout
```cpp
// Same as SPSCQueue, plus:
T* batch_ptrs[CNT];  // Thread-local storage for batch pointers
```

**Memory overhead**: `CNT × sizeof(T*)` bytes (e.g., 8KB for CNT=1024)

### Thread Safety
- **Same as SPSCQueue**: Single producer, single consumer only
- Batch operations are **not** atomic with respect to single operations
- Don't mix batch and single operations concurrently

### Correctness
- **Memory ordering preserved**: `memory_order_release` on push, `memory_order_acquire` on pop
- **Wraparound safe**: Uses modulo arithmetic with power-of-2 sizes
- **Overflow detection**: `allocBatch`/`frontBatch` check available space

## Performance Tips

1. **Choose batch size based on use case:**
   - Latency-sensitive: 4-8
   - Balanced: 16-32
   - Throughput-focused: 64+

2. **Use adaptive batching:**
   - Batch when traffic is high
   - Flush on timeout for latency guarantee
   - Fall back to single-item on low traffic

3. **Buffer on producer side:**
   - Collect messages before batching
   - Reduces contention
   - Better cache utilization

4. **Process in batches on consumer:**
   - SIMD-friendly
   - Better instruction cache usage
   - Amortize processing overhead

## Files

- **[SPSCQueueBatch.h](SPSCQueueBatch.h)** - Main implementation
- **[BATCH_PROCESSING.md](BATCH_PROCESSING.md)** - Detailed documentation
- **[examples/batch_example.cc](examples/batch_example.cc)** - Usage examples
- **[test/batch_benchmark.cc](test/batch_benchmark.cc)** - Performance benchmarks

## See Also

- [PERFORMANCE_OPTIMIZATION.md](PERFORMANCE_OPTIMIZATION.md) - All optimization techniques
- [OPTIMIZATION_RESULTS.md](OPTIMIZATION_RESULTS.md) - Benchmark results
- [QUICK_START.md](QUICK_START.md) - Quick start guide
