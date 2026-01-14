# SPSC Queue Implementation Comparison

## ⚠️ DEPRECATED - See COMPLETE_RESULTS.md for accurate measurements

**This file contains results from incorrect measurement methodology.**

The measurements here included alloc() overhead and used inconsistent throttling, inflating latency numbers by 3-4x. See [COMPLETE_RESULTS.md](COMPLETE_RESULTS.md) for accurate results using the correct methodology that matches multhread_q.cc.

---

## OLD Benchmark Results (INCORRECT METHODOLOGY)

**Test Configuration:**
- Messages: 1,000,000
- Message size: 64 bytes
- Queue size: 4,096
- CPU: Intel Core i7-11800H @ 2.30GHz
- Compiler: g++ -std=c++20 -O3 -march=native

**PROBLEMS WITH THIS BENCHMARK:**
- ❌ Measured alloc() + push() instead of just queue latency
- ❌ No consistent throttling (1000 cycles)
- ❌ Wrong timestamp placement
- ❌ Different methodology from reference benchmark (multhread_q.cc)

### Summary Table (INACCURATE - DO NOT USE)

| Implementation | Throughput | Prod p50 | Prod p99 | E2E p50 | E2E p99 | E2E p99.9 |
|----------------|------------|----------|----------|---------|---------|-----------|
| **SPSCQueue (original)** | 12.23 M/s | 47 ns | 86 ns | **201 ns** ✅ | 25,620 ns | 80,335 ns |
| **Asymmetric (batch=16)** | 13.44 M/s | 45 ns | 82 ns | 233 ns | **9,303 ns** ✅ | **18,339 ns** ✅ |
| **rigtorp::SPSCQueue** | **14.79 M/s** ✅ | 37 ns | 111 ns | 400 ns | 21,780 ns | 37,283 ns |
| **moodycamel::ReaderWriterQueue** | 11.85 M/s | **33 ns** ✅ | 124 ns | 610 ns | 255,135 ns | 282,296 ns |

## Detailed Analysis

### 1. SPSCQueue (Original - Meng Rao)

**Strengths:**
- ✅ **Best end-to-end p50 latency** (201 ns)
- ✅ Good producer latency (47 ns p50)
- ✅ Simple, clean implementation
- ✅ Uses memory_order_consume for producer reads

**Weaknesses:**
- ⚠️ Moderate throughput (12.23 Mops/sec)
- ⚠️ Higher tail latency (p99.9: 80μs)

**Best for:** General-purpose low-latency applications

---

### 2. Asymmetric (Ours - Consumer Batching)

**Strengths:**
- ✅ **Best tail latency** (p99: 9.3μs, p99.9: 18.3μs)
- ✅ Good producer latency (45 ns p50)
- ✅ **+10% throughput** vs original
- ✅ Simple producer code (same as original)
- ✅ **-64% p99 latency** vs original

**Weaknesses:**
- ⚠️ Slightly higher p50 e2e latency (233ns vs 201ns)
- ⚠️ Requires consumer batch API

**Best for:** **Latency-critical producers with batch-friendly consumers** ⭐

**Key Innovation:** Only consumer batches → producer stays simple and fast!

---

### 3. rigtorp::SPSCQueue

**Strengths:**
- ✅ **Highest throughput** (14.79 Mops/sec) - **+21% vs original**
- ✅ Excellent producer latency (37 ns p50)
- ✅ Modern C++ design (allocator support, emplace, etc.)
- ✅ Well-tested and production-proven

**Weaknesses:**
- ⚠️ Higher e2e latency (400 ns p50)
- ⚠️ Copy-based API (try_push by value)
- ⚠️ More complex implementation

**Best for:** High-throughput applications, modern C++ codebases

**Why it's faster:**
- Uses `std::hardware_destructive_interference_size` for alignment
- Optimized for throughput over latency
- Likely better branch prediction

---

### 4. moodycamel::ReaderWriterQueue

**Strengths:**
- ✅ **Best producer latency** (33 ns p50) - **lowest!**
- ✅ Dynamically sized (can grow)
- ✅ Very popular and widely used
- ✅ Feature-rich (blocking operations, bulk ops)

**Weaknesses:**
- ⚠️ **Worst tail latency** (p99: 255μs, p99.9: 282μs)
- ⚠️ Highest e2e latency (610 ns p50)
- ⚠️ Memory allocations (dynamic growth)
- ⚠️ More complex implementation

**Best for:** Variable queue sizes, ease of use, not latency-critical

**Why tail latency is high:**
- Dynamic memory allocation
- More complex internal logic
- Block-based design

---

## Key Findings

### 1. Throughput Winner: rigtorp::SPSCQueue (14.79 Mops/sec)

**+21% faster than original!**

Reasons:
- Optimized memory layout (hardware destructive interference size)
- Better cache utilization
- Modern C++ optimizations

### 2. Producer Latency Winner: moodycamel (33 ns p50)

**-30% lower than original!**

But comes with trade-offs:
- Much higher e2e latency
- Dynamic allocations
- Complex implementation

### 3. E2E Latency Winner: Original SPSCQueue (201 ns p50)

**Best for latency-sensitive applications**

Simple, predictable, low latency.

### 4. Tail Latency Winner: Asymmetric (p99: 9.3μs)

**-64% better p99 than original!**

Consumer batching dramatically reduces tail latency while keeping producer simple.

---

## Trade-off Analysis

### Latency vs Throughput

```
Low Latency ←                                    → High Throughput
│                                                              │
SPSCQueue (orig) → Asymmetric → rigtorp → moodycamel
201ns p50           233ns p50     400ns       610ns
12.23 M/s           13.44 M/s     14.79 M/s   11.85 M/s
```

**Sweet spot:** Asymmetric queue
- Good latency (233ns p50)
- Good throughput (13.44 M/s)
- Best tail latency (9.3μs p99)

### Tail Latency Comparison

| Implementation | p99 | p99.9 | Stability |
|----------------|-----|-------|-----------|
| **Asymmetric** | **9.3 μs** | **18.3 μs** | ⭐⭐⭐⭐⭐ Best |
| Original | 25.6 μs | 80.3 μs | ⭐⭐⭐⭐ Good |
| rigtorp | 21.8 μs | 37.3 μs | ⭐⭐⭐⭐ Good |
| moodycamel | 255 μs | 282 μs | ⭐ Poor |

**Asymmetric queue has 3x better tail latency than next best!**

---

## Recommendations

### When to Use Each Implementation

#### ✅ Use **SPSCQueue (Original)** when:
- Need predictable, low latency (p50 < 250ns)
- Simple, proven implementation
- General-purpose use case
- **Recommended starting point**

#### ✅ Use **Asymmetric** when:
- **Producer is latency-critical** ⭐
- Consumer can process in batches
- Need best tail latency (p99 < 10μs)
- **Best for: trading, logging, event handling**

#### ✅ Use **rigtorp::SPSCQueue** when:
- Need maximum throughput (>14 Mops/sec)
- Modern C++ codebase
- Can tolerate higher e2e latency
- Want production-proven code

#### ✅ Use **moodycamel::ReaderWriterQueue** when:
- Need dynamic sizing
- Variable workload
- Ease of use > performance
- Blocking operations needed
- **NOT for low-latency applications**

---

## Performance Characteristics Summary

| Metric | Original | Asymmetric | rigtorp | moodycamel |
|--------|----------|------------|---------|------------|
| **Throughput** | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| **Producer Latency** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **E2E Latency** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ |
| **Tail Latency** | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐ |
| **Simplicity** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ |
| **Features** | ⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |

---

## Our Innovation: Asymmetric Queue

### Why It's Special

**Key insight:** Producer and consumer have different optimization goals!

```
Producer: Already has read_idx_cach
         → Checking space is cheap
         → No need to buffer
         → Push immediately = LOW LATENCY

Consumer: Processes many messages
         → Can amortize overhead
         → Batch pop = HIGH THROUGHPUT + BEST TAIL LATENCY
```

### Results Prove the Concept

Compared to original SPSCQueue:
- ✅ **Producer latency**: 45ns vs 47ns (-4%) - stays fast!
- ✅ **Throughput**: +10% (13.44 vs 12.23 Mops/sec)
- ✅ **Tail latency**: -64% p99 (9.3μs vs 25.6μs)
- ✅ **p99.9 latency**: -77% (18.3μs vs 80.3μs)

**This is the sweet spot for most applications!**

---

## Conclusion

### Overall Winner: Depends on Your Needs

**For maximum throughput:**
→ **rigtorp::SPSCQueue** (14.79 Mops/sec)

**For lowest producer latency:**
→ **moodycamel::ReaderWriterQueue** (33 ns p50)

**For lowest e2e latency:**
→ **SPSCQueue (original)** (201 ns p50)

**For best tail latency & balanced performance:**
→ **Asymmetric** (9.3μs p99, 13.44 Mops/sec) ⭐ **Our recommendation**

### The Asymmetric Advantage

**If your use case is:**
- Producer is latency-sensitive (logging, order submission, event capture)
- Consumer can batch (processing, forwarding, aggregation)

**Then Asymmetric queue gives you:**
- ✅ Lowest tail latency (3x better than others)
- ✅ Fast producer (45ns, simple code)
- ✅ Good throughput (+10%)
- ✅ Simple implementation

**This is why we built it - and benchmarks prove it works! 🎯**

---

## Files

- [test/comprehensive_comparison.cc](test/comprehensive_comparison.cc) - Benchmark code
- [SPSCQueue.h](SPSCQueue.h) - Original implementation
- [SPSCQueueAsymmetric.h](SPSCQueueAsymmetric.h) - Our optimized version
- [rigtorp_spsc/](rigtorp_spsc/) - rigtorp implementation
- [moodycamel_rwq/](moodycamel_rwq/) - moodycamel implementation
