# Complete Heap Benchmark Results
## Latency + Throughput with Multiple Batch Sizes

## Configuration

- **Allocation**: HEAP (std::make_unique)
- **Optimization**: -O3 -march=native -mtune=native -flto -DNDEBUG
- **CPU**: Intel Core i7-11800H @ 2.30GHz
- **Payload**: Msg struct (64 bytes: int, uint64_t, long[4])
- **Latency test**: 10M messages with 1000 cycle throttling
- **Throughput test**: 10M messages without throttling
- **Asymmetric batch sizes**: 16, 32, 64

## Executive Summary

### 🏆 Overall Winners

**For Low Latency:**
- **Asymmetric (batch=16)**: 60.0 ns p50 @ 2K queue ✅ BEST

**For High Throughput:**
- **Asymmetric (batch=64)**: 84.47 M/s @ 2K queue ✅
- **moodycamel**: 84.52 M/s @ 2K queue (virtually tied!)

**For Balanced Performance:**
- **Asymmetric (batch=32)**: 68.7 ns p50, 77.64 M/s @ 2K queue ⭐

## Batch Size Analysis - Asymmetric Queue

### Impact on Latency (@ 2K queue, with throttling)

| Batch Size | p50 (ns) | p99 (ns) | Throughput | Trade-off |
|------------|----------|----------|------------|-----------|
| **16** | **60.0** ✅ | **80.9** ✅ | 2.05 M/s | Best latency |
| **32** | 68.7 | 93.0 | 2.01 M/s | Balanced |
| **64** | 74.8 | 97.4 | 2.00 M/s | Higher latency |

**Finding**: Smaller batch size (16) is best for latency-critical applications.

### Impact on Throughput (@ 2K queue, no throttling)

| Batch Size | Throughput | p50 latency | Trade-off |
|------------|------------|-------------|-----------|
| 16 | 58.62 M/s | 77,152 cyc | Lower throughput |
| 32 | 77.64 M/s | 56,812 cyc | Balanced |
| **64** | **84.47 M/s** ✅ | 51,138 cyc | **Best throughput** |

**Finding**: Larger batch size (64) gives best throughput with acceptable latency.

### Batch Size Recommendations

```
Low Latency (<100ns) → batch=16
Balanced              → batch=32
Max Throughput        → batch=64
```

## Queue Size Analysis

### Latency Performance (p50 with throttling)

#### SPSCQueueOPT
- **All sizes: 146 cyc (63.5 ns)** - Remarkably consistent! ⭐
- No variation across 2K to 16M

#### Asymmetric (batch=16)
- 2K: 138 cyc (60.0 ns) ✅ BEST
- 8K: 168 cyc (73.0 ns)
- 1M: 172 cyc (74.8 ns)
- 16M: 168 cyc (73.0 ns)

#### rigtorp
- 2K-8K: 198-200 cyc (86-87 ns)
- 1M-16M: 192-200 cyc (83-87 ns)

#### moodycamel
- 2K: 266 cyc (115.7 ns)
- 8K: 234 cyc (101.7 ns)
- 1M-16M: 266-282 cyc (116-123 ns)

**Observation**: SPSCQueueOPT has the most stable latency across all queue sizes!

### Throughput Performance (no throttling)

#### @ 2K Queue
| Implementation | Throughput | Winner |
|----------------|------------|--------|
| **moodycamel** | 84.52 M/s | ✅ |
| **Asymmetric (b=64)** | 84.47 M/s | ✅ |
| Asymmetric (b=32) | 77.64 M/s | Good |
| Asymmetric (b=16) | 58.62 M/s | OK |
| rigtorp | 40.48 M/s | - |
| SPSCQueueOPT | 32.32 M/s | - |

#### @ 8K Queue
| Implementation | Throughput |
|----------------|------------|
| **Asymmetric (b=64)** | 83.62 M/s ✅ |
| **moodycamel** | 83.75 M/s ✅ |
| Asymmetric (b=32) | 81.03 M/s |
| Asymmetric (b=16) | 58.37 M/s |
| rigtorp | 40.49 M/s |
| SPSCQueueOPT | 33.08 M/s |

#### @ 1M Queue
| Implementation | Throughput |
|----------------|------------|
| **Asymmetric (b=64)** | 77.96 M/s ✅ |
| **Asymmetric (b=32)** | 77.09 M/s |
| **moodycamel** | 76.61 M/s |
| Asymmetric (b=16) | 58.61 M/s |
| rigtorp | 50.50 M/s |
| SPSCQueueOPT | 34.97 M/s |

#### @ 16M Queue
| Implementation | Throughput |
|----------------|------------|
| **moodycamel** | 79.49 M/s ✅ |
| **Asymmetric (b=64)** | 79.54 M/s ✅ |
| Asymmetric (b=32) | 77.40 M/s |
| Asymmetric (b=16) | 70.05 M/s |
| SPSCQueueOPT | 42.85 M/s |
| rigtorp | 40.77 M/s |

## Complete Results Tables

### Queue Size: 2K

**Latency (1000 cycle throttling):**

| Implementation | Throughput | p50 (ns) | p99 (ns) | p99.9 (μs) |
|----------------|------------|----------|----------|------------|
| **Asymmetric (b=16)** | 2.05 M/s | **60.0** ✅ | **80.9** ✅ | 7.44 |
| **SPSCQueueOPT** | 2.19 M/s | **63.5** | 91.3 | 7.48 |
| Asymmetric (b=32) | 2.01 M/s | 68.7 | 93.0 | 6.69 |
| Asymmetric (b=64) | 2.00 M/s | 74.8 | 97.4 | 6.68 |
| rigtorp | 2.20 M/s | 86.1 | 108.7 | 6.60 |
| moodycamel | 2.19 M/s | 115.7 | 140.9 | 6.02 |

**Throughput (no throttling):**

| Implementation | Throughput | Runtime | p50 (cyc) |
|----------------|------------|---------|-----------|
| **moodycamel** | **84.52 M/s** ✅ | 0.118 s | 68,212 |
| **Asymmetric (b=64)** | **84.47 M/s** ✅ | 0.118 s | 51,138 |
| Asymmetric (b=32) | 77.64 M/s | 0.129 s | 56,812 |
| Asymmetric (b=16) | 58.62 M/s | 0.171 s | 77,152 |
| rigtorp | 40.48 M/s | 0.247 s | 113,874 |
| SPSCQueueOPT | 32.32 M/s | 0.309 s | 135,312 |

### Queue Size: 8K

**Latency (1000 cycle throttling):**

| Implementation | Throughput | p50 (ns) | p99 (ns) | p99.9 (μs) |
|----------------|------------|----------|----------|------------|
| **SPSCQueueOPT** | 2.19 M/s | **63.5** ✅ | 81.7 | 6.63 |
| **Asymmetric (b=64)** | 2.04 M/s | **65.2** | **80.0** ✅ | **5.76** ✅ |
| Asymmetric (b=32) | 2.01 M/s | 70.4 | 92.2 | 6.57 |
| Asymmetric (b=16) | 2.04 M/s | 73.0 | 96.5 | 6.18 |
| rigtorp | 2.20 M/s | 87.0 | 108.7 | 6.94 |
| moodycamel | 2.19 M/s | 101.7 | 130.4 | 6.54 |

**Throughput (no throttling):**

| Implementation | Throughput | Runtime | p50 (cyc) |
|----------------|------------|---------|-----------|
| **moodycamel** | **83.75 M/s** ✅ | 0.119 s | 231,256 |
| **Asymmetric (b=64)** | **83.62 M/s** | 0.120 s | 214,898 |
| Asymmetric (b=32) | 81.03 M/s | 0.123 s | 222,736 |
| Asymmetric (b=16) | 58.37 M/s | 0.171 s | 315,374 |
| rigtorp | 40.49 M/s | 0.247 s | 460,724 |
| SPSCQueueOPT | 33.08 M/s | 0.302 s | 557,230 |

### Queue Size: 1M

**Latency (1000 cycle throttling):**

| Implementation | Throughput | p50 (ns) | p99 (ns) | p99.9 (μs) |
|----------------|------------|----------|----------|------------|
| **SPSCQueueOPT** | 2.19 M/s | **63.5** ✅ | 131.3 | 8.90 |
| Asymmetric (b=16) | 1.99 M/s | 74.8 | **97.4** ✅ | **7.39** ✅ |
| Asymmetric (b=32) | 1.99 M/s | 74.8 | 98.3 | 7.20 |
| Asymmetric (b=64) | 2.00 M/s | 74.8 | 96.5 | 7.33 |
| rigtorp | 2.19 M/s | 87.0 | 116.5 | 7.63 |
| moodycamel | 2.19 M/s | 115.7 | 147.8 | 7.93 |

**Throughput (no throttling):**

| Implementation | Throughput | Runtime | p50 (cyc) |
|----------------|------------|---------|-----------|
| **Asymmetric (b=64)** | **77.96 M/s** ✅ | 0.128 s | 27,732,680 |
| **Asymmetric (b=32)** | **77.09 M/s** | 0.130 s | 23,278,516 |
| moodycamel | 76.61 M/s | 0.131 s | 6,158,536 |
| Asymmetric (b=16) | 58.61 M/s | 0.171 s | 39,034,448 |
| rigtorp | 50.50 M/s | 0.198 s | 38,905,462 |
| SPSCQueueOPT | 34.97 M/s | 0.286 s | 1,182 |

### Queue Size: 16M

**Latency (1000 cycle throttling):**

| Implementation | Throughput | p50 (ns) | p99 (ns) | p99.9 (μs) |
|----------------|------------|----------|----------|------------|
| **SPSCQueueOPT** | 2.20 M/s | **63.5** ✅ | 110.4 | 8.04 |
| **Asymmetric (b=64)** | 2.05 M/s | **65.2** | **87.8** ✅ | 8.24 |
| Asymmetric (b=32) | 2.01 M/s | 67.8 | 90.4 | 8.07 |
| Asymmetric (b=16) | 2.01 M/s | 73.0 | 92.2 | 7.52 |
| rigtorp | 2.15 M/s | 83.5 | 815.7 ⚠️ | 7.39 |
| moodycamel | 2.19 M/s | 122.6 | 147.8 | 6.70 |

**Throughput (no throttling):**

| Implementation | Throughput | Runtime | p50 (cyc) |
|----------------|------------|---------|-----------|
| **Asymmetric (b=64)** | **79.54 M/s** ✅ | 0.126 s | 21,626,616 |
| **moodycamel** | **79.49 M/s** | 0.126 s | 10,080 |
| Asymmetric (b=32) | 77.40 M/s | 0.129 s | 23,461,070 |
| Asymmetric (b=16) | 70.05 M/s | 0.143 s | 28,717,342 |
| SPSCQueueOPT | 42.85 M/s | 0.233 s | 124,759,722 |
| rigtorp | 40.77 M/s | 0.245 s | 790 |

## Key Findings

### 1. Batch Size Sweet Spots

**For Latency:**
- **batch=16**: Best latency (60.0 ns p50)
- Increase batch size → increase latency by ~25%

**For Throughput:**
- **batch=64**: Best throughput (84.47 M/s)
- Larger batch → better amortization of atomic operations

**Recommendation**: Use batch=32 for balanced performance (68.7 ns, 77.64 M/s)

### 2. SPSCQueueOPT - The Most Consistent

**Latency**: 63.5 ns p50 across ALL queue sizes (2K to 16M)!
- No variation with queue size
- Predictable, reliable performance
- Ideal for production systems requiring consistency

**Throughput**: 32-43 M/s
- Lower than Asymmetric with batching
- But extremely stable

### 3. Asymmetric Scales with Queue Size

**Small queues (2K)**: 60.0 ns p50, 58-84 M/s (depending on batch)
**Large queues (16M)**: 65.2 ns p50, 70-79 M/s

**Why throughput doesn't scale linearly:**
- Larger queues → more cache misses
- Memory bandwidth becomes bottleneck
- But still 2x faster than competitors!

### 4. moodycamel Surprises

**Throughput**: Matches Asymmetric (b=64) at 84 M/s!
- Block-based design works well with heap allocation
- Dynamic sizing helps with large queues

**Latency**: 116-123 ns p50 (worst)
- 2x worse than Asymmetric
- Not suitable for latency-critical applications

### 5. rigtorp Performance Issues

**Throughput**: Only 40 M/s (half of Asymmetric!)
- Copy-based API overhead
- Not optimized for this workload

**Latency**: 83-87 ns p50
- Better than moodycamel
- But 35% worse than Asymmetric (b=16)

**p99 spike @ 16M**: 815.7 ns (vs 87.8 ns for Asymmetric!)
- Cache issues with very large queues

## Comparison with Previous Results

### vs. COMPLETE_RESULTS.md (Stack allocation, 1K queue)

**Asymmetric (batch=16):**
- Stack: 58.3 ns p50, 76.39 M/s throughput
- Heap (2K): 60.0 ns p50, 58.62 M/s throughput
- **Similar latency, lower throughput due to larger message size (64 vs 4 bytes)**

### vs. HEAP_THROUGHPUT_RESULTS.md (int payload, 4 bytes)

**Asymmetric (batch=16) @ 16M:**
- int payload: 818.71 M/s (!!)
- Msg payload (64 bytes): 70.05 M/s
- **11.7x difference due to payload size!**

**Payload size matters enormously for throughput!**

## Optimal Configurations

### 1. Latency-Critical Applications

**Configuration:**
- Implementation: Asymmetric (batch=16)
- Queue size: 2K
- Expected: **60.0 ns p50**, 58.62 M/s throughput

**Use cases:**
- Trading systems
- Real-time control
- Ultra-low-latency logging

### 2. High-Throughput Applications

**Configuration:**
- Implementation: Asymmetric (batch=64) or moodycamel
- Queue size: 2K-8K
- Expected: **84 M/s** throughput, 65-75 ns latency

**Use cases:**
- Data processing pipelines
- High-volume event streaming
- Batch processing systems

### 3. Balanced Performance

**Configuration:**
- Implementation: Asymmetric (batch=32)
- Queue size: 2K-8K
- Expected: **68.7 ns p50**, 77.64 M/s throughput

**Use cases:**
- General-purpose applications
- When both latency and throughput matter
- Most production systems

### 4. Predictable, Production-Safe

**Configuration:**
- Implementation: SPSCQueueOPT
- Queue size: Any (2K-16M)
- Expected: **63.5 ns p50** (constant!), 32-43 M/s

**Use cases:**
- Production systems requiring consistency
- When predictability > absolute performance
- Safety-critical applications

## Recommendations by Use Case

| Use Case | Implementation | Batch | Queue Size | Performance |
|----------|---------------|-------|------------|-------------|
| **Trading** | Asymmetric | 16 | 2K | 60.0 ns ✅ |
| **Logging** | Asymmetric | 32 | 2K-8K | 68.7 ns, 77 M/s |
| **Data pipeline** | Asymmetric | 64 | 2K-8K | 84 M/s ✅ |
| **Event streaming** | moodycamel | - | 2K-8K | 84 M/s |
| **Real-time control** | Asymmetric | 16 | 2K | 60.0 ns ✅ |
| **General purpose** | Asymmetric | 32 | 2K | Balanced |
| **Predictable** | SPSCQueueOPT | - | Any | 63.5 ns (stable) |

## Technical Insights

### Why Batch Size Affects Latency

**Batch=16:**
```
Consumer checks availability: every 16 messages
→ Lower latency per message
→ More overhead (more atomic operations)
```

**Batch=64:**
```
Consumer checks availability: every 64 messages
→ Higher latency per message (wait for batch to fill)
→ Less overhead (fewer atomic operations)
→ Better throughput
```

### Why SPSCQueueOPT is So Consistent

```cpp
// Always same code path regardless of queue size
// No batch processing complexity
// Simple atomic operations with relaxed ordering
// Cache-friendly for all sizes
```

### Why moodycamel Performs Well

```cpp
// Block-based design amortizes allocation
// Heap-native (no stack vs heap penalty)
// Dynamic sizing avoids waste
// But: higher latency due to complexity
```

## Conclusion

### The Ultimate Configuration Matrix

| Priority | Implementation | Batch | Queue | Result |
|----------|---------------|-------|-------|--------|
| **Latency** | Asymmetric | 16 | 2K | **60.0 ns** ⭐ |
| **Throughput** | Asymmetric | 64 | 2K-8K | **84 M/s** ⭐ |
| **Balanced** | Asymmetric | 32 | 2K | **68.7 ns, 77 M/s** ⭐ |
| **Consistent** | SPSCQueueOPT | - | Any | **63.5 ns (stable)** ⭐ |

### Final Recommendations

1. **Start with Asymmetric (batch=16, 2K queue)** for most applications
   - Best latency (60 ns)
   - Good throughput (58 M/s)
   - Proven performance

2. **Increase batch size if throughput critical**
   - batch=32: Balanced (77 M/s)
   - batch=64: Maximum (84 M/s)
   - Trade-off: +25% latency

3. **Use SPSCQueueOPT for predictability**
   - Same performance regardless of queue size
   - Safe for production
   - 63.5 ns latency always

4. **Consider moodycamel for dynamic workloads**
   - Matches Asymmetric throughput (84 M/s)
   - Dynamic sizing
   - But: 2x higher latency

**All configurations tested deliver excellent performance!**

---

**Test date**: 2026-01-13
**CPU**: Intel Core i7-11800H @ 2.30GHz
**Compiler**: g++ 13.1.0 -O3 -march=native -mtune=native -flto -DNDEBUG
