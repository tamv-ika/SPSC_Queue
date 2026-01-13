# Quick Start Guide - Performance Optimization

## TL;DR - Làm Gì Để Tăng Performance?

### 1. Chọn Queue Phù Hợp (30 giây)

```cpp
// ✅ RECOMMENDED: Use SPSCQueueOPT với queue size lớn hơn
#include "SPSCQueueOPT.h"
typedef SPSCQueueOPT<YourMsg, 4096> FastQueue;  // 4K slots

// ❌ AVOID: Queue size nhỏ
typedef SPSCQueue<YourMsg, 256> SlowQueue;  // Chỉ 256 slots
```

**Improvement**: +15% throughput, -85% p99 latency

---

### 2. Apply System Optimizations (2 phút)

```bash
# Chạy script optimization (requires sudo)
sudo ./optimize_system.sh
```

**Improvement**: -20% latency variance, -30% interrupts

---

### 3. (Optional) Kernel Parameters cho Production (5 phút + reboot)

```bash
# Edit grub config
sudo nano /etc/default/grub

# Thêm dòng này (thay 6,7 bằng CPUs bạn dùng):
GRUB_CMDLINE_LINUX="isolcpus=6,7 nohz_full=6,7 rcu_nocbs=6,7"

# Apply
sudo update-grub
sudo reboot
```

**Improvement**: -50-80% tail latency

---

## Benchmark Tools

### Test Latency với Percentiles
```bash
cd test
g++ -std=c++20 -O3 -march=native -o latency_bench latency_analysis_v2.cc -pthread
./latency_bench
```

### Comprehensive Comparison
```bash
cd test
g++ -std=c++20 -O3 -march=native -o comp_bench comprehensive_bench.cc -pthread
./comp_bench
```

---

## Performance Cheat Sheet

| Optimization | Impact | Effort | Priority |
|--------------|--------|--------|----------|
| Use SPSCQueueOPT | +15% throughput | 1 line | 🔴 High |
| Queue size 4K-16K | -85% p99 latency | 1 line | 🔴 High |
| System script | -20-30% variance | 1 command | 🟡 Medium |
| Kernel params | -50-80% tail lat | 5 min + reboot | 🟡 Medium |
| Match producer/consumer rate | -70-90% tail lat | Design dependent | 🟢 Low |

---

## Expected Performance

### Stock System (Laptop, no optimization)
- Throughput: 2 Mops/sec (throttled), 30 Mops/sec (max)
- p50 latency: 70-120 ns
- p99 latency: 100-1,700 ns
- p99.99 latency: 20-70 μs

### Optimized System (với script + queue size)
- Throughput: 2-3 Mops/sec (throttled), 30-40 Mops/sec (max)
- p50 latency: 60-80 ns
- p99 latency: 100-200 ns
- p99.99 latency: <10 μs

### Production System (kernel params + real-time)
- Throughput: 30-50 Mops/sec
- p50 latency: 50-70 ns
- p99 latency: 80-150 ns
- p99.99 latency: <5 μs

---

## FAQ

**Q: SPSCQueue hay SPSCQueueOPT?**
- **SPSCQueueOPT** cho throughput cao, low latency
- **SPSCQueue** nếu cần crash safety (shared memory)

**Q: Queue size bao nhiêu?**
- **4K-16K slots** cho best latency
- Larger = better (ít saturation)

**Q: Tại sao tail latency cao?**
- Queue saturation (producer nhanh hơn consumer)
- OS interrupts
- CPU frequency scaling
- Scheduler preemption

**Q: Làm sao giảm outliers?**
1. Increase queue size
2. Throttle producer
3. Apply system optimizations
4. Use real-time scheduling

---

## Code Examples

### Basic Usage
```cpp
#include "SPSCQueueOPT.h"

SPSCQueueOPT<int, 4096> queue;

// Producer
void producer() {
    int* slot = queue.alloc();
    if (slot) {
        *slot = 42;
        queue.push();
    }
}

// Consumer
void consumer() {
    int* slot = queue.front();
    if (slot) {
        int value = *slot;
        queue.pop();
    }
}
```

### With CPU Pinning
```cpp
#include "cpupin.h"

void producer() {
    cpupin(6);  // Pin to CPU 6
    // ... produce messages
}

void consumer() {
    cpupin(7);  // Pin to CPU 7
    // ... consume messages
}
```

### With Real-Time Priority
```cpp
#include <sched.h>
#include <sys/mman.h>

void setup_realtime() {
    // Lock memory
    mlockall(MCL_CURRENT | MCL_FUTURE);

    // Set SCHED_FIFO
    struct sched_param param;
    param.sched_priority = 99;
    sched_setscheduler(0, SCHED_FIFO, &param);
}
```

---

## Troubleshooting

**High p99.99 latency (>50 μs)?**
- Check queue size (increase to 4K+)
- Check if queue is saturating
- Apply system optimizations

**Inconsistent latency?**
- Check CPU frequency governor (should be 'performance')
- Check HT siblings (should be disabled)
- Check IRQ affinity

**Low throughput (<10 Mops/sec)?**
- Check if using SPSCQueueOPT
- Check CPU pinning
- Check for memory allocation in hot path

---

## Next Steps

1. Run [comprehensive_bench.cc](test/comprehensive_bench.cc) to baseline
2. Apply optimizations one by one
3. Re-run benchmark to measure impact
4. Read [OPTIMIZATION_RESULTS.md](OPTIMIZATION_RESULTS.md) for details

**Happy optimizing! 🚀**
