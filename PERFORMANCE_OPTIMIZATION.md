# Các Cách Tối Ưu Hóa Performance cho SPSCQueue

## 1. Code-Level Optimizations (Đã có trong repo)

### A. SPSCQueue vs SPSCQueueOPT

#### **SPSCQueue** (Original)
```cpp
// Sử dụng index counters
alignas(128) uint32_t write_idx = 0;
uint32_t read_idx_cach = 0;

T* alloc() {
    if (write_idx - read_idx_cach == CNT) {
        read_idx_cach = atomic_load(read_idx);  // Atomic load
        if (write_idx - read_idx_cach == CNT) return nullptr;
    }
    return &data[write_idx & mask];
}

void push() {
    atomic_store(write_idx, write_idx + 1);  // Atomic store index
}
```

**Đặc điểm**:
- ✅ Atomic operations trên indices
- ✅ Cache read_idx để giảm atomic loads
- ✅ An toàn khi crash (indices luôn consistent)

#### **SPSCQueueOPT** (Optimized)
```cpp
// Sử dụng per-slot availability flags
struct alignas(64) Block {
    bool avail = false;  // Per-slot flag
    T data;
} blk[CNT];

uint32_t free_write_cnt = CNT - 1;  // Cache free slots

T* alloc() {
    if (free_write_cnt == 0) {
        uint32_t rd_idx = atomic_load(read_idx, relaxed);  // RELAXED!
        free_write_cnt = calculate_free_slots();
        if (free_write_cnt == 0) return nullptr;
    }
    return &blk[write_idx].data;
}

void push() {
    atomic_store(blk[write_idx].avail, true, release);  // Flag per slot
    write_idx = (write_idx + 1) & mask;
    free_write_cnt--;
}
```

**Tối ưu hóa**:
1. **memory_order_relaxed** cho read_idx load (thay vì consume)
2. **Per-slot availability flags** → consumer không cần đọc write_idx mỗi lần
3. **Cache số lượng slots trống** (free_write_cnt)
4. **64-byte alignment** cho mỗi Block (cache line)

**Trade-off**:
- ⚠️ **Không an toàn khi crash** giữa chừng push/pop
- ⚠️ Tốn nhiều memory hơn (64 bytes/slot thay vì chỉ sizeof(T))

## 2. System-Level Optimizations

### A. CPU Affinity & Isolation 🔴 (Quan trọng nhất)

```bash
# Isolate CPUs khỏi OS scheduler
# Add to /etc/default/grub:
GRUB_CMDLINE_LINUX="isolcpus=6,7 nohz_full=6,7 rcu_nocbs=6,7"

# Update grub
sudo update-grub
sudo reboot
```

**Impact**: Giảm context switches và interrupts → giảm tail latency 50-80%

### B. CPU Frequency Scaling 🟡

```bash
# Set performance governor
for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    echo performance | sudo tee $cpu
done

# Disable turbo boost (để stable latency)
echo 1 | sudo tee /sys/devices/system/cpu/intel_pmu/allow_tsx_force_abort
```

**Impact**: Latency ổn định hơn, giảm frequency transition spikes

### C. Disable Hyper-Threading 🟠

```bash
# Disable HT cho CPUs 14, 15 (siblings của 6, 7)
echo 0 | sudo tee /sys/devices/system/cpu/cpu14/online
echo 0 | sudo tee /sys/devices/system/cpu/cpu15/online
```

**Impact**: Loại bỏ contention giữa HT siblings → giảm jitter 20-40%

### D. IRQ Affinity 🟡

```bash
# Move tất cả IRQs ra khỏi CPUs 6, 7
for irq in $(ls /proc/irq/); do
    if [ -f /proc/irq/$irq/smp_affinity ]; then
        # Set affinity to CPUs 0-5 (bitmask: 0x3F)
        echo "3f" | sudo tee /proc/irq/$irq/smp_affinity 2>/dev/null
    fi
done
```

**Impact**: Loại bỏ timer interrupts → giảm preemption

### E. Huge Pages 🟢 (Minor)

```bash
# Enable transparent huge pages
echo always | sudo tee /sys/kernel/mm/transparent_hugepage/enabled

# Pre-allocate huge pages
echo 128 | sudo tee /proc/sys/vm/nr_hugepages
```

**Impact**: Giảm TLB misses, nhưng effect nhỏ cho SPSC queue

### F. Real-Time Scheduling 🔴

```cpp
#include <sched.h>
#include <sys/mman.h>

void setup_realtime() {
    // Lock memory to prevent paging
    mlockall(MCL_CURRENT | MCL_FUTURE);

    // Set SCHED_FIFO priority
    struct sched_param param;
    param.sched_priority = 99;
    sched_setscheduler(0, SCHED_FIFO, &param);
}
```

**Impact**: Loại bỏ scheduler preemption → worst case latency giảm 90%

## 3. Algorithm-Level Optimizations

### A. Batch Processing

```cpp
// Thay vì push 1 message tại một thời điểm:
template<typename Writer>
size_t tryPushBatch(Writer* writers, size_t count) {
    size_t pushed = 0;
    for (size_t i = 0; i < count; i++) {
        T* p = alloc();
        if (!p) break;
        writers[i](p);
        push();
        pushed++;
    }
    return pushed;
}
```

**Impact**: Amortize atomic operations overhead

### B. Prefetching

```cpp
T* front() {
    if (read_idx == write_idx_cach) {
        write_idx_cach = atomic_load(write_idx);
        if (read_idx == write_idx_cach) return nullptr;

        // Prefetch next few slots
        __builtin_prefetch(&data[(read_idx + 1) & mask]);
        __builtin_prefetch(&data[(read_idx + 2) & mask]);
    }
    return &data[read_idx & mask];
}
```

**Impact**: Giảm cache miss latency 10-20%

### C. Adaptive Spinning vs Blocking

```cpp
T* front_adaptive() {
    constexpr int MAX_SPIN = 1000;
    int spin_count = 0;

    while (true) {
        T* msg = front();
        if (msg) return msg;

        if (spin_count++ < MAX_SPIN) {
            _mm_pause();  // CPU pause instruction
        } else {
            // Switch to futex-based blocking
            futex_wait(...);
            spin_count = 0;
        }
    }
}
```

**Impact**: Cân bằng giữa low latency và CPU usage

### D. Larger Queue Size

```cpp
// Thay vì 1024, dùng 64K hoặc 256K
typedef SPSCQueue<Msg, 1 << 16> MsgQ;  // 64K slots
```

**Impact**: Giảm queue saturation → giảm tail latency đáng kể

**Trade-off**: Tốn nhiều memory hơn

## 4. Compiler Optimizations

### A. PGO (Profile-Guided Optimization)

```bash
# Step 1: Compile with instrumentation
g++ -std=c++20 -O3 -fprofile-generate -o bench benchmark.cc

# Step 2: Run to collect profile
./bench

# Step 3: Recompile with profile data
g++ -std=c++20 -O3 -fprofile-use -o bench_pgo benchmark.cc
```

**Impact**: 5-15% throughput improvement

### B. LTO (Link-Time Optimization)

```bash
g++ -std=c++20 -O3 -flto -o bench benchmark.cc
```

**Impact**: Inline across translation units

### C. Target-Specific Optimizations

```bash
# Optimize for your specific CPU
g++ -std=c++20 -O3 -march=native -mtune=native -o bench benchmark.cc
```

**Impact**: Use SIMD and specific instructions

## 5. Benchmark Tool để So Sánh

Tôi sẽ tạo tool để test tất cả optimizations:
- SPSCQueue vs SPSCQueueOPT
- Các queue sizes khác nhau
- Với/không system optimizations
