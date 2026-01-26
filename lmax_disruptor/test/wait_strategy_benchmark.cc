/*
 * Wait Strategy Benchmark
 *
 * Measures latency and throughput using BatchEventProcessor with different wait strategies.
 */

#include <iostream>
#include <iomanip>
#include <thread>
#include <vector>
#include <algorithm>
#include <chrono>
#include <atomic>
#include <memory>
#include <emmintrin.h>

#include "../include/lmax/disruptor.h"

// RDTSC for precise timing
inline uint64_t rdtscp() {
    uint32_t lo, hi;
    __asm__ volatile("rdtscp" : "=a"(lo), "=d"(hi) : : "rcx", "memory");
    return (uint64_t(hi) << 32) | lo;
}

// CPU pinning (Linux)
#ifdef __linux__
#include <sched.h>
#include <pthread.h>
void pinThread(int cpu) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}
#else
void pinThread(int) {}
#endif

inline void spinWait(uint64_t cycles) {
    uint64_t expire = rdtscp() + cycles;
    while (rdtscp() < expire) {
        _mm_pause();
    }
}

struct Event {
    uint64_t timestamp;
    uint64_t sequence;
    uint64_t padding[6];
};

struct LatencyStats {
    uint64_t min, p50, p90, p99, p999, max;
    size_t count;

    void calculate(uint64_t* data, size_t n) {
        if (n == 0) return;
        count = n;
        std::sort(data, data + n);
        min = data[0];
        max = data[n - 1];
        p50 = data[n / 2];
        p90 = data[(size_t)(n * 0.90)];
        p99 = data[(size_t)(n * 0.99)];
        p999 = data[(size_t)(n * 0.999)];
    }
};

// Event handler that records latencies
class LatencyHandler : public lmax::EventHandler<Event> {
public:
    uint64_t* latencies;
    size_t count = 0;
    size_t maxCount;
    int consumerCpu;

    LatencyHandler(uint64_t* lat, size_t max, int cpu)
        : latencies(lat), maxCount(max), consumerCpu(cpu) {}

    void onStart() override {
        pinThread(consumerCpu);
    }

    void onEvent(Event& event, int64_t sequence, bool endOfBatch) override {
        if (count < maxCount) {
            uint64_t now = rdtscp();
            latencies[count++] = now - event.timestamp;
        }
    }
};

// Event handler for throughput (no latency recording)
class ThroughputHandler : public lmax::EventHandler<Event> {
public:
    std::atomic<size_t> count{0};
    int consumerCpu;

    ThroughputHandler(int cpu) : consumerCpu(cpu) {}

    void onStart() override {
        pinThread(consumerCpu);
    }

    void onEvent(Event& event, int64_t sequence, bool endOfBatch) override {
        count.fetch_add(1, std::memory_order_relaxed);
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Latency Benchmark using BatchEventProcessor
// ═══════════════════════════════════════════════════════════════════════════

template<typename WaitStrategy>
LatencyStats benchmarkLatency(size_t numMessages, uint64_t throttleCycles,
                               int producerCpu, int consumerCpu) {
    constexpr size_t BUFFER_SIZE = 65536;

    using RingBufferType = lmax::RingBuffer<Event, BUFFER_SIZE, WaitStrategy>;
    RingBufferType ring;

    std::unique_ptr<uint64_t[]> latencies(new uint64_t[numMessages]);
    LatencyHandler handler(latencies.get(), numMessages, consumerCpu);

    auto barrier = ring.newBarrier();
    lmax::BatchEventProcessor<Event, RingBufferType, decltype(barrier)>
        processor(ring, barrier, handler);

    ring.addGatingSequence(processor.getSequence());
    processor.start();

    // Wait for processor to start
    while (!processor.isRunning()) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    // Producer
    pinThread(producerCpu);

    for (size_t i = 0; i < numMessages; i++) {
        if (throttleCycles > 0) {
            spinWait(throttleCycles);
        }

        int64_t seq = ring.next();
        Event* e = ring.get(seq);
        e->sequence = seq;
        std::atomic_thread_fence(std::memory_order_release);
        e->timestamp = rdtscp();
        ring.publish(seq);
    }

    // Wait for all messages to be processed
    while (handler.count < numMessages) {
        std::this_thread::yield();
    }

    processor.halt();
    processor.join();

    LatencyStats stats{};
    stats.calculate(latencies.get(), handler.count);
    return stats;
}

// ═══════════════════════════════════════════════════════════════════════════
// Throughput Benchmark using BatchEventProcessor
// ═══════════════════════════════════════════════════════════════════════════

template<typename WaitStrategy>
double benchmarkThroughput(size_t numMessages, int producerCpu, int consumerCpu) {
    constexpr size_t BUFFER_SIZE = 65536;

    using RingBufferType = lmax::RingBuffer<Event, BUFFER_SIZE, WaitStrategy>;
    RingBufferType ring;

    ThroughputHandler handler(consumerCpu);

    auto barrier = ring.newBarrier();
    lmax::BatchEventProcessor<Event, RingBufferType, decltype(barrier)>
        processor(ring, barrier, handler);

    ring.addGatingSequence(processor.getSequence());
    processor.start();

    while (!processor.isRunning()) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    pinThread(producerCpu);

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < numMessages; i++) {
        int64_t seq = ring.next();
        Event* e = ring.get(seq);
        e->sequence = i;
        ring.publish(seq);
    }

    while (handler.count.load(std::memory_order_relaxed) < numMessages) {
        std::this_thread::yield();
    }

    auto end = std::chrono::high_resolution_clock::now();

    processor.halt();
    processor.join();

    double seconds = std::chrono::duration<double>(end - start).count();
    return numMessages / seconds / 1e6;
}

void printLatency(const char* name, const LatencyStats& s, double cpuGhz) {
    auto toNs = [cpuGhz](uint64_t cycles) -> int64_t {
        return static_cast<int64_t>(cycles / cpuGhz);
    };

    std::cout << std::setw(18) << name << ": "
              << "min=" << std::setw(4) << toNs(s.min) << "ns  "
              << "p50=" << std::setw(4) << toNs(s.p50) << "ns  "
              << "p90=" << std::setw(4) << toNs(s.p90) << "ns  "
              << "p99=" << std::setw(5) << toNs(s.p99) << "ns  "
              << "p99.9=" << std::setw(6) << toNs(s.p999) << "ns  "
              << "max=" << std::setw(7) << toNs(s.max) << "ns\n";
}

void printThroughput(const char* name, double mops) {
    std::cout << std::setw(18) << name << ": "
              << std::fixed << std::setprecision(2) << mops << " M ops/sec\n";
}

int main(int argc, char* argv[]) {
    size_t numMessages = 1000000;
    size_t throughputMessages = 10000000;
    int producerCpu = 2;
    int consumerCpu = 3;
    double cpuGhz = 2.9;
    uint64_t throttleCycles = 1000;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--messages" && i + 1 < argc) numMessages = std::stoull(argv[++i]);
        else if (arg == "--cpu-ghz" && i + 1 < argc) cpuGhz = std::stod(argv[++i]);
        else if (arg == "--throttle" && i + 1 < argc) throttleCycles = std::stoull(argv[++i]);
        else if (arg == "--producer-cpu" && i + 1 < argc) producerCpu = std::stoi(argv[++i]);
        else if (arg == "--consumer-cpu" && i + 1 < argc) consumerCpu = std::stoi(argv[++i]);
    }

    std::cout << "═══════════════════════════════════════════════════════════════════════════\n";
    std::cout << "           Wait Strategy Benchmark (BatchEventProcessor)\n";
    std::cout << "═══════════════════════════════════════════════════════════════════════════\n\n";

    std::cout << "Configuration:\n";
    std::cout << "  Messages (latency):    " << numMessages << "\n";
    std::cout << "  Messages (throughput): " << throughputMessages << "\n";
    std::cout << "  Producer CPU: " << producerCpu << ", Consumer CPU: " << consumerCpu << "\n";
    std::cout << "  CPU freq: " << cpuGhz << " GHz\n";
    std::cout << "  Throttle: " << throttleCycles << " cycles (~" << (int)(throttleCycles / cpuGhz) << "ns)\n\n";

    // Warmup
    std::cout << "Warming up...\n\n";
    benchmarkLatency<lmax::BusySpinWait>(10000, throttleCycles, producerCpu, consumerCpu);

    // ═══════════════════════════════════════════════════════════════════════════
    std::cout << "═══════════════════════════════════════════════════════════════════════════\n";
    std::cout << "                         LATENCY (throttled)\n";
    std::cout << "═══════════════════════════════════════════════════════════════════════════\n\n";

    auto busySpin = benchmarkLatency<lmax::BusySpinWait>(numMessages, throttleCycles, producerCpu, consumerCpu);
    printLatency("BusySpinWait", busySpin, cpuGhz);

    auto yielding = benchmarkLatency<lmax::YieldingWait>(numMessages, throttleCycles, producerCpu, consumerCpu);
    printLatency("YieldingWait", yielding, cpuGhz);

    auto sleeping = benchmarkLatency<lmax::SleepingWait>(numMessages, throttleCycles, producerCpu, consumerCpu);
    printLatency("SleepingWait", sleeping, cpuGhz);

    auto backoff = benchmarkLatency<lmax::BackoffWait>(numMessages, throttleCycles, producerCpu, consumerCpu);
    printLatency("BackoffWait", backoff, cpuGhz);

    auto lite = benchmarkLatency<lmax::LiteWait>(numMessages, throttleCycles, producerCpu, consumerCpu);
    printLatency("LiteWait", lite, cpuGhz);

    auto nowait = benchmarkLatency<lmax::NoWait>(numMessages, throttleCycles, producerCpu, consumerCpu);
    printLatency("NoWait", nowait, cpuGhz);

    // ═══════════════════════════════════════════════════════════════════════════
    std::cout << "\n═══════════════════════════════════════════════════════════════════════════\n";
    std::cout << "                      THROUGHPUT (no throttle)\n";
    std::cout << "═══════════════════════════════════════════════════════════════════════════\n\n";

    double t1 = benchmarkThroughput<lmax::BusySpinWait>(throughputMessages, producerCpu, consumerCpu);
    printThroughput("BusySpinWait", t1);

    double t2 = benchmarkThroughput<lmax::YieldingWait>(throughputMessages, producerCpu, consumerCpu);
    printThroughput("YieldingWait", t2);

    double t3 = benchmarkThroughput<lmax::SleepingWait>(throughputMessages, producerCpu, consumerCpu);
    printThroughput("SleepingWait", t3);

    double t4 = benchmarkThroughput<lmax::BackoffWait>(throughputMessages, producerCpu, consumerCpu);
    printThroughput("BackoffWait", t4);

    double t5 = benchmarkThroughput<lmax::LiteWait>(throughputMessages, producerCpu, consumerCpu);
    printThroughput("LiteWait", t5);

    double t6 = benchmarkThroughput<lmax::NoWait>(throughputMessages, producerCpu, consumerCpu);
    printThroughput("NoWait", t6);

    // ═══════════════════════════════════════════════════════════════════════════
    std::cout << "\n═══════════════════════════════════════════════════════════════════════════\n";
    std::cout << "                              SUMMARY\n";
    std::cout << "═══════════════════════════════════════════════════════════════════════════\n\n";

    std::cout << "Recommendation:\n";
    std::cout << "  - Ultra-low latency: BusySpinWait (dedicated cores, highest CPU)\n";
    std::cout << "  - Balanced:          YieldingWait (good latency, lower CPU)\n";
    std::cout << "  - CPU-friendly:      SleepingWait or BackoffWait (lowest CPU)\n";

    std::cout << "\n═══════════════════════════════════════════════════════════════════════════\n";

    return 0;
}
