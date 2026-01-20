/*
 * LMAX Disruptor Benchmark
 *
 * Compares latency and throughput with SPSCQueue.
 * Uses throttling and pre-allocated arrays for accurate measurement.
 */

#include <iostream>
#include <iomanip>
#include <thread>
#include <vector>
#include <algorithm>
#include <chrono>
#include <atomic>
#include <cstring>
#include <memory>
#include <emmintrin.h>

#include "../include/lmax/disruptor.h"

// Include SPSCQueue for comparison
#include "../../SPSCQueue.h"
#include "../../SPSCQueueOPT.h"

// RDTSC for precise timing
inline uint64_t rdtsc()
{
    uint32_t lo, hi;
    __asm__ volatile("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)hi << 32) | lo;
}

inline uint64_t rdtscp()
{
    uint32_t lo, hi;
    __asm__ volatile(
        "rdtscp"
        : "=a"(lo), "=d"(hi)
        :
        : "rcx", "memory");
    return (uint64_t(hi) << 32) | lo;
}

// CPU pinning (Linux)
#ifdef __linux__
#include <sched.h>
#include <pthread.h>

void pinThread(int cpu)
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}
#else
void pinThread(int) {}
#endif

// Spin wait for throttling
inline void spinWait(uint64_t cycles)
{
    uint64_t expire = rdtsc() + cycles;
    while (rdtsc() < expire)
    {
        // _mm_pause();
    }
}

struct Event
{
    uint64_t timestamp;
    uint64_t sequence;
    uint64_t padding[6]; // Pad to 64 bytes
};

// Calculate percentiles from pre-sorted array
struct LatencyResult
{
    uint64_t min, p50, p90, p99, p999, max;
    double mean;
    size_t count;
    size_t outliers; // Count of filtered outliers

    void calculate(uint64_t *data, size_t n, bool filterOutliers = false)
    {
        if (n == 0)
            return;

        std::sort(data, data + n);

        // Optionally filter OS noise outliers (>1µs @ 2.3GHz = ~2300 cycles)
        size_t effectiveN = n;
        outliers = 0;
        if (filterOutliers)
        {
            const uint64_t OUTLIER_THRESHOLD = 2300; // ~1µs
            while (effectiveN > 0 && data[effectiveN - 1] > OUTLIER_THRESHOLD)
            {
                effectiveN--;
                outliers++;
            }
        }

        count = effectiveN;
        if (effectiveN == 0)
            return;

        min = data[0];
        max = data[effectiveN - 1];
        p50 = data[effectiveN / 2];
        p90 = data[(size_t)(effectiveN * 0.90)];
        p99 = data[(size_t)(effectiveN * 0.99)];
        p999 = data[(size_t)(effectiveN * 0.999)];

        uint64_t sum = 0;
        for (size_t i = 0; i < effectiveN; i++)
            sum += data[i];
        mean = (double)sum / effectiveN;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// LMAX Disruptor Latency Benchmark
// ═══════════════════════════════════════════════════════════════════════════

// Version that returns raw latencies for external processing
template <typename WaitStrategy>
size_t benchmarkLMAXLatencyRaw(size_t numMessages, uint64_t throttleCycles,
                               int producerCpu, int consumerCpu, uint64_t *outLatencies)
{
    constexpr size_t BUFFER_SIZE = 65536;

    lmax::RingBuffer<Event, BUFFER_SIZE, WaitStrategy> ring;
    lmax::Sequence consumerSeq(lmax::INITIAL_CURSOR_VALUE);

    ring.addGatingSequence(consumerSeq);

    std::atomic<size_t> writeIdx{0};
    std::atomic<bool> consumerReady{false};

    // Consumer
    std::thread consumer([&]()
                         {
        pinThread(consumerCpu);
        const lmax::Sequence& cursor = ring.cursor();
        int64_t nextSeq = 0;
        size_t received = 0;
        size_t tmpWriteIdx = 0;

        consumerReady.store(true, std::memory_order_release);

        while (received < numMessages) {
            int64_t available = cursor.getRelaxed();
            if (nextSeq > available) {
                _mm_pause();
                continue;
            }
            std::atomic_thread_fence(std::memory_order_acquire);

            const Event* e = ring.get(nextSeq);
            uint64_t now = rdtscp();
            outLatencies[tmpWriteIdx++] = now - e->timestamp;
            consumerSeq.setRelaxed(nextSeq);
            nextSeq++;
            received++;
        }
        consumerSeq.set(nextSeq - 1);
        writeIdx.store(tmpWriteIdx, std::memory_order_relaxed); });

    while (!consumerReady.load(std::memory_order_acquire))
        _mm_pause();

    pinThread(producerCpu);
    for (size_t i = 0; i < numMessages; i++)
    {
        if (throttleCycles > 0)
            spinWait(throttleCycles);
        int64_t seq = ring.next();
        Event *e = ring.get(seq);
        e->sequence = seq;
        std::atomic_thread_fence(std::memory_order_release);
        e->timestamp = rdtscp();
        ring.publish(seq);
    }

    consumer.join();
    return writeIdx.load(std::memory_order_relaxed);
}

template <typename WaitStrategy>
LatencyResult benchmarkLMAXLatency(size_t numMessages, uint64_t throttleCycles,
                                   int producerCpu, int consumerCpu)
{
    constexpr size_t BUFFER_SIZE = 65536;
    constexpr size_t WARMUP_MESSAGES = 0; // Skip warmup for accurate measurement

    lmax::RingBuffer<Event, BUFFER_SIZE, WaitStrategy> ring;
    lmax::Sequence consumerSeq(lmax::INITIAL_CURSOR_VALUE);

    ring.addGatingSequence(consumerSeq);

    // Pre-allocate latency array
    std::unique_ptr<uint64_t[]> latencies(new uint64_t[numMessages]{0});
    std::atomic<size_t> writeIdx{0};
    std::atomic<bool> done{false};
    std::atomic<bool> consumerReady{false};

    // Consumer
    std::thread consumer([&]()
                         {
        pinThread(consumerCpu);

        // Direct access to cursor - avoid barrier overhead
        const lmax::Sequence& cursor = ring.cursor();
        int64_t nextSeq = 0;
        size_t received = 0;
        size_t tmpWriteIdx = 0;

        consumerReady.store(true, std::memory_order_release);
        int64_t availableCache = 0;
        while (received < numMessages) {
            // Use relaxed load first to poll
            int64_t available = cursor.getRelaxed();

            if (nextSeq > available) {
                // _mm_pause();
                continue;
            }

            // Data is available - acquire fence to see the data
            std::atomic_thread_fence(std::memory_order_acquire);

            // Process ONE message and update consumer sequence immediately
            // This avoids producer stalls at wrap point
            const Event* e = ring.get(nextSeq);
            uint64_t now = rdtscp();
            latencies[tmpWriteIdx++] = now - e->timestamp;

            // Update consumer sequence immediately with relaxed ordering
            // Producer only needs to see this eventually for backpressure
            consumerSeq.setRelaxed(nextSeq);

            nextSeq++;
            received++;
        }
        // Final update with release semantics
        consumerSeq.set(nextSeq - 1);
        writeIdx.store(tmpWriteIdx, std::memory_order_relaxed); });

    // Wait for consumer to be ready
    while (!consumerReady.load(std::memory_order_acquire))
    {
        _mm_pause();
    }

    // Producer with throttling
    pinThread(producerCpu);

    size_t totalMessages = numMessages + WARMUP_MESSAGES;
    for (size_t i = 0; i < totalMessages; i++)
    {
        if (throttleCycles > 0)
        {
            spinWait(throttleCycles);
        }

        int64_t seq = ring.next();
        Event *e = ring.get(seq);
        e->sequence = seq;
        // Use lfence before rdtscp to serialize all prior stores
        // This ensures the timestamp is taken AFTER data is written
        std::atomic_thread_fence(std::memory_order_release);
        e->timestamp = rdtscp();
        ring.publish(seq);
    }

    done.store(true, std::memory_order_relaxed);
    consumer.join();

    LatencyResult result{};
    size_t actualCount = std::min(writeIdx.load(std::memory_order_relaxed), numMessages);
    std::cout << "Actual count: " << actualCount << std::endl;
    result.calculate(latencies.get(), actualCount);
    return result;
}

// ═══════════════════════════════════════════════════════════════════════════
// SPSCQueue Latency Benchmark
// ═══════════════════════════════════════════════════════════════════════════

template <typename Queue>
LatencyResult benchmarkSPSCQueueLatency(Queue &queue, size_t numMessages, uint64_t throttleCycles,
                                        int producerCpu, int consumerCpu)
{
    constexpr size_t WARMUP_MESSAGES = 10000;

    std::unique_ptr<uint64_t[]> latencies(new uint64_t[numMessages]);
    std::atomic<size_t> latencyCount{0};
    std::atomic<bool> producerDone{false};
    std::atomic<bool> consumerReady{false};

    size_t totalMessages = numMessages + WARMUP_MESSAGES;

    // Consumer
    std::thread consumer([&]()
                         {
        pinThread(consumerCpu);
        size_t consumed = 0;

        consumerReady.store(true, std::memory_order_release);

        while (consumed < totalMessages) {
            Event* e = queue.front();
            if (e) {
                uint64_t now = rdtsc();

                // Only record after warmup
                if (consumed >= WARMUP_MESSAGES && e->timestamp > 0 && now > e->timestamp) {
                    uint64_t latency = now - e->timestamp;
                    // Filter obvious outliers
                    if (latency < 10000000) {
                        size_t idx = latencyCount.fetch_add(1, std::memory_order_relaxed);
                        if (idx < numMessages) {
                            latencies[idx] = latency;
                        }
                    }
                }

                queue.pop();
                consumed++;
            }
        } });

    // Wait for consumer to be ready
    while (!consumerReady.load(std::memory_order_acquire))
    {
        _mm_pause();
    }

    // Producer with throttling
    pinThread(producerCpu);

    for (size_t i = 0; i < totalMessages; i++)
    {
        if (throttleCycles > 0)
        {
            spinWait(throttleCycles);
        }

        while (true)
        {
            Event *e = queue.alloc();
            if (e)
            {
                e->timestamp = rdtsc();
                e->sequence = i;
                queue.push();
                break;
            }
            _mm_pause();
        }
    }

    producerDone.store(true, std::memory_order_relaxed);
    consumer.join();

    LatencyResult result{};
    size_t actualCount = latencyCount.load();
    if (actualCount > 0 && actualCount <= numMessages)
    {
        result.calculate(latencies.get(), actualCount);
    }
    return result;
}

// ═══════════════════════════════════════════════════════════════════════════
// Throughput Benchmark
// ═══════════════════════════════════════════════════════════════════════════

template <typename WaitStrategy>
double benchmarkLMAXThroughput(size_t numMessages, int producerCpu, int consumerCpu)
{
    constexpr size_t BUFFER_SIZE = 65536;
    lmax::RingBuffer<Event, BUFFER_SIZE, WaitStrategy> ring;
    lmax::Sequence consumerSeq(lmax::INITIAL_CURSOR_VALUE);

    ring.addGatingSequence(consumerSeq);

    std::atomic<bool> done{false};

    auto start = std::chrono::high_resolution_clock::now();

    // Consumer
    std::thread consumer([&]()
                         {
        pinThread(consumerCpu);

        auto barrier = ring.newBarrier();
        int64_t nextSeq = 0;

        while (!done.load(std::memory_order_relaxed) || nextSeq <= ring.getCursor()) {
            int64_t available = barrier.available();

            while (nextSeq <= available) {
                [[maybe_unused]] volatile const Event* e = ring.get(nextSeq);
                nextSeq++;
            }

            if (nextSeq > 0) {
                consumerSeq.set(nextSeq - 1);
            }
        } });

    // Producer (no throttling)
    pinThread(producerCpu);

    for (size_t i = 0; i < numMessages; i++)
    {
        int64_t seq = ring.next();
        Event *e = ring.get(seq);
        e->sequence = i;
        ring.publish(seq);
    }

    done.store(true, std::memory_order_relaxed);
    consumer.join();

    auto end = std::chrono::high_resolution_clock::now();
    double seconds = std::chrono::duration<double>(end - start).count();

    return numMessages / seconds / 1e6;
}

template <typename Queue>
double benchmarkSPSCQueueThroughput(Queue &queue, size_t numMessages, int producerCpu, int consumerCpu)
{
    std::atomic<bool> producerDone{false};

    auto start = std::chrono::high_resolution_clock::now();

    // Consumer
    std::thread consumer([&]()
                         {
        pinThread(consumerCpu);
        size_t consumed = 0;

        while (consumed < numMessages) {
            Event* e = queue.front();
            if (e) {
                queue.pop();
                consumed++;
            }
        } });

    // Producer
    pinThread(producerCpu);

    for (size_t i = 0; i < numMessages; i++)
    {
        while (true)
        {
            Event *e = queue.alloc();
            if (e)
            {
                e->sequence = i;
                queue.push();
                break;
            }
            _mm_pause();
        }
    }

    producerDone.store(true, std::memory_order_relaxed);
    consumer.join();

    auto end = std::chrono::high_resolution_clock::now();
    double seconds = std::chrono::duration<double>(end - start).count();

    return numMessages / seconds / 1e6;
}

void printLatency(const char *name, const LatencyResult &r, double cpuGhz)
{
    auto toNs = [cpuGhz](uint64_t cycles) -> int64_t
    {
        return static_cast<int64_t>(cycles / cpuGhz);
    };

    std::cout << std::setw(25) << name << ": "
              << "min=" << std::setw(5) << toNs(r.min) << "ns, "
              << "p50=" << std::setw(5) << toNs(r.p50) << "ns, "
              << "p90=" << std::setw(5) << toNs(r.p90) << "ns, "
              << "p99=" << std::setw(6) << toNs(r.p99) << "ns, "
              << "p99.9=" << std::setw(7) << toNs(r.p999) << "ns, "
              << "max=" << std::setw(8) << toNs(r.max) << "ns";
    if (r.outliers > 0)
    {
        std::cout << " (filtered " << r.outliers << " OS outliers >1µs)";
    }
    std::cout << "\n";
}

int main()
{
    constexpr size_t NUM_MESSAGES = 5000000;
    constexpr size_t THROUGHPUT_MESSAGES = 10000000;
    constexpr int PRODUCER_CPU = 2;
    constexpr int CONSUMER_CPU = 3;
    constexpr double CPU_GHZ = 2.3;            // Adjust for your CPU
    constexpr uint64_t THROTTLE_CYCLES = 1000; // ~430ns @ 2.3GHz

    std::cout << "═══════════════════════════════════════════════════════════════════════════\n";
    std::cout << "                    LMAX Disruptor C++ Benchmark\n";
    std::cout << "═══════════════════════════════════════════════════════════════════════════\n\n";

    std::cout << "Configuration:\n";
    std::cout << "  Messages (latency): " << NUM_MESSAGES << "\n";
    std::cout << "  Messages (throughput): " << THROUGHPUT_MESSAGES << "\n";
    std::cout << "  Producer CPU: " << PRODUCER_CPU << ", Consumer CPU: " << CONSUMER_CPU << "\n";
    std::cout << "  CPU freq: " << CPU_GHZ << " GHz\n";
    std::cout << "  Throttle: " << THROTTLE_CYCLES << " cycles (~" << (int)(THROTTLE_CYCLES / CPU_GHZ) << "ns)\n\n";

    // Warmup
    std::cout << "Warming up...\n";
    benchmarkLMAXLatency<lmax::BusySpinWait>(10000, THROTTLE_CYCLES, PRODUCER_CPU, CONSUMER_CPU);

    std::cout << "\n═══════════════════════════════════════════════════════════════════════════\n";
    std::cout << "                      LATENCY COMPARISON (throttled)\n";
    std::cout << "═══════════════════════════════════════════════════════════════════════════\n\n";

    // benchmarkLMAXLatencyRaw - raw + filtered
    {
        std::unique_ptr<uint64_t[]> latencies(new uint64_t[NUM_MESSAGES]);
        auto lmaxBusy = benchmarkLMAXLatencyRaw<lmax::BusySpinWait>(NUM_MESSAGES, THROTTLE_CYCLES, PRODUCER_CPU, CONSUMER_CPU, latencies.get());

        LatencyResult raw{};
        raw.calculate(latencies.get(), lmaxBusy, false);
        printLatency("LMAX BusySpinWait (raw)", raw, CPU_GHZ);

        // Re-calculate with filtering (copy data since it's sorted)
        std::unique_ptr<uint64_t[]> latenciesCopy(new uint64_t[NUM_MESSAGES]);
        auto lmaxBusy2 = benchmarkLMAXLatencyRaw<lmax::BusySpinWait>(NUM_MESSAGES, THROTTLE_CYCLES, PRODUCER_CPU, CONSUMER_CPU, latenciesCopy.get());
        LatencyResult filtered{};
        filtered.calculate(latenciesCopy.get(), lmaxBusy2, true);
        printLatency("LMAX BusySpin (filtered)", filtered, CPU_GHZ);
    }
    std::cout << "\n";

    // benchmarkLMAXLatency - other wait strategies
    auto lmaxBusy = benchmarkLMAXLatency<lmax::BusySpinWait>(NUM_MESSAGES, THROTTLE_CYCLES, PRODUCER_CPU, CONSUMER_CPU);
    printLatency("LMAX BusyWait", lmaxBusy, CPU_GHZ);

    auto lmaxYield = benchmarkLMAXLatency<lmax::YieldingWait>(NUM_MESSAGES, THROTTLE_CYCLES, PRODUCER_CPU, CONSUMER_CPU);
    printLatency("LMAX YieldingWait", lmaxYield, CPU_GHZ);

    for (int i = 0; i < 10; i++)
    {
        auto lmaxLite = benchmarkLMAXLatency<lmax::LiteWait>(NUM_MESSAGES, THROTTLE_CYCLES, PRODUCER_CPU, CONSUMER_CPU);
        printLatency("LMAX LiteWait", lmaxLite, CPU_GHZ);
    }

    auto lmaxNoWait = benchmarkLMAXLatency<lmax::NoWait>(NUM_MESSAGES, THROTTLE_CYCLES, PRODUCER_CPU, CONSUMER_CPU);
    printLatency("LMAX NoWait", lmaxNoWait, CPU_GHZ);

    std::cout << "\n═══════════════════════════════════════════════════════════════════════════\n";
    std::cout << "                    THROUGHPUT COMPARISON (no throttle)\n";
    std::cout << "═══════════════════════════════════════════════════════════════════════════\n\n";

    double lmaxT1 = benchmarkLMAXThroughput<lmax::BusySpinWait>(THROUGHPUT_MESSAGES, PRODUCER_CPU, CONSUMER_CPU);
    std::cout << std::setw(25) << "LMAX BusySpinWait" << ": " << std::fixed << std::setprecision(2) << lmaxT1 << " M ops/sec\n";

    double lmaxT2 = benchmarkLMAXThroughput<lmax::YieldingWait>(THROUGHPUT_MESSAGES, PRODUCER_CPU, CONSUMER_CPU);
    std::cout << std::setw(25) << "LMAX YieldingWait" << ": " << std::fixed << std::setprecision(2) << lmaxT2 << " M ops/sec\n";

    std::cout << "\n═══════════════════════════════════════════════════════════════════════════\n";

    return 0;
}
