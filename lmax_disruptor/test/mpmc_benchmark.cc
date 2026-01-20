/*
 * LMAX Disruptor Multi-Producer Benchmark
 *
 * Compares:
 * - Single Producer vs Multi Producer throughput
 * - Pipeline throughput
 * - Batch processor performance
 * - Diamond pattern (1P -> 2C parallel -> 1C final)
 * - Latency measurements
 */

#include <iostream>
#include <iomanip>
#include <thread>
#include <vector>
#include <algorithm>
#include <chrono>
#include <atomic>
#include <numeric>
#include <emmintrin.h>
#include <cmath>

#include "../include/lmax/disruptor.h"

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
    __asm__ volatile("rdtscp" : "=a"(lo), "=d"(hi) : : "rcx", "memory");
    return (uint64_t(hi) << 32) | lo;
}

// CPU pinning (Linux)
#ifdef __linux__
#include <sched.h>
#include <pthread.h>

void pinThread(int cpu)
{
    if (cpu < 0)
        return;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}
#else
void pinThread(int) {}
#endif

struct Event
{
    uint64_t timestamp;
    uint64_t sequence;
    uint64_t producerId;
    uint64_t padding[5]; // Pad to 64 bytes
};

// ═══════════════════════════════════════════════════════════════════════════
// Single Producer Throughput
// ═══════════════════════════════════════════════════════════════════════════

double benchmarkSPSCThroughput(size_t numMessages)
{
    constexpr size_t BUFFER_SIZE = 65536;
    lmax::RingBuffer<Event, BUFFER_SIZE> ring;
    lmax::Sequence consumerSeq;

    ring.addGatingSequence(consumerSeq);

    std::atomic<bool> done{false};

    auto start = std::chrono::high_resolution_clock::now();

    // Consumer
    std::thread consumer([&]()
                         {
        int64_t nextSeq = 0;

        while (nextSeq < (int64_t)numMessages) {
            int64_t available = ring.available();
            if (available < nextSeq) {
                _mm_pause();
                continue;
            }
            while (nextSeq <= available && nextSeq < (int64_t)numMessages) {
                [[maybe_unused]] volatile const Event* e = ring.get(nextSeq);
                nextSeq++;
            }
            consumerSeq.set(nextSeq - 1);
        }
        done.store(true, std::memory_order_release); });

    // Producer
    for (size_t i = 0; i < numMessages; i++)
    {
        int64_t seq = ring.next();
        Event *e = ring.get(seq);
        e->sequence = i;
        e->producerId = 0;
        ring.publish(seq);
    }

    consumer.join();

    auto end = std::chrono::high_resolution_clock::now();
    double seconds = std::chrono::duration<double>(end - start).count();

    return numMessages / seconds / 1e6;
}

// ═══════════════════════════════════════════════════════════════════════════
// Multi Producer Throughput
// ═══════════════════════════════════════════════════════════════════════════

double benchmarkMPMCThroughput(size_t numMessages, int numProducers)
{
    constexpr size_t BUFFER_SIZE = 65536;
    lmax::MPMCRingBuffer<Event, BUFFER_SIZE> ring;
    lmax::Sequence consumerSeq;

    ring.addGatingSequence(consumerSeq);

    std::atomic<bool> startFlag{false};
    std::atomic<int> producersReady{0};
    std::atomic<uint64_t> consumed{0};

    size_t messagesPerProducer = numMessages / numProducers;

    auto start = std::chrono::high_resolution_clock::now();

    // Consumer - simple polling loop
    std::thread consumer([&]()
                         {
        int64_t nextSeq = 0;

        while (consumed.load(std::memory_order_relaxed) < numMessages) {
            int64_t cursor = ring.getCursor();
            if (cursor < nextSeq) {
                _mm_pause();
                continue;
            }

            // For multi-producer, check contiguous availability
            int64_t available = ring.getHighestPublishedSequence(nextSeq, cursor);
            if (available < nextSeq) {
                _mm_pause();
                continue;
            }

            while (nextSeq <= available) {
                [[maybe_unused]] volatile const Event* e = ring.get(nextSeq);
                consumed.fetch_add(1, std::memory_order_relaxed);
                nextSeq++;
            }

            consumerSeq.set(nextSeq - 1);
        } });

    // Producers
    std::vector<std::thread> producers;
    for (int p = 0; p < numProducers; p++)
    {
        producers.emplace_back([&, p, messagesPerProducer]()
                               {
            producersReady.fetch_add(1, std::memory_order_relaxed);

            while (!startFlag.load(std::memory_order_acquire)) {
                _mm_pause();
            }

            for (size_t i = 0; i < messagesPerProducer; i++) {
                int64_t seq = ring.next();
                Event* e = ring.get(seq);
                e->sequence = i;
                e->producerId = p;
                ring.publish(seq);
            } });
    }

    // Wait for all producers to be ready
    while (producersReady.load(std::memory_order_relaxed) < numProducers)
    {
        _mm_pause();
    }

    // Start producers
    startFlag.store(true, std::memory_order_release);

    // Wait for completion
    for (auto &t : producers)
    {
        t.join();
    }
    consumer.join();

    auto end = std::chrono::high_resolution_clock::now();
    double seconds = std::chrono::duration<double>(end - start).count();

    return consumed.load() / seconds / 1e6;
}

// ═══════════════════════════════════════════════════════════════════════════
// Pipeline Throughput (WAL -> Replicator -> MatchingEngine)
// ═══════════════════════════════════════════════════════════════════════════

double benchmarkPipelineThroughput(size_t numMessages)
{
    constexpr size_t BUFFER_SIZE = 65536;
    lmax::RingBuffer<Event, BUFFER_SIZE> ring;

    lmax::Sequence walSeq;
    lmax::Sequence replSeq;
    lmax::Sequence meSeq;

    ring.addGatingSequence(meSeq);

    std::atomic<uint64_t> meProcessed{0};

    auto start = std::chrono::high_resolution_clock::now();

    // WAL thread
    std::thread walThread([&]()
                          {
        int64_t nextSeq = 0;
        while (nextSeq < (int64_t)numMessages) {
            int64_t available = ring.available();
            if (available < nextSeq) {
                _mm_pause();
                continue;
            }
            while (nextSeq <= available && nextSeq < (int64_t)numMessages) {
                [[maybe_unused]] volatile const Event* e = ring.get(nextSeq);
                nextSeq++;
            }
            walSeq.set(nextSeq - 1);
        } });

    // Replicator thread
    std::thread replThread([&]()
                           {
        int64_t nextSeq = 0;
        while (nextSeq < (int64_t)numMessages) {
            int64_t available = walSeq.get();
            if (available < nextSeq) {
                _mm_pause();
                continue;
            }
            while (nextSeq <= available && nextSeq < (int64_t)numMessages) {
                [[maybe_unused]] volatile const Event* e = ring.get(nextSeq);
                nextSeq++;
            }
            replSeq.set(nextSeq - 1);
        } });

    // Matching Engine thread
    std::thread meThread([&]()
                         {
        int64_t nextSeq = 0;
        while (nextSeq < (int64_t)numMessages) {
            int64_t available = replSeq.get();
            if (available < nextSeq) {
                _mm_pause();
                continue;
            }
            while (nextSeq <= available && nextSeq < (int64_t)numMessages) {
                [[maybe_unused]] volatile const Event* e = ring.get(nextSeq);
                meProcessed.fetch_add(1, std::memory_order_relaxed);
                nextSeq++;
            }
            meSeq.set(nextSeq - 1);
        } });

    // Producer
    for (size_t i = 0; i < numMessages; i++)
    {
        int64_t seq = ring.next();
        Event *e = ring.get(seq);
        e->sequence = i;
        ring.publish(seq);
    }

    // Wait for pipeline to complete
    walThread.join();
    replThread.join();
    meThread.join();

    auto end = std::chrono::high_resolution_clock::now();
    double seconds = std::chrono::duration<double>(end - start).count();

    return numMessages / seconds / 1e6;
}

// ═══════════════════════════════════════════════════════════════════════════
// Batch Event Processor Throughput
// ═══════════════════════════════════════════════════════════════════════════

class BenchmarkHandler : public lmax::EventHandler<Event>
{
public:
    std::atomic<uint64_t> count{0};
    std::atomic<int> batchCount{0};

    void onEvent(Event & /*event*/, int64_t /*sequence*/, bool endOfBatch) override
    {
        count.fetch_add(1, std::memory_order_relaxed);
        if (endOfBatch)
        {
            batchCount.fetch_add(1, std::memory_order_relaxed);
        }
    }
};

double benchmarkBatchProcessorThroughput(size_t numMessages, int &outBatches)
{
    constexpr size_t BUFFER_SIZE = 65536;
    lmax::RingBuffer<Event, BUFFER_SIZE> ring;

    auto barrier = ring.newBarrier();
    BenchmarkHandler handler;

    lmax::BatchEventProcessor<Event, decltype(ring), decltype(barrier)>
        processor(ring, barrier, handler);

    ring.addGatingSequence(processor.getSequence());

    auto start = std::chrono::high_resolution_clock::now();

    // Start processor
    processor.start();

    // Producer
    for (size_t i = 0; i < numMessages; i++)
    {
        int64_t seq = ring.next();
        Event *e = ring.get(seq);
        e->sequence = i;
        ring.publish(seq);
    }

    // Wait for processor with timeout
    auto timeout = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (handler.count.load(std::memory_order_relaxed) < numMessages)
    {
        if (std::chrono::steady_clock::now() > timeout)
        {
            std::cerr << "Timeout! Processed: " << handler.count.load() << "/" << numMessages << "\n";
            break;
        }
        std::this_thread::yield();
    }

    processor.halt();
    processor.join();

    auto end = std::chrono::high_resolution_clock::now();
    double seconds = std::chrono::duration<double>(end - start).count();

    outBatches = handler.batchCount.load();
    return handler.count.load() / seconds / 1e6;
}

// ═══════════════════════════════════════════════════════════════════════════
// Latency Statistics
// ═══════════════════════════════════════════════════════════════════════════

// Spin wait for throttling (prevents producer from overwhelming consumer)
inline void spinWait(uint64_t cycles)
{
    uint64_t expire = rdtsc() + cycles;
    while (rdtsc() < expire)
    {
        // busy spin
    }
}

struct LatencyStats
{
    double min;
    double max;
    double mean;
    double p50;
    double p90;
    double p99;
    double p999;
    double p9999;
};

LatencyStats calculateLatencyStats(std::vector<uint64_t> &latencies, double cyclesPerNs)
{
    if (latencies.empty())
    {
        return {0, 0, 0, 0, 0, 0, 0, 0};
    }

    std::sort(latencies.begin(), latencies.end());

    size_t n = latencies.size();
    double sum = 0;
    for (auto l : latencies)
    {
        sum += l;
    }

    auto toNs = [cyclesPerNs](uint64_t cycles)
    {
        return cycles / cyclesPerNs;
    };

    return {
        toNs(latencies.front()),
        toNs(latencies.back()),
        toNs(static_cast<uint64_t>(sum / n)),
        toNs(latencies[n * 50 / 100]),
        toNs(latencies[n * 90 / 100]),
        toNs(latencies[n * 99 / 100]),
        toNs(latencies[std::min(n - 1, n * 999 / 1000)]),
        toNs(latencies[std::min(n - 1, n * 9999 / 10000)])};
}

double estimateCyclesPerNs()
{
    auto start = std::chrono::high_resolution_clock::now();
    uint64_t startCycles = rdtsc();

    // Spin for ~10ms
    while (std::chrono::high_resolution_clock::now() - start < std::chrono::milliseconds(10))
    {
        _mm_pause();
    }

    uint64_t endCycles = rdtsc();
    auto end = std::chrono::high_resolution_clock::now();

    double ns = std::chrono::duration<double, std::nano>(end - start).count();
    return (endCycles - startCycles) / ns;
}

// ═══════════════════════════════════════════════════════════════════════════
// SPSC Latency Benchmark (with throttling for accurate measurement)
// ═══════════════════════════════════════════════════════════════════════════

LatencyStats benchmarkSPSCLatency(size_t numMessages, double cyclesPerNs, uint64_t throttleCycles)
{
    constexpr size_t BUFFER_SIZE = 65536;
    lmax::RingBuffer<Event, BUFFER_SIZE> ring;
    lmax::Sequence consumerSeq;

    ring.addGatingSequence(consumerSeq);

    std::vector<uint64_t> latencies(numMessages);
    std::atomic<bool> consumerReady{false};

    std::thread consumer([&]()
                         {
        pinThread(3);  // Use different core
        const lmax::Sequence& cursor = ring.cursor();
        int64_t nextSeq = 0;
        size_t idx = 0;

        consumerReady.store(true, std::memory_order_release);

        while (nextSeq < (int64_t)numMessages) {
            // Relaxed poll first
            int64_t available = cursor.getRelaxed();
            if (available < nextSeq) {
                _mm_pause();
                continue;
            }

            // Acquire fence to see the data
            std::atomic_thread_fence(std::memory_order_acquire);

            // Process one message at a time for accurate latency
            const Event* e = ring.get(nextSeq);
            uint64_t endTime = rdtscp();
            latencies[idx++] = endTime - e->timestamp;

            // Update consumer sequence immediately (relaxed for throughput)
            consumerSeq.setRelaxed(nextSeq);
            nextSeq++;
        }
        consumerSeq.set(nextSeq - 1); });

    // Wait for consumer to be ready
    while (!consumerReady.load(std::memory_order_acquire))
    {
        _mm_pause();
    }

    // Producer with throttling
    pinThread(2); // Use different core
    for (size_t i = 0; i < numMessages; i++)
    {
        if (throttleCycles > 0)
        {
            spinWait(throttleCycles);
        }

        int64_t seq = ring.next();
        Event *e = ring.get(seq);
        e->sequence = i;

        // Memory fence before timestamp to ensure data is written
        std::atomic_thread_fence(std::memory_order_release);
        e->timestamp = rdtscp();
        ring.publish(seq);
    }

    consumer.join();

    return calculateLatencyStats(latencies, cyclesPerNs);
}

// ═══════════════════════════════════════════════════════════════════════════
// Diamond Pattern: 1P -> 2C parallel -> 1C final
// ═══════════════════════════════════════════════════════════════════════════

class DiamondHandler : public lmax::EventHandler<Event>
{
public:
    std::atomic<uint64_t> count{0};
    int id;

    explicit DiamondHandler(int id) : id(id) {}

    void onEvent(Event &event, int64_t /*sequence*/, bool /*endOfBatch*/) override
    {
        // Simulate some work
        event.padding[id] = event.sequence + id;
        count.fetch_add(1, std::memory_order_relaxed);
    }
};

double benchmarkDiamondThroughput(size_t numMessages)
{
    constexpr size_t BUFFER_SIZE = 65536;
    lmax::RingBuffer<Event, BUFFER_SIZE> ring;

    // Create barriers and handlers
    auto barrier1 = ring.newBarrier(); // For parallel consumers (from producer)

    DiamondHandler handler1(1);
    DiamondHandler handler2(2);

    // Create parallel processors
    lmax::BatchEventProcessor<Event, decltype(ring), decltype(barrier1)>
        processor1(ring, barrier1, handler1);
    lmax::BatchEventProcessor<Event, decltype(ring), decltype(barrier1)>
        processor2(ring, barrier1, handler2);

    // Create barrier for final consumer (depends on both processor1 and processor2)
    auto barrier2 = ring.newBarrier({&processor1.getSequence(), &processor2.getSequence()});

    DiamondHandler finalHandler(3);
    lmax::BatchEventProcessor<Event, decltype(ring), decltype(barrier2)>
        finalProcessor(ring, barrier2, finalHandler);

    // Producer is gated by final consumer only
    ring.addGatingSequence(finalProcessor.getSequence());

    auto start = std::chrono::high_resolution_clock::now();

    // Start all processors
    processor1.start();
    processor2.start();
    finalProcessor.start();

    // Producer
    for (size_t i = 0; i < numMessages; i++)
    {
        int64_t seq = ring.next();
        Event *e = ring.get(seq);
        e->sequence = i;
        ring.publish(seq);
    }

    // Wait for final processor with timeout
    auto timeout = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (finalHandler.count.load(std::memory_order_relaxed) < numMessages)
    {
        if (std::chrono::steady_clock::now() > timeout)
        {
            std::cerr << "Diamond timeout! Final: " << finalHandler.count.load()
                      << ", P1: " << handler1.count.load()
                      << ", P2: " << handler2.count.load() << "/" << numMessages << "\n";
            break;
        }
        std::this_thread::yield();
    }

    // Stop all processors
    processor1.halt();
    processor2.halt();
    finalProcessor.halt();

    processor1.join();
    processor2.join();
    finalProcessor.join();

    auto end = std::chrono::high_resolution_clock::now();
    double seconds = std::chrono::duration<double>(end - start).count();

    return finalHandler.count.load() / seconds / 1e6;
}

// Diamond pattern with manual threads (for latency measurement)
LatencyStats benchmarkDiamondLatency(size_t numMessages, double cyclesPerNs, uint64_t throttleCycles)
{
    constexpr size_t BUFFER_SIZE = 65536;
    lmax::RingBuffer<Event, BUFFER_SIZE> ring;

    // Sequences start at -1, final consumer waits until they're >= nextSeq
    lmax::Sequence consumer1Seq;
    lmax::Sequence consumer2Seq;
    lmax::Sequence finalSeq;

    // Producer gated by final consumer
    ring.addGatingSequence(finalSeq);

    std::vector<uint64_t> latencies(numMessages);
    std::atomic<int> readyCount{0};

    // Consumer 1 (parallel) - reads from producer, signals to final
    std::thread consumer1([&]()
                          {
        pinThread(4);
        const lmax::Sequence& cursor = ring.cursor();
        int64_t nextSeq = 0;

        readyCount.fetch_add(1, std::memory_order_release);

        while (nextSeq < (int64_t)numMessages) {
            int64_t available = cursor.getRelaxed();
            if (available < nextSeq) {
                _mm_pause();
                continue;
            }
            std::atomic_thread_fence(std::memory_order_acquire);

            // Process all available
            while (nextSeq <= available && nextSeq < (int64_t)numMessages) {
                Event* e = const_cast<Event*>(ring.get(nextSeq));
                e->padding[0] = e->sequence + 1;  // Some work
                nextSeq++;
            }
            consumer1Seq.set(nextSeq - 1);
        } });

    // Consumer 2 (parallel) - reads from producer, signals to final
    std::thread consumer2([&]()
                          {
        pinThread(5);
        const lmax::Sequence& cursor = ring.cursor();
        int64_t nextSeq = 0;

        readyCount.fetch_add(1, std::memory_order_release);

        while (nextSeq < (int64_t)numMessages) {
            int64_t available = cursor.getRelaxed();
            if (available < nextSeq) {
                _mm_pause();
                continue;
            }
            std::atomic_thread_fence(std::memory_order_acquire);

            // Process all available
            while (nextSeq <= available && nextSeq < (int64_t)numMessages) {
                Event* e = const_cast<Event*>(ring.get(nextSeq));
                e->padding[1] = e->sequence + 2;  // Some work
                nextSeq++;
            }
            consumer2Seq.set(nextSeq - 1);
        } });

    // Final consumer (waits for both consumer1 and consumer2)
    std::thread finalConsumer([&]()
                              {
        pinThread(6);
        int64_t nextSeq = 0;
        size_t idx = 0;

        readyCount.fetch_add(1, std::memory_order_release);

        while (nextSeq < (int64_t)numMessages) {
            // Wait for both parallel consumers to have processed this sequence
            int64_t avail1 = consumer1Seq.get();
            int64_t avail2 = consumer2Seq.get();
            int64_t available = std::min(avail1, avail2);

            if (available < nextSeq) {
                _mm_pause();
                continue;
            }

            // Process one message at a time for accurate latency
            const Event* e = ring.get(nextSeq);
            uint64_t endTime = rdtscp();
            latencies[idx++] = endTime - e->timestamp;
            finalSeq.set(nextSeq);
            nextSeq++;
        } });

    // Wait for all consumers to be ready
    while (readyCount.load(std::memory_order_acquire) < 3)
    {
        _mm_pause();
    }

    // Producer with throttling
    pinThread(2);
    for (size_t i = 0; i < numMessages; i++)
    {
        if (throttleCycles > 0)
        {
            spinWait(throttleCycles);
        }

        int64_t seq = ring.next();
        Event *e = ring.get(seq);
        e->sequence = i;

        std::atomic_thread_fence(std::memory_order_release);
        e->timestamp = rdtscp();
        ring.publish(seq);
    }

    consumer1.join();
    consumer2.join();
    finalConsumer.join();

    return calculateLatencyStats(latencies, cyclesPerNs);
}

// ═══════════════════════════════════════════════════════════════════════════
// Batch Processing Latency Benchmark (busy-spin for low latency)
// ═══════════════════════════════════════════════════════════════════════════

// This simulates what BatchEventProcessor does but with busy-spin instead of yield
// to measure the true latency potential of batch processing
LatencyStats benchmarkBatchProcessorLatency(size_t numMessages, double cyclesPerNs, uint64_t throttleCycles)
{
    constexpr size_t BUFFER_SIZE = 65536;
    lmax::RingBuffer<Event, BUFFER_SIZE> ring;
    lmax::Sequence consumerSeq;

    ring.addGatingSequence(consumerSeq);

    std::vector<uint64_t> latencies(numMessages);
    std::atomic<bool> consumerReady{false};

    // Consumer thread - batch processing with busy-spin
    std::thread consumer([&]()
                         {
        pinThread(3);
        const lmax::Sequence& cursor = ring.cursor();
        int64_t nextSeq = 0;
        size_t idx = 0;

        consumerReady.store(true, std::memory_order_release);

        while (nextSeq < (int64_t)numMessages) {
            // Busy-spin poll (not yield)
            int64_t available = cursor.getRelaxed();
            if (available < nextSeq) {
                _mm_pause();
                continue;
            }

            std::atomic_thread_fence(std::memory_order_acquire);

            // Process batch - measure latency for each message in the batch
            while (nextSeq <= available && nextSeq < (int64_t)numMessages) {
                const Event* e = ring.get(nextSeq);
                uint64_t endTime = rdtscp();
                latencies[idx++] = endTime - e->timestamp;
                nextSeq++;
            }

            // Update consumer sequence after batch (like BatchEventProcessor does)
            consumerSeq.set(nextSeq - 1);
        } });

    // Wait for consumer to be ready
    while (!consumerReady.load(std::memory_order_acquire))
    {
        _mm_pause();
    }

    // Producer with throttling
    pinThread(2);
    for (size_t i = 0; i < numMessages; i++)
    {
        if (throttleCycles > 0)
        {
            spinWait(throttleCycles);
        }

        int64_t seq = ring.next();
        Event *e = ring.get(seq);
        e->sequence = i;

        std::atomic_thread_fence(std::memory_order_release);
        e->timestamp = rdtscp();
        ring.publish(seq);
    }

    consumer.join();

    return calculateLatencyStats(latencies, cyclesPerNs);
}

void printLatencyStats(const std::string &name, const LatencyStats &stats)
{
    std::cout << name << ":\n";
    std::cout << "  Min:     " << std::fixed << std::setprecision(0) << stats.min << " ns\n";
    std::cout << "  Mean:    " << std::fixed << std::setprecision(0) << stats.mean << " ns\n";
    std::cout << "  P50:     " << std::fixed << std::setprecision(0) << stats.p50 << " ns\n";
    std::cout << "  P90:     " << std::fixed << std::setprecision(0) << stats.p90 << " ns\n";
    std::cout << "  P99:     " << std::fixed << std::setprecision(0) << stats.p99 << " ns\n";
    std::cout << "  P99.9:   " << std::fixed << std::setprecision(0) << stats.p999 << " ns\n";
    std::cout << "  P99.99:  " << std::fixed << std::setprecision(0) << stats.p9999 << " ns\n";
    std::cout << "  Max:     " << std::fixed << std::setprecision(0) << stats.max << " ns\n";
}

// ═══════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════

int main()
{
    constexpr size_t MESSAGES = 5000000;

    std::cout << "═══════════════════════════════════════════════════════════════════════════\n";
    std::cout << "              LMAX Disruptor Multi-Producer/Pipeline Benchmark\n";
    std::cout << "═══════════════════════════════════════════════════════════════════════════\n\n";

    std::cout << "Messages per test: " << MESSAGES << "\n\n";

    // ═══════════════════════════════════════════════════════════════════════
    std::cout << "═══════════════════════════════════════════════════════════════════════════\n";
    std::cout << "                         THROUGHPUT COMPARISON\n";
    std::cout << "═══════════════════════════════════════════════════════════════════════════\n\n";

    // Single producer baseline
    std::cout << "Single Producer (SPSC):\n";
    for (int i = 0; i < 3; i++)
    {
        double t = benchmarkSPSCThroughput(MESSAGES);
        std::cout << "  Run " << (i + 1) << ": " << std::fixed << std::setprecision(2)
                  << t << " M ops/sec\n";
    }
    std::cout << "\n";

    // Multi producer scaling
    std::cout << "Multi Producer (MPMC) - Scaling with producers:\n";
    for (int numProducers : {1, 2, 4})
    {
        double t = benchmarkMPMCThroughput(MESSAGES, numProducers);
        std::cout << "  " << numProducers << " producer(s): " << std::fixed
                  << std::setprecision(2) << t << " M ops/sec\n";
    }
    std::cout << "\n";

    // Pipeline throughput
    std::cout << "Pipeline (3-stage: WAL -> Replicator -> ME):\n";
    for (int i = 0; i < 3; i++)
    {
        double t = benchmarkPipelineThroughput(MESSAGES);
        std::cout << "  Run " << (i + 1) << ": " << std::fixed << std::setprecision(2)
                  << t << " M ops/sec\n";
    }
    std::cout << "\n";

    // Batch processor throughput
    std::cout << "Batch Event Processor:\n";
    for (int i = 0; i < 3; i++)
    {
        int batches = 0;
        double t = benchmarkBatchProcessorThroughput(MESSAGES, batches);
        std::cout << "  Run " << (i + 1) << ": " << std::fixed << std::setprecision(2)
                  << t << " M ops/sec"
                  << " (" << batches << " batches, avg size: " << MESSAGES / std::max(1, batches) << ")\n";
    }
    std::cout << "\n";

    // Diamond pattern throughput
    std::cout << "Diamond Pattern (1P -> 2C parallel -> 1C final):\n";
    for (int i = 0; i < 3; i++)
    {
        double t = benchmarkDiamondThroughput(MESSAGES);
        std::cout << "  Run " << (i + 1) << ": " << std::fixed << std::setprecision(2)
                  << t << " M ops/sec\n";
    }

    // ═══════════════════════════════════════════════════════════════════════
    std::cout << "\n═══════════════════════════════════════════════════════════════════════════\n";
    std::cout << "                           LATENCY MEASUREMENTS\n";
    std::cout << "═══════════════════════════════════════════════════════════════════════════\n\n";

    // Calibrate TSC
    std::cout << "Calibrating TSC... ";
    double cyclesPerNs = estimateCyclesPerNs();
    std::cout << std::fixed << std::setprecision(2) << cyclesPerNs << " cycles/ns\n\n";

    constexpr size_t LATENCY_MESSAGES = 1000000;
    constexpr uint64_t THROTTLE_CYCLES = 1000; // ~350ns @ 2.9GHz - prevents producer overwhelming consumer

    std::cout << "Throttle: " << THROTTLE_CYCLES << " cycles (~"
              << (int)(THROTTLE_CYCLES / cyclesPerNs) << " ns between messages)\n\n";

    // SPSC Latency
    std::cout << "Running SPSC latency test (" << LATENCY_MESSAGES << " messages)...\n";
    auto spscLatency = benchmarkSPSCLatency(LATENCY_MESSAGES, cyclesPerNs, THROTTLE_CYCLES);
    printLatencyStats("SPSC Latency", spscLatency);
    std::cout << "\n";

    // Batch Processor Latency
    std::cout << "Running Batch Processor latency test (" << LATENCY_MESSAGES << " messages)...\n";
    auto batchLatency = benchmarkBatchProcessorLatency(LATENCY_MESSAGES, cyclesPerNs, THROTTLE_CYCLES);
    printLatencyStats("Batch Processor Latency", batchLatency);
    std::cout << "\n";

    // Diamond Latency
    std::cout << "Running Diamond pattern latency test (" << LATENCY_MESSAGES << " messages)...\n";
    auto diamondLatency = benchmarkDiamondLatency(LATENCY_MESSAGES, cyclesPerNs, THROTTLE_CYCLES);
    printLatencyStats("Diamond Latency (1P -> 2C -> 1C)", diamondLatency);

    std::cout << "\n═══════════════════════════════════════════════════════════════════════════\n";
    std::cout << "                              BENCHMARK COMPLETE\n";
    std::cout << "═══════════════════════════════════════════════════════════════════════════\n";

    return 0;
}
