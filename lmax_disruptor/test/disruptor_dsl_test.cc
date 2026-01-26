/*
 * Disruptor DSL Test
 *
 * Tests the fluent API for configuring event processing pipelines:
 * - Simple chain: h1 -> h2 -> h3
 * - Parallel handlers: h1 -> (h2a, h2b) -> h3
 * - Diamond pattern with after()
 */

#include <iostream>
#include <iomanip>
#include <thread>
#include <atomic>
#include <chrono>
#include <cassert>
#include <vector>
#include <algorithm>
#include <memory>
#include <emmintrin.h>
#include "../include/lmax/disruptor.h"

// RDTSC for precise timing
inline uint64_t rdtscp() {
    uint32_t lo, hi;
    __asm__ volatile("rdtscp" : "=a"(lo), "=d"(hi) : : "rcx", "memory");
    return (uint64_t(hi) << 32) | lo;
}

inline void spinWait(uint64_t cycles) {
    uint64_t expire = rdtscp() + cycles;
    while (rdtscp() < expire) {
        _mm_pause();
    }
}

struct TestEvent {
    int64_t value;
    uint64_t timestamp;  // For latency measurement
    int stage1;
    int stage2a;
    int stage2b;
    int stage3;
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

    void print(double cpuGhz) const {
        auto toNs = [cpuGhz](uint64_t cycles) -> int64_t {
            return static_cast<int64_t>(cycles / cpuGhz);
        };
        std::cout << "  min=" << std::setw(4) << toNs(min) << "ns  "
                  << "p50=" << std::setw(4) << toNs(p50) << "ns  "
                  << "p90=" << std::setw(4) << toNs(p90) << "ns  "
                  << "p99=" << std::setw(5) << toNs(p99) << "ns  "
                  << "p99.9=" << std::setw(6) << toNs(p999) << "ns  "
                  << "max=" << std::setw(7) << toNs(max) << "ns\n";
    }
};

// Handler that increments a counter and records processing order
class CountingHandler : public lmax::EventHandler<TestEvent> {
public:
    std::string name;
    std::atomic<int64_t> count{0};
    int* stageField;

    CountingHandler(const std::string& n, int TestEvent::* field)
        : name(n), stageField(nullptr), fieldPtr_(field) {}

    void onEvent(TestEvent& event, int64_t sequence, bool endOfBatch) override {
        event.*fieldPtr_ = static_cast<int>(sequence + 1);
        count.fetch_add(1, std::memory_order_relaxed);
    }

    void onStart() override {
        std::cout << "  " << name << " started\n";
    }

    void onShutdown() override {
        std::cout << "  " << name << " shutdown (processed " << count.load() << " events)\n";
    }

private:
    int TestEvent::* fieldPtr_;
};

// Handler that records latencies (for final stage)
class LatencyHandler : public lmax::EventHandler<TestEvent> {
public:
    uint64_t* latencies;
    std::atomic<size_t> count{0};
    size_t maxCount;

    LatencyHandler(uint64_t* lat, size_t max) : latencies(lat), maxCount(max) {}

    void onEvent(TestEvent& event, int64_t sequence, bool endOfBatch) override {
        size_t idx = count.load(std::memory_order_relaxed);
        if (idx < maxCount) {
            uint64_t now = rdtscp();
            latencies[idx] = now - event.timestamp;
            count.fetch_add(1, std::memory_order_relaxed);
        }
    }
};

// Simple handler for throughput (no latency recording)
class SimpleHandler : public lmax::EventHandler<TestEvent> {
public:
    std::atomic<size_t> count{0};

    void onEvent(TestEvent& event, int64_t sequence, bool endOfBatch) override {
        count.fetch_add(1, std::memory_order_relaxed);
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Test 1: Simple Chain (h1 -> h2 -> h3)
// ═══════════════════════════════════════════════════════════════════════════

bool testSimpleChain() {
    std::cout << "\n=== Test: Simple Chain (h1 -> h2 -> h3) ===\n";

    constexpr size_t NUM_EVENTS = 10000;

    lmax::Disruptor<TestEvent, 1024, lmax::YieldingWait> disruptor;

    CountingHandler h1("Handler1", &TestEvent::stage1);
    CountingHandler h2("Handler2", &TestEvent::stage2a);
    CountingHandler h3("Handler3", &TestEvent::stage3);

    // Simple chain: h1 -> h2 -> h3
    disruptor.handleEventsWith(h1).then(h2).then(h3);

    disruptor.start();

    // Publish events
    for (size_t i = 0; i < NUM_EVENTS; i++) {
        disruptor.publishEvent([i](TestEvent& e, int64_t seq) {
            e.value = i;
            e.stage1 = 0;
            e.stage2a = 0;
            e.stage2b = 0;
            e.stage3 = 0;
        });
    }

    // Wait for all events
    while (h3.count.load() < NUM_EVENTS) {
        std::this_thread::yield();
    }

    disruptor.shutdown();

    bool success = (h1.count.load() == NUM_EVENTS &&
                   h2.count.load() == NUM_EVENTS &&
                   h3.count.load() == NUM_EVENTS);

    std::cout << "Result: " << (success ? "PASSED" : "FAILED") << "\n";
    return success;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 2: Parallel Handlers (h1 -> (h2a, h2b) -> h3)
// ═══════════════════════════════════════════════════════════════════════════

bool testParallelHandlers() {
    std::cout << "\n=== Test: Parallel Handlers (h1 -> (h2a, h2b) -> h3) ===\n";

    constexpr size_t NUM_EVENTS = 10000;

    lmax::Disruptor<TestEvent, 1024, lmax::YieldingWait> disruptor;

    CountingHandler h1("Handler1", &TestEvent::stage1);
    CountingHandler h2a("Handler2a", &TestEvent::stage2a);
    CountingHandler h2b("Handler2b", &TestEvent::stage2b);
    CountingHandler h3("Handler3", &TestEvent::stage3);

    // Parallel: h1 -> (h2a, h2b) -> h3
    disruptor.handleEventsWith(h1)
             .then(h2a, h2b)
             .then(h3);

    disruptor.start();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        disruptor.publishEvent([i](TestEvent& e, int64_t seq) {
            e.value = i;
            e.stage1 = 0;
            e.stage2a = 0;
            e.stage2b = 0;
            e.stage3 = 0;
        });
    }

    while (h3.count.load() < NUM_EVENTS) {
        std::this_thread::yield();
    }

    disruptor.shutdown();

    bool success = (h1.count.load() == NUM_EVENTS &&
                   h2a.count.load() == NUM_EVENTS &&
                   h2b.count.load() == NUM_EVENTS &&
                   h3.count.load() == NUM_EVENTS);

    std::cout << "Result: " << (success ? "PASSED" : "FAILED") << "\n";
    return success;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 3: Diamond Pattern with after()
// ═══════════════════════════════════════════════════════════════════════════

bool testDiamondPattern() {
    std::cout << "\n=== Test: Diamond Pattern with after() ===\n";
    std::cout << "  Pattern: (h1, h2) -> h3\n";

    constexpr size_t NUM_EVENTS = 10000;

    lmax::Disruptor<TestEvent, 1024, lmax::YieldingWait> disruptor;

    CountingHandler h1("Handler1", &TestEvent::stage1);
    CountingHandler h2("Handler2", &TestEvent::stage2a);
    CountingHandler h3("Handler3", &TestEvent::stage3);

    // Diamond: h1 and h2 run in parallel, h3 waits for both
    auto& g1 = disruptor.handleEventsWith(h1);
    auto& g2 = disruptor.handleEventsWith(h2);
    disruptor.after(g1, g2).handleEventsWith(h3);

    disruptor.start();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        disruptor.publishEvent([i](TestEvent& e, int64_t seq) {
            e.value = i;
            e.stage1 = 0;
            e.stage2a = 0;
            e.stage2b = 0;
            e.stage3 = 0;
        });
    }

    while (h3.count.load() < NUM_EVENTS) {
        std::this_thread::yield();
    }

    disruptor.shutdown();

    bool success = (h1.count.load() == NUM_EVENTS &&
                   h2.count.load() == NUM_EVENTS &&
                   h3.count.load() == NUM_EVENTS);

    std::cout << "Result: " << (success ? "PASSED" : "FAILED") << "\n";
    return success;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4: Throughput Test
// ═══════════════════════════════════════════════════════════════════════════

bool testThroughput() {
    std::cout << "\n=== Test: Throughput ===\n";

    constexpr size_t NUM_EVENTS = 1000000;

    lmax::Disruptor<TestEvent, 65536, lmax::BusySpinWait> disruptor;

    CountingHandler h1("Handler", &TestEvent::stage1);

    disruptor.handleEventsWith(h1);
    disruptor.start();

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        disruptor.publishEvent([i](TestEvent& e, int64_t seq) {
            e.value = i;
        });
    }

    while (h1.count.load() < NUM_EVENTS) {
        std::this_thread::yield();
    }

    auto end = std::chrono::high_resolution_clock::now();
    double seconds = std::chrono::duration<double>(end - start).count();
    double mops = NUM_EVENTS / seconds / 1e6;

    disruptor.shutdown();

    std::cout << "  Throughput: " << std::fixed << std::setprecision(2) << mops << " M ops/sec\n";
    std::cout << "Result: PASSED\n";
    return true;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 5: Batch Publishing
// ═══════════════════════════════════════════════════════════════════════════

bool testBatchPublishing() {
    std::cout << "\n=== Test: Batch Publishing ===\n";

    constexpr size_t NUM_EVENTS = 10000;
    constexpr size_t BATCH_SIZE = 10;

    lmax::Disruptor<TestEvent, 1024, lmax::YieldingWait> disruptor;

    CountingHandler h1("Handler", &TestEvent::stage1);

    disruptor.handleEventsWith(h1);
    disruptor.start();

    size_t published = 0;
    while (published < NUM_EVENTS) {
        disruptor.publishEvents([&published](TestEvent& e, int64_t seq) {
            e.value = published++;
        }, BATCH_SIZE);
    }

    while (h1.count.load() < NUM_EVENTS) {
        std::this_thread::yield();
    }

    disruptor.shutdown();

    bool success = (h1.count.load() == NUM_EVENTS);
    std::cout << "Result: " << (success ? "PASSED" : "FAILED") << "\n";
    return success;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 6: Try Publish (non-blocking)
// ═══════════════════════════════════════════════════════════════════════════

bool testTryPublish() {
    std::cout << "\n=== Test: Try Publish (non-blocking) ===\n";

    constexpr size_t NUM_EVENTS = 1000;

    lmax::Disruptor<TestEvent, 1024, lmax::YieldingWait> disruptor;

    CountingHandler h1("Handler", &TestEvent::stage1);

    disruptor.handleEventsWith(h1);
    disruptor.start();

    size_t published = 0;
    size_t failed = 0;

    while (published < NUM_EVENTS) {
        bool success = disruptor.tryPublishEvent([published](TestEvent& e, int64_t seq) {
            e.value = published;
        });

        if (success) {
            published++;
        } else {
            failed++;
            std::this_thread::yield();
        }
    }

    while (h1.count.load() < NUM_EVENTS) {
        std::this_thread::yield();
    }

    disruptor.shutdown();

    std::cout << "  Published: " << published << ", Retries: " << failed << "\n";

    bool success = (h1.count.load() == NUM_EVENTS);
    std::cout << "Result: " << (success ? "PASSED" : "FAILED") << "\n";
    return success;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 7: Diamond Throughput
// ═══════════════════════════════════════════════════════════════════════════

bool testDiamondThroughput() {
    std::cout << "\n=== Test: Diamond Throughput ===\n";
    std::cout << "  Pattern: (h1, h2) -> h3\n";

    constexpr size_t NUM_EVENTS = 1000000;

    lmax::Disruptor<TestEvent, 65536, lmax::BusySpinWait> disruptor;

    SimpleHandler h1, h2, h3;

    auto& g1 = disruptor.handleEventsWith(h1);
    auto& g2 = disruptor.handleEventsWith(h2);
    disruptor.after(g1, g2).handleEventsWith(h3);

    disruptor.start();

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        disruptor.publishEvent([i](TestEvent& e, int64_t seq) {
            e.value = i;
        });
    }

    while (h3.count.load() < NUM_EVENTS) {
        std::this_thread::yield();
    }

    auto end = std::chrono::high_resolution_clock::now();
    double seconds = std::chrono::duration<double>(end - start).count();
    double mops = NUM_EVENTS / seconds / 1e6;

    disruptor.shutdown();

    std::cout << "  Throughput: " << std::fixed << std::setprecision(2) << mops << " M ops/sec\n";
    std::cout << "Result: PASSED\n";
    return true;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 8: Diamond Latency
// ═══════════════════════════════════════════════════════════════════════════

bool testDiamondLatency() {
    std::cout << "\n=== Test: Diamond Latency ===\n";
    std::cout << "  Pattern: (h1, h2) -> h3\n";

    constexpr size_t NUM_EVENTS = 100000;
    constexpr uint64_t THROTTLE_CYCLES = 1000;
    constexpr double CPU_GHZ = 2.9;

    lmax::Disruptor<TestEvent, 65536, lmax::BusySpinWait> disruptor;

    SimpleHandler h1, h2;
    std::unique_ptr<uint64_t[]> latencies(new uint64_t[NUM_EVENTS]);
    LatencyHandler h3(latencies.get(), NUM_EVENTS);

    auto& g1 = disruptor.handleEventsWith(h1);
    auto& g2 = disruptor.handleEventsWith(h2);
    disruptor.after(g1, g2).handleEventsWith(h3);

    disruptor.start();

    // Wait for handlers to start
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        spinWait(THROTTLE_CYCLES);

        disruptor.publishEvent([i](TestEvent& e, int64_t seq) {
            e.value = i;
            e.timestamp = rdtscp();
        });
    }

    while (h3.count.load() < NUM_EVENTS) {
        std::this_thread::yield();
    }

    disruptor.shutdown();

    LatencyStats stats;
    stats.calculate(latencies.get(), h3.count.load());
    stats.print(CPU_GHZ);

    std::cout << "Result: PASSED\n";
    return true;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 9: Pipeline Latency (h1 -> h2 -> h3)
// ═══════════════════════════════════════════════════════════════════════════

bool testPipelineLatency() {
    std::cout << "\n=== Test: Pipeline Latency ===\n";
    std::cout << "  Pattern: h1 -> h2 -> h3\n";

    constexpr size_t NUM_EVENTS = 100000;
    constexpr uint64_t THROTTLE_CYCLES = 1000;
    constexpr double CPU_GHZ = 2.9;

    lmax::Disruptor<TestEvent, 65536, lmax::BusySpinWait> disruptor;

    SimpleHandler h1, h2;
    std::unique_ptr<uint64_t[]> latencies(new uint64_t[NUM_EVENTS]);
    LatencyHandler h3(latencies.get(), NUM_EVENTS);

    disruptor.handleEventsWith(h1).then(h2).then(h3);

    disruptor.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        spinWait(THROTTLE_CYCLES);

        disruptor.publishEvent([i](TestEvent& e, int64_t seq) {
            e.value = i;
            e.timestamp = rdtscp();
        });
    }

    while (h3.count.load() < NUM_EVENTS) {
        std::this_thread::yield();
    }

    disruptor.shutdown();

    LatencyStats stats;
    stats.calculate(latencies.get(), h3.count.load());
    stats.print(CPU_GHZ);

    std::cout << "Result: PASSED\n";
    return true;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 10: Diamond with OptimizedBarrier - Throughput
// ═══════════════════════════════════════════════════════════════════════════

bool testDiamondOptimizedThroughput() {
    std::cout << "\n=== Test: Diamond Optimized Throughput ===\n";
    std::cout << "  Pattern: (h1, h2) -> h3 (OptimizedBarrier)\n";

    constexpr size_t NUM_EVENTS = 1000000;
    constexpr size_t BUFFER_SIZE = 65536;

    using RingBufferType = lmax::RingBuffer<TestEvent, BUFFER_SIZE, lmax::BusySpinWait>;
    RingBufferType ring;

    lmax::Sequence h1Seq, h2Seq, h3Seq;

    // h1 and h2 wait on cursor (parallel)
    auto h1Barrier = ring.newBarrier();
    auto h2Barrier = ring.newBarrier();

    // h3 uses OptimizedBarrier to wait on both h1 and h2
    auto h3Barrier = ring.newOptimizedBarrier({&h1Seq, &h2Seq});

    ring.addGatingSequence(h3Seq);

    std::atomic<bool> done{false};
    std::atomic<size_t> h3Count{0};

    // h1 consumer
    std::thread t1([&]() {
        int64_t next = 0;
        while (!done.load(std::memory_order_relaxed) || next <= ring.getCursor()) {
            int64_t avail = h1Barrier.waitFor(next);
            if (avail == lmax::ALERTED) break;
            while (next <= avail) {
                h1Seq.set(next);
                next++;
            }
        }
    });

    // h2 consumer
    std::thread t2([&]() {
        int64_t next = 0;
        while (!done.load(std::memory_order_relaxed) || next <= ring.getCursor()) {
            int64_t avail = h2Barrier.waitFor(next);
            if (avail == lmax::ALERTED) break;
            while (next <= avail) {
                h2Seq.set(next);
                next++;
            }
        }
    });

    // h3 consumer (uses optimized barrier)
    std::thread t3([&]() {
        int64_t next = 0;
        while (!done.load(std::memory_order_relaxed) || next <= h1Seq.get() || next <= h2Seq.get()) {
            int64_t avail = h3Barrier.waitFor(next);
            if (avail == lmax::ALERTED) break;
            while (next <= avail) {
                h3Seq.set(next);
                h3Count.fetch_add(1, std::memory_order_relaxed);
                next++;
            }
        }
    });

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        int64_t seq = ring.next();
        TestEvent* e = ring.get(seq);
        e->value = i;
        ring.publish(seq);
    }

    while (h3Count.load() < NUM_EVENTS) {
        std::this_thread::yield();
    }

    auto end = std::chrono::high_resolution_clock::now();

    done.store(true);
    h1Barrier.alert();
    h2Barrier.alert();
    h3Barrier.alert();

    t1.join();
    t2.join();
    t3.join();

    double seconds = std::chrono::duration<double>(end - start).count();
    double mops = NUM_EVENTS / seconds / 1e6;

    std::cout << "  Throughput: " << std::fixed << std::setprecision(2) << mops << " M ops/sec\n";
    std::cout << "Result: PASSED\n";
    return true;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 11: Diamond with OptimizedBarrier - Latency
// ═══════════════════════════════════════════════════════════════════════════

bool testDiamondOptimizedLatency() {
    std::cout << "\n=== Test: Diamond Optimized Latency ===\n";
    std::cout << "  Pattern: (h1, h2) -> h3 (OptimizedBarrier)\n";

    constexpr size_t NUM_EVENTS = 100000;
    constexpr size_t BUFFER_SIZE = 65536;
    constexpr uint64_t THROTTLE_CYCLES = 1000;
    constexpr double CPU_GHZ = 2.9;

    using RingBufferType = lmax::RingBuffer<TestEvent, BUFFER_SIZE, lmax::BusySpinWait>;
    RingBufferType ring;

    lmax::Sequence h1Seq, h2Seq, h3Seq;

    auto h1Barrier = ring.newBarrier();
    auto h2Barrier = ring.newBarrier();
    auto h3Barrier = ring.newOptimizedBarrier({&h1Seq, &h2Seq});

    ring.addGatingSequence(h3Seq);

    std::atomic<bool> done{false};
    std::unique_ptr<uint64_t[]> latencies(new uint64_t[NUM_EVENTS]);
    std::atomic<size_t> h3Count{0};

    std::thread t1([&]() {
        int64_t next = 0;
        while (!done.load(std::memory_order_relaxed) || next <= ring.getCursor()) {
            int64_t avail = h1Barrier.waitFor(next);
            if (avail == lmax::ALERTED) break;
            while (next <= avail) {
                h1Seq.set(next);
                next++;
            }
        }
    });

    std::thread t2([&]() {
        int64_t next = 0;
        while (!done.load(std::memory_order_relaxed) || next <= ring.getCursor()) {
            int64_t avail = h2Barrier.waitFor(next);
            if (avail == lmax::ALERTED) break;
            while (next <= avail) {
                h2Seq.set(next);
                next++;
            }
        }
    });

    std::thread t3([&]() {
        int64_t next = 0;
        while (!done.load(std::memory_order_relaxed) || next <= h1Seq.get() || next <= h2Seq.get()) {
            int64_t avail = h3Barrier.waitFor(next);
            if (avail == lmax::ALERTED) break;
            while (next <= avail) {
                const TestEvent* e = ring.get(next);
                size_t idx = h3Count.load(std::memory_order_relaxed);
                if (idx < NUM_EVENTS) {
                    latencies[idx] = rdtscp() - e->timestamp;
                }
                h3Seq.set(next);
                h3Count.fetch_add(1, std::memory_order_relaxed);
                next++;
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        spinWait(THROTTLE_CYCLES);

        int64_t seq = ring.next();
        TestEvent* e = ring.get(seq);
        e->value = i;
        std::atomic_thread_fence(std::memory_order_release);
        e->timestamp = rdtscp();
        ring.publish(seq);
    }

    while (h3Count.load() < NUM_EVENTS) {
        std::this_thread::yield();
    }

    done.store(true);
    h1Barrier.alert();
    h2Barrier.alert();
    h3Barrier.alert();

    t1.join();
    t2.join();
    t3.join();

    LatencyStats stats;
    stats.calculate(latencies.get(), h3Count.load());
    stats.print(CPU_GHZ);

    std::cout << "Result: PASSED\n";
    return true;
}

int main() {
    std::cout << "═══════════════════════════════════════════════════════════════════\n";
    std::cout << "                    Disruptor DSL Tests\n";
    std::cout << "═══════════════════════════════════════════════════════════════════\n";

    int passed = 0;
    int failed = 0;

    if (testSimpleChain()) passed++; else failed++;
    if (testParallelHandlers()) passed++; else failed++;
    if (testDiamondPattern()) passed++; else failed++;
    if (testThroughput()) passed++; else failed++;
    if (testBatchPublishing()) passed++; else failed++;
    if (testTryPublish()) passed++; else failed++;
    if (testDiamondThroughput()) passed++; else failed++;
    if (testDiamondLatency()) passed++; else failed++;
    if (testPipelineLatency()) passed++; else failed++;
    if (testDiamondOptimizedThroughput()) passed++; else failed++;
    if (testDiamondOptimizedLatency()) passed++; else failed++;

    std::cout << "\n═══════════════════════════════════════════════════════════════════\n";
    std::cout << "Results: " << passed << " passed, " << failed << " failed\n";
    std::cout << "═══════════════════════════════════════════════════════════════════\n";

    return failed == 0 ? 0 : 1;
}
