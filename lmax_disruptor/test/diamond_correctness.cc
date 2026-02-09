/*
 * Diamond Pattern Correctness Verification
 *
 * Verifies that in pattern (h1, h2, h3) -> h4:
 * - h4 ONLY processes events AFTER h1, h2, h3 have ALL processed them
 * - No event is processed by h4 before all upstream handlers complete
 */

#include <iostream>
#include <iomanip>
#include <thread>
#include <atomic>
#include <vector>
#include <cassert>
#include "../include/lmax/disruptor.h"

struct TestEvent {
    int64_t value;

    // Timestamps when each handler processed this event
    std::atomic<uint64_t> h1_time{0};
    std::atomic<uint64_t> h2_time{0};
    std::atomic<uint64_t> h3_time{0};
    std::atomic<uint64_t> h4_time{0};

    // Sequence numbers when each handler processed
    std::atomic<int64_t> h1_seq{-1};
    std::atomic<int64_t> h2_seq{-1};
    std::atomic<int64_t> h3_seq{-1};
    std::atomic<int64_t> h4_seq{-1};
};

inline uint64_t rdtscp() {
    uint32_t lo, hi;
    __asm__ volatile("rdtscp" : "=a"(lo), "=d"(hi) : : "rcx", "memory");
    return (uint64_t(hi) << 32) | lo;
}

// ═══════════════════════════════════════════════════════════════════════════
// Handlers that record when they process each event
// ═══════════════════════════════════════════════════════════════════════════

class TimestampHandler : public lmax::EventHandler<TestEvent> {
public:
    std::atomic<uint64_t> TestEvent::* timeField;
    std::atomic<int64_t> TestEvent::* seqField;
    std::atomic<size_t> count{0};
    const char* name;

    TimestampHandler(const char* n,
                     std::atomic<uint64_t> TestEvent::* tf,
                     std::atomic<int64_t> TestEvent::* sf)
        : name(n), timeField(tf), seqField(sf) {}

    void onEvent(TestEvent& event, int64_t sequence, bool endOfBatch) override {
        // Record when we processed this event
        (event.*timeField).store(rdtscp(), std::memory_order_release);
        (event.*seqField).store(sequence, std::memory_order_release);
        count.fetch_add(1, std::memory_order_relaxed);
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Test 1: Basic Diamond Correctness
// ═══════════════════════════════════════════════════════════════════════════

bool testDiamondCorrectness() {
    std::cout << "\n=== Test: Diamond Correctness Verification ===\n";
    std::cout << "  Pattern: (h1, h2, h3) -> h4\n";

    constexpr size_t NUM_EVENTS = 10000;

    lmax::Disruptor<TestEvent, 16384, lmax::YieldingWait> disruptor;

    TimestampHandler h1("h1", &TestEvent::h1_time, &TestEvent::h1_seq);
    TimestampHandler h2("h2", &TestEvent::h2_time, &TestEvent::h2_seq);
    TimestampHandler h3("h3", &TestEvent::h3_time, &TestEvent::h3_seq);
    TimestampHandler h4("h4", &TestEvent::h4_time, &TestEvent::h4_seq);

    auto& g1 = disruptor.handleEventsWith(h1);
    auto& g2 = disruptor.handleEventsWith(h2);
    auto& g3 = disruptor.handleEventsWith(h3);
    disruptor.after(g1, g2, g3).handleEventsWith(h4);

    disruptor.start();

    // Publish events
    for (size_t i = 0; i < NUM_EVENTS; i++) {
        disruptor.publishEvent([i](TestEvent& e, int64_t seq) {
            e.value = i;
            e.h1_time.store(0, std::memory_order_relaxed);
            e.h2_time.store(0, std::memory_order_relaxed);
            e.h3_time.store(0, std::memory_order_relaxed);
            e.h4_time.store(0, std::memory_order_relaxed);
            e.h1_seq.store(-1, std::memory_order_relaxed);
            e.h2_seq.store(-1, std::memory_order_relaxed);
            e.h3_seq.store(-1, std::memory_order_relaxed);
            e.h4_seq.store(-1, std::memory_order_relaxed);
        });
    }

    // Wait for all events to be processed
    while (h4.count.load() < NUM_EVENTS) {
        std::this_thread::yield();
    }

    disruptor.shutdown();

    std::cout << "  All handlers processed " << NUM_EVENTS << " events\n";

    // Now verify: we can't check individual events because they're in ring buffer
    // and may have been overwritten. But we can verify counts match.
    bool success = (h1.count.load() == NUM_EVENTS &&
                   h2.count.load() == NUM_EVENTS &&
                   h3.count.load() == NUM_EVENTS &&
                   h4.count.load() == NUM_EVENTS);

    std::cout << "  h1: " << h1.count.load() << ", h2: " << h2.count.load()
              << ", h3: " << h3.count.load() << ", h4: " << h4.count.load() << "\n";
    std::cout << "Result: " << (success ? "PASSED" : "FAILED") << "\n";
    return success;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 2: Diamond Order Verification (small buffer to catch violations)
// ═══════════════════════════════════════════════════════════════════════════

class OrderVerifyHandler : public lmax::EventHandler<TestEvent> {
public:
    std::atomic<int64_t>* upstreamSeqs[3];  // Sequences of upstream handlers
    int numUpstreams;
    std::atomic<size_t> violations{0};
    std::atomic<size_t> count{0};
    const char* name;

    OrderVerifyHandler(const char* n) : name(n), numUpstreams(0) {
        upstreamSeqs[0] = upstreamSeqs[1] = upstreamSeqs[2] = nullptr;
    }

    void setUpstreams(std::atomic<int64_t>* s1, std::atomic<int64_t>* s2 = nullptr,
                      std::atomic<int64_t>* s3 = nullptr) {
        upstreamSeqs[0] = s1;
        upstreamSeqs[1] = s2;
        upstreamSeqs[2] = s3;
        numUpstreams = (s1 ? 1 : 0) + (s2 ? 1 : 0) + (s3 ? 1 : 0);
    }

    void onEvent(TestEvent& event, int64_t sequence, bool endOfBatch) override {
        // Check that all upstreams have processed this sequence
        for (int i = 0; i < numUpstreams; i++) {
            if (upstreamSeqs[i]) {
                int64_t upstreamSeq = upstreamSeqs[i]->load(std::memory_order_acquire);
                if (upstreamSeq < sequence) {
                    // VIOLATION: We're processing before upstream finished
                    violations.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
        count.fetch_add(1, std::memory_order_relaxed);
    }
};

class TrackingHandler : public lmax::EventHandler<TestEvent> {
public:
    std::atomic<int64_t> lastSeq{-1};
    std::atomic<size_t> count{0};
    const char* name;

    TrackingHandler(const char* n) : name(n) {}

    void onEvent(TestEvent& event, int64_t sequence, bool endOfBatch) override {
        lastSeq.store(sequence, std::memory_order_release);
        count.fetch_add(1, std::memory_order_relaxed);
    }
};

bool testDiamondOrderVerification() {
    std::cout << "\n=== Test: Diamond Order Verification ===\n";
    std::cout << "  Verifying h4 never processes before h1, h2, h3\n";

    constexpr size_t NUM_EVENTS = 100000;

    lmax::Disruptor<TestEvent, 1024, lmax::BusySpinWait> disruptor;  // Small buffer

    TrackingHandler h1("h1"), h2("h2"), h3("h3");
    OrderVerifyHandler h4("h4");

    // h4 checks it never runs ahead of h1, h2, h3
    h4.setUpstreams(&h1.lastSeq, &h2.lastSeq, &h3.lastSeq);

    auto& g1 = disruptor.handleEventsWith(h1);
    auto& g2 = disruptor.handleEventsWith(h2);
    auto& g3 = disruptor.handleEventsWith(h3);
    disruptor.after(g1, g2, g3).handleEventsWith(h4);

    disruptor.start();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        disruptor.publishEvent([i](TestEvent& e, int64_t seq) {
            e.value = i;
        });
    }

    while (h4.count.load() < NUM_EVENTS) {
        std::this_thread::yield();
    }

    disruptor.shutdown();

    size_t violations = h4.violations.load();
    std::cout << "  Events: " << NUM_EVENTS << "\n";
    std::cout << "  Violations: " << violations << "\n";
    std::cout << "Result: " << (violations == 0 ? "PASSED" : "FAILED") << "\n";

    return violations == 0;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 3: Stress test with variable processing times
// ═══════════════════════════════════════════════════════════════════════════

class SlowHandler : public lmax::EventHandler<TestEvent> {
public:
    std::atomic<int64_t> lastSeq{-1};
    std::atomic<size_t> count{0};
    int delayMultiplier;
    const char* name;

    SlowHandler(const char* n, int delay) : name(n), delayMultiplier(delay) {}

    void onEvent(TestEvent& event, int64_t sequence, bool endOfBatch) override {
        // Simulate variable processing time
        volatile int sum = 0;
        for (int i = 0; i < delayMultiplier * 10; i++) {
            sum += i;
        }
        lastSeq.store(sequence, std::memory_order_release);
        count.fetch_add(1, std::memory_order_relaxed);
    }
};

bool testDiamondStress() {
    std::cout << "\n=== Test: Diamond Stress (Variable Processing Times) ===\n";
    std::cout << "  h1: fast, h2: medium, h3: slow -> h4 verifies order\n";

    constexpr size_t NUM_EVENTS = 50000;

    lmax::Disruptor<TestEvent, 1024, lmax::YieldingWait> disruptor;

    SlowHandler h1("h1", 1);    // Fast
    SlowHandler h2("h2", 5);    // Medium
    SlowHandler h3("h3", 10);   // Slow
    OrderVerifyHandler h4("h4");

    h4.setUpstreams(&h1.lastSeq, &h2.lastSeq, &h3.lastSeq);

    auto& g1 = disruptor.handleEventsWith(h1);
    auto& g2 = disruptor.handleEventsWith(h2);
    auto& g3 = disruptor.handleEventsWith(h3);
    disruptor.after(g1, g2, g3).handleEventsWith(h4);

    disruptor.start();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        disruptor.publishEvent([i](TestEvent& e, int64_t seq) {
            e.value = i;
        });
    }

    while (h4.count.load() < NUM_EVENTS) {
        std::this_thread::yield();
    }

    disruptor.shutdown();

    size_t violations = h4.violations.load();
    std::cout << "  Events: " << NUM_EVENTS << "\n";
    std::cout << "  h1: " << h1.count.load() << ", h2: " << h2.count.load()
              << ", h3: " << h3.count.load() << ", h4: " << h4.count.load() << "\n";
    std::cout << "  Violations: " << violations << "\n";
    std::cout << "Result: " << (violations == 0 ? "PASSED" : "FAILED") << "\n";

    return violations == 0;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4: Raw implementation verification (same pattern)
// ═══════════════════════════════════════════════════════════════════════════

bool testRawDiamondCorrectness() {
    std::cout << "\n=== Test: Raw Diamond Correctness ===\n";
    std::cout << "  Verifying raw SequenceBarrier enforces order\n";

    constexpr size_t NUM_EVENTS = 100000;
    constexpr size_t BUFFER_SIZE = 1024;

    using RingBufferType = lmax::RingBuffer<TestEvent, BUFFER_SIZE, lmax::BusySpinWait>;
    RingBufferType ring;

    lmax::Sequence h1Seq, h2Seq, h3Seq, h4Seq;
    std::atomic<size_t> violations{0};

    auto h1Barrier = ring.newBarrier();
    auto h2Barrier = ring.newBarrier();
    auto h3Barrier = ring.newBarrier();
    auto h4Barrier = ring.newBarrier({&h1Seq, &h2Seq, &h3Seq});

    ring.addGatingSequence(h4Seq);

    std::atomic<bool> done{false};
    std::atomic<size_t> h4Count{0};

    auto consumer = [&](auto& barrier, lmax::Sequence& seq) {
        int64_t next = 0;
        while (!done.load(std::memory_order_relaxed) || next <= ring.getCursor()) {
            int64_t avail = barrier.waitFor(next);
            if (avail == lmax::ALERTED) break;
            while (next <= avail) {
                seq.set(next);
                next++;
            }
        }
    };

    std::thread t1([&]() { consumer(h1Barrier, h1Seq); });
    std::thread t2([&]() { consumer(h2Barrier, h2Seq); });
    std::thread t3([&]() { consumer(h3Barrier, h3Seq); });
    std::thread t4([&]() {
        int64_t next = 0;
        while (!done.load(std::memory_order_relaxed) ||
               next <= h1Seq.get() || next <= h2Seq.get() || next <= h3Seq.get()) {
            int64_t avail = h4Barrier.waitFor(next);
            if (avail == lmax::ALERTED) break;
            while (next <= avail) {
                // Verify: all upstream sequences should be >= next
                if (h1Seq.get() < next || h2Seq.get() < next || h3Seq.get() < next) {
                    violations.fetch_add(1, std::memory_order_relaxed);
                }
                h4Seq.set(next);
                h4Count.fetch_add(1, std::memory_order_relaxed);
                next++;
            }
        }
    });

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        int64_t seq = ring.next();
        ring.get(seq)->value = i;
        ring.publish(seq);
    }

    while (h4Count.load() < NUM_EVENTS) std::this_thread::yield();

    done.store(true);
    h1Barrier.alert(); h2Barrier.alert(); h3Barrier.alert(); h4Barrier.alert();
    t1.join(); t2.join(); t3.join(); t4.join();

    std::cout << "  Events: " << NUM_EVENTS << "\n";
    std::cout << "  Violations: " << violations.load() << "\n";
    std::cout << "Result: " << (violations.load() == 0 ? "PASSED" : "FAILED") << "\n";

    return violations.load() == 0;
}

int main() {
    std::cout << "═══════════════════════════════════════════════════════════════════\n";
    std::cout << "              Diamond Pattern Correctness Tests\n";
    std::cout << "═══════════════════════════════════════════════════════════════════\n";

    int passed = 0, failed = 0;

    if (testDiamondCorrectness()) passed++; else failed++;
    if (testDiamondOrderVerification()) passed++; else failed++;
    if (testDiamondStress()) passed++; else failed++;
    if (testRawDiamondCorrectness()) passed++; else failed++;

    std::cout << "\n═══════════════════════════════════════════════════════════════════\n";
    std::cout << "Results: " << passed << " passed, " << failed << " failed\n";
    std::cout << "═══════════════════════════════════════════════════════════════════\n";

    return failed == 0 ? 0 : 1;
}
