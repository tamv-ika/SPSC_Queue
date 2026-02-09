/*
 * Isolated Overhead Analysis
 * Test each overhead source separately to identify the bottleneck
 */

#include <iostream>
#include <iomanip>
#include <thread>
#include <atomic>
#include <chrono>
#include "../include/lmax/disruptor.h"

struct TestEvent {
    int64_t value;
};

constexpr size_t NUM_EVENTS = 1000000;
constexpr size_t BUFFER_SIZE = 65536;

using RingBufferType = lmax::RingBuffer<TestEvent, BUFFER_SIZE, lmax::BusySpinWait>;

// ═══════════════════════════════════════════════════════════════════════════
// Test 1: Minimal - just update sequence (NO atomic counter anywhere)
// ═══════════════════════════════════════════════════════════════════════════

double testMinimal() {
    RingBufferType ring;
    lmax::Sequence h1Seq, h2Seq, h3Seq, h4Seq;

    auto h1Barrier = ring.newBarrier();
    auto h2Barrier = ring.newBarrier();
    auto h3Barrier = ring.newBarrier();
    auto h4Barrier = ring.newBarrier({&h1Seq, &h2Seq, &h3Seq});

    ring.addGatingSequence(h4Seq);

    std::atomic<bool> done{false};

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
    std::thread t4([&]() { consumer(h4Barrier, h4Seq); });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        int64_t seq = ring.next();
        ring.get(seq)->value = i;
        ring.publish(seq);
    }

    // Wait by checking h4's sequence directly
    while (h4Seq.get() < NUM_EVENTS - 1) std::this_thread::yield();

    auto end = std::chrono::high_resolution_clock::now();

    done.store(true);
    h1Barrier.alert(); h2Barrier.alert(); h3Barrier.alert(); h4Barrier.alert();
    t1.join(); t2.join(); t3.join(); t4.join();

    return NUM_EVENTS / std::chrono::duration<double>(end - start).count() / 1e6;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 2: With atomic counter on h4 only (like raw test)
// ═══════════════════════════════════════════════════════════════════════════

double testWithH4Counter() {
    RingBufferType ring;
    lmax::Sequence h1Seq, h2Seq, h3Seq, h4Seq;

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
                h4Count.fetch_add(1, std::memory_order_relaxed);  // Only h4 has counter
                h4Seq.set(next);
                next++;
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        int64_t seq = ring.next();
        ring.get(seq)->value = i;
        ring.publish(seq);
    }

    while (h4Count.load() < NUM_EVENTS) std::this_thread::yield();

    auto end = std::chrono::high_resolution_clock::now();

    done.store(true);
    h1Barrier.alert(); h2Barrier.alert(); h3Barrier.alert(); h4Barrier.alert();
    t1.join(); t2.join(); t3.join(); t4.join();

    return NUM_EVENTS / std::chrono::duration<double>(end - start).count() / 1e6;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 3: With atomic counter on ALL 4 handlers (like DSL SimpleHandler)
// ═══════════════════════════════════════════════════════════════════════════

double testWithAllCounters() {
    RingBufferType ring;
    lmax::Sequence h1Seq, h2Seq, h3Seq, h4Seq;

    auto h1Barrier = ring.newBarrier();
    auto h2Barrier = ring.newBarrier();
    auto h3Barrier = ring.newBarrier();
    auto h4Barrier = ring.newBarrier({&h1Seq, &h2Seq, &h3Seq});

    ring.addGatingSequence(h4Seq);

    std::atomic<bool> done{false};
    std::atomic<size_t> count1{0}, count2{0}, count3{0}, count4{0};

    auto consumer = [&](auto& barrier, lmax::Sequence& seq, std::atomic<size_t>& count) {
        int64_t next = 0;
        while (!done.load(std::memory_order_relaxed) || next <= ring.getCursor()) {
            int64_t avail = barrier.waitFor(next);
            if (avail == lmax::ALERTED) break;
            while (next <= avail) {
                count.fetch_add(1, std::memory_order_relaxed);  // Each has counter
                seq.set(next);
                next++;
            }
        }
    };

    std::thread t1([&]() { consumer(h1Barrier, h1Seq, count1); });
    std::thread t2([&]() { consumer(h2Barrier, h2Seq, count2); });
    std::thread t3([&]() { consumer(h3Barrier, h3Seq, count3); });
    std::thread t4([&]() {
        int64_t next = 0;
        while (!done.load(std::memory_order_relaxed) ||
               next <= h1Seq.get() || next <= h2Seq.get() || next <= h3Seq.get()) {
            int64_t avail = h4Barrier.waitFor(next);
            if (avail == lmax::ALERTED) break;
            while (next <= avail) {
                count4.fetch_add(1, std::memory_order_relaxed);
                h4Seq.set(next);
                next++;
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        int64_t seq = ring.next();
        ring.get(seq)->value = i;
        ring.publish(seq);
    }

    while (count4.load() < NUM_EVENTS) std::this_thread::yield();

    auto end = std::chrono::high_resolution_clock::now();

    done.store(true);
    h1Barrier.alert(); h2Barrier.alert(); h3Barrier.alert(); h4Barrier.alert();
    t1.join(); t2.join(); t3.join(); t4.join();

    return NUM_EVENTS / std::chrono::duration<double>(end - start).count() / 1e6;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4: DSL
// ═══════════════════════════════════════════════════════════════════════════

class SimpleHandler : public lmax::EventHandler<TestEvent> {
public:
    std::atomic<size_t> count{0};
    void onEvent(TestEvent& event, int64_t sequence, bool endOfBatch) override {
        count.fetch_add(1, std::memory_order_relaxed);
    }
};

double testDSL() {
    lmax::Disruptor<TestEvent, BUFFER_SIZE, lmax::BusySpinWait> disruptor;

    SimpleHandler h1, h2, h3, h4;

    auto& g1 = disruptor.handleEventsWith(h1);
    auto& g2 = disruptor.handleEventsWith(h2);
    auto& g3 = disruptor.handleEventsWith(h3);
    disruptor.after(g1, g2, g3).handleEventsWith(h4);

    disruptor.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        disruptor.publishEvent([i](TestEvent& e, int64_t seq) {
            e.value = i;
        });
    }

    while (h4.count.load() < NUM_EVENTS) std::this_thread::yield();

    auto end = std::chrono::high_resolution_clock::now();

    disruptor.shutdown();

    return NUM_EVENTS / std::chrono::duration<double>(end - start).count() / 1e6;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 5: DSL with NoOp handler (no atomic counter)
// ═══════════════════════════════════════════════════════════════════════════

class NoOpHandler : public lmax::EventHandler<TestEvent> {
public:
    void onEvent(TestEvent& event, int64_t sequence, bool endOfBatch) override {
        // Do nothing - just the virtual call overhead
    }
};

double testDSLNoOp() {
    lmax::Disruptor<TestEvent, BUFFER_SIZE, lmax::BusySpinWait> disruptor;

    NoOpHandler h1, h2, h3;
    SimpleHandler h4;  // Need counter on h4 to know when done

    auto& g1 = disruptor.handleEventsWith(h1);
    auto& g2 = disruptor.handleEventsWith(h2);
    auto& g3 = disruptor.handleEventsWith(h3);
    disruptor.after(g1, g2, g3).handleEventsWith(h4);

    disruptor.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        disruptor.publishEvent([i](TestEvent& e, int64_t seq) {
            e.value = i;
        });
    }

    while (h4.count.load() < NUM_EVENTS) std::this_thread::yield();

    auto end = std::chrono::high_resolution_clock::now();

    disruptor.shutdown();

    return NUM_EVENTS / std::chrono::duration<double>(end - start).count() / 1e6;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 6: DSL with NoExcept handler (no try-catch in hot path)
// ═══════════════════════════════════════════════════════════════════════════

class NoExceptHandler : public lmax::NoExceptEventHandler<TestEvent> {
public:
    std::atomic<size_t> count{0};
    lmax::ProcessResult onEvent(TestEvent& event, int64_t sequence, bool endOfBatch) noexcept override {
        count.fetch_add(1, std::memory_order_relaxed);
        return lmax::ProcessResult::OK;
    }
};

double testDSLNoExcept() {
    lmax::Disruptor<TestEvent, BUFFER_SIZE, lmax::BusySpinWait> disruptor;

    NoExceptHandler h1, h2, h3, h4;

    auto& g1 = disruptor.handleEventsWith(h1);
    auto& g2 = disruptor.handleEventsWith(h2);
    auto& g3 = disruptor.handleEventsWith(h3);
    disruptor.after(g1, g2, g3).handleEventsWith(h4);

    disruptor.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        disruptor.publishEvent([i](TestEvent& e, int64_t seq) {
            e.value = i;
        });
    }

    while (h4.count.load() < NUM_EVENTS) std::this_thread::yield();

    auto end = std::chrono::high_resolution_clock::now();

    disruptor.shutdown();

    return NUM_EVENTS / std::chrono::duration<double>(end - start).count() / 1e6;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 7: DSL NoExcept with NoOp handlers (pure DSL overhead without try-catch)
// ═══════════════════════════════════════════════════════════════════════════

class NoExceptNoOpHandler : public lmax::NoExceptEventHandler<TestEvent> {
public:
    lmax::ProcessResult onEvent(TestEvent& event, int64_t sequence, bool endOfBatch) noexcept override {
        return lmax::ProcessResult::OK;  // Do nothing
    }
};

double testDSLNoExceptNoOp() {
    lmax::Disruptor<TestEvent, BUFFER_SIZE, lmax::BusySpinWait> disruptor;

    NoExceptNoOpHandler h1, h2, h3;
    NoExceptHandler h4;  // Need counter on h4 to know when done

    auto& g1 = disruptor.handleEventsWith(h1);
    auto& g2 = disruptor.handleEventsWith(h2);
    auto& g3 = disruptor.handleEventsWith(h3);
    disruptor.after(g1, g2, g3).handleEventsWith(h4);

    disruptor.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        disruptor.publishEvent([i](TestEvent& e, int64_t seq) {
            e.value = i;
        });
    }

    while (h4.count.load() < NUM_EVENTS) std::this_thread::yield();

    auto end = std::chrono::high_resolution_clock::now();

    disruptor.shutdown();

    return NUM_EVENTS / std::chrono::duration<double>(end - start).count() / 1e6;
}

int main() {
    std::cout << "═══════════════════════════════════════════════════════════════════\n";
    std::cout << "              Isolated Overhead Analysis\n";
    std::cout << "              Pattern: (h1, h2, h3) -> h4\n";
    std::cout << "═══════════════════════════════════════════════════════════════════\n\n";

    std::cout << std::fixed << std::setprecision(2);

    // Warmup
    testMinimal();
    testDSL();

    double minimal = testMinimal();
    std::cout << "1. Minimal (no counters):           " << std::setw(7) << minimal << " M ops/sec (baseline)\n";

    double h4Counter = testWithH4Counter();
    std::cout << "2. + Counter on h4 only:            " << std::setw(7) << h4Counter
              << " M ops/sec  (" << (minimal/h4Counter - 1)*100 << "% slower)\n";

    double allCounters = testWithAllCounters();
    std::cout << "3. + Counter on ALL handlers:       " << std::setw(7) << allCounters
              << " M ops/sec  (" << (minimal/allCounters - 1)*100 << "% slower)\n";

    double dslNoOp = testDSLNoOp();
    std::cout << "4. DSL with NoOp (h1-h3 no counter):" << std::setw(7) << dslNoOp
              << " M ops/sec  (" << (minimal/dslNoOp - 1)*100 << "% slower)\n";

    double dsl = testDSL();
    std::cout << "5. DSL with counters on all:        " << std::setw(7) << dsl
              << " M ops/sec  (" << (minimal/dsl - 1)*100 << "% slower)\n";

    double dslNoExcept = testDSLNoExcept();
    std::cout << "6. DSL NoExcept with counters:      " << std::setw(7) << dslNoExcept
              << " M ops/sec  (" << (minimal/dslNoExcept - 1)*100 << "% slower)\n";

    double dslNoExceptNoOp = testDSLNoExceptNoOp();
    std::cout << "7. DSL NoExcept NoOp (h1-h3):       " << std::setw(7) << dslNoExceptNoOp
              << " M ops/sec  (" << (minimal/dslNoExceptNoOp - 1)*100 << "% slower)\n";

    std::cout << "\n═══════════════════════════════════════════════════════════════════\n";
    std::cout << "Analysis:\n";
    std::cout << "  - Counter overhead (h4 only):      " << std::setprecision(1) << (minimal/h4Counter - 1)*100 << "%\n";
    std::cout << "  - Counter overhead (all 4):        " << (minimal/allCounters - 1)*100 << "%\n";
    std::cout << "  - DSL overhead (with try-catch):   " << (dslNoOp > 0 ? (minimal/dslNoOp - 1)*100 : 0) << "%\n";
    std::cout << "  - DSL overhead (noexcept):         " << (dslNoExceptNoOp > 0 ? (minimal/dslNoExceptNoOp - 1)*100 : 0) << "%\n";
    std::cout << "  - Try-catch overhead:              " << (dslNoExceptNoOp/dslNoOp - 1)*100 << "%\n";
    std::cout << "═══════════════════════════════════════════════════════════════════\n";

    return 0;
}
