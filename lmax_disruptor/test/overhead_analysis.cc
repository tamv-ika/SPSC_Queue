/*
 * Overhead Analysis - Identify sources of DSL overhead
 *
 * Compare:
 * 1. Raw threads (baseline)
 * 2. Raw + atomic counter (like SimpleHandler)
 * 3. Raw + virtual call
 * 4. Raw + try-catch
 * 5. Raw + state check
 * 6. Full BatchEventProcessor
 */

#include <iostream>
#include <iomanip>
#include <thread>
#include <atomic>
#include <chrono>
#include <memory>
#include "../include/lmax/disruptor.h"

struct TestEvent {
    int64_t value;
};

constexpr size_t NUM_EVENTS = 1000000;
constexpr size_t BUFFER_SIZE = 65536;

using RingBufferType = lmax::RingBuffer<TestEvent, BUFFER_SIZE, lmax::BusySpinWait>;

// ═══════════════════════════════════════════════════════════════════════════
// Test 1: Raw baseline (minimal work)
// ═══════════════════════════════════════════════════════════════════════════

double testRawBaseline() {
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
                h4Seq.set(next);
                h4Count.fetch_add(1, std::memory_order_relaxed);
                next++;
            }
        }
    });

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
// Test 2: Raw + atomic counter per event (like SimpleHandler)
// ═══════════════════════════════════════════════════════════════════════════

double testWithAtomicCounter() {
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
                count.fetch_add(1, std::memory_order_relaxed);  // Added atomic increment
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
// Test 3: Raw + virtual function call
// ═══════════════════════════════════════════════════════════════════════════

class IHandler {
public:
    virtual ~IHandler() = default;
    virtual void onEvent(TestEvent& e, int64_t seq) = 0;
};

class CountingHandlerV : public IHandler {
public:
    std::atomic<size_t> count{0};
    void onEvent(TestEvent& e, int64_t seq) override {
        count.fetch_add(1, std::memory_order_relaxed);
    }
};

double testWithVirtualCall() {
    RingBufferType ring;
    lmax::Sequence h1Seq, h2Seq, h3Seq, h4Seq;

    auto h1Barrier = ring.newBarrier();
    auto h2Barrier = ring.newBarrier();
    auto h3Barrier = ring.newBarrier();
    auto h4Barrier = ring.newBarrier({&h1Seq, &h2Seq, &h3Seq});

    ring.addGatingSequence(h4Seq);

    std::atomic<bool> done{false};
    CountingHandlerV handler1, handler2, handler3, handler4;

    auto consumer = [&](auto& barrier, lmax::Sequence& seq, IHandler& handler) {
        int64_t next = 0;
        while (!done.load(std::memory_order_relaxed) || next <= ring.getCursor()) {
            int64_t avail = barrier.waitFor(next);
            if (avail == lmax::ALERTED) break;
            while (next <= avail) {
                TestEvent* e = ring.get(next);
                handler.onEvent(*e, next);  // Virtual call
                seq.set(next);
                next++;
            }
        }
    };

    std::thread t1([&]() { consumer(h1Barrier, h1Seq, handler1); });
    std::thread t2([&]() { consumer(h2Barrier, h2Seq, handler2); });
    std::thread t3([&]() { consumer(h3Barrier, h3Seq, handler3); });
    std::thread t4([&]() {
        int64_t next = 0;
        while (!done.load(std::memory_order_relaxed) ||
               next <= h1Seq.get() || next <= h2Seq.get() || next <= h3Seq.get()) {
            int64_t avail = h4Barrier.waitFor(next);
            if (avail == lmax::ALERTED) break;
            while (next <= avail) {
                TestEvent* e = ring.get(next);
                handler4.onEvent(*e, next);
                h4Seq.set(next);
                next++;
            }
        }
    });

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        int64_t seq = ring.next();
        ring.get(seq)->value = i;
        ring.publish(seq);
    }

    while (handler4.count.load() < NUM_EVENTS) std::this_thread::yield();

    auto end = std::chrono::high_resolution_clock::now();

    done.store(true);
    h1Barrier.alert(); h2Barrier.alert(); h3Barrier.alert(); h4Barrier.alert();
    t1.join(); t2.join(); t3.join(); t4.join();

    return NUM_EVENTS / std::chrono::duration<double>(end - start).count() / 1e6;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4: Raw + try-catch
// ═══════════════════════════════════════════════════════════════════════════

double testWithTryCatch() {
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
                try {
                    count.fetch_add(1, std::memory_order_relaxed);
                } catch (...) {
                    // Exception handling
                }
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
                try {
                    count4.fetch_add(1, std::memory_order_relaxed);
                } catch (...) {}
                h4Seq.set(next);
                next++;
            }
        }
    });

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
// Test 5: Raw + state check in loop (like BatchEventProcessor)
// ═══════════════════════════════════════════════════════════════════════════

double testWithStateCheck() {
    RingBufferType ring;
    lmax::Sequence h1Seq, h2Seq, h3Seq, h4Seq;

    auto h1Barrier = ring.newBarrier();
    auto h2Barrier = ring.newBarrier();
    auto h3Barrier = ring.newBarrier();
    auto h4Barrier = ring.newBarrier({&h1Seq, &h2Seq, &h3Seq});

    ring.addGatingSequence(h4Seq);

    enum class State { RUNNING, HALTING };
    std::atomic<State> state1{State::RUNNING}, state2{State::RUNNING};
    std::atomic<State> state3{State::RUNNING}, state4{State::RUNNING};
    std::atomic<size_t> count1{0}, count2{0}, count3{0}, count4{0};

    auto consumer = [&](auto& barrier, lmax::Sequence& seq,
                       std::atomic<State>& state, std::atomic<size_t>& count) {
        int64_t next = 0;
        while (state.load(std::memory_order_acquire) == State::RUNNING) {  // State check
            int64_t avail = barrier.waitFor(next);
            if (avail == lmax::ALERTED) break;
            while (next <= avail) {
                count.fetch_add(1, std::memory_order_relaxed);
                seq.set(next);
                next++;
            }
        }
    };

    std::thread t1([&]() { consumer(h1Barrier, h1Seq, state1, count1); });
    std::thread t2([&]() { consumer(h2Barrier, h2Seq, state2, count2); });
    std::thread t3([&]() { consumer(h3Barrier, h3Seq, state3, count3); });
    std::thread t4([&]() {
        int64_t next = 0;
        while (state4.load(std::memory_order_acquire) == State::RUNNING) {
            int64_t avail = h4Barrier.waitFor(next);
            if (avail == lmax::ALERTED) break;
            while (next <= avail) {
                count4.fetch_add(1, std::memory_order_relaxed);
                h4Seq.set(next);
                next++;
            }
        }
    });

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        int64_t seq = ring.next();
        ring.get(seq)->value = i;
        ring.publish(seq);
    }

    while (count4.load() < NUM_EVENTS) std::this_thread::yield();

    auto end = std::chrono::high_resolution_clock::now();

    state1.store(State::HALTING); state2.store(State::HALTING);
    state3.store(State::HALTING); state4.store(State::HALTING);
    h1Barrier.alert(); h2Barrier.alert(); h3Barrier.alert(); h4Barrier.alert();
    t1.join(); t2.join(); t3.join(); t4.join();

    return NUM_EVENTS / std::chrono::duration<double>(end - start).count() / 1e6;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 6: Full simulation (virtual + try-catch + state)
// ═══════════════════════════════════════════════════════════════════════════

double testFullSimulation() {
    RingBufferType ring;
    lmax::Sequence h1Seq, h2Seq, h3Seq, h4Seq;

    auto h1Barrier = ring.newBarrier();
    auto h2Barrier = ring.newBarrier();
    auto h3Barrier = ring.newBarrier();
    auto h4Barrier = ring.newBarrier({&h1Seq, &h2Seq, &h3Seq});

    ring.addGatingSequence(h4Seq);

    enum class State { RUNNING, HALTING };
    std::atomic<State> state1{State::RUNNING}, state2{State::RUNNING};
    std::atomic<State> state3{State::RUNNING}, state4{State::RUNNING};
    CountingHandlerV handler1, handler2, handler3, handler4;

    auto consumer = [&](auto& barrier, lmax::Sequence& seq,
                       std::atomic<State>& state, IHandler& handler) {
        int64_t next = 0;
        while (state.load(std::memory_order_acquire) == State::RUNNING) {
            int64_t avail = barrier.waitFor(next);
            if (avail == lmax::ALERTED) break;
            while (next <= avail) {
                TestEvent* e = ring.get(next);
                try {
                    handler.onEvent(*e, next);  // Virtual + try-catch
                } catch (...) {}
                seq.set(next);
                next++;
            }
        }
    };

    std::thread t1([&]() { consumer(h1Barrier, h1Seq, state1, handler1); });
    std::thread t2([&]() { consumer(h2Barrier, h2Seq, state2, handler2); });
    std::thread t3([&]() { consumer(h3Barrier, h3Seq, state3, handler3); });
    std::thread t4([&]() {
        int64_t next = 0;
        while (state4.load(std::memory_order_acquire) == State::RUNNING) {
            int64_t avail = h4Barrier.waitFor(next);
            if (avail == lmax::ALERTED) break;
            while (next <= avail) {
                TestEvent* e = ring.get(next);
                try {
                    handler4.onEvent(*e, next);
                } catch (...) {}
                h4Seq.set(next);
                next++;
            }
        }
    });

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_EVENTS; i++) {
        int64_t seq = ring.next();
        ring.get(seq)->value = i;
        ring.publish(seq);
    }

    while (handler4.count.load() < NUM_EVENTS) std::this_thread::yield();

    auto end = std::chrono::high_resolution_clock::now();

    state1.store(State::HALTING); state2.store(State::HALTING);
    state3.store(State::HALTING); state4.store(State::HALTING);
    h1Barrier.alert(); h2Barrier.alert(); h3Barrier.alert(); h4Barrier.alert();
    t1.join(); t2.join(); t3.join(); t4.join();

    return NUM_EVENTS / std::chrono::duration<double>(end - start).count() / 1e6;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 7: DSL (actual BatchEventProcessor)
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
    std::cout << "              DSL Overhead Analysis\n";
    std::cout << "              Pattern: (h1, h2, h3) -> h4\n";
    std::cout << "═══════════════════════════════════════════════════════════════════\n\n";

    // Warmup
    testRawBaseline();
    testDSL();

    std::cout << std::fixed << std::setprecision(2);

    double baseline = testRawBaseline();
    std::cout << "1. Raw baseline (minimal):        " << std::setw(7) << baseline << " M ops/sec\n";

    double withCounter = testWithAtomicCounter();
    std::cout << "2. + Atomic counter per event:    " << std::setw(7) << withCounter
              << " M ops/sec  (" << std::setw(5) << (baseline/withCounter - 1)*100 << "% slower)\n";

    double withVirtual = testWithVirtualCall();
    std::cout << "3. + Virtual function call:       " << std::setw(7) << withVirtual
              << " M ops/sec  (" << std::setw(5) << (baseline/withVirtual - 1)*100 << "% slower)\n";

    double withTryCatch = testWithTryCatch();
    std::cout << "4. + Try-catch block:             " << std::setw(7) << withTryCatch
              << " M ops/sec  (" << std::setw(5) << (baseline/withTryCatch - 1)*100 << "% slower)\n";

    double withState = testWithStateCheck();
    std::cout << "5. + State check in loop:         " << std::setw(7) << withState
              << " M ops/sec  (" << std::setw(5) << (baseline/withState - 1)*100 << "% slower)\n";

    double fullSim = testFullSimulation();
    std::cout << "6. Full simulation (all above):   " << std::setw(7) << fullSim
              << " M ops/sec  (" << std::setw(5) << (baseline/fullSim - 1)*100 << "% slower)\n";

    double dsl = testDSL();
    std::cout << "7. Actual DSL (BatchProcessor):   " << std::setw(7) << dsl
              << " M ops/sec  (" << std::setw(5) << (baseline/dsl - 1)*100 << "% slower)\n";

    std::cout << "\n═══════════════════════════════════════════════════════════════════\n";
    std::cout << "Summary: DSL is " << std::setprecision(1) << (baseline/dsl) << "x slower than raw baseline\n";
    std::cout << "═══════════════════════════════════════════════════════════════════\n";

    return 0;
}
