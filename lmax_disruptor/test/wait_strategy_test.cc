/*
 * Wait Strategy Test - Tests BatchEventProcessor with multiple wait strategies
 *
 * Validates that the alert mechanism and waitFor() work correctly with:
 * - BusySpinWait (lowest latency, highest CPU)
 * - YieldingWait (balanced)
 * - SleepingWait (lower CPU, higher latency)
 * - BackoffWait (exponential backoff)
 * - LiteWait (minimal pause)
 */

#include <iostream>
#include <iomanip>
#include <thread>
#include <atomic>
#include <chrono>
#include <cassert>
#include <string>
#include "../include/lmax/disruptor.h"

struct TestEvent {
    int64_t value;
    bool processed;
};

// Test event handler that counts processed events
template<typename T>
class CountingHandler : public lmax::EventHandler<T> {
public:
    std::atomic<int64_t> count{0};
    std::atomic<int64_t> sum{0};
    std::atomic<bool> started{false};
    std::atomic<bool> shutdown{false};

    void onEvent(T& event, int64_t sequence, bool endOfBatch) override {
        event.processed = true;
        sum.fetch_add(event.value, std::memory_order_relaxed);
        count.fetch_add(1, std::memory_order_relaxed);
    }

    void onStart() override {
        started.store(true, std::memory_order_release);
    }

    void onShutdown() override {
        shutdown.store(true, std::memory_order_release);
    }
};

// Test with a specific wait strategy
template<typename WaitStrategy>
bool testWaitStrategy(const std::string& name, size_t numEvents) {
    std::cout << "Testing " << name << "..." << std::flush;

    using RingBufferType = lmax::RingBuffer<TestEvent, 1024, WaitStrategy>;
    RingBufferType ring;

    // Create barrier and handler
    auto barrier = ring.newBarrier();
    CountingHandler<TestEvent> handler;

    // Create processor
    lmax::BatchEventProcessor<TestEvent, RingBufferType, decltype(barrier)>
        processor(ring, barrier, handler);

    // Add processor's sequence as gating sequence
    ring.addGatingSequence(processor.getSequence());

    // Start processor
    processor.start();

    // Wait for processor to start
    while (!handler.started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    // Publish events
    int64_t expectedSum = 0;
    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < numEvents; i++) {
        int64_t seq = ring.next();
        TestEvent* e = ring.get(seq);
        e->value = static_cast<int64_t>(i + 1);
        e->processed = false;
        expectedSum += e->value;
        ring.publish(seq);
    }

    // Wait for all events to be processed
    while (handler.count.load(std::memory_order_relaxed) < static_cast<int64_t>(numEvents)) {
        std::this_thread::yield();
    }

    auto end = std::chrono::high_resolution_clock::now();

    // Halt processor (uses alert mechanism)
    processor.halt();
    processor.join();

    // Verify results
    bool success = true;

    if (handler.count.load() != static_cast<int64_t>(numEvents)) {
        std::cout << " FAILED (count mismatch: " << handler.count.load()
                  << " != " << numEvents << ")\n";
        success = false;
    }

    if (handler.sum.load() != expectedSum) {
        std::cout << " FAILED (sum mismatch: " << handler.sum.load()
                  << " != " << expectedSum << ")\n";
        success = false;
    }

    if (!handler.shutdown.load()) {
        std::cout << " FAILED (onShutdown not called)\n";
        success = false;
    }

    if (processor.getState() != lmax::ProcessorState::HALTED) {
        std::cout << " FAILED (processor not halted)\n";
        success = false;
    }

    if (success) {
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughput = numEvents / seconds / 1e6;
        std::cout << " PASSED (" << std::fixed << std::setprecision(2)
                  << throughput << " M events/sec)\n";
    }

    return success;
}

// Test shutdown while waiting (no events published)
template<typename WaitStrategy>
bool testShutdownWhileWaiting(const std::string& name) {
    std::cout << "Testing " << name << " shutdown while waiting..." << std::flush;

    using RingBufferType = lmax::RingBuffer<TestEvent, 1024, WaitStrategy>;
    RingBufferType ring;

    auto barrier = ring.newBarrier();
    CountingHandler<TestEvent> handler;

    lmax::BatchEventProcessor<TestEvent, RingBufferType, decltype(barrier)>
        processor(ring, barrier, handler);

    ring.addGatingSequence(processor.getSequence());

    // Start processor
    processor.start();

    // Wait for processor to start
    while (!handler.started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    // Don't publish any events - processor should be waiting in waitFor()
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Halt should alert the barrier and wake up the processor
    auto start = std::chrono::high_resolution_clock::now();
    processor.halt();
    processor.join();
    auto end = std::chrono::high_resolution_clock::now();

    double ms = std::chrono::duration<double, std::milli>(end - start).count();

    bool success = true;

    if (!handler.shutdown.load()) {
        std::cout << " FAILED (onShutdown not called)\n";
        success = false;
    }

    if (processor.getState() != lmax::ProcessorState::HALTED) {
        std::cout << " FAILED (processor not halted)\n";
        success = false;
    }

    // Shutdown should be fast (< 100ms) due to alert
    if (ms > 100) {
        std::cout << " FAILED (shutdown took too long: " << ms << "ms)\n";
        success = false;
    }

    if (success) {
        std::cout << " PASSED (shutdown in " << std::fixed << std::setprecision(2)
                  << ms << "ms)\n";
    }

    return success;
}

// Test pipeline with dependencies
template<typename WaitStrategy>
bool testPipeline(const std::string& name, size_t numEvents) {
    std::cout << "Testing " << name << " pipeline..." << std::flush;

    using RingBufferType = lmax::RingBuffer<TestEvent, 1024, WaitStrategy>;
    RingBufferType ring;

    // Handler 1 - first stage
    CountingHandler<TestEvent> handler1;
    auto barrier1 = ring.newBarrier();
    lmax::BatchEventProcessor<TestEvent, RingBufferType, decltype(barrier1)>
        processor1(ring, barrier1, handler1);

    // Handler 2 - second stage (depends on handler1)
    CountingHandler<TestEvent> handler2;
    auto barrier2 = ring.newBarrier({&processor1.getSequence()});
    lmax::BatchEventProcessor<TestEvent, RingBufferType, decltype(barrier2)>
        processor2(ring, barrier2, handler2);

    // Gate on final processor
    ring.addGatingSequence(processor2.getSequence());

    // Start processors (order matters for dependencies)
    processor1.start();
    processor2.start();

    // Wait for both to start
    while (!handler1.started.load() || !handler2.started.load()) {
        std::this_thread::yield();
    }

    // Publish events
    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < numEvents; i++) {
        int64_t seq = ring.next();
        TestEvent* e = ring.get(seq);
        e->value = static_cast<int64_t>(i + 1);
        e->processed = false;
        ring.publish(seq);
    }

    // Wait for all events to be processed by both handlers
    while (handler2.count.load(std::memory_order_relaxed) < static_cast<int64_t>(numEvents)) {
        std::this_thread::yield();
    }

    auto end = std::chrono::high_resolution_clock::now();

    // Halt processors (reverse order is cleaner but not required)
    processor2.halt();
    processor1.halt();
    processor2.join();
    processor1.join();

    bool success = true;

    if (handler1.count.load() != static_cast<int64_t>(numEvents)) {
        std::cout << " FAILED (handler1 count: " << handler1.count.load() << ")\n";
        success = false;
    }

    if (handler2.count.load() != static_cast<int64_t>(numEvents)) {
        std::cout << " FAILED (handler2 count: " << handler2.count.load() << ")\n";
        success = false;
    }

    if (success) {
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughput = numEvents / seconds / 1e6;
        std::cout << " PASSED (" << std::fixed << std::setprecision(2)
                  << throughput << " M events/sec)\n";
    }

    return success;
}

int main() {
    std::cout << "═══════════════════════════════════════════════════════════════════\n";
    std::cout << "          Wait Strategy Tests for BatchEventProcessor\n";
    std::cout << "═══════════════════════════════════════════════════════════════════\n\n";

    constexpr size_t NUM_EVENTS = 100000;
    int passed = 0;
    int failed = 0;

    std::cout << "--- Basic Processing Tests ---\n";

    if (testWaitStrategy<lmax::BusySpinWait>("BusySpinWait", NUM_EVENTS)) passed++; else failed++;
    if (testWaitStrategy<lmax::YieldingWait>("YieldingWait", NUM_EVENTS)) passed++; else failed++;
    if (testWaitStrategy<lmax::SleepingWait>("SleepingWait", NUM_EVENTS)) passed++; else failed++;
    if (testWaitStrategy<lmax::BackoffWait>("BackoffWait", NUM_EVENTS)) passed++; else failed++;
    if (testWaitStrategy<lmax::LiteWait>("LiteWait", NUM_EVENTS)) passed++; else failed++;

    std::cout << "\n--- Shutdown While Waiting Tests (alert mechanism) ---\n";

    if (testShutdownWhileWaiting<lmax::BusySpinWait>("BusySpinWait")) passed++; else failed++;
    if (testShutdownWhileWaiting<lmax::YieldingWait>("YieldingWait")) passed++; else failed++;
    if (testShutdownWhileWaiting<lmax::SleepingWait>("SleepingWait")) passed++; else failed++;
    if (testShutdownWhileWaiting<lmax::BackoffWait>("BackoffWait")) passed++; else failed++;
    if (testShutdownWhileWaiting<lmax::LiteWait>("LiteWait")) passed++; else failed++;

    std::cout << "\n--- Pipeline Tests (with dependencies) ---\n";

    if (testPipeline<lmax::BusySpinWait>("BusySpinWait", NUM_EVENTS)) passed++; else failed++;
    if (testPipeline<lmax::YieldingWait>("YieldingWait", NUM_EVENTS)) passed++; else failed++;
    if (testPipeline<lmax::SleepingWait>("SleepingWait", NUM_EVENTS)) passed++; else failed++;

    std::cout << "\n═══════════════════════════════════════════════════════════════════\n";
    std::cout << "Results: " << passed << " passed, " << failed << " failed\n";
    std::cout << "═══════════════════════════════════════════════════════════════════\n";

    return failed == 0 ? 0 : 1;
}
