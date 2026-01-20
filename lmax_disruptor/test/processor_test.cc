/*
 * Batch Event Processor and Pipeline Test
 *
 * Tests:
 * - BatchEventProcessor with EventHandler
 * - Pipelined (chained) processors
 */

#include <iostream>
#include <thread>
#include <cassert>
#include <atomic>
#include <vector>
#include <chrono>
#include "../include/lmax/disruptor.h"

struct OrderEvent {
    uint64_t orderId;
    uint64_t price;
    uint64_t quantity;
    bool walProcessed;
    bool replicatorProcessed;
    bool matchingEngineProcessed;
};

// ═══════════════════════════════════════════════════════════════════════════
// TEST: Basic Batch Processor
// ═══════════════════════════════════════════════════════════════════════════

class CountingHandler : public lmax::EventHandler<OrderEvent> {
public:
    std::atomic<uint64_t> count{0};
    std::atomic<uint64_t> sum{0};
    std::atomic<int> batchCount{0};

    void onEvent(OrderEvent& event, int64_t /*sequence*/, bool endOfBatch) override {
        count.fetch_add(1, std::memory_order_relaxed);
        sum.fetch_add(event.orderId, std::memory_order_relaxed);
        if (endOfBatch) {
            batchCount.fetch_add(1, std::memory_order_relaxed);
        }
    }
};

void testBasicBatchProcessor() {
    std::cout << "Test: Basic batch processor... ";

    lmax::RingBuffer<OrderEvent, 1024> ring;
    auto barrier = ring.newBarrier();
    CountingHandler handler;

    lmax::BatchEventProcessor<OrderEvent, decltype(ring), decltype(barrier)>
        processor(ring, barrier, handler);

    // Add processor's sequence as gating (so producer waits for consumer)
    ring.addGatingSequence(processor.getSequence());

    // Start processor
    processor.start();

    // Publish events
    constexpr int NUM_EVENTS = 1000;
    uint64_t expectedSum = 0;

    for (int i = 0; i < NUM_EVENTS; i++) {
        int64_t seq = ring.next();
        OrderEvent* e = ring.get(seq);
        e->orderId = i;
        e->price = i * 100;
        e->quantity = i * 10;
        expectedSum += i;
        ring.publish(seq);
    }

    // Wait for processor to catch up (with timeout)
    auto start = std::chrono::steady_clock::now();
    while (handler.count.load(std::memory_order_relaxed) < NUM_EVENTS) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed > std::chrono::seconds(5)) {
            std::cerr << "TIMEOUT waiting for events\n";
            break;
        }
    }

    // Stop processor
    processor.halt();
    processor.join();

    assert(handler.count.load() == NUM_EVENTS);
    assert(handler.sum.load() == expectedSum);
    assert(handler.batchCount.load() > 0);  // Should have processed in batches

    std::cout << "PASSED (" << NUM_EVENTS << " events in "
              << handler.batchCount.load() << " batches)\n";
}

// ═══════════════════════════════════════════════════════════════════════════
// TEST: Pipeline (WAL -> Replicator -> Matching Engine) - Non-blocking
// ═══════════════════════════════════════════════════════════════════════════

void testPipelineManual() {
    std::cout << "Test: Pipeline (manual setup)... ";

    constexpr int NUM_EVENTS = 10000;

    lmax::RingBuffer<OrderEvent, 8192> ring;

    // Create sequences for each stage
    lmax::Sequence walSeq;
    lmax::Sequence replSeq;
    lmax::Sequence meSeq;

    // Producer is gated by the slowest (final) consumer
    ring.addGatingSequence(meSeq);

    std::atomic<bool> done{false};
    std::atomic<uint64_t> meCount{0};

    // WAL thread - uses polling instead of blocking waitFor
    std::thread walThread([&]() {
        int64_t nextSeq = 0;
        while (!done.load(std::memory_order_relaxed) || nextSeq <= ring.getCursor()) {
            int64_t available = ring.available();  // Non-blocking
            if (available < nextSeq) {
                std::this_thread::yield();
                continue;
            }
            while (nextSeq <= available) {
                OrderEvent* e = const_cast<OrderEvent*>(ring.get(nextSeq));
                e->walProcessed = true;
                nextSeq++;
            }
            walSeq.set(nextSeq - 1);
        }
    });

    // Replicator thread
    std::thread replThread([&]() {
        int64_t nextSeq = 0;
        while (!done.load(std::memory_order_relaxed) || nextSeq <= walSeq.get()) {
            int64_t available = walSeq.get();  // Non-blocking
            if (available < nextSeq) {
                std::this_thread::yield();
                continue;
            }
            while (nextSeq <= available) {
                OrderEvent* e = const_cast<OrderEvent*>(ring.get(nextSeq));
                assert(e->walProcessed);
                e->replicatorProcessed = true;
                nextSeq++;
            }
            replSeq.set(nextSeq - 1);
        }
    });

    // Matching Engine thread
    std::thread meThread([&]() {
        int64_t nextSeq = 0;
        while (!done.load(std::memory_order_relaxed) || nextSeq <= replSeq.get()) {
            int64_t available = replSeq.get();  // Non-blocking
            if (available < nextSeq) {
                std::this_thread::yield();
                continue;
            }
            while (nextSeq <= available) {
                OrderEvent* e = const_cast<OrderEvent*>(ring.get(nextSeq));
                assert(e->walProcessed);
                assert(e->replicatorProcessed);
                e->matchingEngineProcessed = true;
                meCount.fetch_add(1, std::memory_order_relaxed);
                nextSeq++;
            }
            meSeq.set(nextSeq - 1);
        }
    });

    // Producer
    for (int i = 0; i < NUM_EVENTS; i++) {
        int64_t seq = ring.next();
        OrderEvent* e = ring.get(seq);
        e->orderId = i;
        e->price = i * 100;
        e->quantity = i * 10;
        e->walProcessed = false;
        e->replicatorProcessed = false;
        e->matchingEngineProcessed = false;
        ring.publish(seq);
    }

    // Wait for all events to be processed (with timeout)
    auto start = std::chrono::steady_clock::now();
    while (meCount.load(std::memory_order_relaxed) < NUM_EVENTS) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed > std::chrono::seconds(5)) {
            std::cerr << "TIMEOUT: meCount=" << meCount.load() << "/" << NUM_EVENTS << "\n";
            break;
        }
    }

    done.store(true, std::memory_order_release);

    walThread.join();
    replThread.join();
    meThread.join();

    assert(meCount.load() == NUM_EVENTS);

    std::cout << "PASSED (" << NUM_EVENTS << " events through 3-stage pipeline)\n";
}

// ═══════════════════════════════════════════════════════════════════════════
// TEST: Functional Handler
// ═══════════════════════════════════════════════════════════════════════════

void testFunctionalHandler() {
    std::cout << "Test: Functional event handler... ";

    lmax::RingBuffer<OrderEvent, 1024> ring;

    std::atomic<uint64_t> count{0};
    std::atomic<uint64_t> sum{0};

    // Create functional handler with lambda
    auto handler = lmax::makeFunctionalHandler<OrderEvent>(
        [&](OrderEvent& event, int64_t /*sequence*/, bool /*endOfBatch*/) {
            count.fetch_add(1, std::memory_order_relaxed);
            sum.fetch_add(event.orderId, std::memory_order_relaxed);
        }
    );

    auto barrier = ring.newBarrier();
    lmax::BatchEventProcessor<OrderEvent, decltype(ring), decltype(barrier)>
        processor(ring, barrier, handler);

    ring.addGatingSequence(processor.getSequence());
    processor.start();

    // Publish
    constexpr int NUM = 500;
    uint64_t expectedSum = 0;
    for (int i = 0; i < NUM; i++) {
        int64_t seq = ring.next();
        ring.get(seq)->orderId = i;
        expectedSum += i;
        ring.publish(seq);
    }

    // Wait with timeout
    auto start = std::chrono::steady_clock::now();
    while (count.load(std::memory_order_relaxed) < NUM) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed > std::chrono::seconds(5)) {
            break;
        }
    }

    processor.halt();
    processor.join();

    assert(count.load() == NUM);
    assert(sum.load() == expectedSum);

    std::cout << "PASSED\n";
}

// ═══════════════════════════════════════════════════════════════════════════
// TEST: Multi-Producer Pipeline
// ═══════════════════════════════════════════════════════════════════════════

void testMultiProducerPipeline() {
    std::cout << "Test: Multi-producer pipeline... ";

    constexpr int NUM_PRODUCERS = 4;
    constexpr int EVENTS_PER_PRODUCER = 5000;
    constexpr int TOTAL_EVENTS = NUM_PRODUCERS * EVENTS_PER_PRODUCER;

    lmax::MPMCRingBuffer<OrderEvent, 16384> ring;

    lmax::Sequence consumerSeq;
    ring.addGatingSequence(consumerSeq);

    std::atomic<bool> startFlag{false};
    std::atomic<bool> consumerDone{false};

    // Producers
    std::vector<std::thread> producers;
    for (int p = 0; p < NUM_PRODUCERS; p++) {
        producers.emplace_back([&, p]() {
            while (!startFlag.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            for (int i = 0; i < EVENTS_PER_PRODUCER; i++) {
                int64_t seq = ring.next();
                OrderEvent* e = ring.get(seq);
                e->orderId = p * EVENTS_PER_PRODUCER + i;
                e->walProcessed = false;
                ring.publish(seq);
            }
        });
    }

    // Consumer with batch processing
    std::atomic<uint64_t> consumed{0};
    std::thread consumer([&]() {
        int64_t nextSeq = 0;

        while (!consumerDone.load(std::memory_order_relaxed)) {
            int64_t cursor = ring.getCursor();
            if (cursor < nextSeq) {
                std::this_thread::yield();
                continue;
            }

            // Get highest contiguously published
            int64_t available = ring.getHighestPublishedSequence(nextSeq, cursor);

            while (nextSeq <= available) {
                // Just consume
                consumed.fetch_add(1, std::memory_order_relaxed);
                nextSeq++;
            }

            if (nextSeq > 0) {
                consumerSeq.set(nextSeq - 1);
            }

            if (consumed.load(std::memory_order_relaxed) >= TOTAL_EVENTS) {
                break;
            }
        }
    });

    // Start producers
    startFlag.store(true, std::memory_order_release);

    // Wait for producers
    for (auto& t : producers) {
        t.join();
    }

    // Wait for consumer with timeout
    auto start = std::chrono::steady_clock::now();
    while (consumed.load(std::memory_order_relaxed) < TOTAL_EVENTS) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed > std::chrono::seconds(5)) {
            std::cerr << "TIMEOUT: consumed=" << consumed.load() << "/" << TOTAL_EVENTS << "\n";
            break;
        }
    }

    consumerDone.store(true, std::memory_order_release);
    consumer.join();

    assert(consumed.load() == TOTAL_EVENTS);

    std::cout << "PASSED (" << TOTAL_EVENTS << " events from "
              << NUM_PRODUCERS << " producers)\n";
}

// ═══════════════════════════════════════════════════════════════════════════
// TEST: Processor State Management
// ═══════════════════════════════════════════════════════════════════════════

void testProcessorStateManagement() {
    std::cout << "Test: Processor state management... ";

    lmax::RingBuffer<OrderEvent, 1024> ring;
    auto barrier = ring.newBarrier();
    CountingHandler handler;

    lmax::BatchEventProcessor<OrderEvent, decltype(ring), decltype(barrier)>
        processor(ring, barrier, handler);

    // Initial state
    assert(processor.getState() == lmax::ProcessorState::IDLE);
    assert(!processor.isRunning());

    // Start
    processor.start();

    // Should be running (may need small delay)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    assert(processor.getState() == lmax::ProcessorState::RUNNING);
    assert(processor.isRunning());

    // Publish some events
    for (int i = 0; i < 10; i++) {
        int64_t seq = ring.next();
        ring.get(seq)->orderId = i;
        ring.publish(seq);
    }

    // Wait for processing with timeout
    auto start = std::chrono::steady_clock::now();
    while (handler.count.load() < 10) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed > std::chrono::seconds(2)) {
            break;
        }
    }

    // Halt
    processor.halt();
    processor.join();

    // Should be halted
    assert(processor.getState() == lmax::ProcessorState::HALTED);
    assert(!processor.isRunning());

    std::cout << "PASSED\n";
}

int main() {
    std::cout << "=== LMAX Disruptor Processor Tests ===\n\n";

    testBasicBatchProcessor();
    testFunctionalHandler();
    testProcessorStateManagement();
    testPipelineManual();
    testMultiProducerPipeline();

    std::cout << "\nAll processor tests PASSED!\n";
    return 0;
}
