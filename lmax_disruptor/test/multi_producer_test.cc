/*
 * Multi-Producer Ring Buffer Test
 *
 * Tests multi-producer functionality with concurrent producers.
 */

#include <iostream>
#include <thread>
#include <cassert>
#include <atomic>
#include <vector>
#include "../include/lmax/disruptor.h"

struct Event {
    uint64_t value;
    uint64_t producerId;
    uint64_t sequence;
};

void testBasicMultiProducerPublish() {
    std::cout << "Test: Basic multi-producer publish... ";

    lmax::MPMCRingBuffer<Event, 1024> ring;

    // Publish 10 events from single thread (basic sanity)
    for (int i = 0; i < 10; i++) {
        int64_t seq = ring.next();
        Event* e = ring.get(seq);
        e->value = i * 100;
        e->producerId = 0;
        e->sequence = seq;
        ring.publish(seq);
    }

    // Verify cursor advanced
    assert(ring.getCursor() == 9);

    // Verify all sequences are available
    for (int64_t seq = 0; seq <= 9; seq++) {
        assert(ring.isAvailable(seq));
    }

    std::cout << "PASSED\n";
}

void testMultiProducerTryNext() {
    std::cout << "Test: Multi-producer tryNext... ";

    lmax::MPMCRingBuffer<Event, 8> ring;
    lmax::Sequence consumerSeq(lmax::INITIAL_CURSOR_VALUE);

    ring.addGatingSequence(consumerSeq);

    // Fill buffer
    for (int i = 0; i < 8; i++) {
        int64_t seq = ring.tryNext();
        assert(seq != lmax::INITIAL_CURSOR_VALUE);
        ring.get(seq)->value = i;
        ring.publish(seq);
    }

    // Buffer should be full now
    int64_t seq = ring.tryNext();
    assert(seq == lmax::INITIAL_CURSOR_VALUE);

    // Consumer processes some events
    consumerSeq.set(3);

    // Now should have space
    for (int i = 0; i < 4; i++) {
        seq = ring.tryNext();
        assert(seq != lmax::INITIAL_CURSOR_VALUE);
        ring.publish(seq);
    }

    std::cout << "PASSED\n";
}

void testConcurrentProducers() {
    std::cout << "Test: Concurrent producers... ";

    constexpr int NUM_PRODUCERS = 4;
    constexpr int EVENTS_PER_PRODUCER = 100000;
    constexpr int TOTAL_EVENTS = NUM_PRODUCERS * EVENTS_PER_PRODUCER;

    lmax::MPMCRingBuffer<Event, 65536> ring;
    lmax::Sequence consumerSeq(lmax::INITIAL_CURSOR_VALUE);

    ring.addGatingSequence(consumerSeq);

    std::atomic<bool> startFlag{false};
    std::atomic<int> producersReady{0};
    std::atomic<int> producersDone{0};

    // Producer threads
    std::vector<std::thread> producers;
    for (int p = 0; p < NUM_PRODUCERS; p++) {
        producers.emplace_back([&, p]() {
            producersReady.fetch_add(1, std::memory_order_relaxed);

            // Wait for start signal
            while (!startFlag.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            for (int i = 0; i < EVENTS_PER_PRODUCER; i++) {
                int64_t seq = ring.next();
                Event* e = ring.get(seq);
                e->value = i;
                e->producerId = p;
                e->sequence = seq;
                ring.publish(seq);
            }

            producersDone.fetch_add(1, std::memory_order_relaxed);
        });
    }

    // Consumer thread
    std::atomic<uint64_t> consumedCount{0};
    std::vector<uint64_t> producerCounts(NUM_PRODUCERS, 0);

    std::thread consumer([&]() {
        auto barrier = ring.newBarrier();
        int64_t nextSeq = 0;

        while (consumedCount.load(std::memory_order_relaxed) < TOTAL_EVENTS) {
            int64_t availableSeq = barrier.waitFor(nextSeq);

            // For multi-producer, need to check actual availability
            int64_t highestPublished = ring.getHighestPublishedSequence(nextSeq, availableSeq);

            while (nextSeq <= highestPublished) {
                const Event* e = ring.get(nextSeq);
                producerCounts[e->producerId]++;
                consumedCount.fetch_add(1, std::memory_order_relaxed);
                nextSeq++;
            }

            if (nextSeq > 0) {
                consumerSeq.set(nextSeq - 1);
            }
        }
    });

    // Wait for all producers to be ready
    while (producersReady.load(std::memory_order_relaxed) < NUM_PRODUCERS) {
        std::this_thread::yield();
    }

    // Start producers
    startFlag.store(true, std::memory_order_release);

    // Wait for producers to finish
    for (auto& t : producers) {
        t.join();
    }

    // Wait for consumer to finish
    consumer.join();

    // Verify all events were consumed
    assert(consumedCount.load() == TOTAL_EVENTS);

    // Verify each producer contributed equally
    for (int p = 0; p < NUM_PRODUCERS; p++) {
        assert(producerCounts[p] == EVENTS_PER_PRODUCER);
    }

    std::cout << "PASSED (" << TOTAL_EVENTS << " events from " << NUM_PRODUCERS << " producers)\n";
}

void testBatchPublishMultiProducer() {
    std::cout << "Test: Batch publish (multi-producer)... ";

    lmax::MPMCRingBuffer<Event, 1024> ring;

    // Batch claim 10 sequences
    int64_t hi = ring.next(10);
    int64_t lo = hi - 10 + 1;

    // Fill batch
    for (int64_t seq = lo; seq <= hi; seq++) {
        Event* e = ring.get(seq);
        e->value = seq * 10;
    }

    // Publish batch
    ring.publish(lo, hi);

    // Verify all are available
    for (int64_t seq = lo; seq <= hi; seq++) {
        assert(ring.isAvailable(seq));
    }

    std::cout << "PASSED\n";
}

void testHighestPublishedSequence() {
    std::cout << "Test: Highest published sequence... ";

    lmax::MPMCRingBuffer<Event, 16> ring;

    // Claim sequences but publish out of order
    int64_t seq0 = ring.next();  // 0
    int64_t seq1 = ring.next();  // 1
    int64_t seq2 = ring.next();  // 2

    // Publish 0 and 2, but not 1
    ring.publish(seq0);
    ring.publish(seq2);

    // Highest contiguous from 0 should be 0 (1 is missing)
    int64_t highest = ring.getHighestPublishedSequence(0, 2);
    assert(highest == 0);

    // Now publish 1
    ring.publish(seq1);

    // Now highest should be 2
    highest = ring.getHighestPublishedSequence(0, 2);
    assert(highest == 2);

    std::cout << "PASSED\n";
}

void testMPMCWithYieldingWait() {
    std::cout << "Test: MPMC with YieldingWait... ";

    lmax::RingBuffer<Event, 1024, lmax::YieldingWait, lmax::MultiProducerType> ring;

    // Basic publish/consume
    for (int i = 0; i < 100; i++) {
        int64_t seq = ring.next();
        ring.get(seq)->value = i;
        ring.publish(seq);
    }

    assert(ring.getCursor() == 99);

    // Verify availability
    for (int64_t seq = 0; seq <= 99; seq++) {
        assert(ring.isAvailable(seq));
    }

    std::cout << "PASSED\n";
}

int main() {
    std::cout << "=== LMAX Disruptor Multi-Producer Tests ===\n\n";

    testBasicMultiProducerPublish();
    testMultiProducerTryNext();
    testBatchPublishMultiProducer();
    testHighestPublishedSequence();
    testMPMCWithYieldingWait();
    testConcurrentProducers();

    std::cout << "\nAll multi-producer tests PASSED!\n";
    return 0;
}
