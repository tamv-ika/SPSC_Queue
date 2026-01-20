/*
 * SPSC Ring Buffer Test
 *
 * Tests basic single producer, single consumer functionality.
 */

#include <iostream>
#include <thread>
#include <cassert>
#include <atomic>
#include "../include/lmax/disruptor.h"

struct Event {
    uint64_t value;
    uint64_t sequence;
};

void testBasicPublishConsume() {
    std::cout << "Test: Basic publish/consume... ";

    lmax::RingBuffer<Event, 1024> ring;

    // Publish 10 events
    for (int i = 0; i < 10; i++) {
        int64_t seq = ring.next();
        Event* e = ring.get(seq);
        e->value = i * 100;
        e->sequence = seq;
        ring.publish(seq);
    }

    // Verify cursor advanced
    assert(ring.getCursor() == 9);

    // Read events
    auto barrier = ring.newBarrier();
    int64_t available = barrier.available();
    assert(available == 9);

    for (int64_t seq = 0; seq <= available; seq++) {
        const Event* e = ring.get(seq);
        assert(e->value == (uint64_t)(seq * 100));
        assert(e->sequence == (uint64_t)seq);
    }

    std::cout << "PASSED\n";
}

void testTryNext() {
    std::cout << "Test: tryNext (non-blocking)... ";

    lmax::RingBuffer<Event, 8> ring;

    // Fill the buffer (no consumer, so no gating)
    // Without gating sequences, buffer can fill completely
    for (int i = 0; i < 100; i++) {
        int64_t seq = ring.tryNext();
        assert(seq != lmax::INITIAL_CURSOR_VALUE);
        ring.get(seq)->value = i;
        ring.publish(seq);
    }

    std::cout << "PASSED\n";
}

void testGating() {
    std::cout << "Test: Gating sequences... ";

    lmax::RingBuffer<Event, 8> ring;
    lmax::Sequence consumerSeq(lmax::INITIAL_CURSOR_VALUE);

    // Add consumer as gating sequence
    ring.addGatingSequence(consumerSeq);

    // Fill buffer (size 8, so can publish 0-7)
    for (int i = 0; i < 8; i++) {
        int64_t seq = ring.tryNext();
        assert(seq != lmax::INITIAL_CURSOR_VALUE);
        ring.get(seq)->value = i;
        ring.publish(seq);
    }

    // Buffer should be full now (consumer at -1, producer at 7)
    int64_t seq = ring.tryNext();
    assert(seq == lmax::INITIAL_CURSOR_VALUE);  // Should fail - buffer full

    // Consumer processes some events
    consumerSeq.set(3);  // Consumed 0,1,2,3

    // Now should have space for 4 more
    for (int i = 0; i < 4; i++) {
        seq = ring.tryNext();
        assert(seq != lmax::INITIAL_CURSOR_VALUE);
        ring.publish(seq);
    }

    // Should be full again
    seq = ring.tryNext();
    assert(seq == lmax::INITIAL_CURSOR_VALUE);

    std::cout << "PASSED\n";
}

void testBatchPublish() {
    std::cout << "Test: Batch publish... ";

    lmax::RingBuffer<Event, 1024> ring;

    // Batch claim 10 sequences
    int64_t hi = ring.next(10);
    int64_t lo = hi - 10 + 1;

    assert(lo == 0);
    assert(hi == 9);

    // Fill batch
    for (int64_t seq = lo; seq <= hi; seq++) {
        Event* e = ring.get(seq);
        e->value = seq * 10;
    }

    // Publish batch
    ring.publish(lo, hi);

    assert(ring.getCursor() == 9);

    std::cout << "PASSED\n";
}

void testMultiThreaded() {
    std::cout << "Test: Multi-threaded SPSC... ";

    constexpr int NUM_EVENTS = 1000000;
    lmax::RingBuffer<Event, 65536> ring;
    lmax::Sequence consumerSeq(lmax::INITIAL_CURSOR_VALUE);

    ring.addGatingSequence(consumerSeq);

    std::atomic<bool> done{false};
    std::atomic<uint64_t> sum{0};

    // Consumer thread
    std::thread consumer([&]() {
        auto barrier = ring.newBarrier();
        int64_t nextSeq = 0;
        uint64_t localSum = 0;

        while (!done.load(std::memory_order_relaxed) || nextSeq <= ring.getCursor()) {
            int64_t available = barrier.available();

            while (nextSeq <= available) {
                const Event* e = ring.get(nextSeq);
                localSum += e->value;
                nextSeq++;
            }

            if (nextSeq > 0) {
                consumerSeq.set(nextSeq - 1);
            }
        }

        sum.store(localSum, std::memory_order_relaxed);
    });

    // Producer
    uint64_t expectedSum = 0;
    for (int i = 0; i < NUM_EVENTS; i++) {
        int64_t seq = ring.next();
        Event* e = ring.get(seq);
        e->value = i;
        expectedSum += i;
        ring.publish(seq);
    }

    done.store(true, std::memory_order_relaxed);
    consumer.join();

    assert(sum.load() == expectedSum);

    std::cout << "PASSED (" << NUM_EVENTS << " events, sum=" << sum.load() << ")\n";
}

void testSPSCQueueCompatibleAPI() {
    std::cout << "Test: SPSCQueue compatible API... ";

    lmax::RingBuffer<Event, 1024> ring;

    // Use tryPush like SPSCQueue
    bool ok = ring.tryPush([](Event* e) {
        e->value = 42;
        e->sequence = 0;
    });
    assert(ok);

    ok = ring.tryPush([](Event* e) {
        e->value = 100;
        e->sequence = 1;
    });
    assert(ok);

    // Verify
    assert(ring.getCursor() == 1);
    assert(ring.get(0)->value == 42);
    assert(ring.get(1)->value == 100);

    std::cout << "PASSED\n";
}

void testBarrierWaitFor() {
    std::cout << "Test: Barrier waitFor... ";

    lmax::RingBuffer<Event, 1024> ring;

    // Publish some events
    for (int i = 0; i < 5; i++) {
        int64_t seq = ring.next();
        ring.get(seq)->value = i;
        ring.publish(seq);
    }

    auto barrier = ring.newBarrier();

    // Wait for sequence 3 - should return immediately
    int64_t available = barrier.waitFor(3);
    assert(available >= 3);
    assert(available == 4);  // Actually all 5 are available

    std::cout << "PASSED\n";
}

int main() {
    std::cout << "=== LMAX Disruptor SPSC Tests ===\n\n";

    testBasicPublishConsume();
    testTryNext();
    testGating();
    testBatchPublish();
    testMultiThreaded();
    testSPSCQueueCompatibleAPI();
    testBarrierWaitFor();

    std::cout << "\nAll tests PASSED!\n";
    return 0;
}
