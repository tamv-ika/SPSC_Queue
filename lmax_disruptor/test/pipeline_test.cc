/*
 * Pipeline Test - WAL -> Replicator -> ME pattern
 *
 * Demonstrates multi-consumer dependency chain:
 * - Producer publishes to ring
 * - WAL handler processes first (waits on cursor)
 * - Replicator processes next (waits on WAL sequence)
 * - Matching Engine processes last (waits on Replicator sequence)
 * - ME is the gating sequence (slowest consumer gates producer)
 */

#include <iostream>
#include <iomanip>
#include <thread>
#include <atomic>
#include <chrono>
#include <cassert>
#include "../include/lmax/disruptor.h"

struct OrderEvent {
    uint64_t orderId;
    uint64_t timestamp;
    double price;
    int quantity;
    bool processed_wal;
    bool processed_repl;
    bool processed_me;
};

int main() {
    std::cout << "═══════════════════════════════════════════════════════════════════\n";
    std::cout << "          Pipeline Test: WAL -> Replicator -> ME\n";
    std::cout << "═══════════════════════════════════════════════════════════════════\n\n";

    constexpr size_t NUM_ORDERS = 100000;
    constexpr size_t BUFFER_SIZE = 4096;

    lmax::RingBuffer<OrderEvent, BUFFER_SIZE, lmax::YieldingWait> ring;

    // Consumer sequences
    lmax::Sequence walSeq(lmax::INITIAL_CURSOR_VALUE);
    lmax::Sequence replSeq(lmax::INITIAL_CURSOR_VALUE);
    lmax::Sequence meSeq(lmax::INITIAL_CURSOR_VALUE);

    // ME is the gating sequence (slowest consumer gates producer)
    ring.addGatingSequence(meSeq);

    std::atomic<bool> done{false};
    std::atomic<uint64_t> walProcessed{0};
    std::atomic<uint64_t> replProcessed{0};
    std::atomic<uint64_t> meProcessed{0};

    // WAL Handler - waits on cursor (first in chain)
    std::thread walHandler([&]() {
        auto barrier = ring.newBarrier();  // Waits on cursor
        int64_t nextSeq = 0;

        while (!done.load(std::memory_order_relaxed) || nextSeq <= ring.getCursor()) {
            int64_t available = barrier.available();

            while (nextSeq <= available) {
                OrderEvent* e = const_cast<OrderEvent*>(ring.get(nextSeq));
                // Simulate WAL write
                e->processed_wal = true;
                walProcessed.fetch_add(1, std::memory_order_relaxed);
                nextSeq++;
            }

            if (nextSeq > 0) {
                walSeq.set(nextSeq - 1);
            }
        }
    });

    // Replicator - waits on WAL sequence
    std::thread replicator([&]() {
        auto barrier = ring.newBarrier({&walSeq});  // Waits on WAL
        int64_t nextSeq = 0;

        while (!done.load(std::memory_order_relaxed) || nextSeq <= walSeq.get()) {
            int64_t available = barrier.available();

            while (nextSeq <= available) {
                OrderEvent* e = const_cast<OrderEvent*>(ring.get(nextSeq));
                assert(e->processed_wal);  // WAL must have processed first
                // Simulate replication
                e->processed_repl = true;
                replProcessed.fetch_add(1, std::memory_order_relaxed);
                nextSeq++;
            }

            if (nextSeq > 0) {
                replSeq.set(nextSeq - 1);
            }
        }
    });

    // Matching Engine - waits on Replicator sequence
    std::thread matchingEngine([&]() {
        auto barrier = ring.newBarrier({&replSeq});  // Waits on Replicator
        int64_t nextSeq = 0;

        while (!done.load(std::memory_order_relaxed) || nextSeq <= replSeq.get()) {
            int64_t available = barrier.available();

            while (nextSeq <= available) {
                const OrderEvent* e = ring.get(nextSeq);
                assert(e->processed_wal);
                assert(e->processed_repl);
                // Simulate matching
                meProcessed.fetch_add(1, std::memory_order_relaxed);
                nextSeq++;
            }

            if (nextSeq > 0) {
                meSeq.set(nextSeq - 1);  // Gates producer
            }
        }
    });

    // Producer
    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < NUM_ORDERS; i++) {
        int64_t seq = ring.next();
        OrderEvent* e = ring.get(seq);
        e->orderId = i;
        e->timestamp = std::chrono::high_resolution_clock::now().time_since_epoch().count();
        e->price = 100.0 + (i % 100);
        e->quantity = 1 + (i % 10);
        e->processed_wal = false;
        e->processed_repl = false;
        e->processed_me = false;
        ring.publish(seq);
    }

    // Wait for all consumers to finish
    while (meProcessed.load(std::memory_order_relaxed) < NUM_ORDERS) {
        std::this_thread::yield();
    }

    done.store(true, std::memory_order_relaxed);

    auto end = std::chrono::high_resolution_clock::now();

    walHandler.join();
    replicator.join();
    matchingEngine.join();

    double seconds = std::chrono::duration<double>(end - start).count();
    double throughput = NUM_ORDERS / seconds / 1e6;

    std::cout << "Results:\n";
    std::cout << "  Orders: " << NUM_ORDERS << "\n";
    std::cout << "  WAL processed: " << walProcessed.load() << "\n";
    std::cout << "  Replicator processed: " << replProcessed.load() << "\n";
    std::cout << "  ME processed: " << meProcessed.load() << "\n";
    std::cout << "  Time: " << std::fixed << std::setprecision(3) << seconds << " sec\n";
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2) << throughput << " M orders/sec\n";

    // Verify all processed
    assert(walProcessed.load() == NUM_ORDERS);
    assert(replProcessed.load() == NUM_ORDERS);
    assert(meProcessed.load() == NUM_ORDERS);

    std::cout << "\nPipeline test PASSED!\n";

    return 0;
}
