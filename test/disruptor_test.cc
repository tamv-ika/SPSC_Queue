/*
 * Unit tests for LMAX Disruptor-style queues
 */

#include "../SPSCDisruptor.h"
#include "../MultiConsumerDisruptor.h"
#include "../ConsumerSequence.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <cassert>
#include <vector>
#include <atomic>

struct TestMsg {
    uint64_t seq;
    uint64_t value;
    char data[48];
};

// ═══════════════════════════════════════════════════════════════════════════════
// Test 1: Basic SPSC operations
// ═══════════════════════════════════════════════════════════════════════════════
void testBasicSPSC() {
    std::cout << "Test 1: Basic SPSC operations... ";

    SPSCDisruptor<TestMsg, 64> queue;

    // Test LMAX-style API
    uint64_t seq1 = queue.next();
    assert(seq1 == 1);
    TestMsg* msg1 = queue.get(seq1);
    msg1->seq = seq1;
    msg1->value = 100;
    queue.publish(seq1);

    uint64_t seq2 = queue.next();
    assert(seq2 == 2);
    TestMsg* msg2 = queue.get(seq2);
    msg2->seq = seq2;
    msg2->value = 200;
    queue.publish(seq2);

    // Consumer reads
    assert(queue.available() == 2);
    const TestMsg* read1 = queue.get(1);
    assert(read1->value == 100);
    const TestMsg* read2 = queue.get(2);
    assert(read2->value == 200);
    queue.markConsumed(2);

    std::cout << "PASSED\n";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Test 2: Compatible API (matches SPSCQueue interface)
// ═══════════════════════════════════════════════════════════════════════════════
void testCompatibleAPI() {
    std::cout << "Test 2: Compatible API... ";

    SPSCDisruptor<TestMsg, 64> queue;

    // Test alloc/push
    TestMsg* slot = queue.alloc();
    assert(slot != nullptr);
    slot->value = 42;
    queue.push();

    // Test front/pop
    TestMsg* front = queue.front();
    assert(front != nullptr);
    assert(front->value == 42);
    queue.pop();

    assert(queue.front() == nullptr);  // Empty now

    // Test tryPush
    bool pushed = queue.tryPush([](TestMsg* msg) {
        msg->value = 123;
    });
    assert(pushed);

    // Test tryPop
    uint64_t readValue = 0;
    bool popped = queue.tryPop([&](TestMsg* msg) {
        readValue = msg->value;
    });
    assert(popped);
    assert(readValue == 123);

    std::cout << "PASSED\n";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Test 3: Batch operations
// ═══════════════════════════════════════════════════════════════════════════════
void testBatchOperations() {
    std::cout << "Test 3: Batch operations... ";

    SPSCDisruptor<TestMsg, 64> queue;

    // Producer batch
    uint64_t lastSeq = queue.next(10);  // Claim 10 slots
    assert(lastSeq == 10);

    for (uint64_t i = 1; i <= 10; i++) {
        TestMsg* msg = queue.get(i);
        msg->seq = i;
        msg->value = i * 100;
    }
    queue.publish(1, 10);

    // Consumer batch via tryPopBatch
    std::vector<uint64_t> values;
    size_t processed = queue.tryPopBatch(20, [&](TestMsg* msg, size_t idx, size_t total) {
        values.push_back(msg->value);
    });

    assert(processed == 10);
    assert(values.size() == 10);
    for (size_t i = 0; i < 10; i++) {
        assert(values[i] == (i + 1) * 100);
    }

    std::cout << "PASSED\n";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Test 4: Backpressure (producer waits when full)
// ═══════════════════════════════════════════════════════════════════════════════
void testBackpressure() {
    std::cout << "Test 4: Backpressure... ";

    SPSCDisruptor<TestMsg, 8> queue;  // Small buffer

    // Fill the buffer (capacity = 8, but can only hold 7 before blocking)
    for (int i = 0; i < 7; i++) {
        uint64_t seq = queue.tryNext();
        assert(seq != 0);
        queue.publish(seq);
    }

    // Next tryNext should fail (buffer full)
    uint64_t seq = queue.tryNext();
    assert(seq == 0);  // Full!

    // Consumer frees space
    queue.markConsumed(3);

    // Now producer can continue
    seq = queue.tryNext();
    assert(seq != 0);

    std::cout << "PASSED\n";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Test 5: Multi-threaded SPSC
// ═══════════════════════════════════════════════════════════════════════════════
void testMultiThreadedSPSC() {
    std::cout << "Test 5: Multi-threaded SPSC... ";

    SPSCDisruptor<TestMsg, 4096> queue;
    constexpr uint64_t NUM_MESSAGES = 100000;
    std::atomic<bool> consumerDone{false};
    std::atomic<uint64_t> lastReceived{0};

    // Consumer thread
    std::thread consumer([&]() {
        uint64_t nextSeq = 1;
        while (nextSeq <= NUM_MESSAGES) {
            uint64_t avail = queue.available();
            while (nextSeq <= avail) {
                const TestMsg* msg = queue.get(nextSeq);
                assert(msg->seq == nextSeq);
                assert(msg->value == nextSeq * 10);
                nextSeq++;
            }
            if (nextSeq > 1) {
                queue.markConsumed(nextSeq - 1);
                lastReceived.store(nextSeq - 1, std::memory_order_relaxed);
            }
        }
        consumerDone.store(true);
    });

    // Producer (main thread)
    for (uint64_t i = 1; i <= NUM_MESSAGES; i++) {
        uint64_t seq = queue.next();
        TestMsg* msg = queue.get(seq);
        msg->seq = seq;
        msg->value = seq * 10;
        queue.publish(seq);
    }

    consumer.join();
    assert(lastReceived.load() == NUM_MESSAGES);

    std::cout << "PASSED\n";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Test 6: MultiConsumerDisruptor with dependency chain
// ═══════════════════════════════════════════════════════════════════════════════
void testMultiConsumerPipeline() {
    std::cout << "Test 6: Multi-consumer pipeline... ";

    MultiConsumerDisruptor<TestMsg, 4096> disruptor;

    // Consumer sequences
    ConsumerSequence stage1Seq;
    ConsumerSequence stage2Seq;

    // Stage2 is the gating sequence (slowest, gates producer)
    disruptor.addGatingSequence(&stage2Seq);

    constexpr uint64_t NUM_MESSAGES = 10000;
    std::atomic<bool> done{false};
    std::atomic<uint64_t> stage2Processed{0};

    // Stage 1: First consumer (waits on cursor)
    std::thread stage1([&]() {
        uint64_t nextSeq = 1;
        while (nextSeq <= NUM_MESSAGES) {
            uint64_t avail = disruptor.waitFor(nextSeq);
            while (nextSeq <= avail) {
                const TestMsg* msg = disruptor.get(nextSeq);
                // Process stage 1...
                nextSeq++;
            }
            stage1Seq.set(nextSeq - 1);
        }
    });

    // Stage 2: Second consumer (waits on stage1Seq)
    std::thread stage2([&]() {
        uint64_t nextSeq = 1;
        while (nextSeq <= NUM_MESSAGES) {
            uint64_t avail = disruptor.waitFor(nextSeq, stage1Seq);
            while (nextSeq <= avail) {
                const TestMsg* msg = disruptor.get(nextSeq);
                assert(msg->value == nextSeq * 100);
                nextSeq++;
            }
            stage2Seq.set(nextSeq - 1);
            stage2Processed.store(nextSeq - 1, std::memory_order_relaxed);
        }
        done.store(true);
    });

    // Producer
    for (uint64_t i = 1; i <= NUM_MESSAGES; i++) {
        uint64_t seq = disruptor.next();
        TestMsg* msg = disruptor.get(seq);
        msg->seq = seq;
        msg->value = seq * 100;
        disruptor.publish(seq);
    }

    stage1.join();
    stage2.join();

    assert(stage2Processed.load() == NUM_MESSAGES);

    std::cout << "PASSED\n";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Test 7: Wait strategies
// ═══════════════════════════════════════════════════════════════════════════════
void testWaitStrategies() {
    std::cout << "Test 7: Wait strategies... ";

    // BusySpin
    {
        SPSCDisruptor<TestMsg, 64, BusySpinWait> queue;
        queue.tryPush([](TestMsg* m) { m->value = 1; });
        uint64_t v = 0;
        queue.tryPop([&](TestMsg* m) { v = m->value; });
        assert(v == 1);
    }

    // Yielding
    {
        SPSCDisruptor<TestMsg, 64, YieldingWait> queue;
        queue.tryPush([](TestMsg* m) { m->value = 2; });
        uint64_t v = 0;
        queue.tryPop([&](TestMsg* m) { v = m->value; });
        assert(v == 2);
    }

    // Sleeping
    {
        SPSCDisruptor<TestMsg, 64, SleepingWait> queue;
        queue.tryPush([](TestMsg* m) { m->value = 3; });
        uint64_t v = 0;
        queue.tryPop([&](TestMsg* m) { v = m->value; });
        assert(v == 3);
    }

    std::cout << "PASSED\n";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Test 8: ConsumerSequence
// ═══════════════════════════════════════════════════════════════════════════════
void testConsumerSequence() {
    std::cout << "Test 8: ConsumerSequence... ";

    ConsumerSequence seq(100);
    assert(seq.get() == 100);

    seq.set(200);
    assert(seq.get() == 200);

    // SequenceGroup
    ConsumerSequence s1(10);
    ConsumerSequence s2(20);
    ConsumerSequence s3(5);

    SequenceGroup group;
    group.add(&s1);
    group.add(&s2);
    group.add(&s3);

    assert(group.getMinimum() == 5);

    s3.set(25);
    assert(group.getMinimum() == 10);

    std::cout << "PASSED\n";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════════
int main() {
    std::cout << "\n=== LMAX Disruptor Queue Tests ===\n\n";

    testBasicSPSC();
    testCompatibleAPI();
    testBatchOperations();
    testBackpressure();
    testMultiThreadedSPSC();
    testMultiConsumerPipeline();
    testWaitStrategies();
    testConsumerSequence();

    std::cout << "\n=== All tests PASSED ===\n\n";
    return 0;
}
