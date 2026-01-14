/**
 * Simple example demonstrating batch operations with SPSCQueueBatch
 *
 * This example shows:
 * 1. Basic batch push/pop operations
 * 2. Performance comparison vs single-item
 * 3. Adaptive batching pattern
 */

#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include "../SPSCQueueBatch.h"

struct Message {
    int id;
    double value;
    char data[48];  // Total 64 bytes
};

// ============================================================
// Example 1: Basic Batch Operations
// ============================================================
void example1_basic_batch() {
    std::cout << "\n=== Example 1: Basic Batch Operations ===\n";

    SPSCQueueBatch<Message, 256> queue;

    // Producer: Push 10 messages in batches of 4
    std::cout << "\nProducer: Pushing 10 messages in batches of 4...\n";
    for (int batch = 0; batch < 3; batch++) {
        size_t batch_size = (batch < 2) ? 4 : 2;  // Last batch has 2

        size_t pushed = queue.tryPushBatch(batch_size,
            [&](Message** slots, size_t count) {
                std::cout << "  Allocating batch of " << count << " slots\n";
                for (size_t i = 0; i < count; i++) {
                    int msg_id = batch * 4 + i;
                    slots[i]->id = msg_id;
                    slots[i]->value = msg_id * 3.14;
                    std::cout << "    Writing message " << msg_id << "\n";
                }
            }
        );

        std::cout << "  → Pushed " << pushed << " messages (1 atomic op)\n";
    }

    // Consumer: Pop in batches of 5
    std::cout << "\nConsumer: Popping in batches of 5...\n";
    size_t total_received = 0;
    while (total_received < 10) {
        size_t popped = queue.tryPopBatch(5,
            [&](Message** slots, size_t count) {
                std::cout << "  Received batch of " << count << " messages:\n";
                for (size_t i = 0; i < count; i++) {
                    std::cout << "    Message " << slots[i]->id
                              << " value=" << slots[i]->value << "\n";
                }
            }
        );

        if (popped > 0) {
            std::cout << "  → Popped " << popped << " messages (1 atomic op)\n";
            total_received += popped;
        }
    }
}

// ============================================================
// Example 2: Performance Comparison
// ============================================================
void example2_performance() {
    std::cout << "\n=== Example 2: Performance Comparison ===\n";

    const int NUM_MSGS = 100000;
    SPSCQueueBatch<Message, 4096> queue;

    auto benchmark = [&](const char* name, size_t batch_size) {
        std::atomic<bool> ready{false};
        std::atomic<size_t> atomic_ops{0};

        auto producer = [&]() {
            while (!ready.load());

            size_t sent = 0;
            while (sent < NUM_MSGS) {
                size_t to_send = std::min(batch_size, (size_t)(NUM_MSGS - sent));

                size_t pushed = queue.tryPushBatch(to_send,
                    [&](Message** slots, size_t count) {
                        for (size_t i = 0; i < count; i++) {
                            slots[i]->id = sent + i;
                            slots[i]->value = (sent + i) * 1.5;
                        }
                    }
                );

                if (pushed > 0) {
                    sent += pushed;
                    atomic_ops++;
                }
            }
        };

        auto consumer = [&]() {
            while (!ready.load());

            size_t received = 0;
            while (received < NUM_MSGS) {
                size_t to_recv = std::min(batch_size, (size_t)(NUM_MSGS - received));

                size_t popped = queue.tryPopBatch(to_recv,
                    [&](Message** slots, size_t count) {
                        // Simulate processing
                        for (size_t i = 0; i < count; i++) {
                            volatile int x = slots[i]->id;
                            (void)x;
                        }
                    }
                );

                if (popped > 0) {
                    received += popped;
                    atomic_ops++;
                }
            }
        };

        std::thread prod(producer);
        std::thread cons(consumer);

        auto start = std::chrono::high_resolution_clock::now();
        ready = true;
        prod.join();
        cons.join();
        auto end = std::chrono::high_resolution_clock::now();

        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        double throughput = NUM_MSGS / (ms / 1000.0) / 1e6;

        std::cout << "\n" << name << ":\n";
        std::cout << "  Time: " << std::fixed << std::setprecision(2) << ms << " ms\n";
        std::cout << "  Throughput: " << throughput << " Mops/sec\n";
        std::cout << "  Atomic ops: " << atomic_ops.load() << "\n";
        std::cout << "  Reduction: " << std::fixed << std::setprecision(1)
                  << (1.0 - (double)atomic_ops.load() / (NUM_MSGS * 2)) * 100 << "%\n";

        return throughput;
    };

    double base_throughput = benchmark("Batch size 1 (single)", 1);
    double batch4 = benchmark("Batch size 4", 4);
    double batch16 = benchmark("Batch size 16", 16);
    double batch64 = benchmark("Batch size 64", 64);

    std::cout << "\nSpeedup summary:\n";
    std::cout << "  Batch 4:  " << std::fixed << std::setprecision(2)
              << batch4 / base_throughput << "x\n";
    std::cout << "  Batch 16: " << batch16 / base_throughput << "x\n";
    std::cout << "  Batch 64: " << batch64 / base_throughput << "x\n";
}

// ============================================================
// Example 3: Adaptive Batching Pattern
// ============================================================
void example3_adaptive() {
    std::cout << "\n=== Example 3: Adaptive Batching ===\n";

    SPSCQueueBatch<Message, 1024> queue;

    const size_t MIN_BATCH = 4;
    const size_t MAX_BATCH = 32;
    const auto MAX_DELAY = std::chrono::microseconds(100);

    // Producer with adaptive batching
    auto producer = [&]() {
        std::vector<Message> buffer;
        buffer.reserve(MAX_BATCH);
        auto last_flush = std::chrono::steady_clock::now();

        // Simulate variable message rate
        for (int i = 0; i < 1000; i++) {
            Message msg;
            msg.id = i;
            msg.value = i * 2.5;
            buffer.push_back(msg);

            auto now = std::chrono::steady_clock::now();
            bool timeout = (now - last_flush) > MAX_DELAY;
            bool min_batch = buffer.size() >= MIN_BATCH;
            bool max_batch = buffer.size() >= MAX_BATCH;

            if ((timeout && min_batch) || max_batch) {
                size_t pushed = queue.tryPushBatch(buffer.size(),
                    [&](Message** slots, size_t count) {
                        for (size_t j = 0; j < count; j++) {
                            *slots[j] = buffer[j];
                        }
                    }
                );

                if (pushed > 0) {
                    std::cout << "Flushed batch of " << pushed
                              << (timeout ? " (timeout)" : " (full)")
                              << "\n";
                    buffer.erase(buffer.begin(), buffer.begin() + pushed);
                    last_flush = now;
                }
            }

            // Simulate variable rate
            if (i % 100 < 50) {
                std::this_thread::sleep_for(std::chrono::microseconds(10));
            }
        }

        // Flush remaining
        if (!buffer.empty()) {
            queue.tryPushBatch(buffer.size(),
                [&](Message** slots, size_t count) {
                    for (size_t j = 0; j < count; j++) {
                        *slots[j] = buffer[j];
                    }
                }
            );
            std::cout << "Final flush: " << buffer.size() << " messages\n";
        }
    };

    // Consumer
    auto consumer = [&]() {
        size_t total = 0;
        while (total < 1000) {
            size_t received = queue.tryPopBatch(16,
                [&](Message** slots, size_t count) {
                    for (size_t i = 0; i < count; i++) {
                        // Process message
                        volatile int x = slots[i]->id;
                        (void)x;
                    }
                }
            );

            if (received > 0) {
                total += received;
            } else {
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
        }
        std::cout << "Consumer received all 1000 messages\n";
    };

    std::thread prod(producer);
    std::thread cons(consumer);
    prod.join();
    cons.join();

    std::cout << "\nAdaptive batching allows:\n";
    std::cout << "  - High throughput when traffic is high\n";
    std::cout << "  - Low latency when traffic is low\n";
    std::cout << "  - Guaranteed max latency via timeout\n";
}

int main() {
    std::cout << "========================================\n";
    std::cout << "  SPSCQueueBatch Examples\n";
    std::cout << "========================================\n";

    example1_basic_batch();
    example2_performance();
    example3_adaptive();

    std::cout << "\n========================================\n";
    std::cout << "All examples completed!\n";
    std::cout << "========================================\n";

    return 0;
}
