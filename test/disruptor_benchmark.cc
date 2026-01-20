/*
 * Benchmark: SPSCDisruptor vs SPSCQueue vs SPSCQueueAsymmetric
 */

#include "../SPSCDisruptor.h"
#include "../SPSCQueue.h"
#include "../SPSCQueueAsymmetric.h"
#include "../MultiConsumerDisruptor.h"
#include "rdtsc.h"
#include "cpupin.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <algorithm>
#include <numeric>
#include <iomanip>
#include <atomic>

struct BenchMsg {
    uint64_t seq;
    uint64_t timestamp;
    uint64_t data[6];  // 64 bytes total
};

constexpr uint64_t NUM_MESSAGES = 1000000;
constexpr uint32_t QUEUE_SIZE = 4096;

// ═══════════════════════════════════════════════════════════════════════════════
// Latency measurement
// ═══════════════════════════════════════════════════════════════════════════════
struct LatencyStats {
    std::vector<uint64_t> latencies;

    void add(uint64_t lat) { latencies.push_back(lat); }

    void report(const char* name) {
        if (latencies.empty()) return;

        std::sort(latencies.begin(), latencies.end());
        size_t n = latencies.size();

        double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
        double avg = sum / n;

        std::cout << name << ":\n";
        std::cout << "  Count:  " << n << "\n";
        std::cout << "  Avg:    " << std::fixed << std::setprecision(1) << avg << " cycles\n";
        std::cout << "  p50:    " << latencies[n * 50 / 100] << " cycles\n";
        std::cout << "  p99:    " << latencies[n * 99 / 100] << " cycles\n";
        std::cout << "  p999:   " << latencies[n * 999 / 1000] << " cycles\n";
        std::cout << "  Max:    " << latencies.back() << " cycles\n";
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Benchmark 1: SPSCDisruptor (LMAX style API)
// ═══════════════════════════════════════════════════════════════════════════════
void benchSPSCDisruptorLMAX() {
    std::cout << "\n--- SPSCDisruptor (LMAX API) ---\n";

    SPSCDisruptor<BenchMsg, QUEUE_SIZE, BusySpinWait> queue;
    LatencyStats stats;
    std::atomic<bool> done{false};

    // Consumer thread
    std::thread consumer([&]() {
        pinThread(7);
        uint64_t nextSeq = 1;

        while (nextSeq <= NUM_MESSAGES) {
            uint64_t avail = queue.available();
            while (nextSeq <= avail) {
                const BenchMsg* msg = queue.get(nextSeq);
                uint64_t lat = rdtscp() - msg->timestamp;
                stats.add(lat);
                nextSeq++;
            }
            if (nextSeq > 1) {
                queue.markConsumed(nextSeq - 1);
            }
        }
        done.store(true);
    });

    // Producer
    pinThread(6);
    auto start = std::chrono::high_resolution_clock::now();

    for (uint64_t i = 1; i <= NUM_MESSAGES; i++) {
        uint64_t seq = queue.next();
        BenchMsg* msg = queue.get(seq);
        msg->seq = seq;
        msg->timestamp = rdtscp();
        queue.publish(seq);
    }

    consumer.join();
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    double throughput = (double)NUM_MESSAGES / duration * 1000.0;
    std::cout << "Throughput: " << std::fixed << std::setprecision(2)
              << throughput / 1e6 << " M msgs/sec\n";
    stats.report("Latency");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Benchmark 2: SPSCDisruptor (Compatible API - like SPSCQueue)
// ═══════════════════════════════════════════════════════════════════════════════
void benchSPSCDisruptorCompat() {
    std::cout << "\n--- SPSCDisruptor (Compatible API) ---\n";

    SPSCDisruptor<BenchMsg, QUEUE_SIZE, BusySpinWait> queue;
    LatencyStats stats;
    std::atomic<bool> done{false};

    // Consumer thread
    std::thread consumer([&]() {
        pinThread(7);
        uint64_t count = 0;

        while (count < NUM_MESSAGES) {
            count += queue.tryPopBatch(64, [&](BenchMsg* msg, size_t, size_t) {
                uint64_t lat = rdtscp() - msg->timestamp;
                stats.add(lat);
            });
        }
        done.store(true);
    });

    // Producer
    pinThread(6);
    auto start = std::chrono::high_resolution_clock::now();

    for (uint64_t i = 1; i <= NUM_MESSAGES; i++) {
        queue.blockPush([i](BenchMsg* msg) {
            msg->seq = i;
            msg->timestamp = rdtscp();
        });
    }

    consumer.join();
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    double throughput = (double)NUM_MESSAGES / duration * 1000.0;
    std::cout << "Throughput: " << std::fixed << std::setprecision(2)
              << throughput / 1e6 << " M msgs/sec\n";
    stats.report("Latency");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Benchmark 3: Original SPSCQueue
// ═══════════════════════════════════════════════════════════════════════════════
void benchOriginalSPSCQueue() {
    std::cout << "\n--- Original SPSCQueue ---\n";

    SPSCQueue<BenchMsg, QUEUE_SIZE> queue;
    LatencyStats stats;
    std::atomic<bool> done{false};

    // Consumer thread
    std::thread consumer([&]() {
        pinThread(7);
        uint64_t count = 0;

        while (count < NUM_MESSAGES) {
            BenchMsg* msg = queue.front();
            if (msg) {
                uint64_t lat = rdtscp() - msg->timestamp;
                stats.add(lat);
                queue.pop();
                count++;
            }
        }
        done.store(true);
    });

    // Producer
    pinThread(6);
    auto start = std::chrono::high_resolution_clock::now();

    for (uint64_t i = 1; i <= NUM_MESSAGES; i++) {
        queue.blockPush([i](BenchMsg* msg) {
            msg->seq = i;
            msg->timestamp = rdtscp();
        });
    }

    consumer.join();
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    double throughput = (double)NUM_MESSAGES / duration * 1000.0;
    std::cout << "Throughput: " << std::fixed << std::setprecision(2)
              << throughput / 1e6 << " M msgs/sec\n";
    stats.report("Latency");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Benchmark 4: SPSCQueueAsymmetric
// ═══════════════════════════════════════════════════════════════════════════════
void benchSPSCQueueAsymmetric() {
    std::cout << "\n--- SPSCQueueAsymmetric ---\n";

    SPSCQueueAsymmetric<BenchMsg, QUEUE_SIZE> queue;
    LatencyStats stats;
    std::atomic<bool> done{false};

    // Consumer thread
    std::thread consumer([&]() {
        pinThread(7);
        uint64_t count = 0;

        while (count < NUM_MESSAGES) {
            count += queue.tryPopBatch(64, [&](BenchMsg* msg, size_t, size_t) {
                uint64_t lat = rdtscp() - msg->timestamp;
                stats.add(lat);
            });
        }
        done.store(true);
    });

    // Producer
    pinThread(6);
    auto start = std::chrono::high_resolution_clock::now();

    for (uint64_t i = 1; i <= NUM_MESSAGES; i++) {
        while (!queue.tryPush([i](BenchMsg* msg) {
            msg->seq = i;
            msg->timestamp = rdtscp();
        })) {}
    }

    consumer.join();
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    double throughput = (double)NUM_MESSAGES / duration * 1000.0;
    std::cout << "Throughput: " << std::fixed << std::setprecision(2)
              << throughput / 1e6 << " M msgs/sec\n";
    stats.report("Latency");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Benchmark 5: MultiConsumerDisruptor (3-stage pipeline)
// ═══════════════════════════════════════════════════════════════════════════════
void benchMultiConsumerPipeline() {
    std::cout << "\n--- MultiConsumerDisruptor (3-stage pipeline) ---\n";

    MultiConsumerDisruptor<BenchMsg, QUEUE_SIZE, BusySpinWait> disruptor;
    ConsumerSequence stage1Seq;
    ConsumerSequence stage2Seq;
    ConsumerSequence stage3Seq;

    disruptor.addGatingSequence(&stage3Seq);

    LatencyStats stats;
    std::atomic<bool> done{false};

    // Stage 1
    std::thread stage1([&]() {
        pinThread(5);
        uint64_t nextSeq = 1;
        while (nextSeq <= NUM_MESSAGES) {
            uint64_t avail = disruptor.waitFor(nextSeq);
            while (nextSeq <= avail) {
                // Simulate work
                volatile int x = 0;
                for (int i = 0; i < 10; i++) x++;
                nextSeq++;
            }
            stage1Seq.set(nextSeq - 1);
        }
    });

    // Stage 2
    std::thread stage2([&]() {
        pinThread(6);
        uint64_t nextSeq = 1;
        while (nextSeq <= NUM_MESSAGES) {
            uint64_t avail = disruptor.waitFor(nextSeq, stage1Seq);
            while (nextSeq <= avail) {
                volatile int x = 0;
                for (int i = 0; i < 10; i++) x++;
                nextSeq++;
            }
            stage2Seq.set(nextSeq - 1);
        }
    });

    // Stage 3 (final, measures latency)
    std::thread stage3([&]() {
        pinThread(7);
        uint64_t nextSeq = 1;
        while (nextSeq <= NUM_MESSAGES) {
            uint64_t avail = disruptor.waitFor(nextSeq, stage2Seq);
            while (nextSeq <= avail) {
                const BenchMsg* msg = disruptor.get(nextSeq);
                uint64_t lat = rdtscp() - msg->timestamp;
                stats.add(lat);
                nextSeq++;
            }
            stage3Seq.set(nextSeq - 1);
        }
        done.store(true);
    });

    // Producer
    pinThread(4);
    auto start = std::chrono::high_resolution_clock::now();

    for (uint64_t i = 1; i <= NUM_MESSAGES; i++) {
        uint64_t seq = disruptor.next();
        BenchMsg* msg = disruptor.get(seq);
        msg->seq = seq;
        msg->timestamp = rdtscp();
        disruptor.publish(seq);
    }

    stage1.join();
    stage2.join();
    stage3.join();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    double throughput = (double)NUM_MESSAGES / duration * 1000.0;
    std::cout << "Throughput: " << std::fixed << std::setprecision(2)
              << throughput / 1e6 << " M msgs/sec\n";
    stats.report("End-to-end Latency (3 stages)");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════════
int main() {
    std::cout << "\n╔════════════════════════════════════════════════════════════╗\n";
    std::cout << "║       LMAX Disruptor Queue Benchmark                        ║\n";
    std::cout << "║       Messages: " << NUM_MESSAGES << "  Queue Size: " << QUEUE_SIZE << "            ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════╝\n";

    benchSPSCDisruptorLMAX();
    benchSPSCDisruptorCompat();
    benchOriginalSPSCQueue();
    benchSPSCQueueAsymmetric();
    benchMultiConsumerPipeline();

    std::cout << "\n=== Benchmark Complete ===\n\n";
    return 0;
}
