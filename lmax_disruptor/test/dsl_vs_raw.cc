/*
 * DSL vs Raw - Clean isolated comparison
 * Run each test multiple times in isolation
 */

#include <iostream>
#include <iomanip>
#include <thread>
#include <atomic>
#include <chrono>
#include <vector>
#include <algorithm>
#include <numeric>
#include "../include/lmax/disruptor.h"

struct TestEvent {
    int64_t value;
};

constexpr size_t NUM_EVENTS = 1000000;
constexpr size_t BUFFER_SIZE = 65536;
constexpr int NUM_RUNS = 5;

using RingBufferType = lmax::RingBuffer<TestEvent, BUFFER_SIZE, lmax::BusySpinWait>;

// ═══════════════════════════════════════════════════════════════════════════
// Raw implementation
// ═══════════════════════════════════════════════════════════════════════════

double runRawTest() {
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

    // Let threads start
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
// DSL implementation
// ═══════════════════════════════════════════════════════════════════════════

class SimpleHandler : public lmax::EventHandler<TestEvent> {
public:
    std::atomic<size_t> count{0};
    void onEvent(TestEvent& event, int64_t sequence, bool endOfBatch) override {
        count.fetch_add(1, std::memory_order_relaxed);
    }
};

double runDSLTest() {
    lmax::Disruptor<TestEvent, BUFFER_SIZE, lmax::BusySpinWait> disruptor;

    SimpleHandler h1, h2, h3, h4;

    auto& g1 = disruptor.handleEventsWith(h1);
    auto& g2 = disruptor.handleEventsWith(h2);
    auto& g3 = disruptor.handleEventsWith(h3);
    disruptor.after(g1, g2, g3).handleEventsWith(h4);

    disruptor.start();

    // Let threads start
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
// Main - run each test multiple times and report stats
// ═══════════════════════════════════════════════════════════════════════════

void printStats(const std::vector<double>& results, const char* name) {
    std::vector<double> sorted = results;
    std::sort(sorted.begin(), sorted.end());

    double sum = std::accumulate(sorted.begin(), sorted.end(), 0.0);
    double avg = sum / sorted.size();
    double median = sorted[sorted.size() / 2];
    double min = sorted.front();
    double max = sorted.back();

    std::cout << std::setw(20) << name << ": "
              << "min=" << std::setw(6) << std::fixed << std::setprecision(2) << min
              << "  median=" << std::setw(6) << median
              << "  avg=" << std::setw(6) << avg
              << "  max=" << std::setw(6) << max << " M ops/sec\n";
}

int main() {
    std::cout << "═══════════════════════════════════════════════════════════════════\n";
    std::cout << "              DSL vs Raw Comparison\n";
    std::cout << "              Pattern: (h1, h2, h3) -> h4\n";
    std::cout << "              " << NUM_RUNS << " runs each, " << NUM_EVENTS << " events\n";
    std::cout << "═══════════════════════════════════════════════════════════════════\n\n";

    std::vector<double> rawResults, dslResults;

    // Warmup
    runRawTest();
    runDSLTest();

    std::cout << "Running tests...\n";

    // Interleave tests to reduce bias
    for (int i = 0; i < NUM_RUNS; i++) {
        std::cout << "  Run " << (i + 1) << "/" << NUM_RUNS << "\r" << std::flush;

        // Small delay between tests
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        rawResults.push_back(runRawTest());

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        dslResults.push_back(runDSLTest());
    }

    std::cout << "\n\nResults:\n";
    std::cout << "───────────────────────────────────────────────────────────────────\n";
    printStats(rawResults, "Raw (SequenceBarrier)");
    printStats(dslResults, "DSL (BatchProcessor)");

    // Calculate ratio using medians
    std::sort(rawResults.begin(), rawResults.end());
    std::sort(dslResults.begin(), dslResults.end());
    double rawMedian = rawResults[rawResults.size() / 2];
    double dslMedian = dslResults[dslResults.size() / 2];

    std::cout << "───────────────────────────────────────────────────────────────────\n";
    std::cout << "Ratio (Raw/DSL median): " << std::setprecision(2) << (rawMedian / dslMedian) << "x\n";
    std::cout << "═══════════════════════════════════════════════════════════════════\n";

    return 0;
}
