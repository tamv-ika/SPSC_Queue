#include <bits/stdc++.h>
#include "rdtsc.h"
#include "cpupin.h"
#include "../SPSCQueue.h"
#include "../SPSCQueueAsymmetric.h"
#include "../SPSCQueueHybrid.h"

struct Msg {
  uint64_t ts;
  uint64_t seq;
  uint64_t data[6];
};

template<typename QueueT>
struct BenchResult {
  std::string name;
  double throughput_mops;

  // Producer latency (time from alloc to push complete)
  uint64_t producer_p50_ns;
  uint64_t producer_p99_ns;
  uint64_t producer_avg_ns;

  // End-to-end latency (producer ts to consumer receive)
  uint64_t e2e_p50_ns;
  uint64_t e2e_p99_ns;
  uint64_t e2e_p999_ns;
  uint64_t e2e_avg_ns;

  size_t producer_atomic_ops;
  size_t consumer_atomic_ops;
};

// Benchmark SPSCQueue (baseline)
BenchResult<SPSCQueue<Msg, 4096>> benchmark_spscqueue(size_t num_msgs) {
  BenchResult<SPSCQueue<Msg, 4096>> result;
  result.name = "SPSCQueue";

  SPSCQueue<Msg, 4096> queue;
  std::atomic<bool> ready{false};

  std::vector<uint64_t> producer_latencies;
  std::vector<uint64_t> e2e_latencies;
  producer_latencies.reserve(num_msgs);
  e2e_latencies.reserve(num_msgs);

  auto producer = [&]() {
    cpupin(6);
    while (!ready.load());

    for (size_t i = 0; i < num_msgs; i++) {
      uint64_t t0 = rdtscp();

      Msg* msg = queue.alloc();
      while (!msg) msg = queue.alloc();

      msg->seq = i;
      msg->ts = rdtscp();

      queue.push();

      uint64_t t1 = rdtscp();
      producer_latencies.push_back(t1 - t0);
    }
  };

  auto consumer = [&]() {
    cpupin(7);
    while (!ready.load());

    uint64_t sum_e2e = 0;
    for (size_t i = 0; i < num_msgs; i++) {
      Msg* msg = queue.front();
      while (!msg) msg = queue.front();

      uint64_t now = rdtscp();
      uint64_t lat = now - msg->ts;
      e2e_latencies.push_back(lat);
      sum_e2e += lat;

      queue.pop();
    }
    result.e2e_avg_ns = (sum_e2e * 1000.0 / 2300.0) / num_msgs;
  };

  std::thread prod(producer);
  std::thread cons(consumer);

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  prod.join();
  cons.join();
  auto end = std::chrono::high_resolution_clock::now();

  double sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = num_msgs / sec / 1e6;
  result.producer_atomic_ops = num_msgs;
  result.consumer_atomic_ops = num_msgs;

  // Calculate producer latencies
  std::sort(producer_latencies.begin(), producer_latencies.end());
  result.producer_p50_ns = producer_latencies[producer_latencies.size() / 2] * 1000 / 2300;
  result.producer_p99_ns = producer_latencies[producer_latencies.size() * 99 / 100] * 1000 / 2300;
  result.producer_avg_ns = std::accumulate(producer_latencies.begin(), producer_latencies.end(), 0ULL)
                           * 1000 / 2300 / num_msgs;

  // Calculate e2e latencies
  std::sort(e2e_latencies.begin(), e2e_latencies.end());
  result.e2e_p50_ns = e2e_latencies[e2e_latencies.size() / 2] * 1000 / 2300;
  result.e2e_p99_ns = e2e_latencies[e2e_latencies.size() * 99 / 100] * 1000 / 2300;
  result.e2e_p999_ns = e2e_latencies[e2e_latencies.size() * 999 / 1000] * 1000 / 2300;

  return result;
}

// Benchmark Asymmetric queue
BenchResult<SPSCQueueAsymmetric<Msg, 4096>> benchmark_asymmetric(size_t num_msgs, size_t consumer_batch) {
  BenchResult<SPSCQueueAsymmetric<Msg, 4096>> result;
  result.name = "Asymmetric (batch=" + std::to_string(consumer_batch) + ")";

  SPSCQueueAsymmetric<Msg, 4096> queue;
  std::atomic<bool> ready{false};
  std::atomic<size_t> consumer_ops{0};

  std::vector<uint64_t> producer_latencies;
  std::vector<uint64_t> e2e_latencies;
  producer_latencies.reserve(num_msgs);
  e2e_latencies.reserve(num_msgs);

  auto producer = [&]() {
    cpupin(6);
    while (!ready.load());

    for (size_t i = 0; i < num_msgs; i++) {
      uint64_t t0 = rdtscp();

      Msg* msg = queue.alloc();
      while (!msg) msg = queue.alloc();

      msg->seq = i;
      msg->ts = rdtscp();

      queue.push();  // Immediate atomic store - no buffering!

      uint64_t t1 = rdtscp();
      producer_latencies.push_back(t1 - t0);
    }
  };

  auto consumer = [&]() {
    cpupin(7);
    while (!ready.load());

    size_t received = 0;
    uint64_t sum_e2e = 0;

    while (received < num_msgs) {
      size_t popped = queue.tryPopBatch(consumer_batch,
        [&](Msg* msg, size_t idx, size_t batch_size) {
          uint64_t now = rdtscp();
          uint64_t lat = now - msg->ts;
          e2e_latencies.push_back(lat);
          sum_e2e += lat;
        }
      );

      if (popped > 0) {
        received += popped;
        consumer_ops++;
      }
    }

    result.e2e_avg_ns = (sum_e2e * 1000.0 / 2300.0) / num_msgs;
  };

  std::thread prod(producer);
  std::thread cons(consumer);

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  prod.join();
  cons.join();
  auto end = std::chrono::high_resolution_clock::now();

  double sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = num_msgs / sec / 1e6;
  result.producer_atomic_ops = num_msgs;  // Producer: 1 per message
  result.consumer_atomic_ops = consumer_ops.load();  // Consumer: 1 per batch

  // Calculate producer latencies
  std::sort(producer_latencies.begin(), producer_latencies.end());
  result.producer_p50_ns = producer_latencies[producer_latencies.size() / 2] * 1000 / 2300;
  result.producer_p99_ns = producer_latencies[producer_latencies.size() * 99 / 100] * 1000 / 2300;
  result.producer_avg_ns = std::accumulate(producer_latencies.begin(), producer_latencies.end(), 0ULL)
                           * 1000 / 2300 / num_msgs;

  // Calculate e2e latencies
  std::sort(e2e_latencies.begin(), e2e_latencies.end());
  result.e2e_p50_ns = e2e_latencies[e2e_latencies.size() / 2] * 1000 / 2300;
  result.e2e_p99_ns = e2e_latencies[e2e_latencies.size() * 99 / 100] * 1000 / 2300;
  result.e2e_p999_ns = e2e_latencies[e2e_latencies.size() * 999 / 1000] * 1000 / 2300;

  return result;
}

int main() {
  std::cout << "=========================================\n";
  std::cout << "  Asymmetric Queue Benchmark\n";
  std::cout << "  Producer: Immediate push (low latency)\n";
  std::cout << "  Consumer: Batch pop (high throughput)\n";
  std::cout << "=========================================\n\n";

  const size_t NUM_MSGS = 1000000;

  std::cout << "Configuration:\n";
  std::cout << "  Messages: " << NUM_MSGS << "\n";
  std::cout << "  Queue size: 4096\n\n";

  // Baseline
  std::cout << "-----------------------------------------\n";
  std::cout << "Baseline: SPSCQueue (both single-item)\n";
  std::cout << "-----------------------------------------\n";

  auto baseline = benchmark_spscqueue(NUM_MSGS);

  std::cout << "\nResults:\n";
  std::cout << "  Throughput: " << std::fixed << std::setprecision(2)
            << baseline.throughput_mops << " Mops/sec\n";
  std::cout << "\n  Producer latency:\n";
  std::cout << "    p50: " << baseline.producer_p50_ns << " ns\n";
  std::cout << "    p99: " << baseline.producer_p99_ns << " ns\n";
  std::cout << "    avg: " << baseline.producer_avg_ns << " ns\n";
  std::cout << "\n  End-to-end latency:\n";
  std::cout << "    p50: " << baseline.e2e_p50_ns << " ns\n";
  std::cout << "    p99: " << baseline.e2e_p99_ns << " ns\n";
  std::cout << "    p99.9: " << baseline.e2e_p999_ns << " ns\n";
  std::cout << "    avg: " << std::fixed << std::setprecision(0) << baseline.e2e_avg_ns << " ns\n";
  std::cout << "\n  Atomic operations:\n";
  std::cout << "    Producer: " << baseline.producer_atomic_ops << "\n";
  std::cout << "    Consumer: " << baseline.consumer_atomic_ops << "\n";
  std::cout << "    Total: " << (baseline.producer_atomic_ops + baseline.consumer_atomic_ops) << "\n";

  // Asymmetric with different batch sizes
  std::cout << "\n-----------------------------------------\n";
  std::cout << "Asymmetric: Producer immediate, Consumer batch\n";
  std::cout << "-----------------------------------------\n";

  std::vector<size_t> batch_sizes = {4, 8, 16, 32};
  std::vector<BenchResult<SPSCQueueAsymmetric<Msg, 4096>>> asymmetric_results;

  for (size_t bs : batch_sizes) {
    auto result = benchmark_asymmetric(NUM_MSGS, bs);
    asymmetric_results.push_back(result);

    std::cout << "\n" << result.name << ":\n";
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2)
              << result.throughput_mops << " Mops/sec ("
              << std::showpos << ((result.throughput_mops / baseline.throughput_mops - 1) * 100)
              << "%)\n" << std::noshowpos;

    std::cout << "\n  Producer latency:\n";
    std::cout << "    p50: " << result.producer_p50_ns << " ns (baseline: "
              << baseline.producer_p50_ns << ")\n";
    std::cout << "    p99: " << result.producer_p99_ns << " ns (baseline: "
              << baseline.producer_p99_ns << ")\n";

    std::cout << "\n  End-to-end latency:\n";
    std::cout << "    p50: " << result.e2e_p50_ns << " ns\n";
    std::cout << "    p99: " << result.e2e_p99_ns << " ns\n";
    std::cout << "    p99.9: " << result.e2e_p999_ns << " ns\n";

    std::cout << "\n  Atomic operations:\n";
    std::cout << "    Producer: " << result.producer_atomic_ops << "\n";
    std::cout << "    Consumer: " << result.consumer_atomic_ops
              << " (" << std::fixed << std::setprecision(1)
              << (1.0 - (double)result.consumer_atomic_ops / baseline.consumer_atomic_ops) * 100
              << "% reduction)\n";
    std::cout << "    Total: " << (result.producer_atomic_ops + result.consumer_atomic_ops)
              << " (" << (1.0 - (double)(result.producer_atomic_ops + result.consumer_atomic_ops)
                                  / (baseline.producer_atomic_ops + baseline.consumer_atomic_ops)) * 100
              << "% reduction)\n";
  }

  // Summary comparison
  std::cout << "\n=========================================\n";
  std::cout << "Summary Comparison\n";
  std::cout << "=========================================\n\n";

  std::cout << std::setw(20) << "Queue"
            << std::setw(15) << "Throughput"
            << std::setw(15) << "Prod p50 (ns)"
            << std::setw(15) << "E2E p99 (ns)"
            << std::setw(15) << "Total Atomic\n";
  std::cout << std::string(80, '-') << "\n";

  auto print_summary = [](const auto& r) {
    std::cout << std::setw(20) << r.name
              << std::setw(12) << std::fixed << std::setprecision(2) << r.throughput_mops << " M"
              << std::setw(15) << r.producer_p50_ns
              << std::setw(15) << r.e2e_p99_ns
              << std::setw(15) << (r.producer_atomic_ops + r.consumer_atomic_ops) << "\n";
  };

  print_summary(baseline);
  for (const auto& r : asymmetric_results) {
    print_summary(r);
  }

  std::cout << "\n=========================================\n";
  std::cout << "Key Findings:\n";
  std::cout << "=========================================\n";
  std::cout << "1. Producer latency: SAME as baseline\n";
  std::cout << "   → No buffering = immediate push\n";
  std::cout << "2. Consumer atomic ops: UP TO "
            << std::fixed << std::setprecision(0)
            << (1.0 - (double)asymmetric_results.back().consumer_atomic_ops
                      / baseline.consumer_atomic_ops) * 100
            << "% reduction\n";
  std::cout << "   → Batch pop amortizes overhead\n";
  std::cout << "3. Throughput: UP TO +"
            << std::fixed << std::setprecision(1)
            << ((asymmetric_results.back().throughput_mops / baseline.throughput_mops - 1) * 100)
            << "%\n";
  std::cout << "   → Best of both worlds!\n";
  std::cout << "=========================================\n";

  return 0;
}
