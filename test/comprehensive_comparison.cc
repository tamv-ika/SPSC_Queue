/**
 * Comprehensive benchmark comparing multiple SPSC queue implementations:
 * 1. Original SPSCQueue (Meng Rao)
 * 2. SPSCQueueAsymmetric (our optimized version)
 * 3. rigtorp::SPSCQueue (Erik Rigtorp)
 * 4. moodycamel::ReaderWriterQueue (Cameron Desrochers)
 */

#include <bits/stdc++.h>
#include "rdtsc.h"
#include "cpupin.h"

// Our implementations
#include "../SPSCQueue.h"
#include "../SPSCQueueAsymmetric.h"

// Third-party implementations
#include "../rigtorp_spsc/include/rigtorp/SPSCQueue.h"
#include "../moodycamel_rwq/readerwriterqueue.h"

struct Msg {
  uint64_t ts;
  uint64_t seq;
  uint64_t data[6];  // 64 bytes total
};

struct BenchResult {
  std::string impl_name;
  double throughput_mops;

  // Producer metrics
  uint64_t producer_p50_ns;
  uint64_t producer_p99_ns;
  uint64_t producer_max_ns;
  double producer_avg_ns;

  // End-to-end metrics
  uint64_t e2e_p50_ns;
  uint64_t e2e_p90_ns;
  uint64_t e2e_p99_ns;
  uint64_t e2e_p999_ns;
  uint64_t e2e_max_ns;
  double e2e_avg_ns;

  double runtime_sec;
};

// ============================================================
// Benchmark: Original SPSCQueue
// ============================================================
BenchResult benchmark_original(size_t num_msgs) {
  BenchResult result;
  result.impl_name = "SPSCQueue (original)";

  SPSCQueue<Msg, 4096> queue;
  std::atomic<bool> ready{false};

  std::vector<uint64_t> producer_lat, e2e_lat;
  producer_lat.reserve(num_msgs);
  e2e_lat.reserve(num_msgs);

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
      producer_lat.push_back(rdtscp() - t0);
    }
  };

  auto consumer = [&]() {
    cpupin(7);
    while (!ready.load());

    uint64_t sum = 0;
    for (size_t i = 0; i < num_msgs; i++) {
      Msg* msg = queue.front();
      while (!msg) msg = queue.front();
      uint64_t now = rdtscp();
      uint64_t lat = now - msg->ts;
      e2e_lat.push_back(lat);
      sum += lat;
      queue.pop();
    }
    result.e2e_avg_ns = (sum * 1000.0 / 2300) / num_msgs;
  };

  std::thread prod(producer);
  std::thread cons(consumer);

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  prod.join();
  cons.join();
  auto end = std::chrono::high_resolution_clock::now();

  result.runtime_sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = num_msgs / result.runtime_sec / 1e6;

  // Calculate percentiles
  std::sort(producer_lat.begin(), producer_lat.end());
  std::sort(e2e_lat.begin(), e2e_lat.end());

  result.producer_p50_ns = producer_lat[producer_lat.size() / 2] * 1000 / 2300;
  result.producer_p99_ns = producer_lat[producer_lat.size() * 99 / 100] * 1000 / 2300;
  result.producer_max_ns = producer_lat.back() * 1000 / 2300;
  result.producer_avg_ns = std::accumulate(producer_lat.begin(), producer_lat.end(), 0ULL) * 1000.0 / 2300 / num_msgs;

  result.e2e_p50_ns = e2e_lat[e2e_lat.size() / 2] * 1000 / 2300;
  result.e2e_p90_ns = e2e_lat[e2e_lat.size() * 90 / 100] * 1000 / 2300;
  result.e2e_p99_ns = e2e_lat[e2e_lat.size() * 99 / 100] * 1000 / 2300;
  result.e2e_p999_ns = e2e_lat[e2e_lat.size() * 999 / 1000] * 1000 / 2300;
  result.e2e_max_ns = e2e_lat.back() * 1000 / 2300;

  return result;
}

// ============================================================
// Benchmark: Asymmetric
// ============================================================
BenchResult benchmark_asymmetric(size_t num_msgs, size_t consumer_batch) {
  BenchResult result;
  result.impl_name = "Asymmetric (batch=" + std::to_string(consumer_batch) + ")";

  SPSCQueueAsymmetric<Msg, 4096> queue;
  std::atomic<bool> ready{false};

  std::vector<uint64_t> producer_lat, e2e_lat;
  producer_lat.reserve(num_msgs);
  e2e_lat.reserve(num_msgs);

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
      producer_lat.push_back(rdtscp() - t0);
    }
  };

  auto consumer = [&]() {
    cpupin(7);
    while (!ready.load());

    size_t received = 0;
    uint64_t sum = 0;
    while (received < num_msgs) {
      size_t popped = queue.tryPopBatch(consumer_batch,
        [&](Msg* msg, size_t idx, size_t batch) {
          uint64_t now = rdtscp();
          uint64_t lat = now - msg->ts;
          e2e_lat.push_back(lat);
          sum += lat;
        }
      );
      received += popped;
    }
    result.e2e_avg_ns = (sum * 1000.0 / 2300) / num_msgs;
  };

  std::thread prod(producer);
  std::thread cons(consumer);

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  prod.join();
  cons.join();
  auto end = std::chrono::high_resolution_clock::now();

  result.runtime_sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = num_msgs / result.runtime_sec / 1e6;

  std::sort(producer_lat.begin(), producer_lat.end());
  std::sort(e2e_lat.begin(), e2e_lat.end());

  result.producer_p50_ns = producer_lat[producer_lat.size() / 2] * 1000 / 2300;
  result.producer_p99_ns = producer_lat[producer_lat.size() * 99 / 100] * 1000 / 2300;
  result.producer_max_ns = producer_lat.back() * 1000 / 2300;
  result.producer_avg_ns = std::accumulate(producer_lat.begin(), producer_lat.end(), 0ULL) * 1000.0 / 2300 / num_msgs;

  result.e2e_p50_ns = e2e_lat[e2e_lat.size() / 2] * 1000 / 2300;
  result.e2e_p90_ns = e2e_lat[e2e_lat.size() * 90 / 100] * 1000 / 2300;
  result.e2e_p99_ns = e2e_lat[e2e_lat.size() * 99 / 100] * 1000 / 2300;
  result.e2e_p999_ns = e2e_lat[e2e_lat.size() * 999 / 1000] * 1000 / 2300;
  result.e2e_max_ns = e2e_lat.back() * 1000 / 2300;

  return result;
}

// ============================================================
// Benchmark: rigtorp SPSCQueue
// ============================================================
BenchResult benchmark_rigtorp(size_t num_msgs) {
  BenchResult result;
  result.impl_name = "rigtorp::SPSCQueue";

  rigtorp::SPSCQueue<Msg> queue(4096);
  std::atomic<bool> ready{false};

  std::vector<uint64_t> producer_lat, e2e_lat;
  producer_lat.reserve(num_msgs);
  e2e_lat.reserve(num_msgs);

  auto producer = [&]() {
    cpupin(6);
    while (!ready.load());

    for (size_t i = 0; i < num_msgs; i++) {
      uint64_t t0 = rdtscp();
      Msg msg;
      msg.seq = i;
      msg.ts = rdtscp();
      while (!queue.try_push(msg));
      producer_lat.push_back(rdtscp() - t0);
    }
  };

  auto consumer = [&]() {
    cpupin(7);
    while (!ready.load());

    uint64_t sum = 0;
    for (size_t i = 0; i < num_msgs; i++) {
      Msg* msg = queue.front();
      while (!msg) msg = queue.front();
      uint64_t now = rdtscp();
      uint64_t lat = now - msg->ts;
      e2e_lat.push_back(lat);
      sum += lat;
      queue.pop();
    }
    result.e2e_avg_ns = (sum * 1000.0 / 2300) / num_msgs;
  };

  std::thread prod(producer);
  std::thread cons(consumer);

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  prod.join();
  cons.join();
  auto end = std::chrono::high_resolution_clock::now();

  result.runtime_sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = num_msgs / result.runtime_sec / 1e6;

  std::sort(producer_lat.begin(), producer_lat.end());
  std::sort(e2e_lat.begin(), e2e_lat.end());

  result.producer_p50_ns = producer_lat[producer_lat.size() / 2] * 1000 / 2300;
  result.producer_p99_ns = producer_lat[producer_lat.size() * 99 / 100] * 1000 / 2300;
  result.producer_max_ns = producer_lat.back() * 1000 / 2300;
  result.producer_avg_ns = std::accumulate(producer_lat.begin(), producer_lat.end(), 0ULL) * 1000.0 / 2300 / num_msgs;

  result.e2e_p50_ns = e2e_lat[e2e_lat.size() / 2] * 1000 / 2300;
  result.e2e_p90_ns = e2e_lat[e2e_lat.size() * 90 / 100] * 1000 / 2300;
  result.e2e_p99_ns = e2e_lat[e2e_lat.size() * 99 / 100] * 1000 / 2300;
  result.e2e_p999_ns = e2e_lat[e2e_lat.size() * 999 / 1000] * 1000 / 2300;
  result.e2e_max_ns = e2e_lat.back() * 1000 / 2300;

  return result;
}

// ============================================================
// Benchmark: moodycamel ReaderWriterQueue
// ============================================================
BenchResult benchmark_moodycamel(size_t num_msgs) {
  BenchResult result;
  result.impl_name = "moodycamel::ReaderWriterQueue";

  moodycamel::ReaderWriterQueue<Msg> queue(4096);
  std::atomic<bool> ready{false};

  std::vector<uint64_t> producer_lat, e2e_lat;
  producer_lat.reserve(num_msgs);
  e2e_lat.reserve(num_msgs);

  auto producer = [&]() {
    cpupin(6);
    while (!ready.load());

    for (size_t i = 0; i < num_msgs; i++) {
      uint64_t t0 = rdtscp();
      Msg msg;
      msg.seq = i;
      msg.ts = rdtscp();
      while (!queue.try_enqueue(msg));
      producer_lat.push_back(rdtscp() - t0);
    }
  };

  auto consumer = [&]() {
    cpupin(7);
    while (!ready.load());

    uint64_t sum = 0;
    Msg msg;
    for (size_t i = 0; i < num_msgs; i++) {
      while (!queue.try_dequeue(msg));
      uint64_t now = rdtscp();
      uint64_t lat = now - msg.ts;
      e2e_lat.push_back(lat);
      sum += lat;
    }
    result.e2e_avg_ns = (sum * 1000.0 / 2300) / num_msgs;
  };

  std::thread prod(producer);
  std::thread cons(consumer);

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  prod.join();
  cons.join();
  auto end = std::chrono::high_resolution_clock::now();

  result.runtime_sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = num_msgs / result.runtime_sec / 1e6;

  std::sort(producer_lat.begin(), producer_lat.end());
  std::sort(e2e_lat.begin(), e2e_lat.end());

  result.producer_p50_ns = producer_lat[producer_lat.size() / 2] * 1000 / 2300;
  result.producer_p99_ns = producer_lat[producer_lat.size() * 99 / 100] * 1000 / 2300;
  result.producer_max_ns = producer_lat.back() * 1000 / 2300;
  result.producer_avg_ns = std::accumulate(producer_lat.begin(), producer_lat.end(), 0ULL) * 1000.0 / 2300 / num_msgs;

  result.e2e_p50_ns = e2e_lat[e2e_lat.size() / 2] * 1000 / 2300;
  result.e2e_p90_ns = e2e_lat[e2e_lat.size() * 90 / 100] * 1000 / 2300;
  result.e2e_p99_ns = e2e_lat[e2e_lat.size() * 99 / 100] * 1000 / 2300;
  result.e2e_p999_ns = e2e_lat[e2e_lat.size() * 999 / 1000] * 1000 / 2300;
  result.e2e_max_ns = e2e_lat.back() * 1000 / 2300;

  return result;
}

void print_result(const BenchResult& r) {
  std::cout << "\n" << r.impl_name << ":\n";
  std::cout << "  Runtime: " << std::fixed << std::setprecision(3) << r.runtime_sec << " sec\n";
  std::cout << "  Throughput: " << std::fixed << std::setprecision(2) << r.throughput_mops << " Mops/sec\n";
  std::cout << "\n  Producer latency:\n";
  std::cout << "    p50: " << r.producer_p50_ns << " ns\n";
  std::cout << "    p99: " << r.producer_p99_ns << " ns\n";
  std::cout << "    max: " << r.producer_max_ns << " ns\n";
  std::cout << "    avg: " << std::fixed << std::setprecision(0) << r.producer_avg_ns << " ns\n";
  std::cout << "\n  End-to-end latency:\n";
  std::cout << "    p50: " << r.e2e_p50_ns << " ns\n";
  std::cout << "    p90: " << r.e2e_p90_ns << " ns\n";
  std::cout << "    p99: " << r.e2e_p99_ns << " ns\n";
  std::cout << "    p99.9: " << r.e2e_p999_ns << " ns\n";
  std::cout << "    max: " << r.e2e_max_ns << " ns\n";
  std::cout << "    avg: " << std::fixed << std::setprecision(0) << r.e2e_avg_ns << " ns\n";
}

int main() {
  std::cout << "=========================================\n";
  std::cout << "  SPSC Queue Implementation Comparison\n";
  std::cout << "=========================================\n\n";

  const size_t NUM_MSGS = 1000000;

  std::cout << "Configuration:\n";
  std::cout << "  Messages: " << NUM_MSGS << "\n";
  std::cout << "  Message size: " << sizeof(Msg) << " bytes\n";
  std::cout << "  Queue size: 4096\n";
  std::cout << "  CPU: 11th Gen Intel Core i7-11800H @ 2.30GHz\n\n";

  std::vector<BenchResult> results;

  std::cout << "-----------------------------------------\n";
  std::cout << "Running benchmarks...\n";
  std::cout << "-----------------------------------------\n";

  results.push_back(benchmark_original(NUM_MSGS));
  print_result(results.back());

  results.push_back(benchmark_asymmetric(NUM_MSGS, 16));
  print_result(results.back());

  results.push_back(benchmark_rigtorp(NUM_MSGS));
  print_result(results.back());

  results.push_back(benchmark_moodycamel(NUM_MSGS));
  print_result(results.back());

  // Summary table
  std::cout << "\n=========================================\n";
  std::cout << "Summary Comparison\n";
  std::cout << "=========================================\n\n";

  std::cout << std::setw(30) << "Implementation"
            << std::setw(15) << "Throughput"
            << std::setw(12) << "Prod p50"
            << std::setw(12) << "Prod p99"
            << std::setw(12) << "E2E p50"
            << std::setw(12) << "E2E p99\n";
  std::cout << std::string(93, '-') << "\n";

  for (const auto& r : results) {
    std::cout << std::setw(30) << r.impl_name
              << std::setw(12) << std::fixed << std::setprecision(2) << r.throughput_mops << " M"
              << std::setw(12) << r.producer_p50_ns
              << std::setw(12) << r.producer_p99_ns
              << std::setw(12) << r.e2e_p50_ns
              << std::setw(12) << r.e2e_p99_ns << "\n";
  }

  // Find best in each category
  auto best_throughput = std::max_element(results.begin(), results.end(),
    [](const auto& a, const auto& b) { return a.throughput_mops < b.throughput_mops; });
  auto best_prod_lat = std::min_element(results.begin(), results.end(),
    [](const auto& a, const auto& b) { return a.producer_p50_ns < b.producer_p50_ns; });
  auto best_e2e_lat = std::min_element(results.begin(), results.end(),
    [](const auto& a, const auto& b) { return a.e2e_p50_ns < b.e2e_p50_ns; });

  std::cout << "\n=========================================\n";
  std::cout << "Winners:\n";
  std::cout << "=========================================\n";
  std::cout << "Best Throughput:      " << best_throughput->impl_name
            << " (" << std::fixed << std::setprecision(2) << best_throughput->throughput_mops << " Mops/sec)\n";
  std::cout << "Best Producer Latency: " << best_prod_lat->impl_name
            << " (" << best_prod_lat->producer_p50_ns << " ns p50)\n";
  std::cout << "Best E2E Latency:      " << best_e2e_lat->impl_name
            << " (" << best_e2e_lat->e2e_p50_ns << " ns p50)\n";
  std::cout << "=========================================\n";

  return 0;
}
