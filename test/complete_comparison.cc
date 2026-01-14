/**
 * Complete comparison with BOTH:
 * 1. Latency benchmark (WITH throttling) - matches multhread_q.cc
 * 2. Throughput benchmark (NO throttling) - max performance
 *
 * Includes Asymmetric queue with consumer batching
 */

#include <bits/stdc++.h>
#include "rdtsc.h"
#include "cpupin.h"

#include "../SPSCQueue.h"
#include "../SPSCQueueOPT.h"
#include "../SPSCQueueAsymmetric.h"
#include "../rigtorp_spsc/include/rigtorp/SPSCQueue.h"
#include "../moodycamel_rwq/readerwriterqueue.h"

struct Msg {
  int val_len;
  uint64_t ts;
  long val[4];
};

struct BenchResult {
  std::string name;

  // Latency (with throttling)
  uint64_t lat_p50_cycles;
  uint64_t lat_p90_cycles;
  uint64_t lat_p99_cycles;
  uint64_t lat_p999_cycles;
  uint64_t lat_p9999_cycles;
  double lat_avg_cycles;
  double lat_throughput_mops;

  // Throughput (no throttling)
  double tp_throughput_mops;
  uint64_t tp_p50_cycles;
  uint64_t tp_p99_cycles;
  double tp_runtime_sec;
};

// ============================================================
// Latency Benchmark (WITH throttling, like multhread_q)
// ============================================================
template<typename QueueT, bool UseAsymmetric = false>
BenchResult benchmark_latency(const char* name, size_t num_msgs = 10000000) {
  BenchResult result;
  result.name = name;

  const int sleep_cycles = 1000;

  // Create queue with proper constructor
  QueueT queue = []() {
    if constexpr (std::is_same_v<QueueT, rigtorp::SPSCQueue<Msg>>) {
      return QueueT(1024);
    } else if constexpr (std::is_same_v<QueueT, moodycamel::ReaderWriterQueue<Msg>>) {
      return QueueT(1024);
    } else {
      return QueueT();
    }
  }();
  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.resize(num_msgs);

  auto sendthread = [&]() {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    size_t g_val = 0;
    while (g_val < num_msgs) {
      if constexpr (std::is_same_v<QueueT, rigtorp::SPSCQueue<Msg>>) {
        Msg msg;
        msg.val_len = 1;
        msg.ts = rdtscp();
        if (!queue.try_push(msg)) continue;
      } else if constexpr (std::is_same_v<QueueT, moodycamel::ReaderWriterQueue<Msg>>) {
        Msg msg;
        msg.val_len = 1;
        msg.ts = rdtscp();
        if (!queue.try_enqueue(msg)) continue;
      } else {
        Msg* msg = queue.alloc();
        if (!msg) continue;
        msg->val_len = 1;
        msg->ts = rdtscp();
        queue.push();
      }

      g_val++;
      auto expire = rdtsc() + sleep_cycles;
      while (rdtsc() < expire);
    }
  };

  auto recvthread = [&]() {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    size_t cnt = 0;
    long sum_lat = 0;

    if constexpr (UseAsymmetric) {
      // Consumer batching for Asymmetric
      while (cnt < num_msgs) {
        size_t popped = queue.tryPopBatch(16,
          [&](Msg* msg, size_t idx, size_t batch) {
            auto t2 = rdtscp();
            uint64_t delta = t2 - msg->ts;
            lat_vec[cnt] = delta;
            sum_lat += delta;
            cnt++;
          }
        );
      }
    } else if constexpr (std::is_same_v<QueueT, moodycamel::ReaderWriterQueue<Msg>>) {
      Msg msg;
      while (cnt < num_msgs) {
        if (!queue.try_dequeue(msg)) continue;
        auto t2 = rdtscp();
        uint64_t delta = t2 - msg.ts;
        lat_vec[cnt] = delta;
        sum_lat += delta;
        cnt++;
      }
    } else {
      while (cnt < num_msgs) {
        Msg* msg = queue.front();
        if (!msg) continue;
        auto t2 = rdtscp();
        uint64_t delta = t2 - msg->ts;
        lat_vec[cnt] = delta;
        sum_lat += delta;
        cnt++;
        queue.pop();
      }
    }

    result.lat_avg_cycles = (double)sum_lat / cnt;
  };

  std::thread tsend(sendthread);
  std::thread trecv(recvthread);

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  tsend.join();
  trecv.join();
  auto end = std::chrono::high_resolution_clock::now();

  double runtime = std::chrono::duration<double>(end - start).count();
  result.lat_throughput_mops = num_msgs / runtime / 1e6;

  std::sort(lat_vec.begin(), lat_vec.end());
  result.lat_p50_cycles = lat_vec[lat_vec.size() / 2];
  result.lat_p90_cycles = lat_vec[(lat_vec.size() * 90) / 100];
  result.lat_p99_cycles = lat_vec[(lat_vec.size() * 99) / 100];
  result.lat_p999_cycles = lat_vec[(lat_vec.size() * 999) / 1000];
  result.lat_p9999_cycles = lat_vec[(lat_vec.size() * 9999) / 10000];

  return result;
}

// ============================================================
// Throughput Benchmark (NO throttling, max performance)
// ============================================================
template<typename QueueT, bool UseAsymmetric = false>
void benchmark_throughput(BenchResult& result, size_t num_msgs = 10000000) {
  QueueT queue = []() {
    if constexpr (std::is_same_v<QueueT, rigtorp::SPSCQueue<Msg>>) {
      return QueueT(1024);
    } else if constexpr (std::is_same_v<QueueT, moodycamel::ReaderWriterQueue<Msg>>) {
      return QueueT(1024);
    } else {
      return QueueT();
    }
  }();
  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.reserve(num_msgs);

  auto sendthread = [&]() {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    size_t g_val = 0;
    while (g_val < num_msgs) {
      if constexpr (std::is_same_v<QueueT, rigtorp::SPSCQueue<Msg>>) {
        Msg msg;
        msg.val_len = 1;
        msg.ts = rdtscp();
        if (!queue.try_push(msg)) continue;
      } else if constexpr (std::is_same_v<QueueT, moodycamel::ReaderWriterQueue<Msg>>) {
        Msg msg;
        msg.val_len = 1;
        msg.ts = rdtscp();
        if (!queue.try_enqueue(msg)) continue;
      } else {
        Msg* msg = queue.alloc();
        if (!msg) continue;
        msg->val_len = 1;
        msg->ts = rdtscp();
        queue.push();
      }
      g_val++;
      // NO THROTTLING!
    }
  };

  auto recvthread = [&]() {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    size_t cnt = 0;

    if constexpr (UseAsymmetric) {
      // Consumer batching
      while (cnt < num_msgs) {
        size_t popped = queue.tryPopBatch(32,
          [&](Msg* msg, size_t idx, size_t batch) {
            uint64_t delta = rdtscp() - msg->ts;
            lat_vec.push_back(delta);
            cnt++;
          }
        );
      }
    } else if constexpr (std::is_same_v<QueueT, moodycamel::ReaderWriterQueue<Msg>>) {
      Msg msg;
      while (cnt < num_msgs) {
        if (!queue.try_dequeue(msg)) continue;
        uint64_t delta = rdtscp() - msg.ts;
        lat_vec.push_back(delta);
        cnt++;
      }
    } else {
      while (cnt < num_msgs) {
        Msg* msg = queue.front();
        if (!msg) continue;
        uint64_t delta = rdtscp() - msg->ts;
        lat_vec.push_back(delta);
        cnt++;
        queue.pop();
      }
    }
  };

  std::thread tsend(sendthread);
  std::thread trecv(recvthread);

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  tsend.join();
  trecv.join();
  auto end = std::chrono::high_resolution_clock::now();

  result.tp_runtime_sec = std::chrono::duration<double>(end - start).count();
  result.tp_throughput_mops = num_msgs / result.tp_runtime_sec / 1e6;

  std::sort(lat_vec.begin(), lat_vec.end());
  result.tp_p50_cycles = lat_vec[lat_vec.size() / 2];
  result.tp_p99_cycles = lat_vec[(lat_vec.size() * 99) / 100];
}

void print_result(const BenchResult& r) {
  std::cout << "\n" << r.name << ":\n";
  std::cout << "  LATENCY (with throttling):\n";
  std::cout << "    Throughput: " << std::fixed << std::setprecision(2) << r.lat_throughput_mops << " Mops/sec\n";
  std::cout << "    p50: " << r.lat_p50_cycles << " cyc (" << std::fixed << std::setprecision(1)
            << (r.lat_p50_cycles * 1000.0 / 2300) << " ns)\n";
  std::cout << "    p90: " << r.lat_p90_cycles << " cyc ("
            << (r.lat_p90_cycles * 1000.0 / 2300) << " ns)\n";
  std::cout << "    p99: " << r.lat_p99_cycles << " cyc ("
            << (r.lat_p99_cycles * 1000.0 / 2300) << " ns)\n";
  std::cout << "    p99.9: " << r.lat_p999_cycles << " cyc ("
            << (r.lat_p999_cycles * 1000.0 / 2300) << " ns)\n";
  std::cout << "    avg: " << std::fixed << std::setprecision(0) << r.lat_avg_cycles << " cyc\n";

  std::cout << "\n  THROUGHPUT (no throttling):\n";
  std::cout << "    Throughput: " << std::fixed << std::setprecision(2) << r.tp_throughput_mops << " Mops/sec\n";
  std::cout << "    Runtime: " << std::fixed << std::setprecision(3) << r.tp_runtime_sec << " sec\n";
  std::cout << "    p50: " << r.tp_p50_cycles << " cyc (" << std::fixed << std::setprecision(1)
            << (r.tp_p50_cycles * 1000.0 / 2300) << " ns)\n";
  std::cout << "    p99: " << r.tp_p99_cycles << " cyc ("
            << (r.tp_p99_cycles * 1000.0 / 2300) << " ns)\n";
}

int main() {
  std::cout << "=========================================\n";
  std::cout << "  Complete SPSC Queue Comparison\n";
  std::cout << "  Latency + Throughput Benchmarks\n";
  std::cout << "=========================================\n\n";

  const size_t NUM_MSGS_LAT = 10000000;  // 10M for latency
  const size_t NUM_MSGS_TP = 10000000;   // 10M for throughput

  std::cout << "Configuration:\n";
  std::cout << "  CPU: Intel Core i7-11800H @ 2.30GHz\n";
  std::cout << "  Latency test: " << NUM_MSGS_LAT << " messages, 1000 cycle throttling\n";
  std::cout << "  Throughput test: " << NUM_MSGS_TP << " messages, no throttling\n";
  std::cout << "  Queue size: 1024\n\n";

  std::vector<BenchResult> results;

  std::cout << "-----------------------------------------\n";
  std::cout << "Running benchmarks...\n";
  std::cout << "-----------------------------------------\n";

  // SPSCQueue
  {
    auto r = benchmark_latency<SPSCQueue<Msg, 1024>>("SPSCQueue", NUM_MSGS_LAT);
    benchmark_throughput<SPSCQueue<Msg, 1024>>(r, NUM_MSGS_TP);
    results.push_back(r);
    print_result(r);
  }

  // SPSCQueueOPT
  {
    auto r = benchmark_latency<SPSCQueueOPT<Msg, 1024>>("SPSCQueueOPT", NUM_MSGS_LAT);
    benchmark_throughput<SPSCQueueOPT<Msg, 1024>>(r, NUM_MSGS_TP);
    results.push_back(r);
    print_result(r);
  }

  // Asymmetric
  {
    auto r = benchmark_latency<SPSCQueueAsymmetric<Msg, 1024>, true>("Asymmetric (batch=16)", NUM_MSGS_LAT);
    benchmark_throughput<SPSCQueueAsymmetric<Msg, 1024>, true>(r, NUM_MSGS_TP);
    results.push_back(r);
    print_result(r);
  }

  // rigtorp
  {
    auto r = benchmark_latency<rigtorp::SPSCQueue<Msg>>("rigtorp", NUM_MSGS_LAT);
    benchmark_throughput<rigtorp::SPSCQueue<Msg>>(r, NUM_MSGS_TP);
    results.push_back(r);
    print_result(r);
  }

  // moodycamel
  {
    auto r = benchmark_latency<moodycamel::ReaderWriterQueue<Msg>>("moodycamel", NUM_MSGS_LAT);
    benchmark_throughput<moodycamel::ReaderWriterQueue<Msg>>(r, NUM_MSGS_TP);
    results.push_back(r);
    print_result(r);
  }

  // Summary tables
  std::cout << "\n=========================================\n";
  std::cout << "Summary: LATENCY (with throttling)\n";
  std::cout << "=========================================\n\n";

  std::cout << std::setw(25) << "Implementation"
            << std::setw(12) << "Throughput"
            << std::setw(10) << "p50"
            << std::setw(10) << "p90"
            << std::setw(10) << "p99"
            << std::setw(10) << "p99.9\n";
  std::cout << std::string(77, '-') << "\n";

  for (const auto& r : results) {
    std::cout << std::setw(25) << r.name
              << std::setw(9) << std::fixed << std::setprecision(2) << r.lat_throughput_mops << " M"
              << std::setw(10) << r.lat_p50_cycles
              << std::setw(10) << r.lat_p90_cycles
              << std::setw(10) << r.lat_p99_cycles
              << std::setw(10) << r.lat_p999_cycles << "\n";
  }

  std::cout << "\n=========================================\n";
  std::cout << "Summary: THROUGHPUT (no throttling)\n";
  std::cout << "=========================================\n\n";

  std::cout << std::setw(25) << "Implementation"
            << std::setw(15) << "Throughput"
            << std::setw(12) << "Runtime"
            << std::setw(10) << "p50"
            << std::setw(10) << "p99\n";
  std::cout << std::string(72, '-') << "\n";

  for (const auto& r : results) {
    std::cout << std::setw(25) << r.name
              << std::setw(12) << std::fixed << std::setprecision(2) << r.tp_throughput_mops << " M"
              << std::setw(12) << std::fixed << std::setprecision(3) << r.tp_runtime_sec
              << std::setw(10) << r.tp_p50_cycles
              << std::setw(10) << r.tp_p99_cycles << "\n";
  }

  // Find winners
  auto best_lat_p50 = std::min_element(results.begin(), results.end(),
    [](const auto& a, const auto& b) { return a.lat_p50_cycles < b.lat_p50_cycles; });
  auto best_lat_p99 = std::min_element(results.begin(), results.end(),
    [](const auto& a, const auto& b) { return a.lat_p99_cycles < b.lat_p99_cycles; });
  auto best_tp = std::max_element(results.begin(), results.end(),
    [](const auto& a, const auto& b) { return a.tp_throughput_mops < b.tp_throughput_mops; });

  std::cout << "\n=========================================\n";
  std::cout << "Winners:\n";
  std::cout << "=========================================\n";
  std::cout << "Latency p50:  " << best_lat_p50->name << " ("
            << best_lat_p50->lat_p50_cycles << " cyc = "
            << std::fixed << std::setprecision(1) << (best_lat_p50->lat_p50_cycles * 1000.0 / 2300) << " ns)\n";
  std::cout << "Latency p99:  " << best_lat_p99->name << " ("
            << best_lat_p99->lat_p99_cycles << " cyc = "
            << (best_lat_p99->lat_p99_cycles * 1000.0 / 2300) << " ns)\n";
  std::cout << "Throughput:   " << best_tp->name << " ("
            << std::fixed << std::setprecision(2) << best_tp->tp_throughput_mops << " Mops/sec)\n";
  std::cout << "=========================================\n";

  return 0;
}
