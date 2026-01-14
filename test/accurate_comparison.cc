/**
 * Accurate comparison using the SAME measurement methodology as multhread_q.cc
 *
 * Key differences from previous benchmarks:
 * 1. WITH throttling (sleep_cycles = 1000) - matches multhread_q
 * 2. Timestamp BEFORE push (not after) - matches multhread_q
 * 3. Measure in CYCLES not nanoseconds
 * 4. 50M messages like multhread_q
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

const int loop = 50000000;  // 50M like multhread_q
const int sleep_cycles = 1000;  // Throttle like multhread_q

struct BenchResult {
  std::string name;
  uint64_t p50_cycles;
  uint64_t p90_cycles;
  uint64_t p99_cycles;
  uint64_t p999_cycles;
  uint64_t p9999_cycles;
  uint64_t worst_cycles;
  double avg_cycles;
  double throughput_mops;
  double runtime_sec;
};

// ============================================================
// Benchmark SPSCQueue (original)
// ============================================================
BenchResult benchmark_spscqueue() {
  BenchResult result;
  result.name = "SPSCQueue";

  SPSCQueue<Msg, 1 << 10> queue;
  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.resize(loop);

  auto sendthread = [&]() {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    int g_val = 0;
    while (g_val < loop) {
      Msg* msg = queue.alloc();
      if (!msg) continue;

      msg->val_len = 1;
      msg->ts = rdtscp();  // Timestamp BEFORE push
      queue.push();
      g_val++;

      // Throttle
      auto expire = rdtsc() + sleep_cycles;
      while (rdtsc() < expire);
    }
  };

  auto recvthread = [&]() {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    int cnt = 0;
    long sum_lat = 0;
    int g_val = 0;

    while (g_val < loop) {
      Msg* msg = queue.front();
      if (!msg) continue;

      auto t2 = rdtscp();
      uint64_t delta = t2 - msg->ts;
      lat_vec[cnt] = delta;
      sum_lat += delta;
      cnt++;
      g_val++;

      queue.pop();
    }

    result.avg_cycles = (double)sum_lat / cnt;
  };

  std::thread tsend(sendthread);
  std::thread trecv(recvthread);

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  tsend.join();
  trecv.join();
  auto end = std::chrono::high_resolution_clock::now();

  result.runtime_sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = loop / result.runtime_sec / 1e6;

  std::sort(lat_vec.begin(), lat_vec.end());
  result.p50_cycles = lat_vec[lat_vec.size() / 2];
  result.p90_cycles = lat_vec[(lat_vec.size() * 90) / 100];
  result.p99_cycles = lat_vec[(lat_vec.size() * 99) / 100];
  result.p999_cycles = lat_vec[(lat_vec.size() * 999) / 1000];
  result.p9999_cycles = lat_vec[(lat_vec.size() * 9999) / 10000];
  result.worst_cycles = lat_vec[lat_vec.size() - 1];

  return result;
}

// ============================================================
// Benchmark SPSCQueueOPT
// ============================================================
BenchResult benchmark_spscqueueopt() {
  BenchResult result;
  result.name = "SPSCQueueOPT";

  SPSCQueueOPT<Msg, 1 << 10> queue;
  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.resize(loop);

  auto sendthread = [&]() {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    int g_val = 0;
    while (g_val < loop) {
      Msg* msg = queue.alloc();
      if (!msg) continue;

      msg->val_len = 1;
      msg->ts = rdtscp();
      queue.push();
      g_val++;

      auto expire = rdtsc() + sleep_cycles;
      while (rdtsc() < expire);
    }
  };

  auto recvthread = [&]() {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    int cnt = 0;
    long sum_lat = 0;
    int g_val = 0;

    while (g_val < loop) {
      Msg* msg = queue.front();
      if (!msg) continue;

      auto t2 = rdtscp();
      uint64_t delta = t2 - msg->ts;
      lat_vec[cnt] = delta;
      sum_lat += delta;
      cnt++;
      g_val++;

      queue.pop();
    }

    result.avg_cycles = (double)sum_lat / cnt;
  };

  std::thread tsend(sendthread);
  std::thread trecv(recvthread);

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  tsend.join();
  trecv.join();
  auto end = std::chrono::high_resolution_clock::now();

  result.runtime_sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = loop / result.runtime_sec / 1e6;

  std::sort(lat_vec.begin(), lat_vec.end());
  result.p50_cycles = lat_vec[lat_vec.size() / 2];
  result.p90_cycles = lat_vec[(lat_vec.size() * 90) / 100];
  result.p99_cycles = lat_vec[(lat_vec.size() * 99) / 100];
  result.p999_cycles = lat_vec[(lat_vec.size() * 999) / 1000];
  result.p9999_cycles = lat_vec[(lat_vec.size() * 9999) / 10000];
  result.worst_cycles = lat_vec[lat_vec.size() - 1];

  return result;
}

// ============================================================
// Benchmark rigtorp
// ============================================================
BenchResult benchmark_rigtorp() {
  BenchResult result;
  result.name = "rigtorp";

  rigtorp::SPSCQueue<Msg> queue(1 << 10);
  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.resize(loop);

  auto sendthread = [&]() {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    int g_val = 0;
    while (g_val < loop) {
      Msg msg;
      msg.val_len = 1;
      msg.ts = rdtscp();

      if (queue.try_push(msg)) {
        g_val++;
        auto expire = rdtsc() + sleep_cycles;
        while (rdtsc() < expire);
      }
    }
  };

  auto recvthread = [&]() {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    int cnt = 0;
    long sum_lat = 0;
    int g_val = 0;

    while (g_val < loop) {
      Msg* msg = queue.front();
      if (!msg) continue;

      auto t2 = rdtscp();
      uint64_t delta = t2 - msg->ts;
      lat_vec[cnt] = delta;
      sum_lat += delta;
      cnt++;
      g_val++;

      queue.pop();
    }

    result.avg_cycles = (double)sum_lat / cnt;
  };

  std::thread tsend(sendthread);
  std::thread trecv(recvthread);

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  tsend.join();
  trecv.join();
  auto end = std::chrono::high_resolution_clock::now();

  result.runtime_sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = loop / result.runtime_sec / 1e6;

  std::sort(lat_vec.begin(), lat_vec.end());
  result.p50_cycles = lat_vec[lat_vec.size() / 2];
  result.p90_cycles = lat_vec[(lat_vec.size() * 90) / 100];
  result.p99_cycles = lat_vec[(lat_vec.size() * 99) / 100];
  result.p999_cycles = lat_vec[(lat_vec.size() * 999) / 1000];
  result.p9999_cycles = lat_vec[(lat_vec.size() * 9999) / 10000];
  result.worst_cycles = lat_vec[lat_vec.size() - 1];

  return result;
}

// ============================================================
// Benchmark moodycamel
// ============================================================
BenchResult benchmark_moodycamel() {
  BenchResult result;
  result.name = "moodycamel";

  moodycamel::ReaderWriterQueue<Msg> queue(1 << 10);
  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.resize(loop);

  auto sendthread = [&]() {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    int g_val = 0;
    while (g_val < loop) {
      Msg msg;
      msg.val_len = 1;
      msg.ts = rdtscp();

      if (queue.try_enqueue(msg)) {
        g_val++;
        auto expire = rdtsc() + sleep_cycles;
        while (rdtsc() < expire);
      }
    }
  };

  auto recvthread = [&]() {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    int cnt = 0;
    long sum_lat = 0;
    int g_val = 0;
    Msg msg;

    while (g_val < loop) {
      if (!queue.try_dequeue(msg)) continue;

      auto t2 = rdtscp();
      uint64_t delta = t2 - msg.ts;
      lat_vec[cnt] = delta;
      sum_lat += delta;
      cnt++;
      g_val++;
    }

    result.avg_cycles = (double)sum_lat / cnt;
  };

  std::thread tsend(sendthread);
  std::thread trecv(recvthread);

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  tsend.join();
  trecv.join();
  auto end = std::chrono::high_resolution_clock::now();

  result.runtime_sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = loop / result.runtime_sec / 1e6;

  std::sort(lat_vec.begin(), lat_vec.end());
  result.p50_cycles = lat_vec[lat_vec.size() / 2];
  result.p90_cycles = lat_vec[(lat_vec.size() * 90) / 100];
  result.p99_cycles = lat_vec[(lat_vec.size() * 99) / 100];
  result.p999_cycles = lat_vec[(lat_vec.size() * 999) / 1000];
  result.p9999_cycles = lat_vec[(lat_vec.size() * 9999) / 10000];
  result.worst_cycles = lat_vec[lat_vec.size() - 1];

  return result;
}

void print_result(const BenchResult& r) {
  std::cout << "\n" << r.name << ":\n";
  std::cout << "  Runtime: " << std::fixed << std::setprecision(2) << r.runtime_sec << " sec\n";
  std::cout << "  Throughput: " << std::fixed << std::setprecision(2) << r.throughput_mops << " Mops/sec\n";
  std::cout << "  Avg latency: " << std::fixed << std::setprecision(0) << r.avg_cycles << " cycles\n";
  std::cout << "\n  Latency percentiles (cycles | nanoseconds @ 2.3GHz):\n";

  auto print_lat = [](const char* label, uint64_t cycles) {
    double ns = cycles * 1000.0 / 2300.0;
    std::cout << "    " << std::setw(6) << label << ": "
              << std::setw(8) << cycles << " cyc | "
              << std::setw(8) << std::fixed << std::setprecision(1) << ns << " ns\n";
  };

  print_lat("p50", r.p50_cycles);
  print_lat("p90", r.p90_cycles);
  print_lat("p99", r.p99_cycles);
  print_lat("p99.9", r.p999_cycles);
  print_lat("p99.99", r.p9999_cycles);
  print_lat("worst", r.worst_cycles);
}

int main() {
  std::cout << "=========================================\n";
  std::cout << "  Accurate SPSC Queue Comparison\n";
  std::cout << "  (Same methodology as multhread_q.cc)\n";
  std::cout << "=========================================\n\n";

  std::cout << "Configuration:\n";
  std::cout << "  Messages: " << loop << "\n";
  std::cout << "  Queue size: " << (1 << 10) << "\n";
  std::cout << "  Throttling: " << sleep_cycles << " cycles (~435ns)\n";
  std::cout << "  CPU: Intel Core i7-11800H @ 2.30GHz\n\n";

  std::vector<BenchResult> results;

  std::cout << "-----------------------------------------\n";
  std::cout << "Running benchmarks...\n";
  std::cout << "-----------------------------------------\n";

  results.push_back(benchmark_spscqueue());
  print_result(results.back());

  results.push_back(benchmark_spscqueueopt());
  print_result(results.back());

  results.push_back(benchmark_rigtorp());
  print_result(results.back());

  results.push_back(benchmark_moodycamel());
  print_result(results.back());

  // Summary table
  std::cout << "\n=========================================\n";
  std::cout << "Summary (Cycles)\n";
  std::cout << "=========================================\n\n";

  std::cout << std::setw(20) << "Implementation"
            << std::setw(12) << "Throughput"
            << std::setw(10) << "p50"
            << std::setw(10) << "p90"
            << std::setw(10) << "p99"
            << std::setw(10) << "p99.9"
            << std::setw(10) << "p99.99\n";
  std::cout << std::string(72, '-') << "\n";

  for (const auto& r : results) {
    std::cout << std::setw(20) << r.name
              << std::setw(9) << std::fixed << std::setprecision(2) << r.throughput_mops << " M"
              << std::setw(10) << r.p50_cycles
              << std::setw(10) << r.p90_cycles
              << std::setw(10) << r.p99_cycles
              << std::setw(10) << r.p999_cycles
              << std::setw(10) << r.p9999_cycles << "\n";
  }

  std::cout << "\n=========================================\n";
  std::cout << "Note: These numbers match multhread_q.cc\n";
  std::cout << "=========================================\n";

  return 0;
}
