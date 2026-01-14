/**
 * Complete benchmark with HEAP allocation
 * Tests both LATENCY (with throttling) and THROUGHPUT (no throttling)
 * Tests multiple batch sizes for Asymmetric queue
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

const int64_t lat_iters = 10000000;  // 10M for latency test
const int64_t tp_iters = 10000000;   // 10M for throughput test
const int sleep_cycles = 1000;        // Throttling

struct BenchResult {
  std::string name;
  size_t queue_size;

  // Latency metrics (with throttling)
  uint64_t lat_p50_cycles;
  uint64_t lat_p90_cycles;
  uint64_t lat_p99_cycles;
  uint64_t lat_p999_cycles;
  double lat_avg_cycles;
  double lat_throughput_mops;

  // Throughput metrics (no throttling)
  double tp_throughput_mops;
  double tp_runtime_sec;
  uint64_t tp_p50_cycles;
  uint64_t tp_p99_cycles;
};

// ============================================================
// Latency Benchmark - SPSCQueueOPT
// ============================================================
template<size_t SIZE>
BenchResult benchmark_latency_spscqueueopt(const char* name, size_t queue_size) {
  BenchResult result;
  result.name = name;
  result.queue_size = queue_size;

  auto q_ptr = std::make_unique<SPSCQueueOPT<Msg, SIZE>>();
  auto& q = *q_ptr;

  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.resize(lat_iters);

  auto sendthread = [&]() {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    int64_t g_val = 0;
    while (g_val < lat_iters) {
      Msg* msg = q.alloc();
      if (!msg) continue;

      msg->val_len = 1;
      msg->ts = rdtscp();
      q.push();
      g_val++;

      auto expire = rdtsc() + sleep_cycles;
      while (rdtsc() < expire);
    }
  };

  auto recvthread = [&]() {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    int64_t cnt = 0;
    long sum_lat = 0;
    int64_t g_val = 0;

    while (g_val < lat_iters) {
      Msg* msg = q.front();
      if (!msg) continue;

      auto t2 = rdtscp();
      uint64_t delta = t2 - msg->ts;
      lat_vec[cnt] = delta;
      sum_lat += delta;
      cnt++;
      g_val++;

      q.pop();
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
  result.lat_throughput_mops = lat_iters / runtime / 1e6;

  std::sort(lat_vec.begin(), lat_vec.end());
  result.lat_p50_cycles = lat_vec[lat_vec.size() / 2];
  result.lat_p90_cycles = lat_vec[(lat_vec.size() * 90) / 100];
  result.lat_p99_cycles = lat_vec[(lat_vec.size() * 99) / 100];
  result.lat_p999_cycles = lat_vec[(lat_vec.size() * 999) / 1000];

  return result;
}

// ============================================================
// Throughput Benchmark - SPSCQueueOPT
// ============================================================
template<size_t SIZE>
void benchmark_throughput_spscqueueopt(BenchResult& result) {
  auto q_ptr = std::make_unique<SPSCQueueOPT<Msg, SIZE>>();
  auto& q = *q_ptr;

  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.reserve(tp_iters);

  auto sendthread = [&]() {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    int64_t g_val = 0;
    while (g_val < tp_iters) {
      Msg* msg = q.alloc();
      if (!msg) continue;

      msg->val_len = 1;
      msg->ts = rdtscp();
      q.push();
      g_val++;
    }
  };

  auto recvthread = [&]() {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    int64_t g_val = 0;

    while (g_val < tp_iters) {
      Msg* msg = q.front();
      if (!msg) continue;

      auto t2 = rdtscp();
      uint64_t delta = t2 - msg->ts;
      lat_vec.push_back(delta);
      g_val++;

      q.pop();
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
  result.tp_throughput_mops = tp_iters / result.tp_runtime_sec / 1e6;

  if (!lat_vec.empty()) {
    std::sort(lat_vec.begin(), lat_vec.end());
    result.tp_p50_cycles = lat_vec[lat_vec.size() / 2];
    result.tp_p99_cycles = lat_vec[(lat_vec.size() * 99) / 100];
  }
}

// ============================================================
// Latency Benchmark - Asymmetric
// ============================================================
template<size_t SIZE, size_t BATCH_SIZE>
BenchResult benchmark_latency_asymmetric(const char* name, size_t queue_size) {
  BenchResult result;
  result.name = name;
  result.queue_size = queue_size;

  auto q_ptr = std::make_unique<SPSCQueueAsymmetric<Msg, SIZE>>();
  auto& q = *q_ptr;

  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.resize(lat_iters);

  auto sendthread = [&]() {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    int64_t g_val = 0;
    while (g_val < lat_iters) {
      Msg* msg = q.alloc();
      if (!msg) continue;

      msg->val_len = 1;
      msg->ts = rdtscp();
      q.push();
      g_val++;

      auto expire = rdtsc() + sleep_cycles;
      while (rdtsc() < expire);
    }
  };

  auto recvthread = [&]() {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    int64_t cnt = 0;
    long sum_lat = 0;

    while (cnt < lat_iters) {
      size_t batch = q.tryPopBatch(BATCH_SIZE, [&](Msg* msg, size_t idx, size_t total) {
        auto t2 = rdtscp();
        uint64_t delta = t2 - msg->ts;
        lat_vec[cnt] = delta;
        sum_lat += delta;
        cnt++;
      });

      if (batch == 0) {
        Msg* msg = q.front();
        if (msg) {
          auto t2 = rdtscp();
          uint64_t delta = t2 - msg->ts;
          lat_vec[cnt] = delta;
          sum_lat += delta;
          cnt++;
          q.pop();
        }
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
  result.lat_throughput_mops = lat_iters / runtime / 1e6;

  std::sort(lat_vec.begin(), lat_vec.end());
  result.lat_p50_cycles = lat_vec[lat_vec.size() / 2];
  result.lat_p90_cycles = lat_vec[(lat_vec.size() * 90) / 100];
  result.lat_p99_cycles = lat_vec[(lat_vec.size() * 99) / 100];
  result.lat_p999_cycles = lat_vec[(lat_vec.size() * 999) / 1000];

  return result;
}

// ============================================================
// Throughput Benchmark - Asymmetric
// ============================================================
template<size_t SIZE, size_t BATCH_SIZE>
void benchmark_throughput_asymmetric(BenchResult& result) {
  auto q_ptr = std::make_unique<SPSCQueueAsymmetric<Msg, SIZE>>();
  auto& q = *q_ptr;

  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.reserve(tp_iters);

  auto sendthread = [&]() {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    int64_t g_val = 0;
    while (g_val < tp_iters) {
      Msg* msg = q.alloc();
      if (!msg) continue;

      msg->val_len = 1;
      msg->ts = rdtscp();
      q.push();
      g_val++;
    }
  };

  auto recvthread = [&]() {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    int64_t cnt = 0;

    while (cnt < tp_iters) {
      size_t batch = q.tryPopBatch(BATCH_SIZE, [&](Msg* msg, size_t idx, size_t total) {
        auto t2 = rdtscp();
        uint64_t delta = t2 - msg->ts;
        lat_vec.push_back(delta);
        cnt++;
      });

      if (batch == 0) {
        Msg* msg = q.front();
        if (msg) {
          auto t2 = rdtscp();
          uint64_t delta = t2 - msg->ts;
          lat_vec.push_back(delta);
          cnt++;
          q.pop();
        }
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
  result.tp_throughput_mops = tp_iters / result.tp_runtime_sec / 1e6;

  if (!lat_vec.empty()) {
    std::sort(lat_vec.begin(), lat_vec.end());
    result.tp_p50_cycles = lat_vec[lat_vec.size() / 2];
    result.tp_p99_cycles = lat_vec[(lat_vec.size() * 99) / 100];
  }
}

// ============================================================
// Latency Benchmark - rigtorp
// ============================================================
BenchResult benchmark_latency_rigtorp(const char* name, size_t queue_size) {
  BenchResult result;
  result.name = name;
  result.queue_size = queue_size;

  rigtorp::SPSCQueue<Msg> q(queue_size);
  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.resize(lat_iters);

  auto sendthread = [&]() {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    int64_t g_val = 0;
    while (g_val < lat_iters) {
      Msg msg;
      msg.val_len = 1;
      msg.ts = rdtscp();

      if (q.try_push(msg)) {
        g_val++;
        auto expire = rdtsc() + sleep_cycles;
        while (rdtsc() < expire);
      }
    }
  };

  auto recvthread = [&]() {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    int64_t cnt = 0;
    long sum_lat = 0;
    int64_t g_val = 0;

    while (g_val < lat_iters) {
      Msg* msg = q.front();
      if (!msg) continue;

      auto t2 = rdtscp();
      uint64_t delta = t2 - msg->ts;
      lat_vec[cnt] = delta;
      sum_lat += delta;
      cnt++;
      g_val++;

      q.pop();
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
  result.lat_throughput_mops = lat_iters / runtime / 1e6;

  std::sort(lat_vec.begin(), lat_vec.end());
  result.lat_p50_cycles = lat_vec[lat_vec.size() / 2];
  result.lat_p90_cycles = lat_vec[(lat_vec.size() * 90) / 100];
  result.lat_p99_cycles = lat_vec[(lat_vec.size() * 99) / 100];
  result.lat_p999_cycles = lat_vec[(lat_vec.size() * 999) / 1000];

  return result;
}

// ============================================================
// Throughput Benchmark - rigtorp
// ============================================================
void benchmark_throughput_rigtorp(BenchResult& result) {
  rigtorp::SPSCQueue<Msg> q(result.queue_size);
  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.reserve(tp_iters);

  auto sendthread = [&]() {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    int64_t g_val = 0;
    while (g_val < tp_iters) {
      Msg msg;
      msg.val_len = 1;
      msg.ts = rdtscp();

      if (q.try_push(msg)) {
        g_val++;
      }
    }
  };

  auto recvthread = [&]() {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    int64_t g_val = 0;

    while (g_val < tp_iters) {
      Msg* msg = q.front();
      if (!msg) continue;

      auto t2 = rdtscp();
      uint64_t delta = t2 - msg->ts;
      lat_vec.push_back(delta);
      g_val++;

      q.pop();
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
  result.tp_throughput_mops = tp_iters / result.tp_runtime_sec / 1e6;

  if (!lat_vec.empty()) {
    std::sort(lat_vec.begin(), lat_vec.end());
    result.tp_p50_cycles = lat_vec[lat_vec.size() / 2];
    result.tp_p99_cycles = lat_vec[(lat_vec.size() * 99) / 100];
  }
}

// ============================================================
// Latency Benchmark - moodycamel
// ============================================================
BenchResult benchmark_latency_moodycamel(const char* name, size_t queue_size) {
  BenchResult result;
  result.name = name;
  result.queue_size = queue_size;

  moodycamel::ReaderWriterQueue<Msg> q(queue_size);
  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.resize(lat_iters);

  auto sendthread = [&]() {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    int64_t g_val = 0;
    while (g_val < lat_iters) {
      Msg msg;
      msg.val_len = 1;
      msg.ts = rdtscp();

      if (q.try_enqueue(msg)) {
        g_val++;
        auto expire = rdtsc() + sleep_cycles;
        while (rdtsc() < expire);
      }
    }
  };

  auto recvthread = [&]() {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    int64_t cnt = 0;
    long sum_lat = 0;
    int64_t g_val = 0;
    Msg msg;

    while (g_val < lat_iters) {
      if (!q.try_dequeue(msg)) continue;

      auto t2 = rdtscp();
      uint64_t delta = t2 - msg.ts;
      lat_vec[cnt] = delta;
      sum_lat += delta;
      cnt++;
      g_val++;
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
  result.lat_throughput_mops = lat_iters / runtime / 1e6;

  std::sort(lat_vec.begin(), lat_vec.end());
  result.lat_p50_cycles = lat_vec[lat_vec.size() / 2];
  result.lat_p90_cycles = lat_vec[(lat_vec.size() * 90) / 100];
  result.lat_p99_cycles = lat_vec[(lat_vec.size() * 99) / 100];
  result.lat_p999_cycles = lat_vec[(lat_vec.size() * 999) / 1000];

  return result;
}

// ============================================================
// Throughput Benchmark - moodycamel
// ============================================================
void benchmark_throughput_moodycamel(BenchResult& result) {
  moodycamel::ReaderWriterQueue<Msg> q(result.queue_size);
  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.reserve(tp_iters);

  auto sendthread = [&]() {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    int64_t g_val = 0;
    while (g_val < tp_iters) {
      Msg msg;
      msg.val_len = 1;
      msg.ts = rdtscp();

      if (q.try_enqueue(msg)) {
        g_val++;
      }
    }
  };

  auto recvthread = [&]() {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    int64_t g_val = 0;
    Msg msg;

    while (g_val < tp_iters) {
      if (!q.try_dequeue(msg)) continue;

      auto t2 = rdtscp();
      uint64_t delta = t2 - msg.ts;
      lat_vec.push_back(delta);
      g_val++;
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
  result.tp_throughput_mops = tp_iters / result.tp_runtime_sec / 1e6;

  if (!lat_vec.empty()) {
    std::sort(lat_vec.begin(), lat_vec.end());
    result.tp_p50_cycles = lat_vec[lat_vec.size() / 2];
    result.tp_p99_cycles = lat_vec[(lat_vec.size() * 99) / 100];
  }
}

// ============================================================
// Wrapper functions
// ============================================================
template<size_t SIZE>
BenchResult benchmark_complete_spscqueueopt(const char* name, size_t queue_size) {
  std::cout << "    Latency test... ";
  std::cout.flush();
  auto result = benchmark_latency_spscqueueopt<SIZE>(name, queue_size);
  std::cout << "✓ ";

  std::cout << "Throughput test... ";
  std::cout.flush();
  benchmark_throughput_spscqueueopt<SIZE>(result);
  std::cout << "✓\n";

  return result;
}

template<size_t SIZE, size_t BATCH_SIZE>
BenchResult benchmark_complete_asymmetric(const char* name, size_t queue_size) {
  std::cout << "    Latency test... ";
  std::cout.flush();
  auto result = benchmark_latency_asymmetric<SIZE, BATCH_SIZE>(name, queue_size);
  std::cout << "✓ ";

  std::cout << "Throughput test... ";
  std::cout.flush();
  benchmark_throughput_asymmetric<SIZE, BATCH_SIZE>(result);
  std::cout << "✓\n";

  return result;
}

BenchResult benchmark_complete_rigtorp(const char* name, size_t queue_size) {
  std::cout << "    Latency test... ";
  std::cout.flush();
  auto result = benchmark_latency_rigtorp(name, queue_size);
  std::cout << "✓ ";

  std::cout << "Throughput test... ";
  std::cout.flush();
  benchmark_throughput_rigtorp(result);
  std::cout << "✓\n";

  return result;
}

BenchResult benchmark_complete_moodycamel(const char* name, size_t queue_size) {
  std::cout << "    Latency test... ";
  std::cout.flush();
  auto result = benchmark_latency_moodycamel(name, queue_size);
  std::cout << "✓ ";

  std::cout << "Throughput test... ";
  std::cout.flush();
  benchmark_throughput_moodycamel(result);
  std::cout << "✓\n";

  return result;
}

void print_result_summary(const BenchResult& r) {
  std::cout << "\n  " << r.name << ":\n";
  std::cout << "    Latency (1000 cyc throttling):\n";
  std::cout << "      p50: " << r.lat_p50_cycles << " cyc ("
            << std::fixed << std::setprecision(1) << (r.lat_p50_cycles * 1000.0 / 2300.0) << " ns)\n";
  std::cout << "      p99: " << r.lat_p99_cycles << " cyc ("
            << std::fixed << std::setprecision(1) << (r.lat_p99_cycles * 1000.0 / 2300.0) << " ns)\n";
  std::cout << "      Throughput: " << std::fixed << std::setprecision(2) << r.lat_throughput_mops << " M/s\n";

  std::cout << "    Throughput (no throttling):\n";
  std::cout << "      Throughput: " << std::fixed << std::setprecision(2) << r.tp_throughput_mops << " M/s\n";
  std::cout << "      p50: " << r.tp_p50_cycles << " cyc ("
            << std::fixed << std::setprecision(1) << (r.tp_p50_cycles * 1000.0 / 2300.0) << " ns)\n";
}

void print_final_table(const std::vector<BenchResult>& results) {
  std::cout << "\n" << std::string(120, '=') << "\n";
  std::cout << "FINAL RESULTS SUMMARY\n";
  std::cout << std::string(120, '=') << "\n\n";

  // Group by queue size
  std::map<size_t, std::vector<BenchResult>> by_size;
  for (const auto& r : results) {
    by_size[r.queue_size].push_back(r);
  }

  for (const auto& [size, size_results] : by_size) {
    std::cout << "\n" << std::string(120, '-') << "\n";
    if (size >= (1 << 20)) {
      std::cout << "Queue Size: " << (size >> 20) << "M (" << size << ")\n";
    } else if (size >= (1 << 10)) {
      std::cout << "Queue Size: " << (size >> 10) << "K (" << size << ")\n";
    } else {
      std::cout << "Queue Size: " << size << "\n";
    }
    std::cout << std::string(120, '-') << "\n\n";

    // Latency table
    std::cout << "LATENCY (with 1000 cycle throttling):\n\n";
    std::cout << std::setw(25) << "Implementation"
              << std::setw(15) << "Throughput"
              << std::setw(12) << "p50 (cyc)"
              << std::setw(12) << "p50 (ns)"
              << std::setw(12) << "p99 (cyc)"
              << std::setw(12) << "p99 (ns)"
              << std::setw(12) << "p99.9 (μs)\n";
    std::cout << std::string(100, '-') << "\n";

    for (const auto& r : size_results) {
      std::cout << std::setw(25) << r.name
                << std::setw(12) << std::fixed << std::setprecision(2) << r.lat_throughput_mops << " M"
                << std::setw(12) << r.lat_p50_cycles
                << std::setw(12) << std::fixed << std::setprecision(1) << (r.lat_p50_cycles * 1000.0 / 2300.0)
                << std::setw(12) << r.lat_p99_cycles
                << std::setw(12) << std::fixed << std::setprecision(1) << (r.lat_p99_cycles * 1000.0 / 2300.0)
                << std::setw(12) << std::fixed << std::setprecision(2) << (r.lat_p999_cycles / 2300.0)
                << "\n";
    }

    // Throughput table
    std::cout << "\n\nTHROUGHPUT (no throttling):\n\n";
    std::cout << std::setw(25) << "Implementation"
              << std::setw(15) << "Throughput"
              << std::setw(15) << "Runtime"
              << std::setw(12) << "p50 (cyc)"
              << std::setw(12) << "p99 (cyc)\n";
    std::cout << std::string(80, '-') << "\n";

    for (const auto& r : size_results) {
      std::cout << std::setw(25) << r.name
                << std::setw(12) << std::fixed << std::setprecision(2) << r.tp_throughput_mops << " M"
                << std::setw(13) << std::fixed << std::setprecision(3) << r.tp_runtime_sec << " s"
                << std::setw(12) << r.tp_p50_cycles
                << std::setw(12) << r.tp_p99_cycles
                << "\n";
    }
  }

  std::cout << "\n" << std::string(120, '=') << "\n";
}

int main() {
  std::cout << "=========================================\n";
  std::cout << "  Complete HEAP Benchmark\n";
  std::cout << "  Latency + Throughput Tests\n";
  std::cout << "=========================================\n\n";

  std::cout << "Configuration:\n";
  std::cout << "  Allocation: HEAP (std::make_unique)\n";
  std::cout << "  CPU: Intel Core i7-11800H @ 2.30GHz\n";
  std::cout << "  Latency test: " << lat_iters << " messages, 1000 cycle throttling\n";
  std::cout << "  Throughput test: " << tp_iters << " messages, no throttling\n";
  std::cout << "  Asymmetric batch sizes: 16, 32, 64\n\n";

  std::vector<size_t> queue_sizes = {2048, 8192, 1 << 20, 1 << 24};  // 2K, 8K, 1M, 16M
  std::vector<BenchResult> all_results;

  for (size_t size : queue_sizes) {
    std::cout << "\n" << std::string(80, '=') << "\n";
    if (size >= (1 << 20)) {
      std::cout << "Testing Queue Size: " << (size >> 20) << "M (" << size << ")\n";
    } else {
      std::cout << "Testing Queue Size: " << (size >> 10) << "K (" << size << ")\n";
    }
    std::cout << std::string(80, '=') << "\n\n";

    // SPSCQueueOPT
    try {
      std::cout << "  SPSCQueueOPT:\n";
      BenchResult r;
      if (size == 2048) r = benchmark_complete_spscqueueopt<2048>("SPSCQueueOPT", size);
      else if (size == 8192) r = benchmark_complete_spscqueueopt<8192>("SPSCQueueOPT", size);
      else if (size == (1 << 20)) r = benchmark_complete_spscqueueopt<1 << 20>("SPSCQueueOPT", size);
      else if (size == (1 << 24)) r = benchmark_complete_spscqueueopt<1 << 24>("SPSCQueueOPT", size);

      all_results.push_back(r);
      print_result_summary(r);
    } catch (const std::exception& e) {
      std::cout << "    Failed: " << e.what() << "\n";
    }

    // Asymmetric batch=16
    try {
      std::cout << "\n  Asymmetric (batch=16):\n";
      BenchResult r;
      if (size == 2048) r = benchmark_complete_asymmetric<2048, 16>("Asymmetric (b=16)", size);
      else if (size == 8192) r = benchmark_complete_asymmetric<8192, 16>("Asymmetric (b=16)", size);
      else if (size == (1 << 20)) r = benchmark_complete_asymmetric<1 << 20, 16>("Asymmetric (b=16)", size);
      else if (size == (1 << 24)) r = benchmark_complete_asymmetric<1 << 24, 16>("Asymmetric (b=16)", size);

      all_results.push_back(r);
      print_result_summary(r);
    } catch (const std::exception& e) {
      std::cout << "    Failed: " << e.what() << "\n";
    }

    // Asymmetric batch=32
    try {
      std::cout << "\n  Asymmetric (batch=32):\n";
      BenchResult r;
      if (size == 2048) r = benchmark_complete_asymmetric<2048, 32>("Asymmetric (b=32)", size);
      else if (size == 8192) r = benchmark_complete_asymmetric<8192, 32>("Asymmetric (b=32)", size);
      else if (size == (1 << 20)) r = benchmark_complete_asymmetric<1 << 20, 32>("Asymmetric (b=32)", size);
      else if (size == (1 << 24)) r = benchmark_complete_asymmetric<1 << 24, 32>("Asymmetric (b=32)", size);

      all_results.push_back(r);
      print_result_summary(r);
    } catch (const std::exception& e) {
      std::cout << "    Failed: " << e.what() << "\n";
    }

    // Asymmetric batch=64
    try {
      std::cout << "\n  Asymmetric (batch=64):\n";
      BenchResult r;
      if (size == 2048) r = benchmark_complete_asymmetric<2048, 64>("Asymmetric (b=64)", size);
      else if (size == 8192) r = benchmark_complete_asymmetric<8192, 64>("Asymmetric (b=64)", size);
      else if (size == (1 << 20)) r = benchmark_complete_asymmetric<1 << 20, 64>("Asymmetric (b=64)", size);
      else if (size == (1 << 24)) r = benchmark_complete_asymmetric<1 << 24, 64>("Asymmetric (b=64)", size);

      all_results.push_back(r);
      print_result_summary(r);
    } catch (const std::exception& e) {
      std::cout << "    Failed: " << e.what() << "\n";
    }

    // rigtorp
    try {
      std::cout << "\n  rigtorp:\n";
      auto r = benchmark_complete_rigtorp("rigtorp", size);
      all_results.push_back(r);
      print_result_summary(r);
    } catch (const std::exception& e) {
      std::cout << "    Failed: " << e.what() << "\n";
    }

    // moodycamel
    try {
      std::cout << "\n  moodycamel:\n";
      auto r = benchmark_complete_moodycamel("moodycamel", size);
      all_results.push_back(r);
      print_result_summary(r);
    } catch (const std::exception& e) {
      std::cout << "    Failed: " << e.what() << "\n";
    }
  }

  print_final_table(all_results);

  return 0;
}
