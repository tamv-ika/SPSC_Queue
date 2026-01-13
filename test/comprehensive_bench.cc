#include <bits/stdc++.h>
#include <sched.h>
#include <sys/mman.h>
#include "rdtsc.h"
#include "cpupin.h"
#include "../SPSCQueue.h"
#include "../SPSCQueueOPT.h"

struct Msg {
  uint64_t ts;
  uint64_t seq;
};

template<typename QueueT>
struct BenchmarkResult {
  std::string queue_name;
  size_t queue_size;
  size_t num_messages;

  // Latency stats (cycles)
  uint64_t p50, p90, p99, p999, p9999, worst;
  double avg_latency;

  // Throughput
  double throughput_mops;
  double runtime_sec;

  // Outlier analysis
  size_t outlier_count;
  double outlier_pct;

  void print() {
    std::cout << "\n=== " << queue_name << " (size=" << queue_size << ") ===\n";
    std::cout << "Messages: " << num_messages << "\n";
    std::cout << "Runtime: " << std::fixed << std::setprecision(3) << runtime_sec << " sec\n";
    std::cout << "Throughput: " << std::fixed << std::setprecision(2) << throughput_mops << " Mops/sec\n";
    std::cout << "\nLatency (cycles | nanoseconds @ 2.3GHz):\n";

    auto print_lat = [](const char* label, uint64_t cycles) {
      double ns = cycles * 1000.0 / 2300.0;
      std::cout << "  " << std::setw(7) << label << ": "
                << std::setw(10) << cycles << " cyc | "
                << std::setw(10) << std::fixed << std::setprecision(2) << ns << " ns\n";
    };

    print_lat("avg", (uint64_t)avg_latency);
    print_lat("p50", p50);
    print_lat("p90", p90);
    print_lat("p99", p99);
    print_lat("p99.9", p999);
    print_lat("p99.99", p9999);
    print_lat("worst", worst);

    std::cout << "\nOutliers (>10000 cyc): " << outlier_count
              << " (" << std::fixed << std::setprecision(2) << outlier_pct << "%)\n";
  }
};

template<typename QueueT>
BenchmarkResult<QueueT> run_benchmark(
    const char* name,
    size_t num_messages,
    int sleep_cycles,
    int producer_cpu,
    int consumer_cpu,
    bool use_realtime = false)
{
  BenchmarkResult<QueueT> result;
  result.queue_name = name;
  result.queue_size = QueueT::CAPACITY;  // Need to add this to queue classes
  result.num_messages = num_messages;

  QueueT queue;
  std::atomic<bool> ready{false};
  std::vector<uint64_t> latencies;
  latencies.reserve(num_messages);

  // Producer thread
  auto producer = [&]() {
    if (!cpupin(producer_cpu)) exit(1);

    if (use_realtime) {
      struct sched_param param;
      param.sched_priority = 99;
      sched_setscheduler(0, SCHED_FIFO, &param);
    }

    while (!ready.load()) ;

    for (size_t i = 0; i < num_messages; i++) {
      Msg* msg = queue.alloc();
      while (!msg) {
        msg = queue.alloc();
      }
      msg->seq = i;
      msg->ts = rdtscp();
      queue.push();

      if (sleep_cycles > 0) {
        auto expire = rdtsc() + sleep_cycles;
        while (rdtsc() < expire) ;
      }
    }
  };

  // Consumer thread
  auto consumer = [&]() {
    if (!cpupin(consumer_cpu)) exit(1);

    if (use_realtime) {
      struct sched_param param;
      param.sched_priority = 98;  // Slightly lower than producer
      sched_setscheduler(0, SCHED_FIFO, &param);
    }

    while (!ready.load()) ;

    uint64_t sum_lat = 0;
    size_t outliers = 0;

    for (size_t i = 0; i < num_messages; i++) {
      Msg* msg = queue.front();
      while (!msg) {
        msg = queue.front();
      }

      uint64_t now = rdtscp();
      uint64_t lat = now - msg->ts;

      latencies.push_back(lat);
      sum_lat += lat;

      if (lat > 10000) outliers++;

      queue.pop();
    }

    result.avg_latency = (double)sum_lat / num_messages;
    result.outlier_count = outliers;
    result.outlier_pct = 100.0 * outliers / num_messages;
  };

  // Run benchmark
  std::thread prod_thread(producer);
  std::thread cons_thread(consumer);

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;

  prod_thread.join();
  cons_thread.join();

  auto end = std::chrono::high_resolution_clock::now();
  result.runtime_sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = num_messages / result.runtime_sec / 1e6;

  // Calculate percentiles
  std::sort(latencies.begin(), latencies.end());
  result.p50 = latencies[latencies.size() / 2];
  result.p90 = latencies[latencies.size() * 90 / 100];
  result.p99 = latencies[latencies.size() * 99 / 100];
  result.p999 = latencies[latencies.size() * 999 / 1000];
  result.p9999 = latencies[latencies.size() * 9999 / 10000];
  result.worst = latencies.back();

  return result;
}

// Need to expose capacity
template<class T, uint32_t CNT>
struct SPSCQueueWrapper : public SPSCQueue<T, CNT> {
  static constexpr uint32_t CAPACITY = CNT;
};

template<class T, uint32_t CNT>
struct SPSCQueueOPTWrapper : public SPSCQueueOPT<T, CNT> {
  static constexpr uint32_t CAPACITY = CNT;
};

int main(int argc, char* argv[]) {
  std::cout << "=================================================\n";
  std::cout << "     SPSC Queue Comprehensive Benchmark\n";
  std::cout << "=================================================\n";

  const size_t NUM_MSGS = 1000000;
  const int THROTTLE = 1000;  // cycles

  std::cout << "\nTest Configuration:\n";
  std::cout << "  Messages: " << NUM_MSGS << "\n";
  std::cout << "  Throttling: " << THROTTLE << " cycles\n";
  std::cout << "  Producer CPU: 6\n";
  std::cout << "  Consumer CPU: 7\n";

  // Get CPU info
  std::ifstream cpuinfo("/proc/cpuinfo");
  std::string line;
  while (std::getline(cpuinfo, line)) {
    if (line.find("model name") != std::string::npos) {
      std::cout << "  " << line << "\n";
      break;
    }
  }

  std::cout << "\n-------------------------------------------------\n";
  std::cout << "Test 1: SPSCQueue vs SPSCQueueOPT (1K slots)\n";
  std::cout << "-------------------------------------------------\n";

  auto r1 = run_benchmark<SPSCQueueWrapper<Msg, 1024>>(
      "SPSCQueue<1K>", NUM_MSGS, THROTTLE, 6, 7);
  r1.print();

  auto r2 = run_benchmark<SPSCQueueOPTWrapper<Msg, 1024>>(
      "SPSCQueueOPT<1K>", NUM_MSGS, THROTTLE, 6, 7);
  r2.print();

  std::cout << "\n-------------------------------------------------\n";
  std::cout << "Test 2: Queue Size Impact (SPSCQueue)\n";
  std::cout << "-------------------------------------------------\n";

  auto r3 = run_benchmark<SPSCQueueWrapper<Msg, 256>>(
      "SPSCQueue<256>", NUM_MSGS, THROTTLE, 6, 7);
  r3.print();

  auto r4 = run_benchmark<SPSCQueueWrapper<Msg, 4096>>(
      "SPSCQueue<4K>", NUM_MSGS, THROTTLE, 6, 7);
  r4.print();

  auto r5 = run_benchmark<SPSCQueueWrapper<Msg, 16384>>(
      "SPSCQueue<16K>", NUM_MSGS, THROTTLE, 6, 7);
  r5.print();

  std::cout << "\n-------------------------------------------------\n";
  std::cout << "Test 3: No Throttling (Max Throughput)\n";
  std::cout << "-------------------------------------------------\n";

  auto r6 = run_benchmark<SPSCQueueWrapper<Msg, 1024>>(
      "SPSCQueue<1K>", NUM_MSGS, 0, 6, 7);
  r6.print();

  auto r7 = run_benchmark<SPSCQueueOPTWrapper<Msg, 1024>>(
      "SPSCQueueOPT<1K>", NUM_MSGS, 0, 6, 7);
  r7.print();

  std::cout << "\n-------------------------------------------------\n";
  std::cout << "Summary Comparison\n";
  std::cout << "-------------------------------------------------\n";
  std::cout << std::setw(20) << "Queue"
            << std::setw(15) << "Throughput"
            << std::setw(12) << "p50 (ns)"
            << std::setw(12) << "p99 (ns)"
            << std::setw(15) << "p99.99 (ns)"
            << std::setw(12) << "Outliers\n";
  std::cout << std::string(86, '-') << "\n";

  auto print_summary = [](const auto& r) {
    std::cout << std::setw(20) << r.queue_name
              << std::setw(12) << std::fixed << std::setprecision(2) << r.throughput_mops << " M"
              << std::setw(12) << std::fixed << std::setprecision(0) << (r.p50 * 1000.0 / 2.3)
              << std::setw(12) << (r.p99 * 1000.0 / 2.3)
              << std::setw(15) << (r.p9999 * 1000.0 / 2.3)
              << std::setw(11) << std::fixed << std::setprecision(2) << r.outlier_pct << "%\n";
  };

  print_summary(r1);
  print_summary(r2);
  print_summary(r3);
  print_summary(r4);
  print_summary(r5);
  print_summary(r6);
  print_summary(r7);

  std::cout << "\n=================================================\n";

  return 0;
}
