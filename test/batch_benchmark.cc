#include <bits/stdc++.h>
#include "rdtsc.h"
#include "cpupin.h"
#include "../SPSCQueue.h"
#include "../SPSCQueueBatch.h"

struct Msg {
  uint64_t ts;
  uint64_t seq;
  uint64_t data[6];  // Total 64 bytes
};

template<typename QueueT>
struct BenchResult {
  std::string name;
  size_t batch_size;
  double throughput_mops;
  double latency_avg_ns;
  uint64_t latency_p50_ns;
  uint64_t latency_p99_ns;
  size_t total_messages;
  double runtime_sec;
  size_t atomic_ops;  // Number of atomic operations
};

// ============================================================
// Benchmark: Single-item operations (baseline)
// ============================================================
template<typename QueueT>
BenchResult<QueueT> benchmark_single(size_t num_messages, int producer_cpu, int consumer_cpu)
{
  BenchResult<QueueT> result;
  result.name = "Single-item";
  result.batch_size = 1;
  result.total_messages = num_messages;

  QueueT queue;
  std::atomic<bool> ready{false};
  std::vector<uint64_t> latencies;
  latencies.reserve(num_messages);

  auto producer = [&]() {
    if (!cpupin(producer_cpu)) exit(1);
    while (!ready.load());

    for (size_t i = 0; i < num_messages; i++) {
      Msg* msg = queue.alloc();
      while (!msg) msg = queue.alloc();

      msg->seq = i;
      msg->ts = rdtscp();
      queue.push();
    }
  };

  auto consumer = [&]() {
    if (!cpupin(consumer_cpu)) exit(1);
    while (!ready.load());

    uint64_t sum_lat = 0;
    for (size_t i = 0; i < num_messages; i++) {
      Msg* msg = queue.front();
      while (!msg) msg = queue.front();

      uint64_t now = rdtscp();
      uint64_t lat = now - msg->ts;
      latencies.push_back(lat);
      sum_lat += lat;

      queue.pop();
    }

    result.latency_avg_ns = (sum_lat * 1000.0 / 2300.0) / num_messages;
  };

  std::thread prod(producer);
  std::thread cons(consumer);

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  prod.join();
  cons.join();
  auto end = std::chrono::high_resolution_clock::now();

  result.runtime_sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = num_messages / result.runtime_sec / 1e6;
  result.atomic_ops = num_messages * 2;  // push + pop per message

  std::sort(latencies.begin(), latencies.end());
  result.latency_p50_ns = latencies[latencies.size() / 2] * 1000 / 2300;
  result.latency_p99_ns = latencies[latencies.size() * 99 / 100] * 1000 / 2300;

  return result;
}

// ============================================================
// Benchmark: Batch operations
// ============================================================
BenchResult<SPSCQueueBatch<Msg, 4096>> benchmark_batch(
    size_t num_messages,
    size_t batch_size,
    int producer_cpu,
    int consumer_cpu)
{
  BenchResult<SPSCQueueBatch<Msg, 4096>> result;
  result.name = "Batch";
  result.batch_size = batch_size;
  result.total_messages = num_messages;

  SPSCQueueBatch<Msg, 4096> queue;
  std::atomic<bool> ready{false};
  std::vector<uint64_t> latencies;
  latencies.reserve(num_messages);

  size_t atomic_pushes = 0;
  size_t atomic_pops = 0;

  auto producer = [&]() {
    if (!cpupin(producer_cpu)) exit(1);
    while (!ready.load());

    size_t sent = 0;
    while (sent < num_messages) {
      size_t to_send = std::min(batch_size, num_messages - sent);

      size_t pushed = queue.tryPushBatch(to_send, [&](Msg** slots, size_t count) {
        uint64_t ts = rdtscp();
        for (size_t i = 0; i < count; i++) {
          slots[i]->seq = sent + i;
          slots[i]->ts = ts;  // Same timestamp for batch
        }
      });

      if (pushed > 0) {
        sent += pushed;
        atomic_pushes++;
      }
    }
  };

  auto consumer = [&]() {
    if (!cpupin(consumer_cpu)) exit(1);
    while (!ready.load());

    size_t received = 0;
    uint64_t sum_lat = 0;

    while (received < num_messages) {
      size_t to_recv = std::min(batch_size, num_messages - received);

      size_t popped = queue.tryPopBatch(to_recv, [&](Msg** slots, size_t count) {
        uint64_t now = rdtscp();
        for (size_t i = 0; i < count; i++) {
          uint64_t lat = now - slots[i]->ts;
          latencies.push_back(lat);
          sum_lat += lat;
        }
      });

      if (popped > 0) {
        received += popped;
        atomic_pops++;
      }
    }

    result.latency_avg_ns = (sum_lat * 1000.0 / 2300.0) / num_messages;
  };

  std::thread prod(producer);
  std::thread cons(consumer);

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  prod.join();
  cons.join();
  auto end = std::chrono::high_resolution_clock::now();

  result.runtime_sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = num_messages / result.runtime_sec / 1e6;
  result.atomic_ops = atomic_pushes + atomic_pops;

  std::sort(latencies.begin(), latencies.end());
  result.latency_p50_ns = latencies[latencies.size() / 2] * 1000 / 2300;
  result.latency_p99_ns = latencies[latencies.size() * 99 / 100] * 1000 / 2300;

  return result;
}

int main()
{
  std::cout << "=========================================\n";
  std::cout << "   Batch vs Single-item Benchmark\n";
  std::cout << "=========================================\n\n";

  const size_t NUM_MSGS = 1000000;
  std::vector<size_t> batch_sizes = {1, 2, 4, 8, 16, 32, 64};

  std::cout << "Configuration:\n";
  std::cout << "  Messages: " << NUM_MSGS << "\n";
  std::cout << "  Message size: " << sizeof(Msg) << " bytes\n";
  std::cout << "  Queue size: 4096 slots\n";
  std::cout << "  Producer CPU: 6\n";
  std::cout << "  Consumer CPU: 7\n\n";

  // Baseline: SPSCQueue single-item
  std::cout << "-----------------------------------------\n";
  std::cout << "Baseline: SPSCQueue (single-item)\n";
  std::cout << "-----------------------------------------\n";
  auto baseline = benchmark_single<SPSCQueue<Msg, 4096>>(NUM_MSGS, 6, 7);

  std::cout << "Throughput: " << std::fixed << std::setprecision(2)
            << baseline.throughput_mops << " Mops/sec\n";
  std::cout << "Latency avg: " << std::fixed << std::setprecision(0)
            << baseline.latency_avg_ns << " ns\n";
  std::cout << "Latency p50: " << baseline.latency_p50_ns << " ns\n";
  std::cout << "Latency p99: " << baseline.latency_p99_ns << " ns\n";
  std::cout << "Atomic ops: " << baseline.atomic_ops << "\n";
  std::cout << "Runtime: " << std::fixed << std::setprecision(3)
            << baseline.runtime_sec << " sec\n\n";

  // Batch tests
  std::cout << "-----------------------------------------\n";
  std::cout << "SPSCQueueBatch (various batch sizes)\n";
  std::cout << "-----------------------------------------\n";

  std::vector<BenchResult<SPSCQueueBatch<Msg, 4096>>> batch_results;

  for (size_t bs : batch_sizes) {
    auto result = benchmark_batch(NUM_MSGS, bs, 6, 7);
    batch_results.push_back(result);

    std::cout << "\nBatch size: " << bs << "\n";
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2)
              << result.throughput_mops << " Mops/sec ("
              << std::showpos << std::fixed << std::setprecision(1)
              << ((result.throughput_mops / baseline.throughput_mops - 1) * 100)
              << "%)\n" << std::noshowpos;
    std::cout << "  Latency avg: " << std::fixed << std::setprecision(0)
              << result.latency_avg_ns << " ns\n";
    std::cout << "  Latency p50: " << result.latency_p50_ns << " ns\n";
    std::cout << "  Latency p99: " << result.latency_p99_ns << " ns\n";
    std::cout << "  Atomic ops: " << result.atomic_ops
              << " (vs " << baseline.atomic_ops << " baseline)\n";
    std::cout << "  Atomic reduction: " << std::fixed << std::setprecision(1)
              << (1.0 - (double)result.atomic_ops / baseline.atomic_ops) * 100 << "%\n";
    std::cout << "  Runtime: " << std::fixed << std::setprecision(3)
              << result.runtime_sec << " sec\n";
  }

  // Summary table
  std::cout << "\n=========================================\n";
  std::cout << "                Summary\n";
  std::cout << "=========================================\n\n";

  std::cout << std::setw(12) << "Batch Size"
            << std::setw(15) << "Throughput"
            << std::setw(15) << "Speedup"
            << std::setw(15) << "Atomic Ops"
            << std::setw(15) << "Reduction\n";
  std::cout << std::string(71, '-') << "\n";

  // Baseline
  std::cout << std::setw(12) << "1 (base)"
            << std::setw(12) << std::fixed << std::setprecision(2)
            << baseline.throughput_mops << " M"
            << std::setw(15) << "1.00x"
            << std::setw(15) << baseline.atomic_ops
            << std::setw(15) << "0.0%\n";

  // Batch results
  for (const auto& r : batch_results) {
    double speedup = r.throughput_mops / baseline.throughput_mops;
    double reduction = (1.0 - (double)r.atomic_ops / baseline.atomic_ops) * 100;

    std::cout << std::setw(12) << r.batch_size
              << std::setw(12) << std::fixed << std::setprecision(2)
              << r.throughput_mops << " M"
              << std::setw(13) << std::fixed << std::setprecision(2) << speedup << "x"
              << std::setw(15) << r.atomic_ops
              << std::setw(13) << std::fixed << std::setprecision(1) << reduction << "%\n";
  }

  std::cout << "\n=========================================\n";
  std::cout << "Key Insights:\n";
  std::cout << "- Batch operations reduce atomic ops by up to "
            << std::fixed << std::setprecision(0)
            << (1.0 - (double)batch_results.back().atomic_ops / baseline.atomic_ops) * 100
            << "%\n";
  std::cout << "- Best throughput: " << std::fixed << std::setprecision(2)
            << batch_results.back().throughput_mops << " Mops/sec (batch="
            << batch_results.back().batch_size << ")\n";
  std::cout << "- Speedup: " << std::fixed << std::setprecision(2)
            << batch_results.back().throughput_mops / baseline.throughput_mops << "x\n";
  std::cout << "=========================================\n";

  return 0;
}
