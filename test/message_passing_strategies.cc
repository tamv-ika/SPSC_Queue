/**
 * Message Passing Strategy Comparison
 *
 * Compares different approaches for inter-thread message passing:
 * 1. Direct value (copy entire message)
 * 2. Pointer (pass pointer to heap-allocated message)
 * 3. Shared pointer (pass shared_ptr)
 * 4. Index-based (pass index to pre-allocated buffer)
 */

#include <bits/stdc++.h>
#include "rdtsc.h"
#include "cpupin.h"

#include "../SPSCQueueOPT.h"
#include "../SPSCQueueAsymmetric.h"

const int64_t iters = 10000000;

// Simulate different message sizes
struct SmallMsg
{
  uint64_t id;
  uint64_t timestamp;
  uint32_t type;
  uint32_t padding;
}; // 24 bytes

struct MediumMsg
{
  uint64_t id;
  uint64_t timestamp;
  uint32_t type;
  uint32_t padding;
  double values[8]; // 64 bytes
}; // 88 bytes

struct LargeMsg
{
  uint64_t id;
  uint64_t timestamp;
  uint32_t type;
  uint32_t padding;
  double values[64]; // 512 bytes
}; // 528 bytes

struct BenchResult
{
  std::string strategy;
  std::string msg_size;
  double throughput_mops;
  double latency_p50_ns;
  double latency_p99_ns;
  double runtime_sec;
};

// ============================================================
// Strategy 1: Direct Value (Copy)
// ============================================================
template <typename MsgT, size_t QUEUE_SIZE = 2048>
BenchResult benchmark_direct_value(const std::string &size_name)
{
  BenchResult result;
  result.strategy = "Direct Value";
  result.msg_size = size_name;

  auto q_ptr = std::make_unique<SPSCQueueOPT<MsgT, QUEUE_SIZE>>();
  auto &q = *q_ptr;

  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.reserve(iters);

  auto producer = std::thread([&]()
                              {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    for (int64_t i = 0; i < iters; ++i) {
      MsgT* slot;
      while (!(slot = q.alloc()));

      // Initialize message
      slot->id = i;
      slot->timestamp = rdtscp();
      slot->type = 1;

      q.push();
    } });

  auto consumer = std::thread([&]()
                              {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    for (int64_t i = 0; i < iters; ++i) {
      MsgT* msg;
      while (!(msg = q.front()));

      auto now = rdtscp();
      lat_vec.push_back(now - msg->timestamp);

      q.pop();
    } });

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  producer.join();
  consumer.join();
  auto end = std::chrono::high_resolution_clock::now();

  result.runtime_sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  std::sort(lat_vec.begin(), lat_vec.end());
  result.latency_p50_ns = lat_vec[lat_vec.size() / 2] * 1000.0 / 2300.0;
  result.latency_p99_ns = lat_vec[(lat_vec.size() * 99) / 100] * 1000.0 / 2300.0;

  return result;
}

// ============================================================
// Strategy 2: Raw Pointer
// ============================================================
template <typename MsgT, size_t QUEUE_SIZE = 2048>
BenchResult benchmark_raw_pointer(const std::string &size_name)
{
  BenchResult result;
  result.strategy = "Raw Pointer";
  result.msg_size = size_name;

  auto q_ptr = std::make_unique<SPSCQueueOPT<MsgT *, QUEUE_SIZE>>();
  auto &q = *q_ptr;

  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.reserve(iters);

  auto producer = std::thread([&]()
                              {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    for (int64_t i = 0; i < iters; ++i) {
      MsgT** slot;
      while (!(slot = q.alloc()));

      // Allocate message on heap
      MsgT* msg = new MsgT();
      msg->id = i;
      msg->timestamp = rdtscp();
      msg->type = 1;

      *slot = msg;
      q.push();
    } });

  auto consumer = std::thread([&]()
                              {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    for (int64_t i = 0; i < iters; ++i) {
      MsgT** msg_ptr;
      while (!(msg_ptr = q.front()));

      auto now = rdtscp();
      MsgT* msg = *msg_ptr;
      lat_vec.push_back(now - msg->timestamp);

      delete msg;  // Free memory
      q.pop();
    } });

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  producer.join();
  consumer.join();
  auto end = std::chrono::high_resolution_clock::now();

  result.runtime_sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  std::sort(lat_vec.begin(), lat_vec.end());
  result.latency_p50_ns = lat_vec[lat_vec.size() / 2] * 1000.0 / 2300.0;
  result.latency_p99_ns = lat_vec[(lat_vec.size() * 99) / 100] * 1000.0 / 2300.0;

  return result;
}

// ============================================================
// Strategy 3: Shared Pointer
// ============================================================
template <typename MsgT, size_t QUEUE_SIZE = 2048>
BenchResult benchmark_shared_ptr(const std::string &size_name)
{
  BenchResult result;
  result.strategy = "Shared Ptr";
  result.msg_size = size_name;

  auto q_ptr = std::make_unique<SPSCQueueOPT<std::shared_ptr<MsgT>, QUEUE_SIZE>>();
  auto &q = *q_ptr;

  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.reserve(iters);

  auto producer = std::thread([&]()
                              {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    for (int64_t i = 0; i < iters; ++i) {
      std::shared_ptr<MsgT>* slot;
      while (!(slot = q.alloc()));

      // Create shared_ptr
      auto msg = std::make_shared<MsgT>();
      msg->id = i;
      msg->timestamp = rdtscp();
      msg->type = 1;

      *slot = msg;
      q.push();
    } });

  auto consumer = std::thread([&]()
                              {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    for (int64_t i = 0; i < iters; ++i) {
      std::shared_ptr<MsgT>* msg_ptr;
      while (!(msg_ptr = q.front()));

      auto now = rdtscp();
      auto msg = *msg_ptr;
      lat_vec.push_back(now - msg->timestamp);

      q.pop();
      // shared_ptr automatically manages lifetime
    } });

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  producer.join();
  consumer.join();
  auto end = std::chrono::high_resolution_clock::now();

  result.runtime_sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  std::sort(lat_vec.begin(), lat_vec.end());
  result.latency_p50_ns = lat_vec[lat_vec.size() / 2] * 1000.0 / 2300.0;
  result.latency_p99_ns = lat_vec[(lat_vec.size() * 99) / 100] * 1000.0 / 2300.0;

  return result;
}

// ============================================================
// Strategy 4: Object Pool (Pre-allocated pointers, reuse)
// ============================================================
template<typename MsgT, size_t QUEUE_SIZE = 2048, size_t POOL_SIZE = 8192>
BenchResult benchmark_object_pool(const std::string& size_name) {
  BenchResult result;
  result.strategy = "Object Pool";
  result.msg_size = size_name;

  // Pre-allocate object pool
  std::vector<MsgT*> pool(POOL_SIZE);
  for (size_t i = 0; i < POOL_SIZE; ++i) {
    pool[i] = new MsgT();
  }

  auto q_ptr = std::make_unique<SPSCQueueOPT<MsgT*, QUEUE_SIZE>>();
  auto& q = *q_ptr;

  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.reserve(iters);

  auto producer = std::thread([&]() {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    for (int64_t i = 0; i < iters; ++i) {
      MsgT** slot;
      while (!(slot = q.alloc()));

      // Reuse pre-allocated object
      uint32_t idx = i % POOL_SIZE;
      MsgT* msg = pool[idx];
      msg->id = i;
      msg->timestamp = rdtscp();
      msg->type = 1;

      *slot = msg;
      q.push();
    }
  });

  auto consumer = std::thread([&]() {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    for (int64_t i = 0; i < iters; ++i) {
      MsgT** msg_ptr;
      while (!(msg_ptr = q.front()));

      auto now = rdtscp();
      MsgT* msg = *msg_ptr;
      lat_vec.push_back(now - msg->timestamp);

      // Don't delete - reuse!
      q.pop();
    }
  });

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  producer.join();
  consumer.join();
  auto end = std::chrono::high_resolution_clock::now();

  result.runtime_sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  std::sort(lat_vec.begin(), lat_vec.end());
  result.latency_p50_ns = lat_vec[lat_vec.size() / 2] * 1000.0 / 2300.0;
  result.latency_p99_ns = lat_vec[(lat_vec.size() * 99) / 100] * 1000.0 / 2300.0;

  // Cleanup pool
  for (auto* ptr : pool) {
    delete ptr;
  }

  return result;
}

// ============================================================
// Strategy 5: Index-Based (Pre-allocated Pool)
// ============================================================
template <typename MsgT, size_t QUEUE_SIZE = 2048, size_t POOL_SIZE = 8192>
BenchResult benchmark_index_based(const std::string &size_name)
{
  BenchResult result;
  result.strategy = "Index-Based";
  result.msg_size = size_name;

  // Pre-allocate message pool
  auto pool = std::make_unique<std::array<MsgT, POOL_SIZE>>();

  auto q_ptr = std::make_unique<SPSCQueueOPT<uint32_t, QUEUE_SIZE>>();
  auto &q = *q_ptr;

  std::atomic<bool> ready{false};
  std::vector<uint64_t> lat_vec;
  lat_vec.reserve(iters);

  auto producer = std::thread([&]()
                              {
    if (!cpupin(6)) exit(1);
    while (!ready.load());

    for (int64_t i = 0; i < iters; ++i) {
      uint32_t* slot;
      while (!(slot = q.alloc()));

      // Use round-robin index into pool
      uint32_t idx = i % POOL_SIZE;

      // Write to pool
      (*pool)[idx].id = i;
      (*pool)[idx].timestamp = rdtscp();
      (*pool)[idx].type = 1;

      *slot = idx;
      q.push();
    } });

  auto consumer = std::thread([&]()
                              {
    if (!cpupin(7)) exit(1);
    while (!ready.load());

    for (int64_t i = 0; i < iters; ++i) {
      uint32_t* idx_ptr;
      while (!(idx_ptr = q.front()));

      uint32_t idx = *idx_ptr;
      auto now = rdtscp();

      // Read from pool
      MsgT& msg = (*pool)[idx];
      lat_vec.push_back(now - msg.timestamp);

      q.pop();
    } });

  auto start = std::chrono::high_resolution_clock::now();
  ready = true;
  producer.join();
  consumer.join();
  auto end = std::chrono::high_resolution_clock::now();

  result.runtime_sec = std::chrono::duration<double>(end - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  std::sort(lat_vec.begin(), lat_vec.end());
  result.latency_p50_ns = lat_vec[lat_vec.size() / 2] * 1000.0 / 2300.0;
  result.latency_p99_ns = lat_vec[(lat_vec.size() * 99) / 100] * 1000.0 / 2300.0;

  return result;
}

void print_results(const std::vector<BenchResult> &results)
{
  std::cout << "\n"
            << std::string(100, '=') << "\n";
  std::cout << "Message Passing Strategy Comparison\n";
  std::cout << std::string(100, '=') << "\n\n";

  // Group by message size
  std::map<std::string, std::vector<BenchResult>> by_size;
  for (const auto &r : results)
  {
    by_size[r.msg_size].push_back(r);
  }

  for (const auto &[size, size_results] : by_size)
  {
    std::cout << "\n"
              << std::string(100, '-') << "\n";
    std::cout << "Message Size: " << size << "\n";
    std::cout << std::string(100, '-') << "\n\n";

    std::cout << std::setw(20) << "Strategy"
              << std::setw(15) << "Throughput"
              << std::setw(15) << "Runtime"
              << std::setw(15) << "p50 Latency"
              << std::setw(15) << "p99 Latency\n";
    std::cout << std::string(80, '-') << "\n";

    // Find best
    double max_tp = 0;
    double min_lat = 1e9;
    for (const auto &r : size_results)
    {
      if (r.throughput_mops > max_tp)
        max_tp = r.throughput_mops;
      if (r.latency_p50_ns < min_lat)
        min_lat = r.latency_p50_ns;
    }

    for (const auto &r : size_results)
    {
      std::cout << std::setw(20) << r.strategy
                << std::setw(12) << std::fixed << std::setprecision(2) << r.throughput_mops << " M"
                << std::setw(13) << std::fixed << std::setprecision(3) << r.runtime_sec << " s"
                << std::setw(13) << std::fixed << std::setprecision(1) << r.latency_p50_ns << " ns"
                << std::setw(13) << std::fixed << std::setprecision(1) << r.latency_p99_ns << " ns";

      if (std::abs(r.throughput_mops - max_tp) < 0.01)
      {
        std::cout << " ✅ BEST TP";
      }
      if (std::abs(r.latency_p50_ns - min_lat) < 1.0)
      {
        std::cout << " ⭐ BEST LAT";
      }
      std::cout << "\n";
    }
  }

  std::cout << "\n"
            << std::string(100, '=') << "\n";
}

int main()
{
  constexpr auto QUEUE_SIZE = 1 << 14;
  std::cout << "=========================================\n";
  std::cout << "  Message Passing Strategy Benchmark\n";
  std::cout << "=========================================\n\n";

  std::cout << "Configuration:\n";
  std::cout << "  Messages: " << iters << "\n";
  std::cout << "  Queue size: 16k\n";
  std::cout << "  CPU: Intel Core i7-11800H @ 2.30GHz\n\n";

  std::cout << "Strategies:\n";
  std::cout << "  1. Direct Value - Copy entire message in queue\n";
  std::cout << "  2. Raw Pointer  - Pass pointer, manual new/delete (worst case)\n";
  std::cout << "  3. Shared Ptr   - Pass shared_ptr, automatic memory management\n";
  std::cout << "  4. Object Pool  - Pre-allocated pointers, reuse objects\n";
  std::cout << "  5. Index-Based  - Pass index to pre-allocated pool\n\n";

  std::vector<BenchResult> all_results;

  // Small messages (24 bytes)
  std::cout << "\n"
            << std::string(60, '=') << "\n";
  std::cout << "Testing Small Messages (24 bytes)...\n";
  std::cout << std::string(60, '=') << "\n";

  std::cout << "  Direct Value... ";
  std::cout.flush();
  all_results.push_back(benchmark_direct_value<SmallMsg, QUEUE_SIZE>("24 bytes"));
  std::cout << "✓\n";

  std::cout << "  Raw Pointer... ";
  std::cout.flush();
  all_results.push_back(benchmark_raw_pointer<SmallMsg, QUEUE_SIZE>("24 bytes"));
  std::cout << "✓\n";

  std::cout << "  Shared Ptr... ";
  std::cout.flush();
  all_results.push_back(benchmark_shared_ptr<SmallMsg, QUEUE_SIZE>("24 bytes"));
  std::cout << "✓\n";

  std::cout << "  Object Pool... ";
  std::cout.flush();
  all_results.push_back(benchmark_object_pool<SmallMsg, QUEUE_SIZE>("24 bytes"));
  std::cout << "✓\n";

  std::cout << "  Index-Based... ";
  std::cout.flush();
  all_results.push_back(benchmark_index_based<SmallMsg, QUEUE_SIZE>("24 bytes"));
  std::cout << "✓\n";

  // Medium messages (88 bytes)
  std::cout << "\n"
            << std::string(60, '=') << "\n";
  std::cout << "Testing Medium Messages (88 bytes)...\n";
  std::cout << std::string(60, '=') << "\n";

  std::cout << "  Direct Value... ";
  std::cout.flush();
  all_results.push_back(benchmark_direct_value<MediumMsg, QUEUE_SIZE>("88 bytes"));
  std::cout << "✓\n";

  std::cout << "  Raw Pointer... ";
  std::cout.flush();
  all_results.push_back(benchmark_raw_pointer<MediumMsg, QUEUE_SIZE>("88 bytes"));
  std::cout << "✓\n";

  std::cout << "  Shared Ptr... ";
  std::cout.flush();
  all_results.push_back(benchmark_shared_ptr<MediumMsg, QUEUE_SIZE>("88 bytes"));
  std::cout << "✓\n";

  std::cout << "  Object Pool... ";
  std::cout.flush();
  all_results.push_back(benchmark_object_pool<MediumMsg, QUEUE_SIZE>("88 bytes"));
  std::cout << "✓\n";

  std::cout << "  Index-Based... ";
  std::cout.flush();
  all_results.push_back(benchmark_index_based<MediumMsg, QUEUE_SIZE>("88 bytes"));
  std::cout << "✓\n";

  // Large messages (528 bytes)
  std::cout << "\n"
            << std::string(60, '=') << "\n";
  std::cout << "Testing Large Messages (528 bytes)...\n";
  std::cout << std::string(60, '=') << "\n";

  std::cout << "  Direct Value... ";
  std::cout.flush();
  all_results.push_back(benchmark_direct_value<LargeMsg, QUEUE_SIZE>("528 bytes"));
  std::cout << "✓\n";

  std::cout << "  Raw Pointer... ";
  std::cout.flush();
  all_results.push_back(benchmark_raw_pointer<LargeMsg, QUEUE_SIZE>("528 bytes"));
  std::cout << "✓\n";

  std::cout << "  Shared Ptr... ";
  std::cout.flush();
  all_results.push_back(benchmark_shared_ptr<LargeMsg, QUEUE_SIZE>("528 bytes"));
  std::cout << "✓\n";

  std::cout << "  Object Pool... ";
  std::cout.flush();
  all_results.push_back(benchmark_object_pool<LargeMsg, QUEUE_SIZE>("528 bytes"));
  std::cout << "✓\n";

  std::cout << "  Index-Based... ";
  std::cout.flush();
  all_results.push_back(benchmark_index_based<LargeMsg, QUEUE_SIZE>("528 bytes"));
  std::cout << "✓\n";

  print_results(all_results);

  std::cout << "\n" << std::string(100, '=') << "\n";
  std::cout << "RECOMMENDATIONS\n";
  std::cout << std::string(100, '=') << "\n\n";

  std::cout << "✅ Small messages (<64 bytes):\n";
  std::cout << "   1st: Direct Value - Copy is cheap, best latency\n";
  std::cout << "   2nd: Index-Based - Zero-copy alternative\n";
  std::cout << "   3rd: Object Pool - If pointer indirection acceptable\n\n";

  std::cout << "✅ Medium messages (64-256 bytes):\n";
  std::cout << "   1st: Index-Based - Best balance\n";
  std::cout << "   2nd: Object Pool - Good if pointer semantics needed\n";
  std::cout << "   3rd: Direct Value - Still viable if copy acceptable\n\n";

  std::cout << "✅ Large messages (>256 bytes):\n";
  std::cout << "   1st: Index-Based - Avoids all copying\n";
  std::cout << "   2nd: Object Pool - Pre-allocated, zero alloc overhead\n";
  std::cout << "   3rd: Direct Value - Only if infrequent messages\n\n";

  std::cout << "⚠️  Raw Pointer (new/delete):\n";
  std::cout << "   → SLOW! Allocation overhead dominates\n";
  std::cout << "   → Use Object Pool instead (pre-allocate)\n\n";

  std::cout << "❌ Avoid Shared Ptr:\n";
  std::cout << "   → Atomic reference counting overhead\n";
  std::cout << "   → Slowest of all strategies\n";
  std::cout << "   → Use Object Pool or Index-Based instead\n\n";

  std::cout << "💡 Key Insights:\n";
  std::cout << "   • Object Pool ≈ Index-Based performance\n";
  std::cout << "   • Object Pool >> Raw Pointer (no new/delete)\n";
  std::cout << "   • Direct Value best for small messages\n";
  std::cout << "   • Avoid allocation in hot path!\n\n";

  return 0;
}
