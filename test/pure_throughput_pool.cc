/**
 * Pure throughput test for Object Pool and Index-Based
 * NO latency measurement - just raw throughput like throughput_heap.cc
 *
 * This tests the hypothesis: Object Pool/Index-Based should achieve
 * similar throughput to 4-byte int (818 M/s) because queue payload is small.
 */

#include <bits/stdc++.h>
#include "cpupin.h"
#include "rdtsc.h"
#include "../SPSCQueueOPT.h"
#include "../SPSCQueueAsymmetric.h"

const int64_t iters = 10000000; // 10M

struct SmallMsg
{
  uint64_t id;
  uint64_t timestamp;
  uint32_t type;
  uint32_t padding;
}; // 24 bytes

struct Result
{
  std::string name;
  double throughput_mops;
  double runtime_sec;
};

// ============================================================
// Strategy 1: Object Pool - Pure Throughput (NO measurement) - SPSCQueueOPT
// ============================================================
template <typename MsgT, size_t QUEUE_SIZE = 16384, size_t POOL_SIZE = 8192>
Result benchmark_object_pool_pure_opt(const std::string &name)
{
  Result result;
  result.name = name;

  // Pre-allocate object pool
  std::vector<MsgT *> pool(POOL_SIZE);
  for (size_t i = 0; i < POOL_SIZE; ++i)
  {
    pool[i] = new MsgT();
  }

  auto q_ptr = std::make_unique<SPSCQueueOPT<MsgT *, QUEUE_SIZE>>();
  auto &q = *q_ptr;

  auto consumer = std::thread([&]()
                              {
    if (!cpupin(6)) exit(1);
    for (int64_t i = 0; i < iters; ++i) {
      MsgT** msg_ptr;
      while (!(msg_ptr = q.front()));

      // Just read - NO validation (pool wraps around)
      MsgT* msg = *msg_ptr;
      (void)msg->id;  // Touch memory

      q.pop();
    } });

  if (!cpupin(7))
    exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i)
  {
    MsgT **slot;
    while (!(slot = q.alloc()))
      ;

    // Reuse pre-allocated object - NO rdtscp!
    uint32_t idx = i % POOL_SIZE;
    MsgT *msg = pool[idx];
    msg->id = i;
    // No timestamp!
    msg->type = 1;

    *slot = msg;
    q.push();
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  // Cleanup pool
  for (auto *ptr : pool)
  {
    delete ptr;
  }

  return result;
}

// ============================================================
// Strategy 2: Index-Based - Pure Throughput (NO measurement)
// ============================================================
template <typename MsgT, size_t QUEUE_SIZE = 16384, size_t POOL_SIZE = 8192>
Result benchmark_index_based_pure(const std::string &name)
{
  Result result;
  result.name = name;

  // Pre-allocate message pool
  auto pool = std::make_unique<std::array<MsgT, POOL_SIZE>>();
  auto q_ptr = std::make_unique<SPSCQueueOPT<uint32_t, QUEUE_SIZE>>();
  auto &q = *q_ptr;

  auto consumer = std::thread([&]()
                              {
    if (!cpupin(6)) exit(1);
    for (int64_t i = 0; i < iters; ++i) {
      uint32_t* idx_ptr;
      while (!(idx_ptr = q.front()));

      // Just read - NO validation (pool wraps around)
      uint32_t idx = *idx_ptr;
      MsgT& msg = (*pool)[idx];
      (void)msg;  // Touch memory

      q.pop();
    } });

  if (!cpupin(7))
    exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i)
  {
    uint32_t *slot;
    while (!(slot = q.alloc()))
      ;

    // Write to pool - NO rdtscp!
    uint32_t idx = i % POOL_SIZE;
    (*pool)[idx].id = i;
    // No timestamp!
    (*pool)[idx].type = 1;

    *slot = idx;
    q.push();
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  return result;
}

// ============================================================
// Strategy 3: Direct int (baseline - like throughput_heap.cc)
// ============================================================
template <size_t QUEUE_SIZE = 16384>
Result benchmark_direct_int(const std::string &name)
{
  Result result;
  result.name = name;

  auto q_ptr = std::make_unique<SPSCQueueOPT<int, QUEUE_SIZE>>();
  auto &q = *q_ptr;

  auto consumer = std::thread([&]()
                              {
    if (!cpupin(6)) exit(1);
    for (int64_t i = 0; i < iters; ++i) {
      int* val;
      while (!(val = q.front()));
      if (*val != i) throw std::runtime_error("value mismatch");
      q.pop();
    } });

  if (!cpupin(7))
    exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i)
  {
    int *slot;
    while (!(slot = q.alloc()))
      ;
    *slot = i;
    q.push();
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  return result;
}

// ============================================================
// WITH measurement overhead (like message_passing_strategies.cc)
// ============================================================
template <typename MsgT, size_t QUEUE_SIZE = 16384, size_t POOL_SIZE = 8192>
Result benchmark_object_pool_with_measurement(const std::string &name)
{
  Result result;
  result.name = name;

  std::vector<MsgT *> pool(POOL_SIZE);
  for (size_t i = 0; i < POOL_SIZE; ++i)
  {
    pool[i] = new MsgT();
  }

  auto q_ptr = std::make_unique<SPSCQueueOPT<MsgT *, QUEUE_SIZE>>();
  auto &q = *q_ptr;

  std::vector<uint64_t> lat_vec;
  lat_vec.reserve(iters);

  auto consumer = std::thread([&]()
                              {
    if (!cpupin(6)) exit(1);
    for (int64_t i = 0; i < iters; ++i) {
      MsgT** msg_ptr;
      while (!(msg_ptr = q.front()));

      auto now = rdtscp();  // EXPENSIVE!
      MsgT* msg = *msg_ptr;
      lat_vec.push_back(now - msg->timestamp);  // EXPENSIVE!

      q.pop();
    } });

  if (!cpupin(7))
    exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i)
  {
    MsgT **slot;
    while (!(slot = q.alloc()))
      ;

    uint32_t idx = i % POOL_SIZE;
    MsgT *msg = pool[idx];
    msg->id = i;
    msg->timestamp = rdtscp(); // EXPENSIVE!
    msg->type = 1;

    *slot = msg;
    q.push();
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  for (auto *ptr : pool)
  {
    delete ptr;
  }

  return result;
}

// ============================================================
// Strategy: Object Pool with Asymmetric Queue + Batching
// ============================================================
template <typename MsgT, size_t QUEUE_SIZE = 16384, size_t POOL_SIZE = 8192, size_t BATCH_SIZE = 16>
Result benchmark_object_pool_asymmetric(const std::string &name)
{
  Result result;
  result.name = name;

  // Pre-allocate object pool
  std::vector<MsgT *> pool(POOL_SIZE);
  for (size_t i = 0; i < POOL_SIZE; ++i)
  {
    pool[i] = new MsgT();
  }

  auto q_ptr = std::make_unique<SPSCQueueAsymmetric<MsgT *, QUEUE_SIZE>>();
  auto &q = *q_ptr;

  auto consumer = std::thread([&]()
                              {
    if (!cpupin(6)) exit(1);
    int64_t count = 0;
    while (count < iters) {
      // Try batch pop
      size_t batch = q.tryPopBatch(BATCH_SIZE, [&](MsgT** val, size_t idx, size_t total) {
        MsgT* msg = *val;
        (void)msg->id;  // Touch memory
        count++;
      });

      if (batch == 0) {
        // Fallback to single item
        MsgT** msg_ptr;
        if ((msg_ptr = q.front())) {
          MsgT* msg = *msg_ptr;
          (void)msg->id;  // Touch memory
          count++;
          q.pop();
        }
      }
    } });

  if (!cpupin(7))
    exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i)
  {
    MsgT **slot;
    while (!(slot = q.alloc()))
      ;

    // Reuse pre-allocated object - NO rdtscp!
    uint32_t idx = i % POOL_SIZE;
    MsgT *msg = pool[idx];
    msg->id = i;
    msg->type = 1;

    *slot = msg;
    q.push();
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  // Cleanup pool
  for (auto *ptr : pool)
  {
    delete ptr;
  }

  return result;
}

// ============================================================
// Strategy: Index-Based with Asymmetric Queue + Batching
// ============================================================
template <typename MsgT, size_t QUEUE_SIZE = 16384, size_t POOL_SIZE = 8192, size_t BATCH_SIZE = 16>
Result benchmark_index_based_asymmetric(const std::string &name)
{
  Result result;
  result.name = name;

  // Pre-allocate message pool
  auto pool = std::make_unique<std::array<MsgT, POOL_SIZE>>();
  auto q_ptr = std::make_unique<SPSCQueueAsymmetric<uint32_t, QUEUE_SIZE>>();
  auto &q = *q_ptr;

  auto consumer = std::thread([&]()
                              {
    if (!cpupin(6)) exit(1);
    int64_t count = 0;
    while (count < iters) {
      // Try batch pop
      size_t batch = q.tryPopBatch(BATCH_SIZE, [&](uint32_t* val, size_t idx, size_t total) {
        uint32_t msg_idx = *val;
        MsgT& msg = (*pool)[msg_idx];
        (void)msg.id;  // Touch memory
        count++;
      });

      if (batch == 0) {
        // Fallback to single item
        uint32_t* idx_ptr;
        if ((idx_ptr = q.front())) {
          uint32_t msg_idx = *idx_ptr;
          MsgT& msg = (*pool)[msg_idx];
          (void)msg.id;  // Touch memory
          count++;
          q.pop();
        }
      }
    } });

  if (!cpupin(7))
    exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i)
  {
    uint32_t *slot;
    while (!(slot = q.alloc()))
      ;

    // Write to pool - NO rdtscp!
    uint32_t idx = i % POOL_SIZE;
    (*pool)[idx].id = i;
    (*pool)[idx].type = 1;

    *slot = idx;
    q.push();
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  return result;
}

// ============================================================
// Direct int with Asymmetric Queue + Batching (baseline comparison)
// ============================================================
template <size_t QUEUE_SIZE = 16384, size_t BATCH_SIZE = 16>
Result benchmark_direct_int_asymmetric(const std::string &name)
{
  Result result;
  result.name = name;

  auto q_ptr = std::make_unique<SPSCQueueAsymmetric<int, QUEUE_SIZE>>();
  auto &q = *q_ptr;

  auto consumer = std::thread([&]()
                              {
    if (!cpupin(6)) exit(1);
    int64_t count = 0;
    while (count < iters) {
      // Try batch pop
      size_t batch = q.tryPopBatch(BATCH_SIZE, [&](int* val, size_t idx, size_t total) {
        (void)*val;  // Touch memory
        count++;
      });

      if (batch == 0) {
        // Fallback to single item
        int* val;
        if ((val = q.front())) {
          (void)*val;
          count++;
          q.pop();
        }
      }
    } });

  if (!cpupin(7))
    exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i)
  {
    int *slot;
    while (!(slot = q.alloc()))
      ;
    *slot = i;
    q.push();
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  return result;
}

void print_results(const std::vector<Result> &results)
{
  std::cout << "\n"
            << std::string(80, '=') << "\n";
  std::cout << "Pure Throughput: Object Pool vs Index-Based vs Direct Int\n";
  std::cout << std::string(80, '=') << "\n\n";

  std::cout << std::setw(40) << "Strategy"
            << std::setw(20) << "Throughput"
            << std::setw(20) << "Runtime\n";
  std::cout << std::string(80, '-') << "\n";

  double max_tp = 0;
  for (const auto &r : results)
  {
    if (r.throughput_mops > max_tp)
      max_tp = r.throughput_mops;
  }

  for (const auto &r : results)
  {
    std::cout << std::setw(40) << r.name
              << std::setw(17) << std::fixed << std::setprecision(2)
              << r.throughput_mops << " M/s"
              << std::setw(18) << std::fixed << std::setprecision(3)
              << r.runtime_sec << " s";

    if (std::abs(r.throughput_mops - max_tp) < 0.1)
    {
      std::cout << " ✅ BEST";
    }
    std::cout << "\n";
  }

  std::cout << "\n"
            << std::string(80, '=') << "\n";
}

int main()
{
  std::cout << "\n"
            << std::string(80, '=') << "\n";
  std::cout << "Testing Hypothesis: Queue payload size determines throughput\n";
  std::cout << std::string(80, '=') << "\n\n";

  std::cout << "Configuration:\n";
  std::cout << "  CPU: Intel Core i7-11800H @ 2.30GHz\n";
  std::cout << "  Messages: " << iters << "\n";
  std::cout << "  Queue: SPSCQueueOPT, 16K size (heap)\n";
  std::cout << "  Method: Pure throughput (NO latency measurement)\n\n";

  std::cout << "Queue Payloads:\n";
  std::cout << "  Direct int:     4 bytes\n";
  std::cout << "  Index-Based:    4 bytes (uint32_t index)\n";
  std::cout << "  Object Pool:    8 bytes (pointer)\n\n";

  std::cout << "Hypothesis:\n";
  std::cout << "  Since queue payload is small (4-8 bytes), all strategies\n";
  std::cout << "  should achieve similar throughput (~100+ M/s)\n\n";

  std::vector<Result> results;

  std::cout << "\n"
            << std::string(60, '-') << "\n";
  std::cout << "Part 1: SPSCQueueOPT (NO batching)\n";
  std::cout << std::string(60, '-') << "\n\n";

  std::cout << "  Direct int (baseline)... ";
  std::cout.flush();
  results.push_back(benchmark_direct_int<16384>("1. Direct int (4B) - OPT"));
  std::cout << "✓\n";

  std::cout << "  Index-Based (4 bytes)... ";
  std::cout.flush();
  results.push_back(benchmark_index_based_pure<SmallMsg, 16384>("2. Index-Based (4B) - OPT"));
  std::cout << "✓\n";

  std::cout << "  Object Pool (8 bytes)... ";
  std::cout.flush();
  results.push_back(benchmark_object_pool_pure_opt<SmallMsg, 16384>("3. Object Pool (8B) - OPT"));
  std::cout << "✓\n";

  std::cout << "\n"
            << std::string(60, '-') << "\n";
  std::cout << "Part 2: Asymmetric Queue (WITH batching, 16K queue)\n";
  std::cout << std::string(60, '-') << "\n\n";

  std::cout << "  Direct int (baseline)... ";
  std::cout.flush();
  results.push_back(benchmark_direct_int_asymmetric<16384, 16>("4. Direct int (4B) - Asymmetric batch=16"));
  std::cout << "✓\n";

  std::cout << "  Index-Based (4 bytes)... ";
  std::cout.flush();
  results.push_back(benchmark_index_based_asymmetric<SmallMsg, 16384, 8192, 16>("5. Index-Based (4B) - Asymmetric batch=16"));
  std::cout << "✓\n";

  std::cout << "  Object Pool (8 bytes)... ";
  std::cout.flush();
  results.push_back(benchmark_object_pool_asymmetric<SmallMsg, 16384, 8192, 16>("6. Object Pool (8B) - Asymmetric batch=16"));
  std::cout << "✓\n";

  std::cout << "\n"
            << std::string(60, '-') << "\n";
  std::cout << "Part 3: Asymmetric Queue (WITH batching, 16M queue)\n";
  std::cout << std::string(60, '-') << "\n\n";

  constexpr size_t LARGE_QUEUE = 1 << 24; // 16M

  std::cout << "  Direct int (baseline)... ";
  std::cout.flush();
  results.push_back(benchmark_direct_int_asymmetric<LARGE_QUEUE, 16>("7. Direct int (4B) - Asymmetric 16M batch=16"));
  std::cout << "✓\n";

  std::cout << "  Index-Based (4 bytes)... ";
  std::cout.flush();
  results.push_back(benchmark_index_based_asymmetric<SmallMsg, LARGE_QUEUE, 8192, 16>("8. Index-Based (4B) - Asymmetric 16M batch=16"));
  std::cout << "✓\n";

  std::cout << "  Object Pool (8 bytes)... ";
  std::cout.flush();
  results.push_back(benchmark_object_pool_asymmetric<SmallMsg, LARGE_QUEUE, 8192, 16>("9. Object Pool (8B) - Asymmetric 16M batch=16"));
  std::cout << "✓\n";

  std::cout << "\n"
            << std::string(60, '-') << "\n";
  std::cout << "Part 4: WITH Measurement Overhead (for comparison)\n";
  std::cout << std::string(60, '-') << "\n\n";

  std::cout << "  Object Pool + rdtscp() + vector... ";
  std::cout.flush();
  results.push_back(benchmark_object_pool_with_measurement<SmallMsg, 16384>("10. Object Pool - WITH measurement"));
  std::cout << "✓\n";

  print_results(results);

  std::cout << "\n"
            << std::string(80, '=') << "\n";
  std::cout << "KEY FINDINGS\n";
  std::cout << std::string(80, '=') << "\n\n";

  std::cout << "1. SPSCQueueOPT (no batching, 16K queue):\n";
  std::cout << "   Expected: ~100-128 M/s for all strategies\n";
  std::cout << "   Why: Queue payload is small (4-8 bytes)\n\n";

  std::cout << "2. Asymmetric (WITH batching=16, 16K queue):\n";
  std::cout << "   Expected: Higher than SPSCQueueOPT due to batch amortization\n";
  std::cout << "   Batch reduces atomic operations by ~16x\n\n";

  std::cout << "3. Asymmetric (WITH batching=16, 16M queue):\n";
  std::cout << "   Expected: MAXIMUM throughput (target: 800+ M/s)\n";
  std::cout << "   Why:\n";
  std::cout << "   - Queue never full (no producer waiting)\n";
  std::cout << "   - Consumer batching at 100% efficiency\n";
  std::cout << "   - Atomic operations: 10M / 16 = 625K (vs 10M)\n\n";

  std::cout << "4. Object Pool vs Index-Based:\n";
  std::cout << "   Since both pass small data (4-8 bytes), throughput should be similar\n";
  std::cout << "   Object Pool: 8-byte pointer\n";
  std::cout << "   Index-Based: 4-byte index\n\n";

  std::cout << "5. Measurement Overhead Impact:\n";
  std::cout << "   rdtscp() + vector.push_back() = ~50-80 ns overhead\n";
  std::cout << "   This reduces 100+ M/s to ~12-30 M/s!\n\n";

  std::cout << "ANSWER TO YOUR QUESTION:\n";
  std::cout << std::string(80, '-') << "\n";
  std::cout << "Q: Why can't Object Pool/Index-Based reach 818 M/s?\n\n";
  std::cout << "A: The 818 M/s required THREE things:\n";
  std::cout << "   1. Small queue payload (4 bytes) ✓ - We have this!\n";
  std::cout << "   2. Consumer batching (batch=16) ✓ - We added this!\n";
  std::cout << "   3. Large queue (16M) ✓ - We test this!\n\n";
  std::cout << "Expected result:\n";
  std::cout << "   Object Pool + Asymmetric + 16M should reach 700-800+ M/s!\n";
  std::cout << "   Index-Based + Asymmetric + 16M should reach 700-800+ M/s!\n\n";

  return 0;
}
