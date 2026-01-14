/**
 * Benchmark: Pointer Passing vs Direct Transfer for Large Messages
 *
 * Tests message sizes: 64, 128, 256, 512, 1024 bytes
 * Compares:
 * 1. Direct Value (copy entire message in queue)
 * 2. Object Pool (8-byte pointer + indirection)
 * 3. Index-Based (4-byte index + indirection)
 *
 * Goal: Find the crossover point where pointer passing beats direct copy
 */

#include <bits/stdc++.h>
#include "cpupin.h"
#include "../SPSCQueueOPT.h"
#include "../SPSCQueueAsymmetric.h"

const int64_t iters = 10000000; // 10M

// Different message sizes
struct Msg64 {
  uint64_t id;
  uint64_t data[7];  // 64 bytes total
};

struct Msg128 {
  uint64_t id;
  uint64_t data[15];  // 128 bytes total
};

struct Msg256 {
  uint64_t id;
  uint64_t data[31];  // 256 bytes total
};

struct Msg512 {
  uint64_t id;
  uint64_t data[63];  // 512 bytes total
};

struct Msg1024 {
  uint64_t id;
  uint64_t data[127];  // 1024 bytes total
};

struct Result {
  std::string strategy;
  size_t msg_size;
  std::string queue_type;
  double throughput_mops;
  double runtime_sec;
};

// ============================================================
// Strategy 1: Direct Value Copy
// ============================================================
template<typename MsgT, size_t QUEUE_SIZE = 16384>
Result benchmark_direct_value(const std::string& queue_type, size_t msg_size) {
  Result result;
  result.strategy = "Direct Value";
  result.msg_size = msg_size;
  result.queue_type = queue_type;

  auto q_ptr = std::make_unique<SPSCQueueOPT<MsgT, QUEUE_SIZE>>();
  auto& q = *q_ptr;

  auto consumer = std::thread([&]() {
    if (!cpupin(6)) exit(1);
    for (int64_t i = 0; i < iters; ++i) {
      MsgT* msg;
      while (!(msg = q.front()));
      (void)msg->id;  // Touch memory
      q.pop();
    }
  });

  if (!cpupin(7)) exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i) {
    MsgT* slot;
    while (!(slot = q.alloc()));
    slot->id = i;
    q.push();
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  return result;
}

// ============================================================
// Strategy 2: Object Pool (Pointer Passing)
// ============================================================
template<typename MsgT, size_t QUEUE_SIZE = 16384, size_t POOL_SIZE = 8192>
Result benchmark_object_pool(const std::string& queue_type, size_t msg_size) {
  Result result;
  result.strategy = "Object Pool";
  result.msg_size = msg_size;
  result.queue_type = queue_type;

  // Pre-allocate object pool
  std::vector<MsgT*> pool(POOL_SIZE);
  for (size_t i = 0; i < POOL_SIZE; ++i) {
    pool[i] = new MsgT();
  }

  auto q_ptr = std::make_unique<SPSCQueueOPT<MsgT*, QUEUE_SIZE>>();
  auto& q = *q_ptr;

  auto consumer = std::thread([&]() {
    if (!cpupin(6)) exit(1);
    for (int64_t i = 0; i < iters; ++i) {
      MsgT** msg_ptr;
      while (!(msg_ptr = q.front()));
      MsgT* msg = *msg_ptr;
      (void)msg->id;  // Touch memory
      q.pop();
    }
  });

  if (!cpupin(7)) exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i) {
    MsgT** slot;
    while (!(slot = q.alloc()));

    uint32_t idx = i % POOL_SIZE;
    MsgT* msg = pool[idx];
    msg->id = i;

    *slot = msg;
    q.push();
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  // Cleanup
  for (auto* ptr : pool) {
    delete ptr;
  }

  return result;
}

// ============================================================
// Strategy 3: Index-Based
// ============================================================
template<typename MsgT, size_t QUEUE_SIZE = 16384, size_t POOL_SIZE = 8192>
Result benchmark_index_based(const std::string& queue_type, size_t msg_size) {
  Result result;
  result.strategy = "Index-Based";
  result.msg_size = msg_size;
  result.queue_type = queue_type;

  auto pool = std::make_unique<std::array<MsgT, POOL_SIZE>>();
  auto q_ptr = std::make_unique<SPSCQueueOPT<uint32_t, QUEUE_SIZE>>();
  auto& q = *q_ptr;

  auto consumer = std::thread([&]() {
    if (!cpupin(6)) exit(1);
    for (int64_t i = 0; i < iters; ++i) {
      uint32_t* idx_ptr;
      while (!(idx_ptr = q.front()));
      uint32_t idx = *idx_ptr;
      MsgT& msg = (*pool)[idx];
      (void)msg.id;  // Touch memory
      q.pop();
    }
  });

  if (!cpupin(7)) exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i) {
    uint32_t* slot;
    while (!(slot = q.alloc()));

    uint32_t idx = i % POOL_SIZE;
    (*pool)[idx].id = i;

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
// With Asymmetric Queue (16M, batch=16)
// ============================================================
template<typename MsgT, size_t QUEUE_SIZE = (1 << 24)>
Result benchmark_direct_value_asymmetric(const std::string& queue_type, size_t msg_size) {
  Result result;
  result.strategy = "Direct Value";
  result.msg_size = msg_size;
  result.queue_type = queue_type;

  auto q_ptr = std::make_unique<SPSCQueueAsymmetric<MsgT, QUEUE_SIZE>>();
  auto& q = *q_ptr;

  auto consumer = std::thread([&]() {
    if (!cpupin(6)) exit(1);
    int64_t count = 0;
    while (count < iters) {
      size_t batch = q.tryPopBatch(16, [&](MsgT* val, size_t idx, size_t total) {
        (void)val->id;
        count++;
      });
      if (batch == 0) {
        MsgT* val;
        if ((val = q.front())) {
          (void)val->id;
          count++;
          q.pop();
        }
      }
    }
  });

  if (!cpupin(7)) exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i) {
    MsgT* slot;
    while (!(slot = q.alloc()));
    slot->id = i;
    q.push();
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  return result;
}

template<typename MsgT, size_t QUEUE_SIZE = (1 << 24), size_t POOL_SIZE = 8192>
Result benchmark_object_pool_asymmetric(const std::string& queue_type, size_t msg_size) {
  Result result;
  result.strategy = "Object Pool";
  result.msg_size = msg_size;
  result.queue_type = queue_type;

  std::vector<MsgT*> pool(POOL_SIZE);
  for (size_t i = 0; i < POOL_SIZE; ++i) {
    pool[i] = new MsgT();
  }

  auto q_ptr = std::make_unique<SPSCQueueAsymmetric<MsgT*, QUEUE_SIZE>>();
  auto& q = *q_ptr;

  auto consumer = std::thread([&]() {
    if (!cpupin(6)) exit(1);
    int64_t count = 0;
    while (count < iters) {
      size_t batch = q.tryPopBatch(16, [&](MsgT** val, size_t idx, size_t total) {
        MsgT* msg = *val;
        (void)msg->id;
        count++;
      });
      if (batch == 0) {
        MsgT** msg_ptr;
        if ((msg_ptr = q.front())) {
          MsgT* msg = *msg_ptr;
          (void)msg->id;
          count++;
          q.pop();
        }
      }
    }
  });

  if (!cpupin(7)) exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i) {
    MsgT** slot;
    while (!(slot = q.alloc()));
    uint32_t idx = i % POOL_SIZE;
    MsgT* msg = pool[idx];
    msg->id = i;
    *slot = msg;
    q.push();
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  for (auto* ptr : pool) {
    delete ptr;
  }

  return result;
}

template<typename MsgT, size_t QUEUE_SIZE = (1 << 24), size_t POOL_SIZE = 8192>
Result benchmark_index_based_asymmetric(const std::string& queue_type, size_t msg_size) {
  Result result;
  result.strategy = "Index-Based";
  result.msg_size = msg_size;
  result.queue_type = queue_type;

  auto pool = std::make_unique<std::array<MsgT, POOL_SIZE>>();
  auto q_ptr = std::make_unique<SPSCQueueAsymmetric<uint32_t, QUEUE_SIZE>>();
  auto& q = *q_ptr;

  auto consumer = std::thread([&]() {
    if (!cpupin(6)) exit(1);
    int64_t count = 0;
    while (count < iters) {
      size_t batch = q.tryPopBatch(16, [&](uint32_t* val, size_t idx, size_t total) {
        uint32_t msg_idx = *val;
        MsgT& msg = (*pool)[msg_idx];
        (void)msg.id;
        count++;
      });
      if (batch == 0) {
        uint32_t* idx_ptr;
        if ((idx_ptr = q.front())) {
          uint32_t msg_idx = *idx_ptr;
          MsgT& msg = (*pool)[msg_idx];
          (void)msg.id;
          count++;
          q.pop();
        }
      }
    }
  });

  if (!cpupin(7)) exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i) {
    uint32_t* slot;
    while (!(slot = q.alloc()));
    uint32_t idx = i % POOL_SIZE;
    (*pool)[idx].id = i;
    *slot = idx;
    q.push();
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.throughput_mops = iters / result.runtime_sec / 1e6;

  return result;
}

void print_results(const std::vector<Result>& results) {
  std::cout << "\n" << std::string(100, '=') << "\n";
  std::cout << "Pointer vs Direct Transfer: Large Message Benchmark\n";
  std::cout << std::string(100, '=') << "\n\n";

  // Group by queue type and message size
  std::map<std::string, std::map<size_t, std::vector<Result>>> grouped;
  for (const auto& r : results) {
    grouped[r.queue_type][r.msg_size].push_back(r);
  }

  for (const auto& [queue_type, size_groups] : grouped) {
    std::cout << "\n" << std::string(100, '-') << "\n";
    std::cout << "Queue Type: " << queue_type << "\n";
    std::cout << std::string(100, '-') << "\n";

    for (const auto& [msg_size, size_results] : size_groups) {
      std::cout << "\nMessage Size: " << msg_size << " bytes\n";
      std::cout << std::string(80, '-') << "\n";

      std::cout << std::setw(20) << "Strategy"
                << std::setw(20) << "Throughput"
                << std::setw(20) << "Runtime"
                << std::setw(20) << "vs Direct Value\n";
      std::cout << std::string(80, '-') << "\n";

      double direct_value_tp = 0;
      for (const auto& r : size_results) {
        if (r.strategy == "Direct Value") {
          direct_value_tp = r.throughput_mops;
          break;
        }
      }

      double max_tp = 0;
      for (const auto& r : size_results) {
        if (r.throughput_mops > max_tp) max_tp = r.throughput_mops;
      }

      for (const auto& r : size_results) {
        std::cout << std::setw(20) << r.strategy
                  << std::setw(17) << std::fixed << std::setprecision(2)
                  << r.throughput_mops << " M/s"
                  << std::setw(18) << std::fixed << std::setprecision(3)
                  << r.runtime_sec << " s";

        if (direct_value_tp > 0 && r.strategy != "Direct Value") {
          double ratio = r.throughput_mops / direct_value_tp;
          std::cout << std::setw(13) << std::fixed << std::setprecision(2)
                    << ratio << "x";
          if (ratio > 1.05) {
            std::cout << " ✅ FASTER";
          } else if (ratio < 0.95) {
            std::cout << " ⚠️ SLOWER";
          } else {
            std::cout << " ≈ SAME";
          }
        } else if (std::abs(r.throughput_mops - max_tp) < 0.1) {
          std::cout << std::setw(20) << "BASELINE";
        }
        std::cout << "\n";
      }
    }
  }

  std::cout << "\n" << std::string(100, '=') << "\n";
}

int main() {
  std::cout << "\n" << std::string(100, '=') << "\n";
  std::cout << "Benchmark: Pointer Passing vs Direct Transfer for Large Messages\n";
  std::cout << std::string(100, '=') << "\n\n";

  std::cout << "Configuration:\n";
  std::cout << "  CPU: Intel Core i7-11800H @ 2.30GHz\n";
  std::cout << "  Messages: " << iters << "\n";
  std::cout << "  Optimization: -O3 -march=native -mtune=native -flto\n\n";

  std::cout << "Testing Message Sizes:\n";
  std::cout << "  64, 128, 256, 512, 1024 bytes\n\n";

  std::cout << "Strategies:\n";
  std::cout << "  1. Direct Value  - Copy entire message into queue\n";
  std::cout << "  2. Object Pool   - Pass 8-byte pointer (one indirection)\n";
  std::cout << "  3. Index-Based   - Pass 4-byte index (one indirection)\n\n";

  std::cout << "Question:\n";
  std::cout << "  At what message size does pointer passing beat direct copy?\n\n";

  std::vector<Result> results;

  // Test with SPSCQueueOPT (16K queue)
  std::cout << "\n" << std::string(80, '-') << "\n";
  std::cout << "Part 1: SPSCQueueOPT (16K queue, no batching)\n";
  std::cout << std::string(80, '-') << "\n\n";

  std::cout << "  64 bytes... ";
  std::cout.flush();
  results.push_back(benchmark_direct_value<Msg64, 16384>("OPT-16K", 64));
  results.push_back(benchmark_object_pool<Msg64, 16384>("OPT-16K", 64));
  results.push_back(benchmark_index_based<Msg64, 16384>("OPT-16K", 64));
  std::cout << "✓\n";

  std::cout << "  128 bytes... ";
  std::cout.flush();
  results.push_back(benchmark_direct_value<Msg128, 16384>("OPT-16K", 128));
  results.push_back(benchmark_object_pool<Msg128, 16384>("OPT-16K", 128));
  results.push_back(benchmark_index_based<Msg128, 16384>("OPT-16K", 128));
  std::cout << "✓\n";

  std::cout << "  256 bytes... ";
  std::cout.flush();
  results.push_back(benchmark_direct_value<Msg256, 16384>("OPT-16K", 256));
  results.push_back(benchmark_object_pool<Msg256, 16384>("OPT-16K", 256));
  results.push_back(benchmark_index_based<Msg256, 16384>("OPT-16K", 256));
  std::cout << "✓\n";

  std::cout << "  512 bytes... ";
  std::cout.flush();
  results.push_back(benchmark_direct_value<Msg512, 16384>("OPT-16K", 512));
  results.push_back(benchmark_object_pool<Msg512, 16384>("OPT-16K", 512));
  results.push_back(benchmark_index_based<Msg512, 16384>("OPT-16K", 512));
  std::cout << "✓\n";

  std::cout << "  1024 bytes... ";
  std::cout.flush();
  results.push_back(benchmark_direct_value<Msg1024, 16384>("OPT-16K", 1024));
  results.push_back(benchmark_object_pool<Msg1024, 16384>("OPT-16K", 1024));
  results.push_back(benchmark_index_based<Msg1024, 16384>("OPT-16K", 1024));
  std::cout << "✓\n";

  // Test with Asymmetric (16M queue, batch=16)
  std::cout << "\n" << std::string(80, '-') << "\n";
  std::cout << "Part 2: Asymmetric (16M queue, batch=16)\n";
  std::cout << std::string(80, '-') << "\n\n";

  std::cout << "  64 bytes... ";
  std::cout.flush();
  results.push_back(benchmark_direct_value_asymmetric<Msg64>("Asymmetric-16M", 64));
  results.push_back(benchmark_object_pool_asymmetric<Msg64>("Asymmetric-16M", 64));
  results.push_back(benchmark_index_based_asymmetric<Msg64>("Asymmetric-16M", 64));
  std::cout << "✓\n";

  std::cout << "  128 bytes... ";
  std::cout.flush();
  results.push_back(benchmark_direct_value_asymmetric<Msg128>("Asymmetric-16M", 128));
  results.push_back(benchmark_object_pool_asymmetric<Msg128>("Asymmetric-16M", 128));
  results.push_back(benchmark_index_based_asymmetric<Msg128>("Asymmetric-16M", 128));
  std::cout << "✓\n";

  std::cout << "  256 bytes... ";
  std::cout.flush();
  results.push_back(benchmark_direct_value_asymmetric<Msg256>("Asymmetric-16M", 256));
  results.push_back(benchmark_object_pool_asymmetric<Msg256>("Asymmetric-16M", 256));
  results.push_back(benchmark_index_based_asymmetric<Msg256>("Asymmetric-16M", 256));
  std::cout << "✓\n";

  std::cout << "  512 bytes... ";
  std::cout.flush();
  results.push_back(benchmark_direct_value_asymmetric<Msg512>("Asymmetric-16M", 512));
  results.push_back(benchmark_object_pool_asymmetric<Msg512>("Asymmetric-16M", 512));
  results.push_back(benchmark_index_based_asymmetric<Msg512>("Asymmetric-16M", 512));
  std::cout << "✓\n";

  std::cout << "  1024 bytes... ";
  std::cout.flush();
  results.push_back(benchmark_direct_value_asymmetric<Msg1024>("Asymmetric-16M", 1024));
  results.push_back(benchmark_object_pool_asymmetric<Msg1024>("Asymmetric-16M", 1024));
  results.push_back(benchmark_index_based_asymmetric<Msg1024>("Asymmetric-16M", 1024));
  std::cout << "✓\n";

  print_results(results);

  std::cout << "\n" << std::string(100, '=') << "\n";
  std::cout << "CONCLUSION\n";
  std::cout << std::string(100, '=') << "\n\n";

  std::cout << "Expected Findings:\n\n";

  std::cout << "1. Small Messages (64-128 bytes):\n";
  std::cout << "   Direct Value likely WINS - copy is cheap, fits in cache lines\n\n";

  std::cout << "2. Medium Messages (256 bytes):\n";
  std::cout << "   CROSSOVER POINT - pointer passing starts to win\n\n";

  std::cout << "3. Large Messages (512-1024 bytes):\n";
  std::cout << "   Pointer/Index WINS - avoids expensive memory copies\n\n";

  std::cout << "Memory Copy Cost:\n";
  std::cout << "  64 bytes   = 1 cache line copy (~2-3 ns)\n";
  std::cout << "  128 bytes  = 2 cache lines (~4-6 ns)\n";
  std::cout << "  256 bytes  = 4 cache lines (~8-12 ns)\n";
  std::cout << "  512 bytes  = 8 cache lines (~16-24 ns)\n";
  std::cout << "  1024 bytes = 16 cache lines (~32-48 ns)\n\n";

  std::cout << "Pointer Indirection Cost:\n";
  std::cout << "  One pointer dereference: ~1-2 ns (if in L1 cache)\n\n";

  std::cout << "Rule of Thumb:\n";
  std::cout << "  - Messages < 128 bytes: Use Direct Value\n";
  std::cout << "  - Messages 128-256 bytes: Either works, test your workload\n";
  std::cout << "  - Messages > 256 bytes: Use Pointer/Index-Based\n\n";

  return 0;
}
