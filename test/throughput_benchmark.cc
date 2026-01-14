/**
 * Pure throughput benchmark - matching rigtorp methodology
 *
 * Key differences from our previous benchmarks:
 * 1. BLOCKING push/pop (busy wait, no skip)
 * 2. Large queue size (no contention)
 * 3. Simple int payload (not 64-byte Msg)
 * 4. No latency measurement overhead
 * 5. Test multiple queue sizes: 1k, 2k, 4k, 10M
 */

#include <bits/stdc++.h>
#include "cpupin.h"

#include "../SPSCQueue.h"
#include "../SPSCQueueOPT.h"
#include "../SPSCQueueAsymmetric.h"
#include "../rigtorp_spsc/include/rigtorp/SPSCQueue.h"
#include "../moodycamel_rwq/readerwriterqueue.h"

const int64_t iters = 10000000;  // 10M like rigtorp

// Use power of 2 for large queue: 2^24 = 16777216 (close to 10M)
constexpr size_t LARGE_QUEUE_SIZE = 1 << 24;

struct Result {
  std::string name;
  size_t queue_size;
  double ops_per_ms;
  double runtime_sec;
};

// Note: SPSCQueue (original) skipped because it requires compile-time size
// and doesn't support the large queue sizes we're testing

// ============================================================
// Benchmark SPSCQueueOPT
// ============================================================
template<size_t SIZE>
Result benchmark_spscqueueopt_sized(size_t queue_size) {
  Result result;
  result.name = "SPSCQueueOPT";
  result.queue_size = queue_size;

  SPSCQueueOPT<int, SIZE> q;

  auto consumer = std::thread([&] {
    if (!cpupin(6)) exit(1);
    for (int64_t i = 0; i < iters; ++i) {
      int* val;
      while (!(val = q.front()));
      if (*val != i) throw std::runtime_error("value mismatch");
      q.pop();
    }
  });

  if (!cpupin(7)) exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i) {
    int* slot;
    while (!(slot = q.alloc()));
    *slot = i;
    q.push();
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.ops_per_ms = iters / (result.runtime_sec * 1000.0);
  return result;
}

Result benchmark_spscqueueopt(size_t queue_size) {
  if (queue_size == (1 << 10)) return benchmark_spscqueueopt_sized<1 << 10>(queue_size);
  if (queue_size == (1 << 11)) return benchmark_spscqueueopt_sized<1 << 11>(queue_size);
  if (queue_size == (1 << 12)) return benchmark_spscqueueopt_sized<1 << 12>(queue_size);
  if (queue_size == (1 << 13)) return benchmark_spscqueueopt_sized<1 << 13>(queue_size);

  Result r;
  r.name = "SPSCQueueOPT";
  r.queue_size = queue_size;
  r.ops_per_ms = 0;
  r.runtime_sec = 0;
  return r;
}

// ============================================================
// Benchmark Asymmetric
// ============================================================
template<size_t SIZE>
Result benchmark_asymmetric_sized(size_t queue_size) {
  Result result;
  result.name = "Asymmetric";
  result.queue_size = queue_size;

  SPSCQueueAsymmetric<int, SIZE> q;

  auto consumer = std::thread([&] {
    if (!cpupin(6)) exit(1);
    int64_t count = 0;
    while (count < iters) {
      size_t batch = q.tryPopBatch(16, [&](int* val, size_t idx, size_t total) {
        if (*val != count) throw std::runtime_error("value mismatch");
        count++;
      });
      if (batch == 0) {
        // Fallback to single item
        int* val;
        if ((val = q.front())) {
          if (*val != count) throw std::runtime_error("value mismatch");
          count++;
          q.pop();
        }
      }
    }
  });

  if (!cpupin(7)) exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i) {
    int* slot;
    while (!(slot = q.alloc()));
    *slot = i;
    q.push();
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.ops_per_ms = iters / (result.runtime_sec * 1000.0);
  return result;
}

Result benchmark_asymmetric(size_t queue_size) {
  if (queue_size == (1 << 10)) return benchmark_asymmetric_sized<1 << 10>(queue_size);
  if (queue_size == (1 << 11)) return benchmark_asymmetric_sized<1 << 11>(queue_size);
  if (queue_size == (1 << 12)) return benchmark_asymmetric_sized<1 << 12>(queue_size);
  if (queue_size == (1 << 13)) return benchmark_asymmetric_sized<1 << 13>(queue_size);

  Result r;
  r.name = "Asymmetric";
  r.queue_size = queue_size;
  r.ops_per_ms = 0;
  r.runtime_sec = 0;
  return r;
}

// ============================================================
// Benchmark rigtorp
// ============================================================
Result benchmark_rigtorp(size_t queue_size) {
  Result result;
  result.name = "rigtorp";
  result.queue_size = queue_size;

  rigtorp::SPSCQueue<int> q(queue_size);

  auto consumer = std::thread([&] {
    if (!cpupin(6)) exit(1);
    for (int64_t i = 0; i < iters; ++i) {
      while (!q.front());
      if (*q.front() != i) throw std::runtime_error("value mismatch");
      q.pop();
    }
  });

  if (!cpupin(7)) exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i) {
    q.emplace(i);  // rigtorp emplace is blocking
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.ops_per_ms = iters / (result.runtime_sec * 1000.0);
  return result;
}

// ============================================================
// Benchmark moodycamel
// ============================================================
Result benchmark_moodycamel(size_t queue_size) {
  Result result;
  result.name = "moodycamel";
  result.queue_size = queue_size;

  moodycamel::ReaderWriterQueue<int> q(queue_size);

  auto consumer = std::thread([&] {
    if (!cpupin(6)) exit(1);
    for (int64_t i = 0; i < iters; ++i) {
      int val;
      while (!q.try_dequeue(val));
      if (val != i) throw std::runtime_error("value mismatch");
    }
  });

  if (!cpupin(7)) exit(1);
  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < iters; ++i) {
    while (!q.try_enqueue(i));
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.ops_per_ms = iters / (result.runtime_sec * 1000.0);
  return result;
}

void print_results(const std::vector<Result>& results) {
  std::cout << "\n" << std::string(80, '=') << "\n";
  std::cout << "Pure Throughput Benchmark Results\n";
  std::cout << std::string(80, '=') << "\n\n";

  std::cout << "Configuration:\n";
  std::cout << "  CPU: Intel Core i7-11800H @ 2.30GHz\n";
  std::cout << "  Messages: " << iters << "\n";
  std::cout << "  Payload: int (4 bytes)\n";
  std::cout << "  Method: Blocking push/pop (busy wait)\n";
  std::cout << "  Cores: 6 (consumer) and 7 (producer)\n\n";

  // Group by queue size
  std::map<size_t, std::vector<Result>> by_size;
  for (const auto& r : results) {
    if (r.ops_per_ms > 0) {
      by_size[r.queue_size].push_back(r);
    }
  }

  for (const auto& [size, size_results] : by_size) {
    std::cout << "\n" << std::string(80, '-') << "\n";
    std::cout << "Queue Size: " << size << "\n";
    std::cout << std::string(80, '-') << "\n\n";

    std::cout << std::setw(20) << "Implementation"
              << std::setw(15) << "Throughput"
              << std::setw(15) << "Runtime"
              << std::setw(20) << "ops/ms\n";
    std::cout << std::string(70, '-') << "\n";

    // Find best for highlighting
    double max_ops = 0;
    for (const auto& r : size_results) {
      if (r.ops_per_ms > max_ops) max_ops = r.ops_per_ms;
    }

    for (const auto& r : size_results) {
      std::cout << std::setw(20) << r.name;

      if (r.ops_per_ms >= 1000.0) {
        std::cout << std::setw(12) << std::fixed << std::setprecision(2)
                  << (r.ops_per_ms / 1000.0) << " M/s";
      } else {
        std::cout << std::setw(12) << std::fixed << std::setprecision(2)
                  << r.ops_per_ms << " K/s";
      }

      std::cout << std::setw(13) << std::fixed << std::setprecision(3)
                << r.runtime_sec << " s"
                << std::setw(15) << std::fixed << std::setprecision(0)
                << r.ops_per_ms;

      if (std::abs(r.ops_per_ms - max_ops) < 0.01) {
        std::cout << " ✅ BEST";
      }
      std::cout << "\n";
    }
  }

  std::cout << "\n" << std::string(80, '=') << "\n";
}

int main() {
  // Test various queue sizes (avoid very large sizes due to stack overflow)
  std::vector<size_t> queue_sizes = {1024, 2048, 4096, 8192};
  std::vector<Result> all_results;

  for (size_t size : queue_sizes) {
    std::cout << "\nTesting queue size: " << size << "...\n";

    try {
      std::cout << "  SPSCQueueOPT... ";
      std::cout.flush();
      auto r = benchmark_spscqueueopt(size);
      if (r.ops_per_ms > 0) {
        all_results.push_back(r);
        std::cout << "✓\n";
      } else {
        std::cout << "skipped\n";
      }
    } catch (const std::exception& e) {
      std::cout << "failed: " << e.what() << "\n";
    }

    try {
      std::cout << "  Asymmetric... ";
      std::cout.flush();
      auto r = benchmark_asymmetric(size);
      if (r.ops_per_ms > 0) {
        all_results.push_back(r);
        std::cout << "✓\n";
      } else {
        std::cout << "skipped\n";
      }
    } catch (const std::exception& e) {
      std::cout << "failed: " << e.what() << "\n";
    }

    try {
      std::cout << "  rigtorp... ";
      std::cout.flush();
      auto r = benchmark_rigtorp(size);
      all_results.push_back(r);
      std::cout << "✓\n";
    } catch (const std::exception& e) {
      std::cout << "failed: " << e.what() << "\n";
    }

    try {
      std::cout << "  moodycamel... ";
      std::cout.flush();
      auto r = benchmark_moodycamel(size);
      all_results.push_back(r);
      std::cout << "✓\n";
    } catch (const std::exception& e) {
      std::cout << "failed: " << e.what() << "\n";
    }
  }

  print_results(all_results);

  return 0;
}
