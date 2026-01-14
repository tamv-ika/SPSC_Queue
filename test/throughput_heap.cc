/**
 * Pure throughput benchmark with HEAP allocation
 * Allows testing very large queue sizes (up to 16M)
 */

#include <bits/stdc++.h>
#include "cpupin.h"

#include "../SPSCQueue.h"
#include "../SPSCQueueOPT.h"
#include "../SPSCQueueAsymmetric.h"
#include "../rigtorp_spsc/include/rigtorp/SPSCQueue.h"
#include "../moodycamel_rwq/readerwriterqueue.h"

const int64_t iters = 10000000;  // 10M

struct Result {
  std::string name;
  size_t queue_size;
  double ops_per_ms;
  double runtime_sec;
};

// ============================================================
// Benchmark SPSCQueueOPT with HEAP allocation
// ============================================================
template<size_t SIZE>
Result benchmark_spscqueueopt_heap(size_t queue_size) {
  Result result;
  result.name = "SPSCQueueOPT";
  result.queue_size = queue_size;

  // Allocate on HEAP
  auto q_ptr = std::make_unique<SPSCQueueOPT<int, SIZE>>();
  auto& q = *q_ptr;

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
  if (queue_size == (1 << 10)) return benchmark_spscqueueopt_heap<1 << 10>(queue_size);
  if (queue_size == (1 << 11)) return benchmark_spscqueueopt_heap<1 << 11>(queue_size);
  if (queue_size == (1 << 12)) return benchmark_spscqueueopt_heap<1 << 12>(queue_size);
  if (queue_size == (1 << 13)) return benchmark_spscqueueopt_heap<1 << 13>(queue_size);
  if (queue_size == (1 << 14)) return benchmark_spscqueueopt_heap<1 << 14>(queue_size);
  if (queue_size == (1 << 20)) return benchmark_spscqueueopt_heap<1 << 20>(queue_size); // 1M
  if (queue_size == (1 << 24)) return benchmark_spscqueueopt_heap<1 << 24>(queue_size); // 16M

  Result r;
  r.name = "SPSCQueueOPT";
  r.queue_size = queue_size;
  r.ops_per_ms = 0;
  r.runtime_sec = 0;
  return r;
}

// ============================================================
// Benchmark Asymmetric with HEAP allocation
// ============================================================
template<size_t SIZE>
Result benchmark_asymmetric_heap(size_t queue_size) {
  Result result;
  result.name = "Asymmetric";
  result.queue_size = queue_size;

  // Allocate on HEAP
  auto q_ptr = std::make_unique<SPSCQueueAsymmetric<int, SIZE>>();
  auto& q = *q_ptr;

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
  if (queue_size == (1 << 10)) return benchmark_asymmetric_heap<1 << 10>(queue_size);
  if (queue_size == (1 << 11)) return benchmark_asymmetric_heap<1 << 11>(queue_size);
  if (queue_size == (1 << 12)) return benchmark_asymmetric_heap<1 << 12>(queue_size);
  if (queue_size == (1 << 13)) return benchmark_asymmetric_heap<1 << 13>(queue_size);
  if (queue_size == (1 << 14)) return benchmark_asymmetric_heap<1 << 14>(queue_size);
  if (queue_size == (1 << 20)) return benchmark_asymmetric_heap<1 << 20>(queue_size);
  if (queue_size == (1 << 24)) return benchmark_asymmetric_heap<1 << 24>(queue_size);

  Result r;
  r.name = "Asymmetric";
  r.queue_size = queue_size;
  r.ops_per_ms = 0;
  r.runtime_sec = 0;
  return r;
}

// ============================================================
// Benchmark rigtorp (already heap-allocated internally)
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
    q.emplace(i);
  }
  consumer.join();
  auto stop = std::chrono::steady_clock::now();

  result.runtime_sec = std::chrono::duration<double>(stop - start).count();
  result.ops_per_ms = iters / (result.runtime_sec * 1000.0);
  return result;
}

// ============================================================
// Benchmark moodycamel (already heap-allocated internally)
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
  std::cout << "Pure Throughput Benchmark (HEAP Allocation)\n";
  std::cout << std::string(80, '=') << "\n\n";

  std::cout << "Configuration:\n";
  std::cout << "  CPU: Intel Core i7-11800H @ 2.30GHz\n";
  std::cout << "  Messages: " << iters << "\n";
  std::cout << "  Payload: int (4 bytes)\n";
  std::cout << "  Allocation: HEAP (std::make_unique)\n";
  std::cout << "  Method: Blocking push/pop (busy wait)\n\n";

  // Group by queue size
  std::map<size_t, std::vector<Result>> by_size;
  for (const auto& r : results) {
    if (r.ops_per_ms > 0) {
      by_size[r.queue_size].push_back(r);
    }
  }

  for (const auto& [size, size_results] : by_size) {
    std::cout << "\n" << std::string(80, '-') << "\n";

    // Format queue size nicely
    if (size >= (1 << 20)) {
      std::cout << "Queue Size: " << (size >> 20) << "M (" << size << ")\n";
    } else if (size >= (1 << 10)) {
      std::cout << "Queue Size: " << (size >> 10) << "K (" << size << ")\n";
    } else {
      std::cout << "Queue Size: " << size << "\n";
    }

    std::cout << std::string(80, '-') << "\n\n";

    std::cout << std::setw(20) << "Implementation"
              << std::setw(15) << "Throughput"
              << std::setw(15) << "Runtime"
              << std::setw(20) << "ops/ms\n";
    std::cout << std::string(70, '-') << "\n";

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
  // Test various queue sizes including LARGE ones
  std::vector<size_t> queue_sizes = {
    1024,           // 1K
    2048,           // 2K
    4096,           // 4K
    8192,           // 8K
    16384,          // 16K
    1 << 20,        // 1M
    1 << 24         // 16M (very large!)
  };

  std::vector<Result> all_results;

  for (size_t size : queue_sizes) {
    std::cout << "\n" << std::string(60, '=') << "\n";
    if (size >= (1 << 20)) {
      std::cout << "Testing queue size: " << (size >> 20) << "M (" << size << ")...\n";
    } else if (size >= (1 << 10)) {
      std::cout << "Testing queue size: " << (size >> 10) << "K (" << size << ")...\n";
    } else {
      std::cout << "Testing queue size: " << size << "...\n";
    }
    std::cout << std::string(60, '=') << "\n";

    try {
      std::cout << "  SPSCQueueOPT... ";
      std::cout.flush();
      auto r = benchmark_spscqueueopt(size);
      if (r.ops_per_ms > 0) {
        all_results.push_back(r);
        std::cout << "✓ " << std::fixed << std::setprecision(2) << (r.ops_per_ms / 1000.0) << " M/s\n";
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
        std::cout << "✓ " << std::fixed << std::setprecision(2) << (r.ops_per_ms / 1000.0) << " M/s\n";
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
      std::cout << "✓ " << std::fixed << std::setprecision(2) << (r.ops_per_ms / 1000.0) << " M/s\n";
    } catch (const std::exception& e) {
      std::cout << "failed: " << e.what() << "\n";
    }

    try {
      std::cout << "  moodycamel... ";
      std::cout.flush();
      auto r = benchmark_moodycamel(size);
      all_results.push_back(r);
      std::cout << "✓ " << std::fixed << std::setprecision(2) << (r.ops_per_ms / 1000.0) << " M/s\n";
    } catch (const std::exception& e) {
      std::cout << "failed: " << e.what() << "\n";
    }
  }

  print_results(all_results);

  return 0;
}
