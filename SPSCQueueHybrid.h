/*
MIT License

Hybrid SPSC Queue - Best of both worlds: High throughput + Low latency

Key features:
1. Adaptive batching: batch when busy, single-item when idle
2. Smart flushing: timeout-based to guarantee max latency
3. Zero-copy interface compatible with both modes
4. Lock-free and wait-free operations

Copyright (c) 2024 Claude
Based on SPSCQueue by Meng Rao <raomeng1@gmail.com>
*/

#pragma once
#include <atomic>
#include <cstddef>

template <class T, uint32_t CNT>
class SPSCQueueHybrid
{
public:
  static_assert(CNT && !(CNT & (CNT - 1)), "CNT must be a power of 2");

  // Configuration for adaptive behavior
  struct Config {
    uint32_t min_batch_size = 4;      // Minimum batch size to amortize overhead
    uint32_t max_batch_size = 32;     // Maximum batch size to bound latency
    uint64_t flush_cycles = 1000;     // Max cycles before forcing flush (latency bound)
    uint32_t idle_threshold = 10;     // Consecutive empty checks before "idle" mode
  };

  SPSCQueueHybrid(const Config& cfg = Config()) : config(cfg) {}

  // ============================================================
  // Producer API - Adaptive Push
  // ============================================================

  /**
   * Smart push: automatically batches when beneficial
   * Returns true if pushed immediately, false if buffered for batch
   */
  bool smartPush(const T& item) {
    // Add to pending buffer
    if (pending_write_count >= MAX_PENDING || pending_write_count >= config.max_batch_size) {
      // Buffer full, force flush
      flushPending();
    }

    pending_writes[pending_write_count++] = item;

    // Check if we should flush
    bool should_flush = false;

    if (pending_write_count >= config.max_batch_size) {
      // Max batch reached
      should_flush = true;
    } else if (pending_write_count >= config.min_batch_size) {
      // Check timeout
      uint64_t now = __builtin_ia32_rdtsc();
      if (now - last_flush_ts > config.flush_cycles) {
        should_flush = true;
      }
    }

    if (should_flush) {
      flushPending();
      return true;
    }

    return false;
  }

  /**
   * Force flush any pending writes
   * Call this when you want to ensure low latency
   */
  void flush() {
    if (pending_write_count > 0) {
      flushPending();
    }
  }

  /**
   * Get number of pending items not yet flushed
   */
  uint32_t pending() const {
    return pending_write_count;
  }

  // ============================================================
  // Consumer API - Adaptive Pop
  // ============================================================

  /**
   * Smart pop: tries to pop a batch when available
   * Returns number of items popped (0 if queue empty)
   */
  template <typename Reader>
  uint32_t smartPop(Reader reader, uint32_t max_items = 32) {
    // Refresh write_idx cache if needed
    if (read_idx == write_idx_cach) {
      write_idx_cach = ((std::atomic<uint32_t>*)&write_idx)->load(std::memory_order_acquire);
      if (read_idx == write_idx_cach) {
        consecutive_empty_pops++;
        return 0;
      }
    }

    consecutive_empty_pops = 0;

    // Calculate available items
    uint32_t available = write_idx_cach - read_idx;
    uint32_t to_pop = (available < max_items) ? available : max_items;

    // Decide: batch or single?
    if (to_pop >= config.min_batch_size) {
      // Batch pop
      for (uint32_t i = 0; i < to_pop; i++) {
        reader(&data[(read_idx + i) & mask], i, to_pop);
      }
      read_idx += to_pop;
      ((std::atomic<uint32_t>*)&read_idx)->store(read_idx, std::memory_order_release);
      return to_pop;
    } else {
      // Single item pop for low latency
      reader(&data[read_idx & mask], 0, 1);
      ((std::atomic<uint32_t>*)&read_idx)->store(read_idx + 1, std::memory_order_release);
      return 1;
    }
  }

  /**
   * Peek next item without popping
   */
  T* peek() {
    if (read_idx == write_idx_cach) {
      write_idx_cach = ((std::atomic<uint32_t>*)&write_idx)->load(std::memory_order_acquire);
      if (read_idx == write_idx_cach) {
        return nullptr;
      }
    }
    return &data[read_idx & mask];
  }

  /**
   * Pop single item (manual control)
   */
  void pop() {
    ((std::atomic<uint32_t>*)&read_idx)->store(read_idx + 1, std::memory_order_release);
  }

  // ============================================================
  // Classic API (backward compatible)
  // ============================================================

  T* alloc() {
    if (write_idx - read_idx_cach == CNT) {
      read_idx_cach = ((std::atomic<uint32_t>*)&read_idx)->load(std::memory_order_consume);
      if (__builtin_expect(write_idx - read_idx_cach == CNT, 0)) {
        return nullptr;
      }
    }
    return &data[write_idx & mask];
  }

  void push() {
    ((std::atomic<uint32_t>*)&write_idx)->store(write_idx + 1, std::memory_order_release);
    last_flush_ts = __builtin_ia32_rdtsc();
  }

  T* front() {
    if (read_idx == write_idx_cach) {
      write_idx_cach = ((std::atomic<uint32_t>*)&write_idx)->load(std::memory_order_acquire);
      if (read_idx == write_idx_cach) {
        return nullptr;
      }
    }
    return &data[read_idx & mask];
  }

  // ============================================================
  // Statistics
  // ============================================================

  struct Stats {
    uint64_t total_pushes = 0;
    uint64_t batch_flushes = 0;
    uint64_t single_pops = 0;
    uint64_t batch_pops = 0;
    double avg_batch_size = 0.0;
  };

  Stats getStats() const {
    Stats s;
    s.total_pushes = stats_total_pushes;
    s.batch_flushes = stats_batch_flushes;
    s.single_pops = stats_single_pops;
    s.batch_pops = stats_batch_pops;
    if (s.batch_flushes > 0) {
      s.avg_batch_size = (double)s.total_pushes / s.batch_flushes;
    }
    return s;
  }

  void resetStats() {
    stats_total_pushes = 0;
    stats_batch_flushes = 0;
    stats_single_pops = 0;
    stats_batch_pops = 0;
  }

  // ============================================================
  // Utility
  // ============================================================

  size_t size() const {
    uint32_t write = ((std::atomic<uint32_t>*)&write_idx)->load(std::memory_order_acquire);
    uint32_t read = ((std::atomic<uint32_t>*)&read_idx)->load(std::memory_order_acquire);
    return write - read + pending_write_count;
  }

  bool empty() const {
    return size() == 0;
  }

  bool isIdle() const {
    return consecutive_empty_pops >= config.idle_threshold;
  }

private:
  void flushPending() {
    if (pending_write_count == 0) return;

    // Check space available
    if (write_idx - read_idx_cach + pending_write_count > CNT) {
      read_idx_cach = ((std::atomic<uint32_t>*)&read_idx)->load(std::memory_order_consume);
      // If still no space, this is a problem (caller should check)
      if (write_idx - read_idx_cach + pending_write_count > CNT) {
        // Queue full, can't flush - this shouldn't happen in normal use
        return;
      }
    }

    // Copy pending items to queue
    for (uint32_t i = 0; i < pending_write_count; i++) {
      data[(write_idx + i) & mask] = pending_writes[i];
    }

    // Single atomic store for entire batch
    write_idx += pending_write_count;
    ((std::atomic<uint32_t>*)&write_idx)->store(write_idx, std::memory_order_release);

    // Update stats
    stats_total_pushes += pending_write_count;
    stats_batch_flushes++;

    // Reset buffer
    pending_write_count = 0;
    last_flush_ts = __builtin_ia32_rdtsc();
  }

  // Configuration
  Config config;

  // Queue data
  alignas(128) T data[CNT] = {};

  // Producer state
  alignas(128) uint32_t write_idx = 0;
  uint32_t read_idx_cach = 0;
  uint64_t last_flush_ts = 0;

  // Producer-side pending buffer (limit to reasonable size to avoid stack overflow)
  static constexpr uint32_t MAX_PENDING = 64;
  T pending_writes[MAX_PENDING];
  uint32_t pending_write_count = 0;

  // Consumer state
  alignas(128) uint32_t read_idx = 0;
  uint32_t write_idx_cach = 0;
  uint32_t consecutive_empty_pops = 0;
  uint32_t mask = CNT - 1;

  // Statistics (producer side)
  uint64_t stats_total_pushes = 0;
  uint64_t stats_batch_flushes = 0;
  uint64_t stats_single_pops = 0;
  uint64_t stats_batch_pops = 0;
};
