/*
MIT License

Asymmetric SPSC Queue - Optimized for different goals on each side

Producer: Low latency push (immediate atomic store)
Consumer: High throughput pop (batched atomic store)

Key insight:
- Producer already has read_idx cache → checking space is cheap
- Producer push is latency-critical → don't buffer, push immediately
- Consumer pop can be batched → amortize atomic store overhead

This gives us:
✓ Low latency for producer (no buffering delay)
✓ High throughput for consumer (batch pops)
✓ Best of both worlds!

Copyright (c) 2024 Claude
Based on SPSCQueue by Meng Rao <raomeng1@gmail.com>
*/

#pragma once
#include <atomic>
#include <cstddef>

template <class T, uint32_t CNT>
class SPSCQueueAsymmetric
{
public:
  static_assert(CNT && !(CNT & (CNT - 1)), "CNT must be a power of 2");

  // ============================================================
  // Producer API - Immediate push (low latency)
  // ============================================================

  /**
   * Allocate slot for writing
   * Fast: uses cached read_idx, only refreshes when needed
   */
  T *alloc()
  {
    if (write_idx - read_idx_cach == CNT)
    {
      read_idx_cach = ((std::atomic<uint32_t> *)&read_idx)->load(std::memory_order_consume);
      if (__builtin_expect(write_idx - read_idx_cach == CNT, 0))
      {
        return nullptr;
      }
    }
    return &data[write_idx & mask];
  }

  /**
   * Push immediately with atomic store
   * No buffering = lowest latency for producer
   */
  void push()
  {
    ((std::atomic<uint32_t> *)&write_idx)->store(write_idx + 1, std::memory_order_release);
  }

  /**
   * Callback-based push
   */
  template <typename Writer>
  bool tryPush(Writer writer)
  {
    T *p = alloc();
    if (!p)
      return false;
    writer(p);
    push();
    return true;
  }

  // ============================================================
  // Consumer API - Batched pop (high throughput)
  // ============================================================

  /**
   * Get available items for batch processing
   * Returns pointer to first item and count via available
   */
  T *frontBatch(size_t max_count, size_t &available)
  {
    // Refresh write_idx cache if needed
    uint32_t avail = write_idx_cach - read_idx;
    if (avail == 0)
    {
      write_idx_cach = ((std::atomic<uint32_t> *)&write_idx)->load(std::memory_order_acquire);
      avail = write_idx_cach - read_idx;
      if (avail == 0)
      {
        available = 0;
        return nullptr;
      }
    }

    // Limit to requested count
    available = (max_count < avail) ? max_count : avail;
    return &data[read_idx & mask];
  }

  /**
   * Pop a batch of items with single atomic store
   * CRITICAL: Only ONE atomic operation for entire batch!
   */
  void popBatch(size_t count)
  {
    read_idx += count;
    ((std::atomic<uint32_t> *)&read_idx)->store(read_idx, std::memory_order_release);
  }

  /**
   * Batch pop with callback
   * Processes multiple items with one atomic store
   */
  template <typename Reader>
  size_t tryPopBatch(size_t max_count, Reader reader)
  {
    size_t available;
    T *first = frontBatch(max_count, available);
    if (available == 0)
      return 0;

    // Process items (might wrap around, so use mask)
    for (size_t i = 0; i < available; i++)
    {
      reader(&data[(read_idx + i) & mask], i, available);
    }

    popBatch(available);
    return available;
  }

  /**
   * Single item pop (for compatibility)
   */
  T *front()
  {
    if (read_idx == write_idx_cach)
    {
      write_idx_cach = ((std::atomic<uint32_t> *)&write_idx)->load(std::memory_order_acquire);
      if (read_idx == write_idx_cach)
      {
        return nullptr;
      }
    }
    return &data[read_idx & mask];
  }

  void pop()
  {
    ((std::atomic<uint32_t> *)&read_idx)->store(read_idx + 1, std::memory_order_release);
  }

  // ============================================================
  // Adaptive consumer API - Auto batch when beneficial
  // ============================================================

  /**
   * Smart pop: automatically decides single vs batch
   * Uses batch when ≥min_batch items available
   */
  template <typename Reader>
  size_t smartPop(Reader reader, size_t max_batch = 32, size_t min_batch = 4)
  {
    size_t available;
    T *first = frontBatch(max_batch, available);

    if (available == 0)
    {
      return 0;
    }

    // Decide: batch or single?
    if (available >= min_batch)
    {
      // Batch: process multiple items, one atomic store
      for (size_t i = 0; i < available; i++)
      {
        reader(&data[(read_idx + i) & mask], i, available);
      }
      popBatch(available);
      return available;
    }
    else
    {
      // Single: low latency for small batches
      reader(&data[read_idx & mask], 0, 1);
      pop();
      return 1;
    }
  }

  // ============================================================
  // Utility
  // ============================================================

  size_t size() const
  {
    uint32_t write = ((std::atomic<uint32_t> *)&write_idx)->load(std::memory_order_acquire);
    uint32_t read = ((std::atomic<uint32_t> *)&read_idx)->load(std::memory_order_acquire);
    return write - read;
  }

  bool empty() const
  {
    return size() == 0;
  }

  size_t available() const
  {
    return CNT - size();
  }

private:
  alignas(128) T data[CNT] = {};

  // Producer state (hot path)
  alignas(128) uint32_t write_idx = 0;
  uint32_t read_idx_cach = 0;  // Cached for fast space check

  // Consumer state (hot path)
  alignas(128) uint32_t read_idx = 0;
  uint32_t write_idx_cach = 0;  // Cached for batch availability check
  uint32_t mask = CNT - 1;
};
