/*
MIT License

Copyright (c) 2018 Meng Rao <raomeng1@gmail.com>
Batch extensions by Claude

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#pragma once
#include <atomic>
#include <cstddef>

template <class T, uint32_t CNT>
class SPSCQueueBatch
{
public:
  static_assert(CNT && !(CNT & (CNT - 1)), "CNT must be a power of 2");

  // ============================================================
  // Original single-item operations
  // ============================================================

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

  void push()
  {
    ((std::atomic<uint32_t> *)&write_idx)->store(write_idx + 1, std::memory_order_release);
  }

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

  template <typename Reader>
  bool tryPop(Reader reader)
  {
    T *v = front();
    if (!v)
      return false;
    reader(v);
    pop();
    return true;
  }

  // ============================================================
  // NEW: Batch operations
  // ============================================================

  /**
   * Allocate a batch of up to 'count' slots for writing.
   * Returns pointer to array of allocated slots and actual count via 'allocated'.
   *
   * Example:
   *   size_t allocated;
   *   T** slots = allocBatch(10, allocated);
   *   for (size_t i = 0; i < allocated; i++) {
   *       *slots[i] = my_data[i];
   *   }
   *   pushBatch(allocated);
   */
  T **allocBatch(size_t count, size_t &allocated)
  {
    // Refresh read_idx_cach if needed
    uint32_t available = CNT - (write_idx - read_idx_cach);
    if (available == 0)
    {
      read_idx_cach = ((std::atomic<uint32_t> *)&read_idx)->load(std::memory_order_consume);
      available = CNT - (write_idx - read_idx_cach);
      if (available == 0)
      {
        allocated = 0;
        return nullptr;
      }
    }

    // Limit to available space
    allocated = (count < available) ? count : available;

    // Fill batch_ptrs with pointers to slots
    uint32_t idx = write_idx;
    for (size_t i = 0; i < allocated; i++)
    {
      batch_ptrs[i] = &data[(idx + i) & mask];
    }

    return batch_ptrs;
  }

  /**
   * Push a batch of 'count' items that were previously allocated.
   * IMPORTANT: Only ONE atomic store for the entire batch!
   */
  void pushBatch(size_t count)
  {
    write_idx += count;
    ((std::atomic<uint32_t> *)&write_idx)->store(write_idx, std::memory_order_release);
  }

  /**
   * Try to push a batch of items using a writer callback.
   * Writer is called once with array of pointers and count.
   * Returns number of items actually pushed (0 if queue full).
   *
   * Example:
   *   size_t pushed = tryPushBatch(10, [](T** slots, size_t count) {
   *       for (size_t i = 0; i < count; i++) {
   *           *slots[i] = my_data[i];
   *       }
   *   });
   */
  template <typename BatchWriter>
  size_t tryPushBatch(size_t max_count, BatchWriter writer)
  {
    size_t allocated;
    T **slots = allocBatch(max_count, allocated);
    if (allocated == 0)
      return 0;

    writer(slots, allocated);
    pushBatch(allocated);
    return allocated;
  }

  /**
   * Get pointers to a batch of available items for reading.
   * Returns array of pointers and actual count via 'available'.
   *
   * Example:
   *   size_t available;
   *   T** slots = frontBatch(10, available);
   *   for (size_t i = 0; i < available; i++) {
   *       process(*slots[i]);
   *   }
   *   popBatch(available);
   */
  T **frontBatch(size_t max_count, size_t &available)
  {
    // Refresh write_idx_cach if needed
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

    // Limit to available items
    available = (max_count < avail) ? max_count : avail;

    // Fill batch_ptrs with pointers to items
    uint32_t idx = read_idx;
    for (size_t i = 0; i < available; i++)
    {
      batch_ptrs[i] = &data[(idx + i) & mask];
    }

    return batch_ptrs;
  }

  /**
   * Pop a batch of 'count' items that were previously read.
   * IMPORTANT: Only ONE atomic store for the entire batch!
   */
  void popBatch(size_t count)
  {
    read_idx += count;
    ((std::atomic<uint32_t> *)&read_idx)->store(read_idx, std::memory_order_release);
  }

  /**
   * Try to pop a batch of items using a reader callback.
   * Reader is called once with array of pointers and count.
   * Returns number of items actually popped (0 if queue empty).
   *
   * Example:
   *   size_t popped = tryPopBatch(10, [](T** slots, size_t count) {
   *       for (size_t i = 0; i < count; i++) {
   *           process(*slots[i]);
   *       }
   *   });
   */
  template <typename BatchReader>
  size_t tryPopBatch(size_t max_count, BatchReader reader)
  {
    size_t available;
    T **slots = frontBatch(max_count, available);
    if (available == 0)
      return 0;

    reader(slots, available);
    popBatch(available);
    return available;
  }

  /**
   * Get number of items currently in the queue.
   * Note: Not atomic with respect to concurrent operations.
   */
  size_t size() const
  {
    uint32_t write = ((std::atomic<uint32_t> *)&write_idx)->load(std::memory_order_acquire);
    uint32_t read = ((std::atomic<uint32_t> *)&read_idx)->load(std::memory_order_acquire);
    return write - read;
  }

  /**
   * Check if queue is empty.
   */
  bool empty() const
  {
    return size() == 0;
  }

  /**
   * Get available space in queue.
   */
  size_t available() const
  {
    return CNT - size();
  }

private:
  alignas(128) T data[CNT] = {};

  alignas(128) uint32_t write_idx = 0;
  uint32_t read_idx_cach = 0; // used only by writing thread

  alignas(128) uint32_t read_idx = 0;
  uint32_t write_idx_cach = 0; // used only by reading thread
  uint32_t mask = CNT - 1;

  // Thread-local storage for batch pointers
  // Max batch size is CNT to avoid overflow
  T *batch_ptrs[CNT];
};
