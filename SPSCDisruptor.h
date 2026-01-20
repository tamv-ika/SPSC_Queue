/*
MIT License

LMAX Disruptor-Style Single Producer Single Consumer Queue

Key differences from traditional SPSC queue:
- Sequence-based: explicit sequence numbers instead of internal indices
- Batch-native: available() returns highest seq, process all at once
- Wait strategies: configurable spin/yield/sleep/block
- Gating: producer explicitly gated by consumer sequence

Copyright (c) 2024
*/

#pragma once
#include <atomic>
#include <cstdint>
#include <cstddef>
#include "WaitStrategy.h"

template<typename T, uint32_t SIZE, typename WaitStrategy = BusySpinWait>
class SPSCDisruptor {
    static_assert(SIZE && !(SIZE & (SIZE - 1)), "SIZE must be a power of 2");
    static constexpr uint32_t MASK = SIZE - 1;

public:
    SPSCDisruptor() = default;

    // ═══════════════════════════════════════════════════════════════════════════
    // PRODUCER API (LMAX Style)
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Claim the next sequence number for writing.
     * Blocks if buffer is full (waiting for consumer).
     * @return The claimed sequence number (1-indexed)
     */
    uint64_t next() {
        uint64_t nextSeq = cursor_.load(std::memory_order_relaxed) + 1;
        uint64_t wrapPoint = nextSeq - SIZE;

        // Wait for consumer to free space
        while (wrapPoint > cachedConsumerSeq_) {
            cachedConsumerSeq_ = consumerSeq_.load(std::memory_order_acquire);
            if (wrapPoint > cachedConsumerSeq_) {
                producerWait_.wait();
            }
        }
        producerWait_.reset();
        return nextSeq;
    }

    /**
     * Try to claim next sequence (non-blocking).
     * @return Sequence number (>0), or 0 if buffer full
     */
    uint64_t tryNext() {
        uint64_t nextSeq = cursor_.load(std::memory_order_relaxed) + 1;
        uint64_t wrapPoint = nextSeq - SIZE;

        if (wrapPoint > cachedConsumerSeq_) {
            cachedConsumerSeq_ = consumerSeq_.load(std::memory_order_acquire);
            if (wrapPoint > cachedConsumerSeq_) {
                return 0;  // Buffer full
            }
        }
        return nextSeq;
    }

    /**
     * Claim a batch of N sequences.
     * @param count Number of slots to claim
     * @return The LAST sequence number (inclusive). First = returned - count + 1
     */
    uint64_t next(uint32_t count) {
        uint64_t currentSeq = cursor_.load(std::memory_order_relaxed);
        uint64_t nextSeq = currentSeq + count;
        uint64_t wrapPoint = nextSeq - SIZE;

        while (wrapPoint > cachedConsumerSeq_) {
            cachedConsumerSeq_ = consumerSeq_.load(std::memory_order_acquire);
            if (wrapPoint > cachedConsumerSeq_) {
                producerWait_.wait();
            }
        }
        producerWait_.reset();
        return nextSeq;
    }

    /**
     * Get slot for writing at given sequence.
     */
    T* get(uint64_t seq) {
        return &buffer_[seq & MASK];
    }

    /**
     * Publish a single sequence (makes it visible to consumer).
     */
    void publish(uint64_t seq) {
        cursor_.store(seq, std::memory_order_release);
        consumerWait_.signalAllWhenBlocking();
    }

    /**
     * Publish a range of sequences [lo, hi] inclusive.
     * Use after claiming a batch with next(count).
     */
    void publish(uint64_t lo, uint64_t hi) {
        cursor_.store(hi, std::memory_order_release);
        consumerWait_.signalAllWhenBlocking();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CONSUMER API (LMAX Style)
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Wait for a sequence to become available.
     * @param seq The sequence to wait for
     * @return The highest available sequence (may be > seq)
     */
    uint64_t waitFor(uint64_t seq) {
        while (cursor_.load(std::memory_order_acquire) < seq) {
            consumerWait_.wait();
        }
        consumerWait_.reset();
        return cursor_.load(std::memory_order_acquire);
    }

    /**
     * Get highest available sequence (non-blocking).
     * @return Highest published sequence, or 0 if empty
     */
    uint64_t available() const {
        return cursor_.load(std::memory_order_acquire);
    }

    /**
     * Get slot for reading at given sequence (const version).
     */
    const T* get(uint64_t seq) const {
        return &buffer_[seq & MASK];
    }

    /**
     * Mark sequences as consumed (allows producer to reuse slots).
     * @param seq The highest consumed sequence
     */
    void markConsumed(uint64_t seq) {
        consumerSeq_.store(seq, std::memory_order_release);
        producerWait_.signalAllWhenBlocking();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // COMPATIBLE API (matches your existing SPSCQueue interface)
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Allocate slot for writing (compatible with SPSCQueue API).
     * @return Pointer to slot, or nullptr if full
     */
    T* alloc() {
        uint64_t seq = tryNext();
        if (seq == 0) return nullptr;
        pendingSeq_ = seq;
        return get(seq);
    }

    /**
     * Push (publish) the allocated slot.
     */
    void push() {
        publish(pendingSeq_);
    }

    /**
     * Try push with callback (compatible with SPSCQueue API).
     */
    template<typename Writer>
    bool tryPush(Writer writer) {
        T* slot = alloc();
        if (!slot) return false;
        writer(slot);
        push();
        return true;
    }

    /**
     * Block push with callback.
     */
    template<typename Writer>
    void blockPush(Writer writer) {
        uint64_t seq = next();
        T* slot = get(seq);
        writer(slot);
        publish(seq);
    }

    /**
     * Get front element for reading (compatible with SPSCQueue API).
     * @return Pointer to front element, or nullptr if empty
     */
    T* front() {
        uint64_t avail = cursor_.load(std::memory_order_acquire);
        if (nextConsumerSeq_ > avail) return nullptr;
        return &buffer_[nextConsumerSeq_ & MASK];
    }

    /**
     * Pop front element (compatible with SPSCQueue API).
     */
    void pop() {
        consumerSeq_.store(nextConsumerSeq_, std::memory_order_release);
        nextConsumerSeq_++;
        producerWait_.signalAllWhenBlocking();
    }

    /**
     * Try pop with callback (compatible with SPSCQueue API).
     */
    template<typename Reader>
    bool tryPop(Reader reader) {
        T* slot = front();
        if (!slot) return false;
        reader(slot);
        pop();
        return true;
    }

    /**
     * Batch pop (compatible with SPSCQueueAsymmetric API).
     * @param maxCount Maximum number of items to pop
     * @param reader Callback: void(T* item, size_t index, size_t totalInBatch)
     * @return Number of items processed
     */
    template<typename Reader>
    size_t tryPopBatch(size_t maxCount, Reader reader) {
        uint64_t avail = cursor_.load(std::memory_order_acquire);
        if (nextConsumerSeq_ > avail) return 0;

        uint64_t count = avail - nextConsumerSeq_ + 1;
        if (count > maxCount) count = maxCount;

        for (size_t i = 0; i < count; i++) {
            uint64_t seq = nextConsumerSeq_ + i;
            reader(&buffer_[seq & MASK], i, count);
        }

        nextConsumerSeq_ += count;
        consumerSeq_.store(nextConsumerSeq_ - 1, std::memory_order_release);
        producerWait_.signalAllWhenBlocking();
        return count;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // UTILITY
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Number of items currently in the queue.
     */
    size_t size() const {
        uint64_t produced = cursor_.load(std::memory_order_acquire);
        uint64_t consumed = consumerSeq_.load(std::memory_order_acquire);
        return static_cast<size_t>(produced - consumed);
    }

    /**
     * Check if queue is empty.
     */
    bool empty() const {
        return cursor_.load(std::memory_order_acquire) < nextConsumerSeq_;
    }

    /**
     * Get buffer capacity.
     */
    static constexpr uint32_t capacity() { return SIZE; }

    /**
     * Get current cursor (producer sequence).
     */
    uint64_t getCursor() const {
        return cursor_.load(std::memory_order_acquire);
    }

    /**
     * Get current consumer sequence.
     */
    uint64_t getConsumerSequence() const {
        return consumerSeq_.load(std::memory_order_acquire);
    }

private:
    // Ring buffer (cache-line aligned)
    alignas(128) T buffer_[SIZE] = {};

    // Producer state (own cache line)
    alignas(64) std::atomic<uint64_t> cursor_{0};     // Last published sequence
    uint64_t cachedConsumerSeq_ = 0;                   // Cached for fast space check
    uint64_t pendingSeq_ = 0;                          // For alloc()/push() API
    WaitStrategy producerWait_;

    // Consumer state (own cache line)
    alignas(64) std::atomic<uint64_t> consumerSeq_{0}; // Last consumed sequence
    uint64_t nextConsumerSeq_ = 1;                     // Next to consume (1-indexed)
    WaitStrategy consumerWait_;
};
