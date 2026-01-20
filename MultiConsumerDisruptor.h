/*
MIT License

LMAX Disruptor-Style Multi-Consumer Ring Buffer

Single producer, multiple consumers with dependency chains.
Each consumer tracks its own sequence and can wait on upstream sequences.

Example pipeline:
  Producer -> WAL Handler -> Replicator -> Matching Engine
                  |              |              |
              walSeq         replSeq        meSeq (gates producer)

Copyright (c) 2024
*/

#pragma once
#include <atomic>
#include <cstdint>
#include <cstddef>
#include <algorithm>
#include "WaitStrategy.h"
#include "ConsumerSequence.h"

template<typename T, uint32_t SIZE, typename WaitStrategy = BusySpinWait>
class MultiConsumerDisruptor {
    static_assert(SIZE && !(SIZE & (SIZE - 1)), "SIZE must be a power of 2");
    static constexpr uint32_t MASK = SIZE - 1;

public:
    MultiConsumerDisruptor() = default;

    // ═══════════════════════════════════════════════════════════════════════════
    // PRODUCER API
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Claim next sequence number for writing.
     * Blocks if buffer is full (gated by slowest consumer).
     * @return The claimed sequence number
     */
    uint64_t next() {
        uint64_t nextSeq = cursor_.load(std::memory_order_relaxed) + 1;
        uint64_t wrapPoint = nextSeq - SIZE;

        while (wrapPoint > cachedGatingSeq_) {
            cachedGatingSeq_ = getMinimumGatingSequence();
            if (wrapPoint > cachedGatingSeq_) {
                producerWait_.wait();
            }
        }
        producerWait_.reset();
        return nextSeq;
    }

    /**
     * Try to claim next sequence (non-blocking).
     * @return Sequence number, or 0 if buffer full
     */
    uint64_t tryNext() {
        uint64_t nextSeq = cursor_.load(std::memory_order_relaxed) + 1;
        uint64_t wrapPoint = nextSeq - SIZE;

        if (wrapPoint > cachedGatingSeq_) {
            cachedGatingSeq_ = getMinimumGatingSequence();
            if (wrapPoint > cachedGatingSeq_) {
                return 0;
            }
        }
        return nextSeq;
    }

    /**
     * Claim a batch of sequences.
     * @param count Number of slots to claim
     * @return The LAST sequence number. First = returned - count + 1
     */
    uint64_t next(uint32_t count) {
        uint64_t currentSeq = cursor_.load(std::memory_order_relaxed);
        uint64_t nextSeq = currentSeq + count;
        uint64_t wrapPoint = nextSeq - SIZE;

        while (wrapPoint > cachedGatingSeq_) {
            cachedGatingSeq_ = getMinimumGatingSequence();
            if (wrapPoint > cachedGatingSeq_) {
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
     * Publish a single sequence.
     */
    void publish(uint64_t seq) {
        cursor_.store(seq, std::memory_order_release);
    }

    /**
     * Publish a range [lo, hi] inclusive.
     */
    void publish(uint64_t lo, uint64_t hi) {
        cursor_.store(hi, std::memory_order_release);
    }

    /**
     * Get current cursor (highest published sequence).
     */
    uint64_t getCursor() const {
        return cursor_.load(std::memory_order_acquire);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CONSUMER SETUP
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Add a gating sequence (these gate the producer).
     * Typically the slowest/final consumers in the pipeline.
     * Producer cannot overwrite slots until gating sequence advances.
     */
    void addGatingSequence(ConsumerSequence* seq) {
        if (gatingCount_ < MAX_GATING_SEQUENCES) {
            gatingSequences_[gatingCount_++] = seq;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CONSUMER API
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Wait for sequence to be available from cursor (first-level consumer).
     * @param seq The sequence to wait for
     * @return The highest available sequence
     */
    uint64_t waitFor(uint64_t seq) {
        while (cursor_.load(std::memory_order_acquire) < seq) {
            consumerWait_.wait();
        }
        consumerWait_.reset();
        return cursor_.load(std::memory_order_acquire);
    }

    /**
     * Wait for sequence to be available from a dependency.
     * Used when a consumer depends on another consumer's progress.
     * @param seq The sequence to wait for
     * @param dependency The upstream consumer's sequence
     * @return The highest available sequence from dependency
     */
    uint64_t waitFor(uint64_t seq, const ConsumerSequence& dependency) {
        while (dependency.get() < seq) {
            consumerWait_.wait();
        }
        consumerWait_.reset();
        return dependency.get();
    }

    /**
     * Wait for sequence from multiple dependencies.
     * Returns the minimum available sequence.
     */
    template<typename... Deps>
    uint64_t waitFor(uint64_t seq, const ConsumerSequence& first, const Deps&... rest) {
        uint64_t minAvail = waitForSingle(seq, first);
        return std::min(minAvail, waitFor(seq, rest...));
    }

    /**
     * Get slot for reading (const version).
     */
    const T* get(uint64_t seq) const {
        return &buffer_[seq & MASK];
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // UTILITY
    // ═══════════════════════════════════════════════════════════════════════════

    static constexpr uint32_t capacity() { return SIZE; }

    /**
     * Get remaining capacity before buffer wraps.
     */
    size_t remainingCapacity() const {
        uint64_t produced = cursor_.load(std::memory_order_acquire);
        uint64_t consumed = getMinimumGatingSequence();
        return SIZE - static_cast<size_t>(produced - consumed);
    }

private:
    uint64_t waitForSingle(uint64_t seq, const ConsumerSequence& dependency) {
        while (dependency.get() < seq) {
            consumerWait_.wait();
        }
        return dependency.get();
    }

    uint64_t getMinimumGatingSequence() const {
        if (gatingCount_ == 0) return UINT64_MAX;

        uint64_t min = gatingSequences_[0]->get();
        for (size_t i = 1; i < gatingCount_; i++) {
            uint64_t val = gatingSequences_[i]->get();
            if (val < min) min = val;
        }
        return min;
    }

    static constexpr size_t MAX_GATING_SEQUENCES = 8;

    // Ring buffer (cache-line aligned)
    alignas(128) T buffer_[SIZE] = {};

    // Producer state
    alignas(64) std::atomic<uint64_t> cursor_{0};
    uint64_t cachedGatingSeq_ = 0;
    WaitStrategy producerWait_;

    // Gating sequences (slowest consumers)
    alignas(64) ConsumerSequence* gatingSequences_[MAX_GATING_SEQUENCES] = {};
    size_t gatingCount_ = 0;
    WaitStrategy consumerWait_;
};

// ═══════════════════════════════════════════════════════════════════════════════
// CONSUMER HANDLER BASE CLASS
// Convenience base for implementing pipeline stages
// ═══════════════════════════════════════════════════════════════════════════════

template<typename T, uint32_t SIZE, typename WaitStrategy = BusySpinWait>
class ConsumerHandler {
public:
    using Disruptor = MultiConsumerDisruptor<T, SIZE, WaitStrategy>;

    ConsumerHandler(Disruptor& disruptor)
        : disruptor_(disruptor) {}

    /**
     * Get this handler's sequence (for downstream consumers to wait on).
     */
    ConsumerSequence& getSequence() { return sequence_; }
    const ConsumerSequence& getSequence() const { return sequence_; }

    /**
     * Process entries from start to end (inclusive).
     * Subclasses override onEvent() to handle each entry.
     */
    void processRange(uint64_t start, uint64_t end) {
        for (uint64_t seq = start; seq <= end; seq++) {
            const T* entry = disruptor_.get(seq);
            onEvent(entry, seq, seq == end);
        }
        sequence_.set(end);
    }

protected:
    /**
     * Override to handle each event.
     * @param event The event data
     * @param sequence The sequence number
     * @param endOfBatch True if this is the last event in current batch
     */
    virtual void onEvent(const T* event, uint64_t sequence, bool endOfBatch) = 0;

    Disruptor& disruptor_;
    ConsumerSequence sequence_;
    uint64_t nextSequence_ = 1;
};
