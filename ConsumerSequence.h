/*
MIT License

Consumer Sequence for LMAX Disruptor-Style Multi-Consumer Patterns

Cache-line padded atomic sequence counter for tracking consumer progress.
Used in dependency chains where consumers wait on upstream sequences.

Copyright (c) 2024
*/

#pragma once
#include <atomic>
#include <cstdint>

/**
 * Cache-line padded sequence counter for consumers.
 *
 * Usage:
 *   ConsumerSequence walSeq;
 *   ConsumerSequence replSeq;
 *
 *   // WAL handler updates its sequence after processing
 *   walSeq.set(processedSeq);
 *
 *   // Replicator waits for WAL
 *   while (walSeq.get() < targetSeq) { wait; }
 */
class ConsumerSequence {
public:
    explicit ConsumerSequence(uint64_t initial = 0) : value_(initial) {}

    // Non-copyable, non-movable (contains atomic)
    ConsumerSequence(const ConsumerSequence&) = delete;
    ConsumerSequence& operator=(const ConsumerSequence&) = delete;
    ConsumerSequence(ConsumerSequence&&) = delete;
    ConsumerSequence& operator=(ConsumerSequence&&) = delete;

    /**
     * Get current sequence value with acquire semantics.
     * Use when reading to ensure visibility of data written before the sequence.
     */
    uint64_t get() const {
        return value_.load(std::memory_order_acquire);
    }

    /**
     * Get current sequence value with relaxed semantics.
     * Use only when you don't need to see data associated with the sequence.
     */
    uint64_t getRelaxed() const {
        return value_.load(std::memory_order_relaxed);
    }

    /**
     * Set sequence value with release semantics.
     * Use after writing data to ensure it's visible to readers.
     */
    void set(uint64_t seq) {
        value_.store(seq, std::memory_order_release);
    }

    /**
     * Set sequence value with relaxed semantics.
     * Use only when ordering doesn't matter.
     */
    void setRelaxed(uint64_t seq) {
        value_.store(seq, std::memory_order_relaxed);
    }

    /**
     * Compare and swap.
     * @return true if successful
     */
    bool compareAndSet(uint64_t expected, uint64_t desired) {
        return value_.compare_exchange_strong(expected, desired,
                                               std::memory_order_acq_rel);
    }

    /**
     * Add delta to sequence atomically.
     * @return The value BEFORE the add
     */
    uint64_t addAndGet(uint64_t delta) {
        return value_.fetch_add(delta, std::memory_order_acq_rel) + delta;
    }

    /**
     * Get reference to underlying atomic (for advanced use).
     */
    std::atomic<uint64_t>& ref() { return value_; }
    const std::atomic<uint64_t>& ref() const { return value_; }

private:
    alignas(64) std::atomic<uint64_t> value_;
    char padding_[64 - sizeof(std::atomic<uint64_t>)];  // Pad to full cache line
};

/**
 * Sequence Group - tracks minimum of multiple sequences.
 * Used for gating producers by the slowest consumer.
 */
class SequenceGroup {
public:
    SequenceGroup() = default;

    /**
     * Add a sequence to track.
     */
    void add(ConsumerSequence* seq) {
        sequences_[count_++] = seq;
    }

    /**
     * Get minimum sequence among all tracked.
     * This is the safe sequence - producer can write up to this + buffer_size.
     */
    uint64_t getMinimum() const {
        if (count_ == 0) return UINT64_MAX;

        uint64_t min = sequences_[0]->get();
        for (size_t i = 1; i < count_; i++) {
            uint64_t val = sequences_[i]->get();
            if (val < min) min = val;
        }
        return min;
    }

    /**
     * Get number of tracked sequences.
     */
    size_t size() const { return count_; }

private:
    static constexpr size_t MAX_SEQUENCES = 16;
    ConsumerSequence* sequences_[MAX_SEQUENCES] = {};
    size_t count_ = 0;
};
