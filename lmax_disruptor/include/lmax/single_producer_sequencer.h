/*
 * High-Performance LMAX Disruptor for C++
 *
 * SingleProducerSequencer - Claims sequences for a single producer
 *
 * This is the hot path - optimized for minimal overhead:
 * - No virtual calls
 * - Cached consumer sequence (avoid repeated atomic reads)
 * - Inline everything
 */

#pragma once

#include "sequence.h"
#include "wait_strategy.h"
#include <array>
#include <cstddef>
#include <stdexcept>

namespace lmax {

/**
 * Single producer sequencer - claims sequences for publishing.
 *
 * Template parameters:
 * - BufferSize: Must be power of 2
 * - WaitStrategy: How to wait when buffer is full
 * - MaxGatingSequences: Maximum number of consumer sequences to track
 */
template<size_t BufferSize, typename WaitStrategy = BusySpinWait, size_t MaxGatingSequences = 8>
class SingleProducerSequencer {
    static_assert((BufferSize & (BufferSize - 1)) == 0, "BufferSize must be power of 2");

public:
    static constexpr size_t BUFFER_SIZE = BufferSize;

    SingleProducerSequencer() noexcept = default;

    // Non-copyable, non-movable
    SingleProducerSequencer(const SingleProducerSequencer&) = delete;
    SingleProducerSequencer& operator=(const SingleProducerSequencer&) = delete;

    /**
     * Add a gating sequence (consumer sequence that gates the producer).
     * Producer cannot advance past (minGatingSequence + bufferSize).
     */
    void addGatingSequence(const Sequence& seq) noexcept {
        if (gatingCount_ < MaxGatingSequences) {
            gatingSequences_[gatingCount_++] = &seq;
        }
    }

    /**
     * Claim the next sequence for publishing (blocking).
     *
     * @return The claimed sequence number
     */
    inline __attribute__((always_inline)) int64_t next() noexcept {
        return next(1);
    }

    /**
     * Claim n sequences for batch publishing (blocking).
     *
     * @param n Number of sequences to claim
     * @return The LAST claimed sequence (first = returned - n + 1)
     */
    inline __attribute__((always_inline)) int64_t next(int n) noexcept {
        int64_t nextValue = nextValue_;
        int64_t nextSequence = nextValue + n;
        int64_t wrapPoint = nextSequence - static_cast<int64_t>(BufferSize);

        // Check if we need to wait for consumers
        if (__builtin_expect(wrapPoint > cachedGatingSequence_ || cachedGatingSequence_ > nextValue, 0)) {
            // Update cursor so consumers can see our intention
            cursor_.set(nextValue);

            int64_t minSequence;
            while (wrapPoint > (minSequence = getMinGatingSequence())) {
                wait_.signalAllWhenBlocking();
                wait_.wait();
            }
            wait_.reset();

            cachedGatingSequence_ = minSequence;
        }

        nextValue_ = nextSequence;
        return nextSequence;
    }

    /**
     * Try to claim next sequence (non-blocking).
     *
     * @return Sequence number if successful, or INITIAL_CURSOR_VALUE (-1) if buffer full
     */
    inline __attribute__((always_inline)) int64_t tryNext() noexcept {
        return tryNext(1);
    }

    /**
     * Try to claim n sequences (non-blocking).
     *
     * @param n Number of sequences to claim
     * @return Last sequence if successful, or INITIAL_CURSOR_VALUE (-1) if not enough space
     */
    inline __attribute__((always_inline)) int64_t tryNext(int n) noexcept {
        int64_t nextValue = nextValue_;
        int64_t nextSequence = nextValue + n;
        int64_t wrapPoint = nextSequence - static_cast<int64_t>(BufferSize);

        if (__builtin_expect(wrapPoint > cachedGatingSequence_ || cachedGatingSequence_ > nextValue, 0)) {
            int64_t minSequence = getMinGatingSequence();
            cachedGatingSequence_ = minSequence;

            if (wrapPoint > minSequence) {
                return INITIAL_CURSOR_VALUE;  // Buffer full
            }
        }

        nextValue_ = nextSequence;
        return nextSequence;
    }

    /**
     * Publish a sequence (make it visible to consumers).
     */
    inline __attribute__((always_inline)) void publish(int64_t sequence) noexcept {
        cursor_.set(sequence);
        wait_.signalAllWhenBlocking();
    }

    /**
     * Publish a range of sequences [lo, hi].
     */
    inline __attribute__((always_inline)) void publish(int64_t lo, int64_t hi) noexcept {
        publish(hi);
    }

    /**
     * Check if buffer has available capacity.
     */
    [[nodiscard]] bool hasAvailableCapacity(int requiredCapacity) const noexcept {
        int64_t nextValue = nextValue_;
        int64_t wrapPoint = (nextValue + requiredCapacity) - static_cast<int64_t>(BufferSize);

        if (wrapPoint > cachedGatingSequence_ || cachedGatingSequence_ > nextValue) {
            int64_t minSequence = getMinGatingSequence();
            // Note: can't update cache in const method
            if (wrapPoint > minSequence) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get remaining capacity.
     */
    [[nodiscard]] int64_t remainingCapacity() const noexcept {
        int64_t consumed = getMinGatingSequence();
        int64_t produced = nextValue_;
        return static_cast<int64_t>(BufferSize) - (produced - consumed);
    }

    /**
     * Get cursor (last published sequence).
     */
    [[nodiscard]] const Sequence& cursor() const noexcept {
        return cursor_;
    }

    /**
     * Get cursor value.
     */
    [[nodiscard]] inline __attribute__((always_inline)) int64_t getCursor() const noexcept {
        return cursor_.get();
    }

    /**
     * Check if sequence is available (published).
     */
    [[nodiscard]] inline __attribute__((always_inline)) bool isAvailable(int64_t sequence) const noexcept {
        return sequence <= cursor_.get();
    }

    /**
     * Get highest published sequence in range.
     * For single producer, this is just the cursor if in range.
     */
    [[nodiscard]] int64_t getHighestPublishedSequence(int64_t lowerBound, int64_t availableSequence) const noexcept {
        return availableSequence;
    }

private:
    [[nodiscard]] inline __attribute__((always_inline)) int64_t getMinGatingSequence() const noexcept {
        if (gatingCount_ == 0) {
            return INITIAL_CURSOR_VALUE;
        }

        int64_t minSeq = gatingSequences_[0]->get();
        for (size_t i = 1; i < gatingCount_; ++i) {
            int64_t seq = gatingSequences_[i]->get();
            if (seq < minSeq) {
                minSeq = seq;
            }
        }
        return minSeq;
    }

    // === Producer state (own cache line) ===
    alignas(64) Sequence cursor_;

    // === Producer-only fields (own cache line) ===
    alignas(64) int64_t nextValue_ = INITIAL_CURSOR_VALUE;
    int64_t cachedGatingSequence_ = INITIAL_CURSOR_VALUE;
    WaitStrategy wait_;

    // === Gating sequences (rarely changes) ===
    alignas(64) std::array<const Sequence*, MaxGatingSequences> gatingSequences_{};
    size_t gatingCount_ = 0;
};

} // namespace lmax
