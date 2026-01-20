/*
 * High-Performance LMAX Disruptor for C++
 *
 * MultiProducerSequencer - Claims sequences for multiple producer threads
 *
 * Key differences from SingleProducerSequencer:
 * - Uses CAS (compare-and-swap) for sequence claiming
 * - Maintains published[] array to track per-slot availability
 * - Supports concurrent producers without locks
 */

#pragma once

#include "sequence.h"
#include "wait_strategy.h"
#include <array>
#include <atomic>
#include <cstddef>
#include <algorithm>

namespace lmax {

/**
 * Multi producer sequencer - claims sequences for publishing from multiple threads.
 *
 * Template parameters:
 * - BufferSize: Must be power of 2
 * - WaitStrategy: How to wait when buffer is full
 * - MaxGatingSequences: Maximum number of consumer sequences to track
 */
template<size_t BufferSize, typename WaitStrategy = BusySpinWait, size_t MaxGatingSequences = 8>
class MultiProducerSequencer {
    static_assert((BufferSize & (BufferSize - 1)) == 0, "BufferSize must be power of 2");
    static constexpr size_t MASK = BufferSize - 1;
    static constexpr int INDEX_SHIFT = __builtin_ctzll(BufferSize);  // log2(BufferSize)

public:
    static constexpr size_t BUFFER_SIZE = BufferSize;

    MultiProducerSequencer() noexcept {
        // Initialize all slots as available (flag = -1)
        for (size_t i = 0; i < BufferSize; ++i) {
            availableBuffer_[i].store(-1, std::memory_order_relaxed);
        }
    }

    // Non-copyable, non-movable
    MultiProducerSequencer(const MultiProducerSequencer&) = delete;
    MultiProducerSequencer& operator=(const MultiProducerSequencer&) = delete;

    /**
     * Add a gating sequence (consumer sequence that gates producers).
     * Producers cannot advance past (minGatingSequence + bufferSize).
     */
    void addGatingSequence(const Sequence& seq) noexcept {
        size_t count = gatingCount_.load(std::memory_order_relaxed);
        if (count < MaxGatingSequences) {
            gatingSequences_[count] = &seq;
            gatingCount_.store(count + 1, std::memory_order_release);
        }
    }

    /**
     * Claim the next sequence for publishing (blocking).
     * Thread-safe - uses CAS for concurrent access.
     *
     * @return The claimed sequence number
     */
    inline int64_t next() noexcept {
        return next(1);
    }

    /**
     * Claim n sequences for batch publishing (blocking).
     * Thread-safe - uses CAS for concurrent access.
     *
     * @param n Number of sequences to claim
     * @return The LAST claimed sequence (first = returned - n + 1)
     */
    inline int64_t next(int n) noexcept {
        int64_t current;
        int64_t next;

        do {
            current = cursor_.get();
            next = current + n;

            int64_t wrapPoint = next - static_cast<int64_t>(BufferSize);
            int64_t cachedGating = cachedGatingSequence_.getRelaxed();

            if (wrapPoint > cachedGating || cachedGating > current) {
                int64_t gatingSequence = getMinGatingSequence();

                if (wrapPoint > gatingSequence) {
                    // Buffer is full, wait for consumers
                    wait_.signalAllWhenBlocking();
                    wait_.wait();
                    continue;
                }

                cachedGatingSequence_.setRelaxed(gatingSequence);
            }
        } while (!cursor_.compareAndSet(current, next));

        wait_.reset();
        return next;
    }

    /**
     * Try to claim next sequence (non-blocking).
     * Thread-safe - uses CAS for concurrent access.
     *
     * @return Sequence number if successful, or INITIAL_CURSOR_VALUE (-1) if buffer full
     */
    inline int64_t tryNext() noexcept {
        return tryNext(1);
    }

    /**
     * Try to claim n sequences (non-blocking).
     * Thread-safe - uses CAS for concurrent access.
     *
     * @param n Number of sequences to claim
     * @return Last sequence if successful, or INITIAL_CURSOR_VALUE (-1) if not enough space
     */
    inline int64_t tryNext(int n) noexcept {
        int64_t current;
        int64_t next;

        do {
            current = cursor_.get();
            next = current + n;

            int64_t wrapPoint = next - static_cast<int64_t>(BufferSize);
            int64_t cachedGating = cachedGatingSequence_.getRelaxed();

            if (wrapPoint > cachedGating || cachedGating > current) {
                int64_t gatingSequence = getMinGatingSequence();

                if (wrapPoint > gatingSequence) {
                    return INITIAL_CURSOR_VALUE;  // Buffer full
                }

                cachedGatingSequence_.setRelaxed(gatingSequence);
            }
        } while (!cursor_.compareAndSet(current, next));

        return next;
    }

    /**
     * Publish a sequence (make it visible to consumers).
     * Must be called AFTER writing data to the slot.
     *
     * For multi-producer, we set the flag in availableBuffer to indicate
     * this specific slot is now available.
     */
    inline void publish(int64_t sequence) noexcept {
        setAvailable(sequence);
        wait_.signalAllWhenBlocking();
    }

    /**
     * Publish a range of sequences [lo, hi].
     */
    inline void publish(int64_t lo, int64_t hi) noexcept {
        for (int64_t seq = lo; seq <= hi; ++seq) {
            setAvailable(seq);
        }
        wait_.signalAllWhenBlocking();
    }

    /**
     * Check if buffer has available capacity.
     */
    [[nodiscard]] bool hasAvailableCapacity(int requiredCapacity) const noexcept {
        int64_t current = cursor_.get();
        int64_t wrapPoint = (current + requiredCapacity) - static_cast<int64_t>(BufferSize);

        int64_t cachedGating = cachedGatingSequence_.getRelaxed();
        if (wrapPoint > cachedGating || cachedGating > current) {
            int64_t minSequence = getMinGatingSequence();
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
        int64_t produced = cursor_.get();
        return static_cast<int64_t>(BufferSize) - (produced - consumed);
    }

    /**
     * Get cursor (last claimed sequence - note: may not be fully published yet).
     */
    [[nodiscard]] const Sequence& cursor() const noexcept {
        return cursor_;
    }

    /**
     * Get cursor value.
     */
    [[nodiscard]] inline int64_t getCursor() const noexcept {
        return cursor_.get();
    }

    /**
     * Check if a specific sequence is available (published).
     * For multi-producer, checks the availableBuffer flag.
     */
    [[nodiscard]] inline bool isAvailable(int64_t sequence) const noexcept {
        size_t index = static_cast<size_t>(sequence) & MASK;
        int expectedFlag = calculateAvailabilityFlag(sequence);
        return availableBuffer_[index].load(std::memory_order_acquire) == expectedFlag;
    }

    /**
     * Get highest published sequence in range [lowerBound, availableSequence].
     *
     * For multi-producer, we need to scan backwards from availableSequence
     * to find the highest contiguously published sequence.
     */
    [[nodiscard]] int64_t getHighestPublishedSequence(int64_t lowerBound, int64_t availableSequence) const noexcept {
        for (int64_t sequence = lowerBound; sequence <= availableSequence; ++sequence) {
            if (!isAvailable(sequence)) {
                return sequence - 1;
            }
        }
        return availableSequence;
    }

private:
    /**
     * Calculate the flag value for a given sequence.
     * The flag encodes which "lap" around the ring buffer this sequence represents.
     */
    [[nodiscard]] inline int calculateAvailabilityFlag(int64_t sequence) const noexcept {
        return static_cast<int>(static_cast<uint64_t>(sequence) >> INDEX_SHIFT);
    }

    /**
     * Set a sequence as available (published).
     */
    inline void setAvailable(int64_t sequence) noexcept {
        size_t index = static_cast<size_t>(sequence) & MASK;
        int flag = calculateAvailabilityFlag(sequence);
        availableBuffer_[index].store(flag, std::memory_order_release);
    }

    [[nodiscard]] inline int64_t getMinGatingSequence() const noexcept {
        size_t count = gatingCount_.load(std::memory_order_acquire);
        if (count == 0) {
            return INITIAL_CURSOR_VALUE;
        }

        int64_t minSeq = gatingSequences_[0]->get();
        for (size_t i = 1; i < count; ++i) {
            int64_t seq = gatingSequences_[i]->get();
            if (seq < minSeq) {
                minSeq = seq;
            }
        }
        return minSeq;
    }

    // === Cursor (shared by all producers via CAS) ===
    alignas(64) Sequence cursor_;

    // === Available buffer - tracks which slots are published ===
    // Each element stores a flag indicating which "wrap" of the buffer has been published
    alignas(64) std::array<std::atomic<int>, BufferSize> availableBuffer_;

    // === Cached gating sequence (reduces atomic reads) ===
    alignas(64) UnpaddedSequence cachedGatingSequence_{INITIAL_CURSOR_VALUE};
    WaitStrategy wait_;

    // === Gating sequences (rarely changes) ===
    alignas(64) std::array<const Sequence*, MaxGatingSequences> gatingSequences_{};
    std::atomic<size_t> gatingCount_{0};
};

} // namespace lmax
