/*
 * High-Performance LMAX Disruptor for C++
 *
 * RingBuffer - Main data structure combining buffer + sequencer
 *
 * This is the primary interface for most use cases.
 * Uses compile-time polymorphism to support both single and multi-producer modes.
 */

#pragma once

#include "sequence.h"
#include "sequence_barrier.h"
#include "single_producer_sequencer.h"
#include "multi_producer_sequencer.h"
#include "wait_strategy.h"
#include <array>
#include <cstddef>
#include <new>
#include <type_traits>

namespace lmax {


/**
 * Producer type tags for compile-time selection.
 */
struct SingleProducerType {};
struct MultiProducerType {};

/**
 * Sequencer type selector - compile-time polymorphism.
 * Selects the appropriate sequencer based on ProducerType tag.
 */
template<typename ProducerType, size_t Size, typename WaitStrategy>
struct SequencerSelector;

// Specialization for single producer
template<size_t Size, typename WaitStrategy>
struct SequencerSelector<SingleProducerType, Size, WaitStrategy> {
    using type = SingleProducerSequencer<Size, WaitStrategy>;
};

// Specialization for multi producer
template<size_t Size, typename WaitStrategy>
struct SequencerSelector<MultiProducerType, Size, WaitStrategy> {
    using type = MultiProducerSequencer<Size, WaitStrategy, 8>;
};

/**
 * Ring Buffer - supports both single and multiple producers via compile-time polymorphism.
 *
 * Template parameters:
 * - T: Event type stored in buffer
 * - Size: Buffer size (must be power of 2)
 * - WaitStrategy: How to wait when buffer full/empty
 * - ProducerType: SingleProducerType (default) or MultiProducerType
 */
template<typename T, size_t Size, typename WaitStrategy = BusySpinWait, typename ProducerType = SingleProducerType>
class RingBuffer {
    static_assert((Size & (Size - 1)) == 0, "Size must be power of 2");
    static constexpr size_t MASK = Size - 1;

public:
    using value_type = T;
    using barrier_type = SequenceBarrier<WaitStrategy>;
    using simple_barrier_type = SimpleBarrier<WaitStrategy>;
    using sequencer_type = typename SequencerSelector<ProducerType, Size, WaitStrategy>::type;
    using producer_type = ProducerType;

    static constexpr bool is_multi_producer = std::is_same_v<ProducerType, MultiProducerType>;

    RingBuffer() = default;

    // Non-copyable, non-movable
    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;

    // ═══════════════════════════════════════════════════════════════════════════
    // PRODUCER API
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Claim next sequence for publishing (blocking).
     * @return Claimed sequence number
     */
    inline __attribute__((always_inline)) int64_t next() noexcept {
        return sequencer_.next();
    }

    /**
     * Claim n sequences for batch publishing (blocking).
     * @param n Number of sequences to claim
     * @return Last claimed sequence (first = returned - n + 1)
     */
    inline __attribute__((always_inline)) int64_t next(int n) noexcept {
        return sequencer_.next(n);
    }

    /**
     * Try to claim next sequence (non-blocking).
     * @return Sequence if successful, -1 if buffer full
     */
    inline __attribute__((always_inline)) int64_t tryNext() noexcept {
        return sequencer_.tryNext();
    }

    /**
     * Try to claim n sequences (non-blocking).
     * @return Last sequence if successful, -1 if insufficient space
     */
    inline __attribute__((always_inline)) int64_t tryNext(int n) noexcept {
        return sequencer_.tryNext(n);
    }

    /**
     * Get slot for writing at given sequence.
     */
    inline __attribute__((always_inline)) T* get(int64_t sequence) noexcept {
        return &buffer_[sequence & MASK];
    }

    /**
     * Publish sequence (make visible to consumers).
     */
    inline __attribute__((always_inline)) void publish(int64_t sequence) noexcept {
        sequencer_.publish(sequence);
    }

    /**
     * Publish range of sequences [lo, hi].
     */
    inline __attribute__((always_inline)) void publish(int64_t lo, int64_t hi) noexcept {
        sequencer_.publish(lo, hi);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CONSUMER API
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Get slot for reading at given sequence (const).
     */
    inline __attribute__((always_inline)) const T* get(int64_t sequence) const noexcept {
        return &buffer_[sequence & MASK];
    }

    /**
     * Create a sequence barrier for consumers.
     * Consumer uses this to wait for sequences.
     */
    simple_barrier_type newBarrier() const noexcept {
        return simple_barrier_type(sequencer_.cursor());
    }

    /**
     * Create a barrier with dependencies on other sequences.
     * Used for pipeline patterns (consumer waits on upstream).
     */
    barrier_type newBarrier(std::initializer_list<const Sequence*> dependencies) const noexcept {
        return barrier_type(sequencer_.cursor(), dependencies);
    }

    /**
     * Get highest available sequence (non-blocking).
     */
    [[nodiscard]] inline __attribute__((always_inline)) int64_t available() const noexcept {
        return sequencer_.getCursor();
    }

    /**
     * Check if sequence is available.
     */
    [[nodiscard]] inline __attribute__((always_inline)) bool isAvailable(int64_t sequence) const noexcept {
        return sequencer_.isAvailable(sequence);
    }

    /**
     * Get highest contiguously published sequence in range.
     * For multi-producer, scans to find highest contiguous sequence.
     * For single-producer, just returns availableSequence.
     *
     * @param lowerBound Start sequence (inclusive)
     * @param availableSequence End sequence (inclusive)
     * @return Highest sequence where all lower sequences are also available
     */
    [[nodiscard]] int64_t getHighestPublishedSequence(int64_t lowerBound, int64_t availableSequence) const noexcept {
        return sequencer_.getHighestPublishedSequence(lowerBound, availableSequence);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // GATING (BACKPRESSURE)
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Add a gating sequence.
     * Producer will not advance past (minGatingSeq + bufferSize).
     * Typically add the slowest/final consumer's sequence.
     */
    void addGatingSequence(const Sequence& sequence) noexcept {
        sequencer_.addGatingSequence(sequence);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // UTILITY
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Get buffer capacity.
     */
    [[nodiscard]] static constexpr size_t capacity() noexcept {
        return Size;
    }

    /**
     * Get cursor reference (for monitoring).
     */
    [[nodiscard]] const Sequence& cursor() const noexcept {
        return sequencer_.cursor();
    }

    /**
     * Get current cursor value.
     */
    [[nodiscard]] inline __attribute__((always_inline)) int64_t getCursor() const noexcept {
        return sequencer_.getCursor();
    }

    /**
     * Check remaining capacity.
     */
    [[nodiscard]] int64_t remainingCapacity() const noexcept {
        return sequencer_.remainingCapacity();
    }

    /**
     * Check if buffer has capacity for n items.
     */
    [[nodiscard]] bool hasAvailableCapacity(int n) const noexcept {
        return sequencer_.hasAvailableCapacity(n);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // COMPATIBLE API (matches SPSCQueue interface)
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Allocate slot for writing (SPSCQueue compatible).
     * @return Pointer to slot, or nullptr if full
     */
    T* alloc() noexcept {
        int64_t seq = tryNext();
        if (seq == INITIAL_CURSOR_VALUE) {
            return nullptr;
        }
        pendingSeq_ = seq;
        return get(seq);
    }

    /**
     * Push the allocated slot (SPSCQueue compatible).
     */
    void push() noexcept {
        publish(pendingSeq_);
    }

    /**
     * Try push with writer callback (SPSCQueue compatible).
     */
    template<typename Writer>
    bool tryPush(Writer&& writer) noexcept {
        T* slot = alloc();
        if (!slot) return false;
        writer(slot);
        push();
        return true;
    }

    /**
     * Block push with writer callback.
     */
    template<typename Writer>
    void blockPush(Writer&& writer) noexcept {
        int64_t seq = next();
        T* slot = get(seq);
        writer(slot);
        publish(seq);
    }

    /**
     * Get the sequencer (for advanced use).
     */
    [[nodiscard]] const sequencer_type& sequencer() const noexcept {
        return sequencer_;
    }

    [[nodiscard]] sequencer_type& sequencer() noexcept {
        return sequencer_;
    }

private:
    // Buffer with cache-line alignment
    alignas(128) std::array<T, Size> buffer_{};

    // Sequencer (manages cursor and gating) - type determined at compile time
    sequencer_type sequencer_;

    // For SPSCQueue compatible API
    int64_t pendingSeq_ = INITIAL_CURSOR_VALUE;
};

// ═══════════════════════════════════════════════════════════════════════════
// CONVENIENCE ALIASES
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Single Producer Single Consumer Ring Buffer.
 */
template<typename T, size_t Size>
using SPSCRingBuffer = RingBuffer<T, Size, BusySpinWait, SingleProducerType>;

/**
 * Multi Producer Multi Consumer Ring Buffer.
 */
template<typename T, size_t Size>
using MPMCRingBuffer = RingBuffer<T, Size, BusySpinWait, MultiProducerType>;

/**
 * Single Producer Ring Buffer with custom wait strategy.
 */
template<typename T, size_t Size, typename WaitStrategy>
using SPRingBuffer = RingBuffer<T, Size, WaitStrategy, SingleProducerType>;

/**
 * Multi Producer Ring Buffer with custom wait strategy.
 */
template<typename T, size_t Size, typename WaitStrategy>
using MPRingBuffer = RingBuffer<T, Size, WaitStrategy, MultiProducerType>;

} // namespace lmax
