/*
 * High-Performance LMAX Disruptor for C++
 *
 * Sequence - Cache-line padded atomic sequence counter
 *
 * Key design:
 * - 64-byte alignment prevents false sharing
 * - int64_t (signed) starts at -1 like Java LMAX
 * - Explicit memory ordering for optimal performance
 */

#pragma once

#include <atomic>
#include <cstdint>

namespace lmax {

// Initial cursor value - nothing published yet
constexpr int64_t INITIAL_CURSOR_VALUE = -1;

/**
 * Cache-line padded sequence counter.
 *
 * Used for:
 * - Cursor (producer's published sequence)
 * - Consumer sequences (tracking consumer progress)
 * - Gating sequences (backpressure control)
 */
class Sequence {
public:
    explicit Sequence(int64_t initial = INITIAL_CURSOR_VALUE) noexcept
        : value_(initial) {}

    // Non-copyable, non-movable (contains atomic)
    Sequence(const Sequence&) = delete;
    Sequence& operator=(const Sequence&) = delete;
    Sequence(Sequence&&) = delete;
    Sequence& operator=(Sequence&&) = delete;

    /**
     * Get current value with acquire semantics.
     * Use when you need to see data written before this sequence.
     */
    [[nodiscard]] inline __attribute__((always_inline)) int64_t get() const noexcept {
        return value_.load(std::memory_order_acquire);
    }

    /**
     * Get current value with relaxed semantics.
     * Use only when you don't need synchronization.
     */
    [[nodiscard]] inline __attribute__((always_inline)) int64_t getRelaxed() const noexcept {
        return value_.load(std::memory_order_relaxed);
    }

    /**
     * Set value with release semantics.
     * Use after writing data to ensure visibility.
     */
    inline __attribute__((always_inline)) void set(int64_t value) noexcept {
        value_.store(value, std::memory_order_release);
    }

    /**
     * Set value with relaxed semantics.
     * Use only when ordering doesn't matter.
     */
    inline __attribute__((always_inline)) void setRelaxed(int64_t value) noexcept {
        value_.store(value, std::memory_order_relaxed);
    }

    /**
     * Compare and swap with acquire-release semantics.
     * Returns true if successful.
     */
    bool compareAndSet(int64_t expected, int64_t desired) noexcept {
        return value_.compare_exchange_strong(
            expected, desired,
            std::memory_order_acq_rel,
            std::memory_order_acquire
        );
    }

    /**
     * Atomic add and return NEW value.
     */
    int64_t addAndGet(int64_t delta) noexcept {
        return value_.fetch_add(delta, std::memory_order_acq_rel) + delta;
    }

    /**
     * Atomic increment and return NEW value.
     */
    int64_t incrementAndGet() noexcept {
        return addAndGet(1);
    }

    /**
     * Direct access to atomic (for advanced use).
     */
    std::atomic<int64_t>& ref() noexcept { return value_; }
    const std::atomic<int64_t>& ref() const noexcept { return value_; }

private:
    // Padding before to prevent false sharing with previous data
    char padding_pre_[56] = {};

    std::atomic<int64_t> value_;

    // Padding after to prevent false sharing with next data
    char padding_post_[56] = {};
};

// Verify cache line alignment
static_assert(sizeof(Sequence) >= 64, "Sequence must be at least cache-line sized");

/**
 * Lightweight sequence for cases where padding is handled externally.
 * Used inside sequencers where layout is already controlled.
 */
class alignas(8) UnpaddedSequence {
public:
    explicit UnpaddedSequence(int64_t initial = INITIAL_CURSOR_VALUE) noexcept
        : value_(initial) {}

    [[nodiscard]] inline __attribute__((always_inline)) int64_t get() const noexcept {
        return value_.load(std::memory_order_acquire);
    }

    [[nodiscard]] inline __attribute__((always_inline)) int64_t getRelaxed() const noexcept {
        return value_.load(std::memory_order_relaxed);
    }

    inline __attribute__((always_inline)) void set(int64_t value) noexcept {
        value_.store(value, std::memory_order_release);
    }

    inline __attribute__((always_inline)) void setRelaxed(int64_t value) noexcept {
        value_.store(value, std::memory_order_relaxed);
    }

    inline __attribute__((always_inline)) bool compareAndSet(int64_t expected, int64_t desired) noexcept {
        return value_.compare_exchange_strong(
            expected, desired,
            std::memory_order_acq_rel,
            std::memory_order_acquire
        );
    }

private:
    std::atomic<int64_t> value_;
};

} // namespace lmax
