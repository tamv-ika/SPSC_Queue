/*
 * High-Performance LMAX Disruptor for C++
 *
 * SequenceBarrier - Consumer wait point with dependency tracking
 *
 * Consumers use barriers to wait for sequences to become available.
 * Supports dependency chains (e.g., WAL -> Replicator -> ME).
 */

#pragma once

#include "sequence.h"
#include "wait_strategy.h"
#include <array>
#include <cstddef>

namespace lmax {

/**
 * Sequence barrier for consumers.
 *
 * Template parameters:
 * - WaitStrategy: How to wait when data not available
 * - MaxDependencies: Maximum number of upstream sequences to track
 */
template<typename WaitStrategy = BusySpinWait, size_t MaxDependencies = 8>
class SequenceBarrier {
public:
    /**
     * Create barrier that waits on cursor only.
     * Used for first-level consumers.
     */
    explicit SequenceBarrier(const Sequence& cursor) noexcept
        : cursor_(cursor)
        , dependencyCount_(0)
    {}

    /**
     * Create barrier that waits on cursor AND dependencies.
     * Used for downstream consumers in a pipeline.
     */
    SequenceBarrier(const Sequence& cursor, std::initializer_list<const Sequence*> deps) noexcept
        : cursor_(cursor)
        , dependencyCount_(0)
    {
        for (const Sequence* dep : deps) {
            if (dependencyCount_ < MaxDependencies && dep != nullptr) {
                dependencies_[dependencyCount_++] = dep;
            }
        }
    }

    /**
     * Add a dependency sequence.
     */
    void addDependency(const Sequence& dep) noexcept {
        if (dependencyCount_ < MaxDependencies) {
            dependencies_[dependencyCount_++] = &dep;
        }
    }

    /**
     * Wait for a sequence to become available.
     *
     * @param sequence The sequence to wait for
     * @return The highest available sequence (may be > requested)
     */
    inline __attribute__((always_inline)) int64_t waitFor(int64_t sequence) noexcept {
        // First check cursor (producer's published sequence)
        int64_t availableSequence;
        while ((availableSequence = cursor_.get()) < sequence) {
            wait_.wait();
        }
        wait_.reset();

        // If we have dependencies, wait for them too
        if (dependencyCount_ > 0) {
            while ((availableSequence = getMinDependencySequence()) < sequence) {
                wait_.wait();
            }
            wait_.reset();
        }

        return availableSequence;
    }

    /**
     * Get highest available sequence without blocking.
     */
    [[nodiscard]] inline __attribute__((always_inline)) int64_t available() const noexcept {
        int64_t cursorSeq = cursor_.get();

        if (dependencyCount_ == 0) {
            return cursorSeq;
        }

        // Return minimum of cursor and all dependencies
        int64_t minSeq = cursorSeq;
        for (size_t i = 0; i < dependencyCount_; ++i) {
            int64_t depSeq = dependencies_[i]->get();
            if (depSeq < minSeq) {
                minSeq = depSeq;
            }
        }
        return minSeq;
    }

    /**
     * Check if a sequence is available (non-blocking).
     */
    [[nodiscard]] inline __attribute__((always_inline)) bool isAvailable(int64_t sequence) const noexcept {
        return available() >= sequence;
    }

    /**
     * Get cursor reference (for monitoring).
     */
    [[nodiscard]] const Sequence& cursor() const noexcept {
        return cursor_;
    }

private:
    [[nodiscard]] inline __attribute__((always_inline)) int64_t getMinDependencySequence() const noexcept {
        int64_t minSeq = dependencies_[0]->get();
        for (size_t i = 1; i < dependencyCount_; ++i) {
            int64_t seq = dependencies_[i]->get();
            if (seq < minSeq) {
                minSeq = seq;
            }
        }
        return minSeq;
    }

    const Sequence& cursor_;
    std::array<const Sequence*, MaxDependencies> dependencies_{};
    size_t dependencyCount_;
    WaitStrategy wait_;
};

/**
 * Simple barrier with no dependencies - just waits on cursor.
 * Slightly more efficient when no pipeline needed.
 */
template<typename WaitStrategy = BusySpinWait>
class SimpleBarrier {
public:
    explicit SimpleBarrier(const Sequence& cursor) noexcept
        : cursor_(cursor)
    {}

    inline __attribute__((always_inline)) int64_t waitFor(int64_t sequence) noexcept {
        int64_t availableSequence;
        while ((availableSequence = cursor_.get()) < sequence) {
            wait_.wait();
        }
        wait_.reset();
        return availableSequence;
    }

    [[nodiscard]] inline __attribute__((always_inline)) int64_t available() const noexcept {
        return cursor_.get();
    }

    /**
     * Get available sequence with relaxed memory ordering.
     * Use when you don't need to synchronize with producer's data writes.
     * (e.g., when just polling for availability)
     */
    [[nodiscard]] inline __attribute__((always_inline)) int64_t availableRelaxed() const noexcept {
        return cursor_.getRelaxed();
    }

    [[nodiscard]] inline __attribute__((always_inline)) bool isAvailable(int64_t sequence) const noexcept {
        return cursor_.get() >= sequence;
    }

private:
    const Sequence& cursor_;
    WaitStrategy wait_;
};

} // namespace lmax
