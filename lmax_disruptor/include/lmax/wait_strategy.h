/*
 * High-Performance LMAX Disruptor for C++
 *
 * WaitStrategy - Template-based wait strategies (ZERO virtual calls)
 *
 * All strategies are structs with inline methods - compiler fully inlines them.
 * This is the key difference from disruptor-cpp which uses virtual interfaces.
 */

#pragma once

#include <thread>
#include <chrono>
#include <atomic>
#include <emmintrin.h>  // _mm_pause

namespace lmax {

/**
 * Busy spin wait - lowest latency, highest CPU usage.
 *
 * Best for:
 * - Ultra-low latency requirements (<100ns)
 * - Dedicated CPU cores
 * - Short wait times
 */
struct BusySpinWait {
    void wait() noexcept {
        _mm_pause();  // ~10 cycles, reduces power and memory contention
    }

    void reset() noexcept {}

    // Called when data becomes available (no-op for busy spin)
    void signalAllWhenBlocking() noexcept {}
};

/**
 * Yielding wait - balanced CPU usage and latency.
 *
 * Spins briefly, then yields to OS scheduler.
 * Good for shared CPU environments.
 */
struct YieldingWait {
    static constexpr int SPIN_TRIES = 100;

    void wait() noexcept {
        if (++counter_ > SPIN_TRIES) {
            counter_ = 0;
            std::this_thread::yield();
        } else {
            _mm_pause();
        }
    }

    void reset() noexcept {
        counter_ = 0;
    }

    void signalAllWhenBlocking() noexcept {}

private:
    int counter_ = 0;
};

/**
 * Sleeping wait - lower CPU, higher latency.
 *
 * Spins briefly, yields, then sleeps.
 * Good for background processing.
 */
struct SleepingWait {
    static constexpr int SPIN_TRIES = 100;
    static constexpr int YIELD_TRIES = 100;

    void wait() noexcept {
        if (counter_ < SPIN_TRIES) {
            counter_++;
            _mm_pause();
        } else if (counter_ < SPIN_TRIES + YIELD_TRIES) {
            counter_++;
            std::this_thread::yield();
        } else {
            std::this_thread::sleep_for(std::chrono::nanoseconds(1));
        }
    }

    void reset() noexcept {
        counter_ = 0;
    }

    void signalAllWhenBlocking() noexcept {}

private:
    int counter_ = 0;
};

/**
 * Blocking wait with exponential backoff.
 *
 * Spins -> yields -> sleeps with increasing duration.
 * Lowest CPU usage for idle periods.
 */
struct BackoffWait {
    static constexpr int SPIN_TRIES = 50;
    static constexpr int YIELD_TRIES = 50;
    static constexpr int MAX_SLEEP_NS = 1000000;  // 1ms max

    void wait() noexcept {
        if (counter_ < SPIN_TRIES) {
            counter_++;
            _mm_pause();
        } else if (counter_ < SPIN_TRIES + YIELD_TRIES) {
            counter_++;
            std::this_thread::yield();
        } else {
            // Exponential backoff sleep
            int sleep_ns = std::min(1 << (counter_ - SPIN_TRIES - YIELD_TRIES), MAX_SLEEP_NS);
            std::this_thread::sleep_for(std::chrono::nanoseconds(sleep_ns));
            counter_++;
        }
    }

    void reset() noexcept {
        counter_ = 0;
    }

    void signalAllWhenBlocking() noexcept {}

private:
    int counter_ = 0;
};

/**
 * Hybrid wait - spins on producer side, yields on consumer side.
 *
 * Configurable spin count before yielding.
 */
template<int SpinCount = 1000>
struct HybridWait {
    void wait() noexcept {
        if (++counter_ > SpinCount) {
            std::this_thread::yield();
            counter_ = 0;
        } else {
            _mm_pause();
        }
    }

    void reset() noexcept {
        counter_ = 0;
    }

    void signalAllWhenBlocking() noexcept {}

private:
    int counter_ = 0;
};

/**
 * No-op wait - for testing or when external synchronization exists.
 */
struct NoWait {
    void wait() noexcept {}
    void reset() noexcept {}
    void signalAllWhenBlocking() noexcept {}
};

/**
 * Lite spin wait - minimal pause, maximum throughput.
 *
 * Just _mm_pause in a tight loop - for maximum throughput
 * when latency spikes are acceptable.
 */
struct LiteWait {
    void wait() noexcept {
        _mm_pause();
        _mm_pause();
        _mm_pause();
        _mm_pause();
    }

    void reset() noexcept {}
    void signalAllWhenBlocking() noexcept {}
};

} // namespace lmax
