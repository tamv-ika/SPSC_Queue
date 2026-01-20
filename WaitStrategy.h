/*
MIT License

Wait Strategies for LMAX Disruptor-Style Queues

Different strategies for consumers/producers waiting on sequences.
Choose based on latency vs CPU usage trade-off.

Copyright (c) 2024
*/

#pragma once
#include <thread>
#include <chrono>
#include <mutex>
#include <condition_variable>

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#include <emmintrin.h>
#define PAUSE_INSTRUCTION() _mm_pause()
#elif defined(__aarch64__) || defined(_M_ARM64)
#define PAUSE_INSTRUCTION() __asm__ __volatile__("yield")
#else
#define PAUSE_INSTRUCTION() ((void)0)
#endif

// ═══════════════════════════════════════════════════════════════════════════════
// BusySpinWait - Lowest latency, highest CPU usage
// ═══════════════════════════════════════════════════════════════════════════════
class BusySpinWait {
public:
    void wait() {
        PAUSE_INSTRUCTION();
    }

    void reset() {}

    void signalAllWhenBlocking() {}
};

// ═══════════════════════════════════════════════════════════════════════════════
// YieldingWait - Balanced: spin briefly, then yield
// ═══════════════════════════════════════════════════════════════════════════════
class YieldingWait {
public:
    static constexpr int SPIN_TRIES = 100;

    void wait() {
        if (++counter_ > SPIN_TRIES) {
            std::this_thread::yield();
            counter_ = 0;
        } else {
            PAUSE_INSTRUCTION();
        }
    }

    void reset() {
        counter_ = 0;
    }

    void signalAllWhenBlocking() {}

private:
    int counter_ = 0;
};

// ═══════════════════════════════════════════════════════════════════════════════
// SleepingWait - Lower CPU, higher latency
// Three phases: spin -> yield -> sleep
// ═══════════════════════════════════════════════════════════════════════════════
class SleepingWait {
public:
    static constexpr int SPIN_TRIES = 100;
    static constexpr int YIELD_TRIES = 100;

    void wait() {
        if (counter_ < SPIN_TRIES) {
            PAUSE_INSTRUCTION();
        } else if (counter_ < SPIN_TRIES + YIELD_TRIES) {
            std::this_thread::yield();
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }
        counter_++;
    }

    void reset() {
        counter_ = 0;
    }

    void signalAllWhenBlocking() {}

private:
    int counter_ = 0;
};

// ═══════════════════════════════════════════════════════════════════════════════
// BlockingWait - Lowest CPU, highest latency
// Uses condition variable for true blocking
// ═══════════════════════════════════════════════════════════════════════════════
class BlockingWait {
public:
    void wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait_for(lock, std::chrono::microseconds(100));
    }

    void reset() {}

    void signalAllWhenBlocking() {
        cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
};

// ═══════════════════════════════════════════════════════════════════════════════
// PhasedBackoffWait - Adaptive: spin -> yield -> park
// Good for variable load scenarios
// ═══════════════════════════════════════════════════════════════════════════════
class PhasedBackoffWait {
public:
    static constexpr int SPIN_TRIES = 10;
    static constexpr int YIELD_TRIES = 10;
    static constexpr int PARK_TRIES = 10;

    void wait() {
        if (spinTries_ < SPIN_TRIES) {
            PAUSE_INSTRUCTION();
            spinTries_++;
        } else if (yieldTries_ < YIELD_TRIES) {
            std::this_thread::yield();
            yieldTries_++;
        } else if (parkTries_ < PARK_TRIES) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(1));
            parkTries_++;
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }
    }

    void reset() {
        spinTries_ = 0;
        yieldTries_ = 0;
        parkTries_ = 0;
    }

    void signalAllWhenBlocking() {}

private:
    int spinTries_ = 0;
    int yieldTries_ = 0;
    int parkTries_ = 0;
};
