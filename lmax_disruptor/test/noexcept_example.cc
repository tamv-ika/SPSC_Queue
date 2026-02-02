/*
 * NoExcept Handler Example
 *
 * Shows both EventHandler (with exceptions) and NoExceptEventHandler (zero-exception)
 * being used with the same DSL API.
 *
 * Compile: g++ -std=c++20 -O3 -o noexcept_example noexcept_example.cc -pthread
 */

#include "../include/lmax/disruptor_dsl.h"
#include <iostream>
#include <chrono>

using namespace lmax;

struct Event {
    int64_t value;
    bool valid;
};

// ═══════════════════════════════════════════════════════════════════════════
// Traditional EventHandler (with exceptions)
// ═══════════════════════════════════════════════════════════════════════════

class TraditionalHandler : public EventHandler<Event> {
public:
    void onEvent(Event& e, int64_t seq, bool endOfBatch) override {
        if (!e.valid) {
            throw std::runtime_error("Invalid event!");  // Can throw
        }
        count_++;
    }

    void onException(std::exception& ex, int64_t seq, Event& e) override {
        errors_++;
        // Continue processing (don't rethrow)
    }

    void onStart() override {
        std::cout << "[Traditional] Started\n";
    }

    void onShutdown() override {
        std::cout << "[Traditional] Shutdown - processed: " << count_
                  << ", errors: " << errors_ << "\n";
    }

private:
    int64_t count_ = 0;
    int64_t errors_ = 0;
};

// ═══════════════════════════════════════════════════════════════════════════
// NoExceptEventHandler (zero-exception, result-based)
// ═══════════════════════════════════════════════════════════════════════════

class NoExceptHandler : public NoExceptEventHandler<Event> {
public:
    ProcessResult onEvent(Event& e, int64_t seq, bool endOfBatch) noexcept override {
        if (!e.valid) {
            errors_++;
            return ProcessResult::SKIP;  // Skip invalid, no exception
        }
        count_++;
        return ProcessResult::OK;
    }

    void onStart() noexcept override {
        std::cout << "[NoExcept] Started\n";
    }

    void onShutdown() noexcept override {
        std::cout << "[NoExcept] Shutdown - processed: " << count_
                  << ", skipped: " << errors_ << "\n";
    }

private:
    int64_t count_ = 0;
    int64_t errors_ = 0;
};

// ═══════════════════════════════════════════════════════════════════════════
// Example: Stop on error
// ═══════════════════════════════════════════════════════════════════════════

class StopOnErrorHandler : public NoExceptEventHandler<Event> {
public:
    ProcessResult onEvent(Event& e, int64_t seq, bool endOfBatch) noexcept override {
        if (!e.valid) {
            std::cout << "[StopOnError] Invalid event at seq " << seq << ", stopping!\n";
            return ProcessResult::STOP;  // Stop processor
        }
        count_++;
        return ProcessResult::OK;
    }

    void onShutdown() noexcept override {
        std::cout << "[StopOnError] Shutdown - processed: " << count_ << "\n";
    }

private:
    int64_t count_ = 0;
};

int main() {
    constexpr int64_t NUM_EVENTS = 10000;

    std::cout << "═══════════════════════════════════════════════════\n";
    std::cout << "     NoExcept vs Traditional Handler Example\n";
    std::cout << "═══════════════════════════════════════════════════\n\n";

    // ─────────────────────────────────────────────────────────────
    // Example 1: Traditional handler (with exceptions)
    // ─────────────────────────────────────────────────────────────
    {
        std::cout << "Example 1: Traditional EventHandler\n";
        std::cout << "─────────────────────────────────────────────────────\n";

        Disruptor<Event, 4096> disruptor;
        TraditionalHandler handler;

        // Same API - DSL detects EventHandler
        disruptor.handleEventsWith(handler);
        disruptor.start();

        for (int64_t i = 0; i < NUM_EVENTS; ++i) {
            disruptor.publishEvent([i](Event& e, int64_t seq) {
                e.value = i;
                e.valid = (i % 100 != 0);  // Every 100th is invalid
            });
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        disruptor.shutdown();
        std::cout << "\n";
    }

    // ─────────────────────────────────────────────────────────────
    // Example 2: NoExcept handler (zero-exception)
    // ─────────────────────────────────────────────────────────────
    {
        std::cout << "Example 2: NoExceptEventHandler\n";
        std::cout << "─────────────────────────────────────────────────────\n";

        Disruptor<Event, 4096> disruptor;
        NoExceptHandler handler;

        // Same API - DSL detects NoExceptEventHandler
        disruptor.handleEventsWith(handler);
        disruptor.start();

        for (int64_t i = 0; i < NUM_EVENTS; ++i) {
            disruptor.publishEvent([i](Event& e, int64_t seq) {
                e.value = i;
                e.valid = (i % 100 != 0);
            });
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        disruptor.shutdown();
        std::cout << "\n";
    }

    // ─────────────────────────────────────────────────────────────
    // Example 3: Stop on error
    // ─────────────────────────────────────────────────────────────
    {
        std::cout << "Example 3: ProcessResult::STOP on error\n";
        std::cout << "─────────────────────────────────────────────────────\n";

        Disruptor<Event, 4096> disruptor;
        StopOnErrorHandler handler;

        disruptor.handleEventsWith(handler);
        disruptor.start();

        // Publish some valid events, then one invalid
        for (int64_t i = 0; i < 100; ++i) {
            disruptor.publishEvent([i](Event& e, int64_t seq) {
                e.value = i;
                e.valid = (i < 50);  // First 50 valid, then invalid
            });
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        disruptor.shutdown();
        std::cout << "\n";
    }

    // ─────────────────────────────────────────────────────────────
    // Example 4: Mixed handlers in pipeline
    // ─────────────────────────────────────────────────────────────
    {
        std::cout << "Example 4: Mixed handlers (Traditional -> NoExcept)\n";
        std::cout << "─────────────────────────────────────────────────────\n";

        Disruptor<Event, 4096> disruptor;

        TraditionalHandler traditional;
        NoExceptHandler noexcept_h;

        // Pipeline: traditional -> noexcept
        // DSL auto-detects each handler type!
        disruptor.handleEventsWith(traditional).then(noexcept_h);

        disruptor.start();

        for (int64_t i = 0; i < NUM_EVENTS; ++i) {
            disruptor.publishEvent([i](Event& e, int64_t seq) {
                e.value = i;
                e.valid = true;
            });
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        disruptor.shutdown();
        std::cout << "\n";
    }

    std::cout << "═══════════════════════════════════════════════════\n";
    std::cout << "                    Done!\n";
    std::cout << "═══════════════════════════════════════════════════\n";

    return 0;
}
