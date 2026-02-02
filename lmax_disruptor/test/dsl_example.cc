/*
 * Disruptor DSL Example
 *
 * Compile: g++ -std=c++20 -O3 -o dsl_example dsl_example.cc -pthread
 * Run: ./dsl_example
 */

#include "../include/lmax/disruptor_dsl.h"
#include <iostream>
#include <chrono>

using namespace lmax;

// Event type
struct OrderEvent {
    int64_t orderId;
    double price;
    int quantity;
};

// Handler 1: Validate order
class ValidationHandler : public EventHandler<OrderEvent> {
public:
    void onEvent(OrderEvent& e, int64_t seq, bool endOfBatch) override {
        // Simulate validation
        validated_++;
    }

    void onStart() override {
        std::cout << "[Validator] Started\n";
    }

    void onShutdown() override {
        std::cout << "[Validator] Shutdown - validated " << validated_ << " orders\n";
    }

private:
    int64_t validated_ = 0;
};

// Handler 2: Log order
class LoggingHandler : public EventHandler<OrderEvent> {
public:
    void onEvent(OrderEvent& e, int64_t seq, bool endOfBatch) override {
        logged_++;
    }

    void onStart() override {
        std::cout << "[Logger] Started\n";
    }

    void onShutdown() override {
        std::cout << "[Logger] Shutdown - logged " << logged_ << " orders\n";
    }

private:
    int64_t logged_ = 0;
};

// Handler 3: Execute order
class ExecutionHandler : public EventHandler<OrderEvent> {
public:
    void onEvent(OrderEvent& e, int64_t seq, bool endOfBatch) override {
        executed_++;
    }

    void onStart() override {
        std::cout << "[Executor] Started\n";
    }

    void onShutdown() override {
        std::cout << "[Executor] Shutdown - executed " << executed_ << " orders\n";
    }

private:
    int64_t executed_ = 0;
};

int main() {
    constexpr int64_t NUM_ORDERS = 100000;

    std::cout << "═══════════════════════════════════════════════════\n";
    std::cout << "           Disruptor DSL Example\n";
    std::cout << "═══════════════════════════════════════════════════\n\n";

    // ─────────────────────────────────────────────────────────────
    // Example 1: Simple Pipeline
    // Validator -> Logger -> Executor
    // ─────────────────────────────────────────────────────────────
    {
        std::cout << "Example 1: Simple Pipeline (Validator -> Logger -> Executor)\n";
        std::cout << "─────────────────────────────────────────────────────────────\n";

        Disruptor<OrderEvent, 4096> disruptor;

        ValidationHandler validator;
        LoggingHandler logger;
        ExecutionHandler executor;

        // Chain: validator -> logger -> executor
        disruptor.handleEventsWith(validator)
                 .then(logger)
                 .then(executor);

        disruptor.start();

        auto startTime = std::chrono::high_resolution_clock::now();

        for (int64_t i = 0; i < NUM_ORDERS; ++i) {
            disruptor.publishEvent([i](OrderEvent& e, int64_t seq) {
                e.orderId = i;
                e.price = 100.0 + (i % 100);
                e.quantity = 1 + (i % 10);
            });
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        disruptor.shutdown();

        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        std::cout << "Processed " << NUM_ORDERS << " orders in " << duration.count() << "ms\n";
        std::cout << "Throughput: " << (NUM_ORDERS * 1000.0 / duration.count() / 1e6) << " M orders/sec\n\n";
    }

    // ─────────────────────────────────────────────────────────────
    // Example 2: Parallel Handlers
    // Validator -> (Logger, Executor) in parallel
    // ─────────────────────────────────────────────────────────────
    {
        std::cout << "Example 2: Parallel Handlers (Validator -> [Logger | Executor])\n";
        std::cout << "─────────────────────────────────────────────────────────────\n";

        Disruptor<OrderEvent, 4096> disruptor;

        ValidationHandler validator;
        LoggingHandler logger;
        ExecutionHandler executor;

        // validator first, then logger and executor in parallel
        disruptor.handleEventsWith(validator)
                 .then(logger, executor);  // Both run in parallel
        
        disruptor.start();

        for (int64_t i = 0; i < NUM_ORDERS; ++i) {
            disruptor.publishEvent([i](OrderEvent& e, int64_t seq) {
                e.orderId = i;
                e.price = 100.0;
                e.quantity = 1;
            });
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        disruptor.shutdown();
        std::cout << "\n";
    }

    // ─────────────────────────────────────────────────────────────
    // Example 3: Diamond Pattern
    // (Validator, Logger) -> Executor
    // ─────────────────────────────────────────────────────────────
    {
        std::cout << "Example 3: Diamond Pattern ([Validator | Logger] -> Executor)\n";
        std::cout << "─────────────────────────────────────────────────────────────\n";

        Disruptor<OrderEvent, 4096> disruptor;

        ValidationHandler validator;
        LoggingHandler logger;
        ExecutionHandler executor;

        // Validator and Logger run in parallel
        auto& g1 = disruptor.handleEventsWith(validator);
        auto& g2 = disruptor.handleEventsWith(logger);

        // Executor waits for both
        disruptor.after(g1, g2).handleEventsWith(executor);

        disruptor.start();

        for (int64_t i = 0; i < NUM_ORDERS; ++i) {
            disruptor.publishEvent([i](OrderEvent& e, int64_t seq) {
                e.orderId = i;
                e.price = 100.0;
                e.quantity = 1;
            });
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        disruptor.shutdown();
        std::cout << "\n";
    }

    // ─────────────────────────────────────────────────────────────
    // Example 4: Batch Publishing
    // ─────────────────────────────────────────────────────────────
    {
        std::cout << "Example 4: Batch Publishing\n";
        std::cout << "─────────────────────────────────────────────────────────────\n";

        Disruptor<OrderEvent, 4096> disruptor;

        ExecutionHandler executor;
        disruptor.handleEventsWith(executor);

        disruptor.start();

        // Publish 100 events at once
        disruptor.publishEvents([](OrderEvent& e, int64_t seq) {
            e.orderId = seq;
            e.price = 100.0;
            e.quantity = 1;
        }, 100);

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        disruptor.shutdown();
        std::cout << "\n";
    }

    std::cout << "═══════════════════════════════════════════════════\n";
    std::cout << "                  All examples done!\n";
    std::cout << "═══════════════════════════════════════════════════\n";

    return 0;
}
