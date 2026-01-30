/*
 * High-Performance LMAX Disruptor for C++
 *
 * AbstractEventProcessor - Minimal base class for custom processors
 *
 * Only provides thread lifecycle management.
 * You handle everything else: sequences, barriers, processing logic.
 */

#pragma once

#include "event_processor.h"
#include <atomic>
#include <thread>
#include <stdexcept>

namespace lmax {

/**
 * AbstractEventProcessor - Minimal base for custom processors.
 *
 * Provides ONLY:
 * - Thread start/halt/join
 * - Running state check
 *
 * You implement:
 * - getSequence() - Your sequence(s)
 * - run() - Your processing loop
 * - doHalt() - Alert your barriers, signal conditions, etc.
 *
 * Example:
 *
 *   class MyProcessor : public AbstractEventProcessor {
 *       RingBuffer& rb_;
 *       SimpleBarrier barrier_;
 *       Sequence sequence_;
 *
 *   public:
 *       MyProcessor(RingBuffer& rb)
 *           : rb_(rb), barrier_(rb.cursor()) {}
 *
 *       const Sequence& getSequence() const noexcept override {
 *           return sequence_;
 *       }
 *
 *   protected:
 *       void run() override {
 *           int64_t nextSeq = sequence_.get() + 1;
 *
 *           while (isRunning()) {
 *               // Check available - you control the wait logic
 *               int64_t avail = barrier_.available();
 *
 *               if (avail >= nextSeq) {
 *                   // Process events
 *                   while (nextSeq <= avail) {
 *                       auto* event = rb_.get(nextSeq);
 *                       process(*event);
 *                       ++nextSeq;
 *                   }
 *                   sequence_.set(avail);
 *               } else {
 *                   // Your wait strategy
 *                   std::this_thread::yield();
 *               }
 *           }
 *       }
 *
 *       void doHalt() noexcept override {
 *           barrier_.alert();
 *       }
 *   };
 */
class AbstractEventProcessor : public IEventProcessor {
public:
    ~AbstractEventProcessor() override {
        halt();
        join();
    }

    AbstractEventProcessor(const AbstractEventProcessor&) = delete;
    AbstractEventProcessor& operator=(const AbstractEventProcessor&) = delete;

    void start() override {
        if (running_.exchange(true, std::memory_order_acq_rel)) {
            throw std::runtime_error("Processor already running");
        }
        thread_ = std::thread([this] { run(); });
    }

    void halt() noexcept override {
        running_.store(false, std::memory_order_release);
        doHalt();
    }

    void join() override {
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    [[nodiscard]] bool isRunning() const noexcept override {
        return running_.load(std::memory_order_acquire);
    }

protected:
    AbstractEventProcessor() = default;

    /**
     * Your main processing loop.
     * Check isRunning() and exit when false.
     */
    virtual void run() = 0;

    /**
     * Called during halt(). Alert barriers, signal conditions, etc.
     */
    virtual void doHalt() noexcept {}

private:
    alignas(64) std::atomic<bool> running_{false};
    std::thread thread_;
};

} // namespace lmax
