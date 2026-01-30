/*
 * High-Performance LMAX Disruptor for C++
 *
 * BatchEventProcessor - Processes events in batches from a ring buffer
 *
 * This is the main consumer component that:
 * - Waits for events using a SequenceBarrier
 * - Processes available events in batches
 * - Maintains a sequence tracking consumed events
 * - Runs in its own thread
 */

#pragma once

#include "sequence.h"
#include "sequence_barrier.h"
#include "event_handler.h"
#include "event_processor.h"
#include <atomic>
#include <thread>
#include <memory>
#include <stdexcept>

namespace lmax {

/**
 * Processor state enumeration.
 */
enum class ProcessorState {
    IDLE,       // Not started
    RUNNING,    // Processing events
    HALTING,    // Stop requested, finishing current batch
    HALTED      // Stopped
};

/**
 * BatchEventProcessor - Consumes and processes events from a ring buffer.
 *
 * Template parameters:
 * - T: Event type
 * - RingBufferType: Type of ring buffer (determines WaitStrategy, etc.)
 * - BarrierType: Type of sequence barrier (SimpleBarrier or SequenceBarrier)
 *
 * Usage:
 *   RingBuffer<Event, 1024> ringBuffer;
 *   MyEventHandler handler;
 *   auto barrier = ringBuffer.newBarrier();
 *   BatchEventProcessor processor(ringBuffer, barrier, handler);
 *   processor.start();
 *   // ... publish events ...
 *   processor.halt();
 */
template<typename T, typename RingBufferType, typename BarrierType>
class BatchEventProcessor : public IEventProcessor {
public:
    /**
     * Construct a batch event processor.
     *
     * @param ringBuffer The ring buffer to consume from
     * @param barrier The barrier to wait on
     * @param handler The handler to process events
     */
    BatchEventProcessor(RingBufferType& ringBuffer,
                       BarrierType& barrier,
                       EventHandler<T>& handler) noexcept
        : ringBuffer_(ringBuffer)
        , barrier_(barrier)
        , handler_(handler)
        , state_(ProcessorState::IDLE)
    {}

    // Non-copyable, non-movable
    BatchEventProcessor(const BatchEventProcessor&) = delete;
    BatchEventProcessor& operator=(const BatchEventProcessor&) = delete;

    ~BatchEventProcessor() {
        halt();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    /**
     * Get the sequence maintained by this processor.
     * This represents the last successfully processed event.
     */
    [[nodiscard]] const Sequence& getSequence() const noexcept override {
        return sequence_;
    }

    /**
     * Get the current processor state.
     */
    [[nodiscard]] ProcessorState getState() const noexcept {
        return state_.load(std::memory_order_acquire);
    }

    /**
     * Check if the processor is running.
     */
    [[nodiscard]] bool isRunning() const noexcept override {
        return state_.load(std::memory_order_acquire) == ProcessorState::RUNNING;
    }

    /**
     * Start the processor in a new thread.
     * Throws if already running.
     */
    void start() override {
        ProcessorState expected = ProcessorState::IDLE;
        if (!state_.compare_exchange_strong(expected, ProcessorState::RUNNING,
                                           std::memory_order_acq_rel)) {
            if (expected == ProcessorState::RUNNING) {
                throw std::runtime_error("Processor is already running");
            }
            if (expected == ProcessorState::HALTING) {
                throw std::runtime_error("Processor is shutting down");
            }
        }

        thread_ = std::thread([this]() { run(); });
    }

    /**
     * Request the processor to halt after completing current batch.
     * Alerts the barrier to interrupt any blocking waitFor() call.
     */
    void halt() noexcept override {
        ProcessorState expected = ProcessorState::RUNNING;
        if (state_.compare_exchange_strong(expected, ProcessorState::HALTING,
                                          std::memory_order_acq_rel)) {
            // Alert the barrier to wake up from waitFor()
            barrier_.alert();
        }
    }

    /**
     * Wait for the processor to stop.
     */
    void join() override {
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    /**
     * Run the processor inline (blocking).
     * Use this for manual control instead of start().
     */
    void run() {
        handler_.onStart();

        int64_t nextSequence = sequence_.get() + 1;

        while (state_.load(std::memory_order_acquire) == ProcessorState::RUNNING) {
            int64_t availableSequence = barrier_.waitFor(nextSequence);

            if (availableSequence == ALERTED) {
                break;
            }

            while (nextSequence <= availableSequence) {
                T* event = const_cast<T*>(ringBuffer_.get(nextSequence));
                bool endOfBatch = (nextSequence == availableSequence);

                try {
                    handler_.onEvent(*event, nextSequence, endOfBatch);
                } catch (std::exception& ex) {
                    handler_.onException(ex, nextSequence, *event);
                }

                ++nextSequence;
            }

            sequence_.set(availableSequence);
        }

        state_.store(ProcessorState::HALTED, std::memory_order_release);
        handler_.onShutdown();
    }

private:
    RingBufferType& ringBuffer_;
    BarrierType& barrier_;
    EventHandler<T>& handler_;

    // Cache-line aligned sequence for this processor
    alignas(64) Sequence sequence_;

    std::atomic<ProcessorState> state_;
    std::thread thread_;
};

/**
 * Helper to create a BatchEventProcessor with type deduction.
 */
template<typename T, typename RingBufferType, typename BarrierType>
auto makeBatchProcessor(RingBufferType& ringBuffer,
                        BarrierType& barrier,
                        EventHandler<T>& handler) {
    return BatchEventProcessor<T, RingBufferType, BarrierType>(ringBuffer, barrier, handler);
}

/**
 * NoOpEventProcessor - A processor that does nothing.
 * Useful for testing or as a placeholder.
 */
template<typename T>
class NoOpEventHandler : public EventHandler<T> {
public:
    void onEvent(T& event, int64_t sequence, bool endOfBatch) override {
        // Do nothing
    }
};

} // namespace lmax
