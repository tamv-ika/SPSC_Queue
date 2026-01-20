/*
 * High-Performance LMAX Disruptor for C++
 *
 * Disruptor DSL - Domain Specific Language for easy disruptor setup
 *
 * Provides a fluent API for configuring:
 * - Event handlers
 * - Handler chains (pipelines)
 * - Worker pools
 * - Gating sequences
 */

#pragma once

#include "ring_buffer.h"
#include "batch_event_processor.h"
#include "event_handler.h"
#include "sequence.h"
#include "sequence_barrier.h"
#include <vector>
#include <memory>
#include <functional>
#include <thread>
#include <atomic>

namespace lmax {

/**
 * Handler group - represents a set of handlers that process events in parallel.
 * Used internally by the Disruptor DSL.
 */
template<typename T, typename RingBufferType>
class EventHandlerGroup {
public:
    using BarrierType = typename RingBufferType::barrier_type;
    using SimpleBarrierType = typename RingBufferType::simple_barrier_type;

    EventHandlerGroup(RingBufferType& ringBuffer,
                     std::vector<const Sequence*> sequences)
        : ringBuffer_(ringBuffer)
        , sequences_(std::move(sequences))
    {}

    /**
     * Get the sequences for all handlers in this group.
     * Used for building dependency chains.
     */
    [[nodiscard]] const std::vector<const Sequence*>& getSequences() const noexcept {
        return sequences_;
    }

private:
    RingBufferType& ringBuffer_;
    std::vector<const Sequence*> sequences_;
};

/**
 * Disruptor - Main orchestration class for setting up event processing pipelines.
 *
 * Template parameters:
 * - T: Event type
 * - Size: Ring buffer size (must be power of 2)
 * - WaitStrategy: Wait strategy for the ring buffer
 * - ProducerType: SingleProducerType or MultiProducerType
 *
 * Example usage:
 *   Disruptor<MyEvent, 1024> disruptor;
 *
 *   // Simple chain: handler1 -> handler2 -> handler3
 *   disruptor.handleEventsWith(handler1)
 *           .then(handler2)
 *           .then(handler3);
 *
 *   // Diamond pattern: handler1 -> (handler2a, handler2b) -> handler3
 *   disruptor.handleEventsWith(handler1)
 *           .then(handler2a, handler2b)
 *           .then(handler3);
 *
 *   disruptor.start();
 *   // ... publish events ...
 *   disruptor.shutdown();
 */
template<typename T, size_t Size, typename WaitStrategy = BusySpinWait, typename ProducerType = SingleProducerType>
class Disruptor {
public:
    using RingBufferType = RingBuffer<T, Size, WaitStrategy, ProducerType>;
    using BarrierType = typename RingBufferType::barrier_type;
    using SimpleBarrierType = typename RingBufferType::simple_barrier_type;

    Disruptor() = default;

    // Non-copyable, non-movable
    Disruptor(const Disruptor&) = delete;
    Disruptor& operator=(const Disruptor&) = delete;

    ~Disruptor() {
        shutdown();
    }

    /**
     * Get the ring buffer for publishing events.
     */
    [[nodiscard]] RingBufferType& getRingBuffer() noexcept {
        return ringBuffer_;
    }

    [[nodiscard]] const RingBufferType& getRingBuffer() const noexcept {
        return ringBuffer_;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // HANDLER CONFIGURATION - Fluent API
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Configure the first handler(s) in the pipeline.
     * These handlers depend only on the producer.
     *
     * @param handlers One or more handlers to process events in parallel
     * @return HandlerChain for chaining additional handlers
     */
    template<typename... Handlers>
    class HandlerChain& handleEventsWith(Handlers&... handlers) {
        return createHandlerChain({}, handlers...);
    }

    /**
     * Chain of handlers - allows building complex pipelines.
     */
    class HandlerChain {
    public:
        HandlerChain(Disruptor& disruptor, std::vector<const Sequence*> dependencies)
            : disruptor_(disruptor)
            , dependencies_(std::move(dependencies))
        {}

        /**
         * Add handler(s) that depend on all previous handlers in this chain.
         */
        template<typename... Handlers>
        HandlerChain& then(Handlers&... handlers) {
            return disruptor_.createHandlerChain(dependencies_, handlers...);
        }

        /**
         * Get sequences for handlers in this chain (for custom dependencies).
         */
        [[nodiscard]] const std::vector<const Sequence*>& getSequences() const noexcept {
            return ownSequences_;
        }

        void setOwnSequences(std::vector<const Sequence*> sequences) {
            ownSequences_ = std::move(sequences);
        }

    private:
        Disruptor& disruptor_;
        std::vector<const Sequence*> dependencies_;
        std::vector<const Sequence*> ownSequences_;
    };

    // ═══════════════════════════════════════════════════════════════════════════
    // LIFECYCLE
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Start all event processors.
     */
    void start() {
        if (started_.exchange(true)) {
            return;  // Already started
        }

        // Start all processors
        for (auto& processor : processors_) {
            processor->start();
        }
    }

    /**
     * Shutdown all event processors.
     * Waits for all processors to complete current events.
     */
    void shutdown() {
        if (!started_.load()) {
            return;
        }

        // Halt all processors
        for (auto& processor : processors_) {
            processor->halt();
        }

        // Join all processor threads
        for (auto& processor : processors_) {
            processor->join();
        }

        started_.store(false);
    }

    /**
     * Check if the disruptor has been started.
     */
    [[nodiscard]] bool isStarted() const noexcept {
        return started_.load(std::memory_order_acquire);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // PUBLISHING (convenience methods)
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Publish a single event using a translator function.
     */
    template<typename Translator>
    void publishEvent(Translator&& translator) {
        int64_t sequence = ringBuffer_.next();
        T* event = ringBuffer_.get(sequence);
        translator(*event, sequence);
        ringBuffer_.publish(sequence);
    }

    /**
     * Try to publish a single event (non-blocking).
     */
    template<typename Translator>
    bool tryPublishEvent(Translator&& translator) {
        int64_t sequence = ringBuffer_.tryNext();
        if (sequence == INITIAL_CURSOR_VALUE) {
            return false;
        }
        T* event = ringBuffer_.get(sequence);
        translator(*event, sequence);
        ringBuffer_.publish(sequence);
        return true;
    }

private:
    /**
     * Internal method to create a handler chain with dependencies.
     */
    template<typename Handler, typename... MoreHandlers>
    HandlerChain& createHandlerChain(const std::vector<const Sequence*>& dependencies,
                                     Handler& handler, MoreHandlers&... moreHandlers) {
        std::vector<const Sequence*> newSequences;

        // Create processor for first handler
        createProcessor(handler, dependencies, newSequences);

        // Recursively create processors for remaining handlers
        if constexpr (sizeof...(MoreHandlers) > 0) {
            createHandlerChain(dependencies, moreHandlers...);
            // Get sequences from all handlers created
        }

        // Create and store the chain
        handlerChains_.emplace_back(std::make_unique<HandlerChain>(*this, newSequences));
        handlerChains_.back()->setOwnSequences(newSequences);

        return *handlerChains_.back();
    }

    /**
     * Create a single processor for a handler.
     */
    void createProcessor(EventHandler<T>& handler,
                        const std::vector<const Sequence*>& dependencies,
                        std::vector<const Sequence*>& outSequences) {
        // Create barrier based on dependencies
        if (dependencies.empty()) {
            // No dependencies - create simple barrier on cursor
            auto barrier = std::make_unique<SimpleBarrierType>(ringBuffer_.cursor());
            auto processor = std::make_unique<ProcessorWrapper>(
                ringBuffer_, *barrier, handler);

            outSequences.push_back(&processor->getSequence());

            // Add to gating sequences (so producer waits for this consumer)
            ringBuffer_.addGatingSequence(processor->getSequence());

            barriers_.push_back(std::move(barrier));
            processors_.push_back(std::move(processor));
        } else {
            // Has dependencies - create barrier with dependency sequences
            auto barrier = std::make_unique<BarrierType>(ringBuffer_.cursor());
            for (const Sequence* dep : dependencies) {
                barrier->addDependency(*dep);
            }

            auto processor = std::make_unique<ProcessorWrapper>(
                ringBuffer_, *barrier, handler);

            outSequences.push_back(&processor->getSequence());

            // Add to gating sequences
            ringBuffer_.addGatingSequence(processor->getSequence());

            dependencyBarriers_.push_back(std::move(barrier));
            processors_.push_back(std::move(processor));
        }
    }

    /**
     * Type-erased processor wrapper to store different processor types.
     */
    class ProcessorWrapper {
    public:
        // Constructor for simple barrier
        ProcessorWrapper(RingBufferType& ringBuffer,
                        SimpleBarrierType& barrier,
                        EventHandler<T>& handler)
            : simpleProcessor_(std::make_unique<
                BatchEventProcessor<T, RingBufferType, SimpleBarrierType>>(
                    ringBuffer, barrier, handler))
        {}

        // Constructor for dependency barrier
        ProcessorWrapper(RingBufferType& ringBuffer,
                        BarrierType& barrier,
                        EventHandler<T>& handler)
            : depProcessor_(std::make_unique<
                BatchEventProcessor<T, RingBufferType, BarrierType>>(
                    ringBuffer, barrier, handler))
        {}

        void start() {
            if (simpleProcessor_) {
                simpleProcessor_->start();
            } else if (depProcessor_) {
                depProcessor_->start();
            }
        }

        void halt() {
            if (simpleProcessor_) {
                simpleProcessor_->halt();
            } else if (depProcessor_) {
                depProcessor_->halt();
            }
        }

        void join() {
            if (simpleProcessor_) {
                simpleProcessor_->join();
            } else if (depProcessor_) {
                depProcessor_->join();
            }
        }

        [[nodiscard]] const Sequence& getSequence() const {
            if (simpleProcessor_) {
                return simpleProcessor_->getSequence();
            }
            return depProcessor_->getSequence();
        }

    private:
        std::unique_ptr<BatchEventProcessor<T, RingBufferType, SimpleBarrierType>> simpleProcessor_;
        std::unique_ptr<BatchEventProcessor<T, RingBufferType, BarrierType>> depProcessor_;
    };

    RingBufferType ringBuffer_;
    std::vector<std::unique_ptr<SimpleBarrierType>> barriers_;
    std::vector<std::unique_ptr<BarrierType>> dependencyBarriers_;
    std::vector<std::unique_ptr<ProcessorWrapper>> processors_;
    std::vector<std::unique_ptr<HandlerChain>> handlerChains_;
    std::atomic<bool> started_{false};
};

// ═══════════════════════════════════════════════════════════════════════════
// CONVENIENCE ALIASES
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Single producer disruptor.
 */
template<typename T, size_t Size, typename WaitStrategy = BusySpinWait>
using SPDisruptor = Disruptor<T, Size, WaitStrategy, SingleProducerType>;

/**
 * Multi producer disruptor.
 */
template<typename T, size_t Size, typename WaitStrategy = BusySpinWait>
using MPDisruptor = Disruptor<T, Size, WaitStrategy, MultiProducerType>;

} // namespace lmax
