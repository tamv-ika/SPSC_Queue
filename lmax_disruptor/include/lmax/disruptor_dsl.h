/*
 * High-Performance LMAX Disruptor for C++
 *
 * Disruptor DSL - Domain Specific Language for easy disruptor setup
 *
 * Provides a fluent API for configuring:
 * - Event handlers and handler chains (pipelines)
 * - Parallel handlers (diamond patterns)
 * - Explicit dependencies with after()
 * - Automatic gating sequence management
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
#include <set>

namespace lmax {

// Forward declarations
template<typename T, size_t Size, typename WaitStrategy, typename ProducerType>
class Disruptor;

/**
 * HandlerGroup - Represents handlers added in a single handleEventsWith/then call.
 * Used for building dependency chains with then() and after().
 */
template<typename T, size_t Size, typename WaitStrategy, typename ProducerType>
class HandlerGroup {
public:
    using DisruptorType = Disruptor<T, Size, WaitStrategy, ProducerType>;

    HandlerGroup(DisruptorType& disruptor, std::vector<const Sequence*> sequences)
        : disruptor_(disruptor)
        , sequences_(std::move(sequences))
    {}

    /**
     * Add handler(s) that depend on ALL handlers in this group.
     *
     * Example:
     *   disruptor.handleEventsWith(h1).then(h2);  // h2 waits for h1
     *   disruptor.handleEventsWith(h1, h2).then(h3);  // h3 waits for both h1 AND h2
     */
    template<typename... Handlers>
    HandlerGroup& then(Handlers&... handlers);

    /**
     * Add handler(s) that depend on specific handler groups.
     *
     * Example:
     *   auto g1 = disruptor.handleEventsWith(h1);
     *   auto g2 = disruptor.handleEventsWith(h2);
     *   disruptor.after(g1, g2).handleEventsWith(h3);  // h3 waits for h1 AND h2
     */
    template<typename... Handlers>
    HandlerGroup& handleEventsWith(Handlers&... handlers);

    /**
     * Get sequences for all handlers in this group.
     */
    [[nodiscard]] const std::vector<const Sequence*>& getSequences() const noexcept {
        return sequences_;
    }

private:
    DisruptorType& disruptor_;
    std::vector<const Sequence*> sequences_;
};

/**
 * Disruptor - Main orchestration class for setting up event processing pipelines.
 *
 * Example usage:
 *
 *   // Simple chain: h1 -> h2 -> h3
 *   Disruptor<Event, 1024> d;
 *   d.handleEventsWith(h1).then(h2).then(h3);
 *   d.start();
 *
 *   // Parallel handlers: h1 -> (h2a, h2b) -> h3
 *   d.handleEventsWith(h1).then(h2a, h2b).then(h3);
 *
 *   // Diamond pattern with after():
 *   auto g1 = d.handleEventsWith(h1);
 *   auto g2 = d.handleEventsWith(h2);
 *   d.after(g1, g2).handleEventsWith(h3);
 */
template<typename T, size_t Size, typename WaitStrategy = BusySpinWait, typename ProducerType = SingleProducerType>
class Disruptor {
public:
    using RingBufferType = RingBuffer<T, Size, WaitStrategy, ProducerType>;
    using BarrierType = typename RingBufferType::barrier_type;
    using SimpleBarrierType = typename RingBufferType::simple_barrier_type;
    using HandlerGroupType = HandlerGroup<T, Size, WaitStrategy, ProducerType>;

    Disruptor() = default;

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
    // HANDLER CONFIGURATION
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Configure handler(s) that depend only on the producer.
     * Multiple handlers will process events in parallel.
     */
    template<typename... Handlers>
    HandlerGroupType& handleEventsWith(Handlers&... handlers) {
        std::vector<const Sequence*> dependencies;  // Empty = depends on cursor only
        return createHandlerGroup(dependencies, handlers...);
    }

    /**
     * Create a dependency specification for multiple handler groups.
     * Use with handleEventsWith() to create handlers that depend on all specified groups.
     *
     * Example:
     *   auto g1 = d.handleEventsWith(h1);
     *   auto g2 = d.handleEventsWith(h2);
     *   d.after(g1, g2).handleEventsWith(h3);
     */
    HandlerGroupType& after(const HandlerGroupType& group) {
        std::vector<const Sequence*> deps = group.getSequences();
        afterDependencies_ = std::move(deps);
        return afterGroup();
    }

    template<typename... Groups>
    HandlerGroupType& after(const HandlerGroupType& first, const Groups&... rest) {
        std::vector<const Sequence*> deps = first.getSequences();
        collectDependencies(deps, rest...);
        afterDependencies_ = std::move(deps);
        return afterGroup();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // LIFECYCLE
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Start all event processors.
     */
    void start() {
        if (started_.exchange(true)) {
            return;
        }

        // Update gating sequences - only gate on consumers that have no dependents
        updateGatingSequences();

        for (auto& processor : processors_) {
            processor->start();
        }
    }

    /**
     * Shutdown all event processors gracefully.
     */
    void shutdown() {
        if (!started_.load()) {
            return;
        }

        for (auto& processor : processors_) {
            processor->halt();
        }

        for (auto& processor : processors_) {
            processor->join();
        }

        started_.store(false);
    }

    /**
     * Check if disruptor is running.
     */
    [[nodiscard]] bool isRunning() const noexcept {
        return started_.load(std::memory_order_acquire);
    }

    /**
     * Get cursor value (last published sequence).
     */
    [[nodiscard]] int64_t getCursor() const noexcept {
        return ringBuffer_.getCursor();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // PUBLISHING
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Publish event using translator (blocking).
     */
    template<typename Translator>
    void publishEvent(Translator&& translator) {
        int64_t seq = ringBuffer_.next();
        T* event = ringBuffer_.get(seq);
        translator(*event, seq);
        ringBuffer_.publish(seq);
    }

    /**
     * Try to publish event (non-blocking).
     */
    template<typename Translator>
    bool tryPublishEvent(Translator&& translator) {
        int64_t seq = ringBuffer_.tryNext();
        if (seq == INITIAL_CURSOR_VALUE) {
            return false;
        }
        T* event = ringBuffer_.get(seq);
        translator(*event, seq);
        ringBuffer_.publish(seq);
        return true;
    }

    /**
     * Publish batch of events.
     */
    template<typename Translator>
    void publishEvents(Translator&& translator, size_t count) {
        int64_t hiSeq = ringBuffer_.next(count);
        int64_t loSeq = hiSeq - count + 1;
        for (int64_t seq = loSeq; seq <= hiSeq; seq++) {
            T* event = ringBuffer_.get(seq);
            translator(*event, seq);
        }
        ringBuffer_.publish(loSeq, hiSeq);
    }

private:
    friend class HandlerGroup<T, Size, WaitStrategy, ProducerType>;

    /**
     * Type-erased processor wrapper.
     */
    class ProcessorWrapper {
    public:
        ProcessorWrapper(RingBufferType& ringBuffer,
                        SimpleBarrierType& barrier,
                        EventHandler<T>& handler)
            : simpleProcessor_(std::make_unique<
                BatchEventProcessor<T, RingBufferType, SimpleBarrierType>>(
                    ringBuffer, barrier, handler))
        {}

        ProcessorWrapper(RingBufferType& ringBuffer,
                        BarrierType& barrier,
                        EventHandler<T>& handler)
            : depProcessor_(std::make_unique<
                BatchEventProcessor<T, RingBufferType, BarrierType>>(
                    ringBuffer, barrier, handler))
        {}

        void start() {
            if (simpleProcessor_) simpleProcessor_->start();
            else if (depProcessor_) depProcessor_->start();
        }

        void halt() {
            if (simpleProcessor_) simpleProcessor_->halt();
            else if (depProcessor_) depProcessor_->halt();
        }

        void join() {
            if (simpleProcessor_) simpleProcessor_->join();
            else if (depProcessor_) depProcessor_->join();
        }

        [[nodiscard]] const Sequence& getSequence() const {
            if (simpleProcessor_) return simpleProcessor_->getSequence();
            return depProcessor_->getSequence();
        }

    private:
        std::unique_ptr<BatchEventProcessor<T, RingBufferType, SimpleBarrierType>> simpleProcessor_;
        std::unique_ptr<BatchEventProcessor<T, RingBufferType, BarrierType>> depProcessor_;
    };

    /**
     * Create handler group with dependencies.
     */
    template<typename Handler>
    HandlerGroupType& createHandlerGroup(const std::vector<const Sequence*>& dependencies,
                                          Handler& handler) {
        std::vector<const Sequence*> sequences;
        createProcessor(handler, dependencies, sequences);

        handlerGroups_.emplace_back(
            std::make_unique<HandlerGroupType>(*this, std::move(sequences)));
        return *handlerGroups_.back();
    }

    template<typename Handler, typename... MoreHandlers>
    HandlerGroupType& createHandlerGroup(const std::vector<const Sequence*>& dependencies,
                                          Handler& handler, MoreHandlers&... more) {
        std::vector<const Sequence*> sequences;

        // Create processor for this handler
        createProcessor(handler, dependencies, sequences);

        // Create processors for remaining handlers (all with same dependencies)
        createProcessors(dependencies, sequences, more...);

        handlerGroups_.emplace_back(
            std::make_unique<HandlerGroupType>(*this, std::move(sequences)));
        return *handlerGroups_.back();
    }

    template<typename Handler>
    void createProcessors(const std::vector<const Sequence*>& dependencies,
                          std::vector<const Sequence*>& sequences,
                          Handler& handler) {
        createProcessor(handler, dependencies, sequences);
    }

    template<typename Handler, typename... MoreHandlers>
    void createProcessors(const std::vector<const Sequence*>& dependencies,
                          std::vector<const Sequence*>& sequences,
                          Handler& handler, MoreHandlers&... more) {
        createProcessor(handler, dependencies, sequences);
        createProcessors(dependencies, sequences, more...);
    }

    /**
     * Create a single processor.
     */
    void createProcessor(EventHandler<T>& handler,
                        const std::vector<const Sequence*>& dependencies,
                        std::vector<const Sequence*>& outSequences) {
        if (dependencies.empty()) {
            auto barrier = std::make_unique<SimpleBarrierType>(ringBuffer_.cursor());
            auto processor = std::make_unique<ProcessorWrapper>(
                ringBuffer_, *barrier, handler);

            const Sequence* seq = &processor->getSequence();
            outSequences.push_back(seq);
            allSequences_.push_back(seq);

            barriers_.push_back(std::move(barrier));
            processors_.push_back(std::move(processor));
        } else {
            auto barrier = std::make_unique<BarrierType>(ringBuffer_.cursor());
            for (const Sequence* dep : dependencies) {
                barrier->addDependency(*dep);
                // Track that 'dep' has a dependent
                sequencesWithDependents_.insert(dep);
            }

            auto processor = std::make_unique<ProcessorWrapper>(
                ringBuffer_, *barrier, handler);

            const Sequence* seq = &processor->getSequence();
            outSequences.push_back(seq);
            allSequences_.push_back(seq);

            dependencyBarriers_.push_back(std::move(barrier));
            processors_.push_back(std::move(processor));
        }
    }

    /**
     * Update gating sequences - only gate on final consumers (those with no dependents).
     */
    void updateGatingSequences() {
        for (const Sequence* seq : allSequences_) {
            if (sequencesWithDependents_.find(seq) == sequencesWithDependents_.end()) {
                // This sequence has no dependents - it's a final consumer
                ringBuffer_.addGatingSequence(*seq);
            }
        }
    }

    /**
     * Helper for after() - returns a group that uses stored dependencies.
     */
    HandlerGroupType& afterGroup() {
        // Create a temporary group that will be used for the next handleEventsWith call
        handlerGroups_.emplace_back(
            std::make_unique<HandlerGroupType>(*this, afterDependencies_));
        return *handlerGroups_.back();
    }

    /**
     * Collect dependencies from multiple groups.
     */
    template<typename... Groups>
    void collectDependencies(std::vector<const Sequence*>& deps, const HandlerGroupType& group, const Groups&... rest) {
        for (const Sequence* seq : group.getSequences()) {
            deps.push_back(seq);
        }
        if constexpr (sizeof...(rest) > 0) {
            collectDependencies(deps, rest...);
        }
    }

    RingBufferType ringBuffer_;
    std::vector<std::unique_ptr<SimpleBarrierType>> barriers_;
    std::vector<std::unique_ptr<BarrierType>> dependencyBarriers_;
    std::vector<std::unique_ptr<ProcessorWrapper>> processors_;
    std::vector<std::unique_ptr<HandlerGroupType>> handlerGroups_;
    std::vector<const Sequence*> allSequences_;
    std::set<const Sequence*> sequencesWithDependents_;
    std::vector<const Sequence*> afterDependencies_;
    std::atomic<bool> started_{false};
};

// ═══════════════════════════════════════════════════════════════════════════
// HandlerGroup method implementations
// ═══════════════════════════════════════════════════════════════════════════

template<typename T, size_t Size, typename WaitStrategy, typename ProducerType>
template<typename... Handlers>
HandlerGroup<T, Size, WaitStrategy, ProducerType>&
HandlerGroup<T, Size, WaitStrategy, ProducerType>::then(Handlers&... handlers) {
    return disruptor_.createHandlerGroup(sequences_, handlers...);
}

template<typename T, size_t Size, typename WaitStrategy, typename ProducerType>
template<typename... Handlers>
HandlerGroup<T, Size, WaitStrategy, ProducerType>&
HandlerGroup<T, Size, WaitStrategy, ProducerType>::handleEventsWith(Handlers&... handlers) {
    return disruptor_.createHandlerGroup(sequences_, handlers...);
}

// ═══════════════════════════════════════════════════════════════════════════
// CONVENIENCE ALIASES
// ═══════════════════════════════════════════════════════════════════════════

template<typename T, size_t Size, typename WaitStrategy = BusySpinWait>
using SPDisruptor = Disruptor<T, Size, WaitStrategy, SingleProducerType>;

template<typename T, size_t Size, typename WaitStrategy = BusySpinWait>
using MPDisruptor = Disruptor<T, Size, WaitStrategy, MultiProducerType>;

} // namespace lmax
