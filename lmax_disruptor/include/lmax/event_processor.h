/*
 * High-Performance LMAX Disruptor for C++
 *
 * IEventProcessor - Virtual interface for event processors
 *
 * Allows different processor types to be managed by the Disruptor DSL
 * with unified lifecycle management.
 */

#pragma once

#include "sequence.h"

namespace lmax {

/**
 * IEventProcessor - Abstract interface for event processors.
 *
 * Implement this interface to create custom processors that can be
 * managed by the Disruptor DSL alongside BatchEventProcessor.
 *
 * Required methods:
 * - getSequence() - Returns processor's sequence for gating/dependencies
 * - start() - Begin processing in a new thread
 * - halt() - Request graceful shutdown
 * - join() - Wait for processor to stop
 * - isRunning() - Check if processor is active
 */
class IEventProcessor {
public:
    virtual ~IEventProcessor() = default;

    /**
     * Get the sequence maintained by this processor.
     * Used for gating (backpressure) and dependency tracking.
     */
    [[nodiscard]] virtual const Sequence& getSequence() const noexcept = 0;

    /**
     * Start the processor (typically spawns a thread).
     */
    virtual void start() = 0;

    /**
     * Request the processor to halt gracefully.
     * Should alert any blocking waits to wake up.
     */
    virtual void halt() noexcept = 0;

    /**
     * Wait for the processor to fully stop.
     */
    virtual void join() = 0;

    /**
     * Check if the processor is currently running.
     */
    [[nodiscard]] virtual bool isRunning() const noexcept = 0;
};

} // namespace lmax
