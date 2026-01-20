/*
 * High-Performance LMAX Disruptor for C++
 *
 * EventHandler - Interface for processing events from the ring buffer
 *
 * Handlers implement onEvent() to process each event.
 * The BatchEventProcessor calls handlers for available events.
 */

#pragma once

#include <cstdint>
#include <functional>

namespace lmax {

/**
 * Interface for handling events from the ring buffer.
 *
 * Template parameter:
 * - T: Event type stored in the ring buffer
 */
template<typename T>
class EventHandler {
public:
    virtual ~EventHandler() = default;

    /**
     * Called when an event becomes available.
     *
     * @param event The event data
     * @param sequence The sequence number of this event
     * @param endOfBatch True if this is the last event in the current batch
     */
    virtual void onEvent(T& event, int64_t sequence, bool endOfBatch) = 0;

    /**
     * Called when the processor starts.
     * Override to perform initialization.
     */
    virtual void onStart() {}

    /**
     * Called when the processor shuts down.
     * Override to perform cleanup.
     */
    virtual void onShutdown() {}

    /**
     * Called when an exception occurs during event processing.
     * Default behavior: rethrow the exception.
     *
     * @param ex The exception that occurred
     * @param sequence The sequence of the event that caused the exception
     * @param event The event that caused the exception
     */
    virtual void onException(std::exception& ex, int64_t sequence, T& event) {
        throw;
    }
};

/**
 * Functional event handler - wraps a lambda/function for convenience.
 *
 * Example:
 *   auto handler = makeFunctionalHandler<MyEvent>([](MyEvent& e, int64_t seq, bool eob) {
 *       process(e);
 *   });
 */
template<typename T>
class FunctionalEventHandler : public EventHandler<T> {
public:
    using HandlerFunc = std::function<void(T&, int64_t, bool)>;

    explicit FunctionalEventHandler(HandlerFunc handler)
        : handler_(std::move(handler)) {}

    void onEvent(T& event, int64_t sequence, bool endOfBatch) override {
        handler_(event, sequence, endOfBatch);
    }

private:
    HandlerFunc handler_;
};

/**
 * Helper to create a functional handler from a lambda.
 */
template<typename T, typename Func>
FunctionalEventHandler<T> makeFunctionalHandler(Func&& func) {
    return FunctionalEventHandler<T>(std::forward<Func>(func));
}

/**
 * Work handler interface - for work pools where each event is processed by one worker.
 *
 * Unlike EventHandler, WorkHandler doesn't receive endOfBatch notifications
 * since events are distributed among workers.
 */
template<typename T>
class WorkHandler {
public:
    virtual ~WorkHandler() = default;

    /**
     * Process an event.
     *
     * @param event The event to process
     */
    virtual void onEvent(T& event) = 0;

    /**
     * Called when the worker starts.
     */
    virtual void onStart() {}

    /**
     * Called when the worker shuts down.
     */
    virtual void onShutdown() {}
};

/**
 * Lifecycle aware interface - for handlers that need start/shutdown notifications.
 */
class LifecycleAware {
public:
    virtual ~LifecycleAware() = default;

    virtual void onStart() = 0;
    virtual void onShutdown() = 0;
};

/**
 * Timeout handler - for handlers that need periodic timeout notifications.
 */
template<typename T>
class TimeoutHandler {
public:
    virtual ~TimeoutHandler() = default;

    /**
     * Called when a timeout occurs while waiting for events.
     *
     * @param sequence The sequence we were waiting for
     */
    virtual void onTimeout(int64_t sequence) = 0;
};

/**
 * Exception handler for processors.
 */
template<typename T>
class ExceptionHandler {
public:
    virtual ~ExceptionHandler() = default;

    /**
     * Handle an exception during event processing.
     *
     * @param ex The exception
     * @param sequence The sequence that caused the exception
     * @param event The event being processed
     */
    virtual void handleEventException(std::exception& ex, int64_t sequence, T& event) = 0;

    /**
     * Handle an exception during startup.
     */
    virtual void handleOnStartException(std::exception& ex) = 0;

    /**
     * Handle an exception during shutdown.
     */
    virtual void handleOnShutdownException(std::exception& ex) = 0;
};

/**
 * Default exception handler - logs and continues.
 */
template<typename T>
class IgnoreExceptionHandler : public ExceptionHandler<T> {
public:
    void handleEventException(std::exception& ex, int64_t sequence, T& event) override {
        // Ignore - production code should log this
    }

    void handleOnStartException(std::exception& ex) override {
        // Ignore
    }

    void handleOnShutdownException(std::exception& ex) override {
        // Ignore
    }
};

/**
 * Fatal exception handler - rethrows all exceptions.
 */
template<typename T>
class FatalExceptionHandler : public ExceptionHandler<T> {
public:
    void handleEventException(std::exception& ex, int64_t sequence, T& event) override {
        throw;
    }

    void handleOnStartException(std::exception& ex) override {
        throw;
    }

    void handleOnShutdownException(std::exception& ex) override {
        throw;
    }
};

} // namespace lmax
