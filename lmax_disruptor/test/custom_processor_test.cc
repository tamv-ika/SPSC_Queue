/*
 * Test for custom event processors with Disruptor DSL
 *
 * Demonstrates:
 * 1. Creating custom processors that implement IEventProcessor
 * 2. Adding custom processors to the DSL
 * 3. Mixing BatchEventProcessor with custom processors
 */

#include "../include/lmax/disruptor_dsl.h"
#include <iostream>
#include <atomic>
#include <chrono>
#include <cassert>

using namespace lmax;

// ═══════════════════════════════════════════════════════════════════════════
// TEST EVENT
// ═══════════════════════════════════════════════════════════════════════════

struct TestEvent {
    int64_t value;
    int64_t timestamp;
};

// ═══════════════════════════════════════════════════════════════════════════
// CUSTOM PROCESSOR: Lambda-based processor
// ═══════════════════════════════════════════════════════════════════════════

template<typename T, typename RingBufferType, typename BarrierType>
class LambdaProcessor : public IEventProcessor {
public:
    using Callback = std::function<void(const T&, int64_t, bool)>;

    LambdaProcessor(RingBufferType& ringBuffer,
                    BarrierType& barrier,
                    Callback callback) noexcept
        : ringBuffer_(ringBuffer)
        , barrier_(barrier)
        , callback_(std::move(callback))
    {}

    ~LambdaProcessor() override {
        halt();
        join();
    }

    [[nodiscard]] const Sequence& getSequence() const noexcept override {
        return sequence_;
    }

    void start() override {
        running_.store(true, std::memory_order_release);
        thread_ = std::thread([this] { run(); });
    }

    void halt() noexcept override {
        running_.store(false, std::memory_order_release);
        barrier_.alert();
    }

    void join() override {
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    [[nodiscard]] bool isRunning() const noexcept override {
        return running_.load(std::memory_order_acquire);
    }

private:
    void run() {
        int64_t nextSequence = sequence_.get() + 1;

        while (running_.load(std::memory_order_acquire)) {
            int64_t availableSequence = barrier_.waitFor(nextSequence);

            if (availableSequence == ALERTED) {
                break;
            }

            while (nextSequence <= availableSequence) {
                const T* event = ringBuffer_.get(nextSequence);
                bool endOfBatch = (nextSequence == availableSequence);
                callback_(*event, nextSequence, endOfBatch);
                ++nextSequence;
            }

            sequence_.set(availableSequence);
        }
    }

    RingBufferType& ringBuffer_;
    BarrierType& barrier_;
    Callback callback_;
    alignas(64) Sequence sequence_;
    std::atomic<bool> running_{false};
    std::thread thread_;
};

// ═══════════════════════════════════════════════════════════════════════════
// CUSTOM PROCESSOR: Sampling processor (processes every Nth event)
// ═══════════════════════════════════════════════════════════════════════════

template<typename T, typename RingBufferType, typename BarrierType>
class SamplingProcessor : public IEventProcessor {
public:
    using Callback = std::function<void(const T&, int64_t)>;

    SamplingProcessor(RingBufferType& ringBuffer,
                      BarrierType& barrier,
                      Callback callback,
                      int64_t sampleRate) noexcept
        : ringBuffer_(ringBuffer)
        , barrier_(barrier)
        , callback_(std::move(callback))
        , sampleRate_(sampleRate)
    {}

    ~SamplingProcessor() override {
        halt();
        join();
    }

    [[nodiscard]] const Sequence& getSequence() const noexcept override {
        return sequence_;
    }

    void start() override {
        running_.store(true, std::memory_order_release);
        thread_ = std::thread([this] { run(); });
    }

    void halt() noexcept override {
        running_.store(false, std::memory_order_release);
        barrier_.alert();
    }

    void join() override {
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    [[nodiscard]] bool isRunning() const noexcept override {
        return running_.load(std::memory_order_acquire);
    }

    [[nodiscard]] int64_t getSampledCount() const noexcept {
        return sampledCount_.load(std::memory_order_acquire);
    }

private:
    void run() {
        int64_t nextSequence = sequence_.get() + 1;

        while (running_.load(std::memory_order_acquire)) {
            int64_t availableSequence = barrier_.waitFor(nextSequence);

            if (availableSequence == ALERTED) {
                break;
            }

            while (nextSequence <= availableSequence) {
                if (nextSequence % sampleRate_ == 0) {
                    const T* event = ringBuffer_.get(nextSequence);
                    callback_(*event, nextSequence);
                    sampledCount_.fetch_add(1, std::memory_order_relaxed);
                }
                ++nextSequence;
            }

            sequence_.set(availableSequence);
        }
    }

    RingBufferType& ringBuffer_;
    BarrierType& barrier_;
    Callback callback_;
    int64_t sampleRate_;
    alignas(64) Sequence sequence_;
    std::atomic<bool> running_{false};
    std::atomic<int64_t> sampledCount_{0};
    std::thread thread_;
};

// ═══════════════════════════════════════════════════════════════════════════
// TEST 1: Custom processor standalone with DSL
// ═══════════════════════════════════════════════════════════════════════════

void testCustomProcessorStandalone() {
    std::cout << "Test 1: Custom processor standalone with DSL\n";

    constexpr size_t BUFFER_SIZE = 1024;
    constexpr int64_t NUM_EVENTS = 10000;

    using DisruptorType = Disruptor<TestEvent, BUFFER_SIZE>;
    using RingBufferType = typename DisruptorType::RingBufferType;
    using SimpleBarrierType = typename DisruptorType::SimpleBarrierType;

    DisruptorType disruptor;
    auto& ringBuffer = disruptor.getRingBuffer();

    std::atomic<int64_t> processedCount{0};

    // Create barrier for custom processor
    auto barrier = std::make_unique<SimpleBarrierType>(ringBuffer.cursor());
    SimpleBarrierType& barrierRef = *barrier;

    // Create custom lambda processor
    auto processor = std::make_unique<LambdaProcessor<TestEvent, RingBufferType, SimpleBarrierType>>(
        ringBuffer,
        barrierRef,
        [&processedCount](const TestEvent& event, int64_t seq, bool endOfBatch) {
            processedCount.fetch_add(1, std::memory_order_relaxed);
        }
    );

    // Add processor to DSL (DSL takes ownership)
    disruptor.addProcessor(std::move(processor), true);

    // Need to keep barrier alive - store it somewhere
    // Note: In real usage, you'd manage this more elegantly

    disruptor.start();

    // Publish events
    for (int64_t i = 0; i < NUM_EVENTS; ++i) {
        disruptor.publishEvent([i](TestEvent& event, int64_t seq) {
            event.value = i;
            event.timestamp = seq;
        });
    }

    // Wait for processing
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    disruptor.shutdown();

    std::cout << "  Processed: " << processedCount.load() << " / " << NUM_EVENTS << "\n";
    assert(processedCount.load() == NUM_EVENTS);
    std::cout << "  PASSED\n\n";
}

// ═══════════════════════════════════════════════════════════════════════════
// TEST 2: Mix BatchEventProcessor with custom processor
// ═══════════════════════════════════════════════════════════════════════════

class CountingHandler : public EventHandler<TestEvent> {
public:
    void onEvent(TestEvent& event, int64_t sequence, bool endOfBatch) override {
        count_.fetch_add(1, std::memory_order_relaxed);
    }

    [[nodiscard]] int64_t getCount() const noexcept {
        return count_.load(std::memory_order_acquire);
    }

private:
    std::atomic<int64_t> count_{0};
};

void testMixedProcessors() {
    std::cout << "Test 2: Mix BatchEventProcessor with custom processor\n";

    constexpr size_t BUFFER_SIZE = 1024;
    constexpr int64_t NUM_EVENTS = 10000;

    using DisruptorType = Disruptor<TestEvent, BUFFER_SIZE>;
    using RingBufferType = typename DisruptorType::RingBufferType;
    using SimpleBarrierType = typename DisruptorType::SimpleBarrierType;

    DisruptorType disruptor;
    auto& ringBuffer = disruptor.getRingBuffer();

    // Standard handler via DSL
    CountingHandler handler1;
    disruptor.handleEventsWith(handler1);

    // Custom processor
    auto barrier = std::make_unique<SimpleBarrierType>(ringBuffer.cursor());
    SimpleBarrierType& barrierRef = *barrier;

    std::atomic<int64_t> customCount{0};
    auto customProcessor = std::make_unique<LambdaProcessor<TestEvent, RingBufferType, SimpleBarrierType>>(
        ringBuffer,
        barrierRef,
        [&customCount](const TestEvent& event, int64_t seq, bool endOfBatch) {
            customCount.fetch_add(1, std::memory_order_relaxed);
        }
    );

    disruptor.addProcessor(std::move(customProcessor), true);

    disruptor.start();

    // Publish events
    for (int64_t i = 0; i < NUM_EVENTS; ++i) {
        disruptor.publishEvent([i](TestEvent& event, int64_t seq) {
            event.value = i;
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    disruptor.shutdown();

    std::cout << "  BatchEventProcessor count: " << handler1.getCount() << "\n";
    std::cout << "  Custom processor count: " << customCount.load() << "\n";
    assert(handler1.getCount() == NUM_EVENTS);
    assert(customCount.load() == NUM_EVENTS);
    std::cout << "  PASSED\n\n";
}

// ═══════════════════════════════════════════════════════════════════════════
// TEST 3: Sampling processor
// ═══════════════════════════════════════════════════════════════════════════

void testSamplingProcessor() {
    std::cout << "Test 3: Sampling processor (every 100th event)\n";

    constexpr size_t BUFFER_SIZE = 1024;
    constexpr int64_t NUM_EVENTS = 10000;
    constexpr int64_t SAMPLE_RATE = 100;

    using DisruptorType = Disruptor<TestEvent, BUFFER_SIZE>;
    using RingBufferType = typename DisruptorType::RingBufferType;
    using SimpleBarrierType = typename DisruptorType::SimpleBarrierType;

    DisruptorType disruptor;
    auto& ringBuffer = disruptor.getRingBuffer();

    auto barrier = std::make_unique<SimpleBarrierType>(ringBuffer.cursor());
    SimpleBarrierType& barrierRef = *barrier;

    std::atomic<int64_t> sampledSum{0};
    auto sampler = std::make_unique<SamplingProcessor<TestEvent, RingBufferType, SimpleBarrierType>>(
        ringBuffer,
        barrierRef,
        [&sampledSum](const TestEvent& event, int64_t seq) {
            sampledSum.fetch_add(event.value, std::memory_order_relaxed);
        },
        SAMPLE_RATE
    );

    auto* samplerPtr = sampler.get();
    disruptor.addProcessor(std::move(sampler), true);

    disruptor.start();

    for (int64_t i = 0; i < NUM_EVENTS; ++i) {
        disruptor.publishEvent([i](TestEvent& event, int64_t seq) {
            event.value = i;
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    disruptor.shutdown();

    int64_t expectedSamples = NUM_EVENTS / SAMPLE_RATE;
    std::cout << "  Total events: " << NUM_EVENTS << "\n";
    std::cout << "  Sample rate: 1/" << SAMPLE_RATE << "\n";
    std::cout << "  Sampled count: " << samplerPtr->getSampledCount() << "\n";
    std::cout << "  Expected samples: " << expectedSamples << "\n";

    assert(samplerPtr->getSampledCount() == expectedSamples);
    std::cout << "  PASSED\n\n";
}

// ═══════════════════════════════════════════════════════════════════════════
// TEST 4: Pipeline with custom processor in the middle
// ═══════════════════════════════════════════════════════════════════════════

void testPipelineWithCustomProcessor() {
    std::cout << "Test 4: Pipeline: Handler1 -> CustomProcessor\n";

    constexpr size_t BUFFER_SIZE = 1024;
    constexpr int64_t NUM_EVENTS = 5000;

    using DisruptorType = Disruptor<TestEvent, BUFFER_SIZE>;
    using RingBufferType = typename DisruptorType::RingBufferType;
    using BarrierType = typename DisruptorType::BarrierType;

    DisruptorType disruptor;
    auto& ringBuffer = disruptor.getRingBuffer();

    // Stage 1: BatchEventProcessor
    CountingHandler handler1;
    auto& group1 = disruptor.handleEventsWith(handler1);

    // Get handler1's sequence from the group
    const Sequence* handler1Seq = group1.getSequences()[0];

    // Stage 2: Custom processor depends on handler1
    auto barrier = std::make_unique<BarrierType>(ringBuffer.cursor());
    barrier->addDependency(*handler1Seq);  // Wait for handler1
    BarrierType& barrierRef = *barrier;

    std::atomic<int64_t> customCount{0};
    auto customProcessor = std::make_unique<LambdaProcessor<TestEvent, RingBufferType, BarrierType>>(
        ringBuffer,
        barrierRef,
        [&customCount](const TestEvent& event, int64_t seq, bool endOfBatch) {
            customCount.fetch_add(1, std::memory_order_relaxed);
        }
    );

    // Add as final consumer (gates the producer)
    disruptor.addProcessor(std::move(customProcessor), true);

    disruptor.start();

    for (int64_t i = 0; i < NUM_EVENTS; ++i) {
        disruptor.publishEvent([i](TestEvent& event, int64_t seq) {
            event.value = i;
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    disruptor.shutdown();

    std::cout << "  Handler1 count: " << handler1.getCount() << "\n";
    std::cout << "  Custom processor count: " << customCount.load() << "\n";
    assert(handler1.getCount() == NUM_EVENTS);
    assert(customCount.load() == NUM_EVENTS);
    std::cout << "  PASSED\n\n";
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════

int main() {
    std::cout << "═══════════════════════════════════════════════════════════════\n";
    std::cout << "Custom Processor Tests\n";
    std::cout << "═══════════════════════════════════════════════════════════════\n\n";

    testCustomProcessorStandalone();
    testMixedProcessors();
    testSamplingProcessor();
    testPipelineWithCustomProcessor();

    std::cout << "═══════════════════════════════════════════════════════════════\n";
    std::cout << "All tests PASSED!\n";
    std::cout << "═══════════════════════════════════════════════════════════════\n";

    return 0;
}
