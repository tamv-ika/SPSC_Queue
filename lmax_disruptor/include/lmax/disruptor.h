/*
 * High-Performance LMAX Disruptor for C++
 *
 * All-in-one include file.
 *
 * === SINGLE PRODUCER (Default) ===
 *
 *   #include <lmax/disruptor.h>
 *
 *   lmax::RingBuffer<Event, 65536> ring;
 *
 *   // Producer
 *   int64_t seq = ring.next();
 *   Event* e = ring.get(seq);
 *   e->data = ...;
 *   ring.publish(seq);
 *
 *   // Consumer
 *   auto barrier = ring.newBarrier();
 *   int64_t available = barrier.waitFor(nextSeq);
 *   while (nextSeq <= available) {
 *       const Event* e = ring.get(nextSeq++);
 *       process(e);
 *   }
 *   consumerSeq.set(nextSeq - 1);
 *
 * === MULTI PRODUCER ===
 *
 *   lmax::MPMCRingBuffer<Event, 65536> ring;
 *   // Same API, but thread-safe for multiple producers
 *
 * === DISRUPTOR DSL (Pipeline Setup) ===
 *
 *   lmax::Disruptor<Event, 65536> disruptor;
 *
 *   // Chain: handler1 -> handler2 -> handler3
 *   disruptor.handleEventsWith(handler1)
 *           .then(handler2)
 *           .then(handler3);
 *
 *   disruptor.start();
 *   disruptor.publishEvent([](Event& e, int64_t seq) {
 *       e.data = ...;
 *   });
 *   disruptor.shutdown();
 */

#pragma once

// Core components
#include "sequence.h"
#include "wait_strategy.h"
#include "sequence_barrier.h"

// Sequencers
#include "single_producer_sequencer.h"
#include "multi_producer_sequencer.h"

// Ring buffer (supports both single and multi producer via templates)
#include "ring_buffer.h"

// Event processing
#include "event_handler.h"
#include "batch_event_processor.h"

// DSL for easy pipeline setup
#include "disruptor_dsl.h"

namespace lmax {

// Version info
constexpr int VERSION_MAJOR = 2;
constexpr int VERSION_MINOR = 0;
constexpr int VERSION_PATCH = 0;

} // namespace lmax
