/*
 * High-Performance LMAX Disruptor for C++
 *
 * All-in-one include file.
 *
 * Usage:
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
 */

#pragma once

#include "sequence.h"
#include "wait_strategy.h"
#include "sequence_barrier.h"
#include "single_producer_sequencer.h"
#include "ring_buffer.h"

namespace lmax {

// Version info
constexpr int VERSION_MAJOR = 1;
constexpr int VERSION_MINOR = 0;
constexpr int VERSION_PATCH = 0;

} // namespace lmax
