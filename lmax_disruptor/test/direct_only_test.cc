#include "../include/lmax/ring_buffer.h"
#include "../include/lmax/sequence_barrier.h"
#include <thread>
#include <vector>
#include <algorithm>

using namespace lmax;
struct Event { int64_t value; };

inline uint64_t rdtsc() {
    unsigned int lo, hi;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)hi << 32) | lo;
}

void pinCPU(int cpu) {
    cpu_set_t set; CPU_ZERO(&set); CPU_SET(cpu, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

int main() {
    constexpr int64_t N = 1000000;
    constexpr double FREQ = 2.9e9;
    
    using RB = RingBuffer<Event, 65536, BusySpinWait>;
    RB rb;
    auto barrier = rb.newBarrier();
    Sequence seq;
    
    std::vector<int64_t> latencies;
    latencies.reserve(N);
    
    rb.addGatingSequence(seq);
    
    std::atomic<bool> running{true};
    std::thread consumer([&]{
        pinCPU(3);
        int64_t nextSeq = seq.get() + 1;
        while (running.load(std::memory_order_acquire)) {
            int64_t avail = barrier.waitFor(nextSeq);
            if (avail == ALERTED) break;
            while (nextSeq <= avail) {
                Event* e = const_cast<Event*>(rb.get(nextSeq));
                latencies.push_back(rdtsc() - e->value);
                ++nextSeq;
            }
            seq.set(avail);
        }
    });
    
    pinCPU(2);
    for (int64_t i = 0; i < N; ++i) {
        int64_t s = rb.next();
        rb.get(s)->value = rdtsc();
        rb.publish(s);
        uint64_t t = rdtsc(); while (rdtsc() - t < 1000) {}
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    running.store(false, std::memory_order_release);
    barrier.alert();
    consumer.join();
    
    std::sort(latencies.begin(), latencies.end());
    size_t n = latencies.size();
    double toNs = 1e9 / FREQ;
    
    printf("Direct inline loop (no virtual, no inheritance):\n");
    printf("  p50=%5.0fns  p90=%5.0fns  p99=%5.0fns  p99.9=%5.0fns\n",
           latencies[n*50/100]*toNs, latencies[n*90/100]*toNs,
           latencies[n*99/100]*toNs, latencies[n*999/1000]*toNs);
    return 0;
}
