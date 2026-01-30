#include "../include/lmax/ring_buffer.h"
#include "../include/lmax/batch_event_processor.h"
#include "../include/lmax/event_handler.h"
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
    
    std::vector<int64_t> latencies;
    latencies.reserve(N);
    
    class Handler : public EventHandler<Event> {
    public:
        std::vector<int64_t>* lat;
        void onEvent(Event& e, int64_t, bool) override {
            lat->push_back(rdtsc() - e.value);
        }
    };
    Handler h;
    h.lat = &latencies;
    
    BatchEventProcessor proc(rb, barrier, h);
    rb.addGatingSequence(proc.getSequence());
    proc.start();
    
    pinCPU(2);
    for (int64_t i = 0; i < N; ++i) {
        int64_t seq = rb.next();
        rb.get(seq)->value = rdtsc();
        rb.publish(seq);
        uint64_t s = rdtsc(); while (rdtsc() - s < 1000) {}
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    proc.halt();
    proc.join();
    
    std::sort(latencies.begin(), latencies.end());
    size_t n = latencies.size();
    double toNs = 1e9 / FREQ;
    
    printf("Virtual BatchEventProcessor (with IEventProcessor):\n");
    printf("  p50=%5.0fns  p90=%5.0fns  p99=%5.0fns  p99.9=%5.0fns\n",
           latencies[n*50/100]*toNs, latencies[n*90/100]*toNs,
           latencies[n*99/100]*toNs, latencies[n*999/1000]*toNs);
    return 0;
}
