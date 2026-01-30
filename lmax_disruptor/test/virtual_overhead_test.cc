#include "../include/lmax/ring_buffer.h"
#include "../include/lmax/batch_event_processor.h"
#include "../include/lmax/event_handler.h"
#include <chrono>
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
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

constexpr int64_t NUM_EVENTS = 500000;
constexpr double CPU_FREQ = 2.9e9;

void printStats(const std::vector<int64_t>& lat, const char* name) {
    std::vector<int64_t> sorted = lat;
    std::sort(sorted.begin(), sorted.end());
    
    size_t n = sorted.size();
    double toNs = 1e9 / CPU_FREQ;
    
    printf("%20s: p50=%5.0fns  p90=%5.0fns  p99=%5.0fns\n",
           name,
           sorted[n * 50 / 100] * toNs,
           sorted[n * 90 / 100] * toNs,
           sorted[n * 99 / 100] * toNs);
}

int main() {
    printf("Virtual vs Non-Virtual Processor Latency Comparison\n");
    printf("════════════════════════════════════════════════════\n\n");

    // Run each test 3 times and show results
    for (int run = 1; run <= 3; ++run) {
        printf("Run %d:\n", run);
        
        std::vector<int64_t> virtualLatencies;
        std::vector<int64_t> directLatencies;
        virtualLatencies.reserve(NUM_EVENTS);
        directLatencies.reserve(NUM_EVENTS);

        // Test 1: Virtual (BatchEventProcessor)
        {
            using RB = RingBuffer<Event, 65536, BusySpinWait>;
            RB rb;
            auto barrier = rb.newBarrier();
            
            class Handler : public EventHandler<Event> {
            public:
                std::vector<int64_t>* lat;
                void onEvent(Event& e, int64_t, bool) override {
                    lat->push_back(rdtsc() - e.value);
                }
            };
            Handler h;
            h.lat = &virtualLatencies;
            
            BatchEventProcessor proc(rb, barrier, h);
            rb.addGatingSequence(proc.getSequence());
            proc.start();
            
            pinCPU(2);
            for (int64_t i = 0; i < NUM_EVENTS; ++i) {
                int64_t seq = rb.next();
                rb.get(seq)->value = rdtsc();
                rb.publish(seq);
                uint64_t s = rdtsc(); while (rdtsc() - s < 1000) {}
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            proc.halt();
            proc.join();
        }

        // Test 2: Direct with waitFor (same logic, no virtual)
        {
            using RB = RingBuffer<Event, 65536, BusySpinWait>;
            RB rb;
            auto barrier = rb.newBarrier();
            Sequence consumerSeq;
            
            rb.addGatingSequence(consumerSeq);
            
            std::atomic<bool> running{true};
            
            std::thread consumer([&]{
                pinCPU(3);
                int64_t nextSeq = consumerSeq.get() + 1;
                while (running.load(std::memory_order_acquire)) {
                    int64_t avail = barrier.waitFor(nextSeq);
                    if (avail == ALERTED) break;
                    while (nextSeq <= avail) {
                        Event* e = const_cast<Event*>(rb.get(nextSeq));
                        directLatencies.push_back(rdtsc() - e->value);
                        ++nextSeq;
                    }
                    consumerSeq.set(avail);
                }
            });
            
            pinCPU(2);
            for (int64_t i = 0; i < NUM_EVENTS; ++i) {
                int64_t seq = rb.next();
                rb.get(seq)->value = rdtsc();
                rb.publish(seq);
                uint64_t s = rdtsc(); while (rdtsc() - s < 1000) {}
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            running.store(false, std::memory_order_release);
            barrier.alert();
            consumer.join();
        }

        printStats(virtualLatencies, "Virtual");
        printStats(directLatencies, "Direct");
        printf("\n");
    }

    return 0;
}
