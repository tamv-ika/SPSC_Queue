// Benchmark app for SPSCQueue and SPSCQueueOPT
// Usage: ./benchmark_spsc [queue_type] [num_ops]
// queue_type: 0 = SPSCQueue, 1 = SPSCQueueOPT
// num_ops: number of enqueue/dequeue operations

#include <iostream>
#include <thread>
#include <chrono>
#include "../SPSCQueue.h"
#include "../SPSCQueueOPT.h"
#include "cpupin.h"

constexpr size_t QUEUE_SIZE = 1024 * 64;

void benchmark_spscqueue(size_t num_ops)
{
    SPSCQueue<int, QUEUE_SIZE> q;
    std::atomic<bool> ready{false};
    std::thread producer([&]()
                         {
                             while (!ready.load())
                                 ;
                             int *msg = nullptr;
                             size_t cnt = 0;
                             while (cnt < num_ops)
                             {
                                 msg = q.alloc();
                                 if (!msg)
                                     continue;
                                 *msg = static_cast<int>(cnt);
                                 q.push();
                                 cnt++;
                             }
                         });
    std::thread consumer([&]()
                         {
        while (!ready.load());
        // int val;
        size_t cnt = 0;
        int* msg = nullptr;
        while (cnt < num_ops) {
            msg = q.front();
            if (!msg) continue;
            // val = *msg;
            q.pop();
            cnt++;
        } });
    auto start = std::chrono::high_resolution_clock::now();
    ready = true;
    producer.join();
    consumer.join();
    auto end = std::chrono::high_resolution_clock::now();
    double seconds = std::chrono::duration<double>(end - start).count();
    std::cout << "SPSCQueue throughput: " << (num_ops / seconds) / 1e6 << " Mops/sec\n";
}

void benchmark_spscqueueopt(size_t num_ops)
{
    SPSCQueueOPT<int, QUEUE_SIZE> q;
    std::atomic<bool> ready{false};
    std::thread producer([&]()
                         {
        if (!cpupin(6)) {
            exit(1);
        }
        while (!ready.load());
        size_t cnt = 0;
        while (cnt < num_ops)
        {
            int *msg = q.alloc();
            if (!msg) continue;
            *msg = static_cast<int>(cnt);
            q.push();
            cnt++;
        } });
    std::thread consumer([&]()
                         {
        if (!cpupin(7)) {
            exit(1);
        }
        while (!ready.load());
        // int val;
        size_t cnt = 0;
        int* msg = nullptr;
        while (cnt < num_ops) {
            msg = q.front();
            if (!msg) continue;
            // val = *msg;
            q.pop();
            cnt++;
        } });
    auto start = std::chrono::high_resolution_clock::now();
    ready = true;
    producer.join();
    consumer.join();
    auto end = std::chrono::high_resolution_clock::now();
    double seconds = std::chrono::duration<double>(end - start).count();
    std::cout << "SPSCQueueOPT throughput: " << (num_ops / seconds) / 1e6 << " Mops/sec\n";
}

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        std::cout << "Usage: " << argv[0] << " [queue_type: 0=SPSCQueue, 1=SPSCQueueOPT] [num_ops]\n";
        return 1;
    }
    int type = std::atoi(argv[1]);
    size_t num_ops = std::stoull(argv[2]);
    if (type == 0)
    {
        benchmark_spscqueue(num_ops);
    }
    else
    {
        benchmark_spscqueueopt(num_ops);
    }
    return 0;
}
