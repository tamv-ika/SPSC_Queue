#include "../SPSCDisruptor.h"
#include <iostream>
#include <cassert>

struct TestMsg {
    uint64_t value;
};

int main() {
    std::cout << "Test 1: Basic alloc/push/front/pop... ";
    {
        SPSCDisruptor<TestMsg, 64> q;

        // Push 1
        TestMsg* s = q.alloc();
        assert(s != nullptr);
        s->value = 100;
        q.push();

        // Read 1
        TestMsg* f = q.front();
        assert(f != nullptr);
        assert(f->value == 100);
        q.pop();

        // Should be empty
        assert(q.front() == nullptr);
    }
    std::cout << "PASSED\n";

    std::cout << "Test 2: LMAX-style API... ";
    {
        SPSCDisruptor<TestMsg, 64> q;

        uint64_t seq = q.next();
        assert(seq == 1);
        q.get(seq)->value = 42;
        q.publish(seq);

        assert(q.available() == 1);
        assert(q.get(1)->value == 42);
        q.markConsumed(1);
    }
    std::cout << "PASSED\n";

    std::cout << "Test 3: Backpressure... ";
    {
        SPSCDisruptor<TestMsg, 8> q;  // Small

        // Fill 7 slots (capacity is 8 but one is reserved)
        for (int i = 0; i < 7; i++) {
            uint64_t seq = q.tryNext();
            assert(seq != 0);
            q.publish(seq);
        }

        // Should be full now
        assert(q.tryNext() == 0);

        // Free some
        q.markConsumed(3);

        // Should work now
        assert(q.tryNext() != 0);
    }
    std::cout << "PASSED\n";

    std::cout << "\n=== All simple tests PASSED ===\n";
    return 0;
}
