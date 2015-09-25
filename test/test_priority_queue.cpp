#include "testsettings.hpp"
#include "test.hpp"

#include <realm/util/priority_queue.hpp>

using namespace realm::util;

TEST(PriorityQueue_Push)
{
    PriorityQueue<int> q;
    q.push(1);
    q.push(9);
    q.push(2);
    q.push(8);
    q.push(3);
    q.push(7);
    q.push(4);
    q.push(5);
    q.push(6);

    for (int i = 0; i < 9; ++i) {
        int expected = 9 - i;
        CHECK_EQUAL(expected, q.top());
        q.pop();
    }
}

TEST(PriorityQueue_EraseMaintainsOrder)
{
    PriorityQueue<int> q;
    for (int i = 0; i < 100; ++i) {
        q.push(i);
    }

    for (size_t i = 0; i < 25; ++i) {
        auto it = q.begin() + i * 3;
        q.erase(it);
    }

    CHECK(std::is_sorted(q.begin(), q.end()));
}

TEST(PriorityQueue_Swap)
{
    PriorityQueue<int> q;
    PriorityQueue<int> p;
    q.push(123);
    p.push(456);
    q.swap(p);
    CHECK_EQUAL(456, q.top());
    CHECK_EQUAL(123, p.top());
}

TEST(PriorityQueue_PopsLargestElement)
{
    PriorityQueue<int> q;
    q.push(1);
    q.push(10000);
    CHECK_EQUAL(10000, q.top());
    q.pop();
    CHECK_EQUAL(1, q.top());
}
