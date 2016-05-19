#include "testsettings.hpp"
#include "test.hpp"

#include <realm/util/ring_buffer.hpp>

using namespace realm;
using namespace realm::util;

TEST(RingBuffer_StartsEmpty)
{
    RingBuffer<char> b;
    CHECK(b.empty());
    CHECK_EQUAL(b.size(), 0);
}

TEST(RingBuffer_PushBackPopFront)
{
    RingBuffer<char> b;
    char ch = 'a';
    std::generate_n(std::back_inserter(b), 26, [&] {
        return ch++;
    });
    const char expected[26 + 1] = "abcdefghijklmnopqrstuvwxyz";
    for (size_t i = 0; i < 26; ++i) {
        CHECK_EQUAL(b.front(), expected[i]);
        b.pop_front();
    }
}

TEST(RingBuffer_PushFrontPopBack)
{
    RingBuffer<char> b;
    char ch = 'a';
    std::generate_n(std::front_inserter(b), 26, [&] {
        return ch++;
    });
    const char expected[27 + 1] = "abcdefghijklmnopqrstuvwxyz";
    for (size_t i = 0; i < 26; ++i) {
        CHECK_EQUAL(b.back(), expected[i]);
        b.pop_back();
    }
}

