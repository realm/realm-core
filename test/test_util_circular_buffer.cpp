#include <type_traits>
#include <algorithm>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <vector>

#include <realm/util/circular_buffer.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;

namespace {

// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment varible `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.


TEST(Util_CircularBuffer_Empty)
{
    CircularBuffer<int> buffer;
    CHECK(buffer.empty());
    CHECK_EQUAL(0, buffer.size());
    CHECK_EQUAL(0, buffer.capacity());
}


TEST(Util_CircularBuffer_PushPopFront)
{
    CircularBuffer<int> buffer;
    buffer.push_front(1);
    CHECK_NOT(buffer.empty());
    CHECK_EQUAL(1, buffer.size());
    CHECK_EQUAL(1, buffer.front());
    std::size_t capacity = buffer.capacity();
    CHECK_GREATER_EQUAL(1, capacity);
    buffer.pop_front();
    CHECK(buffer.empty());
    CHECK_EQUAL(0, buffer.size());
    CHECK_EQUAL(capacity, buffer.capacity());
    buffer.push_front(2);
    CHECK_NOT(buffer.empty());
    CHECK_EQUAL(1, buffer.size());
    CHECK_EQUAL(2, buffer.front());
    CHECK_EQUAL(capacity, buffer.capacity());
    buffer.push_front(3);
    buffer.push_front(4);
    buffer.push_front(5);
    buffer.push_front(6);
    CHECK_NOT(buffer.empty());
    CHECK_EQUAL(5, buffer.size());
    CHECK_EQUAL(6, buffer[0]);
    CHECK_EQUAL(5, buffer[1]);
    CHECK_EQUAL(4, buffer[2]);
    CHECK_EQUAL(3, buffer[3]);
    CHECK_EQUAL(2, buffer[4]);
    std::size_t capacity_2 = buffer.capacity();
    CHECK_GREATER_EQUAL(capacity_2, capacity);
    buffer.pop_front();
    buffer.pop_front();
    CHECK_NOT(buffer.empty());
    CHECK_EQUAL(3, buffer.size());
    CHECK_EQUAL(4, buffer[0]);
    CHECK_EQUAL(3, buffer[1]);
    CHECK_EQUAL(2, buffer[2]);
    CHECK_EQUAL(capacity_2, buffer.capacity());
}


TEST(Util_CircularBuffer_PushPopBack)
{
    CircularBuffer<int> buffer;
    buffer.push_back(1);
    CHECK_NOT(buffer.empty());
    CHECK_EQUAL(1, buffer.size());
    CHECK_EQUAL(1, buffer.back());
    std::size_t capacity = buffer.capacity();
    CHECK_GREATER_EQUAL(1, capacity);
    buffer.pop_back();
    CHECK(buffer.empty());
    CHECK_EQUAL(0, buffer.size());
    CHECK_EQUAL(capacity, buffer.capacity());
    buffer.push_back(2);
    CHECK_NOT(buffer.empty());
    CHECK_EQUAL(1, buffer.size());
    CHECK_EQUAL(2, buffer.back());
    CHECK_EQUAL(capacity, buffer.capacity());
    buffer.push_back(3);
    buffer.push_back(4);
    buffer.push_back(5);
    buffer.push_back(6);
    CHECK_NOT(buffer.empty());
    CHECK_EQUAL(5, buffer.size());
    CHECK_EQUAL(2, buffer[0]);
    CHECK_EQUAL(3, buffer[1]);
    CHECK_EQUAL(4, buffer[2]);
    CHECK_EQUAL(5, buffer[3]);
    CHECK_EQUAL(6, buffer[4]);
    std::size_t capacity_2 = buffer.capacity();
    CHECK_GREATER_EQUAL(capacity_2, capacity);
    buffer.pop_back();
    buffer.pop_back();
    CHECK_NOT(buffer.empty());
    CHECK_EQUAL(3, buffer.size());
    CHECK_EQUAL(2, buffer[0]);
    CHECK_EQUAL(3, buffer[1]);
    CHECK_EQUAL(4, buffer[2]);
    CHECK_EQUAL(capacity_2, buffer.capacity());
}


TEST(Util_CircularBuffer_PushPopFrontBack)
{
    CircularBuffer<int> buffer;
    buffer.push_front(1);
    buffer.push_back(2);
    CHECK_NOT(buffer.empty());
    CHECK_EQUAL(2, buffer.size());
    CHECK_EQUAL(1, buffer[0]);
    CHECK_EQUAL(2, buffer[1]);
    buffer.push_front(3);
    buffer.push_back(4);
    CHECK_EQUAL(4, buffer.size());
    CHECK_EQUAL(3, buffer[0]);
    CHECK_EQUAL(1, buffer[1]);
    CHECK_EQUAL(2, buffer[2]);
    CHECK_EQUAL(4, buffer[3]);
    std::size_t capacity = buffer.capacity();
    buffer.pop_front();
    buffer.push_back(5);
    CHECK_EQUAL(4, buffer.size());
    CHECK_EQUAL(1, buffer[0]);
    CHECK_EQUAL(2, buffer[1]);
    CHECK_EQUAL(4, buffer[2]);
    CHECK_EQUAL(5, buffer[3]);
    CHECK_EQUAL(capacity, buffer.capacity());
    buffer.pop_front();
    buffer.push_back(6);
    buffer.pop_front();
    buffer.push_back(7);
    CHECK_EQUAL(4, buffer.size());
    CHECK_EQUAL(4, buffer[0]);
    CHECK_EQUAL(5, buffer[1]);
    CHECK_EQUAL(6, buffer[2]);
    CHECK_EQUAL(7, buffer[3]);
    CHECK_EQUAL(capacity, buffer.capacity());
    buffer.pop_front();
    buffer.push_back(8);
    buffer.pop_front();
    buffer.push_back(9);
    buffer.pop_front();
    buffer.push_back(10);
    CHECK_EQUAL(4, buffer.size());
    CHECK_EQUAL(7, buffer[0]);
    CHECK_EQUAL(8, buffer[1]);
    CHECK_EQUAL(9, buffer[2]);
    CHECK_EQUAL(10, buffer[3]);
    CHECK_EQUAL(capacity, buffer.capacity());
    buffer.pop_front();
    buffer.push_back(11);
    buffer.pop_front();
    buffer.push_back(12);
    buffer.pop_front();
    buffer.push_back(13);
    buffer.pop_front();
    buffer.push_back(14);
    CHECK_EQUAL(4, buffer.size());
    CHECK_EQUAL(11, buffer[0]);
    CHECK_EQUAL(12, buffer[1]);
    CHECK_EQUAL(13, buffer[2]);
    CHECK_EQUAL(14, buffer[3]);
    CHECK_EQUAL(capacity, buffer.capacity());
    buffer.pop_back();
    buffer.push_front(15);
    CHECK_EQUAL(4, buffer.size());
    CHECK_EQUAL(15, buffer[0]);
    CHECK_EQUAL(11, buffer[1]);
    CHECK_EQUAL(12, buffer[2]);
    CHECK_EQUAL(13, buffer[3]);
    CHECK_EQUAL(capacity, buffer.capacity());
    buffer.pop_back();
    buffer.push_front(16);
    buffer.pop_back();
    buffer.push_front(17);
    CHECK_EQUAL(4, buffer.size());
    CHECK_EQUAL(17, buffer[0]);
    CHECK_EQUAL(16, buffer[1]);
    CHECK_EQUAL(15, buffer[2]);
    CHECK_EQUAL(11, buffer[3]);
    CHECK_EQUAL(capacity, buffer.capacity());
    buffer.pop_back();
    buffer.push_front(18);
    buffer.pop_back();
    buffer.push_front(19);
    buffer.pop_back();
    buffer.push_front(20);
    CHECK_EQUAL(4, buffer.size());
    CHECK_EQUAL(20, buffer[0]);
    CHECK_EQUAL(19, buffer[1]);
    CHECK_EQUAL(18, buffer[2]);
    CHECK_EQUAL(17, buffer[3]);
    CHECK_EQUAL(capacity, buffer.capacity());
    buffer.pop_back();
    buffer.push_front(21);
    buffer.pop_back();
    buffer.push_front(22);
    buffer.pop_back();
    buffer.push_front(23);
    buffer.pop_back();
    buffer.push_front(24);
    CHECK_EQUAL(4, buffer.size());
    CHECK_EQUAL(24, buffer[0]);
    CHECK_EQUAL(23, buffer[1]);
    CHECK_EQUAL(22, buffer[2]);
    CHECK_EQUAL(21, buffer[3]);
    CHECK_EQUAL(capacity, buffer.capacity());
    buffer.pop_front();
    buffer.pop_back();
    CHECK_NOT(buffer.empty());
    CHECK_EQUAL(2, buffer.size());
    CHECK_EQUAL(23, buffer[0]);
    CHECK_EQUAL(22, buffer[1]);
    CHECK_EQUAL(capacity, buffer.capacity());
    buffer.pop_front();
    buffer.pop_back();
    CHECK(buffer.empty());
    CHECK_EQUAL(0, buffer.size());
    CHECK_EQUAL(capacity, buffer.capacity());
}


TEST(Util_CircularBuffer_RangeCheckingSubscribe)
{
    CircularBuffer<int> buffer;
    CHECK(buffer.empty());
    CHECK_THROW(buffer.at(0), std::out_of_range);
    buffer.push_back(1);
    CHECK_EQUAL(1, buffer.at(0));
    CHECK_THROW(buffer.at(1), std::out_of_range);
    buffer.push_back(2);
    CHECK_EQUAL(1, buffer.at(0));
    CHECK_EQUAL(2, buffer.at(1));
    CHECK_THROW(buffer.at(2), std::out_of_range);
}


TEST(Util_CircularBuffer_ConstructFromInitializerList)
{
    CircularBuffer<int> buffer{1, 2, 3};
    CHECK_NOT(buffer.empty());
    CHECK_EQUAL(3, buffer.size());
    CHECK_EQUAL(3, buffer.capacity());
    CHECK_EQUAL(1, buffer[0]);
    CHECK_EQUAL(2, buffer[1]);
    CHECK_EQUAL(3, buffer[2]);
}


TEST(Util_CircularBuffer_AssignFromInitializerList)
{
    CircularBuffer<int> buffer;
    buffer = {1, 2, 3};
    CHECK_NOT(buffer.empty());
    CHECK_EQUAL(3, buffer.size());
    CHECK_EQUAL(3, buffer.capacity());
    CHECK_EQUAL(1, buffer[0]);
    CHECK_EQUAL(2, buffer[1]);
    CHECK_EQUAL(3, buffer[2]);
    buffer.assign({4, 5, 6, 7});
    CHECK_NOT(buffer.empty());
    CHECK_EQUAL(4, buffer.size());
    CHECK_LESS_EQUAL(4, buffer.capacity());
    CHECK_EQUAL(4, buffer[0]);
    CHECK_EQUAL(5, buffer[1]);
    CHECK_EQUAL(6, buffer[2]);
    CHECK_EQUAL(7, buffer[3]);
}


TEST(Util_CircularBuffer_Clear)
{
    CircularBuffer<int> buffer{1, 2, 3};
    std::size_t capacity = buffer.capacity();
    buffer.clear();
    CHECK(buffer.empty());
    CHECK_EQUAL(0, buffer.size());
    CHECK_EQUAL(capacity, buffer.capacity());
}


TEST(Util_CircularBuffer_Equality)
{
    CircularBuffer<int> buffer_1{1, 2, 3};
    CircularBuffer<int> buffer_2{1, 2, 3};
    CircularBuffer<int> buffer_3{1, 2, 4};
    CHECK(buffer_1 == buffer_2);
    CHECK(buffer_1 != buffer_3);
    CHECK(buffer_2 != buffer_3);
    CHECK_NOT(buffer_1 != buffer_2);
    CHECK_NOT(buffer_1 == buffer_3);
    CHECK_NOT(buffer_2 == buffer_3);
}


TEST(Util_CircularBuffer_CopyConstruct)
{
    CircularBuffer<int> buffer_1{1, 2, 3};
    CircularBuffer<int> buffer_2 = buffer_1;
    CHECK_NOT(buffer_2.empty());
    CHECK_EQUAL(3, buffer_2.size());
    CHECK_EQUAL(3, buffer_2.capacity());
    CHECK(buffer_2 == buffer_1);
}


TEST(Util_CircularBuffer_CopyAssign)
{
    CircularBuffer<int> buffer_1{1, 2, 3};
    CircularBuffer<int> buffer_2{4, 5, 6};
    buffer_2 = buffer_1; // Copy assign
    CHECK_NOT(buffer_2.empty());
    CHECK_EQUAL(3, buffer_2.size());
    CHECK_EQUAL(3, buffer_2.capacity());
    CHECK(buffer_2 == buffer_1);
}


TEST(Util_CircularBuffer_BeginEnd)
{
    std::vector<int> vector{1, 2, 3};
    CircularBuffer<int> buffer{1, 2, 3};
    const CircularBuffer<int>& cbuffer = buffer;
    CHECK(std::equal(vector.begin(), vector.end(), buffer.begin(), buffer.end()));
    CHECK(std::equal(vector.begin(), vector.end(), cbuffer.begin(), cbuffer.end()));
    CHECK(std::equal(vector.begin(), vector.end(), buffer.cbegin(), buffer.cend()));
    CHECK(std::equal(vector.rbegin(), vector.rend(), buffer.rbegin(), buffer.rend()));
    CHECK(std::equal(vector.rbegin(), vector.rend(), cbuffer.rbegin(), cbuffer.rend()));
    CHECK(std::equal(vector.rbegin(), vector.rend(), buffer.crbegin(), buffer.crend()));
}


TEST(Util_CircularBuffer_ConstructFromSize)
{
    CircularBuffer<int> buffer(3);
    CHECK_EQUAL(3, buffer.size());
    CHECK_EQUAL(3, buffer.capacity());
    CHECK(std::all_of(buffer.begin(), buffer.end(), [](int value) {
        return (value == 0);
    }));
}

TEST(Util_CircularBuffer_ConstructFromSizeAndValue)
{
    CircularBuffer<int> buffer(3, 7);
    CHECK_EQUAL(3, buffer.size());
    CHECK_EQUAL(3, buffer.capacity());
    CHECK(std::all_of(buffer.begin(), buffer.end(), [](int value) {
        return (value == 7);
    }));
}


TEST(Util_CircularBuffer_AssignFromSizeAndValue)
{
    CircularBuffer<int> buffer;
    buffer.assign(3, 7);
    CHECK_EQUAL(3, buffer.size());
    CHECK_EQUAL(3, buffer.capacity());
    CHECK(std::all_of(buffer.begin(), buffer.end(), [](int value) {
        return (value == 7);
    }));
}


TEST(Util_CircularBuffer_ConstructFromNonrandomAccessIterator)
{
    std::list<int> list{1, 2, 3};
    CircularBuffer<int> buffer(list.begin(), list.end());
    CHECK_EQUAL(3, buffer.size());
    CHECK_EQUAL(3, buffer.capacity());
    CHECK(std::equal(list.begin(), list.end(), buffer.begin(), buffer.end()));
}


TEST(Util_CircularBuffer_ConstructFromRandomAccessIterator)
{
    std::vector<int> vector{1, 2, 3};
    CircularBuffer<int> buffer(vector.begin(), vector.end());
    CHECK_EQUAL(3, buffer.size());
    CHECK_EQUAL(3, buffer.capacity());
    CHECK(std::equal(vector.begin(), vector.end(), buffer.begin(), buffer.end()));
}


TEST(Util_CircularBuffer_AssignFromNonrandomAccessIterator)
{
    std::list<int> list{1, 2, 3};
    CircularBuffer<int> buffer{4, 5, 6};
    buffer.assign(list.begin(), list.end());
    CHECK_EQUAL(3, buffer.size());
    CHECK_EQUAL(3, buffer.capacity());
    CHECK(std::equal(list.begin(), list.end(), buffer.begin(), buffer.end()));
}


TEST(Util_CircularBuffer_AssignFromRandomAccessIterator)
{
    std::vector<int> vector{1, 2, 3};
    CircularBuffer<int> buffer{4, 5, 6};
    buffer.assign(vector.begin(), vector.end());
    CHECK_EQUAL(3, buffer.size());
    CHECK_EQUAL(3, buffer.capacity());
    CHECK(std::equal(vector.begin(), vector.end(), buffer.begin(), buffer.end()));
}


TEST(Util_CircularBuffer_MoveConstruct)
{
    CircularBuffer<int> buffer_1{1, 2, 3};
    CircularBuffer<int> buffer_2(std::move(buffer_1));
    CHECK_EQUAL(0, buffer_1.size());
    CHECK_EQUAL(3, buffer_2.size());
    CHECK_EQUAL(0, buffer_1.capacity());
    CHECK_EQUAL(3, buffer_2.capacity());
    CHECK_EQUAL(1, buffer_2[0]);
    CHECK_EQUAL(2, buffer_2[1]);
    CHECK_EQUAL(3, buffer_2[2]);
}


TEST(Util_CircularBuffer_MoveAssign)
{
    CircularBuffer<int> buffer_1{1, 2, 3};
    CircularBuffer<int> buffer_2{4, 5, 6};
    buffer_2 = std::move(buffer_1);
    CHECK_EQUAL(0, buffer_1.size());
    CHECK_EQUAL(3, buffer_2.size());
    CHECK_EQUAL(0, buffer_1.capacity());
    CHECK_EQUAL(3, buffer_2.capacity());
    CHECK_EQUAL(1, buffer_2[0]);
    CHECK_EQUAL(2, buffer_2[1]);
    CHECK_EQUAL(3, buffer_2[2]);
}


TEST(Util_CircularBuffer_IteratorEquality)
{
    CircularBuffer<int> buffer;
    CircularBuffer<int>& cbuffer = buffer;
    CHECK(buffer.begin() == buffer.end());
    CHECK(buffer.cbegin() == buffer.cend());
    CHECK(buffer.begin() == buffer.cend());
    CHECK(buffer.cbegin() == buffer.end());
    CHECK(cbuffer.begin() == cbuffer.end());
    CHECK(buffer.begin() == cbuffer.end());
    CHECK(cbuffer.begin() == buffer.end());
    CHECK_NOT(buffer.begin() != buffer.end());
    CHECK_NOT(buffer.cbegin() != buffer.cend());
    CHECK_NOT(buffer.begin() != buffer.cend());
    CHECK_NOT(buffer.cbegin() != buffer.end());
    CHECK_NOT(cbuffer.begin() != cbuffer.end());
    CHECK_NOT(buffer.begin() != cbuffer.end());
    CHECK_NOT(cbuffer.begin() != buffer.end());
    buffer.push_back(0);
    CHECK_NOT(buffer.begin() == buffer.end());
    CHECK_NOT(buffer.cbegin() == buffer.cend());
    CHECK_NOT(buffer.begin() == buffer.cend());
    CHECK_NOT(buffer.cbegin() == buffer.end());
    CHECK_NOT(cbuffer.begin() == cbuffer.end());
    CHECK_NOT(buffer.begin() == cbuffer.end());
    CHECK_NOT(cbuffer.begin() == buffer.end());
    CHECK(buffer.begin() != buffer.end());
    CHECK(buffer.cbegin() != buffer.cend());
    CHECK(buffer.begin() != buffer.cend());
    CHECK(buffer.cbegin() != buffer.end());
    CHECK(cbuffer.begin() != cbuffer.end());
    CHECK(buffer.begin() != cbuffer.end());
    CHECK(cbuffer.begin() != buffer.end());
}


TEST(Util_CircularBuffer_IteratorOperations)
{
    CircularBuffer<int> buffer{1, 2, 3};
    auto i_1 = buffer.begin();
    auto i_2 = 1 + i_1;
    CHECK_EQUAL(2, *i_2);
}


TEST(Util_CircularBuffer_Resize)
{
    CircularBuffer<int> buffer;
    buffer.resize(0);
    CHECK(buffer.empty());
    CHECK_EQUAL(0, buffer.capacity());
    buffer.resize(0, 7);
    CHECK(buffer.empty());
    CHECK_EQUAL(0, buffer.capacity());
    buffer.resize(3);
    CHECK_EQUAL(3, buffer.capacity());
    CHECK(buffer == CircularBuffer<int>({0, 0, 0}));
    buffer.resize(1);
    CHECK_EQUAL(3, buffer.capacity());
    CHECK(buffer == CircularBuffer<int>({0}));
    buffer.resize(0, 7);
    CHECK_EQUAL(3, buffer.capacity());
    CHECK(buffer == CircularBuffer<int>());
    buffer.resize(3, 7);
    CHECK_EQUAL(3, buffer.capacity());
    CHECK(buffer == CircularBuffer<int>({7, 7, 7}));
    buffer.resize(4, 8);
    CHECK(buffer == CircularBuffer<int>({7, 7, 7, 8}));
    buffer.pop_front();
    buffer.resize(4, 9);
    CHECK(buffer == CircularBuffer<int>({7, 7, 8, 9}));
    buffer.resize(2, 10);
    CHECK(buffer == CircularBuffer<int>({7, 7}));
    buffer.resize(3);
    CHECK(buffer == CircularBuffer<int>({7, 7, 0}));
}


TEST(Util_CircularBuffer_Resize2)
{
    CircularBuffer<std::unique_ptr<int>> buffer;
    buffer.push_back(std::make_unique<int>(1));
    buffer.push_back(std::make_unique<int>(2));
    buffer.push_back(std::make_unique<int>(3));
    buffer.resize(2);
    CHECK_EQUAL(2, buffer.size());
    CHECK_EQUAL(1, *buffer[0]);
    CHECK_EQUAL(2, *buffer[1]);
    buffer.push_back(std::make_unique<int>(4));
    buffer.resize(2);
    CHECK_EQUAL(2, buffer.size());
    CHECK_EQUAL(1, *buffer[0]);
    CHECK_EQUAL(2, *buffer[1]);
    buffer.push_front(std::make_unique<int>(5));
    buffer.resize(2);
    CHECK_EQUAL(2, buffer.size());
    CHECK_EQUAL(5, *buffer[0]);
    CHECK_EQUAL(1, *buffer[1]);
}


TEST(Util_CircularBuffer_ShrinkToFit)
{
    CircularBuffer<int> buffer;
    buffer.shrink_to_fit();
    CHECK_EQUAL(0, buffer.capacity());
    buffer.push_back(1);
    buffer.shrink_to_fit();
    CHECK_EQUAL(1, buffer.capacity());
    buffer.shrink_to_fit();
    CHECK_EQUAL(1, buffer.capacity());
    buffer.push_back(2);
    buffer.shrink_to_fit();
    CHECK_EQUAL(2, buffer.capacity());
    buffer.shrink_to_fit();
    CHECK_EQUAL(2, buffer.capacity());
    buffer.push_back(3);
    buffer.shrink_to_fit();
    CHECK_EQUAL(3, buffer.capacity());
    buffer.shrink_to_fit();
    CHECK_EQUAL(3, buffer.capacity());
    buffer.push_back(4);
    buffer.shrink_to_fit();
    CHECK_EQUAL(4, buffer.capacity());
    buffer.shrink_to_fit();
    CHECK_EQUAL(4, buffer.capacity());
    CHECK(buffer == CircularBuffer<int>({1, 2, 3, 4}));
}


TEST(Util_CircularBuffer_Swap)
{
    CircularBuffer<int> buffer_1{1, 2, 3};
    CircularBuffer<int> buffer_2{4, 5};
    swap(buffer_1, buffer_2);
    CHECK(buffer_1 == CircularBuffer<int>({4, 5}));
    CHECK(buffer_2 == CircularBuffer<int>({1, 2, 3}));
}


TEST(Util_CircularBuffer_ExceptionSafetyInConstructFromIteratorPair)
{
    struct Context {
        int num_instances = 0;
        int num_copy_ops = 0;
    };

    class X {
    public:
        X(Context& context)
            : m_context{context}
        {
            ++m_context.num_instances;
        }
        X(const X& x)
            : m_context{x.m_context}
        {
            if (++m_context.num_copy_ops == 2)
                throw std::bad_alloc{};
            ++m_context.num_instances;
        }
        ~X()
        {
            --m_context.num_instances;
        }

    private:
        Context& m_context;
    };

    class Iter {
    public:
        using difference_type = std::ptrdiff_t;
        using value_type = X;
        using pointer = X*;
        using reference = X&;
        using iterator_category = std::input_iterator_tag;
        Iter(pointer ptr)
            : m_ptr{ptr}
        {
        }
        reference operator*() const
        {
            return *m_ptr;
        }
        pointer operator->() const
        {
            return m_ptr;
        }
        Iter& operator++()
        {
            ++m_ptr;
            return *this;
        }
        Iter operator++(int)
        {
            Iter i = *this;
            operator++();
            return i;
        }
        bool operator==(Iter i) const
        {
            return (m_ptr == i.m_ptr);
        }
        bool operator!=(Iter i) const
        {
            return (m_ptr != i.m_ptr);
        }

    private:
        pointer m_ptr = nullptr;
    };

    Context context;
    {
        std::aligned_storage<sizeof(X), alignof(X)>::type mem[3];
        X* init = new (&mem[0]) X{context};
        new (&mem[1]) X{context};
        new (&mem[2]) X{context};
        Iter i_1{init}, i_2{init + 3};
        try {
            CircularBuffer<X>(i_1, i_2);
        }
        catch (std::bad_alloc&) {
        }
        init[0].~X();
        init[1].~X();
        init[2].~X();
    }
    CHECK_EQUAL(0, context.num_instances);
}

} // unnamed namespace
