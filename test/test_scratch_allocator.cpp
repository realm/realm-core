#include "test.hpp"

#include <realm/util/scratch_allocator.hpp>

#include <vector>
#include <map>
#include <unordered_map>

using namespace realm;
using namespace realm::util;

TEST(ScratchAllocator_Scopes)
{
    ScratchMemory memory;
    auto pos_before_1 = memory.get_current_position();
    {
        ScratchArena arena1{memory};
        std::vector<int, ScratchAllocator<int>> vec1{arena1};
        vec1.resize(100000);
        std::fill(vec1.begin(), vec1.end(), 123);

        auto pos_before_2 = memory.get_current_position();
        {
            ScratchArena arena2{memory};
            std::vector<float, ScratchAllocator<float>> vec2{arena2};
            vec2.resize(100000);
            std::fill(vec2.begin(), vec2.end(), 456.f);

            for (auto i : vec1) {
                CHECK_EQUAL(i, 123);
            }
            for (auto f : vec2) {
                CHECK_EQUAL(f, 456.f);
            }
        }
        auto pos_after_2 = memory.get_current_position();
        CHECK_EQUAL(pos_before_2.bytes(), pos_after_2.bytes());

        std::vector<double, ScratchAllocator<double>> vec3{arena1};
        vec3.resize(100000);
        std::fill(vec3.begin(), vec3.end(), 789.0);
        for (auto i : vec1) {
            CHECK_EQUAL(i, 123);
        }
        for (auto d : vec3) {
            CHECK_EQUAL(d, 789.0);
        }
    }
    auto pos_after_1 = memory.get_current_position();
    CHECK_EQUAL(pos_before_1.bytes(), pos_after_1.bytes());
}

TEST(ScratchAllocator_UniquePtr)
{
    ScratchMemory memory;
    {
        ScratchArena arena{memory};
        std::unique_ptr<char[], ScratchDeleter<char[]>> ptr = util::make_unique<char[]>(arena, 1000);
    }
    CHECK_GREATER_EQUAL(memory.get_high_mark().bytes(), 1000);
}

TEST(ScratchAllocator_Vector)
{
    ScratchMemory memory;
    {
        ScratchArena space{memory};
        std::vector<int, ScratchAllocator<int>> myvec{space};
        for (int i = 0; i < 1000000; ++i) {
            myvec.push_back(i);
        }
    }
}

TEST(ScratchAllocator_Map)
{
    ScratchMemory memory;
    {
        ScratchArena space{memory};
        std::map<int, int, std::less<>, ScratchAllocator<std::pair<const int, int>>> mymap{space};

        for (int i = 0; i < 1000000; ++i) {
            mymap[i] = i;
        }
    }
}

TEST(ScratchAllocator_UnorderedMap)
{
    ScratchMemory memory;
    {
        ScratchArena space{memory};
        std::unordered_map<int, int, std::hash<int>, std::equal_to<>, ScratchAllocator<std::pair<const int, int>>>
            mymap{space};

        for (int i = 0; i < 1000000; ++i) {
            mymap[i] = i;
        }
    }
}

// Only provided for performance comparison with ScratchAllocator_Map.
// Doesn't test anything.
TEST_IF(ScratchAllocator_Map_operator_new, false)
{
    std::map<int, int> mymap;

    for (int i = 0; i < 1000000; ++i) {
        mymap[i] = i;
    }
}
