#include "testsettings.hpp"

#ifdef TEST_UTIL_FUNCTIONAL

#include <functional>
#include <type_traits>

#include "realm/util/functional.hpp"

#include "test.hpp"

namespace realm::util {
namespace {

struct MoveOnlyType {
    MoveOnlyType() noexcept = default;
    MoveOnlyType(MoveOnlyType&& other)
        : moved_from(false)
        , moved_to(true)
    {
        other.moved_from = true;
    }

    MoveOnlyType& operator=(MoveOnlyType&& other) noexcept
    {
        moved_to = true;
        other.moved_from = true;
        return *this;
    }

    MoveOnlyType(const MoveOnlyType&) = delete;
    MoveOnlyType& operator=(const MoveOnlyType&) = delete;

    bool moved_from = false;
    bool moved_to = false;
};

TEST(Util_UniqueFunction)
{
    static_assert(!std::is_copy_constructible_v<MoveOnlyType> && !std::is_copy_assignable_v<MoveOnlyType>);
    MoveOnlyType will_move;
    CHECK(!will_move.moved_from && !will_move.moved_to);
    bool function_called = false;
    UniqueFunction func([moved = std::move(will_move), &function_called, this] {
        CHECK(moved.moved_to && !moved.moved_from);
        function_called = true;
    });

    CHECK(will_move.moved_from);
    CHECK(static_cast<bool>(func));
    auto func_moved = std::move(func);
    CHECK(!static_cast<bool>(func));
    CHECK(static_cast<bool>(func_moved));
    CHECK(!function_called);
    func_moved();
    CHECK(function_called);
    CHECK(func_moved);

    // Check that we can construct noexcept UniqueFunction's that std::is_nothrow_invocable_r_v will be true for
    // (this is what Future's expect for their get_async callbacks).
    auto noexcept_fn = [](auto&& func) {
        static_assert(std::is_nothrow_invocable_r_v<void, decltype(func)>);
        func();
    };

    UniqueFunction<void() noexcept> fn = []() noexcept {};
    noexcept_fn(std::move(fn));

    // Check that std::swap works.
    UniqueFunction<int(int)> swap_a([](int a) {
        return a * a;
    });
    UniqueFunction<int(int)> swap_b;
    CHECK(swap_a);
    CHECK(!swap_b);

    std::swap(swap_a, swap_b);

    CHECK(swap_b);
    CHECK(!swap_a);

    // Check that std::swap works on noexcept UniqueFunction's
    UniqueFunction<int(int) noexcept> swap_a_noexcept([](int a) noexcept {
        return a * a;
    });
    UniqueFunction<int(int) noexcept> swap_b_noexcept;
    CHECK(swap_a_noexcept);
    CHECK(!swap_b_noexcept);

    std::swap(swap_a_noexcept, swap_b_noexcept);

    CHECK(swap_b_noexcept);
    CHECK(!swap_a_noexcept);
}

} // namespace
} // namespace realm::util

#endif
