#include "testsettings.hpp"

#ifdef TEST_UTIL_FUNCTIONAL

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
}

} // namespace
} // namespace realm::util

#endif
