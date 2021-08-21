#include "testsettings.hpp"

#ifdef TEST_UTIL_FUTURE

#include <thread>
#include <type_traits>

#include "realm/util/future.hpp"
#include "realm/utilities.hpp"

#include "test.hpp"

namespace realm::util {
namespace {

static_assert(std::is_same_v<FutureContinuationResult<std::function<void()>>, void>);
static_assert(std::is_same_v<FutureContinuationResult<std::function<Status()>>, void>);
static_assert(std::is_same_v<FutureContinuationResult<std::function<Future<void>()>>, void>);
static_assert(std::is_same_v<FutureContinuationResult<std::function<int()>>, int>);
static_assert(std::is_same_v<FutureContinuationResult<std::function<StatusWith<int>()>>, int>);
static_assert(std::is_same_v<FutureContinuationResult<std::function<Future<int>()>>, int>);
static_assert(std::is_same_v<FutureContinuationResult<std::function<int(bool)>, bool>, int>);

template <typename T>
auto overload_check(T) -> FutureContinuationResult<std::function<std::true_type(bool)>, T>;
auto overload_check(...) -> std::false_type;

static_assert(decltype(overload_check(bool()))::value);         // match.
static_assert(!decltype(overload_check(std::string()))::value); // SFINAE-failure.

template <typename T, typename Func>
void complete_promise(Promise<T>* promise, Func&& func)
{
    promise->emplace_value(func());
}

template <typename Func>
void complete_promise(Promise<void>* promise, Func&& func)
{
    func();
    promise->emplace_value();
}

template <typename Func, typename Result = std::result_of_t<Func && ()>>
Future<Result> async(Func&& func)
{
    auto pf = make_promise_future<Result>();

    std::thread([promise = std::move(pf.promise), func = std::forward<Func>(func)]() mutable {
#if !__has_feature(thread_sanitizer)
        // TSAN works better without this sleep, but it is useful for testing correctness.
        millisleep(100); // Try to wait until after the Future has been handled.
#endif
        try {
            complete_promise(&promise, func);
        }
        catch (...) {
            promise.set_error(exception_to_status());
        }
    }).detach();

    return std::move(pf.future);
}

const auto fail_status = Status(ErrorCodes::Error(10000), "expected failure");
const auto fail_status_2 = Status(ErrorCodes::Error(10001), "expected failure");
// Tests a Future completed by completionExpr using testFunc. The Future will be completed in
// various ways to maximize test coverage.
template <typename CompletionFunc, typename TestFunc,
          typename = std::enable_if_t<!std::is_void_v<std::result_of_t<CompletionFunc()>>>>
void FUTURE_SUCCESS_TEST(const CompletionFunc& completion, const TestFunc& test) noexcept
{
    using CompletionType = decltype(completion());
    { // immediate future
        test(Future<CompletionType>::make_ready(completion()));
    }
    { // ready future from promise
        auto pf = make_promise_future<CompletionType>();
        pf.promise.emplace_value(completion());
        test(std::move(pf.future));
    }

    { // async future
        test(async([&] {
            return completion();
        }));
    }
}

template <typename CompletionFunc, typename TestFunc,
          typename = std::enable_if_t<std::is_void_v<std::result_of_t<CompletionFunc()>>>, typename = void>
void FUTURE_SUCCESS_TEST(const CompletionFunc& completion, const TestFunc& test) noexcept
{
    using CompletionType = decltype(completion());
    { // immediate future
        completion();
        test(Future<CompletionType>::make_ready());
    }
    { // ready future from promise
        auto pf = make_promise_future<CompletionType>();
        completion();
        pf.promise.emplace_value();
        test(std::move(pf.future));
    }

    { // async future
        test(async([&] {
            return completion();
        }));
    }
}

template <typename CompletionType, typename TestFunc>
void FUTURE_FAIL_TEST(const TestFunc& test) noexcept
{
    { // immediate future
        test(Future<CompletionType>::make_ready(fail_status));
    }
    { // ready future from promise
        auto pf = make_promise_future<CompletionType>();
        pf.promise.set_error(fail_status);
        test(std::move(pf.future));
    }

    { // async future
        test(async([&]() -> CompletionType {
            throw ExceptionForStatus(fail_status);
            REALM_UNREACHABLE();
        }));
    }
}

TEST(Future_Success_getLvalue)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
            CHECK_EQUAL(fut.get(), 1);
        });
}

TEST(Future_Success_getConstLvalue)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](const Future<int>& fut) {
            CHECK_EQUAL(fut.get(), 1);
        });
}

TEST(Future_Success_getRvalue)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
            CHECK_EQUAL(std::move(fut).get(), 1);
        });
}

TEST(Future_Success_getNothrowLvalue)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
            CHECK_EQUAL(fut.get_no_throw(), 1);
        });
}

TEST(Future_Success_getNothrowConstLvalue)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](const Future<int>& fut) {
            CHECK_EQUAL(fut.get_no_throw(), 1);
        });
}

TEST(Future_Success_getNothrowRvalue)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
            CHECK_EQUAL(std::move(fut).get_no_throw(), 1);
        });
}

TEST(Future_Success_getAsync)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
            auto pf = make_promise_future<int>();
            std::move(fut).get_async([outside = std::move(pf.promise), this](StatusWith<int> sw) mutable {
                CHECK(sw.is_ok());
                outside.emplace_value(sw.get_value());
            });
            CHECK_EQUAL(std::move(pf.future).get(), 1);
        });
}

TEST(Future_Fail_getLvalue)
{
    FUTURE_FAIL_TEST<int>([&](Future<int>&& fut) {
        CHECK_THROW_EX(fut.get(), ExceptionForStatus, (e.to_status() == fail_status));
    });
}

TEST(Future_Fail_getConstLvalue)
{
    FUTURE_FAIL_TEST<int>([&](const Future<int>& fut) {
        CHECK_THROW_EX(fut.get(), ExceptionForStatus, (e.to_status() == fail_status));
    });
}

TEST(Future_Fail_getRvalue)
{
    FUTURE_FAIL_TEST<int>([&](const Future<int>& fut) {
        CHECK_THROW_EX(std::move(fut).get(), ExceptionForStatus, (e.to_status() == fail_status));
    });
}

TEST(Future_Fail_getNothrowLvalue)
{
    FUTURE_FAIL_TEST<int>([&](Future<int>&& fut) {
        CHECK_EQUAL(fut.get_no_throw(), fail_status);
    });
}

TEST(Future_Fail_getNothrowConstLvalue)
{
    FUTURE_FAIL_TEST<int>([&](const Future<int>& fut) {
        CHECK_EQUAL(fut.get_no_throw(), fail_status);
    });
}

TEST(Future_Fail_getNothrowRvalue)
{
    FUTURE_FAIL_TEST<int>([&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut).get_no_throw(), fail_status);
    });
}

TEST(Future_Fail_getAsync)
{
    FUTURE_FAIL_TEST<int>([&](Future<int>&& fut) {
        auto pf = make_promise_future<int>();
        std::move(fut).get_async([outside = std::move(pf.promise), this](StatusWith<int> sw) mutable {
            CHECK(!sw.is_ok());
            outside.set_error(sw.get_status());
        });
        CHECK_EQUAL(std::move(pf.future).get_no_throw(), fail_status);
    });
}

TEST(Future_Success_isReady)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
            const auto id = std::this_thread::get_id();
            while (!fut.is_ready()) {
            }
            std::move(fut).get_async([&](StatusWith<int> status) {
                CHECK_EQUAL(std::this_thread::get_id(), id);
                CHECK_EQUAL(status, 1);
            });
        });
}

TEST(Future_Fail_isReady)
{
    FUTURE_FAIL_TEST<int>([&](Future<int>&& fut) {
        const auto id = std::this_thread::get_id();
        while (!fut.is_ready()) {
        }
        std::move(fut).get_async([&](StatusWith<int> status) {
            CHECK_EQUAL(std::this_thread::get_id(), id);
            CHECK(!status.is_ok());
        });
    });
}

TEST(Future_Void_Success_getLvalue)
{
    FUTURE_SUCCESS_TEST([] {},
                        [](Future<void>&& fut) {
                            fut.get();
                        });
}

TEST(Future_Void_Success_getConstLvalue)
{
    FUTURE_SUCCESS_TEST([] {},
                        [](const Future<void>& fut) {
                            fut.get();
                        });
}

TEST(Future_Void_Success_getRvalue)
{
    FUTURE_SUCCESS_TEST([] {},
                        [](Future<void>&& fut) {
                            std::move(fut).get();
                        });
}

TEST(Future_Void_Success_getNothrowLvalue)
{
    FUTURE_SUCCESS_TEST([] {},
                        [&](Future<void>&& fut) {
                            CHECK_EQUAL(fut.get_no_throw(), Status::OK());
                        });
}

TEST(Future_Void_Success_getNothrowConstLvalue)
{
    FUTURE_SUCCESS_TEST([] {},
                        [&](const Future<void>& fut) {
                            CHECK_EQUAL(fut.get_no_throw(), Status::OK());
                        });
}

TEST(Future_Void_Success_getNothrowRvalue)
{
    FUTURE_SUCCESS_TEST([] {},
                        [&](Future<void>&& fut) {
                            CHECK_EQUAL(std::move(fut).get_no_throw(), Status::OK());
                        });
}

TEST(Future_Void_Success_getAsync)
{
    FUTURE_SUCCESS_TEST([] {},
                        [&](Future<void>&& fut) {
                            auto pf = make_promise_future<void>();
                            std::move(fut).get_async([outside = std::move(pf.promise), this](Status status) mutable {
                                CHECK(status.is_ok());
                                outside.emplace_value();
                            });
                            CHECK_EQUAL(std::move(pf.future).get_no_throw(), Status::OK());
                        });
}

TEST(Future_Void_Fail_getLvalue)
{
    FUTURE_FAIL_TEST<void>([&](Future<void>&& fut) {
        CHECK_THROW_EX(fut.get(), ExceptionForStatus, (e.to_status() == fail_status));
    });
}

TEST(Future_Void_Fail_getConstLvalue)
{
    FUTURE_FAIL_TEST<void>([&](const Future<void>& fut) {
        CHECK_THROW_EX(fut.get(), ExceptionForStatus, (e.to_status() == fail_status));
    });
}

TEST(Future_Void_Fail_getRvalue)
{
    FUTURE_FAIL_TEST<void>([&](Future<void>&& fut) {
        CHECK_THROW_EX(fut.get(), ExceptionForStatus, (e.to_status() == fail_status));
    });
}

TEST(Future_Void_Fail_getNothrowLvalue)
{
    FUTURE_FAIL_TEST<void>([&](Future<void>&& fut) {
        CHECK_EQUAL(fut.get_no_throw(), fail_status);
    });
}

TEST(Future_Void_Fail_getNothrowConstLvalue)
{
    FUTURE_FAIL_TEST<void>([&](const Future<void>& fut) {
        CHECK_EQUAL(fut.get_no_throw(), fail_status);
    });
}

TEST(Future_Void_Fail_getNothrowRvalue)
{
    FUTURE_FAIL_TEST<void>([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut).get_no_throw(), fail_status);
    });
}

TEST(Future_Void_Fail_getAsync)
{
    FUTURE_FAIL_TEST<void>([&](Future<void>&& fut) {
        auto pf = make_promise_future<void>();
        std::move(fut).get_async([outside = std::move(pf.promise), this](Status status) mutable {
            CHECK(!status.is_ok());
            outside.set_error(status);
        });
        CHECK_EQUAL(std::move(pf.future).get_no_throw(), fail_status);
    });
}

TEST(Future_Void_Success_isReady)
{
    FUTURE_SUCCESS_TEST([] {},
                        [&](Future<void>&& fut) {
                            const auto id = std::this_thread::get_id();
                            while (!fut.is_ready()) {
                            }
                            std::move(fut).get_async([&](Status status) {
                                CHECK_EQUAL(std::this_thread::get_id(), id);
                                CHECK(status.is_ok());
                            });
                        });
}

TEST(Future_Void_Fail_isReady)
{
    FUTURE_FAIL_TEST<void>([&](Future<void>&& fut) {
        const auto id = std::this_thread::get_id();
        while (!fut.is_ready()) {
        }
        std::move(fut).get_async([&](Status status) {
            CHECK_EQUAL(std::this_thread::get_id(), id);
            CHECK(!status.is_ok());
        });
    });
}


// A move-only type that isn't default constructible. It has binary ops with int to make it easier
// to have a common format with the above tests.
struct Widget {
    explicit Widget(int val)
        : val(val)
    {
    }

    Widget(Widget&& other)
        : Widget(other.val)
    {
    }
    Widget& operator=(Widget&& other)
    {
        val = other.val;
        return *this;
    }

    Widget() = delete;
    Widget(const Widget&) = delete;
    Widget& operator=(const Widget&) = delete;

    Widget operator+(int i) const
    {
        return Widget(val + i);
    }

    bool operator==(int i) const
    {
        return val == i;
    }

    bool operator==(Widget w) const
    {
        return val == w.val;
    }

    int val;
};

std::ostream& operator<<(std::ostream& stream, const Widget& widget)
{
    return stream << "Widget(" << widget.val << ')';
}

TEST(Future_MoveOnly_Success_getLvalue)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(fut.get(), 1);
        });
}

TEST(Future_MoveOnly_Success_getConstLvalue)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return Widget(1);
        },
        [&](const Future<Widget>& fut) {
            CHECK_EQUAL(fut.get(), 1);
        });
}

TEST(Future_MoveOnly_Success_getRvalue)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(std::move(fut).get(), 1);
        });
}

#if 0 // Needs copy
TEST(Future_MoveOnly_Success_getNothrowLvalue) {
    FUTURE_SUCCESS_TEST([] { return Widget(1); },
                        [&](Future<Widget>&& fut) { CHECK_EQUAL(fut.get_no_throw(), 1); });
}

TEST(Future_MoveOnly_Success_getNothrowConstLvalue) {
    FUTURE_SUCCESS_TEST([] { return Widget(1); },
                        [&](const Future<Widget>& fut) { CHECK_EQUAL(fut.get_no_throw(), 1); });
}
#endif

TEST(Future_MoveOnly_Success_getNothrowRvalue)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            auto sw_widget = std::move(fut).get_no_throw();
            CHECK(sw_widget.is_ok());
            CHECK_EQUAL(sw_widget.get_value().val, 1);
        });
}

TEST(Future_MoveOnly_Success_getAsync)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            auto pf = make_promise_future<Widget>();
            std::move(fut).get_async([outside = std::move(pf.promise), this](StatusWith<Widget> sw) mutable {
                CHECK(sw.is_ok());
                outside.emplace_value(std::move(sw.get_value()));
            });
            CHECK_EQUAL(std::move(pf.future).get(), 1);
        });
}

TEST(Future_MoveOnly_Fail_getLvalue)
{
    FUTURE_FAIL_TEST<Widget>([&](Future<Widget>&& fut) {
        CHECK_THROW_EX(fut.get(), ExceptionForStatus, (e.to_status() == fail_status));
    });
}

TEST(Future_MoveOnly_Fail_getConstLvalue)
{
    FUTURE_FAIL_TEST<Widget>([&](const Future<Widget>& fut) {
        CHECK_THROW_EX(fut.get(), ExceptionForStatus, (e.to_status() == fail_status));
    });
}

TEST(Future_MoveOnly_Fail_getRvalue)
{
    FUTURE_FAIL_TEST<Widget>([&](Future<Widget>&& fut) {
        CHECK_THROW_EX(fut.get(), ExceptionForStatus, (e.to_status() == fail_status));
    });
}

#if 0 // Needs copy
TEST(Future_MoveOnly_Fail_getNothrowLvalue) {
    FUTURE_FAIL_TEST<Widget>([&](Future<Widget>&& fut) { CHECK_EQUAL(fut.get_no_throw(), failStatus); });
}

TEST(Future_MoveOnly_Fail_getNothrowConstLvalue) {
    FUTURE_FAIL_TEST<Widget>(
        [&](const Future<Widget>& fut) { CHECK_EQUAL(fut.get_no_throw(), failStatus); });
}
#endif

TEST(Future_MoveOnly_Fail_getNothrowRvalue)
{
    FUTURE_FAIL_TEST<Widget>([&](Future<Widget>&& fut) {
        CHECK_EQUAL(std::move(fut).get_no_throw().get_status(), fail_status);
    });
}

TEST(Future_MoveOnly_Fail_getAsync)
{
    FUTURE_FAIL_TEST<Widget>([&](Future<Widget>&& fut) {
        auto pf = make_promise_future<Widget>();
        std::move(fut).get_async([outside = std::move(pf.promise), this](StatusWith<Widget> sw) mutable {
            CHECK(!sw.is_ok());
            outside.set_error(sw.get_status());
        });
        CHECK_EQUAL(std::move(pf.future).get_no_throw(), fail_status);
    });
}

TEST(Promise_Success_setFrom)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
            auto pf = make_promise_future<int>();
            pf.promise.set_from(std::move(fut));
            CHECK_EQUAL(std::move(pf.future).get(), 1);
        });
}

TEST(Promise_Fail_setFrom)
{
    FUTURE_FAIL_TEST<int>([&](Future<int>&& fut) {
        auto pf = make_promise_future<int>();
        pf.promise.set_from(std::move(fut));
        CHECK_THROW_EX(std::move(pf.future).get(), ExceptionForStatus, (e.to_status() == fail_status));
    });
}

TEST(Promise_void_Success_setFrom)
{
    FUTURE_SUCCESS_TEST([] {},
                        [&](Future<void>&& fut) {
                            auto pf = make_promise_future<void>();
                            pf.promise.set_from(std::move(fut));
                            CHECK(std::move(pf.future).get_no_throw().is_ok());
                        });
}

TEST(Promise_void_Fail_setFrom)
{
    FUTURE_FAIL_TEST<void>([&](Future<void>&& fut) {
        auto pf = make_promise_future<void>();
        pf.promise.set_from(std::move(fut));
        CHECK_THROW_EX(std::move(pf.future).get(), ExceptionForStatus, (e.to_status() == fail_status));
    });
}

} // namespace
} // namespace realm::util

#endif
