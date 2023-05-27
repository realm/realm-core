#include "testsettings.hpp"

#ifdef TEST_UTIL_FUTURE

#include <thread>
#include <type_traits>

#include "realm/util/features.h"
#include "realm/util/future.hpp"
#include "realm/utilities.hpp"

#include "test.hpp"

namespace realm::util {
namespace {

static_assert(std::is_same_v<FutureContinuationResult<std::function<void()>>, void>);
static_assert(std::is_same_v<FutureContinuationResult<std::function<Status()>>, void>);
static_assert(std::is_same_v<FutureContinuationResult<std::function<Future<void>()>>, void>);
static_assert(std::is_same_v<FutureContinuationResult<std::function<int()>>, int>);
static_assert(std::is_same_v<FutureContinuationResult<std::function<Expected<int>()>>, int>);
static_assert(std::is_same_v<FutureContinuationResult<std::function<Future<int>()>>, int>);
static_assert(std::is_same_v<FutureContinuationResult<std::function<int(bool)>, bool>, int>);

template <typename T>
auto overload_check(T) -> FutureContinuationResult<std::function<std::true_type(bool)>, T>;
[[maybe_unused]] auto overload_check(...) -> std::false_type;

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

template <typename Func, typename Result = std::invoke_result_t<Func>>
Future<Result> async(Func&& func)
{
    auto pf = make_promise_future<Result>();

    std::thread([promise = std::move(pf.promise), func = std::forward<Func>(func)]() mutable {
#if !REALM_SANITIZE_THREAD
        // TSAN works better without this sleep, but it is useful for testing correctness.
        millisleep(100); // Try to wait until after the Future has been handled.
#endif
        try {
            complete_promise(&promise, func);
        }
        catch (...) {
            promise.set_from(exception_to_status());
        }
    }).detach();

    return std::move(pf.future);
}

const auto fail_status = Status(ErrorCodes::Error(10000), "expected failure");
const auto fail_status_2 = Status(ErrorCodes::Error(10001), "expected failure");
// Tests a Future completed by completionExpr using testFunc. The Future will be completed in
// various ways to maximize test coverage.
template <typename CompletionFunc, typename TestFunc,
          typename = std::enable_if_t<!std::is_void_v<std::invoke_result_t<CompletionFunc>>>>
void future_success_test(const CompletionFunc& completion, const TestFunc& test) noexcept
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

template <typename TestFunc>
void future_success_test(const TestFunc& test) noexcept
{
    { // immediate future
        test(Future<void>::make_ready());
    }
    { // ready future from promise
        auto pf = make_promise_future<void>();
        pf.promise.emplace_value();
        test(std::move(pf.future));
    }

    { // async future
        test(async([] {}));
    }
}

template <typename CompletionType, typename TestFunc>
void future_failure_test(const TestFunc& test) noexcept
{
    { // immediate future
        test(Future<CompletionType>::make_ready(fail_status));
    }
    { // ready future from promise
        auto pf = make_promise_future<CompletionType>();
        pf.promise.set_from(fail_status);
        test(std::move(pf.future));
    }

    { // async future
        test(async([&]() -> CompletionType {
            throw Exception(fail_status);
            REALM_UNREACHABLE();
        }));
    }
}

template <typename T, typename TestFunc, typename = std::enable_if_t<!std::is_invocable_v<T>>>
void future_success_test(T value, const TestFunc& test) noexcept
{
    { // immediate future
        test(Future<T>::make_ready(value));
    }
    { // ready future from promise
        auto pf = make_promise_future<T>();
        pf.promise.emplace_value(value);
        test(std::move(pf.future));
    }

    { // async future
        test(async([&] {
            return value;
        }));
    }
}

TEST(Future_Success_getLvalue)
{
    future_success_test(1, [&](Future<int>&& fut) {
        CHECK_EQUAL(fut.get(), 1);
    });
}

TEST(Future_Success_getConstLvalue)
{
    future_success_test(1, [&](const Future<int>& fut) {
        CHECK_EQUAL(fut.get(), 1);
    });
}

TEST(Future_Success_getRvalue)
{
    future_success_test(1, [&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut).get(), 1);
    });
}

TEST(Future_Success_getNothrowLvalue)
{
    future_success_test(1, [&](Future<int>&& fut) {
        CHECK_EQUAL(fut.get_no_throw(), 1);
    });
}

TEST(Future_Success_getNothrowConstLvalue)
{
    future_success_test(1, [&](const Future<int>& fut) {
        CHECK_EQUAL(fut.get_no_throw(), 1);
    });
}

TEST(Future_Success_getNothrowRvalue)
{
    future_success_test(1, [&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut).get_no_throw(), 1);
    });
}

TEST(Future_Success_getAsync)
{
    future_success_test(1, [&](Future<int>&& fut) {
        auto pf = make_promise_future<int>();
        std::move(fut).get_async([outside = std::move(pf.promise), this](Expected<int>&& sw) mutable noexcept {
            CHECK(sw.has_value());
            outside.emplace_value(*sw);
        });
        CHECK_EQUAL(std::move(pf.future).get(), 1);
    });
}

TEST(Future_Fail_getLvalue)
{
    future_failure_test<int>([&](Future<int>&& fut) {
        CHECK_THROW_EX(fut.get(), Exception, (e.to_status() == fail_status));
    });
}

TEST(Future_Fail_getConstLvalue)
{
    future_failure_test<int>([&](const Future<int>& fut) {
        CHECK_THROW_EX(fut.get(), Exception, (e.to_status() == fail_status));
    });
}

TEST(Future_Fail_getRvalue)
{
    future_failure_test<int>([&](const Future<int>& fut) {
        CHECK_THROW_EX(std::move(fut).get(), Exception, (e.to_status() == fail_status));
    });
}

TEST(Future_Fail_getNothrowLvalue)
{
    future_failure_test<int>([&](Future<int>&& fut) {
        CHECK_EQUAL(fut.get_no_throw(), fail_status);
    });
}

TEST(Future_Fail_getNothrowConstLvalue)
{
    future_failure_test<int>([&](const Future<int>& fut) {
        CHECK_EQUAL(fut.get_no_throw(), fail_status);
    });
}

TEST(Future_Fail_getNothrowRvalue)
{
    future_failure_test<int>([&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut).get_no_throw(), fail_status);
    });
}

TEST(Future_Fail_getAsync)
{
    future_failure_test<int>([&](Future<int>&& fut) {
        auto pf = make_promise_future<int>();
        std::move(fut).get_async([outside = std::move(pf.promise), this](Expected<int>&& sw) mutable noexcept {
            CHECK(!sw.has_value());
            outside.set_from(sw);
        });
        CHECK_EQUAL(std::move(pf.future).get_no_throw(), fail_status);
    });
}

TEST(Future_Success_isReady)
{
    future_success_test(1, [&](Future<int>&& fut) {
        const auto id = std::this_thread::get_id();
        while (!fut.is_ready()) {
        }
        std::move(fut).get_async([&](Expected<int>&& status) noexcept {
            CHECK_EQUAL(std::this_thread::get_id(), id);
            CHECK_EQUAL(status, 1);
        });
    });
}

TEST(Future_Fail_isReady)
{
    future_failure_test<int>([&](Future<int>&& fut) {
        const auto id = std::this_thread::get_id();
        while (!fut.is_ready()) {
        }
        std::move(fut).get_async([&](Expected<int>&& status) noexcept {
            CHECK_EQUAL(std::this_thread::get_id(), id);
            CHECK(!status.has_value());
        });
    });
}

TEST(Future_Success_thenSimple)
{
    future_success_test(1, [&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([](int i) {
                            return i + 2;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Success_thenSimpleAuto)
{
    future_success_test(1, [&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([](auto i) {
                            return i + 2;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Success_thenVoid)
{
    future_success_test(1, [&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([&](int i) {
                            CHECK_EQUAL(i, 1);
                        })
                        .then([] {
                            return 3;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Success_thenStatus)
{
    future_success_test(1, [&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([&](int i) {
                            CHECK_EQUAL(i, 1);
                            return Status::OK();
                        })
                        .then([] {
                            return 3;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Success_thenError_Status)
{
    future_success_test(1, [&](Future<int>&& fut) {
        auto fut2 = std::move(fut).then([](int) {
            return fail_status;
        });
        static_assert(std::is_same_v<decltype(fut2), Future<void>>);
        CHECK_THROW_EX(fut2.get(), Exception, (e.to_status() == fail_status));
    });
}


TEST(Future_Success_thenError_Expected)
{
    future_success_test(1, [&](Future<int>&& fut) {
        auto fut2 = std::move(fut).then([&](int) {
            return Expected<double>(fail_status);
        });
        static_assert(std::is_same_v<decltype(fut2), Future<double>>);
        CHECK_THROW_EX(fut2.get(), Exception, (e.to_status() == fail_status));
    });
}

TEST(Future_Success_thenFutureImmediate)
{
    future_success_test(1, [&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([](int i) {
                            return Future<int>::make_ready(i + 2);
                        })
                        .get(),
                    3);
    });
}


TEST(Future_Success_thenFutureReady)
{
    future_success_test(1, [&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([](int i) {
                            auto pf = make_promise_future<int>();
                            pf.promise.emplace_value(i + 2);
                            return std::move(pf.future);
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Success_thenFutureAsync)
{
    future_success_test(1, [&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([](int i) {
                            return async([i] {
                                return i + 2;
                            });
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Success_thenFutureAsyncThrow)
{
    future_success_test(1, [&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([](int) {
                            throw Exception(fail_status);
                            return Future<int>();
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_Fail_thenSimple)
{
    future_failure_test<int>([&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([](int) {
                            throw Exception(fail_status);
                            return int();
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_Fail_thenFutureAsync)
{
    future_failure_test<int>([&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([](int i) {
                            throw Exception(fail_status);
                            return Future<int>(i + 1);
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_Success_onErrorSimple)
{
    future_success_test(1, [&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([](Status) {
                            throw Exception(fail_status);
                            return 0;
                        })
                        .then([](int i) {
                            return i + 2;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Success_onErrorFutureAsync)
{
    future_success_test(1, [&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([](Status) {
                            throw Exception(fail_status);
                            return Future<int>();
                        })
                        .then([](int i) {
                            return i + 2;
                        })
                        .get(),
                    3);
    });
}


TEST(Future_Fail_onErrorSimple)
{
    future_failure_test<int>([&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Expected<void>&& s) noexcept {
                            CHECK_EQUAL(s, fail_status);
                            return 3;
                        })
                        .get_no_throw(),
                    3);
    });
}

TEST(Future_Fail_onErrorError_throw)
{
    future_failure_test<int>([&](Future<int>&& fut) {
        auto fut2 = std::move(fut).on_error([&](Expected<void>&& s) -> int {
            CHECK_EQUAL(s, fail_status);
            throw Exception(fail_status_2);
        });
        CHECK_EQUAL(fut2.get_no_throw(), fail_status_2);
    });
}

TEST(Future_Fail_onErrorError_Expected)
{
    future_failure_test<int>([&](Future<int>&& fut) {
        auto fut2 = std::move(fut).on_error([&](Expected<void>&& s) noexcept {
            CHECK_EQUAL(s, fail_status);
            return Expected<int>(fail_status_2);
        });
        CHECK_EQUAL(fut2.get_no_throw(), fail_status_2);
    });
}

TEST(Future_Fail_onErrorFutureImmediate)
{
    future_failure_test<int>([&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Expected<void>&& s) noexcept {
                            CHECK_EQUAL(s, fail_status);
                            return Future<int>::make_ready(3);
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Fail_onErrorFutureReady)
{
    future_failure_test<int>([&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Expected<void>&& s) noexcept {
                            CHECK_EQUAL(s, fail_status);
                            auto pf = make_promise_future<int>();
                            pf.promise.emplace_value(3);
                            return std::move(pf.future);
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Fail_onErrorFutureAsync)
{
    future_failure_test<int>([&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Expected<void>&& s) noexcept {
                            CHECK_EQUAL(s, fail_status);
                            return async([] {
                                return 3;
                            });
                        })
                        .get(),
                    3);
    });
}


TEST(Future_Void_Success_getLvalue)
{
    future_success_test([](Future<void>&& fut) {
        fut.get();
    });
}

TEST(Future_Void_Success_getConstLvalue)
{
    future_success_test([](const Future<void>& fut) {
        fut.get();
    });
}

TEST(Future_Void_Success_getRvalue)
{
    future_success_test([](Future<void>&& fut) {
        std::move(fut).get();
    });
}

TEST(Future_Void_Success_getNothrowLvalue)
{
    future_success_test([&](Future<void>&& fut) {
        CHECK_EQUAL(fut.get_no_throw(), Status::OK());
    });
}

TEST(Future_Void_Success_getNothrowConstLvalue)
{
    future_success_test([&](const Future<void>& fut) {
        CHECK_EQUAL(fut.get_no_throw(), Status::OK());
    });
}

TEST(Future_Void_Success_getNothrowRvalue)
{
    future_success_test([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut).get_no_throw(), Status::OK());
    });
}

TEST(Future_Void_Success_getAsync)
{
    future_success_test([&](Future<void>&& fut) {
        auto pf = make_promise_future<void>();
        std::move(fut).get_async([outside = std::move(pf.promise), this](Expected<void>&& status) mutable noexcept {
            CHECK(status.has_value());
            outside.emplace_value();
        });
        CHECK_EQUAL(std::move(pf.future).get_no_throw(), Status::OK());
    });
}

TEST(Future_Void_Fail_getLvalue)
{
    future_failure_test<void>([&](Future<void>&& fut) {
        CHECK_THROW_EX(fut.get(), Exception, (e.to_status() == fail_status));
    });
}

TEST(Future_Void_Fail_getConstLvalue)
{
    future_failure_test<void>([&](const Future<void>& fut) {
        CHECK_THROW_EX(fut.get(), Exception, (e.to_status() == fail_status));
    });
}

TEST(Future_Void_Fail_getRvalue)
{
    future_failure_test<void>([&](Future<void>&& fut) {
        CHECK_THROW_EX(fut.get(), Exception, (e.to_status() == fail_status));
    });
}

TEST(Future_Void_Fail_getNothrowLvalue)
{
    future_failure_test<void>([&](Future<void>&& fut) {
        CHECK_EQUAL(fut.get_no_throw(), fail_status);
    });
}

TEST(Future_Void_Fail_getNothrowConstLvalue)
{
    future_failure_test<void>([&](const Future<void>& fut) {
        CHECK_EQUAL(fut.get_no_throw(), fail_status);
    });
}

TEST(Future_Void_Fail_getNothrowRvalue)
{
    future_failure_test<void>([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut).get_no_throw(), fail_status);
    });
}

TEST(Future_Void_Fail_getAsync)
{
    future_failure_test<void>([&](Future<void>&& fut) {
        auto pf = make_promise_future<void>();
        std::move(fut).get_async([outside = std::move(pf.promise), this](Expected<void>&& status) mutable noexcept {
            CHECK(!status.has_value());
            outside.set_from(status);
        });
        CHECK_EQUAL(std::move(pf.future).get_no_throw(), fail_status);
    });
}

TEST(Future_Void_Success_isReady)
{
    future_success_test([&](Future<void>&& fut) {
        const auto id = std::this_thread::get_id();
        while (!fut.is_ready()) {
        }
        std::move(fut).get_async([&](Expected<void>&& result) noexcept {
            CHECK_EQUAL(std::this_thread::get_id(), id);
            CHECK(result.has_value());
        });
    });
}

TEST(Future_Void_Fail_isReady)
{
    future_failure_test<void>([&](Future<void>&& fut) {
        const auto id = std::this_thread::get_id();
        while (!fut.is_ready()) {
        }
        std::move(fut).get_async([&](Expected<void>&& result) noexcept {
            CHECK_EQUAL(std::this_thread::get_id(), id);
            CHECK(!result.has_value());
        });
    });
}

TEST(Future_Void_Success_thenSimple)
{
    future_success_test([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([]() {
                            return 3;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Success_thenVoid)
{
    future_success_test([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([] {})
                        .then([] {
                            return 3;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Success_thenStatus)
{
    future_success_test([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([] {})
                        .then([] {
                            return 3;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Success_thenError_Status)
{
    future_success_test([&](Future<void>&& fut) {
        auto fut2 = std::move(fut).then([]() {
            return fail_status;
        });
        static_assert(std::is_same_v<decltype(fut2), Future<void>>);
        CHECK_EQUAL(fut2.get_no_throw(), fail_status);
    });
}

TEST(Future_Void_Success_thenError_Expected)
{
    future_success_test([&](Future<void>&& fut) {
        auto fut2 = std::move(fut).then([]() {
            return Expected<double>(fail_status);
        });
        static_assert(std::is_same_v<decltype(fut2), Future<double>>);
        CHECK_EQUAL(fut2.get_no_throw(), fail_status);
    });
}

TEST(Future_Void_Success_thenFutureImmediate)
{
    future_success_test([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([]() {
                            return Future<int>::make_ready(3);
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Success_thenFutureReady)
{
    future_success_test([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([]() {
                            auto pf = make_promise_future<int>();
                            pf.promise.emplace_value(3);
                            return std::move(pf.future);
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Success_thenFutureAsync)
{
    future_success_test([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([]() {
                            return async([] {
                                return 3;
                            });
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Fail_thenSimple)
{
    future_failure_test<void>([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([]() {
                            throw Exception(fail_status);
                            return int();
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_Void_Fail_thenFutureAsync)
{
    future_failure_test<void>([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([]() {
                            throw Exception(fail_status);
                            return Future<int>();
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_Void_Success_onErrorSimple)
{
    future_success_test([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([](Status) {
                            throw Exception(fail_status);
                        })
                        .then([] {
                            return 3;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Success_onErrorFutureAsync)
{
    future_success_test([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([](Status) {
                            throw Exception(fail_status);
                            return Future<void>();
                        })
                        .then([] {
                            return 3;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Fail_onErrorSimple)
{
    future_failure_test<void>([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Expected<void>&& s) noexcept {
                            CHECK_EQUAL(s, fail_status);
                        })
                        .then([] {
                            return 3;
                        })
                        .get_no_throw(),
                    3);
    });
}

TEST(Future_Void_Fail_onErrorError_throw)
{
    future_failure_test<void>([&](Future<void>&& fut) {
        auto fut2 = std::move(fut).on_error([&](Expected<void>&& s) {
            CHECK_EQUAL(s, fail_status);
            throw Exception(fail_status_2);
        });
        CHECK_EQUAL(fut2.get_no_throw(), fail_status_2);
    });
}

TEST(Future_Void_Fail_onErrorError_Status)
{
    future_failure_test<void>([&](Future<void>&& fut) {
        auto fut2 = std::move(fut).on_error([&](Expected<void>&& s) noexcept {
            CHECK_EQUAL(s, fail_status);
            return fail_status_2;
        });
        CHECK_EQUAL(fut2.get_no_throw(), fail_status_2);
    });
}

TEST(Future_Void_Fail_onErrorFutureImmediate)
{
    future_failure_test<void>([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Expected<void>&& s) noexcept {
                            CHECK_EQUAL(s, fail_status);
                            return Future<void>::make_ready();
                        })
                        .then([] {
                            return 3;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Fail_onErrorFutureReady)
{
    future_failure_test<void>([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Expected<void>&& s) noexcept {
                            CHECK_EQUAL(s, fail_status);
                            auto pf = make_promise_future<void>();
                            pf.promise.emplace_value();
                            return std::move(pf.future);
                        })
                        .then([] {
                            return 3;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Fail_onErrorFutureAsync)
{
    future_failure_test<void>([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Expected<void>&& s) noexcept {
                            CHECK_EQUAL(s, fail_status);
                            return async([] {});
                        })
                        .then([] {
                            return 3;
                        })
                        .get(),
                    3);
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

    int val{0};
};

std::ostream& operator<<(std::ostream& stream, const Widget& widget)
{
    return stream << "Widget(" << widget.val << ')';
}

TEST(Future_MoveOnly_Success_getLvalue)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(fut.get(), 1);
        });
}

TEST(Future_MoveOnly_Success_getConstLvalue)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](const Future<Widget>& fut) {
            CHECK_EQUAL(fut.get(), 1);
        });
}

TEST(Future_MoveOnly_Success_getRvalue)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(std::move(fut).get(), 1);
        });
}

#if 0 // Needs copy
TEST(Future_MoveOnly_Success_getNothrowLvalue) {
    future_success_test([] { return Widget(1); },
                        [&](Future<Widget>&& fut) { CHECK_EQUAL(fut.get_no_throw(), 1); });
}

TEST(Future_MoveOnly_Success_getNothrowConstLvalue) {
    future_success_test([] { return Widget(1); },
                        [&](const Future<Widget>& fut) { CHECK_EQUAL(fut.get_no_throw(), 1); });
}
#endif

TEST(Future_MoveOnly_Success_getNothrowRvalue)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            auto sw_widget = std::move(fut).get_no_throw();
            CHECK(sw_widget.has_value());
            CHECK_EQUAL(sw_widget->val, 1);
        });
}

TEST(Future_MoveOnly_Success_getAsync)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            auto pf = make_promise_future<Widget>();
            std::move(fut).get_async([outside = std::move(pf.promise), this](Expected<Widget>&& sw) mutable noexcept {
                CHECK(sw.has_value());
                outside.emplace_value(std::move(*sw));
            });
            CHECK_EQUAL(std::move(pf.future).get(), 1);
        });
}

TEST(Future_MoveOnly_Fail_getLvalue)
{
    future_failure_test<Widget>([&](Future<Widget>&& fut) {
        CHECK_THROW_EX(fut.get(), Exception, (e.to_status() == fail_status));
    });
}

TEST(Future_MoveOnly_Fail_getConstLvalue)
{
    future_failure_test<Widget>([&](const Future<Widget>& fut) {
        CHECK_THROW_EX(fut.get(), Exception, (e.to_status() == fail_status));
    });
}

TEST(Future_MoveOnly_Fail_getRvalue)
{
    future_failure_test<Widget>([&](Future<Widget>&& fut) {
        CHECK_THROW_EX(fut.get(), Exception, (e.to_status() == fail_status));
    });
}

#if 0 // Needs copy
TEST(Future_MoveOnly_Fail_getNothrowLvalue) {
    future_failure_test<Widget>([&](Future<Widget>&& fut) { CHECK_EQUAL(fut.get_no_throw(), failStatus); });
}

TEST(Future_MoveOnly_Fail_getNothrowConstLvalue) {
    future_failure_test<Widget>(
                                [&](const Future<Widget>& fut) { CHECK_EQUAL(fut.get_no_throw(), failStatus); });
}
#endif

TEST(Future_MoveOnly_Fail_getNothrowRvalue)
{
    future_failure_test<Widget>([&](Future<Widget>&& fut) {
        CHECK_EQUAL(std::move(fut).get_no_throw().error(), fail_status);
    });
}

TEST(Future_MoveOnly_Fail_getAsync)
{
    future_failure_test<Widget>([&](Future<Widget>&& fut) {
        auto pf = make_promise_future<Widget>();
        std::move(fut).get_async([outside = std::move(pf.promise), this](Expected<Widget>&& sw) mutable noexcept {
            CHECK(!sw.has_value());
            outside.set_from(std::move(sw));
        });
        CHECK_EQUAL(std::move(pf.future).get_no_throw(), fail_status);
    });
}

TEST(Future_MoveOnly_Success_thenSimple)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .then([](Widget i) {
                                return i + 2;
                            })
                            .get(),
                        3);
        });
}

TEST(Future_MoveOnly_Success_thenSimpleAuto)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .then([](auto&& i) {
                                return i + 2;
                            })
                            .get(),
                        3);
        });
}

TEST(Future_MoveOnly_Success_thenVoid)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .then([&](Widget i) {
                                CHECK_EQUAL(i, 1);
                            })
                            .then([] {
                                return Widget(3);
                            })
                            .get(),
                        3);
        });
}

TEST(Future_MoveOnly_Success_thenStatus)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .then([&](Widget i) {
                                CHECK_EQUAL(i, 1);
                                return Status::OK();
                            })
                            .then([] {
                                return Widget(3);
                            })
                            .get(),
                        3);
        });
}

TEST(Future_MoveOnly_Success_thenError_Status)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            auto fut2 = std::move(fut).then([](Widget) {
                return fail_status;
            });
            static_assert(std::is_same_v<decltype(fut2), Future<void>>);
            CHECK_EQUAL(fut2.get_no_throw(), fail_status);
        });
}

TEST(Future_MoveOnly_Success_thenError_Expected)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            auto fut2 = std::move(fut).then([](Widget) {
                return Expected<double>(fail_status);
            });
            static_assert(std::is_same_v<decltype(fut2), Future<double>>);
            CHECK_EQUAL(fut2.get_no_throw(), fail_status);
        });
}

TEST(Future_MoveOnly_Success_thenFutureImmediate)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .then([](Widget i) {
                                return Future<Widget>::make_ready(Widget(i + 2));
                            })
                            .get(),
                        3);
        });
}

TEST(Future_MoveOnly_Success_thenFutureReady)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .then([](Widget i) {
                                auto pf = make_promise_future<Widget>();
                                pf.promise.emplace_value(i + 2);
                                return std::move(pf.future);
                            })
                            .get(),
                        3);
        });
}

TEST(Future_MoveOnly_Success_thenFutureAsync)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .then([&](Widget i) {
                                return async([i = i.val] {
                                    return Widget(i + 2);
                                });
                            })
                            .get(),
                        3);
        });
}

TEST(Future_MoveOnly_Success_thenFutureAsyncThrow)
{
    Future<Widget> foo;
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .then([](Widget) {
                                throw Exception(fail_status);
                                return Future<Widget>();
                            })
                            .get_no_throw(),
                        fail_status);
        });
}

TEST(Future_MoveOnly_Fail_thenSimple)
{
    future_failure_test<Widget>([&](Future<Widget>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([&](Widget) {
                            throw Exception(fail_status);
                            return Widget(0);
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_MoveOnly_Fail_thenFutureAsync)
{
    future_failure_test<Widget>([&](Future<Widget>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([](Widget) {
                            throw Exception(fail_status);
                            return Future<Widget>();
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_MoveOnly_Success_onErrorSimple)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .on_error([](Status) {
                                throw Exception(fail_status);
                                return Widget(0);
                            })
                            .then([](Widget i) {
                                return i + 2;
                            })
                            .get(),
                        3);
        });
}

TEST(Future_MoveOnly_Success_onErrorFutureAsync)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .on_error([](Status) {
                                throw Exception(fail_status);
                                return Future<Widget>();
                            })
                            .then([](Widget i) {
                                return i + 2;
                            })
                            .get(),
                        3);
        });
}

TEST(Future_MoveOnly_Fail_onErrorSimple)
{
    future_failure_test<Widget>([&](Future<Widget>&& fut) {
        auto sw_widget = std::move(fut)
                             .on_error([&](Expected<void>&& s) noexcept {
                                 CHECK_EQUAL(s, fail_status);
                                 return Widget(3);
                             })
                             .get_no_throw();
        CHECK(sw_widget.has_value());
        CHECK_EQUAL(sw_widget.value(), 3);
    });
}
TEST(Future_MoveOnly_Fail_onErrorError_throw)
{
    future_failure_test<Widget>([&](Future<Widget>&& fut) {
        auto fut2 = std::move(fut).on_error([&](Expected<void>&& s) -> Widget {
            CHECK_EQUAL(s, fail_status);
            throw Exception(fail_status_2);
        });
        CHECK_EQUAL(std::move(fut2).get_no_throw(), fail_status_2);
    });
}

TEST(Future_MoveOnly_Fail_onErrorError_Expected)
{
    future_failure_test<Widget>([&](Future<Widget>&& fut) {
        auto fut2 = std::move(fut).on_error([&](Expected<void>&& s) noexcept {
            CHECK_EQUAL(s, fail_status);
            return Expected<Widget>(fail_status_2);
        });
        CHECK_EQUAL(std::move(fut2).get_no_throw(), fail_status_2);
    });
}

TEST(Future_MoveOnly_Fail_onErrorFutureImmediate)
{
    future_failure_test<Widget>([&](Future<Widget>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Expected<void>&& s) noexcept {
                            CHECK_EQUAL(s, fail_status);
                            return Future<Widget>::make_ready(Widget(3));
                        })
                        .get(),
                    3);
    });
}

TEST(Future_MoveOnly_Fail_onErrorFutureReady)
{
    future_failure_test<Widget>([&](Future<Widget>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Expected<void>&& s) noexcept {
                            CHECK_EQUAL(s, fail_status);
                            auto pf = make_promise_future<Widget>();
                            pf.promise.emplace_value(3);
                            return std::move(pf.future);
                        })
                        .get(),
                    3);
    });
}

TEST(Future_MoveOnly_Fail_onErrorFutureAsync)
{
    future_failure_test<Widget>([&](Future<Widget>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Expected<void>&& s) noexcept {
                            CHECK_EQUAL(s, fail_status);
                            return async([] {
                                return Widget(3);
                            });
                        })
                        .get(),
                    3);
    });
}

TEST(Future_MoveOnly_Success_onCompletionSimple)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [this](auto&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .on_completion([](Expected<Widget>&& i) noexcept {
                                return i.value() + 2;
                            })
                            .get(),
                        3);
        });
}

TEST(Future_MoveOnly_Success_onCompletionVoid)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [this](auto&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .on_completion([this](Expected<Widget>&& i) noexcept {
                                CHECK_EQUAL(i.value(), 1);
                            })
                            .on_completion([this](Expected<void>&& s) noexcept {
                                CHECK(s.has_value());
                                return Widget(3);
                            })
                            .get(),
                        3);
        });
}

TEST(Future_MoveOnly_Success_onCompletionStatus)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [this](auto&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .on_completion([this](Expected<Widget>&& i) noexcept {
                                CHECK_EQUAL(i.value(), 1);
                                return Status::OK();
                            })
                            .on_completion([this](Expected<void>&& s) noexcept {
                                CHECK(s.has_value());
                                return Widget(3);
                            })
                            .get(),
                        3);
        });
}

TEST(Future_MoveOnly_Success_onCompletionError_Status)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [this](auto&& fut) {
            auto fut2 = std::move(fut).on_completion([](Expected<Widget>&&) noexcept {
                return fail_status;
            });
            static_assert(future_details::is_future<decltype(fut2)>);
            static_assert(std::is_same_v<typename decltype(fut2)::value_type, void>);
            CHECK_EQUAL(fut2.get_no_throw(), fail_status);
        });
}

TEST(Future_MoveOnly_Success_onCompletionError_Expected)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [this](auto&& fut) {
            auto fut2 = std::move(fut).on_completion([](Expected<Widget>&&) noexcept {
                return Expected<double>(fail_status);
            });
            static_assert(future_details::is_future<decltype(fut2)>);
            static_assert(std::is_same_v<typename decltype(fut2)::value_type, double>);
            CHECK_EQUAL(fut2.get_no_throw(), fail_status);
        });
}

TEST(Future_MoveOnly_Success_onCompletionFutureImmediate)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [this](auto&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .on_completion([](Expected<Widget>&& i) noexcept {
                                return Future<Widget>::make_ready(Widget(i.value() + 2));
                            })
                            .get(),
                        3);
        });
}

TEST(Future_MoveOnly_Success_onCompletionFutureReady)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [this](auto&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .on_completion([](Expected<Widget>&& i) noexcept {
                                auto pf = make_promise_future<Widget>();
                                pf.promise.emplace_value(i.value() + 2);
                                return std::move(pf.future);
                            })
                            .get(),
                        3);
        });
}

TEST(Future_MoveOnly_Success_onCompletionFutureAsync)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [this](auto&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .on_completion([&](Expected<Widget>&& i) noexcept {
                                return async([i = i.value().val] {
                                    return Widget(i + 2);
                                });
                            })
                            .get(),
                        3);
        });
}

TEST(Future_MoveOnly_Success_onCompletionFutureAsyncThrow)
{
    future_success_test(
        [] {
            return Widget(1);
        },
        [this](auto&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .on_completion([](Expected<Widget>&&) {
                                throw Exception(fail_status);
                                return Future<Widget>();
                            })
                            .get_no_throw(),
                        fail_status);
        });
}

TEST(Future_MoveOnly_Fail_onCompletionSimple)
{
    future_failure_test<Widget>([this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<Widget>&& i) noexcept {
                            CHECK_NOT(i.has_value());
                            return i.error();
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_MoveOnly_Fail_onCompletionFutureAsync)
{
    future_failure_test<Widget>([this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<Widget>&& i) noexcept {
                            CHECK_NOT(i.has_value());
                            return i.error();
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_MoveOnly_Fail_onCompletionError_throw)
{
    future_failure_test<Widget>([this](auto&& fut) {
        auto fut2 = std::move(fut).on_completion([this](Expected<Widget>&& s) -> Widget {
            CHECK_EQUAL(s.error(), fail_status);
            throw Exception(fail_status_2);
        });
        CHECK_EQUAL(std::move(fut2).get_no_throw(), fail_status_2);
    });
}

TEST(Future_MoveOnly_Fail_onCompletionError_Expected)
{
    future_failure_test<Widget>([this](auto&& fut) {
        auto fut2 = std::move(fut).on_completion([this](Expected<Widget>&& s) noexcept {
            CHECK_EQUAL(s.error(), fail_status);
            return Expected<Widget>(fail_status_2);
        });
        CHECK_EQUAL(std::move(fut2).get_no_throw(), fail_status_2);
    });
}

TEST(Future_MoveOnly_Fail_onCompletionFutureImmediate)
{
    future_failure_test<Widget>([this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<Widget>&& s) noexcept {
                            CHECK_EQUAL(s.error(), fail_status);
                            return Future<Widget>::make_ready(Widget(3));
                        })
                        .get(),
                    3);
    });
}

TEST(Future_MoveOnly_Fail_onCompletionFutureReady)
{
    future_failure_test<Widget>([this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<Widget>&& s) noexcept {
                            CHECK_EQUAL(s.error(), fail_status);
                            auto pf = make_promise_future<Widget>();
                            pf.promise.emplace_value(3);
                            return std::move(pf.future);
                        })
                        .get(),
                    3);
    });
}

// This is the motivating case for SharedStateBase::isJustForContinuation. Without that logic, there
// would be a long chain of SharedStates, growing longer with each recursion. That logic exists to
// limit it to a fixed-size chain.
TEST(Future_EdgeCases_looping_onError)
{
    int tries = 10;
    UniqueFunction<Future<int>()> read = [&] {
        return async([&] {
                   if (--tries != 0) {
                       throw Exception(fail_status);
                   }
                   return tries;
               })
            .on_error([&](Status) {
                return read();
            });
    };
    CHECK_EQUAL(read().get(), 0);
}

// This tests for a bug in an earlier implementation of isJustForContinuation. Due to an off-by-one,
// it would replace the "then" continuation's SharedState. A different type is used for the return
// from then to cause it to fail a checked_cast close to the bug in debug builds.
TEST(Future_EdgeCases_looping_onError_with_then)
{
    int tries = 10;
    UniqueFunction<Future<int>()> read = [&] {
        return async([&] {
                   if (--tries != 0) {
                       throw Exception(fail_status);
                   }
                   return tries;
               })
            .on_error([&](Status) {
                return read();
            });
    };
    CHECK_EQUAL(read()
                    .then([](int x) {
                        return x + 0.5;
                    })
                    .get(),
                0.5);
}

TEST(Promise_Success_setFrom)
{
    future_success_test(1, [&](Future<int>&& fut) {
        auto pf = make_promise_future<int>();
        pf.promise.set_from(std::move(fut));
        CHECK_EQUAL(std::move(pf.future).get(), 1);
    });
}

TEST(Promise_Fail_setFrom)
{
    future_failure_test<int>([&](Future<int>&& fut) {
        auto pf = make_promise_future<int>();
        pf.promise.set_from(std::move(fut));
        CHECK_THROW_EX(std::move(pf.future).get(), Exception, (e.to_status() == fail_status));
    });
}

TEST(Promise_void_Success_setFrom)
{
    future_success_test([&](Future<void>&& fut) {
        auto pf = make_promise_future<void>();
        pf.promise.set_from(std::move(fut));
        CHECK(std::move(pf.future).get_no_throw().has_value());
    });
}

TEST(Promise_void_Fail_setFrom)
{
    future_failure_test<void>([&](Future<void>&& fut) {
        auto pf = make_promise_future<void>();
        pf.promise.set_from(std::move(fut));
        CHECK_THROW_EX(std::move(pf.future).get(), Exception, (e.to_status() == fail_status));
    });
}

TEST(Future_Success_onCompletionSimple)
{
    future_success_test(1, [this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([](Expected<int>&& i) noexcept -> int {
                            return i.value() + 2;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Success_onCompletionVoid)
{
    future_success_test(1, [this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<int>&& i) noexcept {
                            CHECK_EQUAL(i.value(), 1);
                        })
                        .on_completion([this](Expected<void>&& s) noexcept -> int {
                            CHECK(s.has_value());
                            return 3;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Success_onCompletionStatus)
{
    future_success_test(1, [this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<int>&& i) noexcept -> Status {
                            CHECK_EQUAL(i, 1);
                            return Status::OK();
                        })
                        .on_completion([this](Expected<void>&& s) noexcept -> int {
                            CHECK(s.has_value());
                            return 3;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Success_onCompletionError_Status)
{
    future_success_test(1, [this](auto&& fut) {
        auto fut2 = std::move(fut).on_completion([](Expected<int>&&) noexcept -> Status {
            return fail_status;
        });
#ifndef _MSC_VER
        static_assert(future_details::is_future<decltype(fut2)>);
        static_assert(std::is_same_v<typename decltype(fut2)::value_type, void>);
#endif
        CHECK_THROW_EX(fut2.get(), Exception, (e.to_status() == fail_status));
    });
}

TEST(Future_Success_onCompletionError_Expected)
{
    future_success_test(1, [this](auto&& fut) {
        auto fut2 = std::move(fut).on_completion([](Expected<int>&&) noexcept -> Expected<double> {
            return Expected<double>(fail_status);
        });
        static_assert(future_details::is_future<decltype(fut2)>);
        static_assert(std::is_same_v<typename decltype(fut2)::value_type, double>);
        CHECK_THROW_EX(fut2.get(), Exception, (e.to_status() == fail_status));
    });
}

TEST(Future_Success_onCompletionFutureImmediate)
{
    future_success_test(1, [this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([](Expected<int>&& i) noexcept -> Future<int> {
                            return Future<int>::make_ready(i.value() + 2);
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Success_onCompletionFutureReady)
{
    future_success_test(1, [this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([](Expected<int>&& i) noexcept -> Future<int> {
                            auto pf = make_promise_future<int>();
                            pf.promise.emplace_value(i.value() + 2);
                            return std::move(pf.future);
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Success_onCompletionFutureAsync)
{
    future_success_test(1, [this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([](Expected<int>&& i) noexcept -> Future<int> {
                            return async([i = i.value()] {
                                return i + 2;
                            });
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Success_onCompletionFutureAsyncThrow)
{
    future_success_test(1, [this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([](Expected<int>&&) -> Future<int> {
                            throw Exception(fail_status);
                            return Future<int>();
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_Fail_onCompletionSimple)
{
    future_failure_test<int>([this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<int>&& i) noexcept -> Status {
                            CHECK_NOT(i.has_value());
                            return i.error();
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_Fail_onCompletionError_throw)
{
    future_failure_test<int>([this](auto&& fut) {
        auto fut2 = std::move(fut).on_completion([this](Expected<int>&& s) -> int {
            CHECK_EQUAL(s.error(), fail_status);
            throw Exception(fail_status);
        });
        CHECK_EQUAL(fut2.get_no_throw(), fail_status);
    });
}

TEST(Future_Fail_onCompletionError_Expected)
{
    future_failure_test<int>([this](auto&& fut) {
        auto fut2 = std::move(fut).on_completion([this](Expected<int>&& s) noexcept -> Expected<int> {
            CHECK_EQUAL(s.error(), fail_status);
            return Expected<int>(fail_status);
        });
        CHECK_EQUAL(fut2.get_no_throw(), fail_status);
    });
}

TEST(Future_Fail_onCompletionFutureImmediate)
{
    future_failure_test<int>([this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<int>&& s) noexcept -> Future<int> {
                            CHECK_EQUAL(s.error(), fail_status);
                            return Future<int>::make_ready(3);
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Fail_onCompletionFutureReady)
{
    future_failure_test<int>([this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<int>&& s) noexcept -> Future<int> {
                            CHECK_EQUAL(s.error(), fail_status);
                            auto pf = make_promise_future<int>();
                            pf.promise.emplace_value(3);
                            return std::move(pf.future);
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Success_onCompletionSimple)
{
    future_success_test([this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<void>&& result) noexcept {
                            CHECK(result.has_value());
                            return 3;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Success_onCompletionVoid)
{
    future_success_test([this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<void>&& result) noexcept {
                            CHECK(result.has_value());
                        })
                        .then([]() {
                            return 3;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Success_onCompletionError_Status)
{
    future_success_test([this](auto&& fut) {
        auto fut2 = std::move(fut).on_completion([this](Expected<void>&& result) noexcept {
            CHECK(result.has_value());
            return fail_status;
        });
        static_assert(future_details::is_future<decltype(fut2)>);
        static_assert(std::is_same_v<typename decltype(fut2)::value_type, void>);
        CHECK_EQUAL(fut2.get_no_throw(), fail_status);
    });
}

TEST(Future_Void_Success_onCompletionError_Expected)
{
    future_success_test([this](auto&& fut) {
        auto fut2 = std::move(fut).on_completion([this](Expected<void>&& result) noexcept {
            CHECK(result.has_value());
            return Expected<double>(fail_status);
        });
        static_assert(future_details::is_future<decltype(fut2)>);
        static_assert(std::is_same_v<typename decltype(fut2)::value_type, double>);
        CHECK_EQUAL(fut2.get_no_throw(), fail_status);
    });
}

TEST(Future_Void_Success_onCompletionFutureImmediate)
{
    future_success_test([this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<void>&& result) noexcept {
                            CHECK(result.has_value());
                            return Future<int>::make_ready(3);
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Success_onCompletionFutureReady)
{
    future_success_test([this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<void>&& result) noexcept {
                            CHECK(result.has_value());
                            auto pf = make_promise_future<int>();
                            pf.promise.emplace_value(3);
                            return std::move(pf.future);
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Success_onCompletionFutureAsync)
{
    future_success_test([this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<void>&& result) noexcept {
                            CHECK(result.has_value());
                            return async([] {
                                return 3;
                            });
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Fail_onCompletionSimple)
{
    future_failure_test<void>([this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<void>&& s) noexcept {
                            CHECK_EQUAL(s, fail_status);
                        })
                        .then([] {
                            return 3;
                        })
                        .get_no_throw(),
                    3);
    });
}

TEST(Future_Void_Fail_onCompletionError_throw)
{
    future_failure_test<void>([this](auto&& fut) {
        auto fut2 = std::move(fut).on_completion([this](Expected<void>&& s) {
            CHECK_EQUAL(s, fail_status);
            throw Exception(fail_status_2);
        });
        CHECK_EQUAL(fut2.get_no_throw(), fail_status_2);
    });
}

TEST(Future_Void_Fail_onCompletionError_Status)
{
    future_failure_test<void>([this](auto&& fut) {
        auto fut2 = std::move(fut).on_completion([this](Expected<void>&& s) noexcept {
            CHECK_EQUAL(s, fail_status);
            return fail_status_2;
        });
        CHECK_EQUAL(fut2.get_no_throw(), fail_status_2);
    });
}

TEST(Future_Void_Fail_onCompletionFutureImmediate)
{
    future_failure_test<void>([this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<void>&& s) noexcept {
                            CHECK_EQUAL(s, fail_status);
                            return Future<void>::make_ready();
                        })
                        .then([] {
                            return 3;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Fail_onCompletionFutureReady)
{
    future_failure_test<void>([this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<void>&& s) noexcept {
                            CHECK_EQUAL(s, fail_status);
                            auto pf = make_promise_future<void>();
                            pf.promise.emplace_value();
                            return std::move(pf.future);
                        })
                        .then([] {
                            return 3;
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Void_Fail_onCompletionFutureAsync)
{
    future_failure_test<void>([this](auto&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_completion([this](Expected<void>&& s) noexcept {
                            CHECK_EQUAL(s, fail_status);
                            return async([] {});
                        })
                        .then([] {
                            return 3;
                        })
                        .get(),
                    3);
    });
}

} // namespace
} // namespace realm::util

#endif
