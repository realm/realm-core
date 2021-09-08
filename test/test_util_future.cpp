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
static_assert(std::is_same_v<FutureContinuationResult<std::function<StatusWith<int>()>>, int>);
static_assert(std::is_same_v<FutureContinuationResult<std::function<Future<int>()>>, int>);
static_assert(std::is_same_v<FutureContinuationResult<std::function<int(bool)>, bool>, int>);

// Disable these tests for now because the static_assert on the SFINAE-failure case means that the
// failure case never gets emitted which causes a compiler warning.
#if 0
template <typename T>
auto overload_check(T) -> FutureContinuationResult<std::function<std::true_type(bool)>, T>;
auto overload_check(...) -> std::false_type;

static_assert(decltype(overload_check(bool()))::value);         // match.
static_assert(!decltype(overload_check(std::string()))::value); // SFINAE-failure.
#endif

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
          typename = std::enable_if_t<!std::is_void_v<std::invoke_result_t<CompletionFunc>>>>
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
          typename = std::enable_if_t<std::is_void_v<std::invoke_result_t<CompletionFunc>>>, typename = void>
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
            std::move(fut).get_async([outside = std::move(pf.promise), this](StatusWith<int> sw) mutable noexcept {
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
        std::move(fut).get_async([outside = std::move(pf.promise), this](StatusWith<int> sw) mutable noexcept {
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
            std::move(fut).get_async([&](StatusWith<int> status) noexcept {
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
        std::move(fut).get_async([&](StatusWith<int> status) noexcept {
            CHECK_EQUAL(std::this_thread::get_id(), id);
            CHECK(!status.is_ok());
        });
    });
}

TEST(Future_Success_thenSimple)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
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
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
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
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
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
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
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
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
            auto fut2 = std::move(fut).then([](int) {
                return fail_status;
            });
            static_assert(std::is_same_v<decltype(fut2), Future<void>>);
            CHECK_THROW_EX(fut2.get(), ExceptionForStatus, (e.to_status() == fail_status));
        });
}


TEST(Future_Success_thenError_StatusWith)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
            auto fut2 = std::move(fut).then([&](int) {
                return StatusWith<double>(fail_status);
            });
            static_assert(std::is_same_v<decltype(fut2), Future<double>>);
            CHECK_THROW_EX(fut2.get(), ExceptionForStatus, (e.to_status() == fail_status));
        });
}

TEST(Future_Success_thenFutureImmediate)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
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
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
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
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
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
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .then([](int) {
                                throw ExceptionForStatus(fail_status);
                                return Future<int>();
                            })
                            .get_no_throw(),
                        fail_status);
        });
}

TEST(Future_Fail_thenSimple)
{
    FUTURE_FAIL_TEST<int>([&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([](int) {
                            throw ExceptionForStatus(fail_status);
                            return int();
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_Fail_thenFutureAsync)
{
    FUTURE_FAIL_TEST<int>([&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([](int i) {
                            throw ExceptionForStatus(fail_status);
                            return Future<int>(i + 1);
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_Success_onErrorSimple)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .on_error([](Status) {
                                throw ExceptionForStatus(fail_status);
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
    FUTURE_SUCCESS_TEST(
        [] {
            return 1;
        },
        [&](Future<int>&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .on_error([](Status) {
                                throw ExceptionForStatus(fail_status);
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
    FUTURE_FAIL_TEST<int>([&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Status s) {
                            CHECK_EQUAL(s, fail_status);
                            return 3;
                        })
                        .get_no_throw(),
                    3);
    });
}

TEST(Future_Fail_onErrorError_throw)
{
    FUTURE_FAIL_TEST<int>([&](Future<int>&& fut) {
        auto fut2 = std::move(fut).on_error([&](Status s) -> int {
            CHECK_EQUAL(s, fail_status);
            throw ExceptionForStatus(fail_status_2);
        });
        CHECK_EQUAL(fut2.get_no_throw(), fail_status_2);
    });
}

TEST(Future_Fail_onErrorError_StatusWith)
{
    FUTURE_FAIL_TEST<int>([&](Future<int>&& fut) {
        auto fut2 = std::move(fut).on_error([&](Status s) {
            CHECK_EQUAL(s, fail_status);
            return StatusWith<int>(fail_status_2);
        });
        CHECK_EQUAL(fut2.get_no_throw(), fail_status_2);
    });
}

TEST(Future_Fail_onErrorFutureImmediate)
{
    FUTURE_FAIL_TEST<int>([&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Status s) {
                            CHECK_EQUAL(s, fail_status);
                            return Future<int>::make_ready(3);
                        })
                        .get(),
                    3);
    });
}

TEST(Future_Fail_onErrorFutureReady)
{
    FUTURE_FAIL_TEST<int>([&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Status s) {
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
    FUTURE_FAIL_TEST<int>([&](Future<int>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Status s) {
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
                            std::move(fut).get_async(
                                [outside = std::move(pf.promise), this](Status status) mutable noexcept {
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
        std::move(fut).get_async([outside = std::move(pf.promise), this](Status status) mutable noexcept {
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
                            std::move(fut).get_async([&](Status status) noexcept {
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
        std::move(fut).get_async([&](Status status) noexcept {
            CHECK_EQUAL(std::this_thread::get_id(), id);
            CHECK(!status.is_ok());
        });
    });
}

TEST(Future_Void_Success_thenSimple)
{
    FUTURE_SUCCESS_TEST([] {},
                        [&](Future<void>&& fut) {
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
    FUTURE_SUCCESS_TEST([] {},
                        [&](Future<void>&& fut) {
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
    FUTURE_SUCCESS_TEST([] {},
                        [&](Future<void>&& fut) {
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
    FUTURE_SUCCESS_TEST([] {},
                        [&](Future<void>&& fut) {
                            auto fut2 = std::move(fut).then([]() {
                                return fail_status;
                            });
                            static_assert(std::is_same_v<decltype(fut2), Future<void>>);
                            CHECK_EQUAL(fut2.get_no_throw(), fail_status);
                        });
}

TEST(Future_Void_Success_thenError_StatusWith)
{
    FUTURE_SUCCESS_TEST([] {},
                        [&](Future<void>&& fut) {
                            auto fut2 = std::move(fut).then([]() {
                                return StatusWith<double>(fail_status);
                            });
                            static_assert(std::is_same_v<decltype(fut2), Future<double>>);
                            CHECK_EQUAL(fut2.get_no_throw(), fail_status);
                        });
}

TEST(Future_Void_Success_thenFutureImmediate)
{
    FUTURE_SUCCESS_TEST([] {},
                        [&](Future<void>&& fut) {
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
    FUTURE_SUCCESS_TEST([] {},
                        [&](Future<void>&& fut) {
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
    FUTURE_SUCCESS_TEST([] {},
                        [&](Future<void>&& fut) {
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
    FUTURE_FAIL_TEST<void>([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([]() {
                            throw ExceptionForStatus(fail_status);
                            return int();
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_Void_Fail_thenFutureAsync)
{
    FUTURE_FAIL_TEST<void>([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([]() {
                            throw ExceptionForStatus(fail_status);
                            return Future<int>();
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_Void_Success_onErrorSimple)
{
    FUTURE_SUCCESS_TEST([] {},
                        [&](Future<void>&& fut) {
                            CHECK_EQUAL(std::move(fut)
                                            .on_error([](Status) {
                                                throw ExceptionForStatus(fail_status);
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
    FUTURE_SUCCESS_TEST([] {},
                        [&](Future<void>&& fut) {
                            CHECK_EQUAL(std::move(fut)
                                            .on_error([](Status) {
                                                throw ExceptionForStatus(fail_status);
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
    FUTURE_FAIL_TEST<void>([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Status s) {
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
    FUTURE_FAIL_TEST<void>([&](Future<void>&& fut) {
        auto fut2 = std::move(fut).on_error([&](Status s) {
            CHECK_EQUAL(s, fail_status);
            throw ExceptionForStatus(fail_status_2);
        });
        CHECK_EQUAL(fut2.get_no_throw(), fail_status_2);
    });
}

TEST(Future_Void_Fail_onErrorError_Status)
{
    FUTURE_FAIL_TEST<void>([&](Future<void>&& fut) {
        auto fut2 = std::move(fut).on_error([&](Status s) {
            CHECK_EQUAL(s, fail_status);
            return fail_status_2;
        });
        CHECK_EQUAL(fut2.get_no_throw(), fail_status_2);
    });
}

TEST(Future_Void_Fail_onErrorFutureImmediate)
{
    FUTURE_FAIL_TEST<void>([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Status s) {
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
    FUTURE_FAIL_TEST<void>([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Status s) {
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
    FUTURE_FAIL_TEST<void>([&](Future<void>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Status s) {
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
            std::move(fut).get_async([outside = std::move(pf.promise), this](StatusWith<Widget> sw) mutable noexcept {
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
        std::move(fut).get_async([outside = std::move(pf.promise), this](StatusWith<Widget> sw) mutable noexcept {
            CHECK(!sw.is_ok());
            outside.set_error(sw.get_status());
        });
        CHECK_EQUAL(std::move(pf.future).get_no_throw(), fail_status);
    });
}

TEST(Future_MoveOnly_Success_thenSimple)
{
    FUTURE_SUCCESS_TEST(
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
    FUTURE_SUCCESS_TEST(
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
    FUTURE_SUCCESS_TEST(
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
    FUTURE_SUCCESS_TEST(
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
    FUTURE_SUCCESS_TEST(
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

TEST(Future_MoveOnly_Success_thenError_StatusWith)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            auto fut2 = std::move(fut).then([](Widget) {
                return StatusWith<double>(fail_status);
            });
            static_assert(std::is_same_v<decltype(fut2), Future<double>>);
            CHECK_EQUAL(fut2.get_no_throw(), fail_status);
        });
}

TEST(Future_MoveOnly_Success_thenFutureImmediate)
{
    FUTURE_SUCCESS_TEST(
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
    FUTURE_SUCCESS_TEST(
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
    FUTURE_SUCCESS_TEST(
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
    FUTURE_SUCCESS_TEST(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .then([](Widget) {
                                throw ExceptionForStatus(fail_status);
                                return Future<Widget>();
                            })
                            .get_no_throw(),
                        fail_status);
        });
}

TEST(Future_MoveOnly_Fail_thenSimple)
{
    FUTURE_FAIL_TEST<Widget>([&](Future<Widget>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([&](Widget) {
                            throw ExceptionForStatus(fail_status);
                            return Widget(0);
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_MoveOnly_Fail_thenFutureAsync)
{
    FUTURE_FAIL_TEST<Widget>([&](Future<Widget>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .then([](Widget) {
                            throw ExceptionForStatus(fail_status);
                            return Future<Widget>();
                        })
                        .get_no_throw(),
                    fail_status);
    });
}

TEST(Future_MoveOnly_Success_onErrorSimple)
{
    FUTURE_SUCCESS_TEST(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .on_error([](Status) {
                                throw ExceptionForStatus(fail_status);
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
    FUTURE_SUCCESS_TEST(
        [] {
            return Widget(1);
        },
        [&](Future<Widget>&& fut) {
            CHECK_EQUAL(std::move(fut)
                            .on_error([](Status) {
                                throw ExceptionForStatus(fail_status);
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
    FUTURE_FAIL_TEST<Widget>([&](Future<Widget>&& fut) {
        auto sw_widget = std::move(fut)
                             .on_error([&](Status s) {
                                 CHECK_EQUAL(s, fail_status);
                                 return Widget(3);
                             })
                             .get_no_throw();
        CHECK(sw_widget.is_ok());
        CHECK_EQUAL(sw_widget.get_value(), 3);
    });
}
TEST(Future_MoveOnly_Fail_onErrorError_throw)
{
    FUTURE_FAIL_TEST<Widget>([&](Future<Widget>&& fut) {
        auto fut2 = std::move(fut).on_error([&](Status s) -> Widget {
            CHECK_EQUAL(s, fail_status);
            throw ExceptionForStatus(fail_status_2);
        });
        CHECK_EQUAL(std::move(fut2).get_no_throw(), fail_status_2);
    });
}

TEST(Future_MoveOnly_Fail_onErrorError_StatusWith)
{
    FUTURE_FAIL_TEST<Widget>([&](Future<Widget>&& fut) {
        auto fut2 = std::move(fut).on_error([&](Status s) {
            CHECK_EQUAL(s, fail_status);
            return StatusWith<Widget>(fail_status_2);
        });
        CHECK_EQUAL(std::move(fut2).get_no_throw(), fail_status_2);
    });
}

TEST(Future_MoveOnly_Fail_onErrorFutureImmediate)
{
    FUTURE_FAIL_TEST<Widget>([&](Future<Widget>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Status s) {
                            CHECK_EQUAL(s, fail_status);
                            return Future<Widget>::make_ready(Widget(3));
                        })
                        .get(),
                    3);
    });
}

TEST(Future_MoveOnly_Fail_onErrorFutureReady)
{
    FUTURE_FAIL_TEST<Widget>([&](Future<Widget>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Status s) {
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
    FUTURE_FAIL_TEST<Widget>([&](Future<Widget>&& fut) {
        CHECK_EQUAL(std::move(fut)
                        .on_error([&](Status s) {
                            CHECK_EQUAL(s, fail_status);
                            return async([] {
                                return Widget(3);
                            });
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
                       throw ExceptionForStatus(fail_status);
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
                       throw ExceptionForStatus(fail_status);
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
