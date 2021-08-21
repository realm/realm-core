/*************************************************************************
 *
 * Copyright 2021 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <condition_variable>
#include <mutex>
#include <type_traits>

#include "realm/status.hpp"
#include "realm/util/assert.hpp"
#include "realm/util/bind_ptr.hpp"
#include "realm/util/features.h"
#include "realm/util/functional.hpp"
#include "realm/util/optional.hpp"
#include "realm/util/scope_exit.hpp"
#include "realm/status.hpp"
#include "realm/status_with.hpp"

namespace realm::util {

template <typename T>
class SharedPromise;

namespace future_details {
template <typename T>
class Promise;

template <typename T>
class Future;

template <>
class Future<void>;

template <typename T>
constexpr static bool is_future = false;
template <typename T>
constexpr static bool is_future<Future<T>> = true;

struct FakeVoid {
};

template <typename T>
using VoidToFakeVoid = std::conditional_t<std::is_void_v<T>, FakeVoid, T>;

template <typename T>
using FakeVoidToVoid = std::conditional_t<std::is_same_v<T, FakeVoid>, void, T>;

template <typename T>
struct UnstatusTypeImpl {
    using type = T;
};
template <typename T>
struct UnstatusTypeImpl<StatusWith<T>> {
    using type = T;
};
template <>
struct UnstatusTypeImpl<Status> {
    using type = void;
};
template <typename T>
using UnstatusType = typename UnstatusTypeImpl<T>::type;

template <typename T>
struct UnwrappedTypeImpl {
    static_assert(!is_future<T>);
    static_assert(!is_status_or_status_with<T>);
    using type = T;
};
template <typename T>
struct UnwrappedTypeImpl<Future<T>> {
    using type = T;
};
template <typename T>
struct UnwrappedTypeImpl<StatusWith<T>> {
    using type = T;
};
template <>
struct UnwrappedTypeImpl<Status> {
    using type = void;
};
template <typename T>
using UnwrappedType = typename UnwrappedTypeImpl<T>::type;
/**
 * call() normalizes arguments to hide the FakeVoid shenanigans from users of Futures.
 * In the future it may also expand tuples to argument lists.
 */
template <typename Func, typename Arg>
inline auto call(Func&& func, Arg&& arg)
{
    return func(std::forward<Arg>(arg));
}

template <typename Func>
inline auto call(Func&& func, FakeVoid)
{
    return func();
}

template <typename Func>
inline auto call(Func&& func, StatusWith<FakeVoid> sw)
{
    return func(sw.get_status());
}

/**
 * no_throw_call() normalizes return values so everything returns StatusWith<T>. Exceptions are
 * converted to !OK statuses. void and Status returns are converted to StatusWith<FakeVoid>
 */
template <typename Func, typename... Args>
inline auto no_throw_call(Func&& func, Args&&... args) noexcept
{
    using RawResult = decltype(call(func, std::forward<Args>(args)...));
    using Result = StatusWith<VoidToFakeVoid<UnstatusType<RawResult>>>;
    try {
        if constexpr (std::is_void_v<RawResult>) {
            call(func, std::forward<Args>(args)...);
            return Result(FakeVoid());
        }
        else if constexpr (std::is_same_v<RawResult, Status>) {
            auto s = call(func, std::forward<Args>(args)...);
            if (!s.is_ok()) {
                return Result(std::move(s));
            }
            return Result(FakeVoid());
        }
        else {
            return Result(call(func, std::forward<Args>(args)...));
        }
    }
    catch (...) {
        return Result(exception_to_status());
    }
}

/**
 * throwing_call() normalizes return values so everything returns T or FakeVoid. !OK Statuses are
 * converted exceptions. void and Status returns are converted to FakeVoid.
 *
 * This is equivalent to uassertStatusOK(statusCall(func, args...)), but avoids catching just to
 * rethrow.
 */
template <typename Func, typename... Args>
inline auto throwing_call(Func&& func, Args&&... args)
{
    using Result = decltype(call(func, std::forward<Args>(args)...));
    if constexpr (std::is_void_v<Result>) {
        call(func, std::forward<Args>(args)...);
        return FakeVoid{};
    }
    else if constexpr (std::is_same_v<Result, Status>) {
        auto res = (call(func, std::forward<Args>(args)...));
        if (!res.is_ok()) {
            throw ExceptionForStatus(std::move(res));
        }
        return FakeVoid{};
    }
    else if constexpr (is_status_with<Result>) {
        auto res = (call(func, std::forward<Args>(args)...));
        if (!res.is_ok()) {
            throw ExceptionForStatus(std::move(res.get_status()));
        }
        return std::move(res.get_value());
    }
    else {
        return call(func, std::forward<Args>(args)...);
    }
}

template <typename Func, typename... Args>
using RawNormalizedCallResult = decltype(throwing_call(std::declval<Func>(), std::declval<Args>()...));

template <typename Func, typename... Args>
using NormalizedCallResult = std::conditional_t<std::is_same<RawNormalizedCallResult<Func, Args...>, FakeVoid>::value,
                                                void, RawNormalizedCallResult<Func, Args...>>;

template <typename T>
struct FutureContinuationResultImpl {
    using type = T;
};
template <typename T>
struct FutureContinuationResultImpl<Future<T>> {
    using type = T;
};
template <typename T>
struct FutureContinuationResultImpl<StatusWith<T>> {
    using type = T;
};
template <>
struct FutureContinuationResultImpl<Status> {
    using type = void;
};

class FutureRefCountable {
    FutureRefCountable(const FutureRefCountable&) = delete;
    FutureRefCountable& operator=(const FutureRefCountable&) = delete;

public:
    void thread_unsafe_inc_refs_to(uint32_t count) const
    {
        REALM_ASSERT_DEBUG(m_refs.load(std::memory_order_relaxed) == (count - 1));
        m_refs.store(count, std::memory_order_relaxed);
    }

protected:
    FutureRefCountable() = default;
    virtual ~FutureRefCountable() = default;

    template <typename>
    friend class ::realm::util::bind_ptr;

    void bind_ptr() const noexcept
    {
        m_refs.fetch_add(1, std::memory_order_relaxed);
    }

    void unbind_ptr() const noexcept
    {
        if (m_refs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            delete this;
        }
    }

private:
    mutable std::atomic<uint32_t> m_refs{0};
};

template <typename T, typename... Args, typename = std::enable_if_t<std::is_base_of_v<FutureRefCountable, T>>>
util::bind_ptr<T> make_intrusive(Args&&... args)
{
    auto ptr = new T(std::forward<Args>(args)...);
    ptr->thread_unsafe_inc_refs_to(1);
    return util::bind_ptr<T>(ptr, util::bind_ptr_base::adopt_tag{});
}

template <typename T>
struct SharedStateImpl;

template <typename T>
using SharedState = SharedStateImpl<VoidToFakeVoid<T>>;

/**
 * SSB is SharedStateBase, and this is its current state.
 */
enum class SSBState : uint8_t {
    kInit,
    kWaiting,
    kFinished, // This should stay last since we have code like assert(state < kFinished).
};

struct SharedStateBase : public FutureRefCountable {
    SharedStateBase(const SharedStateBase&) = delete;
    SharedStateBase(SharedStateBase&&) = delete;
    SharedStateBase& operator=(const SharedStateBase&) = delete;
    SharedStateBase& operator=(SharedStateBase&&) = delete;

    virtual ~SharedStateBase() = default;

    // This is called by the future side.
    void wait() noexcept
    {
        if (m_state.load(std::memory_order_acquire) == SSBState::kFinished) {
            return;
        }

        m_cv.emplace();

        auto old_state = SSBState::kInit;
        if (REALM_UNLIKELY(
                !m_state.compare_exchange_strong(old_state, SSBState::kWaiting, std::memory_order_acq_rel))) {
            REALM_ASSERT_DEBUG(old_state == SSBState::kFinished);
            return;
        }

        std::unique_lock<std::mutex> lk(m_mutex);
        m_cv->wait(lk, [&] {
            return m_state.load(std::memory_order_acquire) == SSBState::kFinished;
        });
    }

    void transition_to_finished() noexcept
    {
        auto old_state = m_state.exchange(SSBState::kFinished, std::memory_order_acq_rel);
        if (old_state == SSBState::kInit) {
            return;
        }

        REALM_ASSERT_DEBUG(old_state == SSBState::kWaiting);

#ifdef REALM_DEBUG
        // If you hit this limit one of two things has probably happened
        //
        // 1. The justForContinuation optimization isn't working.
        // 2. You may be creating a variable length chain.
        constexpr size_t kMaxDepth = 32;

        size_t depth = 0;
        for (auto ssb = m_continuation.get(); ssb;
             ssb = ssb->m_state.load(std::memory_order_acquire) == SSBState::kWaiting ? ssb->m_continuation.get()
                                                                                      : nullptr) {
            depth++;
            REALM_ASSERT(depth < kMaxDepth);
        }
#endif
        if (m_callback) {
            m_callback(this);
        }

        if (m_cv) {
            std::lock_guard<std::mutex> lk(m_mutex);
            m_cv->notify_all();
        }
    }

    void set_status(Status status) noexcept
    {
        REALM_ASSERT_DEBUG(m_state.load() < SSBState::kFinished);
        m_status = std::move(status);
        transition_to_finished();
    }

    // All the rest of the methods are only called by the promise side.

    //
    // Concurrency Rules for members: Each non-atomic member is initially owned by either the
    // Promise side or the Future side, indicated by a P/F comment. The general rule is that members
    // representing the propagating data are owned by Promise, while members representing what
    // to do with the data are owned by Future. The owner may freely modify the members it owns
    // until it releases them by doing a release-store to state of kFinished from Promise or
    // kWaiting from Future. Promise can acquire access to all members by doing an acquire-load of
    // state and seeing kWaiting (or Future with kFinished). Transitions should be done via
    // acquire-release exchanges to combine both actions.
    //
    // Future::propagateResults uses an alternative mechanism to transfer ownership of the
    // continuation member. The logical Future-side does a release-store of true to
    // isJustForContinuation, and the Promise-side can do an acquire-load seeing true to get access.
    //

    std::atomic<SSBState> m_state{SSBState::kInit};

    // This is used to prevent infinite chains of SharedStates that just propagate results.
    std::atomic<bool> m_is_just_for_continuation{false};

    // This is likely to be a different derived type from this, since it is the logical output of
    // callback.
    util::bind_ptr<SharedStateBase> m_continuation; // F

    util::UniqueFunction<void(SharedStateBase*)> m_callback; // F

    std::mutex m_mutex;                           // F
    util::Optional<std::condition_variable> m_cv; // F

    Status m_status = Status::OK(); // P

protected:
    SharedStateBase() = default;
};

template <typename T>
struct SharedStateImpl final : public SharedStateBase {
    static_assert(!std::is_void_v<T>);

    void fill_from(SharedState<T>&& other)
    {
        REALM_ASSERT_DEBUG(m_state.load() < SSBState::kFinished);
        REALM_ASSERT_DEBUG(other.m_state.load() == SSBState::kFinished);
        if (other.m_status.is_ok()) {
            REALM_ASSERT_DEBUG(other.m_data);
            m_data = std::move(other.m_data);
        }
        else {
            m_status = std::move(other.m_status);
        }
        transition_to_finished();
    }

    template <typename... Args>
    void emplace_value(Args&&... args) noexcept
    {
        REALM_ASSERT_DEBUG(m_state.load() < SSBState::kFinished);
        try {
            m_data.emplace(std::forward<Args>(args)...);
        }
        catch (...) {
            m_status = exception_to_status();
        }
        transition_to_finished();
    }

    void set_from(StatusWith<T> roe)
    {
        if (roe.is_ok()) {
            emplace_value(std::move(roe.get_value()));
        }
        else {
            set_status(roe.get_status());
        }
    }

    util::Optional<T> m_data; // P
};

} // namespace future_details

// These are in the future_details namespace to get access to its contents, but they are part of the
// public API.
using future_details::Future;
using future_details::Promise;

/**
 * This class represents the producer side of a Future.
 *
 * This is a single-shot class. You may only extract the Future once, and you may either set a value
 * or error at most once. Extracting the future and setting the value/error can be done in either
 * order.
 *
 * If the Future has been extracted, but no value or error has been set at the time this Promise is
 * destroyed, a error will be set with ErrorCode::BrokenPromise. This should generally be considered
 * a programmer error, and should not be relied upon. We may make it debug-fatal in the future.
 *
 * Only one thread can use a given Promise at a time. It is legal to have different threads setting
 * the value/error and extracting the Future, but it is the user's responsibility to ensure that
 * those calls are strictly synchronized. This is usually easiest to achieve by calling
 * make_promise_future<T>() then passing a SharedPromise to the completing threads.
 *
 * If the result is ready when producing the Future, it is more efficient to use
 * make_ready_future_with() or Future<T>::make_ready() than to use a Promise<T>.
 */
template <typename T>
class future_details::Promise {
public:
    using value_type = T;

    Promise() = default;

    ~Promise()
    {
        if (REALM_UNLIKELY(m_shared_state)) {
            m_shared_state->set_status({ErrorCodes::BrokenPromise, "Broken Promise"});
        }
    }

    // If we want to enable move-assignability, we need to handle breaking the promise on the old
    // value of this.
    Promise& operator=(Promise&&) = delete;

    // The default move construction is fine.
    Promise(Promise&&) = default;

    /**
     * Sets the value into this Promise when the passed-in Future completes, which may have already
     * happened. If it hasn't, it is still safe to destroy this Promise since it is no longer
     * involved.
     */
    void set_from(Future<T>&& future) noexcept;

    void set_from_status_with(StatusWith<T> sw) noexcept
    {
        set_impl([&] {
            m_shared_state->set_from(std::move(sw));
        });
    }

    template <typename... Args>
    void emplace_value(Args&&... args) noexcept
    {
        set_impl([&] {
            m_shared_state->emplace_value(std::forward<Args>(args)...);
        });
    }

    void set_error(Status status) noexcept
    {
        REALM_ASSERT_DEBUG(!status.is_ok());
        set_impl([&] {
            m_shared_state->set_status(std::move(status));
        });
    }

    static auto make_promise_future_impl()
    {
        struct PromiseAndFuture {
            Promise<T> promise;
            Future<T> future = promise.get_future();
        };
        return PromiseAndFuture();
    }

private:
    // This is not public because we found it frequently was involved in races.  The
    // `make_promise_future<T>` API avoids those races entirely.
    Future<T> get_future() noexcept;

    friend class Future<void>;

    template <typename Func>
    void set_impl(Func&& do_set) noexcept
    {
        REALM_ASSERT(m_shared_state);
        do_set();
        m_shared_state.reset();
    }

    util::bind_ptr<SharedState<T>> m_shared_state = make_intrusive<SharedState<T>>();
};

/**
 * Future<T> is logically a possibly-deferred T or exception_ptr
 * As is usual for rvalue-qualified methods, you may call at most one of them on a given Future.
 *
 * A future may be passed between threads, but only one thread may use it at a time.
 *
 * TODO decide if destroying a Future before extracting the result should cancel work or should
 * cancellation be explicit. For now avoid unnecessarily throwing away active Futures since the
 * behavior may change. End all Future chains with either a blocking call to get() or a
 * non-blocking call to getAsync().
 */
template <typename T>
class REALM_NODISCARD future_details::Future {
public:
    static_assert(!is_future<T>, "Future<Future<T>> is banned. Just use Future<T> instead.");
    static_assert(!std::is_reference<T>::value, "Future<T&> is banned.");
    static_assert(!std::is_const<T>::value, "Future<const T> is banned.");
    static_assert(!std::is_array<T>::value, "Future<T[]> is banned.");

    using value_type = T;

    /**
     * Constructs a Future in a moved-from state that can only be assigned to or destroyed.
     */
    Future() = default;

    Future& operator=(Future&&) = default;
    Future(Future&&) = default;

    Future(const Future&) = delete;
    Future& operator=(const Future&) = delete;

    /* implicit */ Future(T val)
        : Future(make_ready(std::move(val)))
    {
    }
    /* implicit */ Future(Status status)
        : Future(make_ready(std::move(status)))
    {
    }
    /* implicit */ Future(StatusWith<T> sw)
        : Future(make_ready(std::move(sw)))
    {
    }

    /**
     * Make a ready Future<T> from a value for cases where you don't need to wait asynchronously.
     *
     * Calling this is faster than getting a Future out of a Promise, and is effectively free. It is
     * fast enough that you never need to avoid returning a Future from an API, even if the result
     * is ready 99.99% of the time.
     *
     * As an example, if you are handing out results from a batch, you can use this when for each
     * result while you have a batch, then use a Promise to return a not-ready Future when you need
     * to get another batch.
     */
    static Future<T> make_ready(T val)
    { // TODO emplace?
        Future out;
        out.m_immediate = std::move(val);
        return out;
    }

    static Future<T> make_ready(Status status)
    {
        auto out = Future<T>(make_intrusive<SharedState<T>>());
        out.m_shared->set_status(std::move(status));
        return out;
    }

    static Future<T> make_ready(StatusWith<T> val)
    {
        if (val.is_ok()) {
            return make_ready(std::move(val.get_value()));
        }
        return make_ready(val.get_status());
    }

    /**
     * If this returns true, get() is guaranteed not to block and callbacks will be immediately
     * invoked. You can't assume anything if this returns false since it may be completed
     * immediately after checking (unless you have independent knowledge that this Future can't
     * complete in the background).
     *
     * Callers must still call get() or similar, even on Future<void>, to ensure that they are
     * correctly sequenced with the completing task, and to be informed about whether the Promise
     * completed successfully.
     *
     * This is generally only useful as an optimization to avoid prep work, such as setting up
     * timeouts, that is unnecessary if the Future is ready already.
     */
    bool is_ready() const
    {
        // This can be a relaxed load because callers are not allowed to use it to establish
        // ordering.
        return m_immediate || m_shared->m_state.load(std::memory_order_relaxed) == SSBState::kFinished;
    }

    /**
     * Gets the value out of this Future, blocking until it is ready.
     *
     * get() methods throw on error, while get_no_throw() returns a ResultOrException<T> with either a value
     * or an std::exception_ptr.
     *
     * These methods can be called multiple times, except for the rvalue overloads.
     */
    T get() &&
    {
        return std::move(get_impl());
    }
    T& get() &
    {
        return get_impl();
    }
    const T& get() const&
    {
        return const_cast<Future*>(this)->get_impl();
    }

    StatusWith<T> get_no_throw() const& noexcept
    {
        if (m_immediate) {
            return *m_immediate;
        }

        m_shared->wait();
        if (!m_shared->m_data) {
            return m_shared->m_status;
        }
        return *m_shared->m_data;
    }

    StatusWith<T> get_no_throw() && noexcept
    {
        if (m_immediate) {
            return std::move(*m_immediate);
        }

        m_shared->wait();
        if (!m_shared->m_data) {
            return m_shared->m_status;
        }
        return std::move(*m_shared->m_data);
    }

    /**
     * This ends the Future continuation chain by calling a callback on completion. Use this to
     * escape back into a callback-based API.
     *
     * For now, the callback must not fail, since there is nowhere to propagate the error to.
     * TODO decide how to handle func throwing.
     */
    template <typename Func> // ResultOrException<T> -> void
    void get_async(Func&& func) && noexcept
    {
        static_assert(std::is_void_v<decltype(call(func, std::declval<StatusWith<T>>()))>,
                      "func passed to get_async must return void");

        return general_impl(
            // on ready success:
            [&](T&& val) {
                call(func, StatusWith<T>(std::move(val)));
            },
            // on ready failure:
            [&](Status&& status) {
                call(func, StatusWith<T>(std::move(status)));
            },
            // on not ready:
            [&] {
                m_shared->m_callback = [func = std::forward<Func>(func)](SharedStateBase* ssb) mutable noexcept {
                    const auto input = static_cast<SharedState<T>*>(ssb);
                    if (input->m_status.is_ok()) {
                        call(func, StatusWith<T>(std::move(*input->m_data)));
                    }
                    else {
                        call(func, StatusWith<T>(std::move(input->m_status)));
                    }
                };
            });
    }

private:
    template <typename T2>
    friend class Future;
    friend class Promise<T>;

    T& get_impl()
    {
        if (m_immediate) {
            return *m_immediate;
        }

        m_shared->wait();
        if (!m_shared->m_status.is_ok()) {
            throw ExceptionForStatus(m_shared->m_status);
        }
        return *m_shared->m_data;
    }

    // All callbacks are called immediately so they are allowed to capture everything by reference.
    // All callbacks should return the same return type.
    template <typename SuccessFunc, typename FailFunc, typename NotReady>
    auto general_impl(SuccessFunc&& success, FailFunc&& fail, NotReady&& notReady) noexcept
    {
        if (m_immediate) {
            return success(std::move(*m_immediate));
        }

        if (m_shared->m_state.load(std::memory_order_acquire) == SSBState::kFinished) {
            if (m_shared->m_data) {
                return success(std::move(*m_shared->m_data));
            }
            else {
                return fail(std::move(m_shared->m_status));
            }
        }

        // This is always done after notReady, which never throws. It is in a ScopeExit to
        // support both void- and value-returning notReady implementations since we can't assign
        // void to a variable.
        auto guard = util::make_scope_exit([&]() noexcept {
            auto old_state = SSBState::kInit;
            if (REALM_UNLIKELY(!m_shared->m_state.compare_exchange_strong(old_state, SSBState::kWaiting,
                                                                          std::memory_order_acq_rel))) {
                REALM_ASSERT_DEBUG(old_state == SSBState::kFinished);
                m_shared->m_callback(m_shared.get());
            }
        });

        return notReady();
    }

    explicit Future(util::bind_ptr<SharedState<T>> ptr)
        : m_shared(std::move(ptr))
    {
    }

    // At most one of these will be active.
    util::Optional<T> m_immediate;
    util::bind_ptr<SharedState<T>> m_shared;
};

/**
 * The void specialization of Future<T>. See the general Future<T> for detailed documentation.
 * It should be the same as the generic Future<T> with the following exceptions:
 *   - Anything mentioning StatusWith<T> will use Status instead.
 *   - Anything returning references to T will just return void since there are no void references.
 *   - Anything taking a T argument will receive no arguments.
 */
template <>
class REALM_NODISCARD future_details::Future<void> {
public:
    using value_type = void;

    /* implicit */ Future()
        : Future(make_ready())
    {
    }
    /* implicit */ Future(Status status)
        : Future(make_ready(std::move(status)))
    {
    }

    static Future<void> make_ready()
    {
        return Future<FakeVoid>::make_ready(FakeVoid{});
    }

    static Future<void> make_ready(Status status)
    {
        if (status.is_ok())
            return make_ready();
        return Future<FakeVoid>::make_ready(std::move(status));
    }

    bool is_ready() const
    {
        return inner.is_ready();
    }

    void get() const
    {
        inner.get();
    }

    Status get_no_throw() const noexcept
    {
        return inner.get_no_throw().get_status();
    }

    template <typename Func> // Status -> void
    void get_async(Func&& func) && noexcept
    {
        return std::move(inner).get_async(std::forward<Func>(func));
    }

    Future<void> ignoreValue() && noexcept
    {
        return std::move(*this);
    }

private:
    template <typename T>
    friend class Future;
    friend class Promise<void>;

    explicit Future(util::bind_ptr<SharedState<FakeVoid>> ptr)
        : inner(std::move(ptr))
    {
    }
    /*implicit*/ Future(Future<FakeVoid>&& inner)
        : inner(std::move(inner))
    {
    }
    /*implicit*/ operator Future<FakeVoid>() &&
    {
        return std::move(inner);
    }

    static Future<void> make_ready(StatusWith<FakeVoid> status)
    {
        return Future<FakeVoid>::make_ready(std::move(status));
    }

    Future<FakeVoid> inner;
};


/**
 * Returns a bound Promise and Future in a struct with friendly names (promise and future) that also
 * works well with C++17 structured bindings.
 */
template <typename T>
inline auto make_promise_future()
{
    return Promise<T>::make_promise_future_impl();
}

/**
 * This metafunction allows APIs that take callbacks and return Future to avoid doing their own type
 * calculus. This results in the base value_type that would result from passing Func to a
 * Future<T>::then(), with the same normalizing of T/StatusWith<T>/Future<T> returns. This is
 * primarily useful for implementations of executors rather than their users.
 *
 * This returns the unwrapped T rather than Future<T> so it will be easy to create a Promise<T>.
 *
 * Examples:
 *
 * FutureContinuationResult<std::function<void()>> == void
 * FutureContinuationResult<std::function<Status()>> == void
 * FutureContinuationResult<std::function<Future<void>()>> == void
 *
 * FutureContinuationResult<std::function<int()>> == int
 * FutureContinuationResult<std::function<StatusWith<int>()>> == int
 * FutureContinuationResult<std::function<Future<int>()>> == int
 *
 * FutureContinuationResult<std::function<int(bool)>, bool> == int
 *
 * FutureContinuationResult<std::function<int(bool)>, NotBool> SFINAE-safe substitution failure.
 */
template <typename Func, typename... Args>
using FutureContinuationResult =
    typename future_details::FutureContinuationResultImpl<std::result_of_t<Func(Args&&...)>>::type;

//
// Implementations of methods that couldn't be defined in the class due to ordering requirements.
//

template <typename T>
inline Future<T> Promise<T>::get_future() noexcept
{
    m_shared_state->thread_unsafe_inc_refs_to(2);
    return Future<T>(util::bind_ptr<SharedState<T>>(m_shared_state.get(), util::bind_ptr_base::adopt_tag{}));
}

template <typename T>
inline void Promise<T>::set_from(Future<T>&& future) noexcept
{
    set_impl([&] {
        std::move(future).propagate_result_to(m_shared_state.get());
    });
}

} // namespace realm::util
