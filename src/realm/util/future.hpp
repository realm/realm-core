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

#pragma once

#include "realm/exceptions.hpp"
#include "realm/status_with.hpp"
#include "realm/util/assert.hpp"
#include "realm/util/bind_ptr.hpp"
#include "realm/util/features.h"
#include "realm/util/functional.hpp"
#include "realm/util/scope_exit.hpp"
#include "realm/util/overload.hpp"

#include <external/mpark/variant.hpp>

#include <condition_variable>
#include <mutex>
#include <type_traits>

namespace realm::util {

namespace future_details {
template <typename T>
class Promise;

template <typename T>
class CopyablePromiseHolder;

template <typename T>
class Future;

template <typename T>
constexpr static bool is_future = false;
template <typename T>
constexpr static bool is_future<Future<T>> = true;

// UnstatusType/UnwrappedType and their implementations are internal helper types for futures to deduce the actual
// type of a value being wrapped by a Expected or Future (or a Status in case of void). They should not be visible
// outside of future_details.
template <typename T>
struct UnstatusTypeImpl {
    using type = T;
};
template <typename T>
struct UnstatusTypeImpl<Expected<T>> {
    using type = typename UnstatusTypeImpl<T>::type;
};
template <>
struct UnstatusTypeImpl<Status> {
    using type = void;
};
template <typename T>
using UnstatusType = typename UnstatusTypeImpl<T>::type;

template <typename T>
struct UnwrappedTypeImpl {
    using type = T;
};
template <typename T>
struct UnwrappedTypeImpl<Future<T>> {
    using type = T;
};
template <typename T>
struct UnwrappedTypeImpl<Expected<T>> {
    using type = T;
};
template <>
struct UnwrappedTypeImpl<Status> {
    using type = void;
};
template <typename T>
using UnwrappedType = typename UnwrappedTypeImpl<T>::type;

/**
 * no_throw_call() normalizes return values so everything returns Expected<T>. Exceptions are
 * converted to !OK statuses. void and Status returns are converted to Expected<void>
 */
template <typename Func, typename... Args>
auto no_throw_call(Func&& func, Args&&... args) noexcept
{
    using RawResult = std::invoke_result_t<Func, Args...>;
    using Result = Expected<UnstatusType<RawResult>>;
    try {
        if constexpr (std::is_void_v<RawResult>) {
            std::invoke(std::forward<Func>(func), std::forward<Args>(args)...);
            return Result();
        }
        else if constexpr (std::is_same_v<RawResult, Status>) {
            auto s = std::invoke(std::forward<Func>(func), std::forward<Args>(args)...);
            if (!s.is_ok()) {
                return Result(std::move(s));
            }
            return Result();
        }
        else {
            return Result(std::invoke(std::forward<Func>(func), std::forward<Args>(args)...));
        }
    }
    catch (...) {
        return Result(exception_to_status());
    }
}

template <typename T, typename Fn>
auto map(T&& val, Fn&& func)
{
    return std::forward<T>(val).and_then([&](auto&&... val) {
        return no_throw_call(func, std::move(val)...);
    });
}

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
        // Assert that we haven't rolled over the counter here.
        auto ref_count = m_refs.fetch_add(1, std::memory_order_relaxed) + 1;
        REALM_ASSERT_DEBUG(ref_count != 0);
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
    return util::bind_ptr<T>(ptr, util::bind_ptr_adopt_tag{});
}

template <typename T>
struct SharedStateImpl;

template <typename T>
using SharedState = SharedStateImpl<T>;

/**
 * SSB is SharedStateBase, and this is its current state.
 */
enum class SSBState : uint8_t {
    Init,
    Waiting,
    Finished, // This should stay last since we have code like assert(state < Finished).
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
        if (m_state.load(std::memory_order_acquire) == SSBState::Finished) {
            return;
        }

        m_cv.emplace();

        auto old_state = SSBState::Init;
        if (REALM_UNLIKELY(
                !m_state.compare_exchange_strong(old_state, SSBState::Waiting, std::memory_order_acq_rel))) {
            REALM_ASSERT_DEBUG(old_state == SSBState::Finished);
            return;
        }

        std::unique_lock<std::mutex> lk(m_mutex);
        m_cv->wait(lk, [&] {
            return m_state.load(std::memory_order_acquire) == SSBState::Finished;
        });
    }

    void transition_to_finished() noexcept
    {
        auto old_state = m_state.exchange(SSBState::Finished, std::memory_order_acq_rel);
        if (old_state == SSBState::Init) {
            return;
        }

        REALM_ASSERT_DEBUG(old_state == SSBState::Waiting);

#ifdef REALM_DEBUG
        // If you hit this limit one of two things has probably happened
        //
        // 1. The justForContinuation optimization isn't working.
        // 2. You may be creating a variable length chain.
        constexpr size_t kMaxDepth = 32;

        size_t depth = 0;
        for (auto ssb = m_continuation.get(); ssb;
             ssb = ssb->m_state.load(std::memory_order_acquire) == SSBState::Waiting ? ssb->m_continuation.get()
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

    // All the rest of the methods are only called by the promise side.

    //
    // Concurrency Rules for members: Each non-atomic member is initially owned by either the
    // Promise side or the Future side, indicated by a P/F comment. The general rule is that members
    // representing the propagating data are owned by Promise, while members representing what
    // to do with the data are owned by Future. The owner may freely modify the members it owns
    // until it releases them by doing a release-store to state of Finished from Promise or
    // Waiting from Future. Promise can acquire access to all members by doing an acquire-load of
    // state and seeing Waiting (or Future with Finished). Transitions should be done via
    // acquire-release exchanges to combine both actions.
    //
    // Future::propagateResults uses an alternative mechanism to transfer ownership of the
    // continuation member. The logical Future-side does a release-store of true to
    // isJustForContinuation, and the Promise-side can do an acquire-load seeing true to get access.
    //

    std::atomic<SSBState> m_state{SSBState::Init};

    // This is used to prevent infinite chains of SharedStates that just propagate results.
    std::atomic<bool> m_is_just_for_continuation{false};

    // This is likely to be a different derived type from this, since it is the logical output of
    // callback.
    util::bind_ptr<SharedStateBase> m_continuation; // F

    util::UniqueFunction<void(SharedStateBase*)> m_callback; // F

    std::mutex m_mutex;                           // F
    util::Optional<std::condition_variable> m_cv; // F

protected:
    SharedStateBase() = default;
};

template <typename T>
struct SharedStateImpl final : public SharedStateBase {
    void fill_from(SharedState<T>&& other)
    {
        REALM_ASSERT_DEBUG(m_state.load() < SSBState::Finished);
        REALM_ASSERT_DEBUG(other.m_state.load() == SSBState::Finished);
        REALM_ASSERT_DEBUG(m_owned_by_promise.load());
        m_data.emplace(std::move(*other.m_data));
        transition_to_finished();
    }

    template <typename... Args>
    void emplace_value(Args&&... args) noexcept
    {
        REALM_ASSERT_DEBUG(m_state.load() < SSBState::Finished);
        REALM_ASSERT_DEBUG(m_owned_by_promise.load());
        try {
            m_data.emplace(std::forward<Args>(args)...);
        }
        catch (...) {
            m_data = exception_to_status();
        }
        transition_to_finished();
    }

    void set_from(Expected<T> roe)
    {
        m_data.emplace(std::move(roe));
        transition_to_finished();
    }

    void set_from(Expected<Future<T>> roe)
    {
        if (!roe.has_value()) {
            m_data = roe.error();
            transition_to_finished();
        }
        else {
            std::move(*roe).propagate_result_to(this);
        }
    }

    void disown() const
    {
        bool was_owned = m_owned_by_promise.exchange(false);
        REALM_ASSERT(was_owned);
        static_cast<void>(was_owned);
    }

    void claim() const
    {
        bool was_owned = m_owned_by_promise.exchange(true);
        REALM_ASSERT(!was_owned);
        static_cast<void>(was_owned);
    }


    mutable std::atomic<bool> m_owned_by_promise{true};
    std::optional<Expected<T>> m_data;
};

template <typename Result, typename T, typename OnReady>
Future<Result> make_continuation(SharedState<T>& state, OnReady&& on_ready)
{
    REALM_ASSERT(!state.m_callback && !state.m_continuation);

    auto continuation = make_intrusive<SharedState<Result>>();
    continuation->thread_unsafe_inc_refs_to(2);
    state.m_continuation.reset(continuation.get(), util::bind_ptr_adopt_tag{});
    state.m_callback = [on_ready = std::forward<OnReady>(on_ready)](SharedStateBase* ssb) mutable noexcept {
        const auto input = static_cast<SharedState<T>*>(ssb);
        const auto output = static_cast<SharedState<Result>*>(ssb->m_continuation.get());
        on_ready(input, output);
    };
    return Future<Result>(std::move(continuation));
}

} // namespace future_details

// These are in the future_details namespace to get access to its contents, but they are part of the
// public API.
using future_details::CopyablePromiseHolder;
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
            m_shared_state->m_data = Status(ErrorCodes::BrokenPromise, "Broken Promise");
        }
    }

    // If we want to enable move-assignability, we need to handle breaking the promise on the old
    // value of this.
    Promise& operator=(Promise&&) = delete;

    // The default move construction is fine.
    Promise(Promise&&) noexcept = default;

    /**
     * Sets the value into this Promise when the passed-in Future completes, which may have already
     * happened. If it hasn't, it is still safe to destroy this Promise since it is no longer
     * involved.
     */
    void set_from(Future<T>&& future) noexcept;

    void set_result(Expected<T> sw) noexcept
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
    friend class CopyablePromiseHolder<T>;

    Promise(util::bind_ptr<SharedState<T>> shared_state)
        : m_shared_state(std::move(shared_state))
    {
        m_shared_state->claim();
    }

    util::bind_ptr<SharedState<T>> release() &&
    {
        auto ret = std::move(m_shared_state);
        ret->disown();
        return ret;
    }

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
 * CopyablePromiseHolder<T> is a lightweight copyable holder for Promises so they can be captured inside
 * of std::function's and other types that require all members to be copyable.
 *
 * The only thing you can do with a CopyablePromiseHolder is extract a regular promise from it exactly once,
 * and copy/move it as you would a util::bind_ptr.
 *
 * Do not use this type to try to fill a Promise from multiple places or threads.
 */
template <typename T>
class future_details::CopyablePromiseHolder {
public:
    CopyablePromiseHolder(Promise<T>&& input)
        : m_shared_state(std::move(input).release())
    {
    }

    CopyablePromiseHolder(const Promise<T>&) = delete;

    Promise<T> get_promise()
    {
        REALM_ASSERT(m_shared_state);
        return Promise<T>(std::move(m_shared_state));
    }

private:
    util::bind_ptr<SharedState<T>> m_shared_state;
};

/**
 * Future<T> is logically a possibly-deferred T or exception_ptr
 * As is usual for rvalue-qualified methods, you may call at most one of them on a given Future.
 *
 * A future may be passed between threads, but only one thread may use it at a time.
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

    template <typename U = T, std::enable_if_t<std::is_same_v<T, U> && !std::is_void_v<U>, int> = 0>
    /* implicit */ Future(U val)
        : m_data(Immediate(std::move(val)))
    {
    }
    /* implicit */ Future(Status status)
        : m_data(Immediate(std::move(status)))
    {
    }
    /* implicit */ Future(Expected<T> sw)
        : m_data(std::move(sw))
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
    template <typename U = T, std::enable_if_t<std::is_same_v<T, U> && !std::is_void_v<U>, int> = 0>
    static Future<T> make_ready(U val)
    {
        return std::move(val);
    }

    template <typename U = T, std::enable_if_t<std::is_same_v<T, U> && std::is_void_v<U>, int> = 0>
    static Future<T> make_ready()
    {
        return Immediate{};
    }

    static Future<T> make_ready(Status status)
    {
        return std::move(status);
    }

    static Future<T> make_ready(Expected<T> val)
    {
        return std::move(val);
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
    bool is_ready() const noexcept
    {
        if (auto shared = mpark::get_if<Shared>(&m_data)) {
            // This can be a relaxed load because callers are not allowed to use it to establish
            // ordering.
            return (*shared)->m_state.load(std::memory_order_relaxed) == SSBState::Finished;
        }
        return true;
    }

    /**
     * Gets the value out of this Future, blocking until it is ready.
     *
     * get() methods throw on error, while get_no_throw() returns a Expected<T> with either a value
     * or an error Status.
     *
     * These methods can be called multiple times, except for the rvalue overloads.
     */
    template <typename U = T, std::enable_if_t<std::is_same_v<T, U> && !std::is_void_v<U>, int> = 0>
    U get() &&
    {
        return std::move(*this).get_no_throw().value();
    }
    template <typename U = T, std::enable_if_t<std::is_same_v<T, U> && !std::is_void_v<U>, int> = 0>
    U& get() &
    {
        return get_no_throw().value();
    }
    template <typename U = T, std::enable_if_t<std::is_same_v<T, U> && !std::is_void_v<U>, int> = 0>
    const U& get() const&
    {
        return get_no_throw().value();
    }
    template <typename U = T, std::enable_if_t<std::is_same_v<T, U> && std::is_void_v<U>, int> = 0>
    void get() const
    {
        get_no_throw().value();
    }

    Expected<T> get_no_throw() && noexcept
    {
        return std::move(get_impl());
    }
    Expected<T>& get_no_throw() & noexcept
    {
        return get_impl();
    }
    const Expected<T>& get_no_throw() const& noexcept
    {
        return const_cast<Future<T>*>(this)->get_impl();
    }

    /**
     * This ends the Future continuation chain by calling a callback on completion. Use this to
     * escape back into a callback-based API.
     *
     * The callback must not throw since it is called from a noexcept context. The callback must take a
     * Expected<T> as its argument and have a return type of void.
     */
    template <typename Func> // (Expected<T>&&) noexcept -> void
    void get_async(Func&& func) && noexcept
    {
        static_assert(std::is_nothrow_invocable_r_v<void, Func, Expected<T>&&>,
                      "get_async() callback must be (Expected<T>&&) noexcept -> void");
        return general_impl(
            [&](Expected<T>&& val) {
                func(std::move(val));
            },
            [&](SharedState<T>& shared) {
                shared.m_callback = [func = std::forward<Func>(func)](SharedStateBase* ssb) mutable noexcept {
                    const auto input = static_cast<SharedState<T>*>(ssb);
                    func(std::move(*input->m_data));
                };
            });
    }

    //
    // The remaining methods are all continuation based and take a callback and return a Future.
    // Each method has a comment indicating the supported signatures for that callback, and a
    // description of when the callback is invoked and how the impacts the returned Future. It may
    // be helpful to think of Future continuation chains as a pipeline of stages that take input
    // from earlier stages and produce output for later stages.
    //
    // Be aware that the callback may be invoked inline at the call-site or at the producer when
    // setting the value. Therefore, you should avoid doing blocking work inside of a callback.
    // Additionally, avoid acquiring any locks or mutexes that the caller already holds, otherwise
    // you risk a deadlock. If either of these concerns apply to your callback, it should schedule
    // itself on an executor, rather than doing work in the callback.
    //
    // Error handling in callbacks: all exceptions thrown propagate to the returned Future
    // automatically.
    //
    // Callbacks that return Future<T> are automatically unwrapped and connected to the returned
    // Future<T>, rather than producing a Future<Future<T>>.

    /**
     * Callbacks passed to then() are only called if the input Future completes successfully.
     * Otherwise the error propagates automatically, bypassing the callback.
     */
    template <typename Func>
    auto then(Func&& func) && noexcept
    {
        using RawResult = typename std::conditional_t<std::is_same_v<T, void>, std::invoke_result<Func>,
                                                      std::invoke_result<Func, T>>::type;
        using Result = UnwrappedType<RawResult>;
        static_assert(!is_future<Result>);
        return general_impl(
            [&](Expected<T>&& val) {
                return Future<Result>(map(std::move(val), func));
            },
            [&](SharedState<T>& shared) {
                return make_continuation<Result>(
                    shared, [func = std::forward<Func>(func)](SharedState<T>* input,
                                                              SharedState<Result>* output) mutable noexcept {
                        output->set_from(map(std::move(*input->m_data), func));
                    });
            });
    }

    /*
     * Callbacks passed to on_completion() are always called with a Expected<T> when the input future completes.
     */
    template <typename Func> // (Expected<T>) -> U
    auto on_completion(Func&& func) && noexcept
    {
        static_assert(std::is_invocable_v<Func, Expected<T>&&>,
                      "on_completion() callback must be invokable with Expected<T>");
        using Result = UnwrappedType<std::invoke_result_t<Func, Expected<T>&&>>;
        return general_impl(
            [&](Expected<T>&& val) {
                return Future<Result>(no_throw_call(std::forward<Func>(func), std::move(val)));
            },
            [&](SharedState<T>& shared) {
                return make_continuation<Result>(
                    shared, [func = std::forward<Func>(func)](SharedState<T>* input,
                                                              SharedState<Result>* output) mutable noexcept {
                        output->set_from(no_throw_call(func, std::move(*input->m_data)));
                    });
            });
    }

    /**
     * Callbacks passed to on_error() are only called if the input Future completes with an error.
     * Otherwise, the successful result propagates automatically, bypassing the callback.
     *
     * The callback can either produce a replacement value (which must be a T), return a replacement
     * Future<T> (such as a by retrying), or return/throw a replacement error.
     *
     * Note that this will only catch errors produced by earlier stages; it is not registering a
     * general error handler for the entire chain.
     */
    template <typename Func>
    Future<T> on_error(Func&& func) && noexcept
    {
        using Result = UnwrappedType<std::invoke_result_t<Func, Status>>;
        static_assert(std::is_same_v<UnwrappedType<Result>, T>,
                      "func passed to Future<T>::on_error must return T, Expected<T>, or Future<T>");

        return general_impl(
            [&](Expected<T>&& val) -> Future<T> {
                if (val.has_value()) {
                    return std::move(val);
                }
                return no_throw_call(func, std::move(val).error());
            },
            [&](SharedState<T>& shared) {
                return make_continuation<T>(
                    shared, [func = std::forward<Func>(func)](SharedState<T>* input,
                                                              SharedState<T>* output) mutable noexcept {
                        if (input->m_data->has_value()) {
                            output->set_from(std::move(*input->m_data));
                        }
                        else {
                            output->set_from(no_throw_call(func, std::move(*input->m_data).error()));
                        }
                    });
            });
    }

    Future<void> ignore_value() && noexcept;

    Future(Expected<Future<T>>&& other)
    {
        if (other.has_value()) {
            m_data = std::move(other->m_data);
        }
        else {
            m_data = Immediate(other.error());
        }
    }

private:
    template <typename T2>
    friend class Future;
    friend class Promise<T>;
    friend struct SharedStateImpl<T>;

    template <typename Result, typename U, typename OnReady>
    friend Future<Result> make_continuation(SharedState<U>& state, OnReady&& on_ready);

    template <typename Ready, typename NotReady>
    decltype(auto) visit(Ready&& ready, NotReady&& not_ready) noexcept
    {
        return mpark::visit(overload{[](std::monostate) -> std::invoke_result_t<Ready, Immediate&> {
                                         REALM_TERMINATE("Invalid moved-from or default constructed Future");
                                     },
                                     [&](Immediate& immediate) -> decltype(auto) {
                                         return ready(immediate);
                                     },
                                     [&](Shared& shared) -> decltype(auto) {
                                         return not_ready(shared);
                                     }},
                            m_data);
    }

    Expected<T>& get_impl()
    {
        return visit(
            [](Immediate& immediate) -> Immediate& {
                return immediate;
            },
            [](Shared& shared) -> Immediate& {
                shared->wait();
                return *shared->m_data;
            });
    }

    // All callbacks are called immediately so they are allowed to capture everything by reference.
    // All callbacks should return the same return type.
    template <typename Ready, typename NotReady>
    decltype(auto) general_impl(Ready&& ready, NotReady&& not_ready) noexcept
    {
        return visit(
            [&](Immediate& immediate) -> decltype(auto) {
                return ready(std::move(immediate));
            },
            [&](Shared& shared) -> decltype(auto) {
                if (shared->m_state.load(std::memory_order_acquire) == SSBState::Finished) {
                    return ready(std::move(*shared->m_data));
                }
                // This is always done after not_ready, which never throws. It is in a
                // ScopeExit to support both void- and value-returning not_ready
                // implementations since we can't assign void to a variable.
                auto guard = util::make_scope_exit([&]() noexcept {
                    auto old_state = SSBState::Init;
                    if (REALM_UNLIKELY(!shared->m_state.compare_exchange_strong(old_state, SSBState::Waiting,
                                                                                std::memory_order_acq_rel))) {
                        REALM_ASSERT_DEBUG(old_state == SSBState::Finished);
                        shared->m_callback(shared.get());
                    }
                });

                return not_ready(*shared);
            });
    }

    void propagate_result_to(SharedState<T>* output) && noexcept
    {
        general_impl(
            [&](Immediate&& immediate) {
                output->set_from(std::move(immediate));
            },
            [&](SharedState<T>& shared) {
                // If the output is just for continuation, bypass it and just directly fill in the
                // SharedState that it would write to. The concurrency situation is a bit subtle
                // here since we are the Future-side of shared, but the Promise-side of output.
                // The rule is that p->isJustForContinuation must be acquire-read as true before
                // examining p->continuation, and p->continuation must be written before doing the
                // release-store of true to p->isJustForContinuation.
                if (output->m_is_just_for_continuation.load(std::memory_order_acquire)) {
                    shared.m_continuation = std::move(output->m_continuation);
                }
                else {
                    shared.m_continuation = util::bind_ptr(output);
                }
                shared.m_is_just_for_continuation.store(true, std::memory_order_release);

                shared.m_callback = [](SharedStateBase* ssb) noexcept {
                    const auto input = static_cast<SharedState<T>*>(ssb);
                    const auto output = static_cast<SharedState<T>*>(ssb->m_continuation.get());
                    output->fill_from(std::move(*input));
                };
            });
    }

    explicit Future(util::bind_ptr<SharedState<T>> ptr)
        : m_data(std::move(ptr))
    {
    }

    using Immediate = Expected<T>;
    using Shared = util::bind_ptr<SharedState<T>>;
    mpark::variant<std::monostate, Immediate, Shared> m_data;
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
 * Future<T>::then(), with the same normalizing of T/Expected<T>/Future<T> returns. This is
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
 * FutureContinuationResult<std::function<Expected<int>()>> == int
 * FutureContinuationResult<std::function<Future<int>()>> == int
 *
 * FutureContinuationResult<std::function<int(bool)>, bool> == int
 *
 * FutureContinuationResult<std::function<int(bool)>, NotBool> SFINAE-safe substitution failure.
 */
template <typename Func, typename... Args>
using FutureContinuationResult = typename future_details::UnwrappedType<std::invoke_result_t<Func, Args...>>;

//
// Implementations of methods that couldn't be defined in the class due to ordering requirements.
//

template <typename T>
inline Future<T> Promise<T>::get_future() noexcept
{
    m_shared_state->thread_unsafe_inc_refs_to(2);
    return Future<T>(util::bind_ptr<SharedState<T>>(m_shared_state.get(), util::bind_ptr_adopt_tag{}));
}

template <typename T>
inline void Promise<T>::set_from(Future<T>&& future) noexcept
{
    set_impl([&] {
        std::move(future).propagate_result_to(m_shared_state.get());
    });
}

} // namespace realm::util
