/*************************************************************************
 *
 * Copyright 2024 Realm Inc.
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

#include <realm/util/future.hpp>

#include <functional>

namespace realm::util {
/*
 * These functions help futurize APIs that take a callback rather than return a future.
 *
 * You must implement the template specialization for status_from_error for whatever error type
 * your async API works with to convert errors into Status's.
 *
 * Then given an API like
 *
 * struct AsyncThing {
 *     struct Result { ... };
 *
 *     void do_async_thing(std::string arg_1, int arg_2,
 *                         util::UniqueFunction<void(std::error_code, Result res)> callback);
 * };
 *
 * AsyncThing thing;
 *
 * you can futurize it by calling:
 *
 * auto future = async_future_adapter<Result, std::error_code>(thing, &AsyncThing::do_async_thing,
 *                                                             anchor, std::string{"hello, world"}, 5);
 *
 * The async function will be called immediately on the calling thread.
 *
 * If you have a network::Service or some other event loop that implements post(), you can use:
 *
 * auto future = async_future_adapter<Result, std::error_code>(service, thing, &AsyncThing::do_async_thing,
 *                                                             anchor, std::string{"hello, world"}, 5);
 *
 * and your future will get created/executed on the event loop rather than the calling thread.
 *
 * The "anchor" is a value that will get captured in the lambda that runs the async function
 * and can be used to make sure any arguments/context for the async function remain alive until
 * at least when the async function gets called.
 */

template <typename Error>
Status status_from_error(Error);

template <typename T, typename Error, typename OperObj, typename Anchor, typename AsyncFn, typename... Args>
auto make_async_future_adapter(OperObj& obj, AsyncFn&& fn_ptr, Anchor&& anchor, Args&&... args)
{
    auto pf = util::make_promise_future<T>();
    auto run = [anchor = std::move(anchor), &obj, fn_ptr, promise = std::move(pf.promise),
                args...](Status status) mutable {
        if (!status.is_ok()) {
            promise.set_error(status);
            return;
        }

        auto fn = std::mem_fn(fn_ptr);
        if constexpr (std::is_void_v<T>) {
            fn(obj, args..., [promise = std::move(promise), anchor = std::move(anchor)](Error ec) mutable {
                if (ec) {
                    promise.set_error(status_from_error(ec));
                }
                else {
                    promise.emplace_value();
                }
            });
        }
        else {
            struct Callable {
                util::Promise<T> promise;
                Anchor anchor;

                void operator()(Error ec, T result)
                {
                    if (ec) {
                        promise.set_error(status_from_error(ec));
                    }
                    else {
                        promise.emplace_value(std::move(result));
                    }
                }

                void operator()(T result, Error ec)
                {
                    (*this)(ec, std::move(result));
                }
            } callback{std::move(promise), std::move(anchor)};
            fn(obj, args..., std::move(callback));
        }
    };
    return std::make_pair(std::move(pf.future), std::move(run));
}

template <typename T, typename Error, typename Service, typename OperObj, typename Anchor, typename AsyncFn,
          typename... Args>
auto async_future_adapter(Service& service, OperObj& obj, AsyncFn&& fn_ptr, Anchor&& anchor, Args&&... args)
{
    auto&& [future, run] = make_async_future_adapter<T, Error>(
        obj, std::forward<AsyncFn>(fn_ptr), std::forward<Anchor>(anchor), std::forward<Args>(args)...);
    service.post(std::move(run));
    return std::move(future);
}

template <typename T, typename Error, typename OperObj, typename Anchor, typename AsyncFn, typename... Args>
auto async_future_adapter(OperObj& obj, AsyncFn&& fn_ptr, Anchor&& anchor, Args&&... args)
{
    auto&& [future, run] = make_async_future_adapter<T, Error>(
        obj, std::forward<AsyncFn>(fn_ptr), std::forward<Anchor>(anchor), std::forward<Args>(args)...);
    run(Status::OK());
    return std::move(future);
}

} // namespace realm::util
