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

#pragma once

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
 *                                                             std::string{"hello, world"}, 5);
 *
 * The async function will be called immediately on the calling thread.
 *
 */

template <typename Error>
Status status_from_error(Error);

template <typename T, typename Error, typename OperObj, typename AsyncFn, typename... Args>
auto do_async_future_adapter(OperObj& obj, AsyncFn&& fn_ptr, Args&&... args)
{
    auto pf = util::make_promise_future<T>();
    auto fn = std::mem_fn(fn_ptr);
    if constexpr (std::is_void_v<T>) {
        fn(obj, args..., [promise = std::move(pf.promise)](Error ec) mutable {
            if constexpr (std::is_same_v<Error, Status>) {
                if (!ec.is_ok()) {
                    promise.set_error(ec);
                    return;
                }
            }
            else {
                auto status = status_from_error(ec);
                if (!status.is_ok()) {
                    promise.set_error(status);
                    return;
                }
            }

            promise.emplace_value();
        });
    }
    else {
        struct Callable {
            util::Promise<T> promise;

            void operator()(Error ec, T result)
            {
                if constexpr (std::is_same_v<Error, Status>) {
                    if (!ec.is_ok()) {
                        promise.set_error(ec);
                        return;
                    }
                }
                else {
                    auto status = status_from_error(ec);
                    if (!status.is_ok()) {
                        promise.set_error(status);
                        return;
                    }
                }
                promise.emplace_value(std::move(result));
            }

            void operator()(T result, Error ec)
            {
                (*this)(ec, std::move(result));
            }
        } callback{std::move(pf.promise)};
        fn(obj, args..., std::move(callback));
    }
    return std::move(pf.future);
}

template <typename T, typename Error, typename OperObj, typename AsyncFn, typename... Args>
auto async_future_adapter(OperObj& obj, AsyncFn&& fn_ptr, Args&&... args)
{
    return do_async_future_adapter<T, Error>(obj, fn_ptr, args...);
}

} // namespace realm::util
