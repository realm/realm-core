/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
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

#ifndef REALM_UTIL_SCOPE_EXIT_HPP
#define REALM_UTIL_SCOPE_EXIT_HPP

#include <exception>
#include <optional>

namespace realm::util {

// A guard which invokes the given function when exiting the scope (either via
// an exception or normal flow), used to clean up state which is not owned by
// an explicit RAII type.
//
// void foo()
// {
//     begin_foo();
//     ScopeExit cleanup([&]() noexcept {
//         end_foo();
//     });
//
//     // Do some things which may throw an exception
// }
template <class H>
class ScopeExit {
public:
    explicit ScopeExit(H&& handler) noexcept(std::is_nothrow_move_constructible<H>::value)
        : m_handler(std::move(handler))
    {
    }

    ScopeExit(ScopeExit&& se) noexcept(std::is_nothrow_move_constructible<H>::value)
        : m_handler(std::move(se.m_handler))
    {
        se.m_handler = std::nullopt;
    }

    ~ScopeExit()
    {
        if (m_handler)
            (*m_handler)();
    }
    void cancel() noexcept
    {
        m_handler = std::nullopt;
    }

    static_assert(noexcept(std::declval<H>()()), "Handler must be nothrow executable");
    static_assert(std::is_nothrow_destructible<H>::value, "Handler must be nothrow destructible");

private:
    std::optional<H> m_handler;
};

template <class H>
ScopeExit(H&&) -> ScopeExit<typename std::remove_reference_t<H>>;

// Similar to ScopeExit, but the handler is *only* invoked if the scope is
// exited via throwing an exception.
template <class H>
class ScopeExitFail : public ScopeExit<H> {
public:
    explicit ScopeExitFail(H&& handler) noexcept(std::is_nothrow_move_constructible<H>::value)
        : ScopeExit<H>(std::move(handler))
    {
    }

    ~ScopeExitFail()
    {
        if (std::uncaught_exceptions() == m_exception_count)
            this->cancel();
    }

private:
    int m_exception_count = std::uncaught_exceptions();
};

template <class H>
ScopeExitFail(H&&) -> ScopeExitFail<typename std::remove_reference_t<H>>;

// A helper which was required pre-C++17. New code should prefer `ScopeExit cleanup([&]() noexcept { ... })`.
template <class H>
ScopeExit<typename std::remove_reference_t<H>> make_scope_exit(H&& handler) noexcept(
    noexcept(ScopeExit<typename std::remove_reference_t<H>>(std::forward<H>(handler))))
{
    return ScopeExit<typename std::remove_reference_t<H>>(std::forward<H>(handler));
}

} // namespace realm::util

#endif // REALM_UTIL_SCOPE_EXIT_HPP
