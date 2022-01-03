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

#include <functional>
#include <memory>
#include <type_traits>

#include "realm/util/assert.hpp"
#include "realm/util/type_traits.hpp"

namespace realm::util {

/**
 * A `UniqueFunction` is a move-only, type-erased functor object similar to `std::function`.
 * It is useful in situations where a functor cannot be wrapped in `std::function` objects because
 * it is incapable of being copied.  Often this happens with C++14 or later lambdas which capture a
 * `std::unique_ptr` by move.  The interface of `UniqueFunction` is nearly identical to
 * `std::function`, except that it is not copyable.
 */
template <typename Function>
class UniqueFunction;

template <typename Function>
class UniqueFunctionBase;

template <typename RetType, typename... Args>
class UniqueFunctionBase<RetType(Args...)> {
public:
    ~UniqueFunctionBase() noexcept = default;
    UniqueFunctionBase() = default;

    UniqueFunctionBase(UniqueFunctionBase&&) noexcept = default;
    UniqueFunctionBase& operator=(UniqueFunctionBase&&) noexcept = default;

    UniqueFunctionBase(std::nullptr_t) noexcept {}
    UniqueFunctionBase& operator=(std::nullptr_t) noexcept
    {
        m_impl.reset();
        return *this;
    }

    void swap(UniqueFunctionBase& that) noexcept
    {
        using std::swap;
        swap(m_impl, that.m_impl);
    }

    friend void swap(UniqueFunctionBase& lhs, UniqueFunctionBase& rhs)
    {
        lhs.swap(rhs);
    }

    explicit operator bool() const noexcept
    {
        return static_cast<bool>(m_impl);
    }

protected:
    class Impl {
    public:
        virtual ~Impl() noexcept = default;

        virtual RetType call(Args&&... args) = 0;

        template <typename Functor>
        static auto make(Functor&& functor)
        {
            struct SpecificImpl : Impl {
                explicit SpecificImpl(Functor&& func)
                    : f(std::forward<Functor>(func))
                {
                }

                RetType call(Args&&... args) override
                {
                    if constexpr (std::is_void_v<RetType>) {
                        f(std::forward<Args>(args)...);
                    }
                    else {
                        return f(std::forward<Args>(args)...);
                    }
                }

                std::decay_t<Functor> f;
            };

            return std::make_unique<SpecificImpl>(std::forward<Functor>(functor));
        }
    };

    explicit UniqueFunctionBase(std::unique_ptr<Impl> impl)
        : m_impl(std::move(impl))
    {
    }

    std::unique_ptr<Impl> m_impl;
};

template <typename RetType, typename... Args>
class UniqueFunction<RetType(Args...)> : public UniqueFunctionBase<RetType(Args...)> {
    template <typename Functor>
    constexpr static bool is_functor =
        !std::is_same_v<std::decay_t<Functor>, UniqueFunction> && std::is_invocable_r_v<RetType, Functor, Args...>;

public:
    using Base = UniqueFunctionBase<RetType(Args...)>;

    using Base::Base;
    using Base::operator=;

    template <typename Functor, std::enable_if_t<is_functor<Functor>, int> = 0,
              std::enable_if_t<std::is_move_constructible<Functor>::value, int> = 0>
    UniqueFunction(Functor&& functor)
        : Base(Base::Impl::make(std::forward<Functor>(functor)))
    {
    }

    RetType operator()(Args... args) const
    {
        return Base::m_impl->call(std::forward<Args>(args)...);
    }
};

template <typename RetType, typename... Args>
class UniqueFunction<RetType(Args...) noexcept> : public UniqueFunctionBase<RetType(Args...)> {
    template <typename Functor>
    constexpr static bool is_noexcept_functor = !std::is_same_v<std::decay_t<Functor>, UniqueFunction> &&
                                                std::is_nothrow_invocable_r_v<RetType, Functor, Args...>;

public:
    using Base = UniqueFunctionBase<RetType(Args...)>;

    using Base::Base;
    using Base::operator=;

    template <typename Functor, std::enable_if_t<is_noexcept_functor<Functor>, int> = 0,
              std::enable_if_t<std::is_move_constructible<Functor>::value, int> = 0>
    UniqueFunction(Functor&& functor)
        : Base(Base::Impl::make(std::forward<Functor>(functor)))
    {
    }

    RetType operator()(Args... args) const noexcept
    {
        return Base::m_impl->call(std::forward<Args>(args)...);
    }
};

/**
 * Helper to pattern-match the signatures for all combinations of const and l-value-qualifed member
 * function pointers. We don't currently support r-value-qualified call operators.
 */
template <typename>
struct UFDeductionHelper {
};
template <typename Class, typename Ret, typename... Args>
struct UFDeductionHelper<Ret (Class::*)(Args...)> : TypeIdentity<Ret(Args...)> {
};
template <typename Class, typename Ret, typename... Args>
struct UFDeductionHelper<Ret (Class::*)(Args...)&> : TypeIdentity<Ret(Args...)> {
};
template <typename Class, typename Ret, typename... Args>
struct UFDeductionHelper<Ret (Class::*)(Args...) const> : TypeIdentity<Ret(Args...)> {
};
template <typename Class, typename Ret, typename... Args>
struct UFDeductionHelper<Ret (Class::*)(Args...) const&> : TypeIdentity<Ret(Args...)> {
};
template <typename Class, typename Ret, typename... Args>
struct UFDeductionHelper<Ret (Class::*)(Args...) noexcept> : TypeIdentity<Ret(Args...)> {
};
template <typename Class, typename Ret, typename... Args>
struct UFDeductionHelper<Ret (Class::*)(Args...)& noexcept> : TypeIdentity<Ret(Args...)> {
};
template <typename Class, typename Ret, typename... Args>
struct UFDeductionHelper<Ret (Class::*)(Args...) const noexcept> : TypeIdentity<Ret(Args...)> {
};
template <typename Class, typename Ret, typename... Args>
struct UFDeductionHelper<Ret (Class::*)(Args...) const& noexcept> : TypeIdentity<Ret(Args...)> {
};

/**
 * Deduction guides for UniqueFunction<Sig> that pluck the signature off of function pointers and
 * non-overloaded, non-generic function objects such as lambdas that don't use `auto` arguments.
 */
template <typename Ret, typename... Args>
UniqueFunction(Ret (*)(Args...)) -> UniqueFunction<Ret(Args...)>;
template <typename T, typename Sig = typename UFDeductionHelper<decltype(&T::operator())>::type>
UniqueFunction(T) -> UniqueFunction<Sig>;

template <typename Ret, typename... Args>
UniqueFunction(Ret (*)(Args...) noexcept) -> UniqueFunction<Ret(Args...) noexcept>;

template <typename Signature>
bool operator==(const UniqueFunction<Signature>& lhs, std::nullptr_t) noexcept
{
    return !lhs;
}

template <typename Signature>
bool operator!=(const UniqueFunction<Signature>& lhs, std::nullptr_t) noexcept
{
    return static_cast<bool>(lhs);
}

template <typename Signature>
bool operator==(std::nullptr_t, const UniqueFunction<Signature>& rhs) noexcept
{
    return !rhs;
}

template <typename Signature>
bool operator!=(std::nullptr_t, const UniqueFunction<Signature>& rhs) noexcept
{
    return static_cast<bool>(rhs);
}

} // namespace realm::util
