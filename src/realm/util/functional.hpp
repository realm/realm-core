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

template <typename Function>
class UniqueFunction;

/**
 * A `UniqueFunction` is a move-only, type-erased functor object similar to `std::function`.
 * It is useful in situations where a functor cannot be wrapped in `std::function` objects because
 * it is incapable of being copied.  Often this happens with C++14 or later lambdas which capture a
 * `std::unique_ptr` by move.  The interface of `UniqueFunction` is nearly identical to
 * `std::function`, except that it is not copyable.
 */
template <typename RetType, typename... Args>
class UniqueFunction<RetType(Args...)> {
private:
    // `TagTypeBase` is used as a base for the `TagType` type, to prevent it from being an
    // aggregate.
    struct TagTypeBase {
    protected:
        TagTypeBase() = default;
    };
    // `TagType` is used as a placeholder type in parameter lists for `enable_if` clauses.  They
    // have to be real parameters, not template parameters, due to MSVC limitations.
    class TagType : TagTypeBase {
        TagType() = default;
        friend UniqueFunction;
    };

public:
    using result_type = RetType;

    ~UniqueFunction() noexcept = default;
    UniqueFunction() = default;

    UniqueFunction(const UniqueFunction&) = delete;
    UniqueFunction& operator=(const UniqueFunction&) = delete;

    UniqueFunction(UniqueFunction&&) noexcept = default;
    UniqueFunction& operator=(UniqueFunction&&) noexcept = default;

    void swap(UniqueFunction& that) noexcept
    {
        using std::swap;
        swap(this->impl, that.impl);
    }

    friend void swap(UniqueFunction& a, UniqueFunction& b) noexcept
    {
        a.swap(b);
    }

    template <typename Functor>
    /* implicit */
    UniqueFunction(
        Functor&& functor,
        // The remaining arguments here are only for SFINAE purposes to enable this ctor when our
        // requirements are met.  They must be concrete parameters not template parameters to work
        // around bugs in some compilers that we presently use.  We may be able to revisit this
        // design after toolchain upgrades for C++17.
        std::enable_if_t<std::is_invocable_r<RetType, Functor, Args...>::value, TagType> = make_tag(),
        std::enable_if_t<std::is_move_constructible<Functor>::value, TagType> = make_tag(),
        std::enable_if_t<!std::is_same<std::decay_t<Functor>, UniqueFunction>::value, TagType> = make_tag())
        : impl(make_impl(std::forward<Functor>(functor)))
    {
    }

    UniqueFunction(std::nullptr_t) noexcept {}

    RetType operator()(Args... args) const
    {
        REALM_ASSERT(static_cast<bool>(*this));
        return impl->call(std::forward<Args>(args)...);
    }

    explicit operator bool() const noexcept
    {
        return static_cast<bool>(this->impl);
    }

    // Needed to make `std::is_convertible<mongo::UniqueFunction<...>, std::function<...>>` be
    // `std::false_type`.  `mongo::UniqueFunction` objects are not convertible to any kind of
    // `std::function` object, since the latter requires a copy constructor, which the former does
    // not provide.  If you see a compiler error which references this line, you have tried to
    // assign a `UniqueFunction` object to a `std::function` object which is impossible -- please
    // check your variables and function signatures.
    //
    // NOTE: This is not quite able to disable all `std::function` conversions on MSVC, at this
    // time.
    template <typename Signature>
    operator std::function<Signature>() const = delete;

private:
    // The `TagType` type cannot be constructed as a default function-parameter in Clang.  So we use
    // a static member function that initializes that default parameter.
    static TagType make_tag()
    {
        return {};
    }

    struct Impl {
        virtual ~Impl() noexcept = default;
        virtual RetType call(Args&&... args) = 0;
    };

    // These overload helpers are needed to squelch problems in the `T ()` -> `void ()` case.
    template <typename Functor>
    static void call_regular_void(const std::true_type is_void, Functor& f, Args&&... args)
    {
        // The result of this call is not cast to void, to help preserve detection of
        // `[[nodiscard]]` violations.
        static_cast<void>(is_void);
        f(std::forward<Args>(args)...);
    }

    template <typename Functor>
    static RetType call_regular_void(const std::false_type is_not_void, Functor& f, Args&&... args)
    {
        static_cast<void>(is_not_void);
        return f(std::forward<Args>(args)...);
    }

    template <typename Functor>
    static auto make_impl(Functor&& functor)
    {
        struct SpecificImpl : Impl {
            explicit SpecificImpl(Functor&& func)
                : f(std::forward<Functor>(func))
            {
            }

            RetType call(Args&&... args) override
            {
                return call_regular_void(std::is_void<RetType>(), f, std::forward<Args>(args)...);
            }

            std::decay_t<Functor> f;
        };

        return std::make_unique<SpecificImpl>(std::forward<Functor>(functor));
    }

    std::unique_ptr<Impl> impl;
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

/**
 * Deduction guides for UniqueFunction<Sig> that pluck the signature off of function pointers and
 * non-overloaded, non-generic function objects such as lambdas that don't use `auto` arguments.
 */
template <typename Ret, typename... Args>
UniqueFunction(Ret (*)(Args...)) -> UniqueFunction<Ret(Args...)>;
template <typename T, typename Sig = typename UFDeductionHelper<decltype(&T::operator())>::type>
UniqueFunction(T) -> UniqueFunction<Sig>;

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
