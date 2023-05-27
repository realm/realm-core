/*************************************************************************
 *
 * Copyright 2023 Realm Inc.
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

#ifndef REALM_EXPECTED_HPP
#define REALM_EXPECTED_HPP

#include <realm/status.hpp>
#include <realm/utilities.hpp>

namespace realm {
template <class T>
class Expected;

namespace _impl {

template <class T>
struct IsExpectedImpl : std::false_type {};
template <class T>
struct IsExpectedImpl<Expected<T>> : std::true_type {};
template <class T>
constexpr bool IsExpected = IsExpectedImpl<std::decay_t<T>>::value;

template <class T, class U>
using ExpectedEnableForwardValue =
    std::enable_if_t<std::conjunction_v<std::negation<realm::is_any<std::decay_t<U>, std::in_place_t, Expected<T>>>,
                                        std::is_constructible<T, U&&>>>;

template <class T, class U, class UR>
constexpr bool ExpectedEnableFromOther =
    std::is_constructible_v<T, UR> &&
    std::negation_v<
        std::disjunction<std::is_constructible<T, Expected<U>&&>, std::is_constructible<T, const Expected<U>&>,
                         std::is_constructible<T, const Expected<U>&&>, std::is_convertible<Expected<U>&, T>,
                         std::is_convertible<Expected<U>&&, T>, std::is_convertible<const Expected<U>&, T>,
                         std::is_convertible<const Expected<U>&&, T>>>;

struct NoInitTag {};

template <class T>
struct ExpectedStorageBase {
    constexpr ExpectedStorageBase(NoInitTag) {}
    constexpr ExpectedStorageBase()
        : m_value(T{})
        , m_has_value(true)
    {
    }

    template <class... Args, std::enable_if_t<std::is_constructible_v<T, Args&&...>>* = nullptr>
    constexpr ExpectedStorageBase(std::in_place_t, Args&&... args)
        : m_value(std::forward<Args>(args)...)
        , m_has_value(true)
    {
    }

    template <class U, class... Args,
              std::enable_if_t<std::is_constructible_v<T, std::initializer_list<U>&, Args&&...>>* = nullptr>
    constexpr ExpectedStorageBase(std::in_place_t, std::initializer_list<U> il, Args&&... args)
        : m_value(il, std::forward<Args>(args)...)
        , m_has_value(true)
    {
    }

    ExpectedStorageBase(Status status)
        : m_error(std::move(status))
        , m_has_value(false)
    {
        REALM_ASSERT(!m_error.is_ok());
    }

    ~ExpectedStorageBase()
    {
        if (m_has_value) {
            m_value.~T();
        }
        else {
            m_error.~Status();
        }
    }

    template <class... Args>
    void construct(Args&&... args) noexcept
    {
        new (std::addressof(m_value)) T(std::forward<Args>(args)...);
        this->m_has_value = true;
    }

    template <class E>
    void construct_error(E&& arg) noexcept
    {
        new (&m_error) Status(std::forward<E>(arg));
        this->m_has_value = false;
    }

    template <class Rhs>
    void construct_with(Rhs&& rhs) noexcept
    {
        if (rhs.has_value()) {
            construct(std::forward<Rhs>(rhs).get());
        }
        else {
            construct_error(std::forward<Rhs>(rhs).geterr());
        }
    }

    template <typename U>
    void assign(U&& rhs)
    {
        if (!rhs.m_has_value) {
            if (m_has_value) {
                m_value.~T();
                construct_error(std::forward<U>(rhs).m_error);
            }
            else {
                m_error = std::forward<U>(rhs).m_error;
            }
            return;
        }

        if (m_has_value) {
            m_value = std::forward<U>(rhs).m_value;
            return;
        }

        if (noexcept(T(std::forward<U>(rhs).m_value))) {
            m_error.~Status();
            construct(std::forward<U>(rhs).m_value);
        }
        else if (std::is_nothrow_move_constructible_v<T>) {
            T tmp(std::forward<U>(rhs).m_value);
            m_error.~Status();
            construct(std::move(tmp));
        }
        else {
            auto tmp = std::move(m_error);
            m_error.~Status();

            try {
                construct(std::forward<U>(rhs).m_value);
            }
            catch (...) {
                m_error = std::move(tmp);
                throw;
            }
        }
    }

    template <typename E>
    void assign_error(E&& e)
    {
        if (m_has_value) {
            m_value.~T();
            m_has_value = false;
            construct_error(std::forward<E>(e));
        }
        else {
            m_error = std::forward<E>(e);
        }
    }

    template <class... Args, std::enable_if_t<std::is_nothrow_constructible<T, Args&&...>::value>* = nullptr>
    void emplace(Args&&... args)
    {
        if (has_value()) {
            m_value.~T();
        }
        else {
            m_error.~Status();
        }
        construct(std::forward<Args>(args)...);
    }

    template <
        class U, class... Args,
        std::enable_if_t<std::is_nothrow_constructible<T, std::initializer_list<U>&, Args&&...>::value>* = nullptr>
    void emplace(std::initializer_list<U> il, Args&&... args)
    {
        if (has_value()) {
            m_value.~T();
        }
        else {
            m_error.~Status();
        }
        construct(il, std::forward<Args>(args)...);
    }

    bool has_value() const noexcept
    {
        return m_has_value;
    }

    constexpr T& get() &
    {
        return m_value;
    }
    constexpr const T& get() const&
    {
        return m_value;
    }
    constexpr T&& get() &&
    {
        return std::move(m_value);
    }
    constexpr const T&& get() const&&
    {
        return std::move(m_value);
    }

    constexpr Status& geterr() & noexcept
    {
        return m_error;
    }
    constexpr const Status& geterr() const& noexcept
    {
        return m_error;
    }
    constexpr Status&& geterr() && noexcept
    {
        return std::move(m_error);
    }
    constexpr const Status&& geterr() const&& noexcept
    {
        return std::move(m_error);
    }

    ExpectedStorageBase(ExpectedStorageBase&& rhs) noexcept(std::is_nothrow_move_constructible_v<T>)
    {
        this->construct_with(std::move(rhs));
    }

    ExpectedStorageBase(ExpectedStorageBase const& rhs) noexcept(std::is_nothrow_copy_constructible_v<T>)
    {
        this->construct_with(rhs);
    }

    ExpectedStorageBase& operator=(ExpectedStorageBase&& rhs) noexcept(std::is_nothrow_move_assignable_v<T>)
    {
        assign(std::move(rhs));
        return *this;
    }

    ExpectedStorageBase& operator=(ExpectedStorageBase const& rhs) noexcept(std::is_nothrow_copy_assignable_v<T>)
    {
        if (this != &rhs) {
            assign(rhs);
        }
        return *this;
    }

    union {
        T m_value;
        Status m_error;
        char m_no_init;
    };
    bool m_has_value;
};

template <bool CanCopy, bool CanMove>
struct ConstructorBase {};
template <>
struct ConstructorBase<false, false> {
    ConstructorBase() = default;
    ConstructorBase(ConstructorBase const&) = delete;
    ConstructorBase(ConstructorBase&&) = delete;
    ConstructorBase& operator=(ConstructorBase const&) = default;
    ConstructorBase& operator=(ConstructorBase&&) = default;
};
template <>
struct ConstructorBase<true, false> {
    ConstructorBase() = default;
    ConstructorBase(ConstructorBase const&) = default;
    ConstructorBase(ConstructorBase&&) = delete;
    ConstructorBase& operator=(ConstructorBase const&) = default;
    ConstructorBase& operator=(ConstructorBase&&) = default;
};
template <>
struct ConstructorBase<false, true> {
    ConstructorBase() = default;
    ConstructorBase(ConstructorBase const&) = delete;
    ConstructorBase(ConstructorBase&&) = default;
    ConstructorBase& operator=(ConstructorBase const&) = default;
    ConstructorBase& operator=(ConstructorBase&&) = default;
};

template <bool CanCopy, bool CanMove>
struct AssignBase {};
template <>
struct AssignBase<false, false> {
    AssignBase() = default;
    AssignBase(AssignBase const&) = default;
    AssignBase(AssignBase&&) = default;
    AssignBase& operator=(AssignBase const&) = delete;
    AssignBase& operator=(AssignBase&&) = delete;
};
template <>
struct AssignBase<true, false> {
    AssignBase() = default;
    AssignBase(AssignBase const&) = default;
    AssignBase(AssignBase&&) = default;
    AssignBase& operator=(AssignBase const&) = default;
    AssignBase& operator=(AssignBase&&) = delete;
};
template <>
struct AssignBase<false, true> {
    AssignBase() = default;
    AssignBase(AssignBase const&) = default;
    AssignBase(AssignBase&&) = default;
    AssignBase& operator=(AssignBase const&) = delete;
    AssignBase& operator=(AssignBase&&) = default;
};


template <typename T>
struct ExpectedStorage : ExpectedStorageBase<T>,
                         ConstructorBase<std::is_copy_constructible_v<T>, std::is_move_constructible_v<T>>,
                         AssignBase<std::is_copy_constructible_v<T>, std::is_move_constructible_v<T>> {
    using ExpectedStorageBase<T>::ExpectedStorageBase;
};

// void version skips storing
template <>
struct ExpectedStorage<void> {
    ExpectedStorage() noexcept
        : m_error(Status::OK())
    {
    }
    ExpectedStorage(std::in_place_t) noexcept
        : m_error(Status::OK())
    {
    }
    ExpectedStorage(Status&& status) noexcept
        : m_error(std::move(status))
    {
    }
    ExpectedStorage(const Status& status) noexcept
        : m_error(status)
    {
    }
    ExpectedStorage(const ExpectedStorage& e) noexcept
        : m_error(e.m_error)
    {
    }
    ExpectedStorage(ExpectedStorage&& e) noexcept
        : m_error(std::move(e.m_error))
    {
    }
    ExpectedStorage& operator=(const ExpectedStorage& e) noexcept
    {
        m_error = e.m_error;
        return *this;
    }
    ExpectedStorage& operator=(ExpectedStorage&& e) noexcept
    {
        m_error = std::move(e.m_error);
        return *this;
    }

    bool has_value() const noexcept
    {
        return this->m_error.is_ok();
    }

    Status& geterr() & noexcept
    {
        return this->m_error;
    }
    const Status& geterr() const& noexcept
    {
        return this->m_error;
    }
    Status&& geterr() && noexcept
    {
        return std::move(this->m_error);
    }
    void get() const noexcept {}
    void emplace() noexcept
    {
        m_error = Status::OK();
    }

    template <typename E>
    void assign_error(E&& e)
    {
        m_error = std::forward<E>(e);
    }

    Status m_error;
};
} // namespace _impl

template <class T>
class Expected : private _impl::ExpectedStorage<T> {
    static_assert(!std::is_reference_v<T>, "T must not be a reference");
    static_assert(!std::is_same_v<T, std::remove_cv_t<std::in_place_t>>, "T must not be std::in_place_t");
    static_assert(!std::is_same_v<T, std::remove_cv_t<Status>>, "T must not be Status");
    template <typename U>
    friend struct _impl::ExpectedStorageBase;

    using Base = _impl::ExpectedStorage<T>;
    using Base::get;
    using Base::geterr;

public:
    using value_type = T;
    using error_type = Status;

    using Base::Base;
    constexpr Expected() = default;
    constexpr Expected(const Expected& rhs) = default;
    constexpr Expected(Expected&& rhs) = default;
    Expected& operator=(const Expected& rhs) = default;
    Expected& operator=(Expected&& rhs) = default;

    template <typename Reason, std::enable_if_t<std::is_constructible_v<std::string_view, Reason>>* = nullptr>
    constexpr Expected(ErrorCodes::Error code, Reason reason)
        : Expected(Status(code, reason))
    {
    }

    template <class U, std::enable_if_t<!std::is_convertible_v<U const&, T>>* = nullptr>
    explicit constexpr Expected(const Expected<U>& rhs)
        : Base(_impl::NoInitTag{})
    {
        if constexpr (_impl::ExpectedEnableFromOther<T, U, const U&>) {
            this->construct_with(rhs);
        }
        else {
            this->construct(rhs);
        }
    }

    template <class U, std::enable_if_t<(std::is_convertible_v<U const&, T>)>* = nullptr>
    constexpr Expected(const Expected<U>& rhs)
        : Base(_impl::NoInitTag{})
    {
        if constexpr (_impl::ExpectedEnableFromOther<T, U, const U&>) {
            this->construct_with(rhs);
        }
        else {
            this->construct(rhs);
        }
    }

    template <class U, std::enable_if_t<!(std::is_convertible_v<U&&, T>)>* = nullptr>
    explicit constexpr Expected(Expected<U>&& rhs)
        : Base(_impl::NoInitTag{})
    {
        if constexpr (_impl::ExpectedEnableFromOther<T, U, U&&>) {
            this->construct_with(std::move(rhs));
        }
        else {
            this->construct(std::move(rhs));
        }
    }

    template <class U, std::enable_if_t<(std::is_convertible_v<U&&, T>)>* = nullptr>
    constexpr Expected(Expected<U>&& rhs)
        : Base(_impl::NoInitTag{})
    {
        if constexpr (_impl::ExpectedEnableFromOther<T, U, U&&>) {
            this->construct_with(std::move(rhs));
        }
        else {
            this->construct(std::move(rhs));
        }
    }

    template <class U = T, std::enable_if_t<!std::is_convertible_v<U&&, T>>* = nullptr,
              _impl::ExpectedEnableForwardValue<T, U>* = nullptr>
    explicit constexpr Expected(U&& v)
        : Base(std::in_place, std::forward<U>(v))
    {
    }

    template <class U = T, std::enable_if_t<std::is_convertible_v<U&&, T>>* = nullptr,
              _impl::ExpectedEnableForwardValue<T, U>* = nullptr>
    constexpr Expected(U&& v)
        : Base(_impl::NoInitTag{})
    {
        this->construct(std::forward<U>(v));
    }

    template <class U = T, class G = T, std::enable_if_t<std::is_nothrow_constructible_v<T, U&&>>* = nullptr,
              std::enable_if_t<!std::is_void_v<G>>* = nullptr,
              std::enable_if_t<(!std::is_same<Expected<T>, std::decay_t<U>>::value &&
                                !std::conjunction<std::is_scalar<T>, std::is_same<T, std::decay_t<U>>>::value &&
                                std::is_constructible_v<T, U> && std::is_assignable_v<G&, U>)>* = nullptr>
    Expected& operator=(U&& v)
    {
        this->assign(std::forward<U>(v));
        return *this;
    }

    template <class U = T, class G = T, std::enable_if_t<!std::is_nothrow_constructible_v<T, U&&>>* = nullptr,
              std::enable_if_t<!std::is_void_v<U>>* = nullptr,
              std::enable_if_t<(!std::is_same<Expected<T>, std::decay_t<U>>::value &&
                                !std::conjunction<std::is_scalar<T>, std::is_same<T, std::decay_t<U>>>::value &&
                                std::is_constructible_v<T, U> && std::is_assignable_v<G&, U>)>* = nullptr>
    Expected& operator=(U&& v)
    {
        this->assign(std::forward<U>(v));
        return *this;
    }

    Expected& operator=(const Status& rhs)
    {
        this->assign_error(rhs);
        return *this;
    }

    Expected& operator=(Status&& rhs) noexcept
    {
        this->assign_error(std::move(rhs));
        return *this;
    }

    using Base::emplace;

    template <class U = T, std::enable_if_t<!std::is_void_v<U>>* = nullptr>
    constexpr const U* operator->() const
    {
        REALM_ASSERT(has_value());
        return std::addressof(get());
    }

    template <class U = T, std::enable_if_t<!std::is_void_v<U>>* = nullptr>
    constexpr U* operator->()
    {
        REALM_ASSERT(has_value());
        return std::addressof(get());
    }

    template <class U = T, std::enable_if_t<!std::is_void_v<U>>* = nullptr>
    constexpr const U& operator*() const&
    {
        REALM_ASSERT(has_value());
        return get();
    }
    template <class U = T, std::enable_if_t<!std::is_void_v<U>>* = nullptr>
    constexpr U& operator*() &
    {
        REALM_ASSERT(has_value());
        return get();
    }
    template <class U = T, std::enable_if_t<!std::is_void_v<U>>* = nullptr>
    constexpr const U&& operator*() const&&
    {
        REALM_ASSERT(has_value());
        return std::move(get());
    }
    template <class U = T, std::enable_if_t<!std::is_void_v<U>>* = nullptr>
    constexpr U&& operator*() &&
    {
        REALM_ASSERT(has_value());
        return std::move(get());
    }

    constexpr bool has_value() const noexcept
    {
        return Base::has_value();
    }
    constexpr explicit operator bool() const noexcept
    {
        return Base::has_value();
    }

    template <class U = T, std::enable_if_t<!std::is_void_v<U>>* = nullptr>
    constexpr const U& value() const&
    {
        if (!has_value())
            throw Exception(error());
        return get();
    }
    template <class U = T, std::enable_if_t<!std::is_void_v<U>>* = nullptr>
    constexpr U& value() &
    {
        if (!has_value())
            throw Exception(error());
        return get();
    }
    template <class U = T, std::enable_if_t<!std::is_void_v<U>>* = nullptr>
    constexpr const U&& value() const&&
    {
        if (!has_value())
            throw Exception(error());
        return std::move(get());
    }
    template <class U = T, std::enable_if_t<!std::is_void_v<U>>* = nullptr>
    constexpr U&& value() &&
    {
        if (!has_value())
            throw Exception(error());
        return std::move(get());
    }
    template <class U = T, std::enable_if_t<std::is_void_v<U>>* = nullptr>
    constexpr void value() const
    {
        if (!has_value())
            throw Exception(error());
    }

    constexpr const Status& error() const& noexcept
    {
        REALM_ASSERT(!has_value());
        return geterr();
    }
    constexpr Status&& error() && noexcept
    {
        REALM_ASSERT(!has_value());
        return std::move(geterr());
    }

    template <class U>
    constexpr T value_or(U&& v) const&
    {
        static_assert(std::is_copy_constructible_v<T> && std::is_convertible_v<U&&, T>,
                      "T must be copy-constructible and convertible to from U&&");
        return *this ? **this : static_cast<T>(std::forward<U>(v));
    }
    template <class U>
    constexpr T value_or(U&& v) &&
    {
        static_assert(std::is_move_constructible_v<T> && std::is_convertible_v<U&&, T>,
                      "T must be move-constructible and convertible to from U&&");
        return *this ? std::move(**this) : static_cast<T>(std::forward<U>(v));
    }

    template <class F>
    constexpr auto and_then(F&& f) &
    {
        return and_then_impl(*this, std::forward<F>(f));
    }
    template <class F>
    constexpr auto and_then(F&& f) &&
    {
        return and_then_impl(std::move(*this), std::forward<F>(f));
    }
    template <class F>
    constexpr auto and_then(F&& f) const&
    {
        return and_then_impl(*this, std::forward<F>(f));
    }
    template <class F>
    constexpr auto and_then(F&& f) const&&
    {
        return and_then_impl(std::move(*this), std::forward<F>(f));
    }

    template <class F>
    constexpr auto map(F&& f) &
    {
        return expected_map_impl(*this, std::forward<F>(f));
    }
    template <class F>
    constexpr auto map(F&& f) &&
    {
        return expected_map_impl(std::move(*this), std::forward<F>(f));
    }
    template <class F>
    constexpr auto map(F&& f) const&
    {
        return expected_map_impl(*this, std::forward<F>(f));
    }
    template <class F>
    constexpr auto map(F&& f) const&&
    {
        return expected_map_impl(std::move(*this), std::forward<F>(f));
    }

    template <class F>
    constexpr auto transform(F&& f) &
    {
        return expected_map_impl(*this, std::forward<F>(f));
    }
    template <class F>
    constexpr auto transform(F&& f) &&
    {
        return expected_map_impl(std::move(*this), std::forward<F>(f));
    }
    template <class F>
    constexpr auto transform(F&& f) const&
    {
        return expected_map_impl(*this, std::forward<F>(f));
    }
    template <class F>
    constexpr auto transform(F&& f) const&&
    {
        return expected_map_impl(std::move(*this), std::forward<F>(f));
    }

    template <class F>
    constexpr auto map_error(F&& f) &
    {
        return map_error_impl(*this, std::forward<F>(f));
    }
    template <class F>
    constexpr auto map_error(F&& f) &&
    {
        return map_error_impl(std::move(*this), std::forward<F>(f));
    }
    template <class F>
    constexpr auto map_error(F&& f) const&
    {
        return map_error_impl(*this, std::forward<F>(f));
    }
    template <class F>
    constexpr auto map_error(F&& f) const&&
    {
        return map_error_impl(std::move(*this), std::forward<F>(f));
    }

    template <class F>
    constexpr auto transform_error(F&& f) &
    {
        return map_error_impl(*this, std::forward<F>(f));
    }
    template <class F>
    constexpr auto transform_error(F&& f) &&
    {
        return map_error_impl(std::move(*this), std::forward<F>(f));
    }
    template <class F>
    constexpr auto transform_error(F&& f) const&
    {
        return map_error_impl(*this, std::forward<F>(f));
    }
    template <class F>
    constexpr auto transform_error(F&& f) const&&
    {
        return map_error_impl(std::move(*this), std::forward<F>(f));
    }

    template <class F>
    Expected constexpr or_else(F&& f) &
    {
        return or_else_impl(*this, std::forward<F>(f));
    }
    template <class F>
    Expected constexpr or_else(F&& f) &&
    {
        return or_else_impl(std::move(*this), std::forward<F>(f));
    }
    template <class F>
    Expected constexpr or_else(F&& f) const&
    {
        return or_else_impl(*this, std::forward<F>(f));
    }
    template <class F>
    Expected constexpr or_else(F&& f) const&&
    {
        return or_else_impl(std::move(*this), std::forward<F>(f));
    }
};

namespace _impl {
template <class Exp>
using ExpectedType = typename std::decay_t<Exp>::value_type;

template <typename T, typename Exp, typename F>
struct InvokeResultTypeImpl {
    using type = std::invoke_result_t<F, decltype(*std::declval<Exp>())>;
};
template <typename Exp, typename F>
struct InvokeResultTypeImpl<void, Exp, F> {
    using type = std::invoke_result_t<F>;
};

template <typename Exp, typename F>
using InvokeResultType = typename InvokeResultTypeImpl<ExpectedType<Exp>, Exp, F>::type;

template <class Exp, class F>
constexpr auto and_then_impl(Exp&& exp, F&& f)
{
    using Ret = InvokeResultType<Exp, F>;
    static_assert(_impl::IsExpected<Ret>, "and_then() must return an Expected");
    if (!exp.has_value())
        return Ret(std::forward<Exp>(exp).error());
    if constexpr (std::is_void_v<ExpectedType<Exp>>) {
        return std::invoke(std::forward<F>(f));
    }
    else {
        return std::invoke(std::forward<F>(f), *std::forward<Exp>(exp));
    }
}

template <class Exp, class F>
constexpr auto expected_map_impl(Exp&& exp, F&& f)
{
    using Ret = InvokeResultType<Exp, F>;
    using Result = Expected<Ret>;
    if (!exp.has_value())
        return Result(std::forward<Exp>(exp).error());

    constexpr bool source_is_void = std::is_void_v<ExpectedType<Exp>>;
    constexpr bool ret_is_void = std::is_void_v<Ret>;
    if constexpr (source_is_void && ret_is_void) {
        std::invoke(std::forward<F>(f));
        return Result();
    }
    else if constexpr (!source_is_void && ret_is_void) {
        std::invoke(std::forward<F>(f), *std::forward<Exp>(exp));
        return Result();
    }
    else if constexpr (source_is_void && !ret_is_void) {
        return Result(std::invoke(std::forward<F>(f)));
    }
    else { // !source_is_void && !ret_is_void
        return Result(std::invoke(std::forward<F>(f), *std::forward<Exp>(exp)));
    }
}

template <class Exp, class F>
constexpr auto map_error_impl(Exp&& exp, F&& f)
{
    static_assert(std::is_invocable_r_v<Status, F, Status>, "map_error() must return a Status");
    using T = ExpectedType<Exp>;
    using Result = Expected<T>;
    if (exp.has_value()) {
        if constexpr (std::is_void_v<T>) {
            return Result();
        }
        else {
            return Result(*std::forward<Exp>(exp));
        }
    }
    return Result(std::invoke(std::forward<F>(f), std::forward<Exp>(exp).error()));
}

template <class Exp, class F>
constexpr auto or_else_impl(Exp&& exp, F&& f)
{
    return exp.has_value() ? std::forward<Exp>(exp) : std::invoke(std::forward<F>(f), std::forward<Exp>(exp).error());
}
} // namespace _impl

template <class T, class U>
constexpr bool operator==(const Expected<T>& lhs, const Expected<U>& rhs)
{
    return (lhs.has_value() != rhs.has_value()) ? false
                                                : (!lhs.has_value() ? lhs.error() == rhs.error() : *lhs == *rhs);
}
template <class T, class U>
constexpr bool operator!=(const Expected<T>& lhs, const Expected<U>& rhs)
{
    return (lhs.has_value() != rhs.has_value()) ? true
                                                : (!lhs.has_value() ? lhs.error() != rhs.error() : *lhs != *rhs);
}
constexpr bool operator==(const Expected<void>& lhs, const Expected<void>& rhs)
{
    return lhs.has_value() == rhs.has_value() && (!lhs.has_value() || lhs.error() == rhs.error());
}
constexpr bool operator!=(const Expected<void>& lhs, const Expected<void>& rhs)
{
    return lhs.has_value() != rhs.has_value() || (!lhs.has_value() && lhs.error() == rhs.error());
}

template <class T, class U>
bool operator==(const Expected<T>& x, const U& v)
{
    return x.has_value() && *x == v;
}
template <class T, class U>
bool operator==(const U& v, const Expected<T>& x)
{
    return x.has_value() && *x == v;
}
template <class T, class U>
bool operator!=(const Expected<T>& x, const U& v)
{
    return x.has_value() || *x != v;
}
template <class T, class U>
bool operator!=(const U& v, const Expected<T>& x)
{
    return x.has_value() || *x != v;
}

template <class T>
bool operator==(const Expected<T>& x, const Status& e)
{
    return !x.has_value() && x.error() == e;
}
template <class T>
bool operator==(const Status& e, const Expected<T>& x)
{
    return !x.has_value() && x.error() == e;
}
template <class T>
bool operator!=(const Expected<T>& x, const Status& e)
{
    return x.has_value() || x.error() != e;
}
template <class T>
bool operator!=(const Status& e, const Expected<T>& x)
{
    return x.has_value() || x.error() != e;
}

inline bool operator==(const Expected<void>& x, const Status& e)
{
    return x.has_value() ? e.is_ok() : x.error() == e;
}
inline bool operator==(const Status& e, const Expected<void>& x)
{
    return x.has_value() ? e.is_ok() : x.error() == e;
}
inline bool operator!=(const Expected<void>& x, const Status& e)
{
    return x.has_value() ? !e.is_ok() : x.error() != e;
}
inline bool operator!=(const Status& e, const Expected<void>& x)
{
    return x.has_value() ? !e.is_ok() : x.error() != e;
}
} // namespace realm

#endif // REALM_EXPECTED_HPP
