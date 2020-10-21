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

#ifndef REALM_TEST_UTIL_SUPER_INT_HPP
#define REALM_TEST_UTIL_SUPER_INT_HPP

#include <cstdint>
#include <limits>
#include <ostream>

#include <realm/util/features.h>
#include <realm/util/type_traits.hpp>
#include <realm/util/safe_int_ops.hpp>

namespace realm {
namespace test_util {


/// Signed integer that guarantees to be able to uniquely represent
/// the values of all fundamental signed and unsigned integer types.
class super_int {
private:
    typedef uintmax_t val_uint;
    typedef std::numeric_limits<val_uint> val_lim;

public:
    /// Number of value bits (excluding the sign bit).
    static const int digits = val_lim::digits;

    super_int() noexcept;
    template <class T>
    explicit super_int(T value) noexcept;

    template <class T>
    bool cast_has_overflow() const noexcept;

    template <class T>
    bool get_as(T&) const noexcept;

    //@{

    /// Arithmetic is done on the `N+1`-bit two's complement
    /// representation of each argument where `N` is the value of
    /// `digits`. The result is reduced modulo `2**(N+1)`.
    friend super_int operator+(super_int, super_int) noexcept;
    friend super_int operator-(super_int, super_int) noexcept;
    friend super_int operator*(super_int, super_int) noexcept;

    //@}

    friend bool operator==(super_int, super_int) noexcept;
    friend bool operator!=(super_int, super_int) noexcept;
    friend bool operator<(super_int, super_int) noexcept;
    friend bool operator<=(super_int, super_int) noexcept;
    friend bool operator>(super_int, super_int) noexcept;
    friend bool operator>=(super_int, super_int) noexcept;

    template <class C, class T>
    friend std::basic_ostream<C, T>& operator<<(std::basic_ostream<C, T>&, super_int);

    bool add_with_overflow_detect(super_int) noexcept;
    bool subtract_with_overflow_detect(super_int) noexcept;
    //    bool multiply_with_overflow_detect(super_int) noexcept;

private:
    // Value bits (not including the sign bit) of the two's complement
    // representation of the stored value.
    val_uint m_value;

    // True if the stored value is the result of `m_value - 2**N`,
    // where `N` is the number of value bits in `val_uint`.
    bool m_sign_bit;
};


// Implementation

inline super_int::super_int() noexcept
{
    m_value = 0;
    m_sign_bit = false;
}

template <class T>
inline super_int::super_int(T value) noexcept
{
    typedef std::numeric_limits<T> lim_t;
    // C++11 (through its inclusion of C99) guarantees that the
    // largest unsigned type has at least as many value bits as any
    // standard signed type (see C99+TC3 section 6.2.6.2 paragraph
    // 2). This means that the following conversion to two's
    // complement representation can throw away at most the sign bit,
    // which is fine, because we handle the sign bit separately.
    m_value = value;
    m_sign_bit = lim_t::is_signed && value < T(0);
}

template <class T>
inline bool super_int::cast_has_overflow() const noexcept
{
    typedef std::numeric_limits<T> lim_t;
    if (*this < super_int(lim_t::min()))
        return true;
    if (*this > super_int(lim_t::max()))
        return true;
    return false;
}

template <class T>
bool super_int::get_as(T& v) const noexcept
{
    // Ensure that the value represented by `*this` is also be
    // representable in T.
    if (cast_has_overflow<T>())
        return false;
    v = T(m_value);
    return true;
}

inline super_int operator+(super_int a, super_int b) noexcept
{
    super_int c;
    c.m_value = a.m_value + b.m_value;
    bool carry = c.m_value < a.m_value;
    c.m_sign_bit = (a.m_sign_bit != b.m_sign_bit) != carry;
    return c;
}

inline super_int operator-(super_int a, super_int b) noexcept
{
    super_int c;
    c.m_value = a.m_value - b.m_value;
    bool borrow = c.m_value > a.m_value;
    c.m_sign_bit = (a.m_sign_bit != b.m_sign_bit) != borrow;
    return c;
}

inline super_int operator*(super_int a, super_int b) noexcept
{
    typedef super_int::val_uint val_uint;
    int msb_pos = super_int::digits - 1;
    val_uint a_1 = a.m_value & 1;
    val_uint a_2 = (val_uint(a.m_sign_bit) << msb_pos) | (a.m_value >> 1);
    val_uint b_1 = b.m_value & 1;
    val_uint b_2 = (val_uint(b.m_sign_bit) << msb_pos) | (b.m_value >> 1);
    val_uint v = ((a_2 * b_2) << 1) + a_2 * b_1 + a_1 * b_2;
    super_int c;
    c.m_value = (v << 1) | (a_1 * b_1);
    c.m_sign_bit = v >> msb_pos != 0;
    return c;
}

inline bool operator==(super_int a, super_int b) noexcept
{
    return a.m_value == b.m_value && a.m_sign_bit == b.m_sign_bit;
}

inline bool operator!=(super_int a, super_int b) noexcept
{
    return !(a == b);
}

inline bool operator<(super_int a, super_int b) noexcept
{
    if (a.m_sign_bit > b.m_sign_bit)
        return true;
    if (a.m_sign_bit == b.m_sign_bit) {
        if (a.m_value < b.m_value)
            return true;
    }
    return false;
}

inline bool operator<=(super_int a, super_int b) noexcept
{
    return !(b < a);
}

inline bool operator>(super_int a, super_int b) noexcept
{
    return b < a;
}

inline bool operator>=(super_int a, super_int b) noexcept
{
    return !(a < b);
}

template <class C, class T>
std::basic_ostream<C, T>& operator<<(std::basic_ostream<C, T>& out, super_int i)
{
    typedef super_int::val_uint val_uint;
    if (i.m_sign_bit) {
        val_uint max = val_uint(-1);
        int last_digit_1 = int(max % 10);
        val_uint other_digits_1 = max / 10;
        // Add one to max value
        ++last_digit_1;
        if (last_digit_1 == 10) {
            last_digit_1 = 0;
            ++other_digits_1;
        }
        int last_digit_2 = int(i.m_value % 10);
        val_uint other_digits_2 = i.m_value / 10;
        // Subtract m_value from max+1
        last_digit_1 -= last_digit_2;
        other_digits_1 -= other_digits_2;
        if (last_digit_1 < 0) {
            last_digit_1 += 10;
            --other_digits_1;
        }
        out << '-';
        if (other_digits_1 > 0)
            out << other_digits_1;
        out << last_digit_1;
    }
    else {
        out << i.m_value;
    }
    return out;
}

inline bool super_int::add_with_overflow_detect(super_int v) noexcept
{
    super_int v_2 = *this + v;
    bool carry = v_2.m_value < m_value;
    bool overflow = m_sign_bit == v.m_sign_bit && m_sign_bit != carry;
    if (overflow)
        return true;
    *this = v_2;
    return false;
}

inline bool super_int::subtract_with_overflow_detect(super_int v) noexcept
{
    super_int v_2 = *this - v;
    bool borrow = v_2.m_value > m_value;
    bool overflow = m_sign_bit != v.m_sign_bit && m_sign_bit == borrow;
    if (overflow)
        return true;
    *this = v_2;
    return false;
}

} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_SUPER_INT_HPP
