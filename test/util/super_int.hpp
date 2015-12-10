/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/
#ifndef REALM_TEST_UTIL_SUPER_INT_HPP
#define REALM_TEST_UTIL_SUPER_INT_HPP

#include <stdint.h>
#include <limits>
#include <ostream>

#include <realm/util/features.h>
#include <realm/util/type_traits.hpp>

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
    template<class T>
    explicit super_int(T value) noexcept;

    template<class T>
    bool cast_has_overflow() const noexcept;

    template<class T>
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

    template<class C, class T>
    friend std::basic_ostream<C,T>& operator<<(std::basic_ostream<C,T>&, super_int);

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
    m_value    = 0;
    m_sign_bit = false;
}

template<class T>
inline super_int::super_int(T value) noexcept
{
    typedef std::numeric_limits<T> lim_t;
    // C++11 (through its inclusion of C99) guarantees that the
    // largest unsigned type has at least as many value bits as any
    // standard signed type (see C99+TC3 section 6.2.6.2 paragraph
    // 2). This means that the following conversion to two's
    // complement representation can throw away at most the sign bit,
    // which is fine, because we handle the sign bit separately.
    m_value    = value;
    m_sign_bit = lim_t::is_signed && util::is_negative(value);
}

template<class T>
inline bool super_int::cast_has_overflow() const noexcept
{
    typedef std::numeric_limits<T> lim_t;
    if (*this < super_int(lim_t::min()))
        return true;
    if (*this > super_int(lim_t::max()))
        return true;
    return false;
}

template<class T>
bool super_int::get_as(T& v) const noexcept
{
    // Ensure that the value represented by `*this` is also be
    // representable in T.
    if (cast_has_overflow<T>())
        return false;
    typedef std::numeric_limits<T> lim_t;
    // The conversion from two's complement to the native
    // representation of negative values below requires no assumptions
    // beyond what is guaranteed by C++11. The unsigned result of
    // `~m_value` is guaranteed to be representable as a non-negative
    // value in `T`, which is a signed type in this case, and
    // therefore it is also guaranteed to be representable in
    // `promoted`. To see that `~m_value` is representable in `T`,
    // first note that `*this >= super_int(lim_t::min())` implies that
    // `m_value >= val_uint(lim_t::min())`, which in turn implies that
    // `~m_value <= lim_u::max() - val_uint(lim_t::min())` where
    // `lim_u` is a shorthand for
    // `std::numeric_limits<value_uint>`. From C99+TC3 section 6.2.6.2
    // paragraph 2, we know that `lim_u::max() - val_uint(min<T>) <=
    // lim_t::max()`, which implies that `~m_value <= lim_t::max()`
    // when combined with the previous result.
    if (lim_t::is_signed && m_sign_bit) {
        // Negative
        typedef typename util::ChooseWidestInt<int, T>::type promoted;
        v = util::cast_to_unsigned<T>(-1 - promoted(~m_value));
    }
    else {
        // Non-negative
        v = util::cast_to_unsigned<T>(m_value);
    }
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
    val_uint v = ((a_2 * b_2) << 1)  +  a_2 * b_1  +  a_1 * b_2;
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

template<class C, class T>
std::basic_ostream<C,T>& operator<<(std::basic_ostream<C,T>& out, super_int i)
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
        last_digit_1   -= last_digit_2;
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

/*
inline bool super_int::multiply_with_overflow_detect(super_int v) REALM_NOEXCEP
{
    // result.m_value == m_value * v.m_value (modulo 2**N where N is number of value bits in uintmax_t)

    // How can the most significant bit in multiplication modulo 2**(N+1) be calculated?

    // 4+1-bit multiplication:
    //
    //       A4     A3     A2     A1     A0
    // X     B4     B3     B2     B1     B0
    // -------------------------------------
    //    A4*B0  A3*B0  A2*B0  A1*B0  A0*B0
    //    A3*B1  A2*B1  A1*B1  A0*B1
    //    A2*B2  A1*B2  A0*B2
    //    A1*B3  A0*B3
    // +  A0*B4
    // -------------------------------------
    //
    // Same thing after second operand has been shifted one bit
    // position to the right:
    //
    //       A3     A2     A1     A0
    // X     B4     B3     B2     B1
    // ------------------------------
    //    A3*B1  A2*B1  A1*B1  A0*B1
    //    A2*B2  A1*B2  A0*B2
    //    A1*B3  A0*B3
    // +  A0*B4
    // ------------------------------
    //
    // Two's complement 4+1-bit multiplication with sign extension to
    // 8+1 bit:
    //
    //       a4     a4     a4     a4     A4     A3     A2     A1     A0
    // X     b4     b4     b4     b4     B4     B3     B2     B1     B0
    // -----------------------------------------------------------------
    //    a4*B0  a4*B0  a4*B0  a4*B0  A4*B0  A3*B0  A2*B0  A1*B0  A0*B0
    //    a4*B1  a4*B1  a4*B1  A4*B1  A3*B1  A2*B1  A1*B1  A0*B1
    //    a4*B2  a4*B2  A4*B2  A3*B2  A2*B2  A1*B2  A0*B2
    //    a4*B3  A4*B3  A3*B3  A2*B3  A1*B3  A0*B3
    //    A4*B4  A3*B4  A2*B4  A1*B4  A0*B4
    //    A3*b4  A2*b4  A1*b4  A0*b4
    //    A2*b4  A1*b4  A0*b4
    //    A1*b4  A0*b4
    // +  A0*b4
    // -----------------------------------------------------------------

    // Without overflow detection:
    int msb_pos = std::numeric_limits<uintmax_t>::digits - 1;
    uintmax_t = v_2 = (uintmax_t(v.m_sign_bit) << msb_pos) | (v.m_value >> 1);
    m_sign_bit = (((m_value * v_2) >> msb_pos) + (m_sign_bit & v.m_value)) & 1 != 0;
    m_value *= v.m_value;

    // How to detect overflow:
    // uintmax_t = a = (uintmax_t(m_sign_bit)   << msb_pos) | (m_value >> 1);
    // uintmax_t = b = (uintmax_t(v.m_sign_bit) << msb_pos) | (v.m_value >> 1);

    uintmax_t a_0_0 = m_value;
    uintmax_t a_0_1 = m_sign_bit * std::numeric_limits<uintmax_t>::max();
    uintmax_t b_0_0 = v.m_value;
    uintmax_t b_0_1 = v.m_sign_bit * std::numeric_limits<uintmax_t>::max();

    // Recombine the 2 chunks into 4 or 5 smaller chunks
    int size_0 = digits;
    int size_1 = size_0 / 2;
    int size_2 = 2 * size_1;
    int size_3 = size_0 - size_2;
    uintmax_t mask_0 = (uintmax_t(1) << size_1) - 1;
    uintmax_t mask_1 = mask_0 << size_1;
    uintmax_t a_1_0 = (a_0_0 & mask_0);
    uintmax_t a_1_1 = (a_0_0 & mask_1) >> size_1;
    uintmax_t a_1_2 = size_3 == 0 ? a_0_1 : (a_0_1 << size_3) | (a_0_0 >> size_2);
    uintmax_t b_1_0 = (b_0_0 & mask_0);
    uintmax_t b_1_1 = (b_0_0 & mask_1) >> size_1;
    uintmax_t b_1_2 = size_3 == 0 ? b_0_1 : (b_0_1 << size_3) | (b_0_0 >> size_2);

    // Make partial products
    uintmax_t c_0_0 = a_1_0 * b_1_0;
    uintmax_t c_0_1 = a_1_0 * b_1_1;
    uintmax_t c_0_2 = a_1_0 * b_1_2;
    uintmax_t c_1_0 = a_1_1 * b_1_0;
    uintmax_t c_1_1 = a_1_1 * b_1_1;
    uintmax_t c_1_2 = a_1_1 * b_1_2;
    uintmax_t c_2_0 = a_1_2 * b_1_0;
    uintmax_t c_2_1 = a_1_2 * b_1_1;
    uintmax_t c_2_2 = a_1_2 * b_1_2;

    // 0 0001 -> 1 1111
    // 1 1111 -> 0 0001
    // 0 0000 -> 0 0000
    // 1 0000 -> 1 0000
    // 1 0001 -> 0 1111

    val_uint a_0, b_0;
    if (m_sign_bit) {
        if (m_value == 0) {
            if (v.m_sign_bit || v.m_value > 1)
                return true;
            if (v.m_value == 0)
                *this = v;
            return false;
        }
        a_0 = ~m_value + 1;
    }
    else {
        a_0 = m_value;
    }
    if (v.m_sign_bit) {
        if (v.m_value == 0) {
            if (m_sign_bit || m_value > 1)
                return true;
            if (m_value == 1)
                *this = v;
            return false;
        }
        b_0 = ~v.m_value + 1;
    }
    else {
        b_0 = v.m_value;
    }

    int msb_pos = digits - 1;
    val_uint a_1 = a_0 & 1;
    val_uint a_2 = a_0 >> 1;
    val_uint b_1 = b_0 & 1;
    val_uint b_2 = b_0 >> 1;
    val_uint v = ((a_2 * b_2) << 1)  +  a_2 * b_1  +  a_1 * b_2;
    super_int c;
    c.m_value = (v << 1) | (a_1 * b_1);
    c.m_sign_bit = v >> msb_pos;

}
*/

} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_SUPER_INT_HPP
