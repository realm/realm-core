/*************************************************************************
 *
 * Copyright 2019 Realm Inc.
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
#include <realm/decimal128.hpp>

#include <realm/string_data.hpp>
#include <realm/util/to_string.hpp>

#include <external/IntelRDFPMathLib20U2/LIBRARY/src/bid_conf.h>
#include <external/IntelRDFPMathLib20U2/LIBRARY/src/bid_functions.h>
#include <cstring>
#include <stdexcept>

namespace {
constexpr int DECIMAL_EXPONENT_BIAS_128 = 6176;
} // namespace

namespace realm {

Decimal128 to_decimal128(const BID_UINT128& val)
{
    Decimal128 tmp;
    memcpy(tmp.raw(), &val, sizeof(BID_UINT128));
    return tmp;
}

BID_UINT128 to_BID_UINT128(const Decimal128& val)
{
    BID_UINT128 ret;
    memcpy(&ret, val.raw(), sizeof(BID_UINT128));
    return ret;
}

Decimal128::Decimal128()
{
    from_int64_t(0);
}

Decimal128::Decimal128(double val, RoundingPrecision rounding_precision)
{
    // The logic below is ported from MongoDB Decimal128 implementation
    uint64_t largest_coeff = (rounding_precision == RoundTo7Digits) ? 1E7 - 1 : 1E15 - 1;
    unsigned flags = 0;
    BID_UINT128 converted_value;
    binary64_to_bid128(&converted_value, &val, &flags);
    memcpy(this, &converted_value, sizeof(*this));

    // If the precision is already less than 15 decimals, or val is infinity or NaN, there's no need to quantize
    if ((get_coefficient_low() <= largest_coeff && get_coefficient_high() == 0) || std::isinf(val) ||
        std::isnan(val)) {
        return;
    }

    // Get the base2 exponent from val.
    int base2Exp;
    frexp(val, &base2Exp);

    // As frexp normalizes val between 0.5 and 1.0 rather than 1.0 and 2.0, adjust.
    base2Exp--;


    // We will use base10Exp = base2Exp * 30103 / (100*1000) as lowerbound (using integer division).
    //
    // This formula is derived from the following, with base2Exp the binary exponent of val:
    //   (1) 10**(base2Exp * log10(2)) == 2**base2Exp
    //   (2) 0.30103 closely approximates log10(2)
    //
    // Exhaustive testing using Python shows :
    //     { base2Exp * 30103 / (100 * 1000) == math.floor(math.log10(2**base2Exp))
    //       for base2Exp in xrange(-1074, 1023) } == { True }
    int base10Exp = (base2Exp * 30103) / (100 * 1000);

    // As integer division truncates, rather than rounds down (as in Python), adjust accordingly.
    if (base2Exp < 0)
        base10Exp--;

    int adjust = (rounding_precision == RoundTo7Digits) ? 6 : 14;
    BID_UINT128 q{1, BID_UINT64(base10Exp - adjust + DECIMAL_EXPONENT_BIAS_128) << 49};
    BID_UINT128 quantizedResult;
    bid128_quantize(&quantizedResult, &converted_value, &q, &flags);
    memcpy(this, &quantizedResult, sizeof(*this));

    // Check if the quantization was done correctly: m_valuem_value stores exactly 15
    // decimal digits of precision (15 digits can fit into the low 64 bits of the decimal)
    if (get_coefficient_low() > largest_coeff) {
        // If we didn't precisely get 15 digits of precision, the original base 10 exponent
        // guess was 1 off, so quantize once more with base10Exp + 1
        adjust--;
        BID_UINT128 q1{1, (BID_UINT64(base10Exp) - adjust + DECIMAL_EXPONENT_BIAS_128) << 49};
        bid128_quantize(&quantizedResult, &converted_value, &q1, &flags);
        memcpy(this, &quantizedResult, sizeof(*this));
    }
}

void Decimal128::from_int64_t(int64_t val)
{
    constexpr uint64_t expon = uint64_t(DECIMAL_EXPONENT_BIAS_128) << 49;
    if (val < 0) {
        m_value.w[1] = expon | 0x8000000000000000ull;
        m_value.w[0] = ~val + 1;
    }
    else {
        m_value.w[1] = expon;
        m_value.w[0] = val;
    }
}

Decimal128::Decimal128(int val)
{
    from_int64_t(static_cast<int64_t>(val));
}

Decimal128::Decimal128(int64_t val)
{
    from_int64_t(val);
}

Decimal128::Decimal128(uint64_t val)
{
    BID_UINT64 tmp(val);
    BID_UINT128 expanded;
    bid128_from_uint64(&expanded, &tmp);
    memcpy(this, &expanded, sizeof(*this));
}

Decimal128::Decimal128(Bid64 val)
{
    unsigned flags = 0;
    BID_UINT64 x(val.w);
    BID_UINT128 tmp;
    bid64_to_bid128(&tmp, &x, &flags);
    memcpy(this, &tmp, sizeof(*this));
}

Decimal128::Decimal128(Bid128 coefficient, int exponent, bool sign)
{
    uint64_t sign_x = sign ? 0x8000000000000000ull : 0;
    m_value = coefficient;
    uint64_t tmp = exponent + DECIMAL_EXPONENT_BIAS_128;
    m_value.w[1] |= (sign_x | (tmp << 49));
}

Decimal128::Decimal128(StringData init)
{
    unsigned flags = 0;
    BID_UINT128 tmp;
    bid128_from_string(&tmp, const_cast<char*>(init.data()), &flags);
    memcpy(this, &tmp, sizeof(*this));
}

Decimal128::Decimal128(null) noexcept
{
    m_value.w[0] = 0xaa;
    m_value.w[1] = 0x7c00000000000000;
}

Decimal128 Decimal128::nan(const char* init)
{
    Bid128 val;
    val.w[0] = strtol(init, nullptr, 10);
    val.w[1] = 0x7c00000000000000ull;
    return Decimal128(val);
}

bool Decimal128::is_null() const
{
    return m_value.w[0] == 0xaa && m_value.w[1] == 0x7c00000000000000;
}

bool Decimal128::is_nan() const
{
    return (m_value.w[1] & 0x7c00000000000000ull) == 0x7c00000000000000ull;
}

bool Decimal128::to_int(int64_t& i) const
{
    BID_SINT64 res;
    unsigned flags = 0;
    BID_UINT128 x = to_BID_UINT128(*this);
    bid128_to_int64_int(&res, &x, &flags);
    if (flags == 0) {
        i = res;
        return true;
    }
    return false;
}


bool Decimal128::operator==(const Decimal128& rhs) const
{
    if (is_null() && rhs.is_null()) {
        return true;
    }
    unsigned flags = 0;
    int ret;
    BID_UINT128 l = to_BID_UINT128(*this);
    BID_UINT128 r = to_BID_UINT128(rhs);
    bid128_quiet_equal(&ret, &l, &r, &flags);
    return ret != 0;
}

bool Decimal128::operator!=(const Decimal128& rhs) const
{
    return !(*this == rhs);
}

int Decimal128::compare(const Decimal128& rhs) const
{
    unsigned flags = 0;
    int ret;
    BID_UINT128 l = to_BID_UINT128(*this);
    BID_UINT128 r = to_BID_UINT128(rhs);
    bid128_quiet_less(&ret, &l, &r, &flags);
    if (ret)
        return -1;
    bid128_quiet_greater(&ret, &l, &r, &flags);
    if (ret)
        return 1;

    // Either equal or one or more is NaN
    bool lhs_is_nan = is_nan();
    bool rhs_is_nan = rhs.is_nan();
    if (!lhs_is_nan && !rhs_is_nan) {
        // Neither is NaN
        return 0;
    }
    if (lhs_is_nan && rhs_is_nan) {
        // We should have stable sorting of NaN
        if (m_value.w[1] == rhs.m_value.w[1]) {
            return m_value.w[0] < rhs.m_value.w[0] ? -1 : 1;
        }
        else {
            return m_value.w[1] < rhs.m_value.w[1] ? -1 : 1;
        }
    }
    // nan vs non-nan should always order nan first
    return lhs_is_nan ? -1 : 1;
}

bool Decimal128::operator<(const Decimal128& rhs) const
{
    return compare(rhs) < 0;
}

bool Decimal128::operator>(const Decimal128& rhs) const
{
    return compare(rhs) > 0;
}

bool Decimal128::operator<=(const Decimal128& rhs) const
{
    return compare(rhs) <= 0;
}

bool Decimal128::operator>=(const Decimal128& rhs) const
{
    return compare(rhs) >= 0;
}

Decimal128 do_multiply(BID_UINT128 x, BID_UINT128 mul)
{
    unsigned flags = 0;
    BID_UINT128 res;
    bid128_mul(&res, &x, &mul, &flags);
    return to_decimal128(res);
}

Decimal128 Decimal128::operator*(int64_t mul) const
{
    BID_UINT128 x = to_BID_UINT128(*this);
    BID_UINT128 y = to_BID_UINT128(Decimal128(mul));
    return do_multiply(x, y);
}

Decimal128 Decimal128::operator*(size_t mul) const
{
    Decimal128 tmp_mul(static_cast<uint64_t>(mul));
    return do_multiply(to_BID_UINT128(*this), to_BID_UINT128(tmp_mul));
}

Decimal128 Decimal128::operator*(int mul) const
{
    BID_UINT128 x = to_BID_UINT128(*this);
    BID_UINT128 y = to_BID_UINT128(Decimal128(mul));
    return do_multiply(x, y);
}

Decimal128 Decimal128::operator*(Decimal128 mul) const
{
    BID_UINT128 x = to_BID_UINT128(*this);
    BID_UINT128 y = to_BID_UINT128(mul);
    return do_multiply(x, y);
}

Decimal128 do_divide(BID_UINT128 x, BID_UINT128 div)
{
    unsigned flags = 0;
    BID_UINT128 res;
    bid128_div(&res, &x, &div, &flags);
    return to_decimal128(res);
}

Decimal128 Decimal128::operator/(int64_t div) const
{
    BID_UINT128 x = to_BID_UINT128(*this);
    BID_UINT128 y = to_BID_UINT128(Decimal128(div));
    return do_divide(x, y);
}

Decimal128 Decimal128::operator/(size_t div) const
{
    Decimal128 tmp_div(static_cast<uint64_t>(div));
    return do_divide(to_BID_UINT128(*this), to_BID_UINT128(tmp_div));
}

Decimal128 Decimal128::operator/(int div) const
{
    BID_UINT128 x = to_BID_UINT128(*this);
    BID_UINT128 y = to_BID_UINT128(Decimal128(div));
    return do_divide(x, y);
}

Decimal128 Decimal128::operator/(Decimal128 div) const
{
    BID_UINT128 x = to_BID_UINT128(*this);
    BID_UINT128 y = to_BID_UINT128(div);
    return do_divide(x, y);
}

Decimal128& Decimal128::operator+=(Decimal128 rhs)
{
    unsigned flags = 0;
    BID_UINT128 x = to_BID_UINT128(*this);
    BID_UINT128 y = to_BID_UINT128(rhs);

    BID_UINT128 res;
    bid128_add(&res, &x, &y, &flags);
    memcpy(this, &res, sizeof(Decimal128));
    return *this;
}

Decimal128& Decimal128::operator-=(Decimal128 rhs)
{
    unsigned flags = 0;
    BID_UINT128 x = to_BID_UINT128(*this);
    BID_UINT128 y = to_BID_UINT128(rhs);

    BID_UINT128 res;
    bid128_sub(&res, &x, &y, &flags);
    memcpy(this, &res, sizeof(Decimal128));
    return *this;
}

bool Decimal128::is_valid_str(StringData str) noexcept
{
    unsigned flags = 0;
    BID_UINT128 tmp;
    bid128_from_string(&tmp, const_cast<char*>(str.data()), &flags);

    return (tmp.w[1] & 0x7c00000000000000ull) != 0x7c00000000000000ull;
}

std::string Decimal128::to_string() const
{
    if (is_null()) {
        return "NULL";
    }

    Bid128 coefficient;
    int exponen;
    bool sign;
    unpack(coefficient, exponen, sign);
    if (coefficient.w[1]) {
        char buffer[64];
        unsigned flags = 0;
        BID_UINT128 x;
        memcpy(&x, this, sizeof(Decimal128));
        bid128_to_string(buffer, &x, &flags);
        return std::string(buffer);
    }

    // The significant is stored in w[0] only. We can get a nicer printout by using this
    // algorithm here.
    std::string ret;
    if (sign)
        ret = "-";

    // check for NaN or Infinity
    if ((m_value.w[1] & 0x7800000000000000ull) == 0x7800000000000000ull) {
        if ((m_value.w[1] & 0x7c00000000000000ull) == 0x7c00000000000000ull) { // x is NAN
            ret += "NaN";
        }
        else {
            ret += "Inf";
        }
        return ret;
    }

    auto digits = util::to_string(coefficient.w[0]);
    size_t digits_before = digits.length();
    while (digits_before > 1 && exponen != 0) {
        digits_before--;
        exponen++;
    }
    ret += digits.substr(0, digits_before);
    if (digits_before < digits.length()) {
        // There are also digits after the decimal sign
        ret += '.';
        ret += digits.substr(digits_before);
    }
    if (exponen != 0) {
        ret += 'E';
        ret += util::to_string(exponen);
    }

    return ret;
}

auto Decimal128::to_bid64() const -> Bid64
{
    unsigned flags = 0;
    BID_UINT64 buffer;
    BID_UINT128 tmp = to_BID_UINT128(*this);
    bid128_to_bid64(&buffer, &tmp, &flags);
    if (flags & ~BID_INEXACT_EXCEPTION)
        throw std::overflow_error("Decimal128::to_bid64 failed");
    return Bid64(buffer);
}

void Decimal128::unpack(Bid128& coefficient, int& exponent, bool& sign) const noexcept
{
    sign = (m_value.w[1] & 0x8000000000000000ull) != 0;
    int64_t exp = m_value.w[1] & 0x7fffffffffffffffull;
    exp >>= 49;
    exponent = int(exp) - DECIMAL_EXPONENT_BIAS_128;
    coefficient.w[0] = get_coefficient_low();
    coefficient.w[1] = get_coefficient_high();
}

} // namespace realm
