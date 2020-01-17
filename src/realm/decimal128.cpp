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

#include <external/IntelRDFPMathLib20U2/LIBRARY/src/bid_conf.h>
#include <external/IntelRDFPMathLib20U2/LIBRARY/src/bid_functions.h>
#include <realm/decimal128.hpp>
#include <cstring>
#include <stdexcept>
#include <sstream>

namespace {
constexpr int DECIMAL_EXPONENT_BIAS_128 = 6176;
constexpr int MAX_STRING_DIGITS = 19;
} // namespace

namespace realm {

// This is a cut down version of bid128_from_string() from the IntelRDFPMathLib20U2 library.
// If we can live with only 19 significant digits, we can avoid a lot of complex code
// as the significant can be stored in w[0] only.
void Decimal128::from_string(const char* ps)
{
    // if null string, return NaN
    if (!ps) {
        m_value.w[1] = 0x7c00000000000000ull;
        m_value.w[0] = 0;
        return;
    }
    // eliminate leading white space
    while ((*ps == ' ') || (*ps == '\t'))
        ps++;

    // c gets first character
    char c = *ps;

    // set up sign_x to be OR'ed with the upper word later
    uint64_t sign_x = (c == '-') ? 0x8000000000000000ull : 0;

    // go to next character if leading sign
    if (c == '-' || c == '+')
        ps++;

    c = *ps;

    // if c isn't a decimal point or a decimal digit, return NaN
    if (!(c == '.' || (c >= '0' && c <= '9'))) {
        m_value.w[1] = 0x7c00000000000000ull | sign_x;
        m_value.w[0] = 0;
        return;
    }

    bool rdx_pt_enc = false;
    if (c == '.') {
        rdx_pt_enc = true;
        ps++;
    }

    // detect zero (and eliminate/ignore leading zeros)
    unsigned right_radix_leading_zeros = 0;
    if (*(ps) == '0') {

        // if all numbers are zeros (with possibly 1 radix point, the number is zero
        // should catch cases such as: 000.0
        while (*ps == '0') {

            ps++;

            // for numbers such as 0.0000000000000000000000000000000000001001,
            // we want to count the leading zeros
            if (rdx_pt_enc) {
                right_radix_leading_zeros++;
            }
            // if this character is a radix point, make sure we haven't already
            // encountered one
            if (*(ps) == '.') {
                if (!rdx_pt_enc) {
                    rdx_pt_enc = true;
                    // if this is the first radix point, and the next character is NULL,
                    // we have a zero
                    if (!*(ps + 1)) {
                        uint64_t tmp = right_radix_leading_zeros;
                        m_value.w[1] = (0x3040000000000000ull - (tmp << 49)) | sign_x;
                        m_value.w[0] = 0;
                        return;
                    }
                    ps = ps + 1;
                }
                else {
                    // if 2 radix points, return NaN
                    m_value.w[1] = 0x7c00000000000000ull | sign_x;
                    m_value.w[0] = 0;
                    return;
                }
            }
            else if (!*(ps)) {
                if (right_radix_leading_zeros > 6176)
                    right_radix_leading_zeros = 6176;
                uint64_t tmp = right_radix_leading_zeros;
                m_value.w[1] = (0x3040000000000000ull - (tmp << 49)) | sign_x;
                m_value.w[0] = 0;
                return;
            }
        }
    }

    c = *ps;

    // initialize local variables
    char buffer[MAX_STRING_DIGITS];
    int ndigits_before = 0;
    int ndigits_total = 0;
    int sgn_exp = 0;
    // pstart_coefficient = ps;

    if (!rdx_pt_enc) {
        // investigate string (before radix point)
        while (c >= '0' && c <= '9') {
            if (ndigits_before == MAX_STRING_DIGITS) {
                throw std::overflow_error("Too many digits before radix point");
            }
            buffer[ndigits_before] = c;
            ps++;
            c = *ps;
            ndigits_before++;
        }

        ndigits_total = ndigits_before;
        if (c == '.') {
            ps++;
        }
    }

    if ((c = *ps)) {
        // investigate string (after radix point)
        while (c >= '0' && c <= '9') {
            if (ndigits_total == MAX_STRING_DIGITS) {
                throw std::overflow_error("Too many digits");
            }
            buffer[ndigits_total] = c;
            ps++;
            c = *ps;
            ndigits_total++;
        }
    }
    int ndigits_after = ndigits_total - ndigits_before;

    // get exponent
    int dec_expon = 0;
    if (c) {
        if (c != 'e' && c != 'E') {
            // return NaN
            m_value.w[1] = 0x7c00000000000000ull;
            m_value.w[0] = 0;
            return;
        }
        ps++;
        c = *ps;
        auto c1 = ps[1];

        // Either the next character must be a digit OR it must be either '-' or '+' AND the following
        // character must be a digit.
        if (!((c >= '0' && c <= '9') || ((c == '+' || c == '-') && c1 >= '0' && c1 <= '9'))) {
            // return NaN
            m_value.w[1] = 0x7c00000000000000ull;
            m_value.w[0] = 0;
            return;
        }

        if (c == '-') {
            sgn_exp = -1;
            ps++;
            c = *ps;
        }
        else if (c == '+') {
            ps++;
            c = (*ps);
        }

        dec_expon = c - '0';
        int i = 1;
        ps++;

        if (!dec_expon) {
            while ((*ps) == '0')
                ps++;
        }
        c = *ps;

        while ((c >= '0' && c <= '9') && i < 7) {
            dec_expon = 10 * dec_expon + (c - '0');
            ps++;
            c = *ps;
            i++;
        }
    }

    dec_expon = (dec_expon + sgn_exp) ^ sgn_exp;
    dec_expon += DECIMAL_EXPONENT_BIAS_128 - ndigits_after - right_radix_leading_zeros;
    uint64_t coeff = 0;
    if (ndigits_total > 0) {
        coeff = buffer[0] - '0';
        for (int i = 1; i < ndigits_total; i++) {
            coeff = 10 * coeff + (buffer[i] - '0');
        }
    }
    m_value.w[0] = coeff;
    uint64_t tmp = dec_expon;
    m_value.w[1] = sign_x | (tmp << 49);
}


Decimal128::Decimal128()
{
    m_value.w[0] = 0xaa;
    m_value.w[1] = 0x7c00000000000000;
}

Decimal128::Decimal128(int64_t val)
{
    BID_SINT64 x(val);
    BID_UINT128 tmp;
    bid128_from_int64(&tmp, &x);
    memcpy(this, &tmp, sizeof(*this));
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

Decimal128::Decimal128(const std::string& init)
{
    from_string(const_cast<char*>(init.c_str()));
}

bool Decimal128::is_null() const
{
    return m_value.w[0] == 0xaa && m_value.w[1] == 0x7c00000000000000;
}

bool Decimal128::operator==(const Decimal128& rhs) const
{
    unsigned flags = 0;
    int ret;
    BID_UINT128 l;
    BID_UINT128 r;
    memcpy(&l, this, sizeof(Decimal128));
    memcpy(&r, &rhs, sizeof(Decimal128));
    bid128_quiet_equal(&ret, &l, &r, &flags);
    return ret != 0;
}

bool Decimal128::operator!=(const Decimal128& rhs) const
{
    return !(*this == rhs);
}

bool Decimal128::operator<(const Decimal128& rhs) const
{
    unsigned flags = 0;
    int ret;
    BID_UINT128 l;
    BID_UINT128 r;
    memcpy(&l, this, sizeof(Decimal128));
    memcpy(&r, &rhs, sizeof(Decimal128));
    bid128_quiet_less(&ret, &l, &r, &flags);
    return ret != 0;
}

bool Decimal128::operator>(const Decimal128& rhs) const
{
    unsigned flags = 0;
    int ret;
    BID_UINT128 l;
    BID_UINT128 r;
    memcpy(&l, this, sizeof(Decimal128));
    memcpy(&r, &rhs, sizeof(Decimal128));
    bid128_quiet_greater(&ret, &l, &r, &flags);
    return ret != 0;
}

bool Decimal128::operator<=(const Decimal128& rhs) const
{
    return !(*this > rhs);
}

bool Decimal128::operator>=(const Decimal128& rhs) const
{
    return !(*this < rhs);
}

std::string Decimal128::to_string() const
{
    // Primitive implementation.
    // Assumes that the significant is stored in w[0] only.
    std::ostringstream ostr;
    ostr << m_value.w[0];
    auto digits = ostr.str();
    ostr.clear();
    bool sign = (m_value.w[1] & 0x8000000000000000ull) != 0;
    int64_t exponen = m_value.w[1] & 0x7fffffffffffffffull;
    exponen >>= 49;
    exponen -= DECIMAL_EXPONENT_BIAS_128;
    size_t digits_before = digits.length();
    while (digits_before > 1 && exponen < 0) {
        digits_before--;
        exponen++;
    }
    std::string ret;
    if (sign)
        ret = "-";
    ret += digits.substr(0, digits_before);
    if (digits_before < digits.length()) {
        // There are also digits after the decimal sign
        ret += '.';
        ret += digits.substr(digits_before);
    }
    if (exponen != 0) {
        ostr.str("");
        ostr << exponen;
        ret += 'E';
        ret += ostr.str();
    }

    return ret;
}

auto Decimal128::to_bid64() const -> Bid64
{
    unsigned flags = 0;
    BID_UINT64 buffer;
    BID_UINT128 tmp;
    memcpy(&tmp, this, sizeof(Decimal128));
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
    coefficient.w[0] = m_value.w[0];
    coefficient.w[1] = m_value.w[1] & 0x00003fffffffffffull;
}


} // namespace realm
