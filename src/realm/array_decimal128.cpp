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

#include <realm/array_decimal128.hpp>
#include <realm/mixed.hpp>

namespace realm {

namespace {

uint8_t min_width(const Decimal128& value, bool zero_width_is_zero)
{
    Decimal128::Bid128 coefficient;
    int exponent;
    bool sign;

    if (value.is_null()) {
        return zero_width_is_zero ? 4 : 0;
    }

    value.unpack(coefficient, exponent, sign);
    if (coefficient.w[1] == 0) {
        if (coefficient.w[0] == 0) {
            return zero_width_is_zero ? 0 : 4;
        }
        if (coefficient.w[0] < (1ull << 23) && exponent > -91 && exponent < 91) {
            return 4;
        }
        if (coefficient.w[0] < (1ull << 53) && exponent > -370 && exponent < 370) {
            return 8;
        }
    }
    return 16;
}
} // namespace

void ArrayDecimal128::set(size_t ndx, Decimal128 value)
{
    REALM_ASSERT(ndx < m_size);
    copy_on_write();
    switch (upgrade_leaf(min_width(value, Array::get_context_flag()))) {
        case 0:
            break;
        case 4: {
            auto values = reinterpret_cast<Decimal::Bid32*>(m_data);
            auto val = value.to_bid32();
            REALM_ASSERT(val);
            values[ndx] = *val;
            break;
        }
        case 8: {
            auto values = reinterpret_cast<Decimal::Bid64*>(m_data);
            auto val = value.to_bid64();
            REALM_ASSERT(val);
            values[ndx] = *val;
            break;
        }
        case 16: {
            auto values = reinterpret_cast<Decimal128*>(m_data);
            values[ndx] = value;
            break;
        }
    }
}

void ArrayDecimal128::insert(size_t ndx, Decimal128 value)
{
    REALM_ASSERT(ndx <= m_size);
    if (m_size == 0 && value == Decimal128()) {
        // zero width should be interpreted as 0
        Array::copy_on_write();
        Array::set_context_flag(true);
    }
    // Allocate room for the new value
    switch (upgrade_leaf(min_width(value, Array::get_context_flag()))) {
        case 0:
            m_size += 1;
            Array::copy_on_write();
            set_header_size(m_size);
            break;
        case 4: {
            alloc(m_size + 1, 4); // Throws

            auto src = reinterpret_cast<Decimal::Bid32*>(m_data) + ndx;
            auto dst = src + 1;

            // Make gap for new value
            memmove(dst, src, sizeof(Decimal::Bid32) * (m_size - 1 - ndx));

            // Set new value
            auto val = value.to_bid32();
            REALM_ASSERT(val);
            *src = *val;
            break;
        }
        case 8: {
            alloc(m_size + 1, 8); // Throws

            auto src = reinterpret_cast<Decimal::Bid64*>(m_data) + ndx;
            auto dst = src + 1;

            // Make gap for new value
            memmove(dst, src, sizeof(Decimal::Bid64) * (m_size - 1 - ndx));

            // Set new value
            auto val = value.to_bid64();
            REALM_ASSERT(val);
            *src = *val;
            break;
        }
        case 16: {
            alloc(m_size + 1, sizeof(Decimal128)); // Throws

            auto src = reinterpret_cast<Decimal128*>(m_data) + ndx;
            auto dst = src + 1;

            // Make gap for new value
            memmove(dst, src, sizeof(Decimal128) * (m_size - 1 - ndx));

            // Set new value
            *src = value;
            break;
        }
    }
}

void ArrayDecimal128::erase(size_t ndx)
{

    REALM_ASSERT(ndx < m_size);

    copy_on_write();

    if (m_width) {
        auto dst = m_data + ndx * m_width;
        memmove(dst, dst + m_width, m_width * (m_size - ndx));
    }

    // Update size (also in header)
    m_size -= 1;
    set_header_size(m_size);
}

void ArrayDecimal128::move(ArrayDecimal128& dst_arr, size_t ndx)
{
    size_t elements_to_move = m_size - ndx;
    if (elements_to_move) {
        if (m_width >= dst_arr.m_width) {
            dst_arr.upgrade_leaf(m_width);
            const auto old_dst_size = dst_arr.m_size;
            dst_arr.alloc(old_dst_size + elements_to_move, m_width);
            auto dst = dst_arr.m_data + old_dst_size * m_width;
            auto src = m_data + ndx * m_width;
            memmove(dst, src, elements_to_move * m_width);
        }
        else {
            for (size_t i = 0; i < elements_to_move; i++) {
                dst_arr.add(get(ndx + i));
            }
        }
    }
    truncate(ndx);
}

size_t ArrayDecimal128::find_first(Decimal128 value, size_t start, size_t end) const noexcept
{
    auto sz = size();
    if (end == size_t(-1))
        end = sz;
    REALM_ASSERT(start <= sz && end <= sz && start <= end);

    bool zero_width_is_zero = Array::get_context_flag();
    auto width = min_width(value, zero_width_is_zero);
    switch (m_width) {
        case 0:
            if (zero_width_is_zero) {
                if (value == Decimal128()) {
                    return 0;
                }
            }
            else {
                if (value.is_null()) {
                    return 0;
                }
            }
            break;
        case 4:
            if (width <= 4) {
                // Worth optimizing here
                auto optval32 = value.to_bid32();
                REALM_ASSERT(optval32);
                auto val32 = *optval32;
                auto values = reinterpret_cast<Decimal128::Bid32*>(this->m_data);
                for (size_t i = start; i < end; i++) {
                    if (values[i] == val32)
                        return i;
                }
            }
            break;
        case 8:
            if (width <= 8) {
                auto values = reinterpret_cast<Decimal128::Bid64*>(this->m_data);
                for (size_t i = start; i < end; i++) {
                    if (Decimal128(values[i]) == value)
                        return i;
                }
            }
            break;
        case 16: {
            auto values = reinterpret_cast<Decimal128*>(this->m_data);
            for (size_t i = start; i < end; i++) {
                if (values[i] == value)
                    return i;
            }
            break;
        }
    }

    return realm::npos;
}

Mixed ArrayDecimal128::get_any(size_t ndx) const
{
    return Mixed(get(ndx));
}


size_t ArrayDecimal128::upgrade_leaf(uint8_t width)
{
    if (m_width == 16) {
        return 16;
    }
    if (width <= m_width) {
        return m_width;
    }

    if (m_size == 0) {
        alloc(m_size, width);
        return width;
    }

    if (m_width == 8) {
        // Upgrade to 16 bytes
        alloc(m_size, 16);
        auto src = reinterpret_cast<Decimal::Bid64*>(m_data);
        auto dst = reinterpret_cast<Decimal::Bid128*>(m_data);
        for (size_t i = m_size; i > 0; --i) {
            auto val = Decimal128(src[i - 1]);
            dst[i - 1] = *val.raw();
        }
        return 16;
    }

    if (m_width == 4) {
        alloc(m_size, width);
        auto src = reinterpret_cast<Decimal::Bid32*>(m_data);
        if (width == 8) {
            // Upgrade to 8 bytes
            auto dst = reinterpret_cast<Decimal::Bid64*>(m_data);
            for (size_t i = m_size; i > 0; --i) {
                auto val = Decimal128(src[i - 1]);
                dst[i - 1] = *val.to_bid64();
            }
        }
        else if (width == 16) {
            // Upgrade to 16 bytes
            auto dst = reinterpret_cast<Decimal::Bid128*>(m_data);
            for (size_t i = m_size; i > 0; --i) {
                auto val = Decimal128(src[i - 1]);
                dst[i - 1] = *val.raw();
            }
        }
        return width;
    }

    // Upgrade from zero width. Fill with either 0 or null.
    Decimal128 fill_value = get_context_flag() ? Decimal128(0) : Decimal128(realm::null());

    if (width == 4) {
        // Upgrade to 4 bytes
        alloc(m_size, 4);
        auto values = reinterpret_cast<Decimal::Bid32*>(m_data);
        auto fill = *fill_value.to_bid32();
        for (size_t i = 0; i < m_size; i++) {
            values[i] = fill;
        }
        return 4;
    }
    else if (width == 8) {
        // Upgrade to 8 bytes
        alloc(m_size, 8);
        auto values = reinterpret_cast<Decimal::Bid64*>(m_data);
        auto fill = *fill_value.to_bid64();
        for (size_t i = 0; i < m_size; i++) {
            values[i] = fill;
        }
        return 8;
    }

    alloc(m_size, 16);
    auto values = reinterpret_cast<Decimal128*>(m_data);
    for (size_t i = 0; i < m_size; i++) {
        values[i] = fill_value;
    }
    return 16;
}


} // namespace realm
