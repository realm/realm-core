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

void ArrayDecimal128::set(size_t ndx, Decimal128 value)
{
    REALM_ASSERT(ndx < m_size);
    copy_on_write();
    auto values = reinterpret_cast<Decimal128*>(m_data);
    values[ndx] = value;
}

void ArrayDecimal128::insert(size_t ndx, Decimal128 value)
{
    REALM_ASSERT(ndx <= m_size);
    // Allocate room for the new value
    alloc(m_size + 1, sizeof(Decimal128)); // Throws

    auto src = reinterpret_cast<Decimal128*>(m_data) + ndx;
    auto dst = src + 1;

    // Make gap for new value
    memmove(dst, src, sizeof(Decimal128) * (m_size - 1 - ndx));

    // Set new value
    *src = value;
}

void ArrayDecimal128::erase(size_t ndx)
{
    REALM_ASSERT(ndx < m_size);

    copy_on_write();

    Decimal128* dst = reinterpret_cast<Decimal128*>(m_data) + ndx;
    Decimal128* src = dst + 1;

    memmove(dst, src, sizeof(Decimal128) * (m_size - ndx));

    // Update size (also in header)
    m_size -= 1;
    set_header_size(m_size);
}

void ArrayDecimal128::move(ArrayDecimal128& dst_arr, size_t ndx)
{
    size_t elements_to_move = m_size - ndx;
    if (elements_to_move) {
        const auto old_dst_size = dst_arr.m_size;
        dst_arr.alloc(old_dst_size + elements_to_move, sizeof(Decimal128));
        Decimal128* dst = reinterpret_cast<Decimal128*>(dst_arr.m_data) + old_dst_size;
        Decimal128* src = reinterpret_cast<Decimal128*>(m_data) + ndx;
        memmove(dst, src, elements_to_move * sizeof(Decimal128));
    }
    truncate(ndx);
}

size_t ArrayDecimal128::find_first(Decimal128 value, size_t start, size_t end) const noexcept
{
    auto sz = size();
    if (end == size_t(-1))
        end = sz;
    REALM_ASSERT(start <= sz && end <= sz && start <= end);

    auto values = reinterpret_cast<Decimal128*>(this->m_data);
    for (size_t i = start; i < end; i++) {
        if (values[i] == value)
            return i;
    }
    return realm::npos;
}

Mixed ArrayDecimal128::get_any(size_t ndx) const
{
    return Mixed(get(ndx));
}


} // namespace realm
