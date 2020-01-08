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

#include <realm/array_object_id.hpp>

namespace realm {

void ArrayObjectId::set(size_t ndx, ObjectId value)
{
    REALM_ASSERT(s_width * ndx < m_size);
    copy_on_write();
    auto values = reinterpret_cast<ObjectId*>(m_data);
    values[ndx] = value;
}

void ArrayObjectId::insert(size_t ndx, ObjectId value)
{
    REALM_ASSERT(s_width * ndx <= m_size);
    // Allocate room for the new value
    alloc(m_size + s_width, 1); // Throws
    m_width = 1;

    auto values = reinterpret_cast<ObjectId*>(m_data);

    // Make gap for new value
    memmove(values + ndx + 1, values + ndx, m_size - s_width * ndx);
    m_size += s_width;

    // Set new value
    values[ndx] = value;
}

void ArrayObjectId::erase(size_t ndx)
{
    REALM_ASSERT(s_width * ndx < m_size);
    copy_on_write();
    // This can throw, but only if array is currently in read-only
    // memory.
    char* dst = m_data + s_width * ndx;
    char* src = dst + s_width;
    size_t cnt = m_data + m_size - src;
    memmove(dst, src, cnt);

    // Update size (also in header)
    m_size -= s_width;
    set_header_size(m_size);
}

void ArrayObjectId::move(ArrayObjectId& dst_arr, size_t ndx)
{
    size_t elements_to_move = size() - ndx;
    if (elements_to_move) {
        char* dst = dst_arr.m_data + dst_arr.m_size;
        dst_arr.alloc(dst_arr.m_size + s_width * elements_to_move, 1);
        memmove(dst, m_data + s_width * ndx, elements_to_move * s_width);
        dst_arr.m_size += s_width * elements_to_move;
    }
    truncate(ndx);
}

size_t ArrayObjectId::find_first(ObjectId value, size_t start, size_t end) const noexcept
{
    auto sz = size();
    if (end == size_t(-1))
        end = sz;
    REALM_ASSERT(start <= sz && end <= sz && start <= end);

    auto values = reinterpret_cast<ObjectId*>(this->m_data);
    for (size_t i = start; i < end; i++) {
        if (values[i] == value)
            return i;
    }
    return realm::npos;
}

} // namespace realm
