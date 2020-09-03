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

#include <realm/array_uuid.hpp>

namespace realm {

void ArrayUUID::set(size_t ndx, const UUID& value)
{
    REALM_ASSERT(is_valid_ndx(ndx));
    copy_on_write();
    reinterpret_cast<UUID*>(this->m_data)[ndx] = value;
}

void ArrayUUID::insert(size_t ndx, const UUID& value)
{
    const auto old_size = size();
    REALM_ASSERT(ndx <= old_size);

    // alloc takes care of copy_on_write
    alloc(m_size + 1, s_width); // Throws

    memmove(m_data + s_width * (ndx + 1), m_data + s_width * ndx, s_width * (old_size - ndx));

    reinterpret_cast<UUID*>(this->m_data)[ndx] = value;
}

void ArrayUUID::erase(size_t ndx)
{
    REALM_ASSERT(is_valid_ndx(ndx));

    // This can throw, but only if array is currently in read-only
    // memory.
    copy_on_write();

    // move data backwards after deletion
    if (ndx < m_size - 1) {
        char* new_begin = m_data + ndx * s_width;
        char* old_begin = new_begin + s_width;
        char* old_end = m_data + m_size * s_width;
        realm::safe_copy_n(old_begin, old_end - old_begin, new_begin);
    }

    --m_size;
    set_header_size(m_size);
}

void ArrayUUID::move(ArrayUUID& dst_arr, size_t ndx)
{
    REALM_ASSERT(is_valid_ndx(ndx));

    const auto old_src_size = size();
    const auto old_dst_size = dst_arr.size();

    const auto n_to_move = old_src_size - ndx;

    // Allocate room for the new value
    dst_arr.alloc(old_dst_size + n_to_move, s_width); // Throws

    char* src = m_data + ndx * s_width;
    char* dest = dst_arr.m_data;
    memmove(dest, src, n_to_move * s_width);

    truncate(ndx);
}

size_t ArrayUUID::find_first(const UUID& value, size_t start, size_t end) const noexcept
{
    auto sz = size();
    if (end == size_t(-1))
        end = sz;
    REALM_ASSERT(start <= sz && end <= sz && start <= end);
    for (size_t i = start; i < end; ++i) {
        const char* data = m_data + (i * s_width);
        if (memcmp(data, &value, sizeof(UUID)) == 0) {
            return i;
        }
    }
    return realm::npos;
}

} // namespace realm
