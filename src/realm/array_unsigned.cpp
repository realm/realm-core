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

#include <realm/array_unsigned.hpp>
#include <algorithm>

namespace realm {

void ArrayUnsigned::set_width(uint8_t width)
{
    // if 'width == 0' then 'uint64_t(-(width != 0))' is 0, so width will become 0
    m_ubound = uint64_t(-(width != 0)) & (uint64_t(-1) >> (sizeof(uint64_t) * 8 - width));
    m_width = width;
}

inline uint8_t ArrayUnsigned::bit_width(uint64_t value)
{
    if (value == 0) {
        return 0;
    }
    if (value < 0x100) {
        return 8;
    }
    if (value < 0x10000) {
        return 16;
    }
    if (value < 0x100000000) {
        return 32;
    }
    return 64;
}

inline void ArrayUnsigned::_set(size_t ndx, uint8_t width, uint64_t value)
{
    if (width == 8) {
        reinterpret_cast<uint8_t*>(m_data)[ndx] = uint8_t(value);
    }
    else if (width == 16) {
        reinterpret_cast<uint16_t*>(m_data)[ndx] = uint16_t(value);
    }
    else if (width == 32) {
        reinterpret_cast<uint32_t*>(m_data)[ndx] = uint32_t(value);
    }
    else {
        reinterpret_cast<uint64_t*>(m_data)[ndx] = uint64_t(value);
    }
}

inline uint64_t ArrayUnsigned::_get(size_t ndx, uint8_t width) const
{
    if (width == 8) {
        return reinterpret_cast<uint8_t*>(m_data)[ndx];
    }
    if (width == 16) {
        return reinterpret_cast<uint16_t*>(m_data)[ndx];
    }
    if (width == 32) {
        return reinterpret_cast<uint32_t*>(m_data)[ndx];
    }
    return reinterpret_cast<uint64_t*>(m_data)[ndx];
}

void ArrayUnsigned::create(size_t initial_size, uint64_t ubound_value)
{
    MemRef mem = create_node(initial_size, get_alloc(), false, Node::type_Normal, wtype_Bits,
                             bit_width(ubound_value)); // Throws
    init_from_mem(mem);
}

bool ArrayUnsigned::update_from_parent(size_t old_baseline) noexcept
{
    REALM_ASSERT_DEBUG(is_attached());
    ArrayParent* parent = get_parent();
    REALM_ASSERT_DEBUG(parent);

    // Array nodes that are part of the previous version of the
    // database will not be overwritten by Group::commit(). This is
    // necessary for robustness in the face of abrupt termination of
    // the process. It also means that we can be sure that an array
    // remains unchanged across a commit if the new ref is equal to
    // the old ref and the ref is below the previous baseline.

    ref_type new_ref = get_ref_from_parent();
    if (new_ref == m_ref && new_ref < old_baseline)
        return false; // Has not changed

    init_from_ref(new_ref);
    return true; // Might have changed
}

size_t ArrayUnsigned::lower_bound(uint64_t value) const noexcept
{
    if (m_width == 8) {
        uint8_t* arr = reinterpret_cast<uint8_t*>(m_data);
        uint8_t* pos = std::lower_bound(arr, arr + m_size, value);
        return pos - arr;
    }
    else if (m_width == 16) {
        uint16_t* arr = reinterpret_cast<uint16_t*>(m_data);
        uint16_t* pos = std::lower_bound(arr, arr + m_size, value);
        return pos - arr;
    }
    else if (m_width == 32) {
        uint32_t* arr = reinterpret_cast<uint32_t*>(m_data);
        uint32_t* pos = std::lower_bound(arr, arr + m_size, value);
        return pos - arr;
    }
    uint64_t* arr = reinterpret_cast<uint64_t*>(m_data);
    uint64_t* pos = std::lower_bound(arr, arr + m_size, value);
    return pos - arr;
}

size_t ArrayUnsigned::upper_bound(uint64_t value) const noexcept
{
    if (m_width == 8) {
        uint8_t* arr = reinterpret_cast<uint8_t*>(m_data);
        uint8_t* pos = std::upper_bound(arr, arr + m_size, value);
        return pos - arr;
    }
    else if (m_width == 16) {
        uint16_t* arr = reinterpret_cast<uint16_t*>(m_data);
        uint16_t* pos = std::upper_bound(arr, arr + m_size, value);
        return pos - arr;
    }
    else if (m_width == 32) {
        uint32_t* arr = reinterpret_cast<uint32_t*>(m_data);
        uint32_t* pos = std::upper_bound(arr, arr + m_size, value);
        return pos - arr;
    }
    uint64_t* arr = reinterpret_cast<uint64_t*>(m_data);
    uint64_t* pos = std::upper_bound(arr, arr + m_size, value);
    return pos - arr;
}

void ArrayUnsigned::insert(size_t ndx, uint64_t value)
{
    bool do_expand = value > m_ubound;
    size_t new_width = do_expand ? bit_width(value) : m_width;

    REALM_ASSERT_DEBUG(!do_expand || new_width > m_width);
    REALM_ASSERT_DEBUG(ndx <= m_size);

    // Check if we need to copy before modifying
    copy_on_write();              // Throws
    alloc(m_size + 1, new_width); // Throws

    // Move values above insertion (may expand)
    if (do_expand) {
        size_t i = m_size;
        while (i > ndx) {
            --i;
            auto tmp = _get(i, m_width);
            _set(i + 1, new_width, tmp);
        }
    }
    else if (ndx != m_size) {
        size_t w = (new_width >> 3);

        char* src_begin = m_data + ndx * w;
        char* src_end = m_data + m_size * w;
        char* dst = src_end + w;

        std::copy_backward(src_begin, src_end, dst);
    }

    // Insert the new value
    _set(ndx, new_width, value);

    // Expand values before insertion
    if (do_expand) {
        size_t i = ndx;
        while (i != 0) {
            --i;
            _set(i, new_width, _get(i, m_width));
        }
        set_width(new_width);
    }

    // Update size
    // (no need to do it in header as it has been done by Alloc)
    ++m_size;
}

void ArrayUnsigned::erase(size_t ndx)
{
    copy_on_write(); // Throws

    size_t w = m_width >> 3;

    char* dst = m_data + ndx * w;
    const char* src = dst + w;
    size_t num_bytes = (m_size - ndx) * w;

    std::copy_n(src, num_bytes, dst);

    // Update size (also in header)
    --m_size;
    set_header_size(m_size);
}

uint64_t ArrayUnsigned::get(size_t index) const
{
    return _get(index, m_width);
}

void ArrayUnsigned::set(size_t ndx, uint64_t value)
{
    copy_on_write(); // Throws

    if (value > m_ubound) {
        size_t new_width = bit_width(value);

        alloc(m_size, new_width); // Throws

        size_t i = m_size;
        while (i) {
            i--;
            auto v = _get(i, m_width);
            _set(i, new_width, v);
        }

        set_width(new_width);
    }

    _set(ndx, m_width, value);
}

void ArrayUnsigned::truncate(size_t ndx)
{
    m_size = ndx;
    copy_on_write();
    set_header_size(m_size);
    if (ndx == 0) {
        set_width(0);
    }
}

} // namespace realm
