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

// This is only for debugging. We use the null bit vector for checking if an index is null.
// This value should be easy to spot in hex dumps, and should also be unlikely to be a "real" ObjectId. For one thing,
// if using the normal generation algorithm, it can only be generated at precisely 2088-05-21T00:11:25. Of course
// users could also be using this as a sentinel so we must support storing this value in a non-null OID.
const ObjectId ArrayObjectIdNull::null_oid = ObjectId("DEADDEAD"
                                                      "DEADDEAD"
                                                      "DEADDEAD");

void ArrayObjectId::set(size_t ndx, const ObjectId& value)
{
    REALM_ASSERT(is_valid_ndx(ndx));
    copy_on_write();

    const auto pos = get_pos(ndx);
    pos.set_value(this, value);
    pos.set_null(this, false);
}

void ArrayObjectId::insert(size_t ndx, const ObjectId& value)
{
    const auto old_size = size();
    REALM_ASSERT(ndx <= old_size);

    // Allocate room for the new value
    const auto new_byte_size = calc_required_bytes(old_size + 1);
    alloc(new_byte_size, 1); // Throws
    m_size = new_byte_size;
    m_width = 1;

    auto dest = get_pos(old_size);

    // Initialize null byte when a new section is taken into use
    if (old_size % 8 == 0) {
        m_data[dest.base_byte] = 0;
    }

    // Make gap for new value
    // Possible optimization: Use memmove + shifting the bitvector to operate in chunks.
    for (size_t i = old_size; i > ndx; i--) {
        auto src = get_pos(i - 1);
        dest.set_value(this, src.get_value(this));
        dest.set_null(this, src.is_null(this));
        dest = src;
    }

    // Set new value
    dest.set_value(this, value);
    dest.set_null(this, false);
}

void ArrayObjectId::erase(size_t ndx)
{
    REALM_ASSERT(is_valid_ndx(ndx));

    // This can throw, but only if array is currently in read-only
    // memory.
    copy_on_write();

    const auto new_size = size() - 1;
    m_size = calc_required_bytes(new_size);
    set_header_size(m_size);

    // Possible optimization: Use memmove + shifting the bitvector to operate in chunks.
    auto dest = get_pos(ndx);
    for (size_t i = ndx; i < new_size; i++) {
        auto src = get_pos(i + 1);
        dest.set_value(this, src.get_value(this));
        dest.set_null(this, src.is_null(this));
        dest = src;
    }
}

void ArrayObjectId::move(ArrayObjectId& dst_arr, size_t ndx)
{
    REALM_ASSERT(is_valid_ndx(ndx));

    const auto old_src_size = size();
    const auto old_dst_size = dst_arr.size();

    const auto n_to_move = old_src_size - ndx;

    // Allocate room for the new value
    const auto new_dest_byte_size = calc_required_bytes(old_dst_size + n_to_move);
    dst_arr.alloc(new_dest_byte_size, 1); // Throws
    dst_arr.m_width = 1;
    dst_arr.m_size = new_dest_byte_size;

    // Initialize last null byte.
    const auto last_in_dst = get_pos(old_dst_size + n_to_move - 1);
    dst_arr.m_data[last_in_dst.base_byte] = 0;

    for (size_t i = 0; i < n_to_move; i++) {
        // Possible optimization: Use memmove + shifting the bitvector to operate in chunks. This can be especially
        // beneficial if ndx and dst_arr.size() are equivalent mod 8.
        const auto src = get_pos(ndx + i);
        const auto dst = get_pos(old_dst_size + i);
        dst.set_value(&dst_arr, src.get_value(this));
        dst.set_null(&dst_arr, src.is_null(this));
    }

    truncate(ndx);
}

size_t ArrayObjectId::find_first(const ObjectId& value, size_t start, size_t end) const noexcept
{
    auto sz = size();
    if (end == size_t(-1))
        end = sz;
    REALM_ASSERT(start <= sz && end <= sz && start <= end);

    for (size_t i = start; i < end; i++) {
        const auto pos = get_pos(i);
        if (!pos.is_null(this) && pos.get_value(this) == value) {
            return i;
        }
    }
    return realm::npos;
}

size_t ArrayObjectIdNull::find_first_null(size_t start, size_t end) const
{
    auto sz = size();
    if (end == size_t(-1))
        end = sz;
    REALM_ASSERT(start <= sz && end <= sz && start <= end);

    auto ndx = start;
    auto bit_ptr = m_data + get_pos(ndx).base_byte;
    auto offset = ndx % 8;

    // Look at the bit vector at the start of each block.
    while (ndx < end) {
        const auto bit_vec = uint8_t(*bit_ptr >> offset);
        if (bit_vec) {
            ndx += ctz(bit_vec);
            return ndx >= end ? realm::npos : ndx;
        }

        ndx += 8 - offset;
        bit_ptr += s_block_size;
        offset = 0; // offset only used during first pass of loop.
    }

    return realm::npos;
}

} // namespace realm
