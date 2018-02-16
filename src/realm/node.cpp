/*************************************************************************
 *
 * Copyright 2018 Realm Inc.
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

#include <realm/node.hpp>
#include <realm/utilities.hpp>

using namespace realm;

MemRef Node::create_element(size_t size, Allocator& alloc, bool context_flag, Type type, WidthType width_type,
                            int width)
{
    size_t byte_size_0 = calc_byte_size(width_type, size, width);
    size_t byte_size = std::max(byte_size_0, size_t(initial_capacity));

    MemRef mem = alloc.alloc(byte_size); // Throws
    char* header = mem.get_addr();

    init_header(header, type == type_InnerBptreeNode, type != type_Normal, context_flag, width_type, width, size,
                byte_size);

    return mem;
}

size_t Node::calc_byte_len(size_t num_items, size_t width) const
{
    REALM_ASSERT_3(get_wtype_from_header(get_header_from_data(m_data)), ==, wtype_Bits);

    // FIXME: Consider calling `calc_aligned_byte_size(size)`
    // instead. Note however, that calc_byte_len() is supposed to return
    // the unaligned byte size. It is probably the case that no harm
    // is done by returning the aligned version, and most callers of
    // calc_byte_len() will actually benefit if calc_byte_len() was
    // changed to always return the aligned byte size.

    size_t bits = num_items * width;
    size_t bytes = (bits + 7) / 8; // round up
    return bytes + header_size;    // add room for 8 byte header
}

size_t Node::calc_item_count(size_t bytes, size_t width) const noexcept
{
    if (width == 0)
        return std::numeric_limits<size_t>::max(); // Zero width gives "infinite" space

    size_t bytes_data = bytes - header_size; // ignore 8 byte header
    size_t total_bits = bytes_data * 8;
    return total_bits / width;
}

void Node::alloc(size_t init_size, size_t new_width)
{
    REALM_ASSERT(is_attached());

    size_t needed_bytes = calc_byte_len(init_size, new_width);
    // this method is not public and callers must (and currently do) ensure that
    // needed_bytes are never larger than max_array_payload.
    REALM_ASSERT_3(needed_bytes, <=, max_array_payload);

    if (is_read_only())
        do_copy_on_write(needed_bytes);

    REALM_ASSERT(!m_alloc.is_read_only(m_ref));
    char* header = get_header_from_data(m_data);
    size_t orig_capacity_bytes = get_capacity_from_header(header);
    size_t orig_width = get_width_from_header(header);

    if (orig_capacity_bytes < needed_bytes) {
        // Double to avoid too many reallocs (or initialize to initial size), but truncate if that exceeds the
        // maximum allowed payload (measured in bytes) for arrays. This limitation is due to 24-bit capacity
        // field in the header.
        size_t new_capacity_bytes = orig_capacity_bytes * 2;
        if (new_capacity_bytes < orig_capacity_bytes) // overflow detected, clamp to max
            new_capacity_bytes = max_array_payload_aligned;
        if (new_capacity_bytes > max_array_payload_aligned) // cap at max allowed allocation
            new_capacity_bytes = max_array_payload_aligned;

        // If doubling is not enough, expand enough to fit
        if (new_capacity_bytes < needed_bytes) {
            size_t rest = (~needed_bytes & 0x7) + 1;
            new_capacity_bytes = needed_bytes;
            if (rest < 8)
                new_capacity_bytes += rest; // 64bit align
        }

        // Allocate and update header
        MemRef mem_ref = m_alloc.realloc_(m_ref, header, orig_capacity_bytes, new_capacity_bytes); // Throws

        header = mem_ref.get_addr();
        set_header_capacity(new_capacity_bytes, header);

        // Update this accessor and its ancestors
        m_ref = mem_ref.get_ref();
        m_data = get_data_from_header(header);
        // FIXME: Trouble when this one throws. We will then leave
        // this array instance in a corrupt state
        update_parent(); // Throws
    }

    // Update header
    if (new_width != orig_width) {
        set_header_width(int(new_width), header);
    }
    set_header_size(init_size, header);
}

void Node::do_copy_on_write(size_t minimum_size)
{
    const char* header = get_header_from_data(m_data);

    // Calculate size in bytes
    size_t array_size = calc_byte_len(m_size, m_width);
    size_t new_size = std::max(array_size, minimum_size);
    new_size = (new_size + 0x7) & ~size_t(0x7); // 64bit blocks
    // Plus a bit of matchcount room for expansion
    if (new_size < max_array_payload - 64)
        new_size += 64;

    // Create new copy of array
    MemRef mref = m_alloc.alloc(new_size); // Throws
    const char* old_begin = header;
    const char* old_end = header + array_size;
    char* new_begin = mref.get_addr();
    realm::safe_copy_n(old_begin, old_end - old_begin, new_begin);

    ref_type old_ref = m_ref;

    // Update internal data
    m_ref = mref.get_ref();
    m_data = get_data_from_header(new_begin);

    // Update capacity in header. Uses m_data to find header, so
    // m_data must be initialized correctly first.
    set_header_capacity(new_size, new_begin);

    update_parent();

#if REALM_ENABLE_MEMDEBUG
    if (!m_alloc.is_read_only(old_ref)) {
        // Overwrite free'd array with 0x77. We cannot overwrite the header because free_() needs to know the size
        // of the allocated block in order to free it. This size is computed from the width and size header
        // fields.
        memset(const_cast<char*>(old_begin) + header_size, 0x77, old_end - old_begin - header_size);
    }
#endif

    // Mark original as deleted, so that the space can be reclaimed in
    // future commits, when no versions are using it anymore
    m_alloc.free_(old_ref, old_begin);
}
