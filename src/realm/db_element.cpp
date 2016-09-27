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

#include <realm/db_element.hpp>
#include <iomanip>

using namespace realm;

void DbElement::init_from_mem(MemRef mem) noexcept
{
    char* header = mem.get_addr();
    // Parse header
    m_is_inner_bptree_node = get_is_inner_bptree_node_from_header(header);
    m_has_refs = get_hasrefs_from_header(header);
    m_context_flag = get_context_flag_from_header(header);
    m_width = get_width_from_header(header);
    m_ref = mem.get_ref();
    m_data = get_data_from_header(header);
    m_size = get_size_from_header(header);

    // Capacity is how many items there are room for
    bool is_read_only = m_alloc.is_read_only(mem.get_ref());
    if (is_read_only) {
        m_capacity = m_size;
    }
    else {
        size_t byte_capacity = get_capacity_from_header(header);
        // FIXME: Avoid calling virtual method calc_item_count() here,
        // instead calculate the capacity in a way similar to what is done
        // in get_byte_size_from_header(). The virtual call makes "life"
        // hard for constructors in derived array classes.
        m_capacity = calc_item_count(byte_capacity, m_width);
    }
}

bool DbElement::update_from_parent(size_t old_baseline) noexcept
{
    auto parent = get_parent();
    REALM_ASSERT_DEBUG(is_attached());
    REALM_ASSERT_DEBUG(parent);

    // Array nodes that are part of the previous version of the
    // database will not be overwritten by Group::commit(). This is
    // necessary for robustness in the face of abrupt termination of
    // the process. It also means that we can be sure that an array
    // remains unchanged across a commit if the new ref is equal to
    // the old ref and the ref is below the previous baseline.

    ref_type new_ref = parent->get_child_ref(get_ndx_in_parent());
    if (new_ref == m_ref && new_ref < old_baseline)
        return false; // Has not changed

    init_from_ref(new_ref);
    return true; // Might have changed
}

void DbElement::truncate(size_t new_size)
{
    REALM_ASSERT(is_attached());
    REALM_ASSERT_3(new_size, <=, m_size);

    copy_on_write(); // Throws

    // Update size in accessor and in header. This leaves the capacity
    // unchanged.
    m_size = new_size;
    set_header_size(new_size);
}

// FIXME: It may be worth trying to combine this with copy_on_write()
// to avoid two copies.
void DbElement::alloc(size_t init_size, size_t width)
{
    REALM_ASSERT(is_attached());
    REALM_ASSERT(!m_alloc.is_read_only(m_ref));
    REALM_ASSERT_3(m_capacity, >, 0);
    if (m_capacity < init_size || width != m_width) {
        size_t needed_bytes = calc_byte_len(init_size, width);

        // this method is not public and callers must (and currently do) ensure that
        // needed_bytes are never larger than max_array_payload.
        REALM_ASSERT_3(needed_bytes, <=, max_array_payload);

        char* header = get_header_from_data(m_data);
        size_t orig_capacity_bytes = get_capacity_from_header(header);
        size_t capacity_bytes = orig_capacity_bytes;

        if (capacity_bytes < needed_bytes) {
            // Double to avoid too many reallocs (or initialize to initial size), but truncate if that exceeds the
            // maximum allowed payload (measured in bytes) for arrays. This limitation is due to 24-bit capacity
            // field in the header.
            size_t new_capacity_bytes = capacity_bytes * 2;
            if (new_capacity_bytes < capacity_bytes) // overflow detected, clamp to max
                new_capacity_bytes = max_array_payload;
            if (new_capacity_bytes > max_array_payload) // cap at max allowed allocation
                new_capacity_bytes = max_array_payload;
            capacity_bytes = new_capacity_bytes;

            // If doubling is not enough, expand enough to fit
            if (capacity_bytes < needed_bytes) {
                size_t rest = (~needed_bytes & 0x7) + 1;
                capacity_bytes = needed_bytes;
                if (rest < 8)
                    capacity_bytes += rest; // 64bit align
            }

            // Allocate and update header
            MemRef mem_ref = m_alloc.realloc_(m_ref, header, orig_capacity_bytes, capacity_bytes); // Throws

            header = mem_ref.get_addr();
            set_header_width(int(width), header);
            set_header_size(init_size, header);
            set_header_capacity(capacity_bytes, header);

            // Update this accessor and its ancestors
            m_ref = mem_ref.get_ref();
            m_data = get_data_from_header(header);
            m_capacity = calc_item_count(capacity_bytes, width);
            // FIXME: Trouble when this one throws. We will then leave
            // this array instance in a corrupt state
            update_parent(); // Throws
            return;
        }

        m_capacity = calc_item_count(capacity_bytes, width);
        set_header_width(int(width));
    }

    // Update header
    set_header_size(init_size);
}

void DbElement::copy_on_write()
{
#if REALM_ENABLE_MEMDEBUG
    // We want to relocate this array regardless if there is a need or not, in order to catch use-after-free bugs.
    // Only exception is inside GroupWriter::write_group() (see explanation at the definition of the m_no_relocation
    // member)
    if (!m_no_relocation) {
#else
    if (m_alloc.is_read_only(m_ref)) {
#endif
        // Calculate size in bytes (plus a bit of matchcount room for expansion)
        size_t array_size = calc_byte_len(m_size, m_width);
        size_t rest = (~array_size & 0x7) + 1;
        if (rest < 8)
            array_size += rest; // 64bit blocks
        size_t new_size = array_size + 64;

        // Create new copy of array
        MemRef mref = m_alloc.alloc(new_size); // Throws
        const char* old_begin = get_header_from_data(m_data);
        const char* old_end = get_header_from_data(m_data) + array_size;
        char* new_begin = mref.get_addr();
        std::copy(old_begin, old_end, new_begin);

        ref_type old_ref = m_ref;

        // Update internal data
        m_ref = mref.get_ref();
        m_data = get_data_from_header(new_begin);
        m_capacity = calc_item_count(new_size, m_width);
        REALM_ASSERT_DEBUG(m_capacity > 0);

        // Update capacity in header. Uses m_data to find header, so
        // m_data must be initialized correctly first.
        set_header_capacity(new_size);

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
}

size_t DbElement::calc_byte_len(size_t num_items, size_t width) const
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

size_t DbElement::calc_item_count(size_t bytes, size_t width) const noexcept
{
    if (width == 0)
        return std::numeric_limits<size_t>::max(); // Zero width gives "infinite" space

    size_t bytes_data = bytes - header_size; // ignore 8 byte header
    size_t total_bits = bytes_data * 8;
    return total_bits / width;
}

// LCOV_EXCL_START ignore debug functions

#ifdef REALM_DEBUG

void DbElement::to_dot_parent_edge(std::ostream& out) const
{
    if (ArrayParent* parent = get_parent()) {
        size_t ndx_in_parent = get_ndx_in_parent();
        std::pair<ref_type, size_t> p = parent->get_to_dot_parent(ndx_in_parent);
        ref_type real_parent_ref = p.first;
        size_t ndx_in_real_parent = p.second;
        out << "n" << std::hex << real_parent_ref << std::dec << ":" << ndx_in_real_parent << " -> n" << std::hex
            << get_ref() << std::dec << std::endl;
    }
}

#endif // LCOV_EXCL_STOP ignore debug functions
