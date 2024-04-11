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

#include <realm/array_with_find.hpp>
#include <realm/utilities.hpp>
#include <realm/impl/destroy_guard.hpp>
#include <realm/column_integer.hpp>
#include <realm/bplustree.hpp>
#include <realm/query_conditions.hpp>
#include <realm/array_integer.hpp>
#include <realm/array_key.hpp>
#include <realm/impl/array_writer.hpp>

#include <array>
#include <cstring> // std::memcpy
#include <iomanip>
#include <limits>
#include <tuple>

#ifdef REALM_DEBUG
#include <iostream>
#include <sstream>
#endif

#ifdef _MSC_VER
#include <intrin.h>
#pragma warning(disable : 4127) // Condition is constant warning
#endif

// Header format (8 bytes):
// ------------------------
//
// In mutable part / outside file:
//
// |--------|--------|--------|--------|--------|--------|--------|--------|
// |         capacity         |reserved|12344555|           size           |
//
//
// In immutable part / in file:
//
// |--------|--------|--------|--------|--------|--------|--------|--------|
// |             checksum              |12344555|           size           |
//
//
//  1: 'is_inner_bptree_node' (inner node of B+-tree).
//
//  2: 'has_refs' (elements whose first bit is zero are refs to subarrays).
//
//  3: 'context_flag' (meaning depends on context)
//
//  4: 'width_scheme' (2 bits)
//
//      value  |  meaning of 'width'  |  number of bytes used after header
//      -------|----------------------|------------------------------------
//        0    |  number of bits      |  ceil(width * size / 8)
//        1    |  number of bytes     |  width * size
//        2    |  ignored             |  size
//
//  5: 'width_ndx' (3 bits)
//
//      'width_ndx'       |  0 |  1 |  2 |  3 |  4 |  5 |  6 |  7 |
//      ------------------|----|----|----|----|----|----|----|----|
//      value of 'width'  |  0 |  1 |  2 |  4 |  8 | 16 | 32 | 64 |
//
//
// 'capacity' is the total number of bytes allocated for this array
// including the header.
//
// 'size' (aka length) is the number of elements in the array.
//
// 'checksum' (not yet implemented) is the checksum of the array
// including the header.
//
//
// Inner node of B+-tree:
// ----------------------
//
// An inner node of a B+-tree is has one of two forms: The 'compact'
// form which uses a single array node, or the 'general' form which
// uses two. The compact form is used by default but is converted to
// the general form when the corresponding subtree is modified in
// certain ways. There are two kinds of modification that require
// conversion to the general form:
//
//  - Insertion of an element into the corresponding subtree, except
//    when insertion occurs after the last element in the subtree
//    (append).
//
//  - Removal of an element from the corresponding subtree, except
//    when the removed element is the last element in the subtree.
//
// Compact form:
//
//   --> | N_c | r_1 | r_2 | ... | r_N | N_t |
//
// General form:
//
//   --> |  .  | r_1 | r_2 | ... | r_N | N_t |  (main array node)
//          |
//           ------> | o_2 | ... | o_N |  (offsets array node)
//
// Here,
//   `r_i` is the i'th child ref,
//   `o_i` is the total number of elements preceeding the i'th child,
//   `N`   is the number of children,
//   'M'   is one less than the number of children,
//   `N_c` is the fixed number of elements per child
//         (`elems_per_child`), and
//   `N_t` is the total number of elements in the subtree
//         (`total_elems_in_subtree`).
//
// `N_c` must always be a power of `REALM_MAX_BPNODE_SIZE`.
//
// It is expected that `N_t` will be removed in a future version of
// the file format. This will make it much more efficient to append
// elements to the B+-tree (or remove elements from the end).
//
// The last child of an inner node on the compact form, may have fewer
// elements than `N_c`. All other children must have exactly `N_c`
// elements in them.
//
// When an inner node is on the general form, and has only one child,
// it has an empty `offsets` array.
//
//
// B+-tree invariants:
//
//  - Every inner node must have at least one child
//    (invar:bptree-nonempty-inner).
//
//  - A leaf node, that is not also a root node, must contain at least
//    one element (invar:bptree-nonempty-leaf).
//
//  - All leaf nodes must reside at the same depth in the tree
//    (invar:bptree-leaf-depth).
//
//  - If an inner node is on the general form, and has a parent, the
//    parent must also be on the general form
//    (invar:bptree-node-form).
//
// It follows from invar:bptree-nonempty-leaf that the root of an
// empty tree (zero elements) is a leaf.
//
// It follows from invar:bptree-nonempty-inner and
// invar:bptree-nonempty-leaf that in a tree with precisely one
// element, every inner node has precisely one child, there is
// precisely one leaf node, and that leaf node has precisely one
// element.
//
// It follows from invar:bptree-node-form that if the root is on the
// compact form, then so is every other inner node in the tree.
//
// In general, when the root node is an inner node, it will have at
// least two children, because otherwise it would be
// superflous. However, to allow for exception safety during element
// insertion and removal, this shall not be guaranteed.

// LIMITATION: The code below makes the non-portable assumption that
// negative number are represented using two's complement. This is not
// guaranteed by C++03, but holds for all known target platforms.
//
// LIMITATION: The code below makes the non-portable assumption that
// the types `int8_t`, `int16_t`, `int32_t`, and `int64_t`
// exist. This is not guaranteed by C++03, but holds for all
// known target platforms.
//
// LIMITATION: The code below makes the assumption that a reference into
// a realm file will never grow in size above what can be represented in
// a size_t, which is 2^31-1 on a 32-bit platform, and 2^63-1 on a 64 bit
// platform.

using namespace realm;
using namespace realm::util;

void QueryStateBase::dyncast() {}

Array::Array(Allocator& allocator) noexcept
    : Node(allocator)
{
}

size_t Array::bit_width(int64_t v)
{
    // FIXME: Assuming there is a 64-bit CPU reverse bitscan
    // instruction and it is fast, then this function could be
    // implemented as a table lookup on the result of the scan
    if ((uint64_t(v) >> 4) == 0) {
        static const int8_t bits[] = {0, 1, 2, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4};
        return bits[int8_t(v)];
    }
    if (v < 0)
        v = ~v;
    // Then check if bits 15-31 used (32b), 7-31 used (16b), else (8b)
    return uint64_t(v) >> 31 ? 64 : uint64_t(v) >> 15 ? 32 : uint64_t(v) >> 7 ? 16 : 8;
}

template <size_t width>
struct Array::VTableForWidth {
    struct PopulatedVTable : Array::VTable {
        PopulatedVTable()
        {
            getter = &Array::get<width>;
            setter = &Array::set<width>;
            chunk_getter = &Array::get_chunk<width>;
            finder[cond_Equal] = &Array::find_vtable<Equal>;
            finder[cond_NotEqual] = &Array::find_vtable<NotEqual>;
            finder[cond_Greater] = &Array::find_vtable<Greater>;
            finder[cond_Less] = &Array::find_vtable<Less>;
        }
    };
    static const PopulatedVTable vtable;
};

struct Array::VTableForEncodedArray {
    struct PopulatedVTableEncoded : Array::VTable {
        PopulatedVTableEncoded()
        {
            getter = &Array::get_from_compressed_array;
            setter = &Array::set_compressed_array;
            chunk_getter = &Array::get_chunk_compressed_array;
            finder[cond_Equal] = &Array::find_compressed_array<Equal>;
            finder[cond_NotEqual] = &Array::find_compressed_array<NotEqual>;
            finder[cond_Greater] = &Array::find_compressed_array<Greater>;
            finder[cond_Less] = &Array::find_compressed_array<Less>;
        }
    };
    static const PopulatedVTableEncoded vtable;
};

template <size_t width>
const typename Array::VTableForWidth<width>::PopulatedVTable Array::VTableForWidth<width>::vtable;
const typename Array::VTableForEncodedArray::PopulatedVTableEncoded Array::VTableForEncodedArray::vtable;

void Array::init_from_mem(MemRef mem) noexcept
{
    // Header is the type of header that has been allocated, in case we are decompressing,
    // the header is of kind A, which is kind of deceiving the purpose of these checks.
    // Since we will try to fetch some data from the just initialised header, and never reset
    // important fields used for type A arrays, like width, lower, upper_bound which are used
    // for expanding the array, but also query the data.
    char* header = mem.get_addr();
    const auto old_header = !NodeHeader::wtype_is_extended(header);
    // Cache all the header info as long as this array is alive and encoded.
    m_integer_compressor.init(header);
    if (!old_header) {
        REALM_ASSERT_DEBUG(NodeHeader::get_encoding(header) == Encoding::Flex ||
                           NodeHeader::get_encoding(header) == Encoding::Packed);
        char* header = mem.get_addr();
        m_ref = mem.get_ref();
        m_data = get_data_from_header(header);
        // encoder knows which format we are compressed into, width and size are read accordingly with the format of
        // the header
        m_size = m_integer_compressor.size();
        m_width = static_cast<uint_least8_t>(m_integer_compressor.width());
        m_lbound = -m_integer_compressor.width_mask();
        m_ubound = m_integer_compressor.width_mask() - 1;
        m_is_inner_bptree_node = get_is_inner_bptree_node_from_header(header);
        m_has_refs = get_hasrefs_from_header(header);
        m_context_flag = get_context_flag_from_header(header);
        m_vtable = &VTableForEncodedArray::vtable;
        m_getter = m_vtable->getter;
    }
    else {
        // Old init phase.
        header = Node::init_from_mem(mem);
        m_is_inner_bptree_node = get_is_inner_bptree_node_from_header(header);
        m_has_refs = get_hasrefs_from_header(header);
        m_context_flag = get_context_flag_from_header(header);
        update_width_cache_from_header();
    }
}

MemRef Array::get_mem() const noexcept
{
    return MemRef(get_header_from_data(m_data), m_ref, m_alloc);
}

void Array::update_from_parent() noexcept
{
    // checking the parent should have nothhing to do with m_data, decoding while updating the parent may be needed if
    // I am wrong. REALM_ASSERT_DEBUG(is_attached());
    ArrayParent* parent = get_parent();
    REALM_ASSERT_DEBUG(parent);
    ref_type new_ref = get_ref_from_parent();
    init_from_ref(new_ref);
}

void Array::set_type(Type type)
{
    REALM_ASSERT_DEBUG(is_attached());

    copy_on_write(); // Throws

    bool init_is_inner_bptree_node = false, init_has_refs = false;
    switch (type) {
        case type_Normal:
            break;
        case type_InnerBptreeNode:
            init_is_inner_bptree_node = true;
            init_has_refs = true;
            break;
        case type_HasRefs:
            init_has_refs = true;
            break;
    }
    m_is_inner_bptree_node = init_is_inner_bptree_node;
    m_has_refs = init_has_refs;

    char* header = get_header();
    set_is_inner_bptree_node_in_header(init_is_inner_bptree_node, header);
    set_hasrefs_in_header(init_has_refs, header);
}

void Array::destroy_children(size_t offset) noexcept
{
    for (size_t i = offset; i != m_size; ++i) {
        int64_t value = get(i);

        // Null-refs indicate empty sub-trees
        if (value == 0)
            continue;

        // A ref is always 8-byte aligned, so the lowest bit
        // cannot be set. If it is, it means that it should not be
        // interpreted as a ref.
        if ((value & 1) != 0)
            continue;

        ref_type ref = to_ref(value);
        destroy_deep(ref, m_alloc);
    }
}

// size_t Array::get_byte_size() const noexcept
//{
//     const auto header = get_header();
//     auto num_bytes = get_byte_size_from_header(header);
//     auto read_only = m_alloc.is_read_only(m_ref) == true;
//     auto capacity = get_capacity_from_header(header);
//     auto bytes_ok = num_bytes <= capacity;
//     REALM_ASSERT(read_only || bytes_ok);
//     REALM_ASSERT_7(m_alloc.is_read_only(m_ref), ==, true, ||, num_bytes, <=, get_capacity_from_header(header));
//     return num_bytes;
// }

ref_type Array::write(_impl::ArrayWriterBase& out, bool deep, bool only_if_modified, bool compress_in_flight) const
{
    REALM_ASSERT_DEBUG(is_attached());
    // The default allocator cannot be trusted wrt is_read_only():
    REALM_ASSERT_DEBUG(!only_if_modified || &m_alloc != &Allocator::get_default());
    if (only_if_modified && m_alloc.is_read_only(m_ref))
        return m_ref;

    if (!deep || !m_has_refs) {
        // however - creating an array using ANYTHING BUT the default allocator during commit is also wrong....
        // it only works by accident, because the whole slab area is reinitialized after commit.
        // We should have: Array encoded_array{Allocator::get_default()};
        Array compressed_array{Allocator::get_default()};
        if (compress_in_flight && size() != 0 && compress_array(compressed_array)) {
#ifdef REALM_DEBUG
            const auto encoding = compressed_array.m_integer_compressor.get_encoding();
            REALM_ASSERT_DEBUG(encoding == Encoding::Flex || encoding == Encoding::Packed);
            REALM_ASSERT_DEBUG(size() == compressed_array.size());
            for (size_t i = 0; i < compressed_array.size(); ++i) {
                REALM_ASSERT_DEBUG(get(i) == compressed_array.get(i));
            }
#endif
            auto ref = compressed_array.do_write_shallow(out);
            compressed_array.destroy();
            return ref;
        }
        return do_write_shallow(out); // Throws
    }

    return do_write_deep(out, only_if_modified, compress_in_flight); // Throws
}

ref_type Array::write(ref_type ref, Allocator& alloc, _impl::ArrayWriterBase& out, bool only_if_modified,
                      bool compress_in_flight)
{
    // The default allocator cannot be trusted wrt is_read_only():
    REALM_ASSERT_DEBUG(!only_if_modified || &alloc != &Allocator::get_default());
    if (only_if_modified && alloc.is_read_only(ref))
        return ref;

    Array array(alloc);
    array.init_from_ref(ref);
    REALM_ASSERT_DEBUG(array.is_attached());

    if (!array.m_has_refs) {
        Array compressed_array{Allocator::get_default()};
        if (compress_in_flight && array.size() != 0 && array.compress_array(compressed_array)) {
#ifdef REALM_DEBUG
            const auto encoding = compressed_array.m_integer_compressor.get_encoding();
            REALM_ASSERT_DEBUG(encoding == Encoding::Flex || encoding == Encoding::Packed);
            REALM_ASSERT_DEBUG(array.size() == compressed_array.size());
            for (size_t i = 0; i < compressed_array.size(); ++i) {
                REALM_ASSERT_DEBUG(array.get(i) == compressed_array.get(i));
            }
#endif
            auto ref = compressed_array.do_write_shallow(out);
            compressed_array.destroy();
            return ref;
        }
        else {
            return array.do_write_shallow(out); // Throws
        }
    }
    return array.do_write_deep(out, only_if_modified, compress_in_flight); // Throws
}


ref_type Array::do_write_shallow(_impl::ArrayWriterBase& out) const
{
    // here we might want to compress the array and write down.
    const char* header = get_header_from_data(m_data);
    size_t byte_size = get_byte_size();
    const auto compressed = is_compressed();
    uint32_t dummy_checksum = compressed ? 0x42424242UL : 0x41414141UL; //
    uint32_t dummy_checksum_bytes = compressed ? 2 : 4; // AAAA / BB (only 2 bytes for extended arrays)
    ref_type new_ref = out.write_array(header, byte_size, dummy_checksum, dummy_checksum_bytes); // Throws
    REALM_ASSERT_3(new_ref % 8, ==, 0);                                                          // 8-byte alignment
    return new_ref;
}


ref_type Array::do_write_deep(_impl::ArrayWriterBase& out, bool only_if_modified, bool compress) const
{
    // Temp array for updated refs
    Array new_array(Allocator::get_default());
    Type type = m_is_inner_bptree_node ? type_InnerBptreeNode : type_HasRefs;
    new_array.create(type, m_context_flag); // Throws
    _impl::ShallowArrayDestroyGuard dg(&new_array);

    // First write out all sub-arrays
    size_t n = size();
    for (size_t i = 0; i < n; ++i) {
        int_fast64_t value = get(i);
        bool is_ref = (value != 0 && (value & 1) == 0);
        if (is_ref) {
            ref_type subref = to_ref(value);
            ref_type new_subref = write(subref, m_alloc, out, only_if_modified, compress); // Throws
            value = from_ref(new_subref);
        }
        new_array.add(value); // Throws
    }
    return new_array.do_write_shallow(out); // Throws
}


void Array::move(size_t begin, size_t end, size_t dest_begin)
{
    REALM_ASSERT_3(begin, <=, end);
    REALM_ASSERT_3(end, <=, m_size);
    REALM_ASSERT_3(dest_begin, <=, m_size);
    REALM_ASSERT_3(end - begin, <=, m_size - dest_begin);
    REALM_ASSERT(!(dest_begin >= begin && dest_begin < end)); // Required by std::copy

    // Check if we need to copy before modifying
    copy_on_write(); // Throws

    size_t bits_per_elem = m_width;
    const char* header = get_header_from_data(m_data);
    if (get_wtype_from_header(header) == wtype_Multiply) {
        bits_per_elem *= 8;
    }

    if (bits_per_elem < 8) {
        // FIXME: Should be optimized
        for (size_t i = begin; i != end; ++i) {
            int_fast64_t v = (this->*m_getter)(i);
            (this->*(m_vtable->setter))(dest_begin++, v);
        }
        return;
    }

    size_t bytes_per_elem = bits_per_elem / 8;
    const char* begin_2 = m_data + begin * bytes_per_elem;
    const char* end_2 = m_data + end * bytes_per_elem;
    char* dest_begin_2 = m_data + dest_begin * bytes_per_elem;
    realm::safe_copy_n(begin_2, end_2 - begin_2, dest_begin_2);
}

void Array::move(Array& dst, size_t ndx)
{
    size_t dest_begin = dst.m_size;
    size_t nb_to_move = m_size - ndx;
    dst.copy_on_write();
    dst.ensure_minimum_width(this->m_ubound);
    dst.alloc(dst.m_size + nb_to_move, dst.m_width); // Make room for the new elements

    // cache variables used in tight loop
    auto getter = m_getter;
    auto setter = dst.m_vtable->setter;
    size_t sz = m_size;

    for (size_t i = ndx; i < sz; i++) {
        auto v = (this->*getter)(i);
        (dst.*setter)(dest_begin++, v);
    }

    truncate(ndx);
}

void Array::set(size_t ndx, int64_t value)
{
    REALM_ASSERT_3(ndx, <, m_size);
    if ((this->*(m_vtable->getter))(ndx) == value)
        return;

    // Check if we need to copy before modifying
    copy_on_write(); // Throws
    // Grow the array if needed to store this value
    ensure_minimum_width(value); // Throws
    // Set the value
    (this->*(m_vtable->setter))(ndx, value);
}

void Array::set_as_ref(size_t ndx, ref_type ref)
{
    set(ndx, from_ref(ref));
}

/*
// Optimization for the common case of adding positive values to a local array
// (happens a lot when returning results to TableViews)
void Array::add_positive_local(int64_t value)
{
    REALM_ASSERT(value >= 0);
    REALM_ASSERT(&m_alloc == &Allocator::get_default());

    if (value <= m_ubound) {
        if (m_size < m_capacity) {
            (this->*(m_vtable->setter))(m_size, value);
            ++m_size;
            set_header_size(m_size);
            return;
        }
    }

    insert(m_size, value);
}
*/

size_t Array::blob_size() const noexcept
{
    if (get_context_flag()) {
        size_t total_size = 0;
        for (size_t i = 0; i < size(); ++i) {
            char* header = m_alloc.translate(Array::get_as_ref(i));
            total_size += Array::get_size_from_header(header);
        }
        return total_size;
    }
    else {
        return m_size;
    }
}

void Array::insert(size_t ndx, int_fast64_t value)
{
    REALM_ASSERT_DEBUG(ndx <= m_size);

    decompress_array(*this);
    const auto old_width = m_width;
    const auto old_size = m_size;
    const Getter old_getter = m_getter; // Save old getter before potential width expansion

    bool do_expand = value < m_lbound || value > m_ubound;
    if (do_expand) {
        size_t width = bit_width(value);
        REALM_ASSERT_DEBUG(width > m_width);
        alloc(m_size + 1, width); // Throws
    }
    else {
        alloc(m_size + 1, m_width); // Throws
    }

    // Move values below insertion (may expand)
    if (do_expand || old_width < 8) {
        size_t i = old_size;
        while (i > ndx) {
            --i;
            int64_t v = (this->*old_getter)(i);
            (this->*(m_vtable->setter))(i + 1, v);
        }
    }
    else if (ndx != old_size) {
        // when byte sized and no expansion, use memmove
        // FIXME: Optimize by simply dividing by 8 (or shifting right by 3 bit positions)
        size_t w = (old_width == 64) ? 8 : (old_width == 32) ? 4 : (old_width == 16) ? 2 : 1;
        char* src_begin = m_data + ndx * w;
        char* src_end = m_data + old_size * w;
        char* dst_end = src_end + w;
        std::copy_backward(src_begin, src_end, dst_end);
    }

    // Insert the new value
    (this->*(m_vtable->setter))(ndx, value);

    // Expand values above insertion
    if (do_expand) {
        size_t i = ndx;
        while (i != 0) {
            --i;
            int64_t v = (this->*old_getter)(i);
            (this->*(m_vtable->setter))(i, v);
        }
    }
}

void Array::copy_on_write()
{
    if (is_read_only() && !decompress_array(*this))
        Node::copy_on_write();
}

void Array::copy_on_write(size_t min_size)
{
    if (is_read_only() && !decompress_array(*this))
        Node::copy_on_write(min_size);
}

void Array::truncate(size_t new_size)
{
    REALM_ASSERT(is_attached());
    REALM_ASSERT_3(new_size, <=, m_size);

    if (new_size == m_size)
        return;

    copy_on_write(); // Throws

    // Update size in accessor and in header. This leaves the capacity
    // unchanged.
    m_size = new_size;
    set_header_size(new_size);

    // If the array is completely cleared, we take the opportunity to
    // drop the width back to zero.
    if (new_size == 0) {
        set_width_in_header(0, get_header());
        update_width_cache_from_header();
    }
}

void Array::truncate_and_destroy_children(size_t new_size)
{
    REALM_ASSERT(is_attached());
    REALM_ASSERT_3(new_size, <=, m_size);

    if (new_size == m_size)
        return;

    copy_on_write(); // Throws

    if (m_has_refs) {
        size_t offset = new_size;
        destroy_children(offset);
    }

    // Update size in accessor and in header. This leaves the capacity
    // unchanged.
    m_size = new_size;
    set_header_size(new_size);

    // If the array is completely cleared, we take the opportunity to
    // drop the width back to zero.
    if (new_size == 0) {
        set_width_in_header(0, get_header());
        update_width_cache_from_header();
    }
}

void Array::do_ensure_minimum_width(int_fast64_t value)
{
    // Make room for the new value
    const size_t width = bit_width(value);

    REALM_ASSERT_3(width, >, m_width);

    Getter old_getter = m_getter; // Save old getter before width expansion
    alloc(m_size, width);         // Throws

    // Expand the old values
    size_t i = m_size;
    while (i != 0) {
        --i;
        int64_t v = (this->*old_getter)(i);
        (this->*(m_vtable->setter))(i, v);
    }
}

size_t Array::size() const noexcept
{
    // in case the array is in compressed format. Never read directly
    // from the header the size, since it will result very likely in a cache miss.
    // For compressed arrays m_size should always be kept updated, due to init_from_mem
    return m_size;
}

bool Array::compress_array(Array& arr) const
{
    if (!is_compressed() && m_integer_compressor.get_encoding() == NodeHeader::Encoding::WTypBits) {
        return m_integer_compressor.compress(*this, arr);
    }
    return false;
}

bool Array::decompress_array(Array& arr) const
{
    return arr.is_compressed() ? m_integer_compressor.decompress(arr) : false;
}

bool Array::try_compress(Array& arr) const
{
    return compress_array(arr);
}

bool Array::try_decompress()
{
    return decompress_array(*this);
}

int64_t Array::get_from_compressed_array(size_t ndx) const noexcept
{
    return m_integer_compressor.get(ndx);
}

void Array::set_compressed_array(size_t ndx, int64_t val)
{
    m_integer_compressor.set_direct(ndx, val);
}

void Array::get_chunk_compressed_array(size_t ndx, int64_t res[8]) const noexcept
{
    m_integer_compressor.get_chunk(ndx, res);
}

size_t Array::calc_aligned_byte_size(size_t size, int width)
{
    REALM_ASSERT(width != 0 && (width & (width - 1)) == 0); // Is a power of two
    size_t max = std::numeric_limits<size_t>::max();
    size_t max_2 = max & ~size_t(7); // Allow for upwards 8-byte alignment
    bool overflow;
    size_t byte_size;
    if (width < 8) {
        size_t elems_per_byte = 8 / width;
        size_t byte_size_0 = size / elems_per_byte;
        if (size % elems_per_byte != 0)
            ++byte_size_0;
        overflow = byte_size_0 > max_2 - header_size;
        byte_size = header_size + byte_size_0;
    }
    else {
        size_t bytes_per_elem = width / 8;
        overflow = size > (max_2 - header_size) / bytes_per_elem;
        byte_size = header_size + size * bytes_per_elem;
    }
    if (overflow)
        throw std::overflow_error("Byte size overflow");
    REALM_ASSERT_3(byte_size, >, 0);
    size_t aligned_byte_size = ((byte_size - 1) | 7) + 1; // 8-byte alignment
    return aligned_byte_size;
}

MemRef Array::clone(MemRef mem, Allocator& alloc, Allocator& target_alloc)
{
    const char* header = mem.get_addr();
    if (!get_hasrefs_from_header(header)) {
        // This array has no subarrays, so we can make a byte-for-byte
        // copy, which is more efficient.

        // Calculate size of new array in bytes
        size_t size = get_byte_size_from_header(header);

        // Create the new array
        MemRef clone_mem = target_alloc.alloc(size); // Throws
        char* clone_header = clone_mem.get_addr();

        // Copy contents
        const char* src_begin = header;
        const char* src_end = header + size;
        char* dst_begin = clone_header;
        realm::safe_copy_n(src_begin, src_end - src_begin, dst_begin);

        // Update with correct capacity
        set_capacity_in_header(size, clone_header);

        return clone_mem;
    }

    // Refs are integers, and integers arrays use wtype_Bits.
    REALM_ASSERT_3(get_wtype_from_header(header), ==, wtype_Bits);

    Array array{alloc};
    array.init_from_mem(mem);

    // Create new empty array of refs
    Array new_array(target_alloc);
    _impl::DeepArrayDestroyGuard dg(&new_array);
    Type type = get_type_from_header(header);
    bool context_flag = get_context_flag_from_header(header);
    new_array.create(type, context_flag); // Throws

    _impl::DeepArrayRefDestroyGuard dg_2(target_alloc);
    size_t n = array.size();
    for (size_t i = 0; i != n; ++i) {
        int_fast64_t value = array.get(i);

        // Null-refs signify empty subtrees. Also, all refs are
        // 8-byte aligned, so the lowest bits cannot be set. If they
        // are, it means that it should not be interpreted as a ref.
        bool is_subarray = value != 0 && (value & 1) == 0;
        if (!is_subarray) {
            new_array.add(value); // Throws
            continue;
        }

        ref_type ref = to_ref(value);
        MemRef new_mem = clone(MemRef(ref, alloc), alloc, target_alloc); // Throws
        dg_2.reset(new_mem.get_ref());
        value = from_ref(new_mem.get_ref());
        new_array.add(value); // Throws
        dg_2.release();
    }

    dg.release();
    return new_array.get_mem();
}

MemRef Array::create(Type type, bool context_flag, WidthType width_type, size_t size, int_fast64_t value,
                     Allocator& alloc)
{
    REALM_ASSERT_DEBUG(value == 0 || width_type == wtype_Bits);
    REALM_ASSERT_DEBUG(size == 0 || width_type != wtype_Ignore);
    int width = 0;
    if (value != 0)
        width = static_cast<int>(bit_width(value));
    auto mem = Node::create_node(size, alloc, context_flag, type, width_type, width);
    if (value != 0) {
        const auto header = mem.get_addr();
        char* data = get_data_from_header(header);
        size_t begin = 0, end = size;
        REALM_TEMPEX(fill_direct, width, (data, begin, end, value));
    }
    return mem;
}

// This is the one installed into the m_vtable->finder slots.
template <class cond>
bool Array::find_vtable(int64_t value, size_t start, size_t end, size_t baseindex, QueryStateBase* state) const
{
    REALM_TEMPEX2(return ArrayWithFind(*this).find_optimized, cond, m_width, (value, start, end, baseindex, state));
}

template <class cond>
bool Array::find_compressed_array(int64_t value, size_t start, size_t end, size_t baseindex,
                                  QueryStateBase* state) const
{
    return m_integer_compressor.find_all<cond>(*this, value, start, end, baseindex, state);
}

void Array::update_width_cache_from_header() noexcept
{
    m_width = get_width_from_header(get_header());
    m_lbound = lbound_for_width(m_width);
    m_ubound = ubound_for_width(m_width);
    REALM_ASSERT_DEBUG(m_lbound <= m_ubound);
    REALM_ASSERT_DEBUG(m_width >= m_lbound);
    REALM_ASSERT_DEBUG(m_width <= m_ubound);
    REALM_TEMPEX(m_vtable = &VTableForWidth, m_width, ::vtable);
    m_getter = m_vtable->getter;
}

// This method reads 8 concecutive values into res[8], starting from index 'ndx'. It's allowed for the 8 values to
// exceed array length; in this case, remainder of res[8] will be be set to 0.
template <size_t w>
void Array::get_chunk(size_t ndx, int64_t res[8]) const noexcept
{
    REALM_ASSERT_3(ndx, <, m_size);
    size_t i = 0;

    // if constexpr to avoid producing spurious warnings resulting from
    // instantiating for too large w
    if constexpr (w > 0 && w <= 4) {
        // Calling get<w>() in a loop results in one load per call to get, but
        // for w < 8 we can do better than that
        constexpr size_t elements_per_byte = 8 / w;

        // Round m_size down to byte granularity as the trailing bits in the last
        // byte are uninitialized
        size_t bytes_available = m_size / elements_per_byte;

        // Round start and end to be byte-aligned. Start is rounded down and
        // end is rounded up as we may read up to 7 unused bits at each end.
        size_t start = ndx / elements_per_byte;
        size_t end = std::min(bytes_available, (ndx + 8 + elements_per_byte - 1) / elements_per_byte);

        if (end > start) {
            // Loop in reverse order because data is stored in little endian order
            uint64_t c = 0;
            for (size_t i = end; i > start; --i) {
                c <<= 8;
                c += *reinterpret_cast<const uint8_t*>(m_data + i - 1);
            }
            // Trim off leading bits which aren't part of the requested range
            c >>= (ndx - start * elements_per_byte) * w;

            uint64_t mask = (1ULL << w) - 1ULL;
            res[0] = (c >> 0 * w) & mask;
            res[1] = (c >> 1 * w) & mask;
            res[2] = (c >> 2 * w) & mask;
            res[3] = (c >> 3 * w) & mask;
            res[4] = (c >> 4 * w) & mask;
            res[5] = (c >> 5 * w) & mask;
            res[6] = (c >> 6 * w) & mask;
            res[7] = (c >> 7 * w) & mask;

            // Read the last few elements via get<w> if needed
            i = std::min<size_t>(8, end * elements_per_byte - ndx);
        }
    }

    for (; i + ndx < m_size && i < 8; i++)
        res[i] = get<w>(ndx + i);
    for (; i < 8; i++)
        res[i] = 0;

#ifdef REALM_DEBUG
    for (int j = 0; j + ndx < m_size && j < 8; j++) {
        int64_t expected = get<w>(ndx + j);
        REALM_ASSERT(res[j] == expected);
    }
#endif
}

template <>
void Array::get_chunk<0>(size_t ndx, int64_t res[8]) const noexcept
{
    REALM_ASSERT_3(ndx, <, m_size);
    memset(res, 0, sizeof(int64_t) * 8);
}


template <size_t width>
void Array::set(size_t ndx, int64_t value)
{
    realm::set_direct<width>(m_data, ndx, value);
}

void Array::_mem_usage(size_t& mem) const noexcept
{
    mem += get_byte_size();
    if (m_has_refs) {
        for (size_t i = 0; i < m_size; ++i) {
            int64_t val = get(i);
            if (val && !(val & 1)) {
                Array subarray(m_alloc);
                subarray.init_from_ref(to_ref(val));
                subarray._mem_usage(mem);
            }
        }
    }
}

#ifdef REALM_DEBUG
namespace {

class MemStatsHandler : public Array::MemUsageHandler {
public:
    MemStatsHandler(MemStats& stats) noexcept
        : m_stats(stats)
    {
    }
    void handle(ref_type, size_t allocated, size_t used) noexcept override
    {
        m_stats.allocated += allocated;
        m_stats.used += used;
        m_stats.array_count += 1;
    }

private:
    MemStats& m_stats;
};

} // anonymous namespace


void Array::stats(MemStats& stats_dest) const noexcept
{
    MemStatsHandler handler(stats_dest);
    report_memory_usage(handler);
}


void Array::report_memory_usage(MemUsageHandler& handler) const
{
    if (m_has_refs)
        report_memory_usage_2(handler); // Throws

    size_t used = get_byte_size();
    size_t allocated;
    if (m_alloc.is_read_only(m_ref)) {
        allocated = used;
    }
    else {
        char* header = get_header_from_data(m_data);
        allocated = get_capacity_from_header(header);
    }
    handler.handle(m_ref, allocated, used); // Throws
}


void Array::report_memory_usage_2(MemUsageHandler& handler) const
{
    Array subarray(m_alloc);
    for (size_t i = 0; i < m_size; ++i) {
        int_fast64_t value = get(i);
        // Skip null refs and values that are not refs. Values are not refs when
        // the least significant bit is set.
        if (value == 0 || (value & 1) == 1)
            continue;

        size_t used;
        ref_type ref = to_ref(value);
        char* header = m_alloc.translate(ref);
        bool array_has_refs = get_hasrefs_from_header(header);
        if (array_has_refs) {
            MemRef mem(header, ref, m_alloc);
            subarray.init_from_mem(mem);
            subarray.report_memory_usage_2(handler); // Throws
            used = subarray.get_byte_size();
        }
        else {
            used = get_byte_size_from_header(header);
        }

        size_t allocated;
        if (m_alloc.is_read_only(ref)) {
            allocated = used;
        }
        else {
            allocated = get_capacity_from_header(header);
        }
        handler.handle(ref, allocated, used); // Throws
    }
}
#endif

void Array::verify() const
{
#ifdef REALM_DEBUG

    REALM_ASSERT(is_attached());
    if (!wtype_is_extended(get_header())) {
        REALM_ASSERT(m_width == 0 || m_width == 1 || m_width == 2 || m_width == 4 || m_width == 8 || m_width == 16 ||
                     m_width == 32 || m_width == 64);
    }
    else {
        REALM_ASSERT(m_width <= 64);
    }

    if (!get_parent())
        return;

    // Check that parent is set correctly
    ref_type ref_in_parent = get_ref_from_parent();
    REALM_ASSERT_3(ref_in_parent, ==, m_ref);
#endif
}

size_t Array::lower_bound_int(int64_t value) const noexcept
{
    if (is_compressed())
        return lower_bound_int_compressed(value);
    REALM_TEMPEX(return lower_bound, m_width, (m_data, m_size, value));
}

size_t Array::upper_bound_int(int64_t value) const noexcept
{
    if (is_compressed())
        return upper_bound_int_compressed(value);
    REALM_TEMPEX(return upper_bound, m_width, (m_data, m_size, value));
}

size_t Array::lower_bound_int_compressed(int64_t value) const noexcept
{
    static impl::EncodedFetcher<IntegerCompressor> encoder;
    encoder.ptr = &m_integer_compressor;
    return lower_bound(m_data, m_size, value, encoder);
}

size_t Array::upper_bound_int_compressed(int64_t value) const noexcept
{
    static impl::EncodedFetcher<IntegerCompressor> encoder;
    encoder.ptr = &m_integer_compressor;
    return upper_bound(m_data, m_size, value, encoder);
}

int_fast64_t Array::get(const char* header, size_t ndx) noexcept
{
    if (wtype_is_extended(header)) {
        static IntegerCompressor compressor;
        compressor.init(header);
        return compressor.get(ndx);
    }

    auto sz = get_size_from_header(header);
    REALM_ASSERT(ndx < sz);
    const char* data = get_data_from_header(header);
    uint_least8_t width = get_width_from_header(header);
    return get_direct(data, width, ndx);
}

std::pair<int64_t, int64_t> Array::get_two(const char* header, size_t ndx) noexcept
{
    return std::make_pair(get(header, ndx), get(header, ndx + 1));
}

bool QueryStateCount::match(size_t, Mixed) noexcept
{
    ++m_match_count;
    return (m_limit > m_match_count);
}

bool QueryStateCount::match(size_t) noexcept
{
    ++m_match_count;
    return (m_limit > m_match_count);
}

bool QueryStateFindFirst::match(size_t index, Mixed) noexcept
{
    m_match_count++;
    m_state = index;
    return false;
}

bool QueryStateFindFirst::match(size_t index) noexcept
{
    ++m_match_count;
    m_state = index;
    return false;
}

template <>
bool QueryStateFindAll<std::vector<ObjKey>>::match(size_t index, Mixed) noexcept
{
    ++m_match_count;

    int64_t key_value = (m_key_values ? m_key_values->get(index) : index) + m_key_offset;
    m_keys.push_back(ObjKey(key_value));

    return (m_limit > m_match_count);
}

template <>
bool QueryStateFindAll<std::vector<ObjKey>>::match(size_t index) noexcept
{
    ++m_match_count;
    int64_t key_value = (m_key_values ? m_key_values->get(index) : index) + m_key_offset;
    m_keys.push_back(ObjKey(key_value));
    return (m_limit > m_match_count);
}

template <>
bool QueryStateFindAll<IntegerColumn>::match(size_t index, Mixed) noexcept
{
    ++m_match_count;
    m_keys.add(index);

    return (m_limit > m_match_count);
}

template <>
bool QueryStateFindAll<IntegerColumn>::match(size_t index) noexcept
{
    ++m_match_count;
    m_keys.add(index);

    return (m_limit > m_match_count);
}

void Array::typed_print(std::string prefix) const
{
    std::cout << "Generic Array " << header_to_string(get_header()) << " @ " << m_ref;
    if (!is_attached()) {
        std::cout << " Unattached";
        return;
    }
    if (size() == 0) {
        std::cout << " Empty" << std::endl;
        return;
    }
    std::cout << " size = " << size() << " {";
    if (has_refs()) {
        std::cout << std::endl;
        for (unsigned n = 0; n < size(); ++n) {
            auto pref = prefix + "  " + to_string(n) + ":\t";
            RefOrTagged rot = get_as_ref_or_tagged(n);
            if (rot.is_ref() && rot.get_as_ref()) {
                Array a(m_alloc);
                a.init_from_ref(rot.get_as_ref());
                std::cout << pref;
                a.typed_print(pref);
            }
            else if (rot.is_tagged()) {
                std::cout << pref << rot.get_as_int() << std::endl;
            }
        }
        std::cout << prefix << "}" << std::endl;
    }
    else {
        std::cout << " Leaf of unknown type }" << std::endl;
        /*
        for (unsigned n = 0; n < size(); ++n) {
            auto pref = prefix + to_string(n) + ":\t";
            std::cout << pref << get(n) << std::endl;
        }
        */
    }
}
