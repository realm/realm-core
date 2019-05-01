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

#include <realm/utilities.hpp>
#include <realm/array.hpp>
#include <realm/array_basic.hpp>
#include <realm/impl/destroy_guard.hpp>
#include <realm/column_integer.hpp>
#include <realm/bplustree.hpp>
#include <realm/query_conditions.hpp>
#include <realm/index_string.hpp>
#include <realm/array_integer.hpp>
#include <realm/array_key.hpp>
#include <realm/impl/array_writer.hpp>


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

void QueryStateBase::dyncast()
{
}

ArrayPayload::~ArrayPayload()
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

    // First flip all bits if bit 63 is set (will now always be zero)
    if (v < 0)
        v = ~v;

    // Then check if bits 15-31 used (32b), 7-31 used (16b), else (8b)
    return uint64_t(v) >> 31 ? 64 : uint64_t(v) >> 15 ? 32 : uint64_t(v) >> 7 ? 16 : 8;
}


void Array::init_from_mem(MemRef mem) noexcept
{
    char* header = Node::init_from_mem(mem);
    // Parse header
    m_is_inner_bptree_node = get_is_inner_bptree_node_from_header(header);
    m_has_refs = get_hasrefs_from_header(header);
    m_context_flag = get_context_flag_from_header(header);

    set_width(m_width);
}

bool Array::update_from_parent(size_t old_baseline) noexcept
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

void Array::set_type(Type type)
{
    REALM_ASSERT(is_attached());

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


ref_type Array::do_write_shallow(_impl::ArrayWriterBase& out) const
{
    // Write flat array
    const char* header = get_header_from_data(m_data);
    size_t byte_size = get_byte_size();
    uint32_t dummy_checksum = 0x41414141UL;                                // "AAAA" in ASCII
    ref_type new_ref = out.write_array(header, byte_size, dummy_checksum); // Throws
    REALM_ASSERT_3(new_ref % 8, ==, 0);                                    // 8-byte alignment
    return new_ref;
}


ref_type Array::do_write_deep(_impl::ArrayWriterBase& out, bool only_if_modified) const
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
            ref_type new_subref = write(subref, m_alloc, out, only_if_modified); // Throws
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
    size_t nb_to_move = m_size - ndx;
    dst.copy_on_write();
    dst.ensure_minimum_width(this->m_ubound);
    dst.alloc(dst.m_size + nb_to_move, dst.m_width); // Make room for the new elements

    // cache variables used in tight loop
    size_t dest_begin = dst.m_size;
    auto getter = m_getter;
    auto setter = dst.m_vtable->setter;
    size_t sz = m_size;

    for (size_t i = ndx; i < sz; i++) {
        auto v = (this->*getter)(i);
        (dst.*setter)(dest_begin++, v);
    }

    dst.m_size += nb_to_move;
    truncate(ndx);
}

void Array::add_to_column(IntegerColumn* column, int64_t value)
{
    column->add(value);
}

void Array::add_to_column(KeyColumn* column, int64_t value)
{
    column->add(ObjKey(value));
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


    Getter old_getter = m_getter; // Save old getter before potential width expansion

    bool do_expand = value < m_lbound || value > m_ubound;
    if (do_expand) {
        size_t width = bit_width(value);
        REALM_ASSERT_DEBUG(width > m_width);
        alloc(m_size + 1, width); // Throws
        set_width(width);
    }
    else {
        alloc(m_size + 1, m_width); // Throws
    }

    // Move values below insertion (may expand)
    if (do_expand || m_width < 8) {
        size_t i = m_size;
        while (i > ndx) {
            --i;
            int64_t v = (this->*old_getter)(i);
            (this->*(m_vtable->setter))(i + 1, v);
        }
    }
    else if (ndx != m_size) {
        // when byte sized and no expansion, use memmove
        // FIXME: Optimize by simply dividing by 8 (or shifting right by 3 bit positions)
        size_t w = (m_width == 64) ? 8 : (m_width == 32) ? 4 : (m_width == 16) ? 2 : 1;
        char* src_begin = m_data + ndx * w;
        char* src_end = m_data + m_size * w;
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

    // Update size
    // (no need to do it in header as it has been done by Alloc)
    ++m_size;
}


void Array::truncate(size_t new_size)
{
    REALM_ASSERT(is_attached());
    REALM_ASSERT_3(new_size, <=, m_size);

    // FIXME: BasicArray<> currently does not work if the width is set
    // to zero, so it must override Array::truncate(). In the future
    // it is expected that BasicArray<> will be improved by allowing
    // for width to be zero when all the values are known to be zero
    // (until the first non-zero value is added). The upshot of this
    // would be that the size of the array in memory would remain tiny
    // regardless of the number of elements it constains, as long as
    // all those elements are zero.
    REALM_ASSERT_DEBUG(!dynamic_cast<ArrayFloat*>(this));
    REALM_ASSERT_DEBUG(!dynamic_cast<ArrayDouble*>(this));

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
        set_width(0);
        set_width_in_header(0, get_header());
    }
}


void Array::truncate_and_destroy_children(size_t new_size)
{
    REALM_ASSERT(is_attached());
    REALM_ASSERT_3(new_size, <=, m_size);

    // FIXME: See FIXME in truncate().
    REALM_ASSERT_DEBUG(!dynamic_cast<ArrayFloat*>(this));
    REALM_ASSERT_DEBUG(!dynamic_cast<ArrayDouble*>(this));

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
        set_width(0);
        set_width_in_header(0, get_header());
    }
}


void Array::do_ensure_minimum_width(int_fast64_t value)
{

    // Make room for the new value
    size_t width = bit_width(value);
    REALM_ASSERT_3(width, >, m_width);

    Getter old_getter = m_getter; // Save old getter before width expansion
    alloc(m_size, width);         // Throws
    set_width(width);

    // Expand the old values
    size_t i = m_size;
    while (i != 0) {
        --i;
        int64_t v = (this->*old_getter)(i);
        (this->*(m_vtable->setter))(i, v);
    }
}

void Array::set_all_to_zero()
{
    if (m_size == 0 || m_width == 0)
        return;

    copy_on_write(); // Throws

    set_width(0);
    set_width_in_header(0, get_header());
}

void Array::adjust_ge(int_fast64_t limit, int_fast64_t diff)
{
    if (diff != 0) {
        for (size_t i = 0, n = size(); i != n;) {
            REALM_TEMPEX(i = adjust_ge, m_width, (i, n, limit, diff))
        }
    }
}

template <size_t w>
size_t Array::adjust_ge(size_t start, size_t end, int_fast64_t limit, int_fast64_t diff)
{
    REALM_ASSERT_DEBUG(diff != 0);

    for (size_t i = start; i != end; ++i) {
        int_fast64_t v = get<w>(i);
        if (v >= limit) {
            int64_t shifted = v + diff;

            // Make sure the new value can actually be stored. If this changes
            // the width, return the current position to the caller so that it
            // can switch to the appropriate specialization for the new width.
            ensure_minimum_width(shifted); // Throws
            copy_on_write();               // Throws
            if (m_width != w)
                return i;

            set<w>(i, shifted);
        }
    }
    return end;
}


// If indirection == nullptr, then return lowest 'i' for which for which this->get(i) >= target or -1 if none. If
// indirection == nullptr then 'this' must be sorted increasingly.
//
// If indirection exists, then return lowest 'i' for which this->get(indirection->get(i)) >= target or -1 if none.
// If indirection exists, then 'this' can be non-sorted, but 'indirection' must point into 'this' such that the values
// pointed at are sorted increasingly
//
// This method is mostly used by query_engine to enumerate table row indexes in increasing order through a TableView
size_t Array::find_gte(const int64_t target, size_t start, size_t end) const
{
    switch (m_width) {
        case 0:
            return find_gte<0>(target, start, end);
        case 1:
            return find_gte<1>(target, start, end);
        case 2:
            return find_gte<2>(target, start, end);
        case 4:
            return find_gte<4>(target, start, end);
        case 8:
            return find_gte<8>(target, start, end);
        case 16:
            return find_gte<16>(target, start, end);
        case 32:
            return find_gte<32>(target, start, end);
        case 64:
            return find_gte<64>(target, start, end);
        default:
            return not_found;
    }
}

template <size_t w>
size_t Array::find_gte(const int64_t target, size_t start, size_t end) const
{
    REALM_ASSERT(start < size());

    if (end > m_size) {
        end = m_size;
    }

#ifdef REALM_DEBUG
    // Reference implementation to illustrate and test behaviour
    size_t ref = 0;
    size_t idx;

    for (idx = start; idx < end; ++idx) {
        if (get(idx) >= target) {
            ref = idx;
            break;
        }
    }

    if (idx == end) {
        ref = not_found;
    }
#endif

    size_t ret;

    if (start >= end || target > ubound_for_width(w)) {
        ret = not_found;
        goto exit;
    }

    if (start + 2 < end) {
        if (get<w>(start) >= target) {
            ret = start;
            goto exit;
        }
        ++start;
        if (get<w>(start) >= target) {
            ret = start;
            goto exit;
        }
        ++start;
    }

    if (target > get<w>(end - 1)) {
        ret = not_found;
        goto exit;
    }

    size_t test_ndx;
    test_ndx = 1;

    for (size_t offset = start + test_ndx;; offset = start + test_ndx) {
        if (offset < end && get<w>(offset) < target)
            start += test_ndx;
        else
            break;

        test_ndx *= 2;
    }

    size_t high;
    high = start + test_ndx + 1;

    if (high > end)
        high = end;

    start--;

    // start of high

    size_t orig_high;
    orig_high = high;
    while (high - start > 1) {
        size_t probe = (start + high) / 2; // FIXME: see lower_bound() for better approach wrt overflow
        int64_t v = get<w>(probe);
        if (v < target)
            start = probe;
        else
            high = probe;
    }
    if (high == orig_high)
        ret = not_found;
    else
        ret = high;

exit:

#ifdef REALM_DEBUG
    REALM_ASSERT_DEBUG(ref == ret);
#endif

    return ret;
}

size_t Array::first_set_bit(unsigned int v) const
{
#if 0 && defined(USE_SSE42) && defined(_MSC_VER) && defined(REALM_PTR_64)
    unsigned long ul;
    // Just 10% faster than MultiplyDeBruijnBitPosition method, on Core i7
    _BitScanForward(&ul, v);
    return ul;
#elif 0 && !defined(_MSC_VER) && defined(USE_SSE42) && defined(REALM_PTR_64)
    return __builtin_clz(v);
#else
    int r;
    static const int MultiplyDeBruijnBitPosition[32] = {0,  1,  28, 2,  29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4,  8,
                                                        31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6,  11, 5,  10, 9};

    r = MultiplyDeBruijnBitPosition[(uint32_t((v & -int(v)) * 0x077CB531U)) >> 27];
    return r;
#endif
}

size_t Array::first_set_bit64(int64_t v) const
{
#if 0 && defined(USE_SSE42) && defined(_MSC_VER) && defined(REALM_PTR_64)
    unsigned long ul;
    _BitScanForward64(&ul, v);
    return ul;

#elif 0 && !defined(_MSC_VER) && defined(USE_SSE42) && defined(REALM_PTR_64)
    return __builtin_clzll(v);
#else
    unsigned int v0 = unsigned(v);
    unsigned int v1 = unsigned(uint64_t(v) >> 32);
    size_t r;

    if (v0 != 0)
        r = first_set_bit(v0);
    else
        r = first_set_bit(v1) + 32;

    return r;
#endif
}


namespace {

template <size_t width>
inline int64_t lower_bits()
{
    if (width == 1)
        return 0xFFFFFFFFFFFFFFFFULL;
    else if (width == 2)
        return 0x5555555555555555ULL;
    else if (width == 4)
        return 0x1111111111111111ULL;
    else if (width == 8)
        return 0x0101010101010101ULL;
    else if (width == 16)
        return 0x0001000100010001ULL;
    else if (width == 32)
        return 0x0000000100000001ULL;
    else if (width == 64)
        return 0x0000000000000001ULL;
    else {
        REALM_ASSERT_DEBUG(false);
        return int64_t(-1);
    }
}

// Return true if 'value' has an element (of bit-width 'width') which is 0
template <size_t width>
inline bool has_zero_element(uint64_t value)
{
    uint64_t hasZeroByte;
    uint64_t lower = lower_bits<width>();
    uint64_t upper = lower_bits<width>() * 1ULL << (width == 0 ? 0 : (width - 1ULL));
    hasZeroByte = (value - lower) & ~value & upper;
    return hasZeroByte != 0;
}


// Finds zero element of bit width 'width'
template <bool eq, size_t width>
size_t find_zero(uint64_t v)
{
    size_t start = 0;
    uint64_t hasZeroByte;

    // Bisection optimization, speeds up small bitwidths with high match frequency. More partions than 2 do NOT pay
    // off because the work done by test_zero() is wasted for the cases where the value exists in first half, but
    // useful if it exists in last half. Sweet spot turns out to be the widths and partitions below.
    if (width <= 8) {
        hasZeroByte = has_zero_element<width>(v | 0xffffffff00000000ULL);
        if (eq ? !hasZeroByte : (v & 0x00000000ffffffffULL) == 0) {
            // 00?? -> increasing
            start += 64 / no0(width) / 2;
            if (width <= 4) {
                hasZeroByte = has_zero_element<width>(v | 0xffff000000000000ULL);
                if (eq ? !hasZeroByte : (v & 0x0000ffffffffffffULL) == 0) {
                    // 000?
                    start += 64 / no0(width) / 4;
                }
            }
        }
        else {
            if (width <= 4) {
                // ??00
                hasZeroByte = has_zero_element<width>(v | 0xffffffffffff0000ULL);
                if (eq ? !hasZeroByte : (v & 0x000000000000ffffULL) == 0) {
                    // 0?00
                    start += 64 / no0(width) / 4;
                }
            }
        }
    }

    uint64_t mask = (width == 64 ? ~0ULL : ((1ULL << (width == 64 ? 0 : width)) -
                                            1ULL)); // Warning free way of computing (1ULL << width) - 1
    while (eq == (((v >> (width * start)) & mask) != 0)) {
        start++;
    }

    return start;
}

} // anonymous namesapce


template <bool find_max, size_t w>
bool Array::minmax(int64_t& result, size_t start, size_t end, size_t* return_ndx) const
{
    size_t best_index = 0;

    if (end == size_t(-1))
        end = m_size;
    REALM_ASSERT_11(start, <, m_size, &&, end, <=, m_size, &&, start, <, end);

    if (m_size == 0)
        return false;

    if (w == 0) {
        if (return_ndx)
            *return_ndx = best_index;
        result = 0;
        return true;
    }

    int64_t m = get<w>(start);
    ++start;

#if 0 // We must now return both value AND index of result. SSE does not support finding index, so we've disabled it
#ifdef REALM_COMPILER_SSE
    if (sseavx<42>()) {
        // Test manually until 128 bit aligned
        for (; (start < end) && (((size_t(m_data) & 0xf) * 8 + start * w) % (128) != 0); start++) {
            if (find_max ? get<w>(start) > m : get<w>(start) < m) {
                m = get<w>(start);
                best_index = start;
            }
        }

        if ((w == 8 || w == 16 || w == 32) && end - start > 2 * sizeof (__m128i) * 8 / no0(w)) {
            __m128i* data = reinterpret_cast<__m128i*>(m_data + start * w / 8);
            __m128i state = data[0];
            char state2[sizeof (state)];

            size_t chunks = (end - start) * w / 8 / sizeof (__m128i);
            for (size_t t = 0; t < chunks; t++) {
                if (w == 8)
                    state = find_max ? _mm_max_epi8(data[t], state) : _mm_min_epi8(data[t], state);
                else if (w == 16)
                    state = find_max ? _mm_max_epi16(data[t], state) : _mm_min_epi16(data[t], state);
                else if (w == 32)
                    state = find_max ? _mm_max_epi32(data[t], state) : _mm_min_epi32(data[t], state);

                start += sizeof (__m128i) * 8 / no0(w);
            }

            // Todo: prevent taking address of 'state' to make the compiler keep it in SSE register in above loop (vc2010/gcc4.6)

            // We originally had declared '__m128i state2' and did an 'state2 = state' assignment. When we read from state2 through int16_t, int32_t or int64_t in GetUniversal(),
            // the compiler thinks it cannot alias state2 and hence reorders the read and assignment.

            // In this fixed version using memcpy, we have char-read-access from __m128i (OK aliasing) and char-write-access to char-array, and finally int8/16/32/64
            // read access from char-array (OK aliasing).
            memcpy(&state2, &state, sizeof state);
            for (size_t t = 0; t < sizeof (__m128i) * 8 / no0(w); ++t) {
                int64_t v = get_universal<w>(reinterpret_cast<char*>(&state2), t);
                if (find_max ? v > m : v < m) {
                    m = v;
                }
            }
        }
    }
#endif
#endif

    for (; start < end; ++start) {
        const int64_t v = get<w>(start);
        if (find_max ? v > m : v < m) {
            m = v;
            best_index = start;
        }
    }

    result = m;
    if (return_ndx)
        *return_ndx = best_index;
    return true;
}

bool Array::maximum(int64_t& result, size_t start, size_t end, size_t* return_ndx) const
{
    REALM_TEMPEX2(return minmax, true, m_width, (result, start, end, return_ndx));
}

bool Array::minimum(int64_t& result, size_t start, size_t end, size_t* return_ndx) const
{
    REALM_TEMPEX2(return minmax, false, m_width, (result, start, end, return_ndx));
}

int64_t Array::sum(size_t start, size_t end) const
{
    REALM_TEMPEX(return sum, m_width, (start, end));
}

template <size_t w>
int64_t Array::sum(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = m_size;
    REALM_ASSERT_EX(end <= m_size && start <= end, start, end, m_size);

    if (w == 0 || start == end)
        return 0;

    int64_t s = 0;

    // Sum manually until 128 bit aligned
    for (; (start < end) && (((size_t(m_data) & 0xf) * 8 + start * w) % 128 != 0); start++) {
        s += get<w>(start);
    }

    if (w == 1 || w == 2 || w == 4) {
        // Sum of bitwidths less than a byte (which are always positive)
        // uses a divide and conquer algorithm that is a variation of popolation count:
        // http://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetParallel

        // static values needed for fast sums
        const uint64_t m2 = 0x3333333333333333ULL;
        const uint64_t m4 = 0x0f0f0f0f0f0f0f0fULL;
        const uint64_t h01 = 0x0101010101010101ULL;

        int64_t* data = reinterpret_cast<int64_t*>(m_data + start * w / 8);
        size_t chunks = (end - start) * w / 8 / sizeof(int64_t);

        for (size_t t = 0; t < chunks; t++) {
            if (w == 1) {

#if 0
#if defined(USE_SSE42) && defined(_MSC_VER) && defined(REALM_PTR_64)
                s += __popcnt64(data[t]);
#elif !defined(_MSC_VER) && defined(USE_SSE42) && defined(REALM_PTR_64)
                s += __builtin_popcountll(data[t]);
#else
                uint64_t a = data[t];
                const uint64_t m1  = 0x5555555555555555ULL;
                a -= (a >> 1) & m1;
                a = (a & m2) + ((a >> 2) & m2);
                a = (a + (a >> 4)) & m4;
                a = (a * h01) >> 56;
                s += a;
#endif
#endif

                s += fast_popcount64(data[t]);
            }
            else if (w == 2) {
                uint64_t a = data[t];
                a = (a & m2) + ((a >> 2) & m2);
                a = (a + (a >> 4)) & m4;
                a = (a * h01) >> 56;

                s += a;
            }
            else if (w == 4) {
                uint64_t a = data[t];
                a = (a & m4) + ((a >> 4) & m4);
                a = (a * h01) >> 56;
                s += a;
            }
        }
        start += sizeof(int64_t) * 8 / no0(w) * chunks;
    }

#ifdef REALM_COMPILER_SSE
    if (sseavx<42>()) {

        // 2000 items summed 500000 times, 8/16/32 bits, miliseconds:
        // Naive, templated get<>: 391 371 374
        // SSE:                     97 148 282

        if ((w == 8 || w == 16 || w == 32) && end - start > sizeof(__m128i) * 8 / no0(w)) {
            __m128i* data = reinterpret_cast<__m128i*>(m_data + start * w / 8);
            __m128i sum_result = {0};
            __m128i sum2;

            size_t chunks = (end - start) * w / 8 / sizeof(__m128i);

            for (size_t t = 0; t < chunks; t++) {
                if (w == 8) {
                    /*
                    // 469 ms AND disadvantage of handling max 64k elements before overflow
                    __m128i vl = _mm_cvtepi8_epi16(data[t]);
                    __m128i vh = data[t];
                    vh.m128i_i64[0] = vh.m128i_i64[1];
                    vh = _mm_cvtepi8_epi16(vh);
                    sum_result = _mm_add_epi16(sum_result, vl);
                    sum_result = _mm_add_epi16(sum_result, vh);
                    */

                    /*
                    // 424 ms
                    __m128i vl = _mm_unpacklo_epi8(data[t], _mm_set1_epi8(0));
                    __m128i vh = _mm_unpackhi_epi8(data[t], _mm_set1_epi8(0));
                    sum_result = _mm_add_epi32(sum_result, _mm_madd_epi16(vl, _mm_set1_epi16(1)));
                    sum_result = _mm_add_epi32(sum_result, _mm_madd_epi16(vh, _mm_set1_epi16(1)));
                    */

                    __m128i vl = _mm_cvtepi8_epi16(data[t]); // sign extend lower words 8->16
                    __m128i vh = data[t];
                    vh = _mm_srli_si128(vh, 8); // v >>= 64
                    vh = _mm_cvtepi8_epi16(vh); // sign extend lower words 8->16
                    __m128i sum1 = _mm_add_epi16(vl, vh);
                    __m128i sumH = _mm_cvtepi16_epi32(sum1);
                    __m128i sumL = _mm_srli_si128(sum1, 8); // v >>= 64
                    sumL = _mm_cvtepi16_epi32(sumL);
                    sum_result = _mm_add_epi32(sum_result, sumL);
                    sum_result = _mm_add_epi32(sum_result, sumH);
                }
                else if (w == 16) {
                    // todo, can overflow for array size > 2^32
                    __m128i vl = _mm_cvtepi16_epi32(data[t]); // sign extend lower words 16->32
                    __m128i vh = data[t];
                    vh = _mm_srli_si128(vh, 8);  // v >>= 64
                    vh = _mm_cvtepi16_epi32(vh); // sign extend lower words 16->32
                    sum_result = _mm_add_epi32(sum_result, vl);
                    sum_result = _mm_add_epi32(sum_result, vh);
                }
                else if (w == 32) {
                    __m128i v = data[t];
                    __m128i v0 = _mm_cvtepi32_epi64(v); // sign extend lower dwords 32->64
                    v = _mm_srli_si128(v, 8);           // v >>= 64
                    __m128i v1 = _mm_cvtepi32_epi64(v); // sign extend lower dwords 32->64
                    sum_result = _mm_add_epi64(sum_result, v0);
                    sum_result = _mm_add_epi64(sum_result, v1);

                    /*
                    __m128i m = _mm_set1_epi32(0xc000);             // test if overflow could happen (still need
                    underflow test).
                    __m128i mm = _mm_and_si128(data[t], m);
                    zz = _mm_or_si128(mm, zz);
                    sum_result = _mm_add_epi32(sum_result, data[t]);
                    */
                }
            }
            start += sizeof(__m128i) * 8 / no0(w) * chunks;

            // prevent taking address of 'state' to make the compiler keep it in SSE register in above loop
            // (vc2010/gcc4.6)
            sum2 = sum_result;

            // Avoid aliasing bug where sum2 might not yet be initialized when accessed by get_universal
            char sum3[sizeof sum2];
            memcpy(&sum3, &sum2, sizeof sum2);

            // Sum elements of sum
            for (size_t t = 0; t < sizeof(__m128i) * 8 / ((w == 8 || w == 16) ? 32 : 64); ++t) {
                int64_t v = get_universal < (w == 8 || w == 16) ? 32 : 64 > (reinterpret_cast<char*>(&sum3), t);
                s += v;
            }
        }
    }
#endif

    // Sum remaining elements
    for (; start < end; ++start)
        s += get<w>(start);

    return s;
}

size_t Array::count(int64_t value) const noexcept
{
    const uint64_t* next = reinterpret_cast<uint64_t*>(m_data);
    size_t value_count = 0;
    const size_t end = m_size;
    size_t i = 0;

    // static values needed for fast population count
    const uint64_t m1 = 0x5555555555555555ULL;
    const uint64_t m2 = 0x3333333333333333ULL;
    const uint64_t m4 = 0x0f0f0f0f0f0f0f0fULL;
    const uint64_t h01 = 0x0101010101010101ULL;

    if (m_width == 0) {
        if (value == 0)
            return m_size;
        return 0;
    }
    if (m_width == 1) {
        if (uint64_t(value) > 1)
            return 0;

        const size_t chunkvals = 64;
        for (; i + chunkvals <= end; i += chunkvals) {
            uint64_t a = next[i / chunkvals];
            if (value == 0)
                a = ~a; // reverse

            a -= (a >> 1) & m1;
            a = (a & m2) + ((a >> 2) & m2);
            a = (a + (a >> 4)) & m4;
            a = (a * h01) >> 56;

            // Could use intrinsic instead:
            // a = __builtin_popcountll(a); // gcc intrinsic

            value_count += to_size_t(a);
        }
    }
    else if (m_width == 2) {
        if (uint64_t(value) > 3)
            return 0;

        const uint64_t v = ~0ULL / 0x3 * value;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL / 0x3 * 0x1;

        const size_t chunkvals = 32;
        for (; i + chunkvals <= end; i += chunkvals) {
            uint64_t a = next[i / chunkvals];
            a ^= v;             // zero matching bit segments
            a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
            a &= m1;            // isolate single bit in each segment
            a ^= m1;            // reverse isolated bits
            // if (!a) continue;

            // Population count
            a = (a & m2) + ((a >> 2) & m2);
            a = (a + (a >> 4)) & m4;
            a = (a * h01) >> 56;

            value_count += to_size_t(a);
        }
    }
    else if (m_width == 4) {
        if (uint64_t(value) > 15)
            return 0;

        const uint64_t v = ~0ULL / 0xF * value;
        const uint64_t m = ~0ULL / 0xF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL / 0xF * 0x7;
        const uint64_t c2 = ~0ULL / 0xF * 0x3;

        const size_t chunkvals = 16;
        for (; i + chunkvals <= end; i += chunkvals) {
            uint64_t a = next[i / chunkvals];
            a ^= v;             // zero matching bit segments
            a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
            a |= (a >> 2) & c2;
            a &= m; // isolate single bit in each segment
            a ^= m; // reverse isolated bits

            // Population count
            a = (a + (a >> 4)) & m4;
            a = (a * h01) >> 56;

            value_count += to_size_t(a);
        }
    }
    else if (m_width == 8) {
        if (value > 0x7FLL || value < -0x80LL)
            return 0; // by casting?

        const uint64_t v = ~0ULL / 0xFF * value;
        const uint64_t m = ~0ULL / 0xFF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL / 0xFF * 0x7F;
        const uint64_t c2 = ~0ULL / 0xFF * 0x3F;
        const uint64_t c3 = ~0ULL / 0xFF * 0x0F;

        const size_t chunkvals = 8;
        for (; i + chunkvals <= end; i += chunkvals) {
            uint64_t a = next[i / chunkvals];
            a ^= v;             // zero matching bit segments
            a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
            a |= (a >> 2) & c2;
            a |= (a >> 4) & c3;
            a &= m; // isolate single bit in each segment
            a ^= m; // reverse isolated bits

            // Population count
            a = (a * h01) >> 56;

            value_count += to_size_t(a);
        }
    }
    else if (m_width == 16) {
        if (value > 0x7FFFLL || value < -0x8000LL)
            return 0; // by casting?

        const uint64_t v = ~0ULL / 0xFFFF * value;
        const uint64_t m = ~0ULL / 0xFFFF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL / 0xFFFF * 0x7FFF;
        const uint64_t c2 = ~0ULL / 0xFFFF * 0x3FFF;
        const uint64_t c3 = ~0ULL / 0xFFFF * 0x0FFF;
        const uint64_t c4 = ~0ULL / 0xFFFF * 0x00FF;

        const size_t chunkvals = 4;
        for (; i + chunkvals <= end; i += chunkvals) {
            uint64_t a = next[i / chunkvals];
            a ^= v;             // zero matching bit segments
            a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
            a |= (a >> 2) & c2;
            a |= (a >> 4) & c3;
            a |= (a >> 8) & c4;
            a &= m; // isolate single bit in each segment
            a ^= m; // reverse isolated bits

            // Population count
            a = (a * h01) >> 56;

            value_count += to_size_t(a);
        }
    }
    else if (m_width == 32) {
        int32_t v = int32_t(value);
        const int32_t* d = reinterpret_cast<int32_t*>(m_data);
        for (; i < end; ++i) {
            if (d[i] == v)
                ++value_count;
        }
        return value_count;
    }
    else if (m_width == 64) {
        const int64_t* d = reinterpret_cast<int64_t*>(m_data);
        for (; i < end; ++i) {
            if (d[i] == value)
                ++value_count;
        }
        return value_count;
    }

    // Check remaining elements
    for (; i < end; ++i)
        if (value == get(i))
            ++value_count;

    return value_count;
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
        throw util::overflow_error("Byte size overflow");
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
    REALM_ASSERT_7(value, ==, 0, ||, width_type, ==, wtype_Bits);
    REALM_ASSERT_7(size, ==, 0, ||, width_type, !=, wtype_Ignore);

    bool is_inner_bptree_node = false, has_refs = false;
    switch (type) {
        case type_Normal:
            break;
        case type_InnerBptreeNode:
            is_inner_bptree_node = true;
            has_refs = true;
            break;
        case type_HasRefs:
            has_refs = true;
            break;
    }

    int width = 0;
    size_t byte_size_0 = header_size;
    if (value != 0) {
        width = int(bit_width(value));
        byte_size_0 = calc_aligned_byte_size(size, width); // Throws
    }
    // Adding zero to Array::initial_capacity to avoid taking the
    // address of that member
    size_t byte_size = std::max(byte_size_0, initial_capacity + 0);
    MemRef mem = alloc.alloc(byte_size); // Throws
    char* header = mem.get_addr();

    init_header(header, is_inner_bptree_node, has_refs, context_flag, width_type, width, size, byte_size);

    if (value != 0) {
        char* data = get_data_from_header(header);
        size_t begin = 0, end = size;
        REALM_TEMPEX(fill_direct, width, (data, begin, end, value));
    }

    return mem;
}

int_fast64_t Array::lbound_for_width(size_t width) noexcept
{
    REALM_TEMPEX(return lbound_for_width, width, ());
}

template <size_t width>
int_fast64_t Array::lbound_for_width() noexcept
{
    if (width == 0) {
        return 0;
    }
    else if (width == 1) {
        return 0;
    }
    else if (width == 2) {
        return 0;
    }
    else if (width == 4) {
        return 0;
    }
    else if (width == 8) {
        return -0x80LL;
    }
    else if (width == 16) {
        return -0x8000LL;
    }
    else if (width == 32) {
        return -0x80000000LL;
    }
    else if (width == 64) {
        return -0x8000000000000000LL;
    }
    else {
        REALM_UNREACHABLE();
    }
}

int_fast64_t Array::ubound_for_width(size_t width) noexcept
{
    REALM_TEMPEX(return ubound_for_width, width, ());
}

template <size_t width>
int_fast64_t Array::ubound_for_width() noexcept
{
    if (width == 0) {
        return 0;
    }
    else if (width == 1) {
        return 1;
    }
    else if (width == 2) {
        return 3;
    }
    else if (width == 4) {
        return 15;
    }
    else if (width == 8) {
        return 0x7FLL;
    }
    else if (width == 16) {
        return 0x7FFFLL;
    }
    else if (width == 32) {
        return 0x7FFFFFFFLL;
    }
    else if (width == 64) {
        return 0x7FFFFFFFFFFFFFFFLL;
    }
    else {
        REALM_UNREACHABLE();
    }
}


template <size_t width>
struct Array::VTableForWidth {
    struct PopulatedVTable : Array::VTable {
        PopulatedVTable()
        {
            getter = &Array::get<width>;
            setter = &Array::set<width>;
            chunk_getter = &Array::get_chunk<width>;
            finder[cond_Equal] = &Array::find<Equal, act_ReturnFirst, width>;
            finder[cond_NotEqual] = &Array::find<NotEqual, act_ReturnFirst, width>;
            finder[cond_Greater] = &Array::find<Greater, act_ReturnFirst, width>;
            finder[cond_Less] = &Array::find<Less, act_ReturnFirst, width>;
        }
    };
    static const PopulatedVTable vtable;
};

template <size_t width>
const typename Array::VTableForWidth<width>::PopulatedVTable Array::VTableForWidth<width>::vtable;

void Array::set_width(size_t width) noexcept
{
    REALM_TEMPEX(set_width, width, ());
}

template <size_t width>
void Array::set_width() noexcept
{
    m_lbound = lbound_for_width<width>();
    m_ubound = ubound_for_width<width>();

    m_width = width;

    m_vtable = &VTableForWidth<width>::vtable;
    m_getter = m_vtable->getter;
}

// This method reads 8 concecutive values into res[8], starting from index 'ndx'. It's allowed for the 8 values to
// exceed array length; in this case, remainder of res[8] will be left untouched.
template <size_t w>
void Array::get_chunk(size_t ndx, int64_t res[8]) const noexcept
{
    REALM_ASSERT_3(ndx, <, m_size);

    // To make Valgrind happy. Todo, I *think* it should work without, now, but if it reappears, add memset again.
    // memset(res, 0, 8*8);

    if (REALM_X86_OR_X64_TRUE && (w == 1 || w == 2 || w == 4) && ndx + 32 < m_size) {
        // This method is *multiple* times faster than performing 8 times get<w>, even if unrolled. Apparently
        // compilers
        // can't figure out to optimize it.
        uint64_t c;
        size_t bytealign = ndx / (8 / no0(w));
        if (w == 1) {
            c = *reinterpret_cast<uint16_t*>(m_data + bytealign);
            c >>= (ndx - bytealign * 8) * w;
        }
        else if (w == 2) {
            c = *reinterpret_cast<uint32_t*>(m_data + bytealign);
            c >>= (ndx - bytealign * 4) * w;
        }
        else if (w == 4) {
            c = *reinterpret_cast<uint64_t*>(m_data + bytealign);
            c >>= (ndx - bytealign * 2) * w;
        }
        uint64_t mask = (w == 64 ? ~0ULL : ((1ULL << (w == 64 ? 0 : w)) - 1ULL));
        // The '?' is to avoid warnings about shifting too much
        res[0] = (c >> 0 * (w > 4 ? 0 : w)) & mask;
        res[1] = (c >> 1 * (w > 4 ? 0 : w)) & mask;
        res[2] = (c >> 2 * (w > 4 ? 0 : w)) & mask;
        res[3] = (c >> 3 * (w > 4 ? 0 : w)) & mask;
        res[4] = (c >> 4 * (w > 4 ? 0 : w)) & mask;
        res[5] = (c >> 5 * (w > 4 ? 0 : w)) & mask;
        res[6] = (c >> 6 * (w > 4 ? 0 : w)) & mask;
        res[7] = (c >> 7 * (w > 4 ? 0 : w)) & mask;
    }
    else {
        size_t i = 0;
        for (; i + ndx < m_size && i < 8; i++)
            res[i] = get<w>(ndx + i);

        for (; i < 8; i++)
            res[i] = 0;
    }

#ifdef REALM_DEBUG
    for (int j = 0; j + ndx < m_size && j < 8; j++) {
        int64_t expected = get<w>(ndx + j);
        if (res[j] != expected)
            REALM_ASSERT(false);
    }
#endif
}


template <size_t width>
void Array::set(size_t ndx, int64_t value)
{
    set_direct<width>(m_data, ndx, value);
}


// LCOV_EXCL_START ignore debug functions

std::pair<ref_type, size_t> Array::get_to_dot_parent(size_t ndx_in_parent) const
{
    return std::make_pair(get_ref(), ndx_in_parent);
}


#ifdef REALM_DEBUG
template <class C, class T>
std::basic_ostream<C, T>& operator<<(std::basic_ostream<C, T>& out, MemStats stats)
{
    std::ostringstream out_2;
    out_2.setf(std::ios::fixed);
    out_2.precision(1);
    double used_percent = 100.0 * stats.used / stats.allocated;
    out_2 << "allocated = " << stats.allocated << ", used = " << stats.used << " (" << used_percent << "%), "
          << "array_count = " << stats.array_count;
    out << out_2.str();
    return out;
}
#endif


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


#ifdef REALM_DEBUG

void Array::print() const
{
    std::cout << std::hex << get_ref() << std::dec << ": (" << size() << ") ";
    for (size_t i = 0; i < size(); ++i) {
        if (i)
            std::cout << ", ";
        std::cout << get(i);
    }
    std::cout << "\n";
}

void Array::verify() const
{
    REALM_ASSERT(is_attached());

    REALM_ASSERT(m_width == 0 || m_width == 1 || m_width == 2 || m_width == 4 || m_width == 8 || m_width == 16 ||
                 m_width == 32 || m_width == 64);

    if (!get_parent())
        return;

    // Check that parent is set correctly
    ref_type ref_in_parent = get_ref_from_parent();
    REALM_ASSERT_3(ref_in_parent, ==, m_ref);
}

namespace {

typedef std::tuple<size_t, int, bool> VerifyBptreeResult;

// Returns (num_elems, leaf-level, general_form)
VerifyBptreeResult verify_bptree(const Array& node, Array::LeafVerifier leaf_verifier)
{
    node.verify();

    // This node must not be a leaf
    REALM_ASSERT_3(node.get_type(), ==, Array::type_InnerBptreeNode);

    REALM_ASSERT_3(node.size(), >=, 2);
    size_t num_children = node.size() - 2;

    // Verify invar:bptree-nonempty-inner
    REALM_ASSERT_3(num_children, >=, 1);

    Allocator& alloc = node.get_alloc();
    Array offsets(alloc);
    size_t elems_per_child = 0;
    bool general_form;
    {
        int_fast64_t first_value = node.get(0);
        general_form = first_value % 2 == 0;
        if (general_form) {
            offsets.init_from_ref(to_ref(first_value));
            offsets.verify();
            REALM_ASSERT_3(offsets.get_type(), ==, Array::type_Normal);
            REALM_ASSERT_3(offsets.size(), ==, num_children - 1);
        }
        else {
            REALM_ASSERT(!int_cast_with_overflow_detect(first_value / 2, elems_per_child));
        }
    }

    size_t num_elems = 0;
    int leaf_level_of_children = -1;
    for (size_t i = 0; i < num_children; ++i) {
        ref_type child_ref = node.get_as_ref(1 + i);
        char* child_header = alloc.translate(child_ref);
        bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_header);
        size_t elems_in_child;
        int leaf_level_of_child;
        if (child_is_leaf) {
            elems_in_child = (*leaf_verifier)(MemRef(child_header, child_ref, alloc), alloc);
            // Verify invar:bptree-nonempty-leaf
            REALM_ASSERT_3(elems_in_child, >=, 1);
            leaf_level_of_child = 0;
        }
        else {
            Array child(alloc);
            child.init_from_ref(child_ref);
            VerifyBptreeResult r = verify_bptree(child, leaf_verifier);
            elems_in_child = std::get<0>(r);
            leaf_level_of_child = std::get<1>(r);
            // Verify invar:bptree-node-form
            bool child_on_general_form = std::get<2>(r);
            REALM_ASSERT(general_form || !child_on_general_form);
        }
        if (i == 0)
            leaf_level_of_children = leaf_level_of_child;
        // Verify invar:bptree-leaf-depth
        REALM_ASSERT_3(leaf_level_of_child, ==, leaf_level_of_children);
        // Check integrity of aggregated per-child element counts
        REALM_ASSERT(!int_add_with_overflow_detect(num_elems, elems_in_child));
        if (general_form) {
            if (i < num_children - 1)
                REALM_ASSERT(int_equal_to(num_elems, offsets.get(i)));
        }
        else { // Compact form
            if (i < num_children - 1) {
                REALM_ASSERT(elems_in_child == elems_per_child);
            }
            else {
                REALM_ASSERT(elems_in_child <= elems_per_child);
            }
        }
    }
    REALM_ASSERT_3(leaf_level_of_children, !=, -1);
    {
        int_fast64_t last_value = node.back();
        REALM_ASSERT_3(last_value % 2, !=, 0);
        size_t total_elems = 0;
        REALM_ASSERT(!int_cast_with_overflow_detect(last_value / 2, total_elems));
        REALM_ASSERT_3(num_elems, ==, total_elems);
    }
    return std::make_tuple(num_elems, 1 + leaf_level_of_children, general_form);
}

} // anonymous namespace

void Array::verify_bptree(LeafVerifier leaf_verifier) const
{
    ::verify_bptree(*this, leaf_verifier);
}


void Array::dump_bptree_structure(std::ostream& out, int level, LeafDumper leaf_dumper) const
{
    bool root_is_leaf = !is_inner_bptree_node();
    if (root_is_leaf) {
        (*leaf_dumper)(get_mem(), m_alloc, out, level);
        return;
    }

    int indent = level * 2;
    out << std::setw(indent) << ""
        << "Inner node (B+ tree) (ref: " << get_ref() << ")\n";

    size_t num_elems_in_subtree = size_t(back() / 2);
    out << std::setw(indent) << ""
        << "  Number of elements in subtree: " << num_elems_in_subtree << "\n";

    bool compact_form = front() % 2 != 0;
    if (compact_form) {
        size_t elems_per_child = size_t(front() / 2);
        out << std::setw(indent) << ""
            << "  Compact form (elements per child: " << elems_per_child << ")\n";
    }
    else { // General form
        Array offsets(m_alloc);
        offsets.init_from_ref(to_ref(front()));
        out << std::setw(indent) << ""
            << "  General form (offsets_ref: " << offsets.get_ref() << ", ";
        if (offsets.is_empty()) {
            out << "no offsets";
        }
        else {
            out << "offsets: ";
            for (size_t i = 0; i != offsets.size(); ++i) {
                if (i != 0)
                    out << ", ";
                out << offsets.get(i);
            }
        }
        out << ")\n";
    }

    size_t num_children = size() - 2;
    size_t child_ref_begin = 1;
    size_t child_ref_end = 1 + num_children;
    for (size_t i = child_ref_begin; i != child_ref_end; ++i) {
        Array child(m_alloc);
        child.init_from_ref(get_as_ref(i));
        child.dump_bptree_structure(out, level + 1, leaf_dumper);
    }
}

void Array::bptree_to_dot(std::ostream& out, ToDotHandler& handler) const
{
    bool root_is_leaf = !is_inner_bptree_node();
    if (root_is_leaf) {
        handler.to_dot(get_mem(), get_parent(), get_ndx_in_parent(), out);
        return;
    }

    ref_type ref = get_ref();
    out << "subgraph cluster_inner_pbtree_node" << ref << " {" << std::endl;
    out << " label = \"\";" << std::endl;

    to_dot(out);

    int_fast64_t first_value = get(0);
    if (first_value % 2 == 0) {
        // On general form / has 'offsets' array
        Array offsets(m_alloc);
        offsets.init_from_ref(to_ref(first_value));
        offsets.set_parent(const_cast<Array*>(this), 0);
        offsets.to_dot(out, "Offsets");
    }

    out << "}" << std::endl;

    size_t num_children = size() - 2;
    size_t child_ref_begin = 1;
    size_t child_ref_end = 1 + num_children;
    for (size_t i = child_ref_begin; i != child_ref_end; ++i) {
        Array child(m_alloc);
        child.init_from_ref(get_as_ref(i));
        child.set_parent(const_cast<Array*>(this), i);
        child.bptree_to_dot(out, handler);
    }
}


void Array::to_dot(std::ostream& out, StringData title) const
{
    ref_type ref = get_ref();

    if (title.size() != 0) {
        out << "subgraph cluster_" << ref << " {" << std::endl;
        out << " label = \"" << title << "\";" << std::endl;
        out << " color = white;" << std::endl;
    }

    out << "n" << std::hex << ref << std::dec << "[shape=none,label=<";
    out << "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\"><TR>" << std::endl;

    // Header
    out << "<TD BGCOLOR=\"lightgrey\"><FONT POINT-SIZE=\"7\"> ";
    out << "0x" << std::hex << ref << std::dec << "<BR/>";
    if (m_is_inner_bptree_node)
        out << "IsNode<BR/>";
    if (m_has_refs)
        out << "HasRefs<BR/>";
    if (m_context_flag)
        out << "ContextFlag<BR/>";
    out << "</FONT></TD>" << std::endl;

    // Values
    for (size_t i = 0; i < m_size; ++i) {
        int64_t v = get(i);
        if (m_has_refs) {
            // zero-refs and refs that are not 64-aligned do not point to sub-trees
            if (v == 0)
                out << "<TD>none";
            else if (v & 0x1)
                out << "<TD BGCOLOR=\"grey90\">" << (uint64_t(v) >> 1);
            else
                out << "<TD PORT=\"" << i << "\">";
        }
        else {
            out << "<TD>" << v;
        }
        out << "</TD>" << std::endl;
    }

    out << "</TR></TABLE>>];" << std::endl;

    if (title.size() != 0)
        out << "}" << std::endl;

    to_dot_parent_edge(out);
}

void Array::to_dot_parent_edge(std::ostream& out) const
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


size_t Array::lower_bound_int(int64_t value) const noexcept
{
    REALM_TEMPEX(return lower_bound, m_width, (m_data, m_size, value));
}

size_t Array::upper_bound_int(int64_t value) const noexcept
{
    REALM_TEMPEX(return upper_bound, m_width, (m_data, m_size, value));
}


void Array::find_all(IntegerColumn* result, int64_t value, size_t col_offset, size_t begin, size_t end) const
{
    REALM_ASSERT_3(begin, <=, size());
    REALM_ASSERT(end == npos || (begin <= end && end <= size()));

    if (end == npos)
        end = m_size;

    QueryState<int64_t> state(act_FindAll, result);
    REALM_TEMPEX3(find, Equal, act_FindAll, m_width, (value, begin, end, col_offset, &state, CallbackDummy()));

    return;
}


bool Array::find(int cond, Action action, int64_t value, size_t start, size_t end, size_t baseindex,
                 QueryState<int64_t>* state, bool nullable_array, bool find_null) const
{
    if (cond == cond_Equal) {
        return find<Equal>(action, value, start, end, baseindex, state, nullable_array, find_null);
    }
    if (cond == cond_NotEqual) {
        return find<NotEqual>(action, value, start, end, baseindex, state, nullable_array, find_null);
    }
    if (cond == cond_Greater) {
        return find<Greater>(action, value, start, end, baseindex, state, nullable_array, find_null);
    }
    if (cond == cond_Less) {
        return find<Less>(action, value, start, end, baseindex, state, nullable_array, find_null);
    }
    if (cond == cond_None) {
        return find<None>(action, value, start, end, baseindex, state, nullable_array, find_null);
    }
    else if (cond == cond_LeftNotNull) {
        return find<NotNull>(action, value, start, end, baseindex, state, nullable_array, find_null);
    }
    REALM_ASSERT_DEBUG(false);
    return false;
}


size_t Array::find_first(int64_t value, size_t start, size_t end) const
{
    return find_first<Equal>(value, start, end);
}

int_fast64_t Array::get(const char* header, size_t ndx) noexcept
{
    const char* data = get_data_from_header(header);
    uint_least8_t width = get_width_from_header(header);
    return get_direct(data, width, ndx);
}


std::pair<int64_t, int64_t> Array::get_two(const char* header, size_t ndx) noexcept
{
    const char* data = get_data_from_header(header);
    uint_least8_t width = get_width_from_header(header);
    std::pair<int64_t, int64_t> p = ::get_two(data, width, ndx);
    return std::make_pair(p.first, p.second);
}


void Array::get_three(const char* header, size_t ndx, ref_type& v0, ref_type& v1, ref_type& v2) noexcept
{
    const char* data = get_data_from_header(header);
    uint_least8_t width = get_width_from_header(header);
    ::get_three(data, width, ndx, v0, v1, v2);
}
