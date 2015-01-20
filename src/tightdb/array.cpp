#include <cstring> // std::memcpy
#include <limits>
#include <iostream>
#include <iomanip>

#ifdef _MSC_VER
#  include <intrin.h>
#  include <win32/types.h>
#  pragma warning (disable : 4127) // Condition is constant warning
#endif

#include <tightdb/util/tuple.hpp>
#include <tightdb/utilities.hpp>
#include <tightdb/array.hpp>
#include <tightdb/array_basic.hpp>
#include <tightdb/impl/destroy_guard.hpp>
#include <tightdb/column.hpp>
#include <tightdb/query_conditions.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/index_string.hpp>


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
// `N_c` must always be a power of `TIGHTDB_MAX_BPNODE_SIZE`.
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


using namespace std;
using namespace tightdb;
using namespace tightdb::util;


namespace {

/// Takes a 64-bit value and returns the minimum number of bits needed
/// to fit the value. For alignment this is rounded up to nearest
/// log2. Posssible results {0, 1, 2, 4, 8, 16, 32, 64}
size_t bit_width(int64_t v)
{
    // FIXME: Assuming there is a 64-bit CPU reverse bitscan
    // instruction and it is fast, then this function could be
    // implemented simply as (v<2 ? v :
    // 2<<rev_bitscan(rev_bitscan(v))).

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

} // anonymous namespace


void Array::init_from_mem(MemRef mem) TIGHTDB_NOEXCEPT
{
    char* header = mem.m_addr;

    // Parse header
    m_is_inner_bptree_node = get_is_inner_bptree_node_from_header(header);
    m_has_refs             = get_hasrefs_from_header(header);
    m_context_flag         = get_context_flag_from_header(header);
    m_width                = get_width_from_header(header);
    m_size                 = get_size_from_header(header);

    // Capacity is how many items there are room for
    bool is_read_only = m_alloc.is_read_only(mem.m_ref);
    if (is_read_only) {
        m_capacity = m_size;
    }
    else {
        size_t byte_capacity = get_capacity_from_header(header);
        // FIXME: Avoid calling virtual method CalcItemCount() here,
        // instead calculate the capacity in a way similar to what is done
        // in get_byte_size_from_header(). The virtual call makes "life"
        // hard for constructors in derived array classes.
        m_capacity = CalcItemCount(byte_capacity, m_width);
    }

    m_ref = mem.m_ref;
    m_data = get_data_from_header(header);

    set_width(m_width);
}

// FIXME: This is a very crude and error prone misuse of Array,
// especially since its use is not isolated inside the array
// class. There seems to be confusion about how to construct an array
// to be used with this method. Somewhere (e.g. in
// Column::find_first()) we use Array(Allocator&). In other places
// (TableViewBase::aggregate()) we use Array(no_prealloc_tag). We must
// at least document the rules governing the use of
// CreateFromHeaderDirect().
//
// FIXME: If we want to keep this method, we should formally define
// what can be termed 'direct read-only' use of an Array instance, and
// what rules apply in this case. Currently Array::clone() just passes
// zero for the 'ref' argument.
//
// FIXME: Assuming that this method is only used for what can be
// termed 'direct read-only' use, the type of the header argument
// should be changed to 'const char*', and a const_cast should be
// added below. This would avoid the need for const_cast's in places
// like Array::clone().
void Array::CreateFromHeaderDirect(char* header, ref_type ref) TIGHTDB_NOEXCEPT
{
    // Parse header
    // We only need limited info for direct read-only use
    m_width    = get_width_from_header(header);
    m_size     = get_size_from_header(header);

    m_ref = ref;
    m_data = get_data_from_header(header);

    set_width(m_width);
}


void Array::set_type(Type type)
{
    TIGHTDB_ASSERT(is_attached());

    copy_on_write(); // Throws

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
    m_is_inner_bptree_node = is_inner_bptree_node;
    m_has_refs = has_refs;
    set_header_is_inner_bptree_node(is_inner_bptree_node);
    set_header_hasrefs(has_refs);
}


bool Array::update_from_parent(size_t old_baseline) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT_DEBUG(is_attached());
    TIGHTDB_ASSERT_DEBUG(m_parent);

    // Array nodes that are part of the previous version of the
    // database will not be overwritten by Group::commit(). This is
    // necessary for robustness in the face of abrupt termination of
    // the process. It also means that we can be sure that an array
    // remains unchanged across a commit if the new ref is equal to
    // the old ref and the ref is below the previous baseline.

    ref_type new_ref = m_parent->get_child_ref(m_ndx_in_parent);
    if (new_ref == m_ref && new_ref < old_baseline)
        return false; // Has not changed

    init_from_ref(new_ref);
    return true; // Might have changed
}


MemRef Array::slice(size_t offset, size_t size, Allocator& target_alloc) const
{
    TIGHTDB_ASSERT(is_attached());

    Array slice(target_alloc);
    _impl::DeepArrayDestroyGuard dg(&slice);
    Type type = get_type();
    slice.create(type, m_context_flag); // Throws
    size_t begin = offset;
    size_t end   = offset + size;
    for (size_t i = begin; i != end; ++i) {
        int_fast64_t value = get(i);
        slice.add(value); // Throws
    }
    dg.release();
    return slice.get_mem();
}


MemRef Array::slice_and_clone_children(size_t offset, size_t size, Allocator& target_alloc) const
{
    TIGHTDB_ASSERT(is_attached());
    if (!has_refs())
        return slice(offset, size, target_alloc); // Throws

    Array slice(target_alloc);
    _impl::DeepArrayDestroyGuard dg(&slice);
    Type type = get_type();
    slice.create(type, m_context_flag); // Throws
    _impl::DeepArrayRefDestroyGuard dg_2(target_alloc);
    size_t begin = offset;
    size_t end   = offset + size;
    for (size_t i = begin; i != end; ++i) {
        int_fast64_t value = get(i);

        // Null-refs signify empty subtrees. Also, all refs are
        // 8-byte aligned, so the lowest bits cannot be set. If they
        // are, it means that it should not be interpreted as a ref.
        bool is_subarray = value != 0 && value % 2 == 0;
        if (!is_subarray) {
            slice.add(value); // Throws
            continue;
        }

        ref_type ref = to_ref(value);
        Allocator& alloc = get_alloc();
        const char* subheader = alloc.translate(ref);
        MemRef new_mem = clone(subheader, alloc, target_alloc); // Throws
        dg_2.reset(new_mem.m_ref);
        value = new_mem.m_ref; // FIXME: Dangerous cast (unsigned -> signed)
        slice.add(value); // Throws
        dg_2.release();
    }
    dg.release();
    return slice.get_mem();
}


// Allocates space for 'size' items being between min and min in size, both inclusive. Crashes! Why? Todo/fixme
void Array::Preset(size_t width, size_t size)
{
    clear_and_destroy_children();
    set_width(width);
    alloc(size, width); // Throws
    m_size = size;
    for (size_t i = 0; i != size; ++i)
        set(i, 0);
}

void Array::Preset(int64_t min, int64_t max, size_t count)
{
    size_t w = ::max(bit_width(max), bit_width(min));
    Preset(w, count);
}


void Array::destroy_children(size_t offset) TIGHTDB_NOEXCEPT
{
    for (size_t i = offset; i != m_size; ++i) {
        int64_t value = get(i);

        // Null-refs indicate empty sub-trees
        if (value == 0)
            continue;

        // A ref is always 8-byte aligned, so the lowest bit
        // cannot be set. If it is, it means that it should not be
        // interpreted as a ref.
        if (value % 2 != 0)
            continue;

        ref_type ref = to_ref(value);
        destroy_deep(ref, m_alloc);
    }
}


void Array::move(size_t begin, size_t end, size_t dest_begin)
{
    TIGHTDB_ASSERT(begin <= end);
    TIGHTDB_ASSERT(end <= m_size);
    TIGHTDB_ASSERT(dest_begin <= m_size);
    TIGHTDB_ASSERT(end - begin <= m_size - dest_begin);
    TIGHTDB_ASSERT(!(dest_begin >= begin && dest_begin < end)); // Required by std::copy

    // Check if we need to copy before modifying
    copy_on_write(); // Throws

    if (m_width < 8) {
        // FIXME: Should be optimized
        for (size_t i = begin; i != end; ++i) {
            int_fast64_t v = (this->*m_getter)(i);
            (this->*m_setter)(dest_begin++, v);
        }
        return;
    }

    size_t bytes_per_elem = m_width / 8;
    const char* begin_2 = m_data + begin      * bytes_per_elem;
    const char* end_2   = m_data + end        * bytes_per_elem;
    char* dest_begin_2  = m_data + dest_begin * bytes_per_elem;
    copy(begin_2, end_2, dest_begin_2);
}

void Array::move_backward(size_t begin, size_t end, size_t dest_end)
{
    TIGHTDB_ASSERT(begin <= end);
    TIGHTDB_ASSERT(end <= m_size);
    TIGHTDB_ASSERT(dest_end <= m_size);
    TIGHTDB_ASSERT(end - begin <= dest_end);
    TIGHTDB_ASSERT(!(dest_end > begin && dest_end <= end)); // Required by std::copy_backward

    // Check if we need to copy before modifying
    copy_on_write(); // Throws

    if (m_width < 8) {
        // FIXME: Should be optimized
        for (size_t i = end; i != begin; --i) {
            int_fast64_t v = (this->*m_getter)(i-1);
            (this->*m_setter)(--dest_end, v);
        }
        return;
    }

    size_t bytes_per_elem = m_width / 8;
    const char* begin_2 = m_data + begin    * bytes_per_elem;
    const char* end_2   = m_data + end      * bytes_per_elem;
    char* dest_end_2    = m_data + dest_end * bytes_per_elem;
    copy_backward(begin_2, end_2, dest_end_2);
}

void Array::add_to_column(Column* column, int64_t value) 
{
    column->add(value);
}

void Array::set(size_t ndx, int64_t value)
{
    TIGHTDB_ASSERT(ndx < m_size);

    // Check if we need to copy before modifying
    copy_on_write(); // Throws

    bool do_expand = value < m_lbound || value > m_ubound;
    if (do_expand) {
        size_t width = bit_width(value);
        TIGHTDB_ASSERT_DEBUG(width > m_width);
        Getter old_getter = m_getter;    // Save old getter before width expansion
        alloc(m_size, width); // Throws
        set_width(width);

        // Expand the old values
        size_t i = m_size;
        while (i != 0) {
            --i;
            int64_t v = (this->*old_getter)(i);
            (this->*m_setter)(i, v);
        }
    }

    // Set the value
    (this->*m_setter)(ndx, value);
}

void Array::set_uint(std::size_t ndx, uint_fast64_t value)
{
    // When a value of a signed type is converted to an unsigned type, the C++
    // standard guarantees that negative values are converted from the native
    // representation to 2's complement, but the effect of conversions in the
    // opposite direction is left unspecified by the
    // standard. `tightdb::util::from_twos_compl()` is used here to perform the
    // correct opposite unsigned-to-signed conversion, which reduces to a no-op
    // when 2's complement is the native representation of negative values.
    set(ndx, from_twos_compl<int_fast64_t>(value));
}

void Array::set_as_ref(std::size_t ndx, ref_type ref)
{
    set(ndx, from_ref(ref));
}

/*
// Optimization for the common case of adding positive values to a local array
// (happens a lot when returning results to TableViews)
void Array::AddPositiveLocal(int64_t value)
{
    TIGHTDB_ASSERT(value >= 0);
    TIGHTDB_ASSERT(&m_alloc == &Allocator::get_default());

    if (value <= m_ubound) {
        if (m_size < m_capacity) {
            (this->*m_setter)(m_size, value);
            ++m_size;
            set_header_size(m_size);
            return;
        }
    }

    insert(m_size, value);
}
*/

void Array::insert(size_t ndx, int_fast64_t value)
{
    TIGHTDB_ASSERT_DEBUG(ndx <= m_size);

    // Check if we need to copy before modifying
    copy_on_write(); // Throws

    Getter old_getter = m_getter; // Save old getter before potential width expansion

    bool do_expand = value < m_lbound || value > m_ubound;
    if (do_expand) {
        size_t width = bit_width(value);
        TIGHTDB_ASSERT_DEBUG(width > m_width);
        alloc(m_size+1, width); // Throws
        set_width(width);
    }
    else {
        alloc(m_size+1, m_width); // Throws
    }

    // Move values below insertion (may expand)
    if (do_expand || m_width < 8) {
        size_t i = m_size;
        while (i > ndx) {
            --i;
            int64_t v = (this->*old_getter)(i);
            (this->*m_setter)(i+1, v);
        }
    }
    else if (ndx != m_size) {
        // when byte sized and no expansion, use memmove
// FIXME: Optimize by simply dividing by 8 (or shifting right by 3 bit positions)
        size_t w = (m_width == 64) ? 8 : (m_width == 32) ? 4 : (m_width == 16) ? 2 : 1;
        char* base = reinterpret_cast<char*>(m_data);
        char* src_begin = base + ndx*w;
        char* src_end   = base + m_size*w;
        char* dst_end   = src_end + w;
        copy_backward(src_begin, src_end, dst_end);
    }

    // Insert the new value
    (this->*m_setter)(ndx, value);

    // Expand values above insertion
    if (do_expand) {
        size_t i = ndx;
        while (i != 0) {
            --i;
            int64_t v = (this->*old_getter)(i);
            (this->*m_setter)(i, v);
        }
    }

    // Update size
    // (no need to do it in header as it has been done by Alloc)
    ++m_size;
}


void Array::truncate(size_t size)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(size <= m_size);

    // FIXME: BasicArray<> currently does not work if the width is set
    // to zero, so it must override Array::truncate(). In the future
    // it is expected that BasicArray<> will be improved by allowing
    // for width to be zero when all the values are known to be zero
    // (until the first non-zero value is added). The upshot of this
    // would be that the size of the array in memory would remain tiny
    // regardless of the number of elements it constains, as long as
    // all those elements are zero.
    TIGHTDB_ASSERT_DEBUG(!dynamic_cast<ArrayFloat*>(this));
    TIGHTDB_ASSERT_DEBUG(!dynamic_cast<ArrayDouble*>(this));

    copy_on_write(); // Throws

    // Update size in accessor and in header. This leaves the capacity
    // unchanged.
    m_size = size;
    set_header_size(size);

    // If the array is completely cleared, we take the opportunity to
    // drop the width back to zero.
    if (size == 0) {
        m_capacity = CalcItemCount(get_capacity_from_header(), 0);
        set_width(0);
        set_header_width(0);
    }
}


void Array::truncate_and_destroy_children(size_t size)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(size <= m_size);

    // FIXME: See FIXME in truncate().
    TIGHTDB_ASSERT_DEBUG(!dynamic_cast<ArrayFloat*>(this));
    TIGHTDB_ASSERT_DEBUG(!dynamic_cast<ArrayDouble*>(this));

    copy_on_write(); // Throws

    if (m_has_refs) {
        size_t offset = size;
        destroy_children(offset);
    }

    // Update size in accessor and in header. This leaves the capacity
    // unchanged.
    m_size = size;
    set_header_size(size);

    // If the array is completely cleared, we take the opportunity to
    // drop the width back to zero.
    if (size == 0) {
        m_capacity = CalcItemCount(get_capacity_from_header(), 0);
        set_width(0);
        set_header_width(0);
    }
}


void Array::ensure_minimum_width(int64_t value)
{
    if (value >= m_lbound && value <= m_ubound)
        return;

    // Check if we need to copy before modifying
    copy_on_write(); // Throws

    // Make room for the new value
    size_t width = bit_width(value);
    TIGHTDB_ASSERT(width > m_width);

    Getter old_getter = m_getter; // Save old getter before width expansion
    alloc(m_size, width); // Throws
    set_width(width);

    // Expand the old values
    size_t i = m_size;
    while (i != 0) {
        --i;
        int64_t v = (this->*old_getter)(i);
        (this->*m_setter)(i, v);
    }
}

void Array::set_all_to_zero()
{
    copy_on_write(); // Throws

    m_capacity = CalcItemCount(get_capacity_from_header(), 0);
    set_width(0);

    // Update header
    set_header_width(0);
}


// If indirection == null_ptr, then return lowest 'i' for which for which this->get(i) >= target or -1 if none. If
// indirection == null_ptr then 'this' must be sorted increasingly.
//
// If indirection exists, then return lowest 'i' for which this->get(indirection->get(i)) >= target or -1 if none.
// If indirection exists, then 'this' can be non-sorted, but 'indirection' must point into 'this' such that the values
// pointed at are sorted increasingly
//
// This method is mostly used by query_engine to enumerate table row indexes in increasing order through a TableView
size_t Array::FindGTE(int64_t target, size_t start, const Array* indirection) const
{
#if TIGHTDB_DEBUG
    // Reference implementation to illustrate and test behaviour
    size_t ref = 0;
    size_t idx;
    for (idx = start; idx < m_size; ++idx) {
        if (get(indirection ? indirection->get(idx) : idx) >= target) {
            ref = idx;
            break;
        }
    }
    if (idx == m_size)
        ref = not_found;
#endif

    size_t ret;

    if (start >= m_size) {
        ret = not_found;
        goto exit;
    }

    if (start + 2 < m_size) {
        if (get(indirection ? to_size_t(indirection->get(start)) : start) >= target) {
            ret = start;
            goto exit;
        }
        ++start;
        if (get(indirection ? to_size_t(indirection->get(start)) : start) >= target) {
            ret = start;
            goto exit;
        }
        ++start;
    }

    // Todo, use templated get<width> from this point for performance
    if (target > get(indirection ? to_size_t(indirection->get(m_size - 1)) : m_size - 1)) {
        ret = not_found;
        goto exit;
    }

    size_t add;
    add = 1;

    for (;;) {
        if (start + add < m_size && get(indirection ? to_size_t(indirection->get(start + add)) : start + add) < target)
            start += add;
        else
            break;
       add *= 2;
    }

    size_t high;
    high = start + add + 1;

    if (high > m_size)
        high = m_size;

   // if (start > 0)
        start--;

    //start og high

    size_t orig_high;
    orig_high = high;
    while (high - start > 1) {
        size_t probe = (start + high) / 2; // FIXME: Prone to overflow - see lower_bound() for a solution
        int64_t v = get(indirection ? to_size_t(indirection->get(probe)) : probe);
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

    TIGHTDB_ASSERT_DEBUG(ref == ret);

    return ret;
}

size_t Array::FirstSetBit(unsigned int v) const
{
#if 0 && defined(USE_SSE42) && defined(_MSC_VER) && defined(TIGHTDB_PTR_64)
    unsigned long ul;
    // Just 10% faster than MultiplyDeBruijnBitPosition method, on Core i7
    _BitScanForward(&ul, v);
    return ul;
#elif 0 && !defined(_MSC_VER) && defined(USE_SSE42) && defined(TIGHTDB_PTR_64)
    return __builtin_clz(v);
#else
    int r;
    static const int MultiplyDeBruijnBitPosition[32] =
    {
        0, 1, 28, 2, 29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8,
        31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6, 11, 5, 10, 9
    };

    r = MultiplyDeBruijnBitPosition[(uint32_t((v & -int(v)) * 0x077CB531U)) >> 27];
return r;
#endif
}

size_t Array::FirstSetBit64(int64_t v) const
{
#if 0 && defined(USE_SSE42) && defined(_MSC_VER) && defined(TIGHTDB_PTR_64)
    unsigned long ul;
    _BitScanForward64(&ul, v);
    return ul;

#elif 0 && !defined(_MSC_VER) && defined(USE_SSE42) && defined(TIGHTDB_PTR_64)
    return __builtin_clzll(v);
#else
    unsigned int v0 = unsigned(v);
    unsigned int v1 = unsigned(uint64_t(v) >> 32);
    size_t r;

    if (v0 != 0)
        r = FirstSetBit(v0);
    else
        r = FirstSetBit(v1) + 32;

    return r;
#endif
}


namespace {

template<size_t width> inline int64_t LowerBits()
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
        TIGHTDB_ASSERT_DEBUG(false);
        return int64_t(-1);
    }
}

// Return true if 'value' has an element (of bit-width 'width') which is 0
template<size_t width> inline bool has_zero_element(uint64_t value) {
    uint64_t hasZeroByte;
    uint64_t lower = LowerBits<width>();
    uint64_t upper = LowerBits<width>() * 1ULL << (width == 0 ? 0 : (width - 1ULL));
    hasZeroByte = (value - lower) & ~value & upper;
    return hasZeroByte != 0;
}


// Finds zero element of bit width 'width'
template<bool eq, size_t width> size_t FindZero(uint64_t v)
{
    size_t start = 0;
    uint64_t hasZeroByte;

    // Bisection optimization, speeds up small bitwidths with high match frequency. More partions than 2 do NOT pay off because
    // the work done by TestZero() is wasted for the cases where the value exists in first half, but useful if it exists in last
    // half. Sweet spot turns out to be the widths and partitions below.
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

    uint64_t mask = (width == 64 ? ~0ULL : ((1ULL << (width == 64 ? 0 : width)) - 1ULL)); // Warning free way of computing (1ULL << width) - 1
    while (eq == (((v >> (width * start)) & mask) != 0)) {
        start++;
    }

    return start;
}

} // anonymous namesapce


template<bool find_max, size_t w> bool Array::minmax(int64_t& result, size_t start, size_t end, size_t* return_ndx) const
{
    size_t best_index = 0;

    if (end == size_t(-1))
        end = m_size;
    TIGHTDB_ASSERT(start < m_size && end <= m_size && start < end);

    if (m_size == 0)
        return false;

    if (w == 0) {
        if (return_ndx)
            *return_ndx = best_index;
        result = 0;
        return true;
    }

    int64_t m = Get<w>(start);
    ++start;

#if 0 // We must now return both value AND index of result. SSE does not support finding index, so we've disabled it
#ifdef TIGHTDB_COMPILER_SSE
    if (sseavx<42>()) {
        // Test manually until 128 bit aligned
        for (; (start < end) && (((size_t(m_data) & 0xf) * 8 + start * w) % (128) != 0); start++) {
            if (find_max ? Get<w>(start) > m : Get<w>(start) < m) {
                m = Get<w>(start);
                best_index = start;
            }
        }

        if ((w == 8 || w == 16 || w == 32) && end - start > 2 * sizeof (__m128i) * 8 / no0(w)) {
            __m128i *data = reinterpret_cast<__m128i*>(m_data + start * w / 8);
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
                int64_t v = GetUniversal<w>(reinterpret_cast<char*>(&state2), t);
                if (find_max ? v > m : v < m) {
                    m = v;
                }
            }
        }
    }
#endif
#endif

    for (; start < end; ++start) {
        const int64_t v = Get<w>(start);
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
    TIGHTDB_TEMPEX2(return minmax, true, m_width, (result, start, end, return_ndx));
}

bool Array::minimum(int64_t& result, size_t start, size_t end, size_t* return_ndx) const
{
    TIGHTDB_TEMPEX2(return minmax, false, m_width, (result, start, end, return_ndx));
}

int64_t Array::sum(size_t start, size_t end) const
{
    TIGHTDB_TEMPEX(return sum, m_width, (start, end));
}

template<size_t w> int64_t Array::sum(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = m_size;
    TIGHTDB_ASSERT(start < m_size && end <= m_size && start < end);

    if (w == 0)
        return 0;

    int64_t s = 0;

    // Sum manually until 128 bit aligned
    for (; (start < end) && (((size_t(m_data) & 0xf) * 8 + start * w) % 128 != 0); start++) {
        s += Get<w>(start);
    }

    if (w == 1 || w == 2 || w == 4) {
        // Sum of bitwidths less than a byte (which are always positive)
        // uses a divide and conquer algorithm that is a variation of popolation count:
        // http://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetParallel

        // static values needed for fast sums
        const uint64_t m2  = 0x3333333333333333ULL;
        const uint64_t m4  = 0x0f0f0f0f0f0f0f0fULL;
        const uint64_t h01 = 0x0101010101010101ULL;

        int64_t *data = reinterpret_cast<int64_t*>(m_data + start * w / 8);
        size_t chunks = (end - start) * w / 8 / sizeof (int64_t);

        for (size_t t = 0; t < chunks; t++) {
            if (w == 1) {

/*
#if defined(USE_SSE42) && defined(_MSC_VER) && defined(TIGHTDB_PTR_64)
                    s += __popcnt64(data[t]);
#elif !defined(_MSC_VER) && defined(USE_SSE42) && defined(TIGHTDB_PTR_64)
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
*/

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
        start += sizeof (int64_t) * 8 / no0(w) * chunks;
    }

#ifdef TIGHTDB_COMPILER_SSE
    if (sseavx<42>()) {

        // 2000 items summed 500000 times, 8/16/32 bits, miliseconds:
        // Naive, templated Get<>: 391 371 374
        // SSE:                     97 148 282

        if ((w == 8 || w == 16 || w == 32) && end - start > sizeof (__m128i) * 8 / no0(w)) {
            __m128i* data = reinterpret_cast<__m128i*>(m_data + start * w / 8);
            __m128i sum = {0};
            __m128i sum2;

            size_t chunks = (end - start) * w / 8 / sizeof (__m128i);

            for (size_t t = 0; t < chunks; t++) {
                if (w == 8) {
                    /*
                    // 469 ms AND disadvantage of handling max 64k elements before overflow
                    __m128i vl = _mm_cvtepi8_epi16(data[t]);
                    __m128i vh = data[t];
                    vh.m128i_i64[0] = vh.m128i_i64[1];
                    vh = _mm_cvtepi8_epi16(vh);
                    sum = _mm_add_epi16(sum, vl);
                    sum = _mm_add_epi16(sum, vh);
                    */

                    /*
                    // 424 ms
                    __m128i vl = _mm_unpacklo_epi8(data[t], _mm_set1_epi8(0));
                    __m128i vh = _mm_unpackhi_epi8(data[t], _mm_set1_epi8(0));
                    sum = _mm_add_epi32(sum, _mm_madd_epi16(vl, _mm_set1_epi16(1)));
                    sum = _mm_add_epi32(sum, _mm_madd_epi16(vh, _mm_set1_epi16(1)));
                    */

                    __m128i vl = _mm_cvtepi8_epi16(data[t]);        // sign extend lower words 8->16
                    __m128i vh = data[t];
                    vh = _mm_srli_si128(vh, 8);                     // v >>= 64
                    vh = _mm_cvtepi8_epi16(vh);                     // sign extend lower words 8->16
                    __m128i sum1 = _mm_add_epi16(vl, vh);
                    __m128i sumH = _mm_cvtepi16_epi32(sum1);
                    __m128i sumL = _mm_srli_si128(sum1, 8);         // v >>= 64
                    sumL = _mm_cvtepi16_epi32(sumL);
                    sum = _mm_add_epi32(sum, sumL);
                    sum = _mm_add_epi32(sum, sumH);
                }
                else if (w == 16) {
                    // todo, can overflow for array size > 2^32
                    __m128i vl = _mm_cvtepi16_epi32(data[t]);       // sign extend lower words 16->32
                    __m128i vh = data[t];
                    vh = _mm_srli_si128(vh, 8);                     // v >>= 64
                    vh = _mm_cvtepi16_epi32(vh);                    // sign extend lower words 16->32
                    sum = _mm_add_epi32(sum, vl);
                    sum = _mm_add_epi32(sum, vh);
                }
                else if (w == 32) {
                    __m128i v = data[t];
                    __m128i v0 = _mm_cvtepi32_epi64(v);             // sign extend lower dwords 32->64
                    v = _mm_srli_si128(v, 8);                       // v >>= 64
                    __m128i v1 = _mm_cvtepi32_epi64(v);             // sign extend lower dwords 32->64
                    sum = _mm_add_epi64(sum, v0);
                    sum = _mm_add_epi64(sum, v1);

                    /*
                    __m128i m = _mm_set1_epi32(0xc000);             // test if overflow could happen (still need underflow test).
                    __m128i mm = _mm_and_si128(data[t], m);
                    zz = _mm_or_si128(mm, zz);
                    sum = _mm_add_epi32(sum, data[t]);
                    */
                }
            }
            start += sizeof (__m128i) * 8 / no0(w) * chunks;

            // prevent taking address of 'state' to make the compiler keep it in SSE register in above loop (vc2010/gcc4.6)
            sum2 = sum;

            // Avoid aliasing bug where sum2 might not yet be initialized when accessed by GetUniversal
            char sum3[sizeof sum2];
            memcpy(&sum3, &sum2, sizeof sum2);

            // Sum elements of sum
            for (size_t t = 0; t < sizeof (__m128i) * 8 / ((w == 8 || w == 16) ? 32 : 64); ++t) {
                int64_t v = GetUniversal<(w == 8 || w == 16) ? 32 : 64>(reinterpret_cast<char*>(&sum3), t);
                s += v;
            }
        }
    }
#endif

    // Sum remaining elements
    for (; start < end; ++start)
        s += Get<w>(start);

    return s;
}

size_t Array::count(int64_t value) const TIGHTDB_NOEXCEPT
{
    const uint64_t* next = reinterpret_cast<uint64_t*>(m_data);
    size_t count = 0;
    const size_t end = m_size;
    size_t i = 0;

    // static values needed for fast population count
    const uint64_t m1  = 0x5555555555555555ULL;
    const uint64_t m2  = 0x3333333333333333ULL;
    const uint64_t m4  = 0x0f0f0f0f0f0f0f0fULL;
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

            count += to_size_t(a);
        }
    }
    else if (m_width == 2) {
        if (uint64_t(value) > 3)
            return 0;

        const uint64_t v = ~0ULL/0x3 * value;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL/0x3 * 0x1;

        const size_t chunkvals = 32;
        for (; i + chunkvals <= end; i += chunkvals) {
            uint64_t a = next[i / chunkvals];
            a ^= v;      // zero matching bit segments
            a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
            a &= m1;     // isolate single bit in each segment
            a ^= m1;     // reverse isolated bits
            //if (!a) continue;

            // Population count
            a = (a & m2) + ((a >> 2) & m2);
            a = (a + (a >> 4)) & m4;
            a = (a * h01) >> 56;

            count += to_size_t(a);
        }
    }
    else if (m_width == 4) {
        if (uint64_t(value) > 15)
            return 0;

        const uint64_t v  = ~0ULL/0xF * value;
        const uint64_t m  = ~0ULL/0xF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL/0xF * 0x7;
        const uint64_t c2 = ~0ULL/0xF * 0x3;

        const size_t chunkvals = 16;
        for (; i + chunkvals <= end; i += chunkvals) {
            uint64_t a = next[i / chunkvals];
            a ^= v;      // zero matching bit segments
            a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
            a |= (a >> 2) & c2;
            a &= m;     // isolate single bit in each segment
            a ^= m;     // reverse isolated bits

            // Population count
            a = (a + (a >> 4)) & m4;
            a = (a * h01) >> 56;

            count += to_size_t(a);
        }
    }
    else if (m_width == 8) {
        if (value > 0x7FLL || value < -0x80LL)
            return 0; // by casting?

        const uint64_t v  = ~0ULL/0xFF * value;
        const uint64_t m  = ~0ULL/0xFF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL/0xFF * 0x7F;
        const uint64_t c2 = ~0ULL/0xFF * 0x3F;
        const uint64_t c3 = ~0ULL/0xFF * 0x0F;

        const size_t chunkvals = 8;
        for (; i + chunkvals <= end; i += chunkvals) {
            uint64_t a = next[i / chunkvals];
            a ^= v;      // zero matching bit segments
            a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
            a |= (a >> 2) & c2;
            a |= (a >> 4) & c3;
            a &= m;     // isolate single bit in each segment
            a ^= m;     // reverse isolated bits

            // Population count
            a = (a * h01) >> 56;

            count += to_size_t(a);
        }
    }
    else if (m_width == 16) {
        if (value > 0x7FFFLL || value < -0x8000LL)
            return 0; // by casting?

        const uint64_t v  = ~0ULL/0xFFFF * value;
        const uint64_t m  = ~0ULL/0xFFFF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL/0xFFFF * 0x7FFF;
        const uint64_t c2 = ~0ULL/0xFFFF * 0x3FFF;
        const uint64_t c3 = ~0ULL/0xFFFF * 0x0FFF;
        const uint64_t c4 = ~0ULL/0xFFFF * 0x00FF;

        const size_t chunkvals = 4;
        for (; i + chunkvals <= end; i += chunkvals) {
            uint64_t a = next[i / chunkvals];
            a ^= v;      // zero matching bit segments
            a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
            a |= (a >> 2) & c2;
            a |= (a >> 4) & c3;
            a |= (a >> 8) & c4;
            a &= m;     // isolate single bit in each segment
            a ^= m;     // reverse isolated bits

            // Population count
            a = (a * h01) >> 56;

            count += to_size_t(a);
        }
    }
    else if (m_width == 32) {
        int32_t v = int32_t(value);
        const int32_t* d = reinterpret_cast<int32_t*>(m_data);
        for (; i < end; ++i) {
            if (d[i] == v)
                ++count;
        }
        return count;
    }
    else if (m_width == 64) {
        const int64_t* d = reinterpret_cast<int64_t*>(m_data);
        for (; i < end; ++i) {
            if (d[i] == value)
                ++count;
        }
        return count;
    }

    // Check remaining elements
    for (; i < end; ++i)
        if (value == get(i))
            ++count;

    return count;
}

size_t Array::calc_aligned_byte_size(size_t size, int width)
{
    TIGHTDB_ASSERT(width != 0 && (width & (width - 1)) == 0); // Is a power of two
    size_t max = numeric_limits<size_t>::max();
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
        throw RuntimeError("Byte size overflow");
    TIGHTDB_ASSERT(byte_size > 0);
    size_t aligned_byte_size = ((byte_size-1) | 7) + 1; // 8-byte alignment
    return aligned_byte_size;
}

size_t Array::CalcByteLen(size_t count, size_t width) const
{
    TIGHTDB_ASSERT(get_wtype_from_header(get_header_from_data(m_data)) == wtype_Bits);

    // FIXME: Consider calling `calc_aligned_byte_size(size)`
    // instead. Note however, that CalcByteLen() is supposed to return
    // the unaligned byte size. It is probably the case that no harm
    // is done by returning the aligned version, and most callers of
    // CalcByteLen() will actually benefit if CalcByteLen() was
    // changed to always return the aligned byte size.

    // FIXME: This arithemtic could overflow. Consider using <tightdb/util/safe_int_ops.hpp>
    size_t bits = count * width;
    size_t bytes = (bits+7) / 8; // round up
    return bytes + header_size; // add room for 8 byte header
}

size_t Array::CalcItemCount(size_t bytes, size_t width) const TIGHTDB_NOEXCEPT
{
    if (width == 0)
        return numeric_limits<size_t>::max(); // Zero width gives "infinite" space

    size_t bytes_data = bytes - header_size; // ignore 8 byte header
    size_t total_bits = bytes_data * 8;
    return total_bits / width;
}

MemRef Array::clone(const char* header, Allocator& alloc, Allocator& target_alloc)
{
    if (!get_hasrefs_from_header(header)) {
        // This array has no subarrays, so we can make a byte-for-byte
        // copy, which is more efficient.

        // Calculate size of new array in bytes
        size_t size = get_byte_size_from_header(header);

        // Create the new array
        MemRef mem = target_alloc.alloc(size); // Throws
        char* clone_header = mem.m_addr;

        // Copy contents
        const char* src_begin = header;
        const char* src_end   = header + size;
        char*       dst_begin = clone_header;
        copy(src_begin, src_end, dst_begin);

        // Update with correct capacity
        set_header_capacity(size, clone_header);

        return mem;
    }

    // Refs are integers, and integers arrays use wtype_Bits.
    TIGHTDB_ASSERT(get_wtype_from_header(header) == wtype_Bits);

    Array array((Array::no_prealloc_tag()));
    array.CreateFromHeaderDirect(const_cast<char*>(header));

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
        bool is_subarray = value != 0 && value % 2 == 0;
        if (!is_subarray) {
            new_array.add(value); // Throws
            continue;
        }

        ref_type ref = to_ref(value);
        const char* subheader = alloc.translate(ref);
        MemRef new_mem = clone(subheader, alloc, target_alloc); // Throws
        dg_2.reset(new_mem.m_ref);
        value = new_mem.m_ref; // FIXME: Dangerous cast (unsigned -> signed)
        new_array.add(value); // Throws
        dg_2.release();
    }

    dg.release();
    return new_array.get_mem();
}

void Array::copy_on_write()
{
    if (!m_alloc.is_read_only(m_ref))
        return;

    // Calculate size in bytes (plus a bit of matchcount room for expansion)
    size_t size = CalcByteLen(m_size, m_width);
    size_t rest = (~size & 0x7) + 1;
    if (rest < 8)
        size += rest; // 64bit blocks
    size_t new_size = size + 64;

    // Create new copy of array
    MemRef mref = m_alloc.alloc(new_size); // Throws
    const char* old_begin = get_header_from_data(m_data);
    const char* old_end   = get_header_from_data(m_data) + size;
    char* new_begin = mref.m_addr;
    copy(old_begin, old_end, new_begin);

    ref_type old_ref = m_ref;

    // Update internal data
    m_ref = mref.m_ref;
    m_data = get_data_from_header(new_begin);
    m_capacity = CalcItemCount(new_size, m_width);
    TIGHTDB_ASSERT_DEBUG(m_capacity > 0);

    // Update capacity in header. Uses m_data to find header, so
    // m_data must be initialized correctly first.
    set_header_capacity(new_size);

    update_parent();

    // Mark original as deleted, so that the space can be reclaimed in
    // future commits, when no versions are using it anymore
    m_alloc.free_(old_ref, old_begin);
}


namespace {

template<size_t width>
void set_direct(char* data, size_t ndx, int_fast64_t value) TIGHTDB_NOEXCEPT
{
    // FIXME: The code below makes the non-portable assumption that
    // negative number are represented using two's complement. See
    // Replication::encode_int() for a possible solution. This is not
    // guaranteed by C++03.
    //
    // FIXME: The code below makes the non-portable assumption that
    // the types `int8_t`, `int16_t`, `int32_t`, and `int64_t`
    // exist. This is not guaranteed by C++03.
    if (width == 0) {
        TIGHTDB_ASSERT_DEBUG(value == 0);
        return;
    }
    else if (width == 1) {
        TIGHTDB_ASSERT_DEBUG(0 <= value && value <= 0x01);
        size_t byte_ndx = ndx / 8;
        size_t bit_ndx  = ndx % 8;
        typedef unsigned char uchar;
        uchar* p = reinterpret_cast<uchar*>(data) + byte_ndx;
        *p = uchar((*p & ~(0x01 << bit_ndx)) | (int(value) & 0x01) << bit_ndx);
    }
    else if (width == 2) {
        TIGHTDB_ASSERT_DEBUG(0 <= value && value <= 0x03);
        size_t byte_ndx = ndx / 4;
        size_t bit_ndx  = ndx % 4 * 2;
        typedef unsigned char uchar;
        uchar* p = reinterpret_cast<uchar*>(data) + byte_ndx;
        *p = uchar((*p & ~(0x03 << bit_ndx)) | (int(value) & 0x03) << bit_ndx);
    }
    else if (width == 4) {
        TIGHTDB_ASSERT_DEBUG(0 <= value && value <= 0x0F);
        size_t byte_ndx = ndx / 2;
        size_t bit_ndx  = ndx % 2 * 4;
        typedef unsigned char uchar;
        uchar* p = reinterpret_cast<uchar*>(data) + byte_ndx;
        *p = uchar((*p & ~(0x0F << bit_ndx)) | (int(value) & 0x0F) << bit_ndx);
    }
    else if (width == 8) {
        TIGHTDB_ASSERT_DEBUG(numeric_limits<int8_t>::min() <= value &&
                             value <= numeric_limits<int8_t>::max());
        *(reinterpret_cast<int8_t*>(data) + ndx) = int8_t(value);
    }
    else if (width == 16) {
        TIGHTDB_ASSERT_DEBUG(numeric_limits<int16_t>::min() <= value &&
                             value <= numeric_limits<int16_t>::max());
        *(reinterpret_cast<int16_t*>(data) + ndx) = int16_t(value);
    }
    else if (width == 32) {
        TIGHTDB_ASSERT_DEBUG(numeric_limits<int32_t>::min() <= value &&
                             value <= numeric_limits<int32_t>::max());
        *(reinterpret_cast<int32_t*>(data) + ndx) = int32_t(value);
    }
    else if (width == 64) {
        TIGHTDB_ASSERT_DEBUG(numeric_limits<int64_t>::min() <= value &&
                             value <= numeric_limits<int64_t>::max());
        *(reinterpret_cast<int64_t*>(data) + ndx) = int64_t(value);
    }
    else {
        TIGHTDB_ASSERT_DEBUG(false);
    }
}

template<size_t width>
void fill_direct(char* data, size_t begin, size_t end, int_fast64_t value) TIGHTDB_NOEXCEPT
{
    for (size_t i = begin; i != end; ++i)
        set_direct<width>(data, i, value);
}

} // anonymous namespace

MemRef Array::create(Type type, bool context_flag, WidthType width_type, size_t size,
                     int_fast64_t value, Allocator& alloc)
{
    TIGHTDB_ASSERT(value == 0 || width_type == wtype_Bits);
    TIGHTDB_ASSERT(size  == 0 || width_type != wtype_Ignore);

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
    size_t byte_size = max(byte_size_0, initial_capacity+0);
    MemRef mem = alloc.alloc(byte_size); // Throws
    char* header = mem.m_addr;

    init_header(header, is_inner_bptree_node, has_refs, context_flag, width_type,
                width, size, byte_size);

    if (value != 0) {
        char* data = get_data_from_header(header);
        size_t begin = 0, end = size;
        TIGHTDB_TEMPEX(fill_direct, width, (data, begin, end, value));
    }

    return mem;
}


// FIXME: It may be worth trying to combine this with copy_on_write()
// to avoid two copies.
void Array::alloc(size_t size, size_t width)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(!m_alloc.is_read_only(m_ref));
    TIGHTDB_ASSERT(m_capacity > 0);
    if (m_capacity < size || width != m_width) {
        size_t needed_bytes   = CalcByteLen(size, width);
        size_t orig_capacity_bytes = get_capacity_from_header();
        size_t capacity_bytes = orig_capacity_bytes;

        if (capacity_bytes < needed_bytes) {
            // Double to avoid too many reallocs (or initialize to initial size)
            capacity_bytes = capacity_bytes * 2; // FIXME: Highly prone to overflow on 32-bit systems

            // If doubling is not enough, expand enough to fit
            if (capacity_bytes < needed_bytes) {
                size_t rest = (~needed_bytes & 0x7) + 1;
                capacity_bytes = needed_bytes;
                if (rest < 8)
                    capacity_bytes += rest; // 64bit align
            }

            // Allocate and update header
            char* header = get_header_from_data(m_data);
            MemRef mem_ref = m_alloc.realloc_(m_ref, header, orig_capacity_bytes,
                                              capacity_bytes); // Throws
            header = mem_ref.m_addr;
            set_header_width(int(width), header);
            set_header_size(size, header);
            set_header_capacity(capacity_bytes, header);

            // Update this accessor and its ancestors
            m_ref      = mem_ref.m_ref;
            m_data     = get_data_from_header(header);
            m_capacity = CalcItemCount(capacity_bytes, width);
            // FIXME: Trouble when this one throws. We will then leave
            // this array instance in a corrupt state
            update_parent(); // Throws
            return;
        }

        m_capacity = CalcItemCount(capacity_bytes, width);
        set_header_width(int(width));
    }

    // Update header
    set_header_size(size);
}


void Array::set_width(size_t width) TIGHTDB_NOEXCEPT
{
    TIGHTDB_TEMPEX(set_width, width, ());
}

template<size_t width> void Array::set_width() TIGHTDB_NOEXCEPT
{
    if (width == 0) {
        m_lbound = 0;
        m_ubound = 0;
    }
    else if (width == 1) {
        m_lbound = 0;
        m_ubound = 1;
    }
    else if (width == 2) {
        m_lbound = 0;
        m_ubound = 3;
    }
    else if (width == 4) {
        m_lbound = 0;
        m_ubound = 15;
    }
    else if (width == 8) {
        m_lbound = -0x80LL;
        m_ubound =  0x7FLL;
    }
    else if (width == 16) {
        m_lbound = -0x8000LL;
        m_ubound =  0x7FFFLL;
    }
    else if (width == 32) {
        m_lbound = -0x80000000LL;
        m_ubound =  0x7FFFFFFFLL;
    }
    else if (width == 64) {
        m_lbound = -0x8000000000000000LL;
        m_ubound =  0x7FFFFFFFFFFFFFFFLL;
    }
    else {
        TIGHTDB_ASSERT_DEBUG(false);
    }

    m_width = width;
    // m_getter = temp is a workaround for a bug in VC2010 that makes it return address of get() instead of Get<n>
    // if the declaration and association of the getter are on two different source lines
    Getter temp_getter = &Array::Get<width>;
    m_getter = temp_getter;

    ChunkGetter temp_chunk_getter = &Array::get_chunk<width>;
    m_chunk_getter = temp_chunk_getter;

    Setter temp_setter = &Array::Set<width>;
    m_setter = temp_setter;

    Finder feq = &Array::find<Equal, act_ReturnFirst, width>;
    m_finder[cond_Equal] = feq;

    Finder fne = &Array::find<NotEqual, act_ReturnFirst, width>;
    m_finder[cond_NotEqual]  = fne;

    Finder fg = &Array::find<Greater, act_ReturnFirst, width>;
    m_finder[cond_Greater] = fg;

    Finder fl =  &Array::find<Less, act_ReturnFirst, width>;
    m_finder[cond_Less] = fl;
}

// This method reads 8 concecutive values into res[8], starting from index 'ndx'. It's allowed for the 8 values to
// exceed array length; in this case, remainder of res[8] will be left untouched.
template<size_t w> void Array::get_chunk(size_t ndx, int64_t res[8]) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < m_size);

    // To make Valgrind happy. Todo, I *think* it should work without, now, but if it reappears, add memset again.
    // memset(res, 0, 8*8);

    if (TIGHTDB_X86_OR_X64_TRUE && (w == 1 || w == 2 || w == 4) && ndx + 32 < m_size) {
        // This method is *multiple* times faster than performing 8 times Get<w>, even if unrolled. Apparently compilers
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
        for(; i + ndx < m_size && i < 8; i++)
            res[i] = Get<w>(ndx + i);

        for(; i < 8; i++)
            res[i] = 0;
    }

#ifdef TIGHTDB_DEBUG
    for(int j = 0; j + ndx < m_size && j < 8; j++) {
        int64_t expected = Get<w>(ndx + j);
        if (res[j] != expected)
            TIGHTDB_ASSERT(false);
    }
#endif


}


template<size_t width> void Array::Set(size_t ndx, int64_t value)
{
    set_direct<width>(m_data, ndx, value);
}


// Sort array.
void Array::sort()
{
    TIGHTDB_TEMPEX(sort, m_width, ());
}


// Find max and min value, but break search if difference exceeds 'maxdiff' (in which case *min and *max is set to 0)
// Useful for counting-sort functions
template<size_t w>bool Array::MinMax(size_t from, size_t to, uint64_t maxdiff, int64_t *min, int64_t *max) const
{
    int64_t min2;
    int64_t max2;
    size_t t;

    max2 = Get<w>(from);
    min2 = max2;

    for (t = from + 1; t < to; t++) {
        int64_t v = Get<w>(t);
        // Utilizes that range test is only needed if max2 or min2 were changed
        if (v < min2) {
            min2 = v;
            if (uint64_t(max2 - min2) > maxdiff)
                break;
        }
        else if (v > max2) {
            max2 = v;
            if (uint64_t(max2 - min2) > maxdiff)
                break;
        }
    }

    if (t < to) {
        *max = 0;
        *min = 0;
        return false;
    }
    else {
        *max = max2;
        *min = min2;
        return true;
    }
}

// Take index pointers to elements as argument and sort the pointers according to values they point at. Leave m_array untouched. The ref array
// is allowed to contain fewer elements than m_array.
void Array::ReferenceSort(Array& ref) const
{
    TIGHTDB_TEMPEX(ReferenceSort, m_width, (ref));
}

template<size_t w>void Array::ReferenceSort(Array& ref) const
{
    if (m_size < 2)
        return;

    // in avg case QuickSort is O(n*log(n)) and CountSort O(n + range), and memory usage is sizeof(size_t)*range for CountSort.
    // So we chose range < m_size as treshold for deciding which to use

    // If range isn't suited for CountSort, it's *probably* discovered very early, within first few values, in most practical cases,
    // and won't add much wasted work. Max wasted work is O(n) which isn't much compared to QuickSort.

//    int64_t min;
//    int64_t max;
//  bool b = MinMax<w>(0, m_size, m_size, &min, &max); // auto detect
//  bool b = MinMax<w>(0, m_size, -1, &min, &max); // force count sort
//  bool b = MinMax<w>(0, m_size, 0, &min, &max); // force quicksort

    /*
    // Count sort disabled for simplicity/stability
    if (b) {
        Array res;
        Array count;

//        Todo, Preset crashes for unknown reasons but would be faster.
//      res.Preset(0, m_size, m_size);
//      count.Preset(0, m_size, max - min + 1);

        for (int64_t t = 0; t < max - min + 1; t++)
            count.add(0);

        // Count occurences of each value
        for (size_t t = 0; t < m_size; t++) {
            size_t i = to_ref(Get<w>(t) - min);
            count.set(i, count.get(i) + 1);
        }

        // Accumulate occurences
        for (size_t t = 1; t < count.size(); t++) {
            count.set(t, count.get(t) + count.get(t - 1));
        }

        for (size_t t = 0; t < m_size; t++)
            res.add(0);

        for (size_t t = m_size; t > 0; t--) {
            size_t v = to_ref(Get<w>(t - 1) - min);
            size_t i = count.get_as_ref(v);
            count.set(v, count.get(v) - 1);
            res.set(i - 1, ref.get(t - 1));
        }

        // Copy result into ref
        for (size_t t = 0; t < res.size(); t++)
            ref.set(t, res.get(t));

        res.destroy();
        count.destroy();
    }
    else
    */
    {
        ReferenceQuickSort(ref);
    }
}

// Sort array
template<size_t w> void Array::sort()
{
    if (m_size < 2)
        return;

    size_t lo = 0;
    size_t hi = m_size - 1;
    vector<size_t> count;
    int64_t min;
    int64_t max;
    bool b = false;

    // in avg case QuickSort is O(n*log(n)) and CountSort O(n + range), and memory usage is sizeof(size_t)*range for CountSort.
    // Se we chose range < m_size as treshold for deciding which to use
    if (m_width <= 8) {
        max = m_ubound;
        min = m_lbound;
        b = true;
    }
    else {
        // If range isn't suited for CountSort, it's *probably* discovered very early, within first few values,
        // in most practical cases, and won't add much wasted work. Max wasted work is O(n) which isn't much
        // compared to QuickSort.
        b = MinMax<w>(lo, hi + 1, m_size, &min, &max);
    }

    if (b) {
        for (int64_t t = 0; t < max - min + 1; t++)
            count.push_back(0);

        // Count occurences of each value
        for (size_t t = lo; t <= hi; t++) {
            size_t i = to_size_t(Get<w>(t) - min); // FIXME: The value of (Get<w>(t) - min) cannot necessarily be stored in size_t.
            count[i]++;
        }

        // Overwrite original array with sorted values
        size_t dst = 0;
        for (int64_t i = 0; i < max - min + 1; i++) {
            size_t c = count[unsigned(i)];
            for (size_t j = 0; j < c; j++) {
                Set<w>(dst, i + min);
                dst++;
            }
        }
    }
    else {
        QuickSort(lo, hi);
    }

    return;
}

void Array::ReferenceQuickSort(Array& ref) const
{
    TIGHTDB_TEMPEX(ReferenceQuickSort, m_width, (0, m_size - 1, ref));
}

template<size_t w> void Array::ReferenceQuickSort(size_t lo, size_t hi, Array& ref) const
{
    // Quicksort based on
    // http://www.inf.fh-flensburg.de/lang/algorithmen/sortieren/quick/quicken.htm
    int i = int(lo);
    int j = int(hi);

    /*
    // Swap both values and references but lookup values directly: 2.85 sec
    // comparison element x
    const size_t ndx = (lo + hi)/2;
    const int64_t x = (size_t)get(ndx);

    // partition
    do {
        while (get(i) < x) i++;
        while (get(j) > x) j--;
        if (i <= j) {
            size_t h = ref.get(i);
            ref.set(i, ref.get(j));
            ref.set(j, h);
        //  h = get(i);
        //  set(i, get(j));
        //  set(j, h);
            i++; j--;
        }
    } while (i <= j);
*/

    // Lookup values indirectly through references, but swap only references: 2.60 sec
    // Templated get/set: 2.40 sec (todo, enable again)
    // comparison element x
    const size_t ndx = (lo + hi)/2;
    const size_t target_ndx = to_size_t(ref.get(ndx));
    const int64_t x = get(target_ndx);

    // partition
    do {
        while (get(to_size_t(ref.get(i))) < x) ++i;
        while (get(to_size_t(ref.get(j))) > x) --j;
        if (i <= j) {
            size_t h = to_size_t(ref.get(i));
            ref.set(i, ref.get(j));
            ref.set(j, h);
            ++i; --j;
        }
    }
    while (i <= j);

    //  recursion
    if (int(lo) < j)
        ReferenceQuickSort<w>(lo, j, ref);
    if (i < int(hi))
        ReferenceQuickSort<w>(i, hi, ref);
}


void Array::QuickSort(size_t lo, size_t hi)
{
    TIGHTDB_TEMPEX(QuickSort, m_width, (lo, hi);)
}

template<size_t w> void Array::QuickSort(size_t lo, size_t hi)
{
    // Quicksort based on
    // http://www.inf.fh-flensburg.de/lang/algorithmen/sortieren/quick/quicken.htm
    int i = int(lo);
    int j = int(hi);

    // comparison element x
    const size_t ndx = (lo + hi)/2;
    const int64_t x = get(ndx);

    // partition
    do {
        while (get(i) < x) ++i;
        while (get(j) > x) --j;
        if (i <= j) {
            int64_t h = get(i);
            set(i, get(j));
            set(j, h);
            ++i; --j;
        }
    }
    while (i <= j);

    //  recursion
    if (int(lo) < j)
        QuickSort(lo, j);
    if (i < int(hi))
        QuickSort(i, hi);
}

vector<int64_t> Array::ToVector() const
{
    vector<int64_t> v;
    const size_t count = size();
    for (size_t t = 0; t < count; ++t)
        v.push_back(get(t));
    return v;
}

bool Array::compare_int(const Array& a) const TIGHTDB_NOEXCEPT
{
    if (a.size() != size())
        return false;

    for (size_t i = 0; i < size(); ++i) {
        if (get(i) != a.get(i))
            return false;
    }

    return true;
}


ref_type Array::insert_bptree_child(Array& offsets, size_t orig_child_ndx,
                                    ref_type new_sibling_ref, TreeInsertBase& state)
{
    // When a child is split, the new child must always be inserted
    // after the original
    size_t orig_child_ref_ndx = 1 + orig_child_ndx;
    size_t insert_ndx = orig_child_ref_ndx + 1;

    TIGHTDB_ASSERT_DEBUG(insert_ndx <= size() - 1);
    if (TIGHTDB_LIKELY(size() < 1 + TIGHTDB_MAX_BPNODE_SIZE + 1)) {
        // Case 1/2: This parent has space for the new child, so it
        // does not have to be split.
        insert(insert_ndx, new_sibling_ref); // Throws
        // +2 because stored value is 1 + 2*total_elems_in_subtree
        adjust(size()-1, +2); // Throws
        if (offsets.is_attached()) {
            size_t elem_ndx_offset = orig_child_ndx > 0 ?
                to_size_t(offsets.get(orig_child_ndx-1)) : 0;
            offsets.insert(orig_child_ndx, elem_ndx_offset + state.m_split_offset); // Throws
            offsets.adjust(orig_child_ndx+1, offsets.size(), +1); // Throws
        }
        return 0; // Parent node was not split
    }

    // Case 2/2: This parent is full, so it needs to be plit.
    //
    // We first create a new sibling of the parent, and then we move
    // some of the children over. The caller must insert the new
    // sibling after the original.
    size_t elem_ndx_offset = 0;
    if (orig_child_ndx > 0) {
        if (offsets.is_attached()) {
            elem_ndx_offset = size_t(offsets.get(orig_child_ndx-1));
        }
        else {
            int_fast64_t elems_per_child = get(0) / 2;
            elem_ndx_offset = size_t(orig_child_ndx * elems_per_child);
        }
    }

    Allocator& alloc = get_alloc();
    Array new_sibling(alloc), new_offsets(alloc);
    new_sibling.create(type_InnerBptreeNode); // Throws
    if (offsets.is_attached()) {
        new_offsets.set_parent(&new_sibling, 0);
        new_offsets.create(type_Normal); // Throws
        // FIXME: Dangerous cast here (unsigned -> signed)
        new_sibling.add(new_offsets.get_ref()); // Throws
    }
    else {
        int_fast64_t v = get(0); // v = 1 + 2 * elems_per_child
        new_sibling.add(v); // Throws
    }
    size_t new_split_offset, new_split_size;
    if (insert_ndx - 1 >= TIGHTDB_MAX_BPNODE_SIZE) {
        TIGHTDB_ASSERT(insert_ndx - 1 == TIGHTDB_MAX_BPNODE_SIZE);
        // Case 1/2: The split child was the last child of the parent
        // to be split. In this case the parent may or may not be on
        // the compact form.
        new_split_offset = elem_ndx_offset + state.m_split_offset;
        new_split_size   = elem_ndx_offset + state.m_split_size;
        new_sibling.add(new_sibling_ref); // Throws
    }
    else {
        // Case 2/2: The split child was not the last child of the
        // parent to be split. Since this is not possible during
        // 'append', we can safely assume that the parent node is on
        // the general form.
        TIGHTDB_ASSERT(new_offsets.is_attached());
        new_split_offset = elem_ndx_offset + state.m_split_size;
        new_split_size = to_size_t(back()/2) + 1;
        TIGHTDB_ASSERT(size() >= 2);
        size_t num_children = size() - 2;
        TIGHTDB_ASSERT(num_children >= 1); // invar:bptree-nonempty-inner
        // Move some refs over
        size_t child_refs_end = 1 + num_children;
        for (size_t i = insert_ndx; i != child_refs_end; ++i)
            new_sibling.add(get(i)); // Throws
        // Move some offsets over
        size_t offsets_end = num_children - 1;
        for (size_t i = orig_child_ndx+1; i != offsets_end; ++i) {
            size_t offset = to_size_t(offsets.get(i));
            // FIXME: Dangerous cast here (unsigned -> signed)
            new_offsets.add(offset - (new_split_offset-1)); // Throws
        }
        // Update original parent
        erase(insert_ndx+1, child_refs_end);
        // FIXME: Dangerous cast here (unsigned -> signed)
        set(insert_ndx, new_sibling_ref); // Throws
        offsets.erase(orig_child_ndx+1, offsets_end);
        // FIXME: Dangerous cast here (unsigned -> signed)
        offsets.set(orig_child_ndx, elem_ndx_offset + state.m_split_offset); // Throws
    }
    // FIXME: Dangerous cast here (unsigned -> signed)
    int_fast64_t v = new_split_offset; // total_elems_in_subtree
    set(size() - 1, 1 + 2*v); // Throws
    // FIXME: Dangerous cast here (unsigned -> signed)
    v = new_split_size - new_split_offset; // total_elems_in_subtree
    new_sibling.add(1 + 2*v); // Throws
    state.m_split_offset = new_split_offset;
    state.m_split_size   = new_split_size;
    return new_sibling.get_ref();
}


// FIXME: Not exception safe (leaks are possible).
ref_type Array::bptree_leaf_insert(size_t ndx, int64_t value, TreeInsertBase& state)
{
    size_t leaf_size = size();
    TIGHTDB_ASSERT_DEBUG(leaf_size <= TIGHTDB_MAX_BPNODE_SIZE);
    if (leaf_size < ndx)
        ndx = leaf_size;
    if (TIGHTDB_LIKELY(leaf_size < TIGHTDB_MAX_BPNODE_SIZE)) {
        insert(ndx, value); // Throws
        return 0; // Leaf was not split
    }

    // Split leaf node
    Array new_leaf(m_alloc);
    new_leaf.create(has_refs() ? type_HasRefs : type_Normal); // Throws
    if (ndx == leaf_size) {
        new_leaf.add(value); // Throws
        state.m_split_offset = ndx;
    }
    else {
        for (size_t i = ndx; i != leaf_size; ++i)
            new_leaf.add(get(i)); // Throws
        truncate(ndx); // Throws
        add(value); // Throws
        state.m_split_offset = ndx + 1;
    }
    state.m_split_size = leaf_size + 1;
    return new_leaf.get_ref();
}


#ifdef TIGHTDB_DEBUG

void Array::print() const
{
    cout << hex << get_ref() << dec << ": (" << size() << ") ";
    for (size_t i = 0; i < size(); ++i) {
        if (i)
            cout << ", ";
        cout << get(i);
    }
    cout << "\n";
}

void Array::Verify() const
{
    TIGHTDB_ASSERT(is_attached());

    TIGHTDB_ASSERT(m_width == 0 || m_width == 1 || m_width == 2 || m_width == 4 ||
                   m_width == 8 || m_width == 16 || m_width == 32 || m_width == 64);

    if (!m_parent)
        return;

    // Check that parent is set correctly
    ref_type ref_in_parent = m_parent->get_child_ref(m_ndx_in_parent);
    TIGHTDB_ASSERT(ref_in_parent == m_ref);
}


namespace {

typedef Tuple<TypeCons<size_t, TypeCons<int, TypeCons<bool, void> > > > VerifyBptreeResult;

// Returns (num_elems, leaf-level, general_form)
VerifyBptreeResult verify_bptree(const Array& node, Array::LeafVerifier leaf_verifier)
{
    node.Verify();

    // This node must not be a leaf
    TIGHTDB_ASSERT(node.get_type() == Array::type_InnerBptreeNode);

    TIGHTDB_ASSERT(node.size() >= 2);
    size_t num_children = node.size() - 2;

    // Verify invar:bptree-nonempty-inner
    TIGHTDB_ASSERT(num_children >= 1);

    Allocator& alloc = node.get_alloc();
    Array offsets(alloc);
    size_t elems_per_child = 0;
    bool general_form;
    {
        int_fast64_t first_value = node.get(0);
        general_form = first_value % 2 == 0;
        if (general_form) {
            offsets.init_from_ref(to_ref(first_value));
            offsets.Verify();
            TIGHTDB_ASSERT(offsets.get_type() == Array::type_Normal);
            TIGHTDB_ASSERT(offsets.size() == num_children - 1);
        }
        else {
            TIGHTDB_ASSERT(!int_cast_with_overflow_detect(first_value/2, elems_per_child));
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
            elems_in_child = (*leaf_verifier)(MemRef(child_header, child_ref), alloc);
            // Verify invar:bptree-nonempty-leaf
            TIGHTDB_ASSERT(elems_in_child >= 1);
            leaf_level_of_child = 0;
        }
        else {
            Array child(alloc);
            child.init_from_ref(child_ref);
            VerifyBptreeResult r = verify_bptree(child, leaf_verifier);
            elems_in_child = at<0>(r);
            leaf_level_of_child = at<1>(r);
            // Verify invar:bptree-node-form
            bool child_on_general_form = at<2>(r);
            TIGHTDB_ASSERT(general_form || !child_on_general_form);
        }
        if (i == 0)
            leaf_level_of_children = leaf_level_of_child;
        // Verify invar:bptree-leaf-depth
        TIGHTDB_ASSERT(leaf_level_of_child == leaf_level_of_children);
        // Check integrity of aggregated per-child element counts
        TIGHTDB_ASSERT(!int_add_with_overflow_detect(num_elems, elems_in_child));
        if (general_form) {
            if (i < num_children - 1)
                TIGHTDB_ASSERT(int_equal_to(num_elems, offsets.get(i)));
        }
        else { // Compact form
            if (i < num_children - 1) {
                TIGHTDB_ASSERT(elems_in_child == elems_per_child);
            }
            else {
                TIGHTDB_ASSERT(elems_in_child <= elems_per_child);
            }
        }
    }
    TIGHTDB_ASSERT(leaf_level_of_children != -1);
    {
        int_fast64_t last_value = node.back();
        TIGHTDB_ASSERT(last_value % 2 != 0);
        size_t total_elems = 0;
        TIGHTDB_ASSERT(!int_cast_with_overflow_detect(last_value/2, total_elems));
        TIGHTDB_ASSERT(num_elems == total_elems);
    }
    return tightdb::util::tuple(num_elems, 1 + leaf_level_of_children, general_form);
}

} // anonymous namespace

void Array::verify_bptree(LeafVerifier leaf_verifier) const
{
    ::verify_bptree(*this, leaf_verifier);
}

void Array::dump_bptree_structure(ostream& out, int level, LeafDumper leaf_dumper) const
{
    bool root_is_leaf = !is_inner_bptree_node();
    if (root_is_leaf) {
        (*leaf_dumper)(get_mem(), m_alloc, out, level);
        return;
    }

    int indent = level * 2;
    out << setw(indent) << "" << "Inner node (B+ tree) (ref: "<<get_ref()<<")\n";

    size_t num_elems_in_subtree = size_t(back() / 2);
    out << setw(indent) << "" << "  Number of elements in subtree: "
        ""<<num_elems_in_subtree<<"\n";

    bool compact_form = front() % 2 != 0;
    if (compact_form) {
        size_t elems_per_child = size_t(front() / 2);
        out << setw(indent) << "" << "  Compact form (elements per child: "
            ""<<elems_per_child<<")\n";
    }
    else { // General form
        Array offsets(m_alloc);
        offsets.init_from_ref(to_ref(front()));
        out << setw(indent) << "" << "  General form (offsets_ref: "
            ""<<offsets.get_ref()<<", ";
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
        child.dump_bptree_structure(out, level+1, leaf_dumper);
    }
}

void Array::bptree_to_dot(ostream& out, ToDotHandler& handler) const
{
    bool root_is_leaf = !is_inner_bptree_node();
    if (root_is_leaf) {
        handler.to_dot(get_mem(), get_parent(), get_ndx_in_parent(), out);
        return;
    }

    ref_type ref  = get_ref();
    out << "subgraph cluster_inner_pbtree_node" << ref << " {" << endl;
    out << " label = \"\";" << endl;

    to_dot(out);

    int_fast64_t first_value = get(0);
    if (first_value % 2 == 0) {
        // On general form / has 'offsets' array
        Array offsets(m_alloc);
        offsets.init_from_ref(to_ref(first_value));
        offsets.set_parent(const_cast<Array*>(this), 0);
        offsets.to_dot(out, "Offsets");
    }

    out << "}" << endl;

    size_t num_children = size() - 2;
    size_t child_ref_begin = 1;
    size_t child_ref_end   = 1 + num_children;
    for (size_t i = child_ref_begin; i != child_ref_end; ++i) {
        Array child(m_alloc);
        child.init_from_ref(get_as_ref(i));
        child.set_parent(const_cast<Array*>(this), i);
        child.bptree_to_dot(out, handler);
    }
}


void Array::to_dot(ostream& out, StringData title) const
{
    ref_type ref = get_ref();

    if (title.size() != 0) {
        out << "subgraph cluster_" << ref << " {" << endl;
        out << " label = \"" << title << "\";" << endl;
        out << " color = white;" << endl;
    }

    out << "n" << hex << ref << dec << "[shape=none,label=<";
    out << "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\"><TR>" << endl;

    // Header
    out << "<TD BGCOLOR=\"lightgrey\"><FONT POINT-SIZE=\"7\"> ";
    out << "0x" << hex << ref << dec << "<BR/>";
    if (m_is_inner_bptree_node)
        out << "IsNode<BR/>";
    if (m_has_refs)
        out << "HasRefs<BR/>";
    out << "</FONT></TD>" << endl;

    // Values
    for (size_t i = 0; i < m_size; ++i) {
        int64_t v =  get(i);
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
        out << "</TD>" << endl;
    }

    out << "</TR></TABLE>>];" << endl;

    if (title.size() != 0)
        out << "}" << endl;

    to_dot_parent_edge(out);
}

void Array::to_dot_parent_edge(ostream& out) const
{
    if (ArrayParent* parent = get_parent()) {
        size_t ndx_in_parent = get_ndx_in_parent();
        pair<ref_type, size_t> p = parent->get_to_dot_parent(ndx_in_parent);
        ref_type real_parent_ref = p.first;
        size_t ndx_in_real_parent = p.second;
        out << "n" << hex << real_parent_ref << dec << ":" << ndx_in_real_parent << ""
            " -> n" << hex << get_ref() << dec << endl;
    }
}

pair<ref_type, size_t> Array::get_to_dot_parent(size_t ndx_in_parent) const
{
    return make_pair(get_ref(), ndx_in_parent);
}


namespace {

class MemStatsHandler: public Array::MemUsageHandler {
public:
    MemStatsHandler(MemStats& stats):
        m_stats(stats)
    {
    }
    void handle(ref_type, size_t allocated, size_t used) TIGHTDB_OVERRIDE
    {
        m_stats.allocated += allocated;
        m_stats.used += used;
        m_stats.array_count += 1;
    }
private:
    MemStats& m_stats;
};

} // anonymous namespace


void Array::stats(MemStats& stats) const
{
    MemStatsHandler handler(stats);
    report_memory_usage(handler);
}


void Array::report_memory_usage(MemUsageHandler& handler) const
{
    if (m_has_refs)
        report_memory_usage_2(handler);

    size_t used = get_byte_size();
    size_t allocated;
    if (m_alloc.is_read_only(m_ref)) {
        allocated = used;
    }
    else {
        char* header = get_header_from_data(m_data);
        allocated = get_capacity_from_header(header);
    }
    handler.handle(m_ref, allocated, used);
}


void Array::report_memory_usage_2(MemUsageHandler& handler) const
{
    Array subarray(m_alloc);
    for (size_t i = 0; i < m_size; ++i) {
        int_fast64_t value = get(i);
        // Skip null refs and values that are not refs. Values are not refs when
        // the least significant bit is set.
        if (value == 0 || value % 2 == 1)
            continue;

        size_t used;
        ref_type ref = to_ref(value);
        char* header = m_alloc.translate(ref);
        bool has_refs = get_hasrefs_from_header(header);
        if (has_refs) {
            MemRef mem(header, ref);
            subarray.init_from_mem(mem);
            subarray.report_memory_usage_2(handler);
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
        handler.handle(ref, allocated, used);
    }
}

#endif // TIGHTDB_DEBUG


namespace {

// Direct access methods

template<int w> int64_t get_direct(const char* data, size_t ndx) TIGHTDB_NOEXCEPT
{
    if (w == 0) {
        return 0;
    }
    if (w == 1) {
        size_t offset = ndx >> 3;
        return (data[offset] >> (ndx & 7)) & 0x01;
    }
    if (w == 2) {
        size_t offset = ndx >> 2;
        return (data[offset] >> ((ndx & 3) << 1)) & 0x03;
    }
    if (w == 4) {
        size_t offset = ndx >> 1;
        return (data[offset] >> ((ndx & 1) << 2)) & 0x0F;
    }
    if (w == 8) {
        return *reinterpret_cast<const signed char*>(data + ndx); // FIXME: Lasse, should this not be a cast to 'const int8_t*'?
    }
    if (w == 16) {
        size_t offset = ndx * 2;
        return *reinterpret_cast<const int16_t*>(data + offset);
    }
    if (w == 32) {
        size_t offset = ndx * 4;
        return *reinterpret_cast<const int32_t*>(data + offset);
    }
    if (w == 64) {
        size_t offset = ndx * 8;
        return *reinterpret_cast<const int64_t*>(data + offset);
    }
    TIGHTDB_ASSERT_DEBUG(false);
    return int64_t(-1);
}

inline int64_t get_direct(const char* data, size_t width, size_t ndx) TIGHTDB_NOEXCEPT
{
    TIGHTDB_TEMPEX(return get_direct, width, (data, ndx));
}


template<int width>
inline pair<int_fast64_t, int_fast64_t> get_two(const char* data, size_t ndx) TIGHTDB_NOEXCEPT
{
    return make_pair(to_size_t(get_direct<width>(data, ndx+0)),
                     to_size_t(get_direct<width>(data, ndx+1)));
}

inline pair<int_fast64_t, int_fast64_t> get_two(const char* data, size_t width,
                                                size_t ndx) TIGHTDB_NOEXCEPT
{
    TIGHTDB_TEMPEX(return get_two, width, (data, ndx));
}


// Lower/upper bound in sorted sequence
// ------------------------------------
//
//   3 3 3 4 4 4 5 6 7 9 9 9
//   ^     ^     ^     ^     ^
//   |     |     |     |     |
//   |     |     |     |      -- Lower and upper bound of 15
//   |     |     |     |
//   |     |     |      -- Lower and upper bound of 8
//   |     |     |
//   |     |      -- Upper bound of 4
//   |     |
//   |      -- Lower bound of 4
//   |
//    -- Lower and upper bound of 1
//
// These functions are semantically identical to std::lower_bound() and
// std::upper_bound().
//
// We currently use binary search. See for example
// http://www.tbray.org/ongoing/When/200x/2003/03/22/Binary.
template<int width>
inline size_t lower_bound(const char* data, size_t size, int64_t value) TIGHTDB_NOEXCEPT
{
// The binary search used here is carefully optimized. Key trick is to use a single
// loop controlling variable (size) instead of high/low pair, and to keep updates
// to size done inside the loop independent of comparisons. Further key to speed
// is to avoid branching inside the loop, using conditional moves instead. This
// provides robust performance for random searches, though predictable searches
// might be slightly faster if we used branches instead. The loop unrolling yields
// a final 5-20% speedup depending on circumstances.

    size_t low = 0;

    while (size >= 8) {
        // The following code (at X, Y and Z) is 3 times manually unrolled instances of (A) below.
        // These code blocks must be kept in sync. Meassurements indicate 3 times unrolling to give
        // the best performance. See (A) for comments on the loop body.
        // (X)
        size_t half = size / 2;
        size_t other_half = size - half;
        size_t probe = low + half;
        size_t other_low = low + other_half;
        int64_t v = get_direct<width>(data, probe);
        size = half;
        low = (v < value) ? other_low : low;

        // (Y)
        half = size / 2;
        other_half = size - half;
        probe = low + half;
        other_low = low + other_half;
        v = get_direct<width>(data, probe);
        size = half;
        low = (v < value) ? other_low : low;

        // (Z)
        half = size / 2;
        other_half = size - half;
        probe = low + half;
        other_low = low + other_half;
        v = get_direct<width>(data, probe);
        size = half;
        low = (v < value) ? other_low : low;
    }
    while (size > 0) {
        // (A)
        // To understand the idea in this code, please note that
        // for performance, computation of size for the next iteration
        // MUST be INDEPENDENT of the conditional. This allows the
        // processor to unroll the loop as fast as possible, and it
        // minimizes the length of dependence chains leading up to branches.
        // Making the unfolding of the loop independent of the data being
        // searched, also minimizes the delays incurred by branch
        // mispredictions, because they can be determined earlier 
        // and the speculation corrected earlier.

        // Counterintuitive:
        // To make size independent of data, we cannot always split the 
        // range at the theoretical optimal point. When we determine that
        // the key is larger than the probe at some index K, and prepare 
        // to search the upper part of the range, you would normally start 
        // the search at the next index, K+1, to get the shortest range.
        // We can only do this when splitting a range with odd number of entries.
        // If there is an even number of entries we search from K instead of K+1.
        // This potentially leads to redundant comparisons, but in practice we
        // gain more performance by making the changes to size predictable.

        // if size is even, half and other_half are the same.
        // if size is odd, half is one less than other_half.
        size_t half = size / 2;
        size_t other_half = size - half;
        size_t probe = low + half;
        size_t other_low = low + other_half;
        int64_t v = get_direct<width>(data, probe);
        size = half;
        // for max performance, the line below should compile into a conditional
        // move instruction. Not all compilers do this. To maximize chance
        // of succes, no computation should be done in the branches of the
        // conditional.
        low = (v < value) ? other_low : low;
    };

    return low;
}

// See lower_bound()
template<int width>
inline size_t upper_bound(const char* data, size_t size, int64_t value) TIGHTDB_NOEXCEPT
{
    size_t low = 0;
    while (size >= 8) {
        size_t half = size / 2;
        size_t other_half = size - half;
        size_t probe = low + half;
        size_t other_low = low + other_half;
        int64_t v = get_direct<width>(data, probe);
        size = half;
        low = (value >= v) ? other_low : low;

        half = size / 2;
        other_half = size - half;
        probe = low + half;
        other_low = low + other_half;
        v = get_direct<width>(data, probe);
        size = half;
        low = (value >= v) ? other_low : low;

        half = size / 2;
        other_half = size - half;
        probe = low + half;
        other_low = low + other_half;
        v = get_direct<width>(data, probe);
        size = half;
        low = (value >= v) ? other_low : low;
    }

    while (size > 0) {
        size_t half = size / 2;
        size_t other_half = size - half;
        size_t probe = low + half;
        size_t other_low = low + other_half;
        int64_t v = get_direct<width>(data, probe);
        size = half;
        low = (value >= v) ? other_low : low;
    };

    return low;
}

} // anonymous namespace



size_t Array::lower_bound_int(int64_t value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_TEMPEX(return ::lower_bound, m_width, (m_data, m_size, value));
}

size_t Array::upper_bound_int(int64_t value) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_TEMPEX(return ::upper_bound, m_width, (m_data, m_size, value));
}


void Array::find_all(Column* result, int64_t value, size_t col_offset, size_t begin, size_t end) const
{
    TIGHTDB_ASSERT(begin <= size());
    TIGHTDB_ASSERT(end == npos || (begin <= end && end <= size()));

    if (end == npos)
        end = m_size;

    if (begin == end)
        return; // FIXME: Why do we have to check and early-out here?

    QueryState<int64_t> state;
    state.init(act_FindAll, result, static_cast<size_t>(-1));
    TIGHTDB_TEMPEX3(find, Equal, act_FindAll, m_width, (value, begin, end, col_offset, &state, CallbackDummy()));

    return;
}


bool Array::find(int cond, Action action, int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t> *state) const
{
    if (cond == cond_Equal) {
        if (action == act_ReturnFirst) {
            TIGHTDB_TEMPEX3(return find, Equal, act_ReturnFirst, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Sum) {
            TIGHTDB_TEMPEX3(return find, Equal, act_Sum, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Min) {
            TIGHTDB_TEMPEX3(return find, Equal, act_Min, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Max) {
            TIGHTDB_TEMPEX3(return find, Equal, act_Max, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Count) {
            TIGHTDB_TEMPEX3(return find, Equal, act_Count, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_FindAll) {
            TIGHTDB_TEMPEX3(return find, Equal, act_FindAll, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_CallbackIdx) {
            TIGHTDB_TEMPEX3(return find, Equal, act_CallbackIdx, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
    }
    if (cond == cond_NotEqual) {
        if (action == act_ReturnFirst) {
            TIGHTDB_TEMPEX3(return find, NotEqual, act_ReturnFirst, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Sum) {
            TIGHTDB_TEMPEX3(return find, NotEqual, act_Sum, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Min) {
            TIGHTDB_TEMPEX3(return find, NotEqual, act_Min, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Max) {
            TIGHTDB_TEMPEX3(return find, NotEqual, act_Max, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Count) {
            TIGHTDB_TEMPEX3(return find, NotEqual, act_Count, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_FindAll) {
            TIGHTDB_TEMPEX3(return find, NotEqual, act_FindAll, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_CallbackIdx) {
            TIGHTDB_TEMPEX3(return find, NotEqual, act_CallbackIdx, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
    }
    if (cond == cond_Greater) {
        if (action == act_ReturnFirst) {
            TIGHTDB_TEMPEX3(return find, Greater, act_ReturnFirst, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Sum) {
            TIGHTDB_TEMPEX3(return find, Greater, act_Sum, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Min) {
            TIGHTDB_TEMPEX3(return find, Greater, act_Min, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Max) {
            TIGHTDB_TEMPEX3(return find, Greater, act_Max, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Count) {
            TIGHTDB_TEMPEX3(return find, Greater, act_Count, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_FindAll) {
            TIGHTDB_TEMPEX3(return find, Greater, act_FindAll, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_CallbackIdx) {
            TIGHTDB_TEMPEX3(return find, Greater, act_CallbackIdx, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
    }
    if (cond == cond_Less) {
        if (action == act_ReturnFirst) {
            TIGHTDB_TEMPEX3(return find, Less, act_ReturnFirst, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Sum) {
            TIGHTDB_TEMPEX3(return find, Less, act_Sum, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Min) {
            TIGHTDB_TEMPEX3(return find, Less, act_Min, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Max) {
            TIGHTDB_TEMPEX3(return find, Less, act_Max, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Count) {
            TIGHTDB_TEMPEX3(return find, Less, act_Count, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_FindAll) {
            TIGHTDB_TEMPEX3(return find, Less, act_FindAll, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_CallbackIdx) {
            TIGHTDB_TEMPEX3(return find, Less, act_CallbackIdx, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
    }
    if (cond == cond_None) {
        if (action == act_ReturnFirst) {
            TIGHTDB_TEMPEX3(return find, None, act_ReturnFirst, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Sum) {
            TIGHTDB_TEMPEX3(return find, None, act_Sum, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Min) {
            TIGHTDB_TEMPEX3(return find, None, act_Min, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Max) {
            TIGHTDB_TEMPEX3(return find, None, act_Max, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Count) {
            TIGHTDB_TEMPEX3(return find, None, act_Count, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_FindAll) {
            TIGHTDB_TEMPEX3(return find, None, act_FindAll, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_CallbackIdx) {
            TIGHTDB_TEMPEX3(return find, None, act_CallbackIdx, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
    }
    TIGHTDB_ASSERT_DEBUG(false);
    return false;

}


size_t Array::find_first(int64_t value, size_t start, size_t end) const
{
    return find_first<Equal>(value, start, end);
}


// Get containing array block direct through column b+-tree without instatiating any Arrays. Calling with
// use_retval = true will return itself if leaf and avoid unneccesary header initialization.
const Array* Array::GetBlock(size_t ndx, Array& arr, size_t& off,
                             bool use_retval) const TIGHTDB_NOEXCEPT
{
    // Reduce time overhead for cols with few entries
    if (!is_inner_bptree_node()) {
        if (!use_retval)
            arr.CreateFromHeaderDirect(get_header_from_data(m_data));
        off = 0;
        return this;
    }

    pair<MemRef, size_t> p = get_bptree_leaf(ndx);
    arr.CreateFromHeaderDirect(p.first.m_addr);
    off = ndx - p.second;
    return &arr;
}

template <IndexMethod method, class T> size_t Array::index_string(StringData value, Column& result, size_t &result_ref, void* column, StringGetter get_func) const
{
    bool first(method == index_FindFirst);
    bool count(method == index_Count);
    bool all(method == index_FindAll);
    bool allnocopy(method == index_FindAll_nocopy);

    StringData value_2 = value;
    const char* data = m_data;
    const char* header;
    size_t width = m_width;
    bool is_inner_node = m_is_inner_bptree_node;
    typedef StringIndex::key_type key_type;
    key_type key;

top:
    // Create 4 byte index key
    key = StringIndex::create_key(value_2);

    for (;;) {
        // Get subnode table
        ref_type offsets_ref = to_ref(get_direct(data, width, 0));

        // Find the position matching the key
        const char* offsets_header = m_alloc.translate(offsets_ref);
        const char* offsets_data = get_data_from_header(offsets_header);
        size_t offsets_size = get_size_from_header(offsets_header);
        size_t pos = ::lower_bound<32>(offsets_data, offsets_size, key); // keys are always 32 bits wide

        // If key is outside range, we know there can be no match
        if (pos == offsets_size)
            return allnocopy ? size_t(FindRes_not_found) : first ? not_found : 0;

        // Get entry under key
        size_t pos_refs = pos + 1; // first entry in refs points to offsets
        int64_t ref = get_direct(data, width, pos_refs);

        if (is_inner_node) {
            // Set vars for next iteration
            header = m_alloc.translate(to_ref(ref));
            data = get_data_from_header(header);
            width = get_width_from_header(header);
            is_inner_node = get_is_inner_bptree_node_from_header(header);
            continue;
        }

        key_type stored_key = key_type(get_direct<32>(offsets_data, pos));

        if (stored_key != key)
            return allnocopy ? size_t(FindRes_not_found) : first ? not_found : 0;

        // Literal row index
        if (ref & 1) {
            size_t row_ref = size_t(uint64_t(ref) >> 1);

            // for integer index, get_func fills out 'buffer' and makes str point at it
            char buffer[8];
            StringData str = (*get_func)(column, row_ref, buffer);
            if (str == value) {
                result_ref = row_ref;
                if (all)
                    result.add(row_ref);

                return first ? row_ref : count ? 1 : FindRes_single;
            }
            return allnocopy ? size_t(FindRes_not_found) : first ? not_found : 0;
        }

        const char* sub_header = m_alloc.translate(to_ref(ref));
        const bool sub_isindex = get_context_flag_from_header(sub_header);

        // List of matching row indexes
        if (!sub_isindex) {
            const bool sub_isleaf = !get_is_inner_bptree_node_from_header(sub_header);
            size_t sub_count;

            // In most cases the row list will just be an array but there
            // might be so many matches that it has branched into a column
            if (sub_isleaf) {
                if (count)
                    sub_count = get_size_from_header(sub_header);
                const size_t sub_width = get_width_from_header(sub_header);
                const char* sub_data = get_data_from_header(sub_header);
                const size_t first_row_ref = to_size_t(get_direct(sub_data, sub_width, 0));

                // for integer index, get_func fills out 'buffer' and makes str point at it
                char buffer[8];
                StringData str = (*get_func)(column, first_row_ref, buffer);
                if (str != value) {
                    if (count)
                        return 0;
                    return allnocopy ? size_t(FindRes_not_found) : first ? not_found : 0;
                }

                result_ref = to_ref(ref);

                if (all) {
                    // Copy all matches into result column
                    const size_t sub_size = get_size_from_header(sub_header);

                    for (size_t i = 0; i < sub_size; ++i) {
                        size_t row_ref = to_size_t(get_direct(sub_data, sub_width, i));
                        result.add(row_ref);
                    }
                }
                else {
                    return allnocopy ? size_t(FindRes_column) : 
                           first ? to_size_t(get_direct(sub_data, sub_width, 0)) : sub_count;
                }
            }
            else {
                const Column sub(m_alloc, to_ref(ref));
                const size_t first_row_ref = to_size_t(sub.get(0));

                if (count)
                    sub_count = sub.size();

                // for integer index, get_func fills out 'buffer' and makes str point at it
                char buffer[8];
                StringData str = (*get_func)(column, first_row_ref, buffer);
                if (str != value)
                    return allnocopy ? size_t(FindRes_not_found) : first ? not_found : 0;

                result_ref = to_ref(ref);
                if (all) {
                    // Copy all matches into result column
                    for (size_t i = 0; i < sub.size(); ++i)
                        result.add(to_size_t(sub.get(i)));
                }
                else {
                    return allnocopy ? size_t(FindRes_column) : first ? to_size_t(sub.get(0)) : sub_count;
                }
            }

            TIGHTDB_ASSERT(method != index_FindAll_nocopy);
            return size_t(FindRes_column);
        }

        // Recurse into sub-index;
        header = sub_header;
        data = get_data_from_header(header);
        width = get_width_from_header(header);
        is_inner_node = get_is_inner_bptree_node_from_header(header);

        if (value_2.size() <= 4)
            value_2 = StringData();
        else
            value_2 = value_2.substr(4);

        goto top;
    }
}

size_t Array::IndexStringFindFirst(StringData value, void* column, StringGetter get_func) const
{
    size_t dummy;
    Column dummycol;
    return index_string<index_FindFirst, StringData>(value, dummycol, dummy, column, get_func);
}


void Array::IndexStringFindAll(Column& result, StringData value, void* column, StringGetter get_func) const
{
    size_t dummy;

    index_string<index_FindAll, StringData>(value, result, dummy, column, get_func);
}


FindRes Array::IndexStringFindAllNoCopy(StringData value, size_t& res_ref, void* column, StringGetter get_func) const
{
    Column dummy;
    return (FindRes)index_string<index_FindAll_nocopy, StringData>(value, dummy, res_ref, column, get_func);
}


size_t Array::IndexStringCount(StringData value, void* column, StringGetter get_func) const
{
    Column dummy;
    size_t dummysizet;
    return index_string<index_Count, StringData>(value, dummy, dummysizet, column, get_func);
}


namespace {

// Find the index of the child node that contains the specified
// element index. Element index zero corresponds to the first element
// of the first leaf node contained in the subtree corresponding with
// the specified 'offsets' array.
//
// Returns (child_ndx, ndx_in_child).
template<int width> inline pair<size_t, size_t>
find_child_from_offsets(const char* offsets_header, size_t elem_ndx) TIGHTDB_NOEXCEPT
{
    const char* offsets_data = Array::get_data_from_header(offsets_header);
    size_t offsets_size = Array::get_size_from_header(offsets_header);
    size_t child_ndx = upper_bound<width>(offsets_data, offsets_size, elem_ndx);
    size_t elem_ndx_offset = child_ndx == 0 ? 0 :
        to_size_t(get_direct<width>(offsets_data, child_ndx-1));
    size_t ndx_in_child = elem_ndx - elem_ndx_offset;
    return make_pair(child_ndx, ndx_in_child);
}


// Returns (child_ndx, ndx_in_child)
inline pair<size_t, size_t> find_bptree_child(int_fast64_t first_value, size_t ndx,
                                              const Allocator& alloc) TIGHTDB_NOEXCEPT
{
    size_t child_ndx;
    size_t ndx_in_child;
    if (first_value % 2 != 0) {
        // Case 1/2: No offsets array (compact form)
        size_t elems_per_child = to_size_t(first_value/2);
        child_ndx    = ndx / elems_per_child;
        ndx_in_child = ndx % elems_per_child;
        // FIXME: It may be worth considering not to store the total
        // number of elements in each compact node. This would also
        // speed up a tight sequence of append-to-column.
    }
    else {
        // Case 2/2: Offsets array (general form)
        ref_type offsets_ref = to_ref(first_value);
        char* offsets_header = alloc.translate(offsets_ref);
        int offsets_width = Array::get_width_from_header(offsets_header);
        pair<size_t, size_t> p;
        TIGHTDB_TEMPEX(p = find_child_from_offsets, offsets_width, (offsets_header, ndx));
        child_ndx    = p.first;
        ndx_in_child = p.second;
    }
    return make_pair(child_ndx, ndx_in_child);
}


// Returns (child_ndx, ndx_in_child)
inline pair<size_t, size_t> find_bptree_child(Array& node, size_t ndx) TIGHTDB_NOEXCEPT
{
    int_fast64_t first_value = node.get(0);
    return find_bptree_child(first_value, ndx, node.get_alloc());
}


// Returns (child_ref, ndx_in_child)
template<int width>
inline pair<ref_type, size_t> find_bptree_child(const char* data, size_t ndx,
                                                const Allocator& alloc) TIGHTDB_NOEXCEPT
{
    int_fast64_t first_value = get_direct<width>(data, 0);
    pair<size_t, size_t> p = find_bptree_child(first_value, ndx, alloc);
    size_t child_ndx    = p.first;
    size_t ndx_in_child = p.second;
    ref_type child_ref = to_ref(get_direct<width>(data, 1 + child_ndx));
    return make_pair(child_ref, ndx_in_child);
}


// Visit leaves of the B+-tree rooted at this inner node, starting
// with the leaf that contains the element at the specified global
// index start offset (`start_offset`), and ending when the handler
// returns false.
//
// The specified node must be an inner node, and the specified handler
// must have the follewing signature:
//
//     bool handler(const Array::NodeInfo& leaf_info)
//
// `node_offset` is the global index of the first element in this
// subtree, and `node_size` is the number of elements in it.
//
// This function returns true if, and only if the handler has returned
// true for all handled leafs.
//
// This function is designed work without the presence of the `N_t`
// field in the inner B+-tree node
// (a.k.a. `total_elems_in_subtree`). This was done in anticipation of
// the removal of the deprecated field in a future version of the
// TightDB file format.
//
// This function is also designed in anticipation of a change in the
// way column accessors work. Some aspects of the implementation of
// this function are not yet as they are intended to be, due the fact
// that column accessors cache the root node rather than the last used
// leaf node. When the behaviour of the column accessors is changed,
// the signature of this function should be changed to
// foreach_bptree_leaf(const array::NodeInfo&, Handler, size_t
// start_offset). This will allow for a number of minor (but
// important) improvements.
template<class Handler>
bool foreach_bptree_leaf(Array& node, size_t node_offset, size_t node_size,
                         Handler handler, size_t start_offset)
    TIGHTDB_NOEXCEPT_IF(noexcept(handler(Array::NodeInfo())))
{
    TIGHTDB_ASSERT(node.is_inner_bptree_node());

    Allocator& alloc = node.get_alloc();
    Array offsets(alloc);
    size_t child_ndx = 0, child_offset = node_offset;
    size_t elems_per_child = 0;
    {
        TIGHTDB_ASSERT(node.size() >= 1);
        int_fast64_t first_value = node.get(0);
        bool is_compact = first_value % 2 != 0;
        if (is_compact) {
            // Compact form
            elems_per_child = to_size_t(first_value/2);
            if (start_offset > node_offset) {
                size_t local_start_offset = start_offset - node_offset;
                child_ndx = local_start_offset / elems_per_child;
                child_offset += child_ndx * elems_per_child;
            }
        }
        else {
            // General form
            ref_type offsets_ref = to_ref(first_value);
            offsets.init_from_ref(offsets_ref);
            if (start_offset > node_offset) {
                size_t local_start_offset = start_offset - node_offset;
                child_ndx = offsets.upper_bound_int(local_start_offset);
                if (child_ndx > 0)
                    child_offset += to_size_t(offsets.get(child_ndx-1));
            }
        }
    }
    TIGHTDB_ASSERT(node.size() >= 2);
    size_t num_children = node.size() - 2;
    TIGHTDB_ASSERT(num_children >= 1); // invar:bptree-nonempty-inner
    Array::NodeInfo child_info;
    child_info.m_parent = &node;
    child_info.m_ndx_in_parent = 1 + child_ndx;
    child_info.m_mem = MemRef(node.get_as_ref(child_info.m_ndx_in_parent), alloc);
    child_info.m_offset = child_offset;
    bool children_are_leaves =
        !Array::get_is_inner_bptree_node_from_header(child_info.m_mem.m_addr);
    for (;;) {
        child_info.m_size = elems_per_child;
        bool is_last_child = child_ndx == num_children - 1;
        if (!is_last_child) {
            bool is_compact = elems_per_child != 0;
            if (!is_compact) {
                size_t next_child_offset = node_offset + to_size_t(offsets.get(child_ndx-1 + 1));
                child_info.m_size = next_child_offset - child_info.m_offset;
            }
        }
        else {
            size_t next_child_offset = node_offset + node_size;
            child_info.m_size = next_child_offset - child_info.m_offset;
        }
        bool go_on;
        if (children_are_leaves) {
            const Array::NodeInfo& const_child_info = child_info;
            go_on = handler(const_child_info);
        }
        else {
            Array child(alloc);
            child.init_from_mem(child_info.m_mem);
            child.set_parent(child_info.m_parent, child_info.m_ndx_in_parent);
            go_on = foreach_bptree_leaf(child, child_info.m_offset, child_info.m_size,
                                        handler, start_offset);
        }
        if (!go_on)
            return false;
        if (is_last_child)
            break;
        ++child_ndx;
        child_info.m_ndx_in_parent = 1 + child_ndx;
        child_info.m_mem = MemRef(node.get_as_ref(child_info.m_ndx_in_parent), alloc);
        child_info.m_offset += child_info.m_size;
    }
    return true;
}


// Same as foreach_bptree_leaf() except that this version is faster
// and has no support for slicing. That also means that the return
// value of the handler is ignored. Finally,
// `Array::NodeInfo::m_offset` and `Array::NodeInfo::m_size` are not
// calculated. With these simplification it is possible to avoid any
// access to the `offsets` array.
template<class Handler> void simplified_foreach_bptree_leaf(Array& node, Handler handler)
    TIGHTDB_NOEXCEPT_IF(noexcept(handler(Array::NodeInfo())))
{
    TIGHTDB_ASSERT(node.is_inner_bptree_node());

    Allocator& alloc = node.get_alloc();
    size_t child_ndx = 0;
    TIGHTDB_ASSERT(node.size() >= 2);
    size_t num_children = node.size() - 2;
    TIGHTDB_ASSERT(num_children >= 1); // invar:bptree-nonempty-inner
    Array::NodeInfo child_info;
    child_info.m_parent = &node;
    child_info.m_ndx_in_parent = 1 + child_ndx;
    child_info.m_mem = MemRef(node.get_as_ref(child_info.m_ndx_in_parent), alloc);
    child_info.m_offset = 0;
    child_info.m_size   = 0;
    bool children_are_leaves =
        !Array::get_is_inner_bptree_node_from_header(child_info.m_mem.m_addr);
    for (;;) {
        if (children_are_leaves) {
            const Array::NodeInfo& const_child_info = child_info;
            handler(const_child_info);
        }
        else {
            Array child(alloc);
            child.init_from_mem(child_info.m_mem);
            child.set_parent(child_info.m_parent, child_info.m_ndx_in_parent);
            simplified_foreach_bptree_leaf(child, handler);
        }
        bool is_last_child = child_ndx == num_children - 1;
        if (is_last_child)
            break;
        ++child_ndx;
        child_info.m_ndx_in_parent = 1 + child_ndx;
        child_info.m_mem = MemRef(node.get_as_ref(child_info.m_ndx_in_parent), alloc);
    }
}


inline void destroy_inner_bptree_node(MemRef mem, int_fast64_t first_value,
                                      Allocator& alloc) TIGHTDB_NOEXCEPT
{
    alloc.free_(mem);
    if (first_value % 2 == 0) {
        // Node has offsets array
        ref_type offsets_ref = to_ref(first_value);
        alloc.free_(offsets_ref, alloc.translate(offsets_ref));
    }
}

void destroy_singlet_bptree_branch(MemRef mem, Allocator& alloc,
                                   Array::EraseHandler& handler) TIGHTDB_NOEXCEPT
{
    MemRef mem_2 = mem;
    for (;;) {
        const char* header = mem_2.m_addr;
        bool is_leaf = !Array::get_is_inner_bptree_node_from_header(header);
        if (is_leaf) {
            handler.destroy_leaf(mem_2);
            return;
        }

        const char* data = Array::get_data_from_header(header);
        int width = Array::get_width_from_header(header);
        size_t ndx = 0;
        pair<int_fast64_t, int_fast64_t> p = get_two(data, width, ndx);
        int_fast64_t first_value = p.first;
        ref_type child_ref = to_ref(p.second);

        destroy_inner_bptree_node(mem_2, first_value, alloc);

        mem_2.m_ref  = child_ref;
        mem_2.m_addr = alloc.translate(child_ref);
    }
}

void elim_superfluous_bptree_root(Array* root, MemRef parent_mem,
                                  int_fast64_t parent_first_value, ref_type child_ref,
                                  Array::EraseHandler& handler)
{
    Allocator& alloc = root->get_alloc();
    char* child_header = alloc.translate(child_ref);
    MemRef child_mem(child_header, child_ref);
    bool child_is_leaf = !Array::get_is_inner_bptree_node_from_header(child_header);
    if (child_is_leaf) {
        handler.replace_root_by_leaf(child_mem); // Throws
        // Since the tree has now been modified, the height reduction
        // operation cannot be aborted without leaking memory, so the
        // rest of the operation must proceed without throwing. This
        // includes retrocursive completion of earlier invocations of
        // this function.
        //
        // Note also that 'root' may be destroy at this point.
    }
    else {
        size_t child_size = Array::get_size_from_header(child_header);
        TIGHTDB_ASSERT(child_size >= 2);
        size_t num_grandchildren = child_size - 2;
        TIGHTDB_ASSERT(num_grandchildren >= 1); // invar:bptree-nonempty-inner
        if (num_grandchildren > 1) {
            // This child is an inner node, and is the closest one to
            // the root that has more than one child, so make it the
            // new root.
            root->init_from_ref(child_ref);
            root->update_parent(); // Throws
            // From this point on, the height reduction operation
            // cannot be aborted without leaking memory, so the rest
            // of the operation must proceed without throwing. This
            // includes retrocursive completion of earlier invocations
            // of this function.
        }
        else {
            // This child is an inner node, but has itself just one
            // child, so continue hight reduction.
            int_fast64_t child_first_value = Array::get(child_header, 0);
            ref_type grandchild_ref = to_ref(Array::get(child_header, 1));
            elim_superfluous_bptree_root(root, child_mem, child_first_value,
                                         grandchild_ref, handler); // Throws
        }
    }

    // At this point, a new root has been installed. The new root is
    // some descendant of the node referenced by 'parent_mem'. Array
    // nodes comprising eliminated B+-tree nodes must be freed. Our
    // job is to free those comprising that parent. It is crucial that
    // this part does not throw.
    alloc.free_(parent_mem);
    if (parent_first_value % 2 == 0) {
        // Parent has offsets array
        ref_type offsets_ref = to_ref(parent_first_value);
        alloc.free_(offsets_ref, alloc.translate(offsets_ref));
    }
}

} // anonymous namespace


pair<MemRef, size_t> Array::get_bptree_leaf(size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_inner_bptree_node());

    size_t ndx_2 = ndx;
    int width = int(m_width);
    const char* data = m_data;

    for (;;) {
        pair<ref_type, size_t> p;
        TIGHTDB_TEMPEX(p = find_bptree_child, width, (data, ndx_2, m_alloc));
        ref_type child_ref  = p.first;
        size_t ndx_in_child = p.second;
        char* child_header = m_alloc.translate(child_ref);
        bool child_is_leaf = !get_is_inner_bptree_node_from_header(child_header);
        if (child_is_leaf) {
            MemRef mem(child_header, child_ref);
            return make_pair(mem, ndx_in_child);
        }
        ndx_2 = ndx_in_child;
        width = get_width_from_header(child_header);
        data = get_data_from_header(child_header);
    }
}


namespace {

class VisitAdapter {
public:
    VisitAdapter(Array::VisitHandler& handler) TIGHTDB_NOEXCEPT:
        m_handler(handler)
    {
    }
    bool operator()(const Array::NodeInfo& leaf_info)
    {
        return m_handler.visit(leaf_info); // Throws
    }
private:
    Array::VisitHandler& m_handler;
};

} // anonymous namespace

// Throws only if handler throws.
bool Array::visit_bptree_leaves(size_t elem_ndx_offset, size_t elems_in_tree,
                                VisitHandler& handler)
{
    TIGHTDB_ASSERT(elem_ndx_offset < elems_in_tree);
    size_t root_offset = 0, root_size = elems_in_tree;
    VisitAdapter adapter(handler);
    size_t start_offset = elem_ndx_offset;
    return foreach_bptree_leaf(*this, root_offset, root_size, adapter, start_offset); // Throws
}


namespace {

class UpdateAdapter {
public:
    UpdateAdapter(Array::UpdateHandler& handler) TIGHTDB_NOEXCEPT:
        m_handler(handler)
    {
    }
    void operator()(const Array::NodeInfo& leaf_info)
    {
        size_t elem_ndx_in_leaf = 0;
        m_handler.update(leaf_info.m_mem, leaf_info.m_parent, leaf_info.m_ndx_in_parent,
                         elem_ndx_in_leaf); // Throws
    }
private:
    Array::UpdateHandler& m_handler;
};

} // anonymous namespace

void Array::update_bptree_leaves(UpdateHandler& handler)
{
    UpdateAdapter adapter(handler);
    simplified_foreach_bptree_leaf(*this, adapter); // Throws
}


void Array::update_bptree_elem(size_t elem_ndx, UpdateHandler& handler)
{
    TIGHTDB_ASSERT(is_inner_bptree_node());

    pair<size_t, size_t> p = find_bptree_child(*this, elem_ndx);
    size_t child_ndx    = p.first;
    size_t ndx_in_child = p.second;
    size_t child_ref_ndx = 1 + child_ndx;
    ref_type child_ref = get_as_ref(child_ref_ndx);
    char* child_header = m_alloc.translate(child_ref);
    MemRef child_mem(child_header, child_ref);
    bool child_is_leaf = !get_is_inner_bptree_node_from_header(child_header);
    if (child_is_leaf) {
        handler.update(child_mem, this, child_ref_ndx, ndx_in_child); // Throws
        return;
    }
    Array child(m_alloc);
    child.init_from_mem(child_mem);
    child.set_parent(this, child_ref_ndx);
    child.update_bptree_elem(ndx_in_child, handler); // Throws
}


void Array::erase_bptree_elem(Array* root, size_t elem_ndx, EraseHandler& handler)
{
    TIGHTDB_ASSERT(root->is_inner_bptree_node());
    TIGHTDB_ASSERT(root->size() >= 1 + 1 + 1); // invar:bptree-nonempty-inner
    TIGHTDB_ASSERT_DEBUG(elem_ndx == npos || elem_ndx+1 != root->get_bptree_size());

    // Note that this function is implemented in a way that makes it
    // fully exception safe. Please be sure to keep it that way.

    bool destroy_root = root->do_erase_bptree_elem(elem_ndx, handler); // Throws

    // do_erase_bptree_elem() returns true if erasing the element
    // would produce an empty tree. In this case, to maintain
    // invar:bptree-nonempty-inner, we must replace the root with an
    // empty leaf.
    //
    // FIXME: ExceptionSafety: While this maintains general exception
    // safety, it does not provide the extra guarantee that we would
    // like, namely that removal of an element is guaranteed to
    // succeed if that element was inserted during the current
    // transaction (noexcept:bptree-erase). This is why we want to be
    // able to have a column with no root node and a zero-ref in
    // Table::m_columns.
    if (destroy_root) {
        MemRef root_mem = root->get_mem();
        TIGHTDB_ASSERT(root->size() >= 2);
        int_fast64_t first_value = root->get(0);
        ref_type child_ref = root->get_as_ref(1);
        Allocator& alloc = root->get_alloc();
        handler.replace_root_by_empty_leaf(); // Throws
        // 'root' may be destroyed at this point
        destroy_inner_bptree_node(root_mem, first_value, alloc);
        char* child_header = alloc.translate(child_ref);
        MemRef child_mem(child_header, child_ref);
        destroy_singlet_bptree_branch(child_mem, alloc, handler);
        return;
    }

    // If at this point, the root has only a single child left, the
    // root has become superfluous, and can be replaced by its single
    // child. This applies recursivly.
    size_t num_children = root->size() - 2;
    if (num_children > 1)
        return;

    // ExceptionSafety: The recursive elimination of superfluous
    // singlet roots is desirable but optional according to the tree
    // invariants. Since we cannot allow an exception to be thrown
    // after having successfully modified the tree, and since the root
    // elimination process cannot be guaranteed to not throw, we have
    // to abort a failed attempt by catching and ignoring the thrown
    // exception. This is always safe due to the exception safety of
    // the root elimination process itself.
    try {
        MemRef root_mem = root->get_mem();
        TIGHTDB_ASSERT(root->size() >= 2);
        int_fast64_t first_value = root->get(0);
        ref_type child_ref = root->get_as_ref(1);
        elim_superfluous_bptree_root(root, root_mem, first_value,
                                     child_ref, handler); // Throws
    }
    catch (...) {
        // Abort optional step by ignoring excpetion
    }
}


bool Array::do_erase_bptree_elem(size_t elem_ndx, EraseHandler& handler)
{
    Array offsets(m_alloc);
    size_t child_ndx;
    size_t ndx_in_child;
    if (elem_ndx == npos) {
        size_t num_children = size() - 2;
        child_ndx    = num_children - 1;
        ndx_in_child = npos;
    }
    else {
        // If this node is not already on the general form, convert it
        // now. Since this conversion will occur from root to leaf, it
        // will maintain invar:bptree-node-form.
        ensure_bptree_offsets(offsets); // Throws

        // Ensure that the offsets array is not in read-only memory. This
        // is necessary to guarantee that the adjustments of the element
        // counts below will succeed.
        offsets.copy_on_write(); // Throws

        // FIXME: Can we pass 'offsets' to find_bptree_child() to
        // speed it up?
        pair<size_t, size_t> p = find_bptree_child(*this, elem_ndx);
        child_ndx    = p.first;
        ndx_in_child = p.second;
    }

    size_t child_ref_ndx = 1 + child_ndx;
    ref_type child_ref = get_as_ref(child_ref_ndx);
    char* child_header = m_alloc.translate(child_ref);
    MemRef child_mem(child_header, child_ref);
    bool child_is_leaf = !get_is_inner_bptree_node_from_header(child_header);
    bool destroy_child;
    if (child_is_leaf) {
        destroy_child =
            handler.erase_leaf_elem(child_mem, this, child_ref_ndx,
                                    ndx_in_child); // Throws
    }
    else {
        Array child(m_alloc);
        child.init_from_mem(child_mem);
        child.set_parent(this, child_ref_ndx);
        destroy_child =
            child.do_erase_bptree_elem(ndx_in_child, handler); // Throws
    }
    size_t num_children = size() - 2;
    if (destroy_child) {
        if (num_children == 1)
            return true; // Destroy this node too
        TIGHTDB_ASSERT(num_children >= 2);
        child_ref = get_as_ref(child_ref_ndx);
        child_header = m_alloc.translate(child_ref);
        child_mem = MemRef(child_header, child_ref);
        erase(child_ref_ndx); // Throws
        destroy_singlet_bptree_branch(child_mem, m_alloc, handler);
        // If the erased element is the last one, we did not attach
        // the offsets array above, even if one was preset. Since we
        // are removing a child, we have to do that now.
        if (elem_ndx == npos) {
            int_fast64_t first_value = front();
            bool general_form = first_value % 2 == 0;
            if (general_form) {
                offsets.init_from_ref(to_ref(first_value));
                offsets.set_parent(this, 0);
            }
        }
    }
    if (offsets.is_attached()) {
        // These adjustments are guaranteed to succeed because of the
        // copy-on-write on the offets array above, and because of the
        // fact that we never increase or insert values.
        size_t offsets_adjust_begin = child_ndx;
        if (destroy_child) {
            if (offsets_adjust_begin == num_children-1)
                --offsets_adjust_begin;
            offsets.erase(offsets_adjust_begin);
        }
        offsets.adjust(offsets_adjust_begin, offsets.size(), -1);
    }

    // The following adjustment is guaranteed to succeed because we
    // decrease the value, and because the subtree rooted at this node
    // has been modified, so this array cannot be in read-only memory
    // any longer.
    adjust(size()-1, -2); // -2 because stored value is 1 + 2*total_elems_in_subtree

    return false; // Element erased and offsets adjusted
}


void Array::create_bptree_offsets(Array& offsets, int_fast64_t first_value)
{
    offsets.create(type_Normal); // Throws
    int_fast64_t elems_per_child = first_value/2;
    int_fast64_t accum_num_elems = 0;
    size_t num_children = size() - 2;
    for (size_t i = 0; i != num_children-1; ++i) {
        accum_num_elems += elems_per_child;
        offsets.add(accum_num_elems); // Throws
    }
    // FIXME: Dangerous cast here (unsigned -> signed)
    set(0, offsets.get_ref()); // Throws
}


int_fast64_t Array::get(const char* header, size_t ndx) TIGHTDB_NOEXCEPT
{
    const char* data = get_data_from_header(header);
    int width = get_width_from_header(header);
    return get_direct(data, width, ndx);
}


pair<int_least64_t, int_least64_t> Array::get_two(const char* header, size_t ndx) TIGHTDB_NOEXCEPT
{
    const char* data = get_data_from_header(header);
    int width = get_width_from_header(header);
    pair<int_fast64_t, int_fast64_t> p = ::get_two(data, width, ndx);
    return make_pair(int_least64_t(p.first), int_least64_t(p.second));
}
