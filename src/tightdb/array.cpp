#include <limits>
#include <iostream>
#include <iomanip>

#ifdef _MSC_VER
#  include <intrin.h>
#  include <win32/types.h>
#  pragma warning (disable : 4127) // Condition is constant warning
#endif

#include <tightdb/terminate.hpp>
#include <tightdb/array.hpp>
#include <tightdb/column.hpp>
#include <tightdb/query_conditions.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/index_string.hpp>
#include <tightdb/utilities.hpp>


// Header format (8 bytes):
//
// |--------|--------|--------|--------|--------|--------|--------|--------|
// |12344555|           size           |         capacity         |reserved|
//
//  1: 'not_leaf' (inner node of B+-tree).
//
//  2: 'has_refs' (elements whose first bit is zero are refs to subarrays).
//
//  3: 'index_flag'
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
// 'size' (aka length) is the number of elements in the array.
//
// 'capacity' is the total number of bytes allocated for this array
// including the header.


using namespace std;
using namespace tightdb;

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


void Array::init_from_ref(ref_type ref) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ref);
    char* header = m_alloc.translate(ref);
    init_from_mem(MemRef(header, ref));
}

void Array::init_from_mem(MemRef mem) TIGHTDB_NOEXCEPT
{
    char* header = mem.m_addr;

    // Parse header
    m_isNode   = !get_isleaf_from_header(header);
    m_hasRefs  = get_hasrefs_from_header(header);
    m_width    = get_width_from_header(header);
    m_size     = get_size_from_header(header);

    // Capacity is how many items there are room for
    size_t byte_capacity = get_capacity_from_header(header);
    // FIXME: Avoid calling virtual method CalcItemCount() here,
    // instead calculate the capacity in a way similar to what is done
    // in get_byte_size_from_header(). The virtual call makes "life"
    // hard for constructors in derived array classes.
    m_capacity = CalcItemCount(byte_capacity, m_width);

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
// FIXME: If we want to keep this methid, we should formally define
// what can be termed 'direct read-only' use of an Array instance, and
// wat rules apply in this case. Currently Array::clone() just passes
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

    bool is_leaf = false, has_refs = false;
    switch (type) {
        case type_Normal:
            is_leaf = true;
            break;
        case type_InnerColumnNode:
            has_refs = true;
            break;
        case type_HasRefs:
            has_refs = is_leaf = true;
            break;
    }
    m_isNode  = !is_leaf;
    m_hasRefs = has_refs;
    set_header_isleaf(is_leaf);
    set_header_hasrefs(has_refs);
}

bool Array::update_from_parent(size_t old_baseline) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_parent);

    // Array nodes that a part of the previous version of the database
    // will not be overwritte by Group::commit(). This is necessary
    // for robustness in the face of abrupt termination of the
    // process. It also means that we can be sure that an array
    // remains unchanged across a commit if the new ref is equal to
    // the old ref and the ref is below the previous basline.

    ref_type new_ref = m_parent->get_child_ref(m_ndx_in_parent);
    if (new_ref == m_ref && new_ref < old_baseline)
        return false; // Has not changed

    init_from_ref(new_ref);
    return true; // Has changed
}

// Allocates space for 'count' items being between min and min in size, both inclusive. Crashes! Why? Todo/fixme
void Array::Preset(size_t bitwidth, size_t count)
{
    clear();
    set_width(bitwidth);
    alloc(count, bitwidth); // Throws
    m_size = count;
    for (size_t n = 0; n < count; n++)
        set(n, 0);
}

void Array::Preset(int64_t min, int64_t max, size_t count)
{
    size_t w = ::max(bit_width(max), bit_width(min));
    Preset(w, count);
}

void Array::set_parent(ArrayParent* parent, size_t ndx_in_parent) TIGHTDB_NOEXCEPT
{
    m_parent = parent;
    m_ndx_in_parent = ndx_in_parent;
}


void Array::destroy_children() TIGHTDB_NOEXCEPT
{
    for (size_t i = 0; i < m_size; ++i) {
        int64_t v = get(i);

        // Null-refs indicate empty sub-trees
        if (v == 0)
            continue;

        // A ref is always 8-byte aligned, so the lowest bit
        // cannot be set. If it is, it means that it should not be
        // interpreted as a ref.
        if (v % 2 != 0)
            continue;

        Array sub(to_ref(v), this, i, m_alloc);
        sub.destroy();
    }
}


void Array::erase(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_size);

    // Check if we need to copy before modifying
    copy_on_write(); // Throws

    // Move values below deletion up
    if (m_width < 8) {
        for (size_t i = ndx+1; i < m_size; ++i) {
            int64_t v = (this->*m_getter)(i);
            (this->*m_setter)(i-1, v);
        }
    }
    else if (ndx < m_size-1) {
        // when byte sized, use memmove
// FIXME: Should probably be optimized as a simple division by 8.
        size_t w = (m_width == 64) ? 8 : (m_width == 32) ? 4 : (m_width == 16) ? 2 : 1;
        char* base = reinterpret_cast<char*>(m_data);
        char* dst_begin = base + ndx*w;
        const char* src_begin = dst_begin + w;
        const char* src_end   = base + m_size*w;
        copy(src_begin, src_end, dst_begin);
    }

    // Update size (also in header)
    --m_size;
    set_header_size(m_size);
}

void Array::set(size_t ndx, int64_t value)
{
    TIGHTDB_ASSERT(ndx < m_size);

    // Check if we need to copy before modifying
    copy_on_write(); // Throws

    bool do_expand = value < m_lbound || value > m_ubound;
    if (do_expand) {
        size_t width = bit_width(value);
        TIGHTDB_ASSERT(width > m_width);
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

void Array::insert(size_t ndx, int64_t value)
{
    TIGHTDB_ASSERT(ndx <= m_size);

    // Check if we need to copy before modifying
    copy_on_write(); // Throws

    Getter old_getter = m_getter; // Save old getter before potential width expansion

    bool do_expand = value < m_lbound || value > m_ubound;
    if (do_expand) {
        size_t width = bit_width(value);
        TIGHTDB_ASSERT(width > m_width);
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


void Array::add(int64_t value)
{
    insert(m_size, value);
}

void Array::resize(size_t count)
{
    TIGHTDB_ASSERT(count <= m_size);

    copy_on_write(); // Throws

    // Update size (also in header)
    m_size = count;
    set_header_size(m_size);
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

void Array::Increment(int64_t value, size_t start, size_t end)
{
    if (end == size_t(-1))
        end = m_size;
    TIGHTDB_ASSERT(start < m_size);
    TIGHTDB_ASSERT(end >= start && end <= m_size);

    // Increment range
    for (size_t i = start; i < end; ++i) {
        set(i, get(i) + value);
    }
}

void Array::IncrementIf(int64_t limit, int64_t value)
{
    // Update (incr or decrement) values bigger or equal to the limit
    for (size_t i = 0; i < m_size; ++i) {
        int64_t v = get(i);
        if (v >= limit)
            set(i, v + value);
    }
}

void Array::adjust(size_t start, int64_t diff)
{
    TIGHTDB_ASSERT(start <= m_size);

    size_t n = m_size;
    for (size_t i = start; i < n; ++i) {
        int64_t v = get(i);
        set(i, v + diff);
    }
}


// return first element E for which E >= target or return -1 if none. Array must be sorted
size_t Array::FindGTE(int64_t target, size_t start) const
{
#if TIGHTDB_DEBUG
    // Reference implementation to illustrate and test behaviour
    size_t ref = 0;
    size_t idx;
    for (idx = start; idx < m_size; ++idx) {
        if (get(idx) >= target) {
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
        if (get(start) >= target) {
            ret = start;
            goto exit;
        }
        ++start;
        if (get(start) >= target) {
            ret = start;
            goto exit;
        }
        ++start;
    }

    // Todo, use templated get<width> from this point for performance
    if (target > get(m_size - 1)) {
        ret = not_found;
        goto exit;
    }

    size_t add;
    add = 1;

    for (;;) {
        if (start + add < m_size && get(start + add) < target)
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
        int64_t v = get(probe);
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

#if TIGHTDB_DEBUG
    TIGHTDB_ASSERT(ref == ret);
#endif

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
        TIGHTDB_ASSERT(false);
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


template<bool find_max, size_t w> bool Array::minmax(int64_t& result, size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = m_size;
    TIGHTDB_ASSERT(start < m_size && end <= m_size && start < end);

    if (m_size == 0)
        return false;

    if (w == 0) {
        result = 0;
        return true;
    }

    int64_t m = Get<w>(start);
    ++start;

#ifdef TIGHTDB_COMPILER_SSE
    if (cpuid_sse<42>()) {
        // Test manually until 128 bit aligned
        for (; (start < end) && (((size_t(m_data) & 0xf) * 8 + start * w) % (128) != 0); start++) {
            if (find_max ? Get<w>(start) > m : Get<w>(start) < m)
                m = Get<w>(start);
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

    for (; start < end; ++start) {
        const int64_t v = Get<w>(start);
        if (find_max ? v > m : v < m) {
            m = v;
        }
    }

    result = m;
    return true;
}

bool Array::maximum(int64_t& result, size_t start, size_t end) const
{
    TIGHTDB_TEMPEX2(return minmax, true, m_width, (result, start, end));
}

bool Array::minimum(int64_t& result, size_t start, size_t end) const
{
    TIGHTDB_TEMPEX2(return minmax, false, m_width, (result, start, end));
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
    if (cpuid_sse<42>()) {

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

size_t Array::count(int64_t value) const
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

size_t Array::CalcByteLen(size_t count, size_t width) const
{
    // FIXME: This arithemtic could overflow. Consider using <tightdb/safe_int_ops.hpp>
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

ref_type Array::clone(const char* header, Allocator& alloc, Allocator& clone_alloc)
{
    if (!get_hasrefs_from_header(header)) {
        // This array has no subarrays, so we can make a byte-for-byte
        // copy, which is more efficient.

        // Calculate size of new array in bytes
        size_t size = get_byte_size_from_header(header);

        // Create the new array
        MemRef mem_ref = clone_alloc.alloc(size); // Throws
        char* clone_header = mem_ref.m_addr;

        // Copy contents
        const char* src_begin = header;
        const char* src_end   = header + size;
        char*       dst_begin = clone_header;
        copy(src_begin, src_end, dst_begin);

        // Update with correct capacity
        set_header_capacity(size, clone_header);

        return mem_ref.m_ref;
    }

    // Refs are integers, and integers arrays use wtype_Bits.
    TIGHTDB_ASSERT(get_wtype_from_header(header) == wtype_Bits);

    Array array((Array::no_prealloc_tag()));
    array.CreateFromHeaderDirect(const_cast<char*>(header));

    // Create new empty array of refs
    MemRef mem_ref = clone_alloc.alloc(initial_capacity); // Throws
    char* clone_header = mem_ref.m_addr;
    {
        bool is_leaf = get_isleaf_from_header(header);
        bool has_refs = true;
        WidthType width_type = wtype_Bits;
        int width = 0;
        size_t size = 0;
        init_header(clone_header, is_leaf, has_refs, width_type, width, size, initial_capacity);
    }

    Array new_array(clone_alloc);
    new_array.init_from_mem(mem_ref);

    size_t n = array.size();
    for (size_t i = 0; i < n; ++i) {
        int64_t value = array.get(i);

        // Null-refs signify empty sub-trees. Also, all refs are
        // 8-byte aligned, so the lowest bits cannot be set. If they
        // are, it means that it should not be interpreted as a ref.
        bool is_subarray = value != 0 && (value & 0x1) == 0;
        if (is_subarray) {
            ref_type ref = to_ref(value);
            const char* subheader = alloc.translate(ref);
            ref_type new_ref = clone(subheader, alloc, clone_alloc);
            value = new_ref;
        }

        new_array.add(value);
    }

    return mem_ref.m_ref;
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
    TIGHTDB_ASSERT(m_capacity > 0);

    // Update capacity in header
    set_header_capacity(new_size); // uses m_data to find header, so m_data must be initialized correctly first

    update_parent();

    // Mark original as deleted, so that the space can be reclaimed in
    // future commits, when no versions are using it anymore
    m_alloc.free_(old_ref, old_begin);
}


ref_type Array::create_empty_array(Type type, WidthType width_type, Allocator& alloc)
{
    bool is_leaf = false, has_refs = false;
    switch (type) {
        case type_Normal:
            is_leaf = true;
            break;
        case type_InnerColumnNode:
            has_refs = true;
            break;
        case type_HasRefs:
            has_refs = is_leaf = true;
            break;
    }

    size_t capacity = initial_capacity;
    MemRef mem_ref = alloc.alloc(capacity); // Throws

    int width = 0;
    size_t size = 0;
    init_header(mem_ref.m_addr, is_leaf, has_refs, width_type, width, size, capacity);

    return mem_ref.m_ref;
}


// FIXME: It may be worth trying to combine this with copy_on_write()
// to avoid two copies.
void Array::alloc(size_t size, size_t width)
{
    TIGHTDB_ASSERT(is_attached());
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
        TIGHTDB_ASSERT(false);
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

template<size_t w> int64_t Array::Get(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return GetUniversal<w>(m_data, ndx);
}

template<size_t w> void Array::get_chunk(size_t ndx, int64_t res[8]) const TIGHTDB_NOEXCEPT
{
    memset(res, 0, 8*8);
    // This method reads 8 concecutive values into res[8], starting from index 'ndx'. It's allowed for the 8 values to
    // exceed array length; in this case, remainder of res[8] will be left untouched.

    TIGHTDB_ASSERT(ndx < m_size);

    if(TIGHTDB_X86_OR_X64_TRUE && (w == 1 || w == 2 || w == 4) && ndx + 32 < m_size) {
        // This method is *multiple* times faster than performing 8 times Get<w>, even if unrolled. Apparently compilers
        // can't figure out to optimize it.
        uint64_t c;
        if(w == 1) {
            c = *reinterpret_cast<uint16_t*>(m_data + ndx / 8);
            c >>= ndx - ndx / 8 * 8;
        }
        else if(w == 2) {
            c = *reinterpret_cast<uint32_t*>(m_data + ndx / 4);
            c >>= ndx - ndx / 4 * 4;
        }
        else if(w == 4) {
            c = *reinterpret_cast<uint64_t*>(m_data + ndx / 2);
            c >>= ndx - ndx / 4 * 2;
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
}

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4127)
#endif
template<size_t w> void Array::Set(size_t ndx, int64_t value)
{
    if (w == 0) {
        return;
    }
    else if (w == 1) {
        size_t offset = ndx >> 3;
        ndx &= 7;
        uint8_t* p = reinterpret_cast<uint8_t*>(m_data) + offset;
        *p = (*p &~ (1 << ndx)) | uint8_t((value & 1) << ndx);
    }
    else if (w == 2) {
        size_t offset = ndx >> 2;
        uint8_t n = uint8_t((ndx & 3) << 1);
        uint8_t* p = reinterpret_cast<uint8_t*>(m_data) + offset;
        *p = (*p &~ (0x03 << n)) | uint8_t((value & 0x03) << n);
    }
    else if (w == 4) {
        size_t offset = ndx >> 1;
        uint8_t n = uint8_t((ndx & 1) << 2);
        uint8_t* p = reinterpret_cast<uint8_t*>(m_data) + offset;
        *p = (*p &~ (0x0F << n)) | uint8_t((value & 0x0F) << n);
    }
    else if (w == 8) {
        *(reinterpret_cast<int8_t*>(m_data) + ndx) = int8_t(value);
    }
    else if (w == 16) {
        *(reinterpret_cast<int16_t*>(m_data) + ndx) = int16_t(value);
    }
    else if (w == 32) {
        *(reinterpret_cast<int32_t*>(m_data) + ndx) = int32_t(value);
    }
    else if (w == 64) {
        *(reinterpret_cast<int64_t*>(m_data) + ndx) = value;
    }
}
#ifdef _MSC_VER
#pragma warning(pop)
#endif

// Sort array.
void Array::sort()
{
    TIGHTDB_TEMPEX(sort, m_width, ());
}

// Find max and min value, but break search if difference exceeds 'maxdiff' (in which case *min and *max is set to 0)
// Useful for counting-sort functions
template<size_t w>bool Array::MinMax(size_t from, size_t to, uint64_t maxdiff, int64_t *min, int64_t *max)
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
void Array::ReferenceSort(Array& ref)
{
    TIGHTDB_TEMPEX(ReferenceSort, m_width, (ref));
}

template<size_t w>void Array::ReferenceSort(Array& ref)
{
    if (m_size < 2)
        return;

    int64_t min;
    int64_t max;

    // in avg case QuickSort is O(n*log(n)) and CountSort O(n + range), and memory usage is sizeof(size_t)*range for CountSort.
    // So we chose range < m_size as treshold for deciding which to use

    // If range isn't suited for CountSort, it's *probably* discovered very early, within first few values, in most practical cases,
    // and won't add much wasted work. Max wasted work is O(n) which isn't much compared to QuickSort.

//  bool b = MinMax<w>(0, m_size, m_size, &min, &max); // auto detect
//  bool b = MinMax<w>(0, m_size, -1, &min, &max); // force count sort
    bool b = MinMax<w>(0, m_size, 0, &min, &max); // force quicksort

    if (b) {
        Array res;
        Array count;

        // Todo, Preset crashes for unknown reasons but would be faster.
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
    else {
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

void Array::ReferenceQuickSort(Array& ref)
{
    TIGHTDB_TEMPEX(ReferenceQuickSort, m_width, (0, m_size - 1, ref));
}

template<size_t w> void Array::ReferenceQuickSort(size_t lo, size_t hi, Array& ref)
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

bool Array::compare_int(const Array& a) const
{
    if (a.size() != size())
        return false;

    for (size_t i = 0; i < size(); ++i) {
        if (get(i) != a.get(i))
            return false;
    }

    return true;
}


ref_type Array::insert_btree_child(Array& offsets, Array& refs, size_t orig_child_ndx,
                                   ref_type new_sibling_ref, TreeInsertBase& state)
{
    size_t elem_ndx_offset = orig_child_ndx == 0 ? 0 : size_t(offsets.get(orig_child_ndx-1));

    // When a node is split the new node must always be inserted after
    // the original
    size_t insert_ndx = orig_child_ndx + 1;

    size_t node_size = refs.size();
    TIGHTDB_ASSERT(insert_ndx <= node_size);
    if (TIGHTDB_LIKELY(node_size < TIGHTDB_MAX_LIST_SIZE)) {
        refs.insert(insert_ndx, new_sibling_ref);
        offsets.insert(insert_ndx - 1, elem_ndx_offset + state.m_split_offset);
        offsets.adjust(insert_ndx, 1);
        return 0; // Parent node was not split
    }

    // Split parent node
    Allocator& alloc = refs.get_alloc();
    Array new_refs(alloc), new_offsets(alloc);
    new_refs.create(type_HasRefs);
    new_offsets.create(type_Normal);
    size_t new_split_offset, new_split_size;
    if (insert_ndx == TIGHTDB_MAX_LIST_SIZE) {
        new_split_offset = elem_ndx_offset + state.m_split_offset;
        new_split_size   = elem_ndx_offset + state.m_split_size;
        offsets.set(orig_child_ndx, new_split_offset);
        new_refs.add(new_sibling_ref);
        new_offsets.add(state.m_split_size - state.m_split_offset);
    }
    else {
        new_split_offset = elem_ndx_offset + state.m_split_size;
        size_t offset = 0;
        for (size_t i = insert_ndx; i < node_size; ++i) {
            new_refs.add(refs.get(i));
            offset = size_t(offsets.get(i)) + 1;
            new_offsets.add(offset - new_split_offset);
        }
        new_split_size = offset; // From last iteration
        refs.resize(insert_ndx);
        refs.add(new_sibling_ref);
        offsets.resize(insert_ndx);
        offsets.set(orig_child_ndx, elem_ndx_offset + state.m_split_offset);
        offsets.add(new_split_offset);
    }

    state.m_split_offset = new_split_offset;
    state.m_split_size   = new_split_size;

    Array new_node(alloc);
    new_node.create(type_InnerColumnNode);
    new_node.add(new_offsets.get_ref());
    new_node.add(new_refs.get_ref());
    return new_node.get_ref();
}


ref_type Array::btree_leaf_insert(size_t ndx, int64_t value, TreeInsertBase& state)
{
    size_t leaf_size = size();
    TIGHTDB_ASSERT(leaf_size <= TIGHTDB_MAX_LIST_SIZE);
    if (leaf_size < ndx)
        ndx = leaf_size;
    if (TIGHTDB_LIKELY(leaf_size < TIGHTDB_MAX_LIST_SIZE)) {
        insert(ndx, value);
        return 0; // Leaf was not split
    }

    // Split leaf node
    Array new_leaf(m_alloc);
    new_leaf.create(has_refs() ? type_HasRefs : type_Normal);
    if (ndx == leaf_size) {
        new_leaf.add(value);
        state.m_split_offset = ndx;
    }
    else {
        for (size_t i = ndx; i != leaf_size; ++i) {
            new_leaf.add(get(i));
        }
        resize(ndx);
        add(value);
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
    TIGHTDB_ASSERT(!is_attached() ||
                   (m_width == 0 || m_width == 1 || m_width == 2 || m_width == 4 ||
                    m_width == 8 || m_width == 16 || m_width == 32 || m_width == 64));

    // Check that parent is set correctly
    if (!m_parent)
        return;

    ref_type ref_in_parent = m_parent->get_child_ref(m_ndx_in_parent);
    TIGHTDB_ASSERT(ref_in_parent == (is_attached() ? m_ref : 0));
}

void Array::to_dot(ostream& out, StringData title) const
{
    ref_type ref = get_ref();

    if (0 < title.size()) {
        out << "subgraph cluster_" << ref << " {" << endl;
        out << " label = \"" << title << "\";" << endl;
        out << " color = white;" << endl;
    }

    out << "n" << hex << ref << dec << "[shape=none,label=<";
    out << "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\"><TR>" << endl;

    // Header
    out << "<TD BGCOLOR=\"lightgrey\"><FONT POINT-SIZE=\"7\"> ";
    out << "0x" << hex << ref << dec << "<BR/>";
    if (m_isNode)
        out << "IsNode<BR/>";
    if (m_hasRefs)
        out << "HasRefs<BR/>";
    out << "</FONT></TD>" << endl;

    // Values
    for (size_t i = 0; i < m_size; ++i) {
        int64_t v =  get(i);
        if (m_hasRefs) {
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
    if (0 < title.size())
        out << "}" << endl;

    if (m_hasRefs) {
        for (size_t i = 0; i < m_size; ++i) {
            int64_t target = get(i);
            if (target == 0 || target & 0x1)
                continue; // zero-refs and refs that are not 64-aligned do not point to sub-trees

            out << "n" << hex << ref << dec << ":" << i;
            out << " -> n" << hex << target << dec << endl;
        }
    }

    out << endl;
}

void Array::stats(MemStats& stats) const
{
    size_t capacity_bytes = get_capacity_from_header();
    size_t bytes_used     = CalcByteLen(m_size, m_width);

    MemStats m(capacity_bytes, bytes_used, 1);
    stats.add(m);

    // Add stats for all sub-arrays
    if (m_hasRefs) {
        for (size_t i = 0; i < m_size; ++i) {
            int64_t v = get(i);
            if (v == 0 || v & 0x1)
                continue; // zero-refs and refs that are not 64-aligned do not point to sub-trees

            Array sub(to_ref(v), 0, 0, get_alloc());
            sub.stats(stats);
        }
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
    TIGHTDB_ASSERT(false);
    return int64_t(-1);
}

inline int64_t get_direct(const char* data, size_t width, size_t ndx) TIGHTDB_NOEXCEPT
{
    TIGHTDB_TEMPEX(return get_direct, width, (data, ndx));
}


// Lower/upper bound in sorted sequence:
// -------------------------------------
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
//
// It may be worth considering if overall efficiency can be improved
// by doing a linear search for short sequences.
template<int width>
inline size_t lower_bound(const char* data, size_t size, int64_t value) TIGHTDB_NOEXCEPT
{
    size_t i = 0;
    size_t size_2 = size;
    while (0 < size_2) {
        size_t half = size_2 / 2;
        size_t mid = i + half;
        int64_t probe = get_direct<width>(data, mid);
        if (probe < value) {
            i = mid + 1;
            size_2 -= half + 1;
        }
        else {
            size_2 = half;
        }
    }
    return i;
}

// See lower_bound()
template<int width>
inline size_t upper_bound(const char* data, size_t size, int64_t value) TIGHTDB_NOEXCEPT
{
    size_t i = 0;
    size_t size_2 = size;
    while (0 < size_2) {
        size_t half = size_2 / 2;
        size_t mid = i + half;
        int64_t probe = get_direct<width>(data, mid);
        if (!(value < probe)) {
            i = mid + 1;
            size_2 -= half + 1;
        }
        else {
            size_2 = half;
        }
    }
    return i;
}

// Find the index of the child node that contains the specified
// element index. Element index zero corresponds to the first element
// of the first leaf node contained in the subtree corresponding with
// the specified 'offsets' array.
//
// Returns (child_ndx, elem_ndx_offset) where 'elem_ndx_offset' is the
// element index of the first element of the identified child.
template<int width> inline pair<size_t, size_t>
find_child(const char* offsets_header, size_t elem_ndx) TIGHTDB_NOEXCEPT
{
    const char* offsets_data = Array::get_data_from_header(offsets_header);
    size_t offsets_size = Array::get_size_from_header(offsets_header);
    size_t child_ndx = upper_bound<width>(offsets_data, offsets_size, elem_ndx);
    size_t elem_ndx_offset = child_ndx == 0 ? 0 :
        to_size_t(get_direct<width>(offsets_data, child_ndx-1));
    return make_pair(child_ndx, elem_ndx_offset);
}


template<int width>
inline pair<size_t, size_t> get_two_as_size(const char* header, size_t ndx) TIGHTDB_NOEXCEPT
{
    const char* data = Array::get_data_from_header(header);
    return make_pair(to_size_t(get_direct<width>(data, ndx+0)),
                     to_size_t(get_direct<width>(data, ndx+1)));
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


void Array::find_all(Array& result, int64_t value, size_t colOffset, size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = m_size;
    TIGHTDB_ASSERT(start < m_size && end <= m_size && start < end);

    QueryState<int64_t> state;
    state.init(act_FindAll, &result, static_cast<size_t>(-1));
//    state.m_state = reinterpret_cast<int64_t>(&result);

    TIGHTDB_TEMPEX3(find, Equal, act_FindAll, m_width, (value, start, end, colOffset, &state, CallbackDummy()));

    return;
}


bool Array::find(int cond, Action action, int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t> *state) const
{
    if (cond == cond_Equal) {
        if (action == act_Sum) {
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
        if (action == act_Sum) {
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
        if (action == act_Sum) {
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
        if (action == act_Sum) {
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
        if (action == act_Sum) {
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
    TIGHTDB_ASSERT(false);
    return false;

}


size_t Array::find_first(int64_t value, size_t start, size_t end) const
{
    return find_first<Equal>(value, start, end);
}


// Get containing array block direct through column b-tree without instatiating any Arrays. Calling with
// use_retval = true will return itself if leaf and avoid unneccesary header initialization.
const Array* Array::GetBlock(size_t ndx, Array& arr, size_t& off,
                             bool use_retval) const TIGHTDB_NOEXCEPT
{
    // Reduce time overhead for cols with few entries
    if (is_leaf()) {
        if (!use_retval)
            arr.CreateFromHeaderDirect(get_header_from_data(m_data));
        off = 0;
        return this;
    }

    pair<MemRef, size_t> p = find_btree_leaf(ndx);
    arr.CreateFromHeaderDirect(p.first.m_addr);
    off = ndx - p.second;
    return &arr;
}


// Find value direct through column b-tree without instatiating any Arrays.
size_t Array::ColumnFind(int64_t target, ref_type ref, Array& cache) const
{
    char* header = m_alloc.translate(ref);
    bool is_leaf = get_isleaf_from_header(header);

    if (is_leaf) {
        cache.CreateFromHeaderDirect(header);
        return cache.find_first(target, 0, -1);
    }

    const char* data = get_data_from_header(header);
    size_t width = get_width_from_header(header);

    // Get subnode table
    ref_type offsets_ref = to_ref(get_direct(data, width, 0));
    ref_type refs_ref    = to_ref(get_direct(data, width, 1));

    const char* offsets_header = m_alloc.translate(offsets_ref);
    const char* offsets_data = get_data_from_header(offsets_header);
    size_t offsets_width  = get_width_from_header(offsets_header);
    size_t offsets_size = get_size_from_header(offsets_header);

    const char* refs_header = m_alloc.translate(refs_ref);
    const char* refs_data = get_data_from_header(refs_header);
    size_t refs_width  = get_width_from_header(refs_header);

    // Iterate over nodes until we find a match
    size_t offset = 0;
    for (size_t i = 0; i < offsets_size; ++i) {
        ref_type ref = to_ref(get_direct(refs_data, refs_width, i));
        size_t result = ColumnFind(target, ref, cache);
        if (result != not_found)
            return offset + result;

        size_t off = to_size_t(get_direct(offsets_data, offsets_width, i));
        offset = off;
    }

    // if we get to here there is no match
    return not_found;
}


size_t Array::IndexStringFindFirst(StringData value, void* column, StringGetter get_func) const
{
    StringData value_2 = value;
    const char* data   = m_data;
    const char* header;
    size_t width = m_width;
    bool is_leaf = !m_isNode;
    typedef StringIndex::key_type key_type;
    key_type key;

top:
    // Create 4 byte index key
    key = StringIndex::create_key(value_2);

    for (;;) {
        // Get subnode table
        ref_type offsets_ref = to_ref(get_direct(data, width, 0));
        ref_type refs_ref    = to_ref(get_direct(data, width, 1));

        // Find the position matching the key
        const char* offsets_header = m_alloc.translate(offsets_ref);
        const char* offsets_data = get_data_from_header(offsets_header);
        size_t offsets_size = get_size_from_header(offsets_header);
        size_t pos = ::lower_bound<32>(offsets_data, offsets_size, key); // keys are always 32 bits wide

        // If key is outside range, we know there can be no match
        if (pos == offsets_size)
            return not_found;

        // Get entry under key
        const char* refs_header = m_alloc.translate(refs_ref);
        const char* refs_data = get_data_from_header(refs_header);
        size_t refs_width  = get_width_from_header(refs_header);
        int64_t ref = get_direct(refs_data, refs_width, pos);

        if (!is_leaf) {
            // Set vars for next iteration
            header  = m_alloc.translate(to_ref(ref));
            data    = get_data_from_header(header);
            width   = get_width_from_header(header);
            is_leaf = get_isleaf_from_header(header);
            continue;
        }

        key_type stored_key = key_type(get_direct<32>(offsets_data, pos));

        if (stored_key != key)
            return not_found;

        // Literal row index
        if (ref & 1) {
            size_t row_ref = size_t(uint64_t(ref) >> 1);

            // If the last byte in the stored key is zero, we know that we have
            // compared against the entire (target) string
            if (!(stored_key << 24))
                return row_ref;

            StringData str = (*get_func)(column, row_ref);
            if (str == value)
                return row_ref;
            return not_found;
        }

        const char* sub_header = m_alloc.translate(to_ref(ref));
        const bool sub_isindex = get_indexflag_from_header(sub_header);

        // List of matching row indexes
        if (!sub_isindex) {
            const char* sub_data = get_data_from_header(sub_header);
            const size_t sub_width  = get_width_from_header(sub_header);
            const bool sub_isleaf = get_isleaf_from_header(sub_header);

            // In most cases the row list will just be an array but
            // there might be so many matches that it has branched
            // into a column
            size_t row_ref;
            if (sub_isleaf)
                row_ref = to_size_t(get_direct(sub_data, sub_width, 0));
            else {
                Array sub(to_ref(ref), 0, 0, m_alloc);
                pair<MemRef, size_t> p = sub.find_btree_leaf(0);
                const char* leaf_header = p.first.m_addr;
                row_ref = to_size_t(get(leaf_header, 0));
            }

            // If the last byte in the stored key is zero, we know
            // that we have compared against the entire (target)
            // string
            if (!(stored_key << 24))
                return row_ref;

            StringData str = (*get_func)(column, row_ref);
            if (str == value)
                return row_ref;
            return not_found;
        }

        // Recurse into sub-index;
        header  = m_alloc.translate(to_ref(ref)); // FIXME: This is wastefull since sub_header already contains this result
        data    = get_data_from_header(header);
        width   = get_width_from_header(header);
        is_leaf = get_isleaf_from_header(header);
        if (value_2.size() <= 4) {
            value_2 = StringData();
        }
        else {
            value_2 = value_2.substr(4);
        }
        goto top;
    }
}


void Array::IndexStringFindAll(Array& result, StringData value, void* column, StringGetter get_func) const
{
    StringData value_2 = value;
    const char* data = m_data;
    const char* header;
    size_t width = m_width;
    bool is_leaf = !m_isNode;
    typedef StringIndex::key_type key_type;
    key_type key;

top:
    // Create 4 byte index key
    key = StringIndex::create_key(value_2);

    for (;;) {
        // Get subnode table
        ref_type offsets_ref = to_ref(get_direct(data, width, 0));
        ref_type refs_ref    = to_ref(get_direct(data, width, 1));

        // Find the position matching the key
        const char* offsets_header = m_alloc.translate(offsets_ref);
        const char* offsets_data = get_data_from_header(offsets_header);
        size_t offsets_size = get_size_from_header(offsets_header);
        size_t pos = ::lower_bound<32>(offsets_data, offsets_size, key); // keys are always 32 bits wide

        // If key is outside range, we know there can be no match
        if (pos == offsets_size)
            return; // not_found

        // Get entry under key
        const char* refs_header = m_alloc.translate(refs_ref);
        const char* refs_data = get_data_from_header(refs_header);
        size_t refs_width  = get_width_from_header(refs_header);
        int64_t ref = get_direct(refs_data, refs_width, pos);

        if (!is_leaf) {
            // Set vars for next iteration
            header  = m_alloc.translate(to_ref(ref));
            data    = get_data_from_header(header);
            width   = get_width_from_header(header);
            is_leaf = get_isleaf_from_header(header);
            continue;
        }

        key_type stored_key = key_type(get_direct<32>(offsets_data, pos));

        if (stored_key != key)
            return; // not_found

        // Literal row index
        if (ref & 1) {
            size_t row_ref = size_t(uint64_t(ref) >> 1);

            // If the last byte in the stored key is zero, we know that we have
            // compared against the entire (target) string
            if (!(stored_key << 24)) {
                result.add(row_ref);
                return;
            }

            StringData str = (*get_func)(column, row_ref);
            if (str == value)
                result.add(row_ref);
            return; // not_found
        }

        const char* sub_header = m_alloc.translate(to_ref(ref));
        const bool sub_isindex = get_indexflag_from_header(sub_header);

        // List of matching row indexes
        if (!sub_isindex) {
            const bool sub_isleaf = get_isleaf_from_header(sub_header);

            // In most cases the row list will just be an array but there
            // might be so many matches that it has branched into a column
            if (sub_isleaf) {
                const size_t sub_width = get_width_from_header(sub_header);
                const char* sub_data = get_data_from_header(sub_header);
                const size_t first_row_ref = to_size_t(get_direct(sub_data, sub_width, 0));

                // If the last byte in the stored key is not zero, we have
                // not yet compared against the entire (target) string
                if ((stored_key << 24)) {
                    StringData str = (*get_func)(column, first_row_ref);
                    if (str != value)
                        return; // not_found
                }

                // Copy all matches into result array
                const size_t sub_size  = get_size_from_header(sub_header);

                for (size_t i = 0; i < sub_size; ++i) {
                    size_t row_ref = to_size_t(get_direct(sub_data, sub_width, i));
                    result.add(row_ref);
                }
            }
            else {
                const Column sub(to_ref(ref), 0, 0, m_alloc);
                const size_t first_row_ref = to_size_t(sub.get(0));

                // If the last byte in the stored key is not zero, we have
                // not yet compared against the entire (target) string
                if ((stored_key << 24)) {
                    StringData str = (*get_func)(column, first_row_ref);
                    if (str != value)
                        return; // not_found
                }

                // Copy all matches into result array
                const size_t sub_size  = sub.size();

                for (size_t i = 0; i < sub_size; ++i) {
                    size_t row_ref = to_size_t(sub.get(i));
                    result.add(row_ref);
                }
            }
            return;
        }

        // Recurse into sub-index;
        header  = m_alloc.translate(to_ref(ref)); // FIXME: This is wastefull since sub_header already contains this result
        data    = get_data_from_header(header);
        width   = get_width_from_header(header);
        is_leaf = get_isleaf_from_header(header);
        if (value_2.size() <= 4) {
            value_2 = StringData();
        }
        else {
            value_2 = value_2.substr(4);
        }
        goto top;
    }
}


FindRes Array::IndexStringFindAllNoCopy(StringData value, size_t& res_ref, void* column, StringGetter get_func) const
{
    StringData value_2 = value;
    const char* data = m_data;
    const char* header;
    size_t width = m_width;
    bool is_leaf = !m_isNode;
    typedef StringIndex::key_type key_type;
    key_type key;

top:
    // Create 4 byte index key
    key = StringIndex::create_key(value_2);

    for (;;) {
        // Get subnode table
        ref_type offsets_ref = to_ref(get_direct(data, width, 0));
        ref_type refs_ref    = to_ref(get_direct(data, width, 1));

        // Find the position matching the key
        const char* offsets_header = m_alloc.translate(offsets_ref);
        const char* offsets_data   = get_data_from_header(offsets_header);
        size_t offsets_size = get_size_from_header(offsets_header);
        size_t pos = ::lower_bound<32>(offsets_data, offsets_size, key); // keys are always 32 bits wide

        // If key is outside range, we know there can be no match
        if (pos == offsets_size)
            return FindRes_not_found;

        // Get entry under key
        const char* refs_header = m_alloc.translate(refs_ref);
        const char* refs_data   = get_data_from_header(refs_header);
        size_t refs_width  = get_width_from_header(refs_header);
        int64_t ref = get_direct(refs_data, refs_width, pos);

        if (!is_leaf) {
            // Set vars for next iteration
            header  = m_alloc.translate(to_ref(ref));
            data    = get_data_from_header(header);
            width   = get_width_from_header(header);
            is_leaf = get_isleaf_from_header(header);
            continue;
        }

        key_type stored_key = key_type(get_direct<32>(offsets_data, pos));

        if (stored_key != key)
            return FindRes_not_found; // not_found

        // Literal row index
        if (ref & 1) {
            size_t row_ref = size_t(uint64_t(ref) >> 1);

            // If the last byte in the stored key is zero, we know that we have
            // compared against the entire (target) string
            if (!(stored_key << 24)) {
                res_ref = row_ref;
                return FindRes_single; // found single
            }

            StringData str = (*get_func)(column, row_ref);
            if (str == value) {
                res_ref = row_ref;
                return FindRes_single; // found single
            }
            return FindRes_not_found; // not_found
        }

        const char* sub_header  = m_alloc.translate(to_ref(ref));
        const bool  sub_isindex = get_indexflag_from_header(sub_header);

        // List of matching row indexes
        if (!sub_isindex) {
            const bool sub_isleaf = get_isleaf_from_header(sub_header);

            // In most cases the row list will just be an array but there
            // might be so many matches that it has branched into a column
            if (sub_isleaf) {
                const size_t sub_width = get_width_from_header(sub_header);
                const char*  sub_data  = get_data_from_header(sub_header);
                const size_t first_row_ref = to_size_t(get_direct(sub_data, sub_width, 0));

                // If the last byte in the stored key is not zero, we have
                // not yet compared against the entire (target) string
                if ((stored_key << 24)) {
                    StringData str = (*get_func)(column, first_row_ref);
                    if (str != value)
                        return FindRes_not_found; // not_found
                }
            }
            else {
                const Column sub(to_ref(ref), 0, 0, m_alloc);
                const size_t first_row_ref = to_size_t(sub.get(0));

                // If the last byte in the stored key is not zero, we have
                // not yet compared against the entire (target) string
                if ((stored_key << 24)) {
                    StringData str = (*get_func)(column, first_row_ref);
                    if (str != value)
                        return FindRes_not_found; // not_found
                }
            }

            // Return a reference to the result column
            res_ref = to_ref(ref);
            return FindRes_column; // column of matches
        }

        // Recurse into sub-index;
        header  = m_alloc.translate(to_ref(ref)); // FIXME: This is wastefull since sub_header already contains this result
        data    = get_data_from_header(header);
        width   = get_width_from_header(header);
        is_leaf = get_isleaf_from_header(header);
        if (value_2.size() <= 4) {
            value_2 = StringData();
        }
        else {
            value_2 = value_2.substr(4);
        }
        goto top;
    }
}


size_t Array::IndexStringCount(StringData value, void* column, StringGetter get_func) const

{
    StringData value_2 = value;
    const char* data   = m_data;
    const char* header;
    size_t width = m_width;
    bool is_leaf = !m_isNode;
    typedef StringIndex::key_type key_type;
    key_type key;

top:
    // Create 4 byte index key
    key = StringIndex::create_key(value_2);

    for (;;) {
        // Get subnode table
        ref_type offsets_ref = to_ref(get_direct(data, width, 0));
        ref_type refs_ref    = to_ref(get_direct(data, width, 1));

        // Find the position matching the key
        const char* offsets_header = m_alloc.translate(offsets_ref);
        const char* offsets_data = get_data_from_header(offsets_header);
        size_t offsets_size = get_size_from_header(offsets_header);
        size_t pos = ::lower_bound<32>(offsets_data, offsets_size, key); // keys are always 32 bits wide

        // If key is outside range, we know there can be no match
        if (pos == offsets_size)
            return 0;

        // Get entry under key
        const char* refs_header = m_alloc.translate(refs_ref);
        const char* refs_data = get_data_from_header(refs_header);
        size_t refs_width  = get_width_from_header(refs_header);
        int64_t ref = get_direct(refs_data, refs_width, pos);

        if (!is_leaf) {
            // Set vars for next iteration
            header  = m_alloc.translate(to_ref(ref));
            data    = get_data_from_header(header);
            width   = get_width_from_header(header);
            is_leaf = get_isleaf_from_header(header);
            continue;
        }

        key_type stored_key = key_type(get_direct<32>(offsets_data, pos));

        if (stored_key != key)
            return 0;

        // Literal row index
        if (ref & 1) {
            const size_t row_ref = size_t((uint64_t(ref) >> 1));

            // If the last byte in the stored key is zero, we know that we have
            // compared against the entire (target) string
            if (!(stored_key << 24))
                return 1;

            StringData str = (*get_func)(column, row_ref);
            if (str == value)
                return 1;
            return 0;
        }

        const char* sub_header = m_alloc.translate(to_ref(ref));
        const bool sub_isindex = get_indexflag_from_header(sub_header);

        // List of matching row indexes
        if (!sub_isindex) {
            const bool sub_isleaf = get_isleaf_from_header(sub_header);
            size_t sub_count;
            size_t row_ref;

            // In most cases the row list will just be an array but
            // there might be so many matches that it has branched
            // into a column
            if (sub_isleaf) {
                sub_count  = get_size_from_header(sub_header);

                // If the last byte in the stored key is zero, we know
                // that we have compared against the entire (target)
                // string
                if (!(stored_key << 24))
                    return sub_count;

                const char* sub_data = get_data_from_header(sub_header);
                const size_t sub_width  = get_width_from_header(sub_header);
                row_ref = to_size_t(get_direct(sub_data, sub_width, 0));
            }
            else {
                const Column sub(to_ref(ref), 0, 0, m_alloc);
                sub_count = sub.size();

                // If the last byte in the stored key is zero, we know
                // that we have compared against the entire (target)
                // string
                if (!(stored_key << 24))
                    return sub_count;

                row_ref = to_size_t(sub.get(0));
            }

            StringData str = (*get_func)(column, row_ref);
            if (str == value)
                return sub_count;
            return 0;
        }

        // Recurse into sub-index;
        header  = m_alloc.translate(to_ref(ref)); // FIXME: This is wastefull since sub_header already contains this result
        data    = get_data_from_header(header);
        width   = get_width_from_header(header);
        is_leaf = get_isleaf_from_header(header);
        if (value_2.size() <= 4) {
            value_2 = StringData();
        }
        else {
            value_2 = value_2.substr(4);
        }
        goto top;
    }
}


pair<MemRef, size_t> Array::find_btree_leaf(size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!is_leaf());
    ref_type offsets_ref = get_as_ref(0);
    ref_type refs_ref    = get_as_ref(1);
    for (;;) {
        char* header = m_alloc.translate(offsets_ref);
        int width = get_width_from_header(header);
        pair<size_t, size_t> p;
        TIGHTDB_TEMPEX(p = find_child, width, (header, ndx));
        size_t child_ndx       = p.first;
        size_t elem_ndx_offset = p.second;
        ndx -= elem_ndx_offset; // local index

        header = m_alloc.translate(refs_ref);
        width = get_width_from_header(header);
        ref_type child_ref = to_ref(get_direct(get_data_from_header(header), width, child_ndx));

        header = m_alloc.translate(child_ref);
        bool child_is_leaf = get_isleaf_from_header(header);
        if (child_is_leaf)
            return make_pair(MemRef(header, child_ref), ndx);

        width = get_width_from_header(header);
        TIGHTDB_TEMPEX(p = ::get_two_as_size, width, (header, 0));
        offsets_ref = p.first;
        refs_ref    = p.second;
    }
}


int64_t Array::get(const char* header, size_t ndx) TIGHTDB_NOEXCEPT
{
    const char* data = get_data_from_header(header);
    int width = get_width_from_header(header);
    return get_direct(data, width, ndx);
}


pair<size_t, size_t> Array::get_size_pair(const char* header, size_t ndx) TIGHTDB_NOEXCEPT
{
    pair<size_t, size_t> p;
    int width = get_width_from_header(header);
    TIGHTDB_TEMPEX(p = ::get_two_as_size, width, (header, ndx));
    return p;
}
