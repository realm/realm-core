#include <limits>
#include <algorithm>
#include <vector>
#include <iostream>
#include <iomanip>

#ifdef _MSC_VER
    #include <intrin.h>
    #include <win32/types.h>
    #pragma warning (disable : 4127) // Condition is constant warning
#endif

#include <tightdb/terminate.hpp>
#include <tightdb/array.hpp>
#include <tightdb/column.hpp>
#include <tightdb/query_conditions.hpp>
#include <tightdb/column_string.hpp>

using namespace std;


namespace tightdb {

// Header format (8 bytes):
// |--------|--------|--------|--------|--------|--------|--------|--------|
// |12344555|          length          |         capacity         |reserved|
//
//  1: isNode  2: hasRefs  3: indexflag 4: multiplier 5: width (packed in 3 bits)

void Array::init_from_ref(size_t ref) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ref);
    char* header = static_cast<char*>(m_alloc.Translate(ref));
    CreateFromHeader(header, ref);
}

void Array::CreateFromHeaderDirect(char* header, size_t ref) TIGHTDB_NOEXCEPT
{
    // Parse header
    // We only need limited info for direct read-only use
    m_width    = get_width_from_header(header);
    m_len      = get_len_from_header(header);

    m_ref = ref;
    m_data = header + 8;

    SetWidth(m_width);
}

void Array::CreateFromHeader(char* header, size_t ref) TIGHTDB_NOEXCEPT
{
    // Parse header
    m_isNode   = get_isnode_from_header(header);
    m_hasRefs  = get_hasrefs_from_header(header);
    m_width    = get_width_from_header(header);
    m_len      = get_len_from_header(header);
    const size_t byte_capacity = get_capacity_from_header(header);

    // Capacity is how many items there are room for
    m_capacity = CalcItemCount(byte_capacity, m_width);

    m_ref = ref;
    m_data = header + 8;

    SetWidth(m_width);
}


void Array::SetType(ColumnDef type)
{
    // If we are reviving an invalidated array
    // we need to reset state first
    if (!m_data) {
        m_ref = 0;
        m_capacity = 0;
        m_len = 0;
        m_width = size_t(-1);
    }

    if (m_ref) CopyOnWrite(); // Throws

    bool is_node = false, has_refs = false;
    switch (type) {
        case coldef_Normal:                               break;
        case coldef_InnerNode: has_refs = is_node = true; break;
        case coldef_HasRefs:   has_refs = true;           break;
    }
    m_isNode  = is_node;
    m_hasRefs = has_refs;

    if (!m_data) {
        // Create array
        Alloc(0, 0);
        SetWidth(0);
    }
    else {
        // Update Header
        set_header_isnode(is_node);
        set_header_hasrefs(has_refs);
    }
}

bool Array::operator==(const Array& a) const
{
    return m_data == a.m_data;
}

void Array::UpdateRef(size_t ref)
{
    init_from_ref(ref);
    update_ref_in_parent();
}

bool Array::UpdateFromParent() TIGHTDB_NOEXCEPT
{
    if (!m_parent) return false;

    // After commit to disk, the array may have moved
    // so get ref from parent and see if it has changed
    const size_t new_ref = m_parent->get_child_ref(m_parentNdx);

    if (new_ref != m_ref) {
        init_from_ref(new_ref);
        return true;
    }
    else {
        // If the file has been remapped it might have
        // moved to a new location
        char* m = static_cast<char*>(m_alloc.Translate(m_ref));
        if (m_data-8 != m) {
            m_data = m + 8;
            return true;
        }
    }

    return false; // not modified
}

/**
 * Takes a 64bit value and return the minimum number of bits needed to fit the
 * value.
 * For alignment this is rounded up to nearest log2.
 * Posssible results {0, 1, 2, 4, 8, 16, 32, 64}
 */
// FIXME: Deprecated use of 'static' - use anonymous namespace instead.
static size_t BitWidth(int64_t v)
{
    // FIXME: Assuming there is a 64-bit CPU reverse bitscan instruction and it is fast, then this function could be implemented simply as (v<2 ? v : 2<<rev_bitscan(rev_bitscan(v))).

    if ((v >> 4) == 0) {
        static const int8_t bits[] = {0, 1, 2, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4};
        return bits[(int8_t)v];
    }

    // First flip all bits if bit 63 is set (will now always be zero)
    if (v < 0) v = ~v;

    // Then check if bits 15-31 used (32b), 7-31 used (16b), else (8b)
    return v >> 31 ? 64 : v >> 15 ? 32 : v >> 7 ? 16 : 8;
}

// Allocates space for 'count' items being between min and min in size, both inclusive. Crashes! Why? Todo/fixme
void Array::Preset(size_t bitwidth, size_t count)
{
    Clear();
    SetWidth(bitwidth);
    Alloc(count, bitwidth); // Throws
    m_len = count;
    for (size_t n = 0; n < count; n++)
        Set(n, 0);
}

void Array::Preset(int64_t min, int64_t max, size_t count)
{
    size_t w = std::max(BitWidth(max), BitWidth(min));
    Preset(w, count);
}

void Array::SetParent(ArrayParent *parent, size_t pndx) TIGHTDB_NOEXCEPT
{
    m_parent = parent;
    m_parentNdx = pndx;
}

void Array::Destroy()
{
    if (!m_data) return;

    if (m_hasRefs) {
        for (size_t i = 0; i < m_len; ++i) {
            const size_t ref = (size_t)Get(i);

            // null-refs signify empty sub-trees
            if (ref == 0) continue;

            // all refs are 64bit aligned, so the lowest bits
            // cannot be set. If they are it means that it should
            // not be interpreted as a ref
            if (ref & 0x1) continue;

            Array sub(ref, this, i, m_alloc);
            sub.Destroy();
        }
    }

    void* const ref = m_data-8;
    m_alloc.Free(m_ref, ref);
    m_data = NULL;
}

void Array::Clear()
{
    CopyOnWrite(); // Throws

    // Make sure we don't have any dangling references
    if (m_hasRefs) {
        for (size_t i = 0; i < size(); ++i) {
            const size_t ref = GetAsRef(i);
            if (ref == 0 || ref & 0x1) continue; // zero-refs and refs that are not 64-aligned do not point to sub-trees

            Array sub(ref, this, i, m_alloc);
            sub.Destroy();
        }
    }

    // Truncate size to zero (but keep capacity)
    m_len      = 0;
    m_capacity = CalcItemCount(get_capacity_from_header(), 0);
    SetWidth(0);

    // Update header
    set_header_len(0);
    set_header_width(0);
}

void Array::Delete(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_len);

    // Check if we need to copy before modifying
    CopyOnWrite(); // Throws

    // Move values below deletion up
    if (m_width < 8) {
        for (size_t i = ndx+1; i < m_len; ++i) {
            const int64_t v = (this->*m_getter)(i);
            (this->*m_setter)(i-1, v);
        }
    }
    else if (ndx < m_len-1) {
        // when byte sized, use memmove
// FIXME: Should probably be optimized as a simple division by 8.
        const size_t w = (m_width == 64) ? 8 : (m_width == 32) ? 4 : (m_width == 16) ? 2 : 1;
        char* const base = reinterpret_cast<char*>(m_data);
        char* const dst_begin = base + ndx*w;
        const char* const src_begin = dst_begin + w;
        const char* const src_end   = base + m_len*w;
        copy(src_begin, src_end, dst_begin);
    }

    // Update length (also in header)
    --m_len;
    set_header_len(m_len);
}

void Array::Set(size_t ndx, int64_t value)
{
    TIGHTDB_ASSERT(ndx < m_len);

    // Check if we need to copy before modifying
    CopyOnWrite(); // Throws

    // Make room for the new value
    size_t width = m_width;

    if (value < m_lbound || value > m_ubound)
        width = BitWidth(value);

    const bool doExpand = (width > m_width);
    if (doExpand) {
        Getter oldGetter = m_getter;    // Save old getter before width expansion
        Alloc(m_len, width); // Throws
        SetWidth(width);

        // Expand the old values
        int k = (int)m_len;
        while (--k >= 0) {
            const int64_t v = (this->*oldGetter)(k);
            (this->*m_setter)(k, v);
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
        if (m_len < m_capacity) {
            (this->*m_setter)(m_len, value);
            ++m_len;
            set_header_len(m_len);
            return;
        }
    }

    Insert(m_len, value);
}
*/

void Array::Insert(size_t ndx, int64_t value)
{
    TIGHTDB_ASSERT(ndx <= m_len);

    // Check if we need to copy before modifying
    CopyOnWrite(); // Throws

    // Make room for the new value
    size_t width = m_width;

    if (value < m_lbound || value > m_ubound)
        width = BitWidth(value);

    Getter oldGetter = m_getter;    // Save old getter before potential width expansion

    const bool doExpand = (width > m_width);
    if (doExpand) {
        Alloc(m_len+1, width); // Throws
        SetWidth(width);
    }
    else {
        Alloc(m_len+1, m_width); // Throws
    }

    // Move values below insertion (may expand)
    if (doExpand || m_width < 8) {
        int k = int(m_len);
        while (--k >= int(ndx)) {
            const int64_t v = (this->*oldGetter)(k);
            (this->*m_setter)(k+1, v);
        }
    }
    else if (ndx != m_len) {
        // when byte sized and no expansion, use memmove
// FIXME: Optimize by simply dividing by 8 (or shifting right by 3 bit positions)
        const size_t w = (m_width == 64) ? 8 : (m_width == 32) ? 4 : (m_width == 16) ? 2 : 1;
        char* const base = reinterpret_cast<char*>(m_data);
        char* const src_begin = base + ndx*w;
        char* const src_end   = base + m_len*w;
        char* const dst_end   = src_end + w;
        copy_backward(src_begin, src_end, dst_end);
    }

    // Insert the new value
    (this->*m_setter)(ndx, value);

    // Expand values above insertion
    if (doExpand) {
        int k = int(ndx);
        while (--k >= 0) {
            const int64_t v = (this->*oldGetter)(k);
            (this->*m_setter)(k, v);
        }
    }

    // Update length
    // (no need to do it in header as it has been done by Alloc)
    ++m_len;
}


void Array::add(int64_t value)
{
    Insert(m_len, value);
}

void Array::Resize(size_t count)
{
    TIGHTDB_ASSERT(count <= m_len);

    CopyOnWrite(); // Throws

    // Update length (also in header)
    m_len = count;
    set_header_len(m_len);
}

void Array::SetAllToZero()
{
    CopyOnWrite(); // Throws

    m_capacity = CalcItemCount(get_capacity_from_header(), 0);
    SetWidth(0);

    // Update header
    set_header_width(0);
}

void Array::Increment(int64_t value, size_t start, size_t end)
{
    if (end == (size_t)-1) end = m_len;
    TIGHTDB_ASSERT(start < m_len);
    TIGHTDB_ASSERT(end >= start && end <= m_len);

    // Increment range
    for (size_t i = start; i < end; ++i) {
        Set(i, Get(i) + value);
    }
}

void Array::IncrementIf(int64_t limit, int64_t value)
{
    // Update (incr or decrement) values bigger or equal to the limit
    for (size_t i = 0; i < m_len; ++i) {
        const int64_t v = Get(i);
        if (v >= limit)
            Set(i, v + value);
    }
}

void Array::Adjust(size_t start, int64_t diff)
{
    TIGHTDB_ASSERT(start <= m_len);

    for (size_t i = start; i < m_len; ++i) {
        const int64_t v = Get(i);
        Set(i, v + diff);
    }
}


// Binary search based on:
// http://www.tbray.org/ongoing/When/200x/2003/03/22/Binary
// Finds position of largest value SMALLER than the target (for lookups in
// nodes)
// Todo: rename to LastLessThan()
template <size_t w> size_t Array::FindPos(int64_t target) const TIGHTDB_NOEXCEPT
{
    size_t low = size_t(-1);
    size_t high = m_len;

    // Binary search based on:
    // http://www.tbray.org/ongoing/When/200x/2003/03/22/Binary
    // Finds position of largest value SMALLER than the target (for lookups in
    // nodes)
    while (high - low > 1) {
        const size_t probe = (low + high) >> 1;
        const int64_t v = Get<w>(probe);

        if (v > target)
            high = probe;
        else
            low = probe;
    }
    if (high == m_len)
        return not_found;
    else
        return high;
}

size_t Array::FindPos(int64_t target) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_TEMPEX(return FindPos, m_width, (target));
}

// BM FIXME: Rename to something better... // FirstGTE()
size_t Array::FindPos2(int64_t target) const
{
    size_t low = (size_t)-1;
    size_t high = m_len;

    // Binary search based on:
    // http://www.tbray.org/ongoing/When/200x/2003/03/22/Binary
    // Finds position of closest value BIGGER OR EQUAL to the target (for
    // lookups in indexes)
    while (high - low > 1) {
        const size_t probe = (low + high) >> 1;
        const int64_t v = Get(probe);

        if (v < target)
            low = probe;
        else
            high = probe;
    }
    if (high == m_len)
        return not_found;
    else
        return high;
}

// return first element E for which E >= target or return -1 if none. Array must be sorted
size_t Array::FindGTE(int64_t target, size_t start) const
{
#if TIGHTDB_DEBUG
    // Reference implementation to illustrate and test behaviour
    size_t ref = 0;
    size_t idx;
    for (idx = start; idx < m_len; ++idx) {
        if (Get(idx) >= target) {
            ref = idx;
            break;
        }
    }
    if (idx == m_len)
        ref = not_found;
#endif

    size_t ret;

    if (start >= m_len) {ret = not_found; goto exit;}

    if (start + 2 < m_len) {
        if (Get(start) >= target) {ret = start; goto exit;} else ++start;
        if (Get(start) >= target) {ret = start; goto exit;} else ++start;
    }

    // Todo, use templated Get<width> from this point for performance
    if (target > Get(m_len - 1)) {ret = not_found; goto exit;}

    size_t add;
    add = 1;

    for(;;) {
        if (start + add < m_len && Get(start + add) < target)
            start += add;
        else
            break;
       add *= 2;
    }

    size_t high;
    high = start + add + 1;

    if (high > m_len)
        high = m_len;

   // if (start > 0)
        start--;

    //start og high

    size_t orig_high;
    orig_high = high;

    while (high - start > 1) {
        const size_t probe = (start + high) / 2;
        const int64_t v = Get(probe);
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

r = MultiplyDeBruijnBitPosition[((uint32_t)((v & -(int)v) * 0x077CB531U)) >> 27];
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
    unsigned int v0 = (unsigned int)v;
    unsigned int v1 = (unsigned int)(v >> 32);
    size_t r;

    if (v0 != 0)
        r = FirstSetBit(v0);
    else
        r = FirstSetBit(v1) + 32;

    return r;
#endif
}


template <size_t width> inline int64_t LowerBits(void)
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
template <size_t width> inline bool has_zero_element(uint64_t value) {
    uint64_t hasZeroByte;
    uint64_t lower = LowerBits<width>();
    uint64_t upper = LowerBits<width>() * 1ULL << (width == 0 ? 0 : (width - 1ULL));
    hasZeroByte = (value - lower) & ~value & upper;
    return hasZeroByte != 0;
}


// Finds zero element of bit width 'width'
template <bool eq, size_t width> size_t FindZero(uint64_t v)
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

template <bool find_max, size_t w> bool Array::minmax(int64_t& result, size_t start, size_t end) const
{
    if (end == (size_t)-1)
        end = m_len;
    TIGHTDB_ASSERT(start < m_len && end <= m_len && start < end);

    if (m_len == 0)
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
        for (; (start < end) && ((((size_t)m_data & 0xf) * 8 + start * w) % (128) != 0); start++) {
            if (find_max ? Get<w>(start) > m : Get<w>(start) < m)
                m = Get<w>(start);
        }

        if ((w == 8 || w == 16 || w == 32) && end - start > 2 * sizeof(__m128i) * 8 / NO0(w)) {
            __m128i *data = (__m128i *)(m_data + start * w / 8);
            __m128i state = data[0];
            char state2[sizeof(state)];

            size_t chunks = (end - start) * w / 8 / sizeof(__m128i);
            for (size_t t = 0; t < chunks; t++) {
                if (w == 8)
                    state = find_max ? _mm_max_epi8(data[t], state) : _mm_min_epi8(data[t], state);
                else if (w == 16)
                    state = find_max ? _mm_max_epi16(data[t], state) : _mm_min_epi16(data[t], state);
                else if (w == 32)
                    state = find_max ? _mm_max_epi32(data[t], state) : _mm_min_epi32(data[t], state);

                start += sizeof(__m128i) * 8 / NO0(w);
            }

            // Todo: prevent taking address of 'state' to make the compiler keep it in SSE register in above loop (vc2010/gcc4.6)

            // We originally had declared '__m128i state2' and did an 'state2 = state' assignment. When we read from state2 through int16_t, int32_t or int64_t in GetUniversal(),
            // the compiler thinks it cannot alias state2 and hence reorders the read and assignment.

            // In this fixed version using memcpy, we have char-read-access from __m128i (OK aliasing) and char-write-access to char-array, and finally int8/16/32/64 
            // read access from char-array (OK aliasing).
            memcpy(&state2, &state, sizeof(state));
            for (size_t t = 0; t < sizeof(__m128i) * 8 / NO0(w); ++t) {
                const int64_t v = GetUniversal<w>(((const char *)&state2), t);
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

template <size_t w> int64_t Array::sum(size_t start, size_t end) const
{
    if (end == (size_t)-1) end = m_len;
    TIGHTDB_ASSERT(start < m_len && end <= m_len && start < end);

    if (w == 0)
        return 0;

    int64_t s = 0;

    // Sum manually until 128 bit aligned
    for (; (start < end) && ((((size_t)m_data & 0xf) * 8 + start * w) % 128 != 0); start++) {
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

        int64_t *data = (int64_t *)(m_data + start * w / 8);
        size_t chunks = (end - start) * w / 8 / sizeof(int64_t);

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
        start += sizeof(int64_t) * 8 / no0(w) * chunks;
    }

#ifdef TIGHTDB_COMPILER_SSE
    if (cpuid_sse<42>()) {

        // 2000 items summed 500000 times, 8/16/32 bits, miliseconds:
        // Naive, templated Get<>: 391 371 374
        // SSE:                     97 148 282

        if ((w == 8 || w == 16 || w == 32) && end - start > sizeof(__m128i) * 8 / no0(w)) {
            __m128i *data = (__m128i *)(m_data + start * w / 8);
            __m128i sum = {0};
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
            start += sizeof(__m128i) * 8 / no0(w) * chunks;

            // prevent taking address of 'state' to make the compiler keep it in SSE register in above loop (vc2010/gcc4.6)
            sum2 = sum;

            // Avoid aliasing bug where sum2 might not yet be initialized when accessed by GetUniversal
            char sum3[sizeof(sum2)];
            memcpy(&sum3, &sum2, sizeof(sum2));

            // Sum elements of sum
            for (size_t t = 0; t < sizeof(__m128i) * 8 / ((w == 8 || w == 16) ? 32 : 64); ++t) {
                int64_t v = GetUniversal<(w == 8 || w == 16) ? 32 : 64>(((const char *)&sum3), t);
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
    const uint64_t* const next = (const uint64_t*)m_data;
    size_t count = 0;
    const size_t end = m_len;
    size_t i = 0;

    // static values needed for fast population count
    const uint64_t m1  = 0x5555555555555555ULL;
    const uint64_t m2  = 0x3333333333333333ULL;
    const uint64_t m4  = 0x0f0f0f0f0f0f0f0fULL;
    const uint64_t h01 = 0x0101010101010101ULL;

    if (m_width == 0) {
        if (value == 0) return m_len;
        else return 0;
    }
    else if (m_width == 1) {
        if ((uint64_t)value > 1) return 0;

        const size_t chunkvals = 64;
        for (; i + chunkvals <= end; i += chunkvals) {
            uint64_t a = next[i / chunkvals];
            if (value == 0) a = ~a; // reverse

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
        if ((uint64_t)value > 3) return 0;

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
        if ((uint64_t)value > 15) return 0;

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
        if (value > 0x7FLL || value < -0x80LL) return 0; // by casting?

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
        if (value > 0x7FFFLL || value < -0x8000LL) return 0; // by casting?

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
        const int32_t v = (int32_t)value;
        const int32_t* const d = (const int32_t*)m_data;
        for (; i < end; ++i) {
            if (d[i] == v)
                ++count;
        }
        return count;
    }
    else if (m_width == 64) {
        const int64_t* const d = (const int64_t*)m_data;
        for (; i < end; ++i) {
            if (d[i] == value)
                ++count;
        }
        return count;
    }

    // Sum remainding elements
    for (; i < end; ++i)
        if (value == Get(i))
            ++count;

    return count;
}


void Array::FindAllHamming(Array& result, uint64_t value, size_t maxdist, size_t offset) const
{
    (void)result;
    (void)value;
    (void)maxdist;
    (void)offset;
}

size_t Array::GetByteSize(bool align) const
{
    size_t len = CalcByteLen(m_len, m_width);
    if (align) {
        const size_t rest = (~len & 0x7)+1;
        if (rest < 8) len += rest; // 64bit blocks
    }
    return len;
}

size_t Array::CalcByteLen(size_t count, size_t width) const
{
    // FIXME: This arithemtic could overflow. Consider using <tightdb/safe_int_ops.hpp>
    const size_t bits = count * width;
    const size_t bytes = (bits+7) / 8; // round up
    return bytes + 8; // add room for 8 byte header
}

size_t Array::CalcItemCount(size_t bytes, size_t width) const TIGHTDB_NOEXCEPT
{
    if (width == 0)
        return numeric_limits<size_t>::max(); // zero width gives infinite space

    const size_t bytes_data = bytes - 8; // ignore 8 byte header
    const size_t total_bits = bytes_data * 8;
    return total_bits / width;
}

void Array::Copy(const Array& a)
{
    // Calculate size in bytes (plus a bit of matchcount room for expansion)
    size_t len = CalcByteLen(a.m_len, a.m_width);
    const size_t rest = (~len & 0x7)+1;
    if (rest < 8) len += rest; // 64bit blocks
    const size_t new_len = len + 64;

    // Create new copy of array
    MemRef mref = m_alloc.Alloc(new_len); // Throws
    const char* src_begin = get_header_from_data(a.m_data);
    const char* src_end   = a.m_data + len;
    char*       dst_begin = static_cast<char*>(mref.pointer);
    copy(src_begin, src_end, dst_begin);

    // Clear old contents
    Destroy();

    // Update internal data
    UpdateRef(mref.ref);
    set_header_capacity(new_len); // uses m_data to find header, so m_data must be initialized correctly first

    // Copy sub-arrays as well
    if (m_hasRefs) {
        for (size_t i = 0; i < m_len; ++i) {
            const size_t ref = GetAsRef(i);

            // null-refs signify empty sub-trees
            if (ref == 0) continue;

            // all refs are 64bit aligned, so the lowest bits
            // cannot be set. If they are it means that it should
            // not be interpreted as a ref
            if (ref & 0x1) continue;

            const Array sub(ref, NULL, 0, a.m_alloc);
            Array cp(m_alloc);
            cp.SetParent(this, i);
            cp.Copy(sub);
        }
    }
}

void Array::CopyOnWrite()
{
    if (!m_alloc.IsReadOnly(m_ref)) return;

    // Calculate size in bytes (plus a bit of matchcount room for expansion)
    size_t len = CalcByteLen(m_len, m_width);
    const size_t rest = (~len & 0x7)+1;
    if (rest < 8) len += rest; // 64bit blocks
    const size_t new_len = len + 64;

    // Create new copy of array
    MemRef mref = m_alloc.Alloc(new_len); // Throws
    const char* old_begin = get_header_from_data(m_data);
    const char* old_end   = m_data + len;
    char* new_begin = static_cast<char*>(mref.pointer);
    copy(old_begin, old_end, new_begin);

    const size_t old_ref = m_ref;

    // Update internal data
    m_ref = mref.ref;
    m_data = get_data_from_header(new_begin);
    m_capacity = CalcItemCount(new_len, m_width);

    // Update capacity in header
    set_header_capacity(new_len); // uses m_data to find header, so m_data must be initialized correctly first

    update_ref_in_parent();

    // Mark original as deleted, so that the space can be reclaimed in
    // future commits, when no versions are using it anymore
    m_alloc.Free(old_ref, old_begin);
}


size_t Array::create_empty_array(ColumnDef type, WidthType width_type, Allocator& alloc)
{
    bool is_node = false, has_refs = false;
    switch (type) {
        case coldef_Normal:                               break;
        case coldef_InnerNode: has_refs = is_node = true; break;
        case coldef_HasRefs:   has_refs = true;           break;
    }

    const size_t capacity = initial_capacity;
    const MemRef mem_ref = alloc.Alloc(capacity); // Throws

    init_header(static_cast<char*>(mem_ref.pointer), is_node, has_refs, width_type, 0, 0, capacity);

    return mem_ref.ref;
}


void Array::Alloc(size_t count, size_t width)
{
    if (count > m_capacity || width != m_width) {
        const size_t needed_bytes = CalcByteLen(count, width);
        size_t capacity_bytes     = m_capacity ? get_capacity_from_header() : 0; // space currently available in bytes

        if (needed_bytes > capacity_bytes) {
            // Double to avoid too many reallocs (or initialize to initial size)
            capacity_bytes = capacity_bytes ? capacity_bytes * 2 : initial_capacity;

            // If doubling is not enough, expand enough to fit
            if (capacity_bytes < needed_bytes) {
                const size_t rest = (~needed_bytes & 0x7)+1;
                capacity_bytes = needed_bytes;
                if (rest < 8) capacity_bytes += rest; // 64bit align
            }

            // Allocate and initialize header
            MemRef mem_ref;
            char* header;
            if (!m_data) {
                mem_ref = m_alloc.Alloc(capacity_bytes); // Throws
                header = static_cast<char*>(mem_ref.pointer);
                init_header(header, m_isNode, m_hasRefs, GetWidthType(), width, count,
                            capacity_bytes);
            }
            else {
                header = get_header_from_data(m_data);
                mem_ref = m_alloc.ReAlloc(m_ref, header, capacity_bytes); // Throws
                header = static_cast<char*>(mem_ref.pointer);
                set_header_width(width, header);
                set_header_len(count, header);
                set_header_capacity(capacity_bytes, header);
            }

            // Update wrapper objects
            m_ref      = mem_ref.ref;
            m_data     = get_data_from_header(header);
            m_capacity = CalcItemCount(capacity_bytes, width);
            update_ref_in_parent(); // FIXME: Trouble when this one throws. We will then leave this array instance in a corrupt state
            return;
        }

        m_capacity = CalcItemCount(capacity_bytes, width);
        set_header_width(width);
    }

    // Update header
    set_header_len(count);
}


void Array::SetWidth(size_t width) TIGHTDB_NOEXCEPT
{
    TIGHTDB_TEMPEX(SetWidth, width, ());
}

template<size_t width> void Array::SetWidth() TIGHTDB_NOEXCEPT
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
    // m_getter = temp is a workaround for a bug in VC2010 that makes it return address of Get() instead of Get<n>
    // if the declaration and association of the getter are on two different source lines
    Getter temp_getter = &Array::Get<width>;
    m_getter = temp_getter;

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
    return GetUniversal<w>((const char *)m_data, ndx);
}

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4127)
#endif
template <size_t w> void Array::Set(size_t ndx, int64_t value)
{
    if (w == 0) {
        return;
    }
    else if (w == 1) {
        const size_t offset = ndx >> 3;
        ndx &= 7;
        uint8_t* p = reinterpret_cast<uint8_t*>(m_data) + offset;
        *p = (*p &~ (1 << ndx)) | uint8_t((value & 1) << ndx);
    }
    else if (w == 2) {
        const size_t offset = ndx >> 2;
        const uint8_t n = uint8_t((ndx & 3) << 1);
        uint8_t* p = reinterpret_cast<uint8_t*>(m_data) + offset;
        *p = (*p &~ (0x03 << n)) | uint8_t((value & 0x03) << n);
    }
    else if (w == 4) {
        const size_t offset = ndx >> 1;
        const uint8_t n = uint8_t((ndx & 1) << 2);
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
template <size_t w>bool Array::MinMax(size_t from, size_t to, uint64_t maxdiff, int64_t *min, int64_t *max)
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
            if ((uint64_t)(max2 - min2) > maxdiff)
                break;
        }
        else if (v > max2) {
            max2 = v;
            if ((uint64_t)(max2 - min2) > maxdiff)
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

template <size_t w>void Array::ReferenceSort(Array& ref)
{
    if (m_len < 2)
        return;

    int64_t min;
    int64_t max;

    // in avg case QuickSort is O(n*log(n)) and CountSort O(n + range), and memory usage is sizeof(size_t)*range for CountSort.
    // So we chose range < m_len as treshold for deciding which to use

    // If range isn't suited for CountSort, it's *probably* discovered very early, within first few values, in most practical cases,
    // and won't add much wasted work. Max wasted work is O(n) which isn't much compared to QuickSort.

//  bool b = MinMax<w>(0, m_len, m_len, &min, &max); // auto detect
//  bool b = MinMax<w>(0, m_len, -1, &min, &max); // force count sort
    bool b = MinMax<w>(0, m_len, 0, &min, &max); // force quicksort

    if (b) {
        Array res;
        Array count;

        // Todo, Preset crashes for unknown reasons but would be faster.
//      res.Preset(0, m_len, m_len);
//      count.Preset(0, m_len, max - min + 1);

        for (int64_t t = 0; t < max - min + 1; t++)
            count.add(0);

        // Count occurences of each value
        for (size_t t = 0; t < m_len; t++) {
            size_t i = to_ref(Get<w>(t) - min);
            count.Set(i, count.Get(i) + 1);
        }

        // Accumulate occurences
        for (size_t t = 1; t < count.size(); t++) {
            count.Set(t, count.Get(t) + count.Get(t - 1));
        }

        for (size_t t = 0; t < m_len; t++)
            res.add(0);

        for (size_t t = m_len; t > 0; t--) {
            size_t v = to_ref(Get<w>(t - 1) - min);
            size_t i = count.GetAsRef(v);
            count.Set(v, count.Get(v) - 1);
            res.Set(i - 1, ref.Get(t - 1));
        }

        // Copy result into ref
        for (size_t t = 0; t < res.size(); t++)
            ref.Set(t, res.Get(t));

        res.Destroy();
        count.Destroy();
    }
    else {
        ReferenceQuickSort(ref);
    }
}

// Sort array
template <size_t w> void Array::sort()
{
    if (m_len < 2)
        return;

    size_t lo = 0;
    size_t hi = m_len - 1;
    std::vector<size_t> count;
    int64_t min;
    int64_t max;
    bool b = false;

    // in avg case QuickSort is O(n*log(n)) and CountSort O(n + range), and memory usage is sizeof(size_t)*range for CountSort.
    // Se we chose range < m_len as treshold for deciding which to use
    if (m_width <= 8) {
        max = m_ubound;
        min = m_lbound;
        b = true;
    }
    else {
        // If range isn't suited for CountSort, it's *probably* discovered very early, within first few values,
        // in most practical cases, and won't add much wasted work. Max wasted work is O(n) which isn't much
        // compared to QuickSort.
        b = MinMax<w>(lo, hi + 1, m_len, &min, &max);
    }

    if (b) {
        for (int64_t t = 0; t < max - min + 1; t++)
            count.push_back(0);

        // Count occurences of each value
        for (size_t t = lo; t <= hi; t++) {
            size_t i = to_ref(Get<w>(t) - min);
            count[i]++;
        }

        // Overwrite original array with sorted values
        size_t dst = 0;
        for (int64_t i = 0; i < max - min + 1; i++) {
            size_t c = count[(unsigned int)i];
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
    TIGHTDB_TEMPEX(ReferenceQuickSort, m_width, (0, m_len - 1, ref));
}

template<size_t w> void Array::ReferenceQuickSort(size_t lo, size_t hi, Array& ref)
{
    // Quicksort based on
    // http://www.inf.fh-flensburg.de/lang/algorithmen/sortieren/quick/quicken.htm
    int i = (int)lo;
    int j = (int)hi;

    /*
    // Swap both values and references but lookup values directly: 2.85 sec
    // comparison element x
    const size_t ndx = (lo + hi)/2;
    const int64_t x = (size_t)Get(ndx);

    // partition
    do {
        while (Get(i) < x) i++;
        while (Get(j) > x) j--;
        if (i <= j) {
            size_t h = ref.Get(i);
            ref.Set(i, ref.Get(j));
            ref.Set(j, h);
        //  h = Get(i);
        //  Set(i, Get(j));
        //  Set(j, h);
            i++; j--;
        }
    } while (i <= j);
*/

    // Lookup values indirectly through references, but swap only references: 2.60 sec
    // Templated get/set: 2.40 sec (todo, enable again)
    // comparison element x
    const size_t ndx = (lo + hi)/2;
    const size_t target_ndx = (size_t)ref.Get(ndx);
    const int64_t x = Get(target_ndx);

    // partition
    do {
        while (Get((size_t)ref.Get(i)) < x) ++i;
        while (Get((size_t)ref.Get(j)) > x) --j;
        if (i <= j) {
            const size_t h = (size_t)ref.Get(i);
            ref.Set(i, ref.Get(j));
            ref.Set(j, h);
            ++i; --j;
        }
    } while (i <= j);

    //  recursion
    if ((int)lo < j) ReferenceQuickSort<w>(lo, j, ref);
    if (i < (int)hi) ReferenceQuickSort<w>(i, hi, ref);
}


void Array::QuickSort(size_t lo, size_t hi)
{
    TIGHTDB_TEMPEX(QuickSort, m_width, (lo, hi);)
}

template<size_t w> void Array::QuickSort(size_t lo, size_t hi)
{
    // Quicksort based on
    // http://www.inf.fh-flensburg.de/lang/algorithmen/sortieren/quick/quicken.htm
    int i = (int)lo;
    int j = (int)hi;

    // comparison element x
    const size_t ndx = (lo + hi)/2;
    const int64_t x = Get(ndx);

    // partition
    do {
        while (Get(i) < x) ++i;
        while (Get(j) > x) --j;
        if (i <= j) {
            const int64_t h = Get(i);
            Set(i, Get(j));
            Set(j, h);
            ++i; --j;
        }
    } while (i <= j);

    //  recursion
    if ((int)lo < j) QuickSort(lo, j);
    if (i < (int)hi) QuickSort(i, hi);
}

std::vector<int64_t> Array::ToVector() const
{
    std::vector<int64_t> v;
    const size_t count = size();
    for (size_t t = 0; t < count; ++t)
        v.push_back(Get(t));
    return v;
}

bool Array::Compare(const Array& c) const
{
    if (c.size() != size()) return false;

    for (size_t i = 0; i < size(); ++i) {
        if (Get(i) != c.Get(i)) return false;
    }

    return true;
}


#ifdef TIGHTDB_DEBUG

void Array::Print() const
{
    std::cout << std::hex << GetRef() << std::dec << ": (" << size() << ") ";
    for (size_t i = 0; i < size(); ++i) {
        if (i) std::cout << ", ";
        std::cout << Get(i);
    }
    std::cout << "\n";
}

void Array::Verify() const
{
    TIGHTDB_ASSERT(!IsValid() || (m_width == 0 || m_width == 1 || m_width == 2 || m_width == 4 ||
                                  m_width == 8 || m_width == 16 || m_width == 32 || m_width == 64));

    // Check that parent is set correctly
    if (!m_parent) return;

    const size_t ref_in_parent = m_parent->get_child_ref(m_parentNdx);
    TIGHTDB_ASSERT(ref_in_parent == (IsValid() ? m_ref : 0));
}

void Array::ToDot(std::ostream& out, const char* title) const
{
    const size_t ref = GetRef();

    if (title) {
        out << "subgraph cluster_" << ref << " {" << std::endl;
        out << " label = \"" << title << "\";" << std::endl;
        out << " color = white;" << std::endl;
    }

    out << "n" << std::hex << ref << std::dec << "[shape=none,label=<";
    out << "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\"><TR>" << std::endl;

    // Header
    out << "<TD BGCOLOR=\"lightgrey\"><FONT POINT-SIZE=\"7\"> ";
    out << "0x" << std::hex << ref << std::dec << "<BR/>";
    if (m_isNode) out << "IsNode<BR/>";
    if (m_hasRefs) out << "HasRefs<BR/>";
    out << "</FONT></TD>" << std::endl;

    // Values
    for (size_t i = 0; i < m_len; ++i) {
        const int64_t v =  Get(i);
        if (m_hasRefs) {
            // zero-refs and refs that are not 64-aligned do not point to sub-trees
            if (v == 0) out << "<TD>none";
            else if (v & 0x1) out << "<TD BGCOLOR=\"grey90\">" << (v >> 1);
            else out << "<TD PORT=\"" << i << "\">";
        }
        else out << "<TD>" << v;
        out << "</TD>" << std::endl;
    }

    out << "</TR></TABLE>>];" << std::endl;
    if (title) out << "}" << std::endl;

    if (m_hasRefs) {
        for (size_t i = 0; i < m_len; ++i) {
            const int64_t target = Get(i);
            if (target == 0 || target & 0x1) continue; // zero-refs and refs that are not 64-aligned do not point to sub-trees

            out << "n" << std::hex << ref << std::dec << ":" << i;
            out << " -> n" << std::hex << target << std::dec << std::endl;
        }
    }

    out << std::endl;
}

void Array::Stats(MemStats& stats) const
{
    const size_t capacity_bytes = get_capacity_from_header();
    const size_t bytes_used     = CalcByteLen(m_len, m_width);

    const MemStats m(capacity_bytes, bytes_used, 1);
    stats.add(m);

    // Add stats for all sub-arrays
    if (m_hasRefs) {
        for (size_t i = 0; i < m_len; ++i) {
            const size_t ref = GetAsRef(i);
            if (ref == 0 || ref & 0x1) continue; // zero-refs and refs that are not 64-aligned do not point to sub-trees

            const Array sub(ref, NULL, 0, GetAllocator());
            sub.Stats(stats);
        }
    }
}

#endif // TIGHTDB_DEBUG

} // namespace tightdb


namespace {

// Direct access methods

template<int w> int64_t get_direct(const char* data, size_t ndx) TIGHTDB_NOEXCEPT
{
    if (w == 0) {
        return 0;
    }
    if (w == 1) {
        const size_t offset = ndx >> 3;
        return (data[offset] >> (ndx & 7)) & 0x01;
    }
    if (w == 2) {
        const size_t offset = ndx >> 2;
        return (data[offset] >> ((ndx & 3) << 1)) & 0x03;
    }
    if (w == 4) {
        const size_t offset = ndx >> 1;
        return (data[offset] >> ((ndx & 1) << 2)) & 0x0F;
    }
    if (w == 8) {
        return *((const signed char*)(data + ndx));
    }
    if (w == 16) {
        const size_t offset = ndx * 2;
        return *(const int16_t*)(data + offset);
    }
    if (w == 32) {
        const size_t offset = ndx * 4;
        return *(const int32_t*)(data + offset);
    }
    if (w == 64) {
        const size_t offset = ndx * 8;
        return *(const int64_t*)(data + offset);
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
inline size_t lower_bound(const char* header, int64_t value) TIGHTDB_NOEXCEPT
{
    const char* data = tightdb::Array::get_data_from_header(header);

    size_t i = 0;
    size_t size = tightdb::Array::get_len_from_header(header);

    while (0 < size) {
        size_t half = size / 2;
        size_t mid = i + half;
        int64_t probe = get_direct<width>(data, mid);
        if (probe < value) {
            i = mid + 1;
            size -= half + 1;
        }
        else size = half;
    }
    return i;
}

// See lower_bound()
template<int width>
inline size_t upper_bound(const char* header, int64_t value) TIGHTDB_NOEXCEPT
{
    const char* data = tightdb::Array::get_data_from_header(header);

    size_t i = 0;
    size_t size = tightdb::Array::get_len_from_header(header);

    while (0 < size) {
        size_t half = size / 2;
        size_t mid = i + half;
        int64_t probe = get_direct<width>(data, mid);
        if (probe <= value) {
            i = mid + 1;
            size -= half + 1;
        }
        else size = half;
    }
    return i;
}


// Find the child that contains the specified local tree index.
// Returns (child_ndx, local_tree_offset) where 'local_tree_offset' is
// the local tree index offset of the located child.
template<int width> inline pair<size_t, size_t>
find_child_offset(const char* header, size_t local_ndx) TIGHTDB_NOEXCEPT
{
    size_t child_ndx = upper_bound<width>(header, local_ndx);
    size_t local_tree_offset = child_ndx == 0 ? 0 :
        get_direct<width>(tightdb::Array::get_data_from_header(header), child_ndx-1);
    return make_pair(child_ndx, local_tree_offset);
}


template<int width>
inline pair<size_t, size_t> get_two_as_size(const char* header, size_t ndx) TIGHTDB_NOEXCEPT
{
    const char* data = tightdb::Array::get_data_from_header(header);
    return make_pair(tightdb::to_ref(get_direct<width>(data, ndx+0)),
                     tightdb::to_ref(get_direct<width>(data, ndx+1)));
}


size_t FindPos2Direct_32(const char* const header, const char* const data, int32_t target)
{
    const size_t len = tightdb::Array::get_len_from_header(header);

    int low = -1;
    int high = int(len); // FIXME: Conversion to 'int' is prone to overflow

    // Binary search based on:
    // http://www.tbray.org/ongoing/When/200x/2003/03/22/Binary
    // Finds position of closest value BIGGER OR EQUAL to the target (for
    // lookups in indexes)
    while (high - low > 1) {
        const size_t probe = (unsigned(low) + unsigned(high)) >> 1;
        const int64_t v = get_direct<32>(data, probe);

        if (v < target) low = int(probe);
        else            high = int(probe);
    }
    if (high == int(len)) return size_t(-1);
    else return size_t(high);
}

}

namespace tightdb {

void Array::find_all(Array& result, int64_t value, size_t colOffset, size_t start, size_t end) const
{
    if (end == size_t(-1)) end = m_len;
    TIGHTDB_ASSERT(start < m_len && end <= m_len && start < end);

    QueryState<int64_t> state;
    state.m_state = reinterpret_cast<int64_t>(&result);

    TIGHTDB_TEMPEX3(find, Equal, act_FindAll, m_width, (value, start, end, colOffset, &state, CallbackDummy()));

    return;
}

void Array::find(int cond, Action action, int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t> *state) const
{
    if (cond == cond_Equal) {
        if (action == act_Sum) {
            TIGHTDB_TEMPEX3(find, Equal, act_Sum, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Min) {
            TIGHTDB_TEMPEX3(find, Equal, act_Min, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Max) {
            TIGHTDB_TEMPEX3(find, Equal, act_Max, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Count) {
            TIGHTDB_TEMPEX3(find, Equal, act_Count, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_FindAll) {
            TIGHTDB_TEMPEX3(find, Equal, act_FindAll, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_CallbackIdx) {
            TIGHTDB_TEMPEX3(find, Equal, act_CallbackIdx, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
    }
    if (cond == cond_NotEqual) {
        if (action == act_Sum) {
            TIGHTDB_TEMPEX3(find, NotEqual, act_Sum, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Min) {
            TIGHTDB_TEMPEX3(find, NotEqual, act_Min, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Max) {
            TIGHTDB_TEMPEX3(find, NotEqual, act_Max, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Count) {
            TIGHTDB_TEMPEX3(find, NotEqual, act_Count, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_FindAll) {
            TIGHTDB_TEMPEX3(find, NotEqual, act_FindAll, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_CallbackIdx) {
            TIGHTDB_TEMPEX3(find, NotEqual, act_CallbackIdx, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
    }
    if (cond == cond_Greater) {
        if (action == act_Sum) {
            TIGHTDB_TEMPEX3(find, Greater, act_Sum, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Min) {
            TIGHTDB_TEMPEX3(find, Greater, act_Min, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Max) {
            TIGHTDB_TEMPEX3(find, Greater, act_Max, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Count) {
            TIGHTDB_TEMPEX3(find, Greater, act_Count, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_FindAll) {
            TIGHTDB_TEMPEX3(find, Greater, act_FindAll, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_CallbackIdx) {
            TIGHTDB_TEMPEX3(find, Greater, act_CallbackIdx, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
    }
    if (cond == cond_Less) {
        if (action == act_Sum) {
            TIGHTDB_TEMPEX3(find, Less, act_Sum, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Min) {
            TIGHTDB_TEMPEX3(find, Less, act_Min, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Max) {
            TIGHTDB_TEMPEX3(find, Less, act_Max, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Count) {
            TIGHTDB_TEMPEX3(find, Less, act_Count, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_FindAll) {
            TIGHTDB_TEMPEX3(find, Less, act_FindAll, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_CallbackIdx) {
            TIGHTDB_TEMPEX3(find, Less, act_CallbackIdx, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
    }
    if (cond == cond_None) {
        if (action == act_Sum) {
            TIGHTDB_TEMPEX3(find, None, act_Sum, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Min) {
            TIGHTDB_TEMPEX3(find, None, act_Min, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Max) {
            TIGHTDB_TEMPEX3(find, None, act_Max, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_Count) {
            TIGHTDB_TEMPEX3(find, None, act_Count, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_FindAll) {
            TIGHTDB_TEMPEX3(find, None, act_FindAll, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
        else if (action == act_CallbackIdx) {
            TIGHTDB_TEMPEX3(find, None, act_CallbackIdx, m_width, (value, start, end, baseindex, state, CallbackDummy()))
        }
    }
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

    pair<const char*, size_t> p = find_leaf(this, ndx);
    arr.CreateFromHeaderDirect(const_cast<char*>(p.first));
    off = ndx - p.second;
    return &arr;
}


// Get value direct through column b-tree without instatiating any Arrays.
int64_t Array::column_get(size_t ndx) const TIGHTDB_NOEXCEPT
{
    if (is_leaf()) return Get(ndx);
    pair<const char*, size_t> p = find_leaf(this, ndx);
    const char* data = get_data_from_header(p.first);
    int width = get_width_from_header(p.first);
    return get_direct(data, width, p.second);
}

const char* Array::string_column_get(size_t ndx) const TIGHTDB_NOEXCEPT
{
    if (is_leaf()) {
        if (HasRefs())
            return static_cast<const ArrayStringLong*>(this)->Get(ndx);
        return static_cast<const ArrayString*>(this)->Get(ndx);
    }

    pair<const char*, size_t> p = find_leaf(this, ndx);
    const char* header = p.first;
    ndx = p.second;

    int width = get_width_from_header(header);
    if (!get_hasrefs_from_header(header)) {
        // short strings
        if (width == 0) return "";
        return get_data_from_header(header) + (ndx * width);
    }

    // long strings
    pair<size_t, size_t> p2;
    TIGHTDB_TEMPEX(p2 = ::get_two_as_size, width, (header, 0));
    const size_t offsets_ref = p2.first;
    const size_t blob_ref    = p2.second;

    size_t offset = 0;
    if (0 < ndx) {
        header = static_cast<char*>(m_alloc.Translate(offsets_ref));
        width  = get_width_from_header(header);
        offset = to_size_t(get_direct(get_data_from_header(header), width, ndx-1));
    }

    header = static_cast<char*>(m_alloc.Translate(blob_ref));
    return get_data_from_header(header) + offset;
}

// Find value direct through column b-tree without instatiating any Arrays.
size_t Array::ColumnFind(int64_t target, size_t ref, Array& cache) const
{
    char* header = static_cast<char*>(m_alloc.Translate(ref));
    bool isNode = get_isnode_from_header(header);

    if (isNode) {
        const char* data = get_data_from_header(header);
        size_t width = get_width_from_header(header);

        // Get subnode table
        size_t ref_offsets = to_size_t(get_direct(data, width, 0));
        size_t ref_refs    = to_size_t(get_direct(data, width, 1));

        const char* offsets_header = static_cast<char*>(m_alloc.Translate(ref_offsets));
        const char* offsets_data = get_data_from_header(offsets_header);
        size_t offsets_width  = get_width_from_header(offsets_header);
        size_t offsets_len = get_len_from_header(offsets_header);

        const char* refs_header = static_cast<char*>(m_alloc.Translate(ref_refs));
        const char* refs_data = get_data_from_header(refs_header);
        size_t refs_width  = get_width_from_header(refs_header);

        // Iterate over nodes until we find a match
        size_t offset = 0;
        for (size_t i = 0; i < offsets_len; ++i) {
            size_t ref = to_ref(get_direct(refs_data, refs_width, i));
            size_t result = ColumnFind(target, ref, cache);
            if (result != not_found)
                return offset + result;

            size_t off = to_size_t(get_direct(offsets_data, offsets_width, i));
            offset = off;
        }

        // if we get to here there is no match
        return not_found;
    }
    else {
        cache.CreateFromHeaderDirect(header);
        return cache.find_first(target, 0, -1);
    }
}

size_t Array::IndexStringFindFirst(const char* value, void* column, StringGetter get_func) const
{
    const char* v = value;
    const char* data   = m_data;
    const char* header;
    size_t width = m_width;
    bool isNode = m_isNode;

top:
    // Create 4 byte index key
    int32_t key = 0;
    if (*v) key  = (int32_t(*v++) << 24);
    if (*v) key |= (int32_t(*v++) << 16);
    if (*v) key |= (int32_t(*v++) << 8);
    if (*v) key |=  int32_t(*v++);

    for (;;) {
        // Get subnode table
        const size_t ref_offsets = to_size_t(get_direct(data, width, 0)); // todo, test if you can use to_ref instead
        const size_t ref_refs    = to_ref(get_direct(data, width, 1));

        // Find the position matching the key
        const char* offsets_header = static_cast<char*>(m_alloc.Translate(ref_offsets));
        const char* offsets_data = get_data_from_header(offsets_header);
        const size_t pos = FindPos2Direct_32(offsets_header, offsets_data, key); // keys are always 32 bits wide

        // If key is outside range, we know there can be no match
        if (pos == not_found) return not_found;

        // Get entry under key
        const char* refs_header = static_cast<char*>(m_alloc.Translate(ref_refs));
        const char* refs_data = get_data_from_header(refs_header);
        const size_t refs_width  = get_width_from_header(refs_header);
        const size_t ref = to_ref(get_direct(refs_data, refs_width, pos));

        if (isNode) {
            // Set vars for next iteration
            header = static_cast<char*>(m_alloc.Translate(ref));
            data   = get_data_from_header(header);
            width  = get_width_from_header(header);
            isNode = get_isnode_from_header(header);
            continue;
        }

        const int32_t stored_key = int32_t(get_direct<32>(offsets_data, pos));

        if (stored_key == key) {
            // Literal row index
            if (ref & 1) {
                const size_t row_ref = (ref >> 1);

                // If the last byte in the stored key is zero, we know that we have
                // compared against the entire (target) string
                if (!(stored_key << 24)) return row_ref;

                const char* str = (*get_func)(column, row_ref);
                if (strcmp(str, value) == 0) return row_ref;
                else return not_found;
            }

            const char* sub_header = static_cast<char*>(m_alloc.Translate(ref));
            const bool sub_isindex = get_indexflag_from_header(sub_header);

            // List of matching row indexes
            if (!sub_isindex) {
                const char* sub_data = get_data_from_header(sub_header);
                const size_t sub_width  = get_width_from_header(sub_header);
                const bool sub_isnode = get_isnode_from_header(sub_header);

                // In most cases the row list will just be an array but there
                // might be so many matches that it has branched into a column
                size_t row_ref;
                if (!sub_isnode)
                    row_ref = get_direct(sub_data, sub_width, 0);
                else {
                    const Array sub(ref, NULL, 0, m_alloc);
                    row_ref = sub.column_get(0);
                }

                // If the last byte in the stored key is zero, we know that we have
                // compared against the entire (target) string
                if (!(stored_key << 24)) return row_ref;

                const char* str = (*get_func)(column, row_ref);
                if (strcmp(str, value) == 0) return row_ref;
                else return not_found;
            }

            // Recurse into sub-index;
            header = static_cast<char*>(m_alloc.Translate(ref));
            data   = get_data_from_header(header);
            width  = get_width_from_header(header);
            isNode = get_isnode_from_header(header);
            goto top;
        }
        else return not_found;
    }
}

void Array::IndexStringFindAll(Array& result, const char* value, void* column, StringGetter get_func) const
{
    const char* v = value;
    const char* data = m_data;
    const char* header;
    size_t width = m_width;
    bool isNode = m_isNode;

top:
    // Create 4 byte index key
    int32_t key = 0;
    if (*v) key  = (int32_t(*v++) << 24);
    if (*v) key |= (int32_t(*v++) << 16);
    if (*v) key |= (int32_t(*v++) << 8);
    if (*v) key |=  int32_t(*v++);

    for (;;) {
        // Get subnode table
        const size_t ref_offsets = to_size_t(get_direct(data, width, 0));
        const size_t ref_refs    = to_ref(get_direct(data, width, 1));

        // Find the position matching the key
        const char* offsets_header = static_cast<char*>(m_alloc.Translate(ref_offsets));
        const char* offsets_data = get_data_from_header(offsets_header);
        const size_t pos = FindPos2Direct_32(offsets_header, offsets_data, key); // keys are always 32 bits wide

        // If key is outside range, we know there can be no match
        if (pos == not_found) return; // not_found

        // Get entry under key
        const char* refs_header = static_cast<char*>(m_alloc.Translate(ref_refs));
        const char* refs_data = get_data_from_header(refs_header);
        const size_t refs_width  = get_width_from_header(refs_header);
        const size_t ref = to_ref(get_direct(refs_data, refs_width, pos));

        if (isNode) {
            // Set vars for next iteration
            header = static_cast<char*>(m_alloc.Translate(ref));
            data   = get_data_from_header(header);
            width  = get_width_from_header(header);
            isNode = get_isnode_from_header(header);
            continue;
        }

        const int32_t stored_key = int32_t(get_direct<32>(offsets_data, pos));

        if (stored_key == key) {
            // Literal row index
            if (ref & 1) {
                const size_t row_ref = (ref >> 1);

                // If the last byte in the stored key is zero, we know that we have
                // compared against the entire (target) string
                if (!(stored_key << 24)) {
                    result.add(row_ref);
                    return;
                }

                const char* const str = (*get_func)(column, row_ref);
                if (strcmp(str, value) == 0) {
                    result.add(row_ref);
                    return;
                }
                else return; // not_found
            }

            const char* sub_header = static_cast<char*>(m_alloc.Translate(ref));
            const bool sub_isindex = get_indexflag_from_header(sub_header);

            // List of matching row indexes
            if (!sub_isindex) {
                const bool sub_isnode = get_isnode_from_header(sub_header);

                // In most cases the row list will just be an array but there
                // might be so many matches that it has branched into a column
                if (!sub_isnode) {
                    const size_t sub_width = get_width_from_header(sub_header);
                    const char* sub_data = get_data_from_header(sub_header);
                    const size_t first_row_ref = get_direct(sub_data, sub_width, 0);

                    // If the last byte in the stored key is not zero, we have
                    // not yet compared against the entire (target) string
                    if ((stored_key << 24)) {
                        const char* const str = (*get_func)(column, first_row_ref);
                        if (strcmp(str, value) != 0)
                            return; // not_found
                    }

                    // Copy all matches into result array
                    const size_t sub_len  = get_len_from_header(sub_header);

                    for (size_t i = 0; i < sub_len; ++i) {
                        const size_t row_ref = get_direct(sub_data, sub_width, i);
                        result.add(row_ref);
                    }
                }
                else {
                    const Column sub(ref, NULL, 0, m_alloc);
                    const size_t first_row_ref = sub.Get(0);

                    // If the last byte in the stored key is not zero, we have
                    // not yet compared against the entire (target) string
                    if ((stored_key << 24)) {
                        const char* const str = (*get_func)(column, first_row_ref);
                        if (strcmp(str, value) != 0)
                            return; // not_found
                    }

                    // Copy all matches into result array
                    const size_t sub_len  = sub.Size();

                    for (size_t i = 0; i < sub_len; ++i) {
                        const size_t row_ref = sub.Get(i);
                        result.add(row_ref);
                    }
                }
                return;
            }

            // Recurse into sub-index;
            header = static_cast<char*>(m_alloc.Translate(ref));
            data   = get_data_from_header(header);
            width  = get_width_from_header(header);
            isNode = get_isnode_from_header(header);
            goto top;
        }
        else return; // not_found
    }
}

size_t Array::IndexStringCount(const char* value, void* column, StringGetter get_func) const
{
    const char* v = value;
    const char* data   = m_data;
    const char* header;
    size_t width = m_width;
    bool isNode = m_isNode;

top:
    // Create 4 byte index key
    int32_t key = 0;
    if (*v) key  = (int32_t(*v++) << 24);
    if (*v) key |= (int32_t(*v++) << 16);
    if (*v) key |= (int32_t(*v++) << 8);
    if (*v) key |=  int32_t(*v++);

    for (;;) {
        // Get subnode table
        const size_t ref_offsets = to_size_t(get_direct(data, width, 0));
        const size_t ref_refs    = to_ref(get_direct(data, width, 1));

        // Find the position matching the key
        const char* offsets_header = static_cast<char*>(m_alloc.Translate(ref_offsets));
        const char* offsets_data = get_data_from_header(offsets_header);
        const size_t pos = FindPos2Direct_32(offsets_header, offsets_data, key); // keys are always 32 bits wide

        // If key is outside range, we know there can be no match
        if (pos == not_found) return 0;

        // Get entry under key
        const char* refs_header = static_cast<char*>(m_alloc.Translate(ref_refs));
        const char* refs_data = get_data_from_header(refs_header);
        const size_t refs_width  = get_width_from_header(refs_header);
        const size_t ref = to_ref(get_direct(refs_data, refs_width, pos));

        if (isNode) {
            // Set vars for next iteration
            header = static_cast<char*>(m_alloc.Translate(ref));
            data   = get_data_from_header(header);
            width  = get_width_from_header(header);
            isNode = get_isnode_from_header(header);
            continue;
        }

        const int32_t stored_key = int32_t(get_direct<32>(offsets_data, pos));

        if (stored_key == key) {
            // Literal row index
            if (ref & 1) {
                const size_t row_ref = (ref >> 1);

                // If the last byte in the stored key is zero, we know that we have
                // compared against the entire (target) string
                if (!(stored_key << 24)) return 1;

                const char* const str = (*get_func)(column, row_ref);
                if (strcmp(str, value) == 0) return 1;
                else return 0;
            }

            const char* sub_header = static_cast<char*>(m_alloc.Translate(ref));
            const bool sub_isindex = get_indexflag_from_header(sub_header);

            // List of matching row indexes
            if (!sub_isindex) {
                const bool sub_isnode = get_isnode_from_header(sub_header);
                size_t sub_count;
                size_t row_ref;

                // In most cases the row list will just be an array but there
                // might be so many matches that it has branched into a column
                if (!sub_isnode) {
                    sub_count  = get_len_from_header(sub_header);

                    // If the last byte in the stored key is zero, we know that we have
                    // compared against the entire (target) string
                    if (!(stored_key << 24)) return sub_count;

                    const char* sub_data = get_data_from_header(sub_header);
                    const size_t sub_width  = get_width_from_header(sub_header);
                    row_ref = get_direct(sub_data, sub_width, 0);
                }
                else {
                    const Column sub(ref, NULL, 0, m_alloc);
                    sub_count = sub.Size();

                    // If the last byte in the stored key is zero, we know that we have
                    // compared against the entire (target) string
                    if (!(stored_key << 24)) return sub_count;

                    row_ref = sub.Get(0);
                }

                const char* const str = (*get_func)(column, row_ref);
                if (strcmp(str, value) == 0) return sub_count;
                else return 0;
            }

            // Recurse into sub-index;
            header = static_cast<char*>(m_alloc.Translate(ref));
            data   = get_data_from_header(header);
            width  = get_width_from_header(header);
            isNode = get_isnode_from_header(header);
            goto top;
        }
        else return 0;
    }
}


pair<const char*, size_t> Array::find_leaf(const Array* root, size_t i) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!root->is_leaf());
    size_t offsets_ref = root->GetAsRef(0);
    size_t refs_ref    = root->GetAsRef(1);
    for (;;) {
        const char* header = static_cast<char*>(root->m_alloc.Translate(offsets_ref));
        int width = get_width_from_header(header);
        pair<size_t, size_t> p;
        TIGHTDB_TEMPEX(p = find_child_offset, width, (header, i));
        size_t child_ndx         = p.first;
        size_t local_tree_offset = p.second;
        i -= local_tree_offset; // local index

        header = static_cast<char*>(root->m_alloc.Translate(refs_ref));
        width = get_width_from_header(header);
        size_t child_ref = to_size_t(get_direct(get_data_from_header(header), width, child_ndx));

        header = static_cast<char*>(root->m_alloc.Translate(child_ref));
        bool child_is_leaf = !get_isnode_from_header(header);
        if (child_is_leaf) return make_pair(header, i);

        width = get_width_from_header(header);
        TIGHTDB_TEMPEX(p = ::get_two_as_size, width, (header, 0));
        offsets_ref = p.first;
        refs_ref    = p.second;
    }
}


size_t Array::get_as_size(const char* header, size_t ndx) TIGHTDB_NOEXCEPT
{
    const char* data = get_data_from_header(header);
    int width        = get_width_from_header(header);
    int64_t v        = get_direct(data, width, ndx);
    return size_t(v);
}

pair<size_t, size_t> Array::get_two_as_size(const char* header, size_t ndx) TIGHTDB_NOEXCEPT
{
    pair<size_t, size_t> p;
    int width = get_width_from_header(header);
    TIGHTDB_TEMPEX(p = ::get_two_as_size, width, (header, ndx));
    return p;
}


} //namespace tightdb
