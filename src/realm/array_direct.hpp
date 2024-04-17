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

#ifndef REALM_ARRAY_DIRECT_HPP
#define REALM_ARRAY_DIRECT_HPP

#include <cstring>
#include <realm/utilities.hpp>
#include <realm/alloc.hpp>

// clang-format off
/* wid == 16/32 likely when accessing offsets in B tree */
#define REALM_TEMPEX(fun, wid, arg) \
    if (wid == 16) {fun<16> arg;} \
    else if (wid == 32) {fun<32> arg;} \
    else if (wid == 0) {fun<0> arg;} \
    else if (wid == 1) {fun<1> arg;} \
    else if (wid == 2) {fun<2> arg;} \
    else if (wid == 4) {fun<4> arg;} \
    else if (wid == 8) {fun<8> arg;} \
    else if (wid == 64) {fun<64> arg;} \
    else {REALM_ASSERT_DEBUG(false); fun<0> arg;}

#define REALM_TEMPEX2(fun, targ, wid, arg) \
    if (wid == 16) {fun<targ, 16> arg;} \
    else if (wid == 32) {fun<targ, 32> arg;} \
    else if (wid == 0) {fun<targ, 0> arg;} \
    else if (wid == 1) {fun<targ, 1> arg;} \
    else if (wid == 2) {fun<targ, 2> arg;} \
    else if (wid == 4) {fun<targ, 4> arg;} \
    else if (wid == 8) {fun<targ, 8> arg;} \
    else if (wid == 64) {fun<targ, 64> arg;} \
    else {REALM_ASSERT_DEBUG(false); fun<targ, 0> arg;}

#define REALM_TEMPEX3(fun, targ1, wid, targ3, arg) \
    if (wid == 16) {fun<targ1, 16, targ3> arg;} \
    else if (wid == 32) {fun<targ1, 32, targ3> arg;} \
    else if (wid == 0) {fun<targ1, 0, targ3> arg;} \
    else if (wid == 1) {fun<targ1, 1, targ3> arg;} \
    else if (wid == 2) {fun<targ1, 2, targ3> arg;} \
    else if (wid == 4) {fun<targ1, 4, targ3> arg;} \
    else if (wid == 8) {fun<targ1, 8, targ3> arg;} \
    else if (wid == 64) {fun<targ1, 64, targ3> arg;} \
    else {REALM_ASSERT_DEBUG(false); fun<targ1, 0, targ3> arg;}

#define REALM_TEMPEX4(fun, targ1, targ3, targ4, wid, arg) \
    if (wid == 16) {fun<targ1, targ3, targ4, 16> arg;} \
    else if (wid == 32) {fun<targ1, targ3, targ4, 32> arg;} \
    else if (wid == 0) {fun<targ1, targ3, targ4, 0> arg;} \
    else if (wid == 1) {fun<targ1, targ3, targ4, 1> arg;} \
    else if (wid == 2) {fun<targ1, targ3, targ4, 2> arg;} \
    else if (wid == 4) {fun<targ1, targ3, targ4, 4> arg;} \
    else if (wid == 8) {fun<targ1, targ3, targ4, 8> arg;} \
    else if (wid == 64) {fun<targ1, targ3, targ4, 64> arg;} \
    else {REALM_ASSERT_DEBUG(false); fun<targ1, targ3, targ4, 0> arg;}
// clang-format on

namespace realm {

// Direct access methods

template <size_t width>
void set_direct(char* data, size_t ndx, int_fast64_t value) noexcept
{
    if (width == 0) {
        REALM_ASSERT_DEBUG(value == 0);
        return;
    }
    else if (width == 1) {
        REALM_ASSERT_DEBUG(0 <= value && value <= 0x01);
        size_t byte_ndx = ndx / 8;
        size_t bit_ndx = ndx % 8;
        typedef unsigned char uchar;
        uchar* p = reinterpret_cast<uchar*>(data) + byte_ndx;
        *p = uchar((*p & ~(0x01 << bit_ndx)) | (int(value) & 0x01) << bit_ndx);
    }
    else if (width == 2) {
        REALM_ASSERT_DEBUG(0 <= value && value <= 0x03);
        size_t byte_ndx = ndx / 4;
        size_t bit_ndx = ndx % 4 * 2;
        typedef unsigned char uchar;
        uchar* p = reinterpret_cast<uchar*>(data) + byte_ndx;
        *p = uchar((*p & ~(0x03 << bit_ndx)) | (int(value) & 0x03) << bit_ndx);
    }
    else if (width == 4) {
        REALM_ASSERT_DEBUG(0 <= value && value <= 0x0F);
        size_t byte_ndx = ndx / 2;
        size_t bit_ndx = ndx % 2 * 4;
        typedef unsigned char uchar;
        uchar* p = reinterpret_cast<uchar*>(data) + byte_ndx;
        *p = uchar((*p & ~(0x0F << bit_ndx)) | (int(value) & 0x0F) << bit_ndx);
    }
    else if (width == 8) {
        REALM_ASSERT_DEBUG(std::numeric_limits<int8_t>::min() <= value &&
                           value <= std::numeric_limits<int8_t>::max());
        *(reinterpret_cast<int8_t*>(data) + ndx) = int8_t(value);
    }
    else if (width == 16) {
        REALM_ASSERT_DEBUG(std::numeric_limits<int16_t>::min() <= value &&
                           value <= std::numeric_limits<int16_t>::max());
        *(reinterpret_cast<int16_t*>(data) + ndx) = int16_t(value);
    }
    else if (width == 32) {
        REALM_ASSERT_DEBUG(std::numeric_limits<int32_t>::min() <= value &&
                           value <= std::numeric_limits<int32_t>::max());
        *(reinterpret_cast<int32_t*>(data) + ndx) = int32_t(value);
    }
    else if (width == 64) {
        REALM_ASSERT_DEBUG(std::numeric_limits<int64_t>::min() <= value &&
                           value <= std::numeric_limits<int64_t>::max());
        *(reinterpret_cast<int64_t*>(data) + ndx) = int64_t(value);
    }
    else {
        REALM_ASSERT_DEBUG(false);
    }
}

inline void set_direct(char* data, size_t width, size_t ndx, int_fast64_t value) noexcept
{
    REALM_TEMPEX(set_direct, width, (data, ndx, value));
}

template <size_t width>
void fill_direct(char* data, size_t begin, size_t end, int_fast64_t value) noexcept
{
    for (size_t i = begin; i != end; ++i)
        set_direct<width>(data, i, value);
}

template <int w>
int64_t get_direct(const char* data, size_t ndx) noexcept
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
        return *reinterpret_cast<const signed char*>(data + ndx);
    }
    if (w == 16) {
        size_t offset = ndx << 1;
        return *reinterpret_cast<const int16_t*>(data + offset);
    }
    if (w == 32) {
        size_t offset = ndx << 2;
        return *reinterpret_cast<const int32_t*>(data + offset);
    }
    if (w == 64) {
        size_t offset = ndx << 3;
        return *reinterpret_cast<const int64_t*>(data + offset);
    }
    REALM_ASSERT_DEBUG(false);
    return int64_t(-1);
}

inline int64_t get_direct(const char* data, size_t width, size_t ndx) noexcept
{
    REALM_TEMPEX(return get_direct, width, (data, ndx));
}

// An iterator for getting a 64 bit word from any (byte-address+bit-offset) address.
class unaligned_word_iter {
public:
    unaligned_word_iter(const uint64_t* data, size_t bit_offset)
        : m_word_ptr(data + (bit_offset >> 6))
        , m_in_word_offset(bit_offset & 0x3F)
    {
    }
    // 'num_bits' number of bits which must be read
    // WARNING returned word may be garbage above the first 'num_bits' bits.
    uint64_t get(size_t num_bits)
    {
        auto first_word = m_word_ptr[0];
        uint64_t result = first_word >> m_in_word_offset;
        // note: above shifts in zeroes
        if (m_in_word_offset + num_bits <= 64)
            return result;
        // if we're here, in_word_offset > 0
        auto first_word_size = 64 - m_in_word_offset;
        auto second_word = m_word_ptr[1];
        result |= second_word << first_word_size;
        // note: above shifts in zeroes below the bits we want
        return result;
    }
    uint64_t get_with_unsafe_prefetch(unsigned num_bits)
    {
        auto first_word = m_word_ptr[0];
        uint64_t result = first_word >> m_in_word_offset;
        // note: above shifts in zeroes
        auto first_word_size = 64 - m_in_word_offset;
        auto second_word = m_word_ptr[1];
        REALM_ASSERT_DEBUG(num_bits <= 64);
        result |= (m_in_word_offset + num_bits > 64) ? (second_word << first_word_size) : 0;
        // note: above shifts in zeroes below the bits we want
        return result;
    }
    // bump the iterator the specified number of bits
    void bump(size_t num_bits)
    {
        auto total_offset = m_in_word_offset + num_bits;
        m_word_ptr += total_offset >> 6;
        m_in_word_offset = total_offset & 0x3F;
    }

private:
    const uint64_t* m_word_ptr;
    unsigned m_in_word_offset;
};

// Read a bit field of up to 64 bits.
// - Any alignment and size is supported
// - The start of the 'data' area must be 64 bit aligned in all cases.
// - For fields of 64-bit or less, the first 64-bit word is filled with the zero-extended
//   value of the bitfield.
// iterator useful for scanning arrays faster than by indexing each element
// supports arrays of pairs by differentiating field size and step size.
class bf_ref;
class bf_iterator {
    friend class FlexCompressor;
    friend class PackedCompressor;
    uint64_t* data_area = nullptr;
    uint64_t* first_word_ptr = nullptr;
    size_t field_position = 0;
    uint8_t field_size = 0;
    uint8_t step_size = 0; // may be different than field_size if used for arrays of pairs
    size_t offset = 0;
    uint64_t mask = 0;

public:
    bf_iterator() = default;
    bf_iterator(const bf_iterator&) = default;
    bf_iterator(bf_iterator&&) = default;
    bf_iterator& operator=(const bf_iterator&) = default;
    bf_iterator& operator=(bf_iterator&&) = default;
    bf_iterator(const uint64_t* data_area, size_t initial_offset, size_t field_size, size_t step_size, size_t index)
    {
        init(data_area, initial_offset, field_size, step_size, index);
    }

    inline void init(const uint64_t* data_area, size_t initial_offset, size_t field_size, size_t step_size, size_t index)
    {
        this->data_area = (uint64_t*)data_area;
        this->field_size = static_cast<uint8_t>(field_size);
        this->step_size = static_cast<uint8_t>(step_size);
        this->offset = initial_offset;
        if (field_size < 64)
            mask = (1ULL << field_size) - 1;
        move(index);
    }

    inline uint64_t get_full_word_with_value() const
    {
        const auto in_word_position = field_position & 0x3F;
        const auto first_word = first_word_ptr[0];
        uint64_t result = first_word >> in_word_position;
        // note: above shifts in zeroes above the bitfield
        if (in_word_position + field_size > 64) {
            // if we're here, in_word_position > 0
            const auto first_word_size = 64 - in_word_position;
            const auto second_word = first_word_ptr[1];
            return result | second_word << first_word_size;
            // note: above shifts in zeroes below the bits we want
        }
        return result;
    }

    inline uint64_t get_value() const
    {
        auto result = get_full_word_with_value();
        // discard any bits above the field we want
        if (field_size < 64)
            result &= mask;
        return result;
    }

    // get unaligned word - this should not be called if the next word extends beyond
    // end of array. For that particular case, you must use get_last_unaligned_word instead.
    inline uint64_t get_unaligned_word() const
    {
        const auto in_word_position = field_position & 0x3F;
        const auto first_word = first_word_ptr[0];
        if (in_word_position == 0)
            return first_word;
        uint64_t result = first_word >> in_word_position;
        // note: above shifts in zeroes above the bitfield
        const auto first_word_size = 64 - in_word_position;
        const auto second_word = first_word_ptr[1];
        result |= second_word << first_word_size;
        // note: above shifts in zeroes below the bits we want
        return result;
    }

    inline uint64_t get_last_unaligned_word() const
    {
        const auto in_word_position = field_position & 0x3F;
        const auto first_word = first_word_ptr[0];
        const uint64_t result = first_word >> in_word_position;
        // note: above shifts in zeroes above the bitfield
        return result;
    }
    void set_value(uint64_t value) const
    {
        const auto in_word_position = field_position & 0x3F;
        auto first_word = first_word_ptr[0];
        uint64_t mask = 0ULL - 1ULL;
        if (field_size < 64) {
            mask = (1ULL << field_size) - 1;
            value &= mask;
        }
        // zero out field in first word:
        const auto first_word_mask = ~(mask << in_word_position);
        first_word &= first_word_mask;
        // or in relevant part of value
        first_word |= value << in_word_position;
        first_word_ptr[0] = first_word;
        if (in_word_position + field_size > 64) {
            // bitfield crosses word boundary.
            // discard the lowest bits of value (it has been written to the first word)
            const auto bits_written_to_first_word = 64 - in_word_position;
            // bit_written_to_first_word must be lower than 64, so shifts based on it are well defined
            value >>= bits_written_to_first_word;
            const auto second_word_mask = mask >> bits_written_to_first_word;
            auto second_word = first_word_ptr[1];
            // zero out the field in second word, then or in the (high part of) value
            second_word &= ~second_word_mask;
            second_word |= value;
            first_word_ptr[1] = second_word;
        }
    }
    inline void operator++()
    {
        const auto next_field_position = field_position + step_size;
        if ((next_field_position >> 6) > (field_position >> 6)) {
            first_word_ptr = data_area + (next_field_position >> 6);
        }
        field_position = next_field_position;
    }

    inline void move(size_t index)
    {
        field_position = offset + index * step_size;
        first_word_ptr = data_area + (field_position >> 6);
    }

    inline uint64_t operator*() const
    {
        return get_value();
    }

    friend bool operator<(const bf_iterator&, const bf_iterator&);
};


inline bool operator<(const bf_iterator& a, const bf_iterator& b)
{
    REALM_ASSERT(a.data_area == b.data_area);
    return a.field_position < b.field_position;
}

class bf_ref {
    bf_iterator it;

public:
    inline bf_ref(bf_iterator& it)
        : it(it)
    {
    }
    inline operator uint64_t() const
    {
        return it.get_value();
    }
    uint64_t operator=(uint64_t value)
    {
        it.set_value(value);
        return value;
    }
};

inline uint64_t read_bitfield(uint64_t* data_area, size_t field_position, size_t width)
{
    bf_iterator it(data_area, field_position, width, width, 0);
    return *it;
}

inline void write_bitfield(uint64_t* data_area, size_t field_position, size_t width, uint64_t value)
{
    bf_iterator it(data_area, field_position, width, width, 0);
    it.set_value(value);
}

inline int64_t sign_extend_field_by_mask(uint64_t sign_mask, uint64_t value)
{
    uint64_t sign_extension = 0ULL - (value & sign_mask);
    return value | sign_extension;
}

inline int64_t sign_extend_value(size_t width, uint64_t value)
{
    uint64_t sign_mask = 1ULL << (width - 1);
    uint64_t sign_extension = 0ULL - (value & sign_mask);
    return value | sign_extension;
}

template <int width>
inline std::pair<int64_t, int64_t> get_two(const char* data, size_t ndx) noexcept
{
    return std::make_pair(to_size_t(get_direct<width>(data, ndx + 0)), to_size_t(get_direct<width>(data, ndx + 1)));
}

inline std::pair<int64_t, int64_t> get_two(const char* data, size_t width, size_t ndx) noexcept
{
    REALM_TEMPEX(return get_two, width, (data, ndx));
}

/* Subword parallel search

    The following provides facilities for subword parallel search for bitfields of any size.
    To simplify, the first bitfield must be aligned within the word: it must occupy the lowest
    bits of the word.

    In general the metods here return a vector with the most significant bit in each field
    marking that a condition was met when comparing the corresponding pair of fields in two
    vectors. Checking if any field meets a condition is as simple as comparing the return
    vector against 0. Finding the first to meet a condition is also supported.

    Vectors are "split" into fields according to a MSB vector, wich indicates the most
    significant bit of each field. The MSB must be passed in as an argument to most
    bit field comparison functions. It can be generated by the field_sign_bit<width> template.

    The simplest condition to test is any_field_NE(A,B), where A and B are words.
    This condition should be true if any bitfield in A is not equal to the corresponding
    field in B.

    This is almost as simple as a direct word compare, but needs to take into account that
    we may want to have part of the words undefined.
*/
constexpr int num_fields_table[65] = {-1, 64, 32, 21, 16, 12, 10, 9, // 0-7
                                      8,  7,  6,  5,  5,  4,  4,  4, // 8-15
                                      4,  3,  3,  3,  3,  3,  2,  2, // 16-23
                                      2,  2,  2,  2,  2,  2,  2,  2, // 24-31
                                      2,  1,  1,  1,  1,  1,  1,  1, // 32-39
                                      1,  1,  1,  1,  1,  1,  1,  1, // 40-47
                                      1,  1,  1,  1,  1,  1,  1,  1, // 48-55
                                      1,  1,  1,  1,  1,  1,  1,  1, // 56-63
                                      1};

constexpr int num_bits_table[65] = {-1, 64, 64, 63, 64, 60, 60, 63, // 0-7
                                    64, 63, 60, 55, 60, 52, 56, 60, // 8-15
                                    64, 51, 54, 57, 60, 63, 44, 46, // 16-23
                                    48, 50, 52, 54, 56, 58, 60, 64, // 24-31
                                    64, 33, 34, 35, 36, 37, 38, 39, // 32-39
                                    40, 41, 42, 43, 44, 45, 46, 47, // 40-47
                                    48, 49, 50, 51, 52, 53, 54, 55, // 48-55
                                    56, 57, 58, 59, 60, 61, 62, 63, // 56-63
                                    64};

inline int num_fields_for_width(size_t width)
{
    REALM_ASSERT_DEBUG(width);
    auto retval = num_fields_table[width];
#ifdef REALM_DEBUG
    REALM_ASSERT_DEBUG(retval == 64 / width);
#endif
    return retval;
}

inline int num_bits_for_width(size_t width)
{
    return num_bits_table[width];
}

inline uint64_t cares_about(int width)
{
    return 0xFFFFFFFFFFFFFFFFULL >> (64 - num_bits_table[width]);
}

// true if any field in A differs from corresponding field in B. If you also want
// to find which fields, use find_all_fields_NE instead.
bool inline any_field_NE(int width, uint64_t A, uint64_t B)
{
    return (A ^ B) & cares_about(width);
}

// Populate all fields in a vector with a given value of a give width.
// Bits outside of the given field are ignored.
inline constexpr uint64_t populate(size_t width, uint64_t value)
{
    value &= 0xFFFFFFFFFFFFFFFFULL >> (64 - width);
    if (width < 8) {
        value |= value << width;
        width <<= 1;
        value |= value << width;
        width <<= 1;
        value |= value << width;
        width <<= 1;
    }
    // width now in range 8..64
    if (width < 32) {
        value |= value << width;
        width <<= 1;
        value |= value << width;
        width <<= 1;
    }
    // width now in range 32..128
    if (width < 64) {
        value |= value << width;
    }
    return value;
}

// provides a set bit in pos 0 of each field, remaining bits zero
inline constexpr uint64_t field_bit0(int width)
{
    return populate(width, 1);
}

// provides a set sign-bit in each field, remaining bits zero
inline constexpr uint64_t field_sign_bit(int width)
{
    return populate(width, 1ULL << (width - 1));
}

/* Unsigned LT.

    This can be determined by trial subtaction. However, some care must be exercised
    since simply subtracting one vector from another will allow carries from one
    bitfield to flow into the next one. To avoid this, we isolate bitfields by clamping
    the MSBs to 1 in A and 0 in B before subtraction. After the subtraction the MSBs in
    the result indicate borrows from the MSB. We then compute overflow (borrow OUT of MSB)
    using boolean logic as described below.

    Unsigned LT is also used to find all zero fields or all non-zero fields, so it is
    the backbone of all comparisons returning vectors.
*/

// compute the overflows in unsigned trial subtraction A-B. The overflows
// will be marked by 1 in the sign bit of each field in the result. Other
// bits in the result are zero.
// Overflow are detected for each field pair where A is less than B.
inline uint64_t unsigned_LT_vector(uint64_t MSBs, uint64_t A, uint64_t B)
{
    // 1. compute borrow from most significant bit
    // Isolate bitfields inside A and B before subtraction (prevent carries from spilling over)
    // do this by clamping most significant bit in A to 1, and msb in B to 0
    auto A_isolated = A | MSBs;                              // 1 op
    auto B_isolated = B & ~MSBs;                             // 2 ops
    auto borrows_into_sign_bit = ~(A_isolated - B_isolated); // 2 ops (total latency 4)

    // 2. determine what subtraction against most significant bit would give:
    // A B borrow-in:   (A-B-borrow-in)
    // 0 0 0            (0-0-0) = 0
    // 0 0 1            (0-0-1) = 1 + borrow-out
    // 0 1 0            (0-1-0) = 1 + borrow-out
    // 0 1 1            (0-1-1) = 0 + borrow-out
    // 1 0 0            (1-0-0) = 1
    // 1 0 1            (1-0-1) = 0
    // 1 1 0            (1-1-0) = 0
    // 1 1 1            (1-1-1) = 1 + borrow-out
    // borrow-out = (~A & B) | (~A & borrow-in) | (A & B & borrow-in)
    // The overflows are simply the borrow-out, now encoded into the sign bits of each field.
    auto overflows = (~A & B) | (~A & borrows_into_sign_bit) | (A & B & borrows_into_sign_bit);
    // ^ 6 ops, total latency 6 (4+2)
    return overflows & MSBs; // 1 op, total latency 7
    // total of 12 ops and a latency of 7. On a beefy CPU 3-4 of those can run in parallel
    // and still reach a combined latency of 10 or less.
}

inline uint64_t find_all_fields_unsigned_LT(uint64_t MSBs, uint64_t A, uint64_t B)
{
    return unsigned_LT_vector(MSBs, A, B);
}

inline uint64_t find_all_fields_NE(uint64_t MSBs, uint64_t A, uint64_t B)
{
    // 0 != A^B, same as asking 0 - (A^B) overflows.
    return unsigned_LT_vector(MSBs, 0, A ^ B);
}

inline uint64_t find_all_fields_EQ(uint64_t MSBs, uint64_t A, uint64_t B)
{
    // get the fields which are EQ and negate the result
    auto all_fields_NE = find_all_fields_NE(MSBs, A, B);
    auto all_fields_NE_negated = ~all_fields_NE;
    // must filter the negated vector so only MSB are left.
    return MSBs & all_fields_NE_negated;
}

inline uint64_t find_all_fields_unsigned_LE(uint64_t MSBs, uint64_t A, uint64_t B)
{
    // Now A <= B is the same as !(A > B) so...
    // reverse A and B to turn (A>B) --> (B<A)
    auto GT = unsigned_LT_vector(MSBs, B, A);
    // Negate the matches
    auto GT_negated = ~GT;
    // and since this negates all bits, filter so we only have MSBs again
    return MSBs & GT_negated;
}

inline uint64_t find_all_fields_unsigned_GE(uint64_t MSBs, uint64_t A, uint64_t B)
{
    return find_all_fields_unsigned_LE(MSBs, B, A);
}

inline uint64_t find_all_fields_unsigned_GT(uint64_t MSBs, uint64_t A, uint64_t B)
{
    return find_all_fields_unsigned_LT(MSBs, B, A);
}

/*
    Handling signed values

    Trial subtraction only works as is for unsigned. We simply transform signed into unsigned
    by pusing all values up by 1<<(field_width-1). This makes all negative values positive and positive
    values remain positive, although larger. Any overflow during the push can be ignored.
    After that transformation Trial subtraction should correctly detect the LT condition.

*/


inline uint64_t find_all_fields_signed_LT(uint64_t MSBs, uint64_t A, uint64_t B)
{
    auto sign_bits = MSBs;
    return unsigned_LT_vector(MSBs, A ^ sign_bits, B ^ sign_bits);
}

inline uint64_t find_all_fields_signed_LE(uint64_t MSBs, uint64_t A, uint64_t B)
{
    auto sign_bits = MSBs;
    return find_all_fields_unsigned_LE(MSBs, A ^ sign_bits, B ^ sign_bits);
}

inline uint64_t find_all_fields_signed_GT(uint64_t MSBs, uint64_t A, uint64_t B)
{
    // A > B is the same as B < A
    return find_all_fields_signed_LT(MSBs, B, A);
}

inline uint64_t find_all_fields_signed_GE(uint64_t MSBs, uint64_t A, uint64_t B)
{
    // A >= B is the same as B <= A
    return find_all_fields_signed_LE(MSBs, B, A);
}

// find the first field which have MSB set (marks overflow after trial subtraction, or other
// requested condition).
struct find_field_desc {
    uint8_t levels;
    uint64_t m1;
    uint64_t m2;
    uint64_t m4;
    uint64_t m8;
    uint64_t m16;
    uint64_t m32;
};

constexpr struct find_field_desc find_field_table[65] = {
    /* 0 */ {0, 0, 0, 0, 0, 0},
    /* 1 */
    {6, 0xAAAAAAAAAAAAAAAA, 0xCCCCCCCCCCCCCCCC, 0xF0F0F0F0F0F0F0F0, 0xFF00FF00FF00FF00, 0xFFFF0000FFFF0000,
     0xFFFFFFFF00000000},
    /* 2 */
    {5, 0xCCCCCCCCCCCCCCCC, 0xF0F0F0F0F0F0F0F0, 0xFF00FF00FF00FF00, 0xFFFF0000FFFF0000, 0xFFFFFFFF00000000, 0},
    /* 3 */
    {5, 0b0000'1110'0011'1000'1110'0011'1000'1110'0011'1000'1110'0011'1000'1110'0011'1000,
     0b0000'1111'1100'0000'1111'1100'0000'1111'1100'0000'1111'1100'0000'1111'1100'0000,
     0b1111'0000'0000'0000'1111'1111'1111'0000'0000'0000'1111'1111'1111'0000'0000'0000,
     0b0000'0000'0000'0000'1111'1111'1111'1111'1111'1111'0000'0000'0000'0000'0000'0000,
     0b1111'1111'1111'1111'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000, 0},
    /* 4 */
    {4, 0xF0F0F0F0F0F0F0F0, 0xFF00FF00FF00FF00, 0xFFFF0000FFFF0000, 0xFFFFFFFF00000000, 0, 0},
    /* 5 */
    {4, 0b0000'1111'1000'0011'1110'0000'1111'1000'0011'1110'0000'1111'1000'0011'1110'0000,
     0b0000'1111'1111'1100'0000'0000'1111'1111'1100'0000'0000'1111'1111'1100'0000'0000,
     0b1111'0000'0000'0000'0000'0000'1111'1111'1111'1111'1111'0000'0000'0000'0000'0000,
     0b1111'1111'1111'1111'1111'1111'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000, 0, 0},
    /* 6 */
    {4, 0b0000'1111'1100'0000'1111'1100'0000'1111'1100'0000'1111'1100'0000'1111'1100'0000,
     0b1111'0000'0000'0000'1111'1111'1111'0000'0000'0000'1111'1111'1111'0000'0000'0000,
     0b0000'0000'0000'0000'1111'1111'1111'1111'1111'1111'0000'0000'0000'0000'0000'0000,
     0b1111'1111'1111'1111'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000, 0, 0},
    /* 7 */
    {4, 0b1000'0000'1111'1110'0000'0011'1111'1000'0000'1111'1110'0000'0011'1111'1000'0000,
     0b0000'0000'1111'1111'1111'1100'0000'0000'0000'1111'1111'1111'1100'0000'0000'0000,
     0b0000'0000'1111'1111'1111'1111'1111'1111'1111'0000'0000'0000'0000'0000'0000'0000,
     0b1111'1111'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000, 0, 0},
    /* 8 */
    {3, 0xFF00FF00FF00FF00, 0xFFFF0000FFFF0000, 0xFFFFFFFF00000000, 0, 0, 0},
    /* 9 */
    {3, 0b1000'0000'0011'1111'1110'0000'0000'1111'1111'1000'0000'0011'1111'1110'0000'0000,
     0b0111'1111'1100'0000'0000'0000'0000'1111'1111'1111'1111'1100'0000'0000'0000'0000,
     0b1111'1111'1111'1111'1111'1111'1111'0000'0000'0000'0000'0000'0000'0000'0000'0000, 0, 0, 0},
    /* 10 */
    {3, 0b0000'1111'1111'1100'0000'0000'1111'1111'1100'0000'0000'1111'1111'1100'0000'0000,
     0b1111'0000'0000'0000'0000'0000'1111'1111'1111'1111'1111'0000'0000'0000'0000'0000,
     0b1111'1111'1111'1111'1111'1111'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000, 0, 0, 0},
    /* 11 */
    {3, 0b1111'1111'1000'0000'0000'1111'1111'1110'0000'0000'0011'1111'1111'1000'0000'0000,
     0b0000'0000'0000'0000'0000'1111'1111'1111'1111'1111'1100'0000'0000'0000'0000'0000,
     0b1111'1111'1111'1111'1111'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000, 0, 0, 0},
    /* 12 */
    {3, 0b1111'0000'0000'0000'1111'1111'1111'0000'0000'0000'1111'1111'1111'0000'0000'0000,
     0b0000'0000'0000'0000'1111'1111'1111'1111'1111'1111'0000'0000'0000'0000'0000'0000,
     0b1111'1111'1111'1111'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000, 0, 0, 0},
    /* 13 */
    {3, 0b1110'0000'0000'0000'1111'1111'1110'0000'0000'0011'1111'1111'1110'0000'0000'0000,
     0b0000'0000'0000'0000'1111'1111'1111'1111'1111'1100'0000'0000'0000'0000'0000'0000,
     0b1111'1111'1111'1111'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000, 0, 0, 0},
    /* 14 */
    {3, 0b0000'0000'1111'1111'1111'1100'0000'0000'0000'1111'1111'1111'1100'0000'0000'0000,
     0b0000'0000'1111'1111'1111'1111'1111'1111'1111'0000'0000'0000'0000'0000'0000'0000,
     0b1111'1111'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000, 0, 0, 0},
    /* 15 */
    {3, 0b0000'1111'1111'1111'1110'0000'0000'0000'0011'1111'1111'1111'1000'0000'0000'0000,
     0b0000'1111'1111'1111'1111'1111'1111'1111'1100'0000'0000'0000'0000'0000'0000'0000,
     0b1111'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000'0000, 0, 0, 0},
    /* 16 */
    {2, 0xFFFF0000FFFF0000, 0xFFFFFFFF00000000, 0, 0, 0, 0},
    /* 17 - as we're only interested in msb of each field we can simplify and use same pattern
     for the next 4 entries */
    {2, 0xF00000FFFFF00000, 0xFFFFFF0000000000, 0, 0, 0, 0},
    /* 18 */
    {2, 0xF00000FFFFF00000, 0xFFFFFF0000000000, 0, 0, 0, 0},
    /* 19 */
    {2, 0xF00000FFFFF00000, 0xFFFFFF0000000000, 0, 0, 0, 0},
    /* 20 */
    {2, 0xF00000FFFFF00000, 0xFFFFFF0000000000, 0, 0, 0, 0},
    /* 21 - and next 4 */
    {2, 0x0000FFFFFF000000, 0xFFFF000000000000, 0, 0, 0, 0},
    /* 22 */
    {2, 0x0000FFFFFF000000, 0xFFFF000000000000, 0, 0, 0, 0},
    /* 23 */
    {2, 0x0000FFFFFF000000, 0xFFFF000000000000, 0, 0, 0, 0},
    /* 24 */
    {2, 0x0000FFFFFF000000, 0xFFFF000000000000, 0, 0, 0, 0},
    /* 25 - and 4 more */
    {2, 0x00FFFFFFF0000000, 0xFF00000000000000, 0, 0, 0, 0},
    /* 26 */
    {2, 0x00FFFFFFF0000000, 0xFF00000000000000, 0, 0, 0, 0},
    /* 27 */
    {2, 0x00FFFFFFF0000000, 0xFF00000000000000, 0, 0, 0, 0},
    /* 28 */
    {2, 0x00FFFFFFF0000000, 0xFF00000000000000, 0, 0, 0, 0},
    /* 29 - last 4 where multiple fields exist */
    {1, 0xFFFFFFFF00000000, 0, 0, 0, 0, 0},
    /* 30 */
    {1, 0xFFFFFFFF00000000, 0, 0, 0, 0, 0},
    /* 31 */
    {1, 0xFFFFFFFF00000000, 0, 0, 0, 0, 0},
    /* 32 */
    {1, 0xFFFFFFFF00000000, 0, 0, 0, 0, 0},
    /* 33 - from here to 64, there is only 1 possible result: 0 */
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0}};

#if 1
constexpr uint32_t inverse_width[65] = {
    65536 * 64 / 1, // never used
    65536 * 64 / 1,  65536 * 64 / 2,  65536 * 64 / 3,  65536 * 64 / 4,  65536 * 64 / 5,  65536 * 64 / 6,
    65536 * 64 / 7,  65536 * 64 / 8,  65536 * 64 / 9,  65536 * 64 / 10, 65536 * 64 / 11, 65536 * 64 / 12,
    65536 * 64 / 13, 65536 * 64 / 14, 65536 * 64 / 15, 65536 * 64 / 16, 65536 * 64 / 17, 65536 * 64 / 18,
    65536 * 64 / 19, 65536 * 64 / 20, 65536 * 64 / 21, 65536 * 64 / 22, 65536 * 64 / 23, 65536 * 64 / 24,
    65536 * 64 / 25, 65536 * 64 / 26, 65536 * 64 / 27, 65536 * 64 / 28, 65536 * 64 / 29, 65536 * 64 / 30,
    65536 * 64 / 31, 65536 * 64 / 32, 65536 * 64 / 33, 65536 * 64 / 34, 65536 * 64 / 35, 65536 * 64 / 36,
    65536 * 64 / 37, 65536 * 64 / 38, 65536 * 64 / 39, 65536 * 64 / 40, 65536 * 64 / 41, 65536 * 64 / 42,
    65536 * 64 / 43, 65536 * 64 / 44, 65536 * 64 / 45, 65536 * 64 / 46, 65536 * 64 / 47, 65536 * 64 / 48,
    65536 * 64 / 49, 65536 * 64 / 50, 65536 * 64 / 51, 65536 * 64 / 52, 65536 * 64 / 53, 65536 * 64 / 54,
    65536 * 64 / 55, 65536 * 64 / 56, 65536 * 64 / 57, 65536 * 64 / 58, 65536 * 64 / 59, 65536 * 64 / 60,
    65536 * 64 / 61, 65536 * 64 / 62, 65536 * 64 / 63, 65536 * 64 / 64,
};

inline size_t countr_zero(uint64_t vector)
{
    unsigned long where;
#if defined(_WIN64)
    if (_BitScanForward64(&where, vector))
        return static_cast<int>(where);
    return 0;
#elif defined(_WIN32)
    uint32_t low = vector & 0xFFFFFFFF;
    if (low) {
        bool scan_ok = _BitScanForward(&where, low);
        REALM_ASSERT_DEBUG(scan_ok);
        return where;
    }
    else {
        low = vector >> 32;
        bool scan_ok = _BitScanForward(&where, low);
        REALM_ASSERT_DEBUG(scan_ok);
        return 32 + where;
    }
#else
    where = __builtin_ctzll(vector);
    return static_cast<int>(where);
#endif
}

inline size_t first_field_marked(size_t width, uint64_t vector)
{
    const auto lz = countr_zero(vector);
    const auto field = (lz * inverse_width[width]) >> 22;
    REALM_ASSERT_DEBUG(width != 0);
    REALM_ASSERT_DEBUG(field == (lz / width));
    return field;
}
#endif
#if 0
inline int first_field_marked(int width, uint64_t vector)
{
    // isolate least significant bit
    vector = vector & (~vector + 1);
    const struct find_field_desc& desc = find_field_table[width];
    int result = 0;
    switch (desc.levels) {
        // the following case entries are intended to fall through
        // (this is a variant of Duff's Device)
        // TODO: disable compiler warnings for it
        case 6:
            result |= (vector & desc.m32) ? 32 : 0;
        case 5:
            result |= (vector & desc.m16) ? 16 : 0;
        case 4:
            result |= (vector & desc.m8) ? 8 : 0;
        case 3:
            result |= (vector & desc.m4) ? 4 : 0;
        case 2:
            result |= (vector & desc.m2) ? 2 : 0;
        case 1:
            result |= (vector & desc.m1) ? 1 : 0;
        default:
            break;
    }
    return result;
}
#endif
#if 0
inline int first_field_marked(int width, uint64_t vector)
{
    // isolate least significant bit
    vector = vector & (~vector + 1);
    // directly compute position of set bit using table
    const struct find_field_desc& desc = find_field_table[width];
    return ((vector & desc.m1) ? 1 : 0) | ((vector & desc.m2) ? 2 : 0) | ((vector & desc.m4) ? 4 : 0) |
           ((vector & desc.m8) ? 8 : 0) | ((vector & desc.m16) ? 16 : 0) | ((vector & desc.m32) ? 32 : 0);
}
#endif
#if 0
inline int first_field_marked(int width, uint64_t vector)
{
    int result = 0;
    auto msb = 1ULL << (width - 1);
    while (msb) {
        if (vector & msb)
            return result;
        msb <<= width;
        result++;
    }
    return -1;
}
#endif

template <typename VectorCompare>
size_t parallel_subword_find(VectorCompare vector_compare, const uint64_t* data, size_t offset, size_t width,
                             uint64_t MSBs, uint64_t search_vector, size_t start, size_t end)
{
    const auto field_count = num_fields_for_width(width);
    const auto bit_count_pr_iteration = num_bits_for_width(width);
    const auto fast_scan_limit = 4 * bit_count_pr_iteration;
    // use signed to make it easier to ascertain correctness of loop condition below
    signed total_bit_count_left = static_cast<signed>(end - start) * static_cast<signed>(width);
    REALM_ASSERT_DEBUG(total_bit_count_left >= 0);
    unaligned_word_iter it(data, offset + start * width);
    uint64_t found_vector = 0;
    while (total_bit_count_left >= fast_scan_limit) {
        // unrolling 2x
        const auto word0 = it.get_with_unsafe_prefetch(bit_count_pr_iteration);
        it.bump(bit_count_pr_iteration);
        const auto word1 = it.get_with_unsafe_prefetch(bit_count_pr_iteration);
        auto found_vector0 = vector_compare(MSBs, word0, search_vector);
        auto found_vector1 = vector_compare(MSBs, word1, search_vector);
        it.bump(bit_count_pr_iteration);
        if (found_vector0) {
            const auto sub_word_index = first_field_marked(width, found_vector0);
            return start + sub_word_index;
        }
        if (found_vector1) {
            const auto sub_word_index = first_field_marked(width, found_vector1);
            return start + field_count + sub_word_index;
        }
        total_bit_count_left -= 2 * bit_count_pr_iteration;
        start += 2 * field_count;
    }
    while (total_bit_count_left >= bit_count_pr_iteration) {
        const auto word = it.get(bit_count_pr_iteration);
        found_vector = vector_compare(MSBs, word, search_vector);
        if (found_vector) {
            const auto sub_word_index = first_field_marked(width, found_vector);
            return start + sub_word_index;
        }
        total_bit_count_left -= bit_count_pr_iteration;
        start += field_count;
        it.bump(bit_count_pr_iteration);
    }
    if (total_bit_count_left) {                         // final subword, may be partial
        const auto word = it.get(total_bit_count_left); // <-- limit lookahead to avoid touching memory beyond array
        found_vector = vector_compare(MSBs, word, search_vector);
        auto last_word_mask = 0xFFFFFFFFFFFFFFFFULL >> (64 - total_bit_count_left);
        found_vector &= last_word_mask;
        if (found_vector) {
            const auto sub_word_index = first_field_marked(width, found_vector);
            return start + sub_word_index;
        }
    }
    return end;
}


namespace impl {

template <int width>
inline int64_t default_fetcher(const char* data, size_t ndx)
{
    return get_direct<width>(data, ndx);
}

template <typename T>
struct EncodedFetcher {

    int64_t operator()(const char*, size_t ndx) const
    {
        return ptr->get(ndx);
    }
    const T* ptr;
};

// Lower and Upper bound are mainly used in the B+tree implementation,
// but also for indexing, we can exploit these functions when the array
// is encoded, just providing a way for fetching the data.
// In this case the width is going to be ignored.

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
template <int width, typename F = decltype(default_fetcher<width>)>
inline size_t lower_bound(const char* data, size_t start, size_t end, int64_t value,
                          F fetcher = default_fetcher) noexcept
{
    // The binary search used here is carefully optimized. Key trick is to use a single
    // loop controlling variable (size) instead of high/low pair, and to keep updates
    // to size done inside the loop independent of comparisons. Further key to speed
    // is to avoid branching inside the loop, using conditional moves instead. This
    // provides robust performance for random searches, though predictable searches
    // might be slightly faster if we used branches instead. The loop unrolling yields
    // a final 5-20% speedup depending on circumstances.

    // size_t low = 0;
    REALM_ASSERT_DEBUG(end >= start);
    size_t size = end - start;
    // size_t low = 0;
    size_t low = start;

    while (size >= 8) {
        // The following code (at X, Y and Z) is 3 times manually unrolled instances of (A) below.
        // These code blocks must be kept in sync. Meassurements indicate 3 times unrolling to give
        // the best performance. See (A) for comments on the loop body.
        // (X)
        size_t half = size / 2;
        size_t other_half = size - half;
        size_t probe = low + half;
        size_t other_low = low + other_half;
        int64_t v = fetcher(data, probe);
        size = half;
        low = (v < value) ? other_low : low;

        // (Y)
        half = size / 2;
        other_half = size - half;
        probe = low + half;
        other_low = low + other_half;
        v = fetcher(data, probe);
        size = half;
        low = (v < value) ? other_low : low;

        // (Z)
        half = size / 2;
        other_half = size - half;
        probe = low + half;
        other_low = low + other_half;
        v = fetcher(data, probe);
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
        int64_t v = fetcher(data, probe); // get_direct<width>(data, probe);
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
template <int width, typename F = decltype(default_fetcher<width>)>
inline size_t upper_bound(const char* data, size_t start, size_t end, int64_t value,
                          F fetcher = default_fetcher) noexcept
{
    REALM_ASSERT_DEBUG(end >= start);
    size_t size = end - start;
    // size_t low = 0;
    size_t low = start;
    while (size >= 8) {
        size_t half = size / 2;
        size_t other_half = size - half;
        size_t probe = low + half;
        size_t other_low = low + other_half;
        int64_t v = fetcher(data, probe);
        size = half;
        low = (value >= v) ? other_low : low;

        half = size / 2;
        other_half = size - half;
        probe = low + half;
        other_low = low + other_half;
        v = fetcher(data, probe);
        size = half;
        low = (value >= v) ? other_low : low;

        half = size / 2;
        other_half = size - half;
        probe = low + half;
        other_low = low + other_half;
        v = fetcher(data, probe);
        size = half;
        low = (value >= v) ? other_low : low;
    }

    while (size > 0) {
        size_t half = size / 2;
        size_t other_half = size - half;
        size_t probe = low + half;
        size_t other_low = low + other_half;
        int64_t v = fetcher(data, probe);
        size = half;
        low = (value >= v) ? other_low : low;
    };

    return low;
}
} // namespace impl

template <int width>
inline size_t lower_bound(const char* data, size_t size, int64_t value) noexcept
{
    return impl::lower_bound<width>(data, 0, size, value, impl::default_fetcher<width>);
}

template <typename T>
inline size_t lower_bound(const char* data, size_t size, int64_t value,
                          const impl::EncodedFetcher<T>& encoder) noexcept
{
    return impl::lower_bound<0>(data, 0, size, value, encoder);
}

template <int width>
inline size_t upper_bound(const char* data, size_t size, int64_t value) noexcept
{
    return impl::upper_bound<width>(data, 0, size, value, impl::default_fetcher<width>);
}

template <typename T>
inline size_t upper_bound(const char* data, size_t size, int64_t value,
                          const impl::EncodedFetcher<T>& encoder) noexcept
{
    return impl::lower_bound<0>(data, 0, size, value, encoder);
}

} // namespace realm

#endif /* ARRAY_TPL_HPP_ */
