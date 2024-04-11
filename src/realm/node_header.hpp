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

#ifndef REALM_NODE_HEADER_HPP
#define REALM_NODE_HEADER_HPP

#include <realm/util/assert.hpp>
#include <realm/utilities.hpp>

namespace {
// helper converting a number of bits into bytes and aligning to 8 byte boundary
static inline size_t align_bits_to8(size_t n)
{
    n = (n + 7) >> 3;
    return (n + 7) & ~size_t(7);
}
} // namespace

namespace realm {

// The header holds metadata for all allocations. It is 8 bytes.
// A field in byte 5 indicates the type of the allocation.
//
// Up to and including Core v 13, this field would always hold values 0,1 or 2.
// when stored in the file. This value now indicates that the chunk of memory
// must be interpreted according to the methods in NodeHeader.
//
const size_t max_array_size = 0x00ffffffL;            // Maximum number of elements in an array
const size_t max_array_payload_aligned = 0x07ffffc0L; // Maximum number of bytes that the payload of an array can be
// Even though the encoding supports arrays with size up to max_array_payload_aligned,
// the maximum allocation size is smaller as it must fit within a memory section
// (a contiguous virtual address range). This limitation is enforced in SlabAlloc::do_alloc().


class NodeHeader {

public:
    enum Type {
        type_Normal,

        /// This array is the main array of an innner node of a B+-tree as used
        /// in table columns.
        type_InnerBptreeNode,

        /// This array may contain refs to subarrays. An element whose least
        /// significant bit is zero, is a ref pointing to a subarray. An element
        /// whose least significant bit is one, is just a value. It is the
        /// responsibility of the application to ensure that non-ref values have
        /// their least significant bit set. This will generally be done by
        /// shifting the desired vlue to the left by one bit position, and then
        /// setting the vacated bit to one.
        type_HasRefs
    };

    enum WidthType {
        // The first 3 encodings where the only one used as far as Core v13.
        wtype_Bits = 0,     // width indicates how many bits every element occupies
        wtype_Multiply = 1, // width indicates how many bytes every element occupies
        wtype_Ignore = 2,   // each element is 1 byte
        wtype_Extend = 3    // the layouts are described in byte 4 of the header.
    };
    // Accessing flags.
    enum class Flags { // bit positions in flags "byte", used for masking
        Context = 1,
        HasRefs = 2,
        InnerBPTree = 4,
        // additional flags can be supported by new layouts, but old layout is full
    };
    // Possible header encodings (and corresponding memory layouts):
    enum class Encoding {
        WTypBits = 0, // Corresponds to wtype_Bits
        WTypMult = 1, // Corresponds to wtype_Multiply
        WTypIgn = 2,  // Corresponds to wtype_Ignore
        Packed = 3,   // wtype is wtype_Extend
        Flex = 5      // wtype is wtype_Extend
    };
    // * Packed: tightly packed array (any element size <= 64)
    // * WTypBits: less tightly packed. Correspond to wtype_Bits
    // * WTypMult: less tightly packed. Correspond to wtype_Multiply
    // * WTypIgn: single byte elements. Correspond to wtype_Ignore
    // encodings with more flexibility but lower number of elements:
    // * Flex: Pair of arrays (2 element sizes, 2 element count)
    //
    // Encodings:     bytes:
    // name:       |  b0   |  b1   |  b2   |  b3   | b4:0-2 | b4:3-4 | b4:5-7 |  b5   |  b6   |  b7  |
    // oldies      |  cap/chksum           |  'A'  | width  | wtype  | flags  |          size        |
    // Packed      |  cap/chksum   | -     | width | flags2 | wtype  | flags  | enc   |     size     |
    // Flex        |  cap/chksum   |  w_A + size_A | flags2 | wtype  | flags  | enc   | w_B + size_B |
    //
    // legend: cap = capacity, chksum = checksum, flags = 3 flag bits, flags2 = 3 additional flag bits
    //         size = number of elements, w_A = bits per A element, w_B = bits per B element
    //         size_A = number of A elements, size_B = number of B elements,
    //         enc = the encoding for the array, corresponding to different memory layouts
    //         For Flex: w + size is 6 bits for element width, 10 bits for number of elements
    //

    static const int header_size = 8; // Number of bytes used by header

    // The encryption layer relies on headers always fitting within a single page.
    static_assert(header_size == 8, "Header must always fit in entirely on a page");

    static char* get_data_from_header(char* header) noexcept
    {
        return header + header_size;
    }

    static char* get_header_from_data(char* data) noexcept
    {
        return data - header_size;
    }

    static const char* get_data_from_header(const char* header) noexcept
    {
        return get_data_from_header(const_cast<char*>(header));
    }

    // Helpers for NodeHeader::Type
    // handles all header formats
    static inline bool get_is_inner_bptree_node_from_header(const char* header) noexcept
    {
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        return (int(h[4]) & 0x80) != 0;
    }

    static inline bool get_hasrefs_from_header(const char* header) noexcept
    {
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        return (int(h[4]) & 0x40) != 0;
    }

    static Type get_type_from_header(const char* header) noexcept
    {
        if (get_is_inner_bptree_node_from_header(header))
            return type_InnerBptreeNode;
        if (get_hasrefs_from_header(header))
            return type_HasRefs;
        return type_Normal;
    }

    static inline bool get_context_flag_from_header(const char* header) noexcept
    {
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        return (int(h[4]) & 0x20) != 0;
    }
    static inline void set_is_inner_bptree_node_in_header(bool value, char* header) noexcept
    {
        typedef unsigned char uchar;
        uchar* h = reinterpret_cast<uchar*>(header);
        h[4] = uchar((int(h[4]) & ~0x80) | int(value) << 7);
    }

    static inline void set_hasrefs_in_header(bool value, char* header) noexcept
    {
        typedef unsigned char uchar;
        uchar* h = reinterpret_cast<uchar*>(header);
        h[4] = uchar((int(h[4]) & ~0x40) | int(value) << 6);
    }

    static inline void set_context_flag_in_header(bool value, char* header) noexcept
    {
        typedef unsigned char uchar;
        uchar* h = reinterpret_cast<uchar*>(header);
        h[4] = uchar((int(h[4]) & ~0x20) | int(value) << 5);
    }

    // Helpers for NodeHeader::WidthType:
    // handles all header formats
    static inline WidthType get_wtype_from_header(const char* header) noexcept
    {
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        int h4 = h[4];
        return WidthType((h4 & 0x18) >> 3);
    }

    static inline bool wtype_is_extended(const char* header) noexcept
    {
        return get_wtype_from_header(header) == wtype_Extend;
    }

    static inline void set_wtype_in_header(WidthType value, char* header) noexcept
    {
        typedef unsigned char uchar;
        uchar* h = reinterpret_cast<uchar*>(header);
        auto h4 = h[4];
        h4 = (h4 & ~0x18) | int(value) << 3;
        h[4] = h4;
    }

    static size_t unsigned_to_num_bits(uint64_t value)
    {
        if constexpr (sizeof(size_t) == sizeof(uint64_t))
            return 1 + log2(value);
        uint32_t high = value >> 32;
        if (high)
            return 33 + log2(high);
        uint32_t low = value & 0xFFFFFFFFUL;
        if (low)
            return 1 + log2(low);
        return 0;
    }

    static inline size_t signed_to_num_bits(int64_t value)
    {
        if (value >= 0)
            return 1 + unsigned_to_num_bits(value);
        else
            return 1 + unsigned_to_num_bits(~value); // <-- is this correct????
    }


    // Helper functions for old layouts only:
    // Handling width and sizes:
    static inline uint_least8_t get_width_from_header(const char* header) noexcept;

    static inline size_t get_size_from_header(const char* header) noexcept;

    static inline void set_width_in_header(size_t value, char* header) noexcept
    {
        REALM_ASSERT_DEBUG(!wtype_is_extended(header));
        // Pack width in 3 bits (log2)
        int w = 0;
        while (value) {
            ++w;
            value >>= 1;
        }
        REALM_ASSERT_3(w, <, 8);

        typedef unsigned char uchar;
        uchar* h = reinterpret_cast<uchar*>(header);
        h[4] = uchar((int(h[4]) & ~0x7) | w);
    }

    static inline void set_size_in_header(size_t value, char* header) noexcept
    {
        REALM_ASSERT_DEBUG(!wtype_is_extended(header));
        REALM_ASSERT_3(value, <=, max_array_size);
        typedef unsigned char uchar;
        uchar* h = reinterpret_cast<uchar*>(header);
        h[5] = uchar((value >> 16) & 0x000000FF);
        h[6] = uchar((value >> 8) & 0x000000FF);
        h[7] = uchar(value & 0x000000FF);
    }


    // Note: The wtype must have been set prior to calling this function
    static size_t get_capacity_from_header(const char* header) noexcept
    {
        if (!wtype_is_extended(header)) {
            typedef unsigned char uchar;
            const uchar* h = reinterpret_cast<const uchar*>(header);
            return (size_t(h[0]) << 19) + (size_t(h[1]) << 11) + (h[2] << 3);
        }
        else {
            return reinterpret_cast<const uint16_t*>(header)[0] << 3;
        }
    }

    // Note: There is a (no longer a correct) copy of this function is test_alloc.cpp
    // Note 2: The wtype must have been set prior to calling this function
    static void set_capacity_in_header(size_t value, char* header) noexcept
    {
        if (!wtype_is_extended(header)) {
            REALM_ASSERT_3(value, <=, (0xffffff << 3));
            typedef unsigned char uchar;
            uchar* h = reinterpret_cast<uchar*>(header);
            h[0] = uchar((value >> 19) & 0x000000FF);
            h[1] = uchar((value >> 11) & 0x000000FF);
            h[2] = uchar(value >> 3 & 0x000000FF);
        }
        else {
            REALM_ASSERT_DEBUG(value < (65536 << 3));
            REALM_ASSERT_DEBUG((value & 0x7) == 0);
            (reinterpret_cast<uint16_t*>(header))[0] = static_cast<uint16_t>(value >> 3);
        }
    }
    static size_t get_byte_size_from_header(const char* header) noexcept;

    // ^ First 3 must overlap numerically with corresponding wtype_X enum.
    static Encoding get_encoding(const char* header)
    {
        auto wtype = get_wtype_from_header(header);
        if (wtype == wtype_Extend) {
            const auto h = reinterpret_cast<const uint8_t*>(header);
            return Encoding{static_cast<int>(h[5]) + 3};
        }
        return Encoding(static_cast<int>(wtype));
    }
    static void set_encoding(char* header, Encoding enc)
    {
        if (enc < Encoding::Packed) {
            set_wtype_in_header(static_cast<WidthType>(enc), header);
        }
        else {
            set_wtype_in_header(wtype_Extend, header);
            auto h = reinterpret_cast<uint8_t*>(header);
            h[5] = static_cast<uint8_t>(enc) - 3;
        }
    }
    static std::string enc_to_string(Encoding enc)
    {
        switch (enc) {
            case Encoding::WTypMult:
                return "Mult";
            case Encoding::WTypIgn:
                return "Ign";
            case Encoding::WTypBits:
                return "Bits";
            case Encoding::Packed:
                return "Pack";
            case Encoding::Flex:
                return "Flex";
            default:
                return "Err";
        }
    }
    static std::string header_to_string(const char* header)
    {
        std::string retval = "{" + enc_to_string(get_encoding(header)) + "}";
        return retval;
    }

private:
    friend class Node;
    friend class IntegerCompressor;
    // Setting element size for encodings with a single element size:
    static void inline set_element_size(char* header, size_t bits_per_element, Encoding);
    // Getting element size for encodings with a single element size:
    static inline size_t get_element_size(const char* header, Encoding);
    // Used only by flex at this stage.
    // Setting element sizes for encodings with two element sizes (called A and B)
    static inline void set_elementA_size(char* header, size_t bits_per_element);
    static inline void set_elementB_size(char* header, size_t bits_per_element);
    // Getting element sizes for encodings with two element sizes (called A and B)
    static inline size_t get_elementA_size(const char* header);
    static inline size_t get_elementB_size(const char* header);
    // Setting num of elements for encodings with two element sizes (called A and B)
    static inline void set_arrayA_num_elements(char* header, size_t num_elements);
    static inline void set_arrayB_num_elements(char* header, size_t num_elements);
    // Getting number of elements for encodings with two element sizes (called A and B)
    static inline size_t get_arrayA_num_elements(const char* header);
    static inline size_t get_arrayB_num_elements(const char* header);
    // Getting the number of elements in the array(s). All encodings except Flex have one number of elements.
    static inline size_t get_num_elements(const char* header, Encoding);
    // Setting the number of elements in the array(s). All encodings except Flex have one number of elements.
    static inline void set_num_elements(char* header, size_t num_elements, Encoding);

    static inline size_t calc_size(size_t num_elements);
    static inline size_t calc_size(size_t num_elements, size_t element_size, Encoding);
    static inline size_t calc_size(size_t arrayA_num_elements, size_t arrayB_num_elements, size_t elementA_size,
                                   size_t elementB_size);

    static size_t calc_byte_size(WidthType wtype, size_t size, uint_least8_t width) noexcept
    {
        // the width need to be adjusted to nearest power of two:
        if (width > 8) {
            if (width > 32)
                width = 64;
            else if (width > 16)
                width = 32;
            else
                width = 16;
        }
        else { // width <= 8
            if (width > 4)
                width = 8;
            else if (width > 2)
                width = 4;
            // else width is already a power of 2
        }
        size_t num_bytes = 0;
        switch (wtype) {
            case wtype_Bits: {
                // Current assumption is that size is at most 2^24 and that width is at most 64.
                // In that case the following will never overflow. (Assuming that size_t is at least 32 bits)
                REALM_ASSERT_3(size, <, 0x1000000);
                size_t num_bits = size * width;
                num_bytes = (num_bits + 7) >> 3;
                break;
            }
            case wtype_Multiply: {
                num_bytes = size * width;
                break;
            }
            case wtype_Ignore:
                num_bytes = size;
                break;
            default: {
                REALM_ASSERT(false);
                break;
            }
        }
        num_bytes += header_size;
        // Ensure 8-byte alignment
        num_bytes = (num_bytes + 7) & ~size_t(7);
        return num_bytes;
    }

    static inline void set_flags(char* header, uint8_t flags)
    {
        REALM_ASSERT_DEBUG(flags <= 7);
        auto h = reinterpret_cast<uint8_t*>(header);
        h[4] = (h[4] & 0b00011111) | flags << 5;
    }
    static inline uint8_t get_flags(char* header)
    {
        auto h = reinterpret_cast<uint8_t*>(header);
        return h[4] >> 5;
    }

    static inline void set_flags2(char* header, uint8_t flags)
    {
        REALM_ASSERT_DEBUG(flags <= 7);
        auto h = reinterpret_cast<uint8_t*>(header);
        h[4] = (h[4] & 0b11111000) | flags;
    }
    static inline uint8_t get_flags2(char* header)
    {
        auto h = reinterpret_cast<uint8_t*>(header);
        return h[4] & 0b0111;
    }
};

inline void NodeHeader::set_element_size(char* header, size_t bits_per_element, Encoding encoding)
{
    switch (encoding) {
        case NodeHeader::Encoding::Packed: {
            REALM_ASSERT_DEBUG(get_encoding(header) == Encoding::Packed);
            REALM_ASSERT_DEBUG(bits_per_element <= 64);
            (reinterpret_cast<uint8_t*>(header)[3] = static_cast<uint8_t>(bits_per_element));
        } break;
        case NodeHeader::Encoding::WTypBits: {
            REALM_ASSERT_DEBUG(bits_per_element <= 64);
            // TODO: Only powers of two allowed
            // TODO: Optimize
            NodeHeader::set_wtype_in_header(wtype_Bits, reinterpret_cast<char*>(header));
            NodeHeader::set_width_in_header(bits_per_element, reinterpret_cast<char*>(header));
        } break;
        case NodeHeader::Encoding::WTypMult: {
            REALM_ASSERT_DEBUG(bits_per_element <= 64);
            REALM_ASSERT_DEBUG((bits_per_element & 0x7) == 0);
            // TODO: Only powers of two allowed
            // TODO: Optimize
            NodeHeader::set_wtype_in_header(wtype_Multiply, reinterpret_cast<char*>(header));
            NodeHeader::set_width_in_header(bits_per_element >> 3, reinterpret_cast<char*>(header));
        } break;
        default:
            REALM_UNREACHABLE();
    }
}

inline size_t NodeHeader::get_element_size(const char* header, Encoding encoding)
{
    switch (encoding) {
        case NodeHeader::Encoding::Packed: {
            REALM_ASSERT_DEBUG(get_encoding(header) == Encoding::Packed);
            const auto bits_per_element = (reinterpret_cast<const uint8_t*>(header))[3];
            REALM_ASSERT_DEBUG(bits_per_element <= 64);
            return bits_per_element;
        } break;
        case NodeHeader::Encoding::WTypBits: {
            REALM_ASSERT_DEBUG(get_wtype_from_header(header) == wtype_Bits);
            const auto bits_per_element = NodeHeader::get_width_from_header(reinterpret_cast<const char*>(header));
            REALM_ASSERT_DEBUG(bits_per_element <= 64);
            return bits_per_element;
        } break;
        case NodeHeader::Encoding::WTypMult: {
            REALM_ASSERT_DEBUG(get_wtype_from_header(header) == wtype_Multiply);
            const auto bits_per_element = NodeHeader::get_width_from_header(reinterpret_cast<const char*>(header))
                                          << 3;
            REALM_ASSERT_DEBUG(bits_per_element <= 64);
            return bits_per_element;
        } break;
        default:
            REALM_UNREACHABLE();
    }
}

inline void NodeHeader::set_elementA_size(char* header, size_t bits_per_element)
{
    // we're a bit low on bits for the Flex encoding, so we need to squeeze stuff
    REALM_ASSERT_DEBUG(get_encoding(header) == Encoding::Flex);
    REALM_ASSERT_DEBUG(bits_per_element <= 64);
    REALM_ASSERT_DEBUG(bits_per_element > 0);
    uint16_t word = (reinterpret_cast<uint16_t*>(header))[1];
    word &= ~(0b111111 << 10);
    //  we only have 6 bits, so store values in range 1-64 as 0-63
    word |= (bits_per_element - 1) << 10;
    (reinterpret_cast<uint16_t*>(header))[1] = word;
}

inline void NodeHeader::set_elementB_size(char* header, size_t bits_per_element)
{
    // we're a bit low on bits for the Flex encoding, so we need to squeeze stuff
    REALM_ASSERT_DEBUG(get_encoding(header) == Encoding::Flex);
    REALM_ASSERT_DEBUG(bits_per_element <= 64);
    REALM_ASSERT_DEBUG(bits_per_element > 0);
    uint16_t word = (reinterpret_cast<uint16_t*>(header))[3];
    word &= ~(0b111111 << 10);
    //  we only have 6 bits, so store values in range 1-64 as 0-63
    word |= (bits_per_element - 1) << 10;
    (reinterpret_cast<uint16_t*>(header))[3] = word;
}

inline size_t NodeHeader::get_elementA_size(const char* header)
{
    const auto encoding = get_encoding(header);
    REALM_ASSERT_DEBUG(encoding == Encoding::Flex);
    uint16_t word = (reinterpret_cast<const uint16_t*>(header))[1];
    auto bits_per_element = (word >> 10) & 0b111111;
    //  we only have 6 bits, so store values in range 1-64 as 0-63
    // this means that Flex cannot support element sizes of 0
    bits_per_element++;
    REALM_ASSERT_DEBUG(bits_per_element <= 64);
    REALM_ASSERT_DEBUG(bits_per_element > 0);
    return bits_per_element;
}

inline size_t NodeHeader::get_elementB_size(const char* header)
{
    REALM_ASSERT_DEBUG(get_encoding(header) == Encoding::Flex);
    uint16_t word = (reinterpret_cast<const uint16_t*>(header))[3];
    auto bits_per_element = (word >> 10) & 0b111111;
    // same as above
    bits_per_element++;
    REALM_ASSERT_DEBUG(bits_per_element <= 64);
    REALM_ASSERT_DEBUG(bits_per_element > 0);
    return bits_per_element;
}

inline size_t NodeHeader::get_num_elements(const char* header, Encoding encoding)
{
    switch (encoding) {
        case NodeHeader::Encoding::Packed:
            REALM_ASSERT_DEBUG(get_encoding(header) == Encoding::Packed);
            return (reinterpret_cast<const uint16_t*>(header))[3];
            break;
        case NodeHeader::Encoding::WTypBits:
        case NodeHeader::Encoding::WTypMult:
        case NodeHeader::Encoding::WTypIgn: {
            REALM_ASSERT_DEBUG(get_wtype_from_header(header) != wtype_Extend);
            typedef unsigned char uchar;
            const uchar* h = reinterpret_cast<const uchar*>(header);
            return (size_t(h[5]) << 16) + (size_t(h[6]) << 8) + h[7];
            break;
        }
        case NodeHeader::Encoding::Flex:
            return get_arrayB_num_elements(header);
            break;
        default:
            REALM_UNREACHABLE();
    }
}

inline void NodeHeader::set_num_elements(char* header, size_t num_elements, Encoding encoding)
{
    switch (encoding) {
        case NodeHeader::Encoding::Packed: {
            REALM_ASSERT_DEBUG(get_encoding(header) == Encoding::Packed);
            REALM_ASSERT_DEBUG(num_elements < 0x10000);
            (reinterpret_cast<uint16_t*>(header))[3] = static_cast<uint16_t>(num_elements);
        } break;
        case NodeHeader::Encoding::WTypBits: {
            REALM_ASSERT_DEBUG(get_encoding(header) == Encoding::WTypBits);
            NodeHeader::set_wtype_in_header(wtype_Bits, header);
            NodeHeader::set_size_in_header(num_elements, header);
        } break;
        case NodeHeader::Encoding::WTypMult: {
            REALM_ASSERT_DEBUG(get_encoding(header) == Encoding::WTypMult);
            NodeHeader::set_wtype_in_header(wtype_Multiply, header);
            NodeHeader::set_size_in_header(num_elements, header);
        } break;
        case NodeHeader::Encoding::WTypIgn: {
            REALM_ASSERT_DEBUG(get_encoding(header) == Encoding::WTypIgn);
            NodeHeader::set_wtype_in_header(wtype_Ignore, header);
            NodeHeader::set_size_in_header(num_elements, header);
        } break;
        default:
            REALM_UNREACHABLE();
    }
}

inline void NodeHeader::set_arrayA_num_elements(char* header, size_t num_elements)
{
    REALM_ASSERT_DEBUG(get_encoding(header) == Encoding::Flex);
    REALM_ASSERT_DEBUG(num_elements < 0b10000000000); // 10 bits
    uint16_t word = (reinterpret_cast<uint16_t*>(header))[1];
    word &= ~(0b1111111111 << 10);
    word |= num_elements << 10;
    (reinterpret_cast<uint16_t*>(header))[1] = word;
}

inline void NodeHeader::set_arrayB_num_elements(char* header, size_t num_elements)
{
    REALM_ASSERT_DEBUG(get_encoding(header) == Encoding::Flex);
    REALM_ASSERT_DEBUG(num_elements < 0b10000000000); // 10 bits
    uint16_t word = (reinterpret_cast<uint16_t*>(header))[3];
    word &= ~(0b1111111111 << 10);
    word |= num_elements << 10;
    (reinterpret_cast<uint16_t*>(header))[3] = word;
}

inline size_t NodeHeader::get_arrayA_num_elements(const char* header)
{
    const auto encoding = get_encoding(header);
    REALM_ASSERT_DEBUG(encoding == Encoding::Flex);
    const uint16_t word = (reinterpret_cast<const uint16_t*>(header))[1];
    const auto num_elements = word & 0b1111111111;
    return num_elements;
}

inline size_t NodeHeader::get_arrayB_num_elements(const char* header)
{
    REALM_ASSERT_DEBUG(get_encoding(header) == Encoding::Flex);
    const uint16_t word = (reinterpret_cast<const uint16_t*>(header))[3];
    const auto num_elements = word & 0b1111111111;
    return num_elements;
}

inline size_t NodeHeader::calc_size(size_t num_elements)
{
    return calc_byte_size(wtype_Ignore, num_elements, 0);
}

inline size_t NodeHeader::calc_size(size_t num_elements, size_t element_size, Encoding encoding)
{
    using Encoding = NodeHeader::Encoding;
    switch (encoding) {
        case Encoding::Packed:
            return NodeHeader::header_size + align_bits_to8(num_elements * element_size);
        case Encoding::WTypBits:
            return calc_byte_size(wtype_Bits, num_elements, static_cast<uint_least8_t>(element_size));
        case Encoding::WTypMult:
            return calc_byte_size(wtype_Multiply, num_elements, static_cast<uint_least8_t>(element_size));
        case Encoding::WTypIgn:
            return calc_byte_size(wtype_Ignore, num_elements, 0);
        default:
            REALM_UNREACHABLE();
    }
}

inline size_t NodeHeader::calc_size(size_t arrayA_num_elements, size_t arrayB_num_elements, size_t elementA_size,
                                    size_t elementB_size)
{
    return NodeHeader::header_size +
           align_bits_to8(arrayA_num_elements * elementA_size + arrayB_num_elements * elementB_size);
}

size_t inline NodeHeader::get_byte_size_from_header(const char* header) noexcept
{
    const auto h = header;

    const auto encoding = get_encoding(h);
    REALM_ASSERT_DEBUG(encoding >= Encoding::WTypBits && encoding <= Encoding::Flex);
    const auto size = get_num_elements(h, encoding);
    switch (encoding) {
        case Encoding::WTypBits:
        case Encoding::WTypIgn:
        case Encoding::WTypMult: {
            const auto width = get_width_from_header(header);
            return calc_byte_size(WidthType(int(encoding)), size, static_cast<uint_least8_t>(width));
        }
        case Encoding::Packed:
            return NodeHeader::header_size + align_bits_to8(size * get_element_size(h, encoding));
        case Encoding::Flex:
            return NodeHeader::header_size + align_bits_to8(get_arrayA_num_elements(h) * get_elementA_size(h) +
                                                            get_arrayB_num_elements(h) * get_elementB_size(h));
        default:
            REALM_UNREACHABLE();
    }
}


uint_least8_t inline NodeHeader::get_width_from_header(const char* header) noexcept
{
    REALM_ASSERT_DEBUG(!wtype_is_extended(header));
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return uint_least8_t((1 << (int(h[4]) & 0x07)) >> 1);
}

// A little helper:
size_t inline NodeHeader::get_size_from_header(const char* header) noexcept
{
    return get_num_elements(header, get_encoding(header));
}

} // namespace realm


namespace {

static inline void init_header(char* header, realm::NodeHeader::Encoding enc, uint8_t flags, uint8_t bits_pr_elem,
                               size_t num_elems)
{
    using Encoding = realm::NodeHeader::Encoding;
    std::fill(header, header + realm::NodeHeader::header_size, 0);
    const auto hb = reinterpret_cast<uint8_t*>(header);
    REALM_ASSERT_DEBUG(enc <= Encoding::Packed);
    if (enc < Encoding::Packed) {
        // old layout
        uint8_t wtype = static_cast<uint8_t>(enc);
        hb[4] = (flags << 5) | (wtype << 3);
        if (enc == Encoding::WTypBits)
            realm::NodeHeader::set_width_in_header(bits_pr_elem, reinterpret_cast<char*>(header));
        else
            realm::NodeHeader::set_width_in_header(bits_pr_elem >> 3, reinterpret_cast<char*>(header));
        realm::NodeHeader::set_size_in_header(num_elems, reinterpret_cast<char*>(header));
    }
    else if (enc == Encoding::Packed) {
        hb[2] = 0;
        hb[3] = static_cast<uint8_t>(bits_pr_elem);
        hb[4] = (flags << 5) | (realm::NodeHeader::wtype_Extend << 3);
        hb[5] = static_cast<uint8_t>(enc) - realm::NodeHeader::wtype_Extend;
        const auto hw = reinterpret_cast<uint16_t*>(header);
        hw[3] = static_cast<uint16_t>(num_elems);
    }
}

// init the header for flex array. Passing A bit width and size (values) and B bit width and size (indices)
static inline void init_header(char* header, realm::NodeHeader::Encoding enc, uint8_t flags, uint8_t bits_pr_elemA,
                               uint8_t bits_pr_elemB, size_t num_elemsA, size_t num_elemsB)
{
    std::fill(header, header + realm::NodeHeader::header_size, 0);
    const auto hb = reinterpret_cast<uint8_t*>(header);
    REALM_ASSERT_DEBUG(enc == realm::NodeHeader::Encoding::Flex);
    REALM_ASSERT_DEBUG(flags < 8);
    hb[4] = (flags << 5) | (realm::NodeHeader::wtype_Extend << 3);
    hb[5] =
        static_cast<int>(realm::NodeHeader::Encoding::Flex) - static_cast<int>(realm::NodeHeader::Encoding::Packed);
    const auto hw = reinterpret_cast<uint16_t*>(header);
    REALM_ASSERT_DEBUG(bits_pr_elemA > 0);
    REALM_ASSERT_DEBUG(bits_pr_elemB > 0);
    REALM_ASSERT_DEBUG(bits_pr_elemA <= 64);
    REALM_ASSERT_DEBUG(bits_pr_elemB <= 64);
    REALM_ASSERT_DEBUG(num_elemsA < 1024);
    REALM_ASSERT_DEBUG(num_elemsB < 1024);
    hw[1] = static_cast<uint16_t>(((bits_pr_elemA - 1) << 10) | num_elemsA);
    hw[3] = static_cast<uint16_t>(((bits_pr_elemB - 1) << 10) | num_elemsB);
    REALM_ASSERT_DEBUG(realm::NodeHeader::get_encoding(header) == realm::NodeHeader::Encoding::Flex);
}
} // namespace


#endif /* REALM_NODE_HEADER_HPP */
