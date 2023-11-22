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

namespace realm {

// The header holds metadata for all allocations. It is 8 bytes.
// byte 3 indicates the type of the allocation.
//
// Up to and including Core v 13, byte 3 would always hold a value of 0x41 'A'
// when stored in the file. This value now indicates that the chunk of memory
// must be interpreted according to the methods in NodeHeader.
//
// If byte 3 has a value different from 0x41, it describes not just
// its low level encoding, but the exact C++ type used to access it.
// This allows us to create an accessor of the correct type to
// access any chunk of memory.

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
    };
    // Accessing flags.
    enum class Flags { // bit positions in flags "byte", used for masking
        Context = 1,
        HasRefs = 2,
        InnerBPTree = 4,
        // additional flags can be supported by new layouts, but old layout (kind=='A') is full
    };

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
    static bool get_is_inner_bptree_node_from_header(const char* header) noexcept
    {
        return get_flags((uint64_t*)header) & (int)Flags::InnerBPTree;

        //        REALM_ASSERT(get_kind((uint64_t*)header) == 'A');
        //        typedef unsigned char uchar;
        //        const uchar* h = reinterpret_cast<const uchar*>(header);
        //        return (int(h[4]) & 0x80) != 0;
    }

    static bool get_hasrefs_from_header(const char* header) noexcept
    {
        return get_flags((uint64_t*)header) & (int)Flags::HasRefs;

        // REALM_ASSERT(get_kind((uint64_t*)header) == 'A');
        // typedef unsigned char uchar;
        // const uchar* h = reinterpret_cast<const uchar*>(header);
        // return (int(h[4]) & 0x40) != 0;
    }

    static Type get_type_from_header(const char* header) noexcept
    {
        REALM_ASSERT(get_kind((uint64_t*)header) == 'A');
        if (get_is_inner_bptree_node_from_header(header))
            return type_InnerBptreeNode;
        if (get_hasrefs_from_header(header))
            return type_HasRefs;
        return type_Normal;
    }

    static bool get_context_flag_from_header(const char* header) noexcept
    {
        return get_flags((uint64_t*)header) & (int)Flags::Context;

        // REALM_ASSERT(get_kind((uint64_t*)header) == 'A');
        // typedef unsigned char uchar;
        // const uchar* h = reinterpret_cast<const uchar*>(header);
        // return (int(h[4]) & 0x20) != 0;
    }
    static void set_is_inner_bptree_node_in_header(bool value, char* header) noexcept
    {
        uint64_t* h = (uint64_t*)header;
        set_flags(h, (get_flags(h) & ~(int)Flags::InnerBPTree) | (value ? (int)Flags::InnerBPTree : 0));

        // REALM_ASSERT(get_kind((uint64_t*)header) == 'A');
        // typedef unsigned char uchar;
        // uchar* h = reinterpret_cast<uchar*>(header);
        // h[4] = uchar((int(h[4]) & ~0x80) | int(value) << 7);
    }

    static void set_hasrefs_in_header(bool value, char* header) noexcept
    {
        uint64_t* h = (uint64_t*)header;
        set_flags(h, (get_flags(h) & ~(int)Flags::HasRefs) | (value ? (int)Flags::HasRefs : 0));

        // REALM_ASSERT(get_kind((uint64_t*)header) == 'A');
        // typedef unsigned char uchar;
        // uchar* h = reinterpret_cast<uchar*>(header);
        // h[4] = uchar((int(h[4]) & ~0x40) | int(value) << 6);
    }

    static void set_context_flag_in_header(bool value, char* header) noexcept
    {
        uint64_t* h = (uint64_t*)header;
        set_flags(h, (get_flags(h) & ~(int)Flags::Context) | (value ? (int)Flags::Context : 0));

        // REALM_ASSERT(get_kind((uint64_t*)header) == 'A');
        // typedef unsigned char uchar;
        // uchar* h = reinterpret_cast<uchar*>(header);
        // h[4] = uchar((int(h[4]) & ~0x20) | int(value) << 5);
    }

    // Helpers for NodeHeader::WidthType:
    // handles all header formats
    static WidthType get_wtype_from_header(const char* header) noexcept
    {
        char kind = (char)get_kind((uint64_t*)header);
        REALM_ASSERT(kind == 'A');
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        int h4 = h[4];
        return WidthType((h4 & 0x18) >> 3);
    }

    static void set_wtype_in_header(WidthType value, char* header) noexcept
    {
        REALM_ASSERT(get_kind((uint64_t*)header) == 'A');
        typedef unsigned char uchar;
        uchar* h = reinterpret_cast<uchar*>(header);
        auto h4 = h[4];
        h4 = (h4 & ~0x18) | int(value) << 3;
        h[4] = h4;
    }

    static int unsigned_to_num_bits(uint64_t value)
    {
        return 1 + log2(value);
    }

    static int signed_to_num_bits(int64_t value)
    {
        if (value >= 0)
            return 1 + unsigned_to_num_bits(value);
        else
            return 1 + unsigned_to_num_bits(~value); // <-- is this correct????
    }


    // Helper functions for old layouts only:
    // Handling width and sizes:
    static uint_least8_t get_width_from_header(const char* header) noexcept
    {
        auto kind = (char)get_kind((uint64_t*)header);
        REALM_ASSERT(kind == 'A');
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        return uint_least8_t((1 << (int(h[4]) & 0x07)) >> 1);
    }

    static size_t get_size_from_header(const char* header) noexcept
    {
        REALM_ASSERT(get_kind((uint64_t*)header) == 'A');
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        return (size_t(h[5]) << 16) + (size_t(h[6]) << 8) + h[7];
    }

    static void set_width_in_header(size_t value, char* header) noexcept
    {
        REALM_ASSERT(get_kind((uint64_t*)header) == 'A');
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

    static void set_size_in_header(size_t value, char* header) noexcept
    {
        REALM_ASSERT(get_kind((uint64_t*)header) == 'A');
        REALM_ASSERT_3(value, <=, max_array_size);
        typedef unsigned char uchar;
        uchar* h = reinterpret_cast<uchar*>(header);
        h[5] = uchar((value >> 16) & 0x000000FF);
        h[6] = uchar((value >> 8) & 0x000000FF);
        h[7] = uchar(value & 0x000000FF);
    }


    static size_t get_capacity_from_header(const char* header) noexcept
    {
        if (get_kind((uint64_t*)header) == 'A') {
            typedef unsigned char uchar;
            const uchar* h = reinterpret_cast<const uchar*>(header);
            return (size_t(h[0]) << 19) + (size_t(h[1]) << 11) + (h[2] << 3);
        }
        else {
            /*I don't see this set while debugging*/
            // NOTE this is set inside array_flex::setup_header_in_flex_format for B arrays
            return ((uint16_t*)header)[0] << 3;
        }
    }

    // Note: There is a (no longer a correct) copy of this function is test_alloc.cpp
    static void set_capacity_in_header(size_t value, char* header) noexcept
    {
        if (get_kind((uint64_t*)header) == 'A') {
            REALM_ASSERT_3(value, <=, (0xffffff << 3));
            typedef unsigned char uchar;
            uchar* h = reinterpret_cast<uchar*>(header);
            h[0] = uchar((value >> 19) & 0x000000FF);
            h[1] = uchar((value >> 11) & 0x000000FF);
            h[2] = uchar(value >> 3 & 0x000000FF);
        }
        else {
            REALM_ASSERT(value < (65536 << 3));
            REALM_ASSERT((value & 0x7) == 0);
            // this could be a problem, it assumes that the last 3 less significant bits are 0.
            // For flex arrays this could not be true if we were to set capacity lower than 128
            ((uint16_t*)header)[0] = value >> 3;
        }
    }

    // helper converting a number of bits into bytes and aligning to 8 byte boundary
    static inline size_t align_bits_to8(size_t n)
    {
        n = (n + 7) >> 3;
        return (n + 7) & ~size_t(7);
    }


    static size_t get_byte_size_from_header(const char* header) noexcept;

    static size_t calc_byte_size(WidthType wtype, size_t size, uint_least8_t width) noexcept
    {
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

        // Ensure 8-byte alignment
        num_bytes = (num_bytes + 7) & ~size_t(7);

        num_bytes += header_size;

        return num_bytes;
    }

    // The 'kind' field provides runtime type information.
    // it allows us to select the proper class for handling the block.
    // For example, this could be the Array class.
    // A class may use one or multiple different encodings, depending
    // on the data it stores.
    // A value of 0x41 ('A') represents the "old" (core 13 or earlier)
    // set of Array_xxxx classes
    static uint8_t get_kind(const uint64_t* header)
    {
        return ((uint8_t*)header)[3];
    }
    static void set_kind(uint64_t* header, uint8_t kind)
    {
        ((uint8_t*)header)[3] = kind;
    }

    // Access to different header formats is done through specializations of a set
    // of access functions. This allows for defining ONLY the abilities which makes
    // sense for each different header encoding. For example: headers for a single
    // array support a 'set_element_size()', while headers for arrays of pairs or
    // pairs of arrays instead support 'set_elementA_size()' and 'set_elementB_size()'.
    //
    // This approach also allows for zero overhead selection between the different
    // header encodings.
    enum class Encoding { Packed, AofP, PofA, Flex, WTypBits, WTypMult, WTypIgn };
    static Encoding get_encoding(const uint64_t* header)
    {
        if (get_kind(header) == 0x41) {
            switch (get_wtype_from_header((const char*)header)) {
                case wtype_Bits:
                    return Encoding::WTypBits;
                case wtype_Multiply:
                    return Encoding::WTypMult;
                case wtype_Ignore:
                    return Encoding::WTypIgn;
                default:
                    REALM_ASSERT(false && "Undefined header encoding");
            }
        }
        else {
            auto h = (const uint8_t*)header;
            const auto v = (h[2] & 0b1111);
            switch (v) {
                case 0:
                    return Encoding::Packed;
                case 1:
                    return Encoding::AofP;
                case 2:
                    return Encoding::PofA;
                case 3:
                    return Encoding::Flex;
                default:
                    REALM_ASSERT(false && "Undefined header encoding");
            }
        }
    }
    static void set_encoding(uint64_t* header, Encoding enc)
    {
        switch (enc) {
            case Encoding::AofP: {
                REALM_ASSERT(get_kind(header) != 0x41);
                auto h = (uint8_t*)header;
                h[2] = (h[2] & 0b11110000) | 1;
                break;
            }
            case Encoding::PofA: {
                REALM_ASSERT(get_kind(header) != 0x41);
                auto h = (uint8_t*)header;
                h[2] = (h[2] & 0b11110000) | 2;
                break;
            }
            case Encoding::Packed: {
                REALM_ASSERT(get_kind(header) != 0x41);
                auto h = (uint8_t*)header;
                h[2] = (h[2] & 0b11110000) | 0;
                break;
            }
            case Encoding::Flex: {
                REALM_ASSERT(get_kind(header) != 0x41);
                auto h = (uint8_t*)header;
                h[2] = (h[2] & 0b11110000) | 3;
                REALM_ASSERT(get_encoding(header) == Encoding::Flex);
                break;
            }
            case Encoding::WTypBits: {
                REALM_ASSERT(get_kind(header) == 0x41);
                set_wtype_in_header(wtype_Bits, (char*)header);
                break;
            }
            case Encoding::WTypMult: {
                REALM_ASSERT(get_kind(header) == 0x41);
                set_wtype_in_header(wtype_Multiply, (char*)header);
                break;
            }
            case Encoding::WTypIgn: {
                REALM_ASSERT(get_kind(header) == 0x41);
                set_wtype_in_header(wtype_Ignore, (char*)header);
                break;
            }
        }
    }
    // * Packed: tightly packed array (any element size <= 64)
    // * WTypBits: less tightly packed. Correspond to wtype_Bits
    // * WTypMult: less tightly packed. Correspond to wtype_Multiply
    // * WTypIgn: single byte elements. Correspond to wtype_Ignore
    // encodings with more flexibility but lower number of elements:
    // * AofP: Array of pairs (2 element sizes, 1 element count)
    // * PofA: Pair of arrays (2 elememt sizes, 1 element count)
    //   Choose between them according to spatial locality
    // Encodings with even more flexibility with even lower number of elements
    // * Flex: Pair of arrays (like PofA) but allowing different element count
    //
    // Encodings:     bytes:
    // name:       |  b0   |  b1   |  b2   |  b3   |  b4   |  b5   |  b6   |  b7   |
    // Packed      |  cap/chksum   | flgs  | kind  | bits pr elm   |  num elmnts   |
    // AofP        |  cap/chksum   | flgs  | kind  | Abpe  | BBpe  |  num elmnts   |
    // PofA        |  cap/chksum   | flgs  | kind  | Abpe  | BBpe  |  num elmnts   |
    // Flex        |  cap/chksum   | flgs  | kind  | Abpe - BBpe -  Ane   -  Bne   |
    // oldies      |     cap/chksum        | 0x41  | lots  |      num elements     |
    //
    // legend: cap = capacity, chksum = checksum, flgs = various flags + encoding,
    //         elm = elements, Abpe = A bits per element, Bbpe = B bits per element
    //         Ane = A num elements, Bne = B num elements,
    //         lots = flags, wtype and width for old formats
    //
    static void init_header(uint64_t* header, uint8_t kind, Encoding enc, uint8_t flags, size_t bits_pr_elem,
                            size_t num_elems)
    {
        std::fill(header, header + header_size, 0);
        auto hb = (uint8_t*)header;
        hb[3] = kind;
        if (kind == 0x41) {
            uint8_t wtype;
            if (enc == Encoding::WTypBits)
                wtype = wtype_Bits;
            else if (enc == Encoding::WTypMult)
                wtype = wtype_Multiply;
            else if (enc == Encoding::WTypIgn)
                wtype = wtype_Ignore;
            else
                REALM_ASSERT(false && "Illegal header encoding for legacy kind of header");
            hb[4] = (flags << 5) | (wtype << 3);
            if (enc == Encoding::WTypBits)
                set_width_in_header(bits_pr_elem, (char*)header);
            else
                set_width_in_header(bits_pr_elem >> 3, (char*)header);
            set_size_in_header(num_elems, (char*)header);
        }
        else {
            if (enc == Encoding::Packed) {
                hb[2] = flags << 4;
                auto hh = (uint16_t*)header;
                hh[2] = bits_pr_elem;
                hh[3] = num_elems;
            }
            else {
                REALM_ASSERT(false && "Illegal header encoding for chosen kind of header");
            }
        }
        hb[3] = kind;
    }
    static void init_header(uint64_t* header, uint8_t kind, Encoding enc, uint8_t flags, size_t bits_pr_elemA,
                            size_t bits_pr_elemB, size_t num_elems)
    {
        std::fill(header, header + header_size, 0);
        auto hb = (uint8_t*)header;
        if (kind == 0x41)
            REALM_ASSERT(false && "Illegal init args for legacy header");
        if (enc == Encoding::AofP) {
            hb[2] = (flags << 4) | 1;
        }
        else if (enc == Encoding::PofA) {
            hb[2] = (flags << 4) | 2;
        }
        else
            REALM_ASSERT(false && "Illegal header encoding for chosen kind of header");
        hb[4] = bits_pr_elemA;
        hb[5] = bits_pr_elemB;
        auto hh = (uint16_t*)header;
        hh[3] = num_elems;
        hb[3] = kind;
    }
    static void init_header(uint64_t* header, uint8_t kind, Encoding enc, uint8_t flags, size_t bits_pr_elemA,
                            size_t bits_pr_elemB, size_t num_elemsA, size_t num_elemsB)
    {
        std::fill(header, header + header_size, 0);
        auto hb = (uint8_t*)header;
        if (kind == 0x41)
            REALM_ASSERT(false && "Illegal init args for legacy header");
        if (enc != Encoding::Flex) {
            REALM_ASSERT(false && "Illegal header encoding for chosen kind of header");
        }
        auto hw = (uint32_t*)header;
        hw[1] = (uint32_t)((bits_pr_elemA << 26) | (bits_pr_elemB << 20) | (num_elemsA << 10) | num_elemsB);
        // flags in the first nibble
        // encoding in the second nibble (3 means flex)
        hb[2] = (hb[2] & 0b11110000) | (flags << 4) | 3;
        hb[3] = kind;
    }

    // Setting element size for encodings with a single element size:
    template <Encoding>
    static void inline set_element_size(uint64_t* header, size_t bits_per_element);
    // Getting element size for encodings with a single element size:
    template <Encoding>
    static inline size_t get_element_size(uint64_t* header);
    // Setting element sizes for encodings with two element sizes (called A and B)
    template <Encoding>
    static inline void set_elementA_size(uint64_t* header, size_t bits_per_element);
    // Setting element sizes for encodings with two element sizes (called A and B)
    template <Encoding>
    static inline void set_elementB_size(uint64_t* header, size_t bits_per_element);
    // Getting element sizes for encodings with two element sizes (called A and B)
    template <Encoding>
    static inline size_t get_elementA_size(uint64_t* header);
    // Getting element sizes for encodings with two element sizes (called A and B)
    template <Encoding>
    static inline size_t get_elementB_size(uint64_t* header);
    // Setting the number of elements in the array(s). All encodings except Flex have one number of elements.
    template <Encoding>
    static inline void set_num_elements(uint64_t* header, size_t num_elements);
    // For the encodings with two size specifications - currently only the Flex encoding
    template <Encoding>
    static inline void set_arrayA_num_elements(uint64_t* header, size_t num_elements);
    template <Encoding>
    static inline void set_arrayB_num_elements(uint64_t* header, size_t num_elements);
    // Getting the number of elements in the array(s). All encodings except Flex have one number of elements.
    template <Encoding>
    static inline size_t get_num_elements(uint64_t* header);
    template <Encoding>
    static inline size_t get_arrayA_num_elements(uint64_t* header);
    template <Encoding>
    static inline size_t get_arrayB_num_elements(uint64_t* header);
    // Compute required size in bytes - multiple forms depending on encoding
    template <Encoding>
    inline size_t calc_size(size_t num_elements);
    template <Encoding>
    inline size_t calc_size(size_t num_elements, size_t element_size);
    template <Encoding>
    inline size_t calc_size(size_t num_elements, size_t elementA_size, size_t elementB_size);
    template <Encoding>
    inline size_t calc_size(size_t arrayA_num_elements, size_t arrayB_num_elements, size_t elementA_size,
                            size_t elementB_size);

    static inline void set_flags(uint64_t* header, uint8_t flags)
    {
        if (get_kind(header) == 'A') {
            REALM_ASSERT(flags <= 7);
            auto h = (uint8_t*)header;
            h[4] = (h[4] & 0b00011111) | flags << 5;
        }
        else {
            auto h = (uint8_t*)header;
            h[2] = (h[2] & 0b00001111) | flags << 4;
        }
    }
    static inline uint8_t get_flags(uint64_t* header)
    {
        if (get_kind(header) == 'A') {
            auto h = (uint8_t*)header;
            return h[4] >> 5;
        }
        else {
            auto h = (uint8_t*)header;
            return h[2] >> 4;
        }
    }
};


template <>
void inline NodeHeader::set_element_size<NodeHeader::Encoding::Packed>(uint64_t* header, size_t bits_per_element)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::Packed);
    REALM_ASSERT(bits_per_element <= 64);
    ((uint16_t*)header)[2] = bits_per_element;
}
template <>
void inline NodeHeader::set_element_size<NodeHeader::Encoding::WTypBits>(uint64_t* header, size_t bits_per_element)
{
    REALM_ASSERT(get_kind(header) == 'A');
    REALM_ASSERT(bits_per_element <= 64);
    // TODO: Only powers of two allowed
    // TODO: Optimize
    NodeHeader::set_wtype_in_header(wtype_Bits, (char*)header);
    NodeHeader::set_width_in_header(bits_per_element, (char*)header);
}
template <>
void inline NodeHeader::set_element_size<NodeHeader::Encoding::WTypMult>(uint64_t* header, size_t bits_per_element)
{
    REALM_ASSERT(get_kind(header) == 'A');
    REALM_ASSERT(bits_per_element <= 64);
    REALM_ASSERT((bits_per_element & 0x7) == 0);
    // TODO: Only powers of two allowed
    // TODO: Optimize
    NodeHeader::set_wtype_in_header(wtype_Multiply, (char*)header);
    NodeHeader::set_width_in_header(bits_per_element >> 3, (char*)header);
}


template <>
inline size_t NodeHeader::get_element_size<NodeHeader::Encoding::Packed>(uint64_t* header)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::Packed);
    auto bits_per_element = ((uint16_t*)header)[2];
    REALM_ASSERT(bits_per_element <= 64);
    return bits_per_element;
}
template <>
inline size_t NodeHeader::get_element_size<NodeHeader::Encoding::WTypBits>(uint64_t* header)
{
    REALM_ASSERT(get_kind(header) == 'A');
    auto bits_per_element = NodeHeader::get_width_from_header((char*)header);
    REALM_ASSERT(bits_per_element <= 64);
    return bits_per_element;
}
template <>
inline size_t NodeHeader::get_element_size<NodeHeader::Encoding::WTypMult>(uint64_t* header)
{
    REALM_ASSERT(get_kind(header) == 'A');
    auto bits_per_element = NodeHeader::get_width_from_header((char*)header) << 3;
    REALM_ASSERT(bits_per_element <= 64);
    return bits_per_element;
}

template <>
inline void NodeHeader::set_elementA_size<NodeHeader::Encoding::AofP>(uint64_t* header, size_t bits_per_element)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::AofP);
    REALM_ASSERT(bits_per_element <= 64);
    ((uint8_t*)header)[4] = bits_per_element;
}
template <>
inline void NodeHeader::set_elementA_size<NodeHeader::Encoding::PofA>(uint64_t* header, size_t bits_per_element)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::PofA);
    REALM_ASSERT(bits_per_element <= 64);
    ((uint8_t*)header)[4] = bits_per_element;
}
template <>
inline void NodeHeader::set_elementA_size<NodeHeader::Encoding::Flex>(uint64_t* header, size_t bits_per_element)
{
    // we're a bit low on bits for the Flex encoding, so we need to squeeze stuff
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::Flex);
    REALM_ASSERT(bits_per_element <= 64);
    REALM_ASSERT(bits_per_element > 0);
    uint32_t word = ((uint32_t*)header)[1];
    word &= ~(0b111111 << 26);
    word |= (bits_per_element - 1) << 26;
    ((uint32_t*)header)[1] = word;
}


template <>
inline void NodeHeader::set_elementB_size<NodeHeader::Encoding::AofP>(uint64_t* header, size_t bits_per_element)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::AofP);
    REALM_ASSERT(bits_per_element <= 64);
    ((uint8_t*)header)[5] = bits_per_element;
}
template <>
inline void NodeHeader::set_elementB_size<NodeHeader::Encoding::PofA>(uint64_t* header, size_t bits_per_element)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::PofA);
    REALM_ASSERT(bits_per_element <= 64);
    ((uint8_t*)header)[5] = bits_per_element;
}
template <>
inline void NodeHeader::set_elementB_size<NodeHeader::Encoding::Flex>(uint64_t* header, size_t bits_per_element)
{
    // we're a bit low on bits for the Flex encoding, so we need to squeeze stuff
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::Flex);
    REALM_ASSERT(bits_per_element <= 64);
    REALM_ASSERT(bits_per_element > 0);
    uint32_t word = ((uint32_t*)header)[1];
    word &= ~(0b111111 << 20);
    word |= (bits_per_element - 1) << 20;
    ((uint32_t*)header)[1] = word;
}


template <>
inline size_t NodeHeader::get_elementA_size<NodeHeader::Encoding::AofP>(uint64_t* header)
{
    REALM_ASSERT(get_kind(header) != 'A');
    auto encoding = get_encoding(header);
    REALM_ASSERT(encoding == Encoding::AofP);
    auto bits_per_element = ((uint8_t*)header)[4];
    REALM_ASSERT(bits_per_element <= 64);
    return bits_per_element;
}
template <>
inline size_t NodeHeader::get_elementA_size<NodeHeader::Encoding::PofA>(uint64_t* header)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::PofA);
    auto bits_per_element = ((uint8_t*)header)[4];
    REALM_ASSERT(bits_per_element <= 64);
    return bits_per_element;
}
template <>
inline size_t NodeHeader::get_elementA_size<NodeHeader::Encoding::Flex>(uint64_t* header)
{
    REALM_ASSERT(get_kind(header) != 'A');
    const auto encoding = get_encoding(header);
    REALM_ASSERT(encoding == Encoding::Flex);
    uint32_t word = ((uint32_t*)header)[1];
    auto bits_per_element = (word >> 26) & 0b111111;
    // TODO: why there is an increment here?? if I need 1 bit, I should not read 2 from the header.
    //       I suspect this is done for supporting the sign bit... but the sign should be an implementation detail.
    // bits_per_element++;
    REALM_ASSERT(bits_per_element <= 64);
    REALM_ASSERT(bits_per_element > 0);
    return bits_per_element;
}

template <>
inline size_t NodeHeader::get_elementB_size<NodeHeader::Encoding::AofP>(uint64_t* header)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::AofP);
    auto bits_per_element = ((uint8_t*)header)[5];
    REALM_ASSERT(bits_per_element <= 64);
    return bits_per_element;
}
template <>
inline size_t NodeHeader::get_elementB_size<NodeHeader::Encoding::PofA>(uint64_t* header)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::PofA);
    auto bits_per_element = ((uint8_t*)header)[5];
    REALM_ASSERT(bits_per_element <= 64);
    return bits_per_element;
}
template <>
inline size_t NodeHeader::get_elementB_size<NodeHeader::Encoding::Flex>(uint64_t* header)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::Flex);
    uint32_t word = ((uint32_t*)header)[1];
    auto bits_per_element = (word >> 20) & 0b111111;
    // same as above
    // bits_per_element++;
    REALM_ASSERT(bits_per_element <= 64);
    REALM_ASSERT(bits_per_element > 0);
    return bits_per_element;
}

template <>
inline void NodeHeader::set_num_elements<NodeHeader::Encoding::Packed>(uint64_t* header, size_t num_elements)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::Packed);
    REALM_ASSERT(num_elements < 0x10000);
    ((uint16_t*)header)[3] = num_elements;
}
template <>
inline void NodeHeader::set_num_elements<NodeHeader::Encoding::WTypBits>(uint64_t* header, size_t num_elements)
{
    // TODO optimize
    REALM_ASSERT(get_kind(header) == 'A');
    NodeHeader::set_wtype_in_header(wtype_Bits, (char*)header);
    NodeHeader::set_size_in_header(num_elements, (char*)header);
}
template <>
inline void NodeHeader::set_num_elements<NodeHeader::Encoding::WTypMult>(uint64_t* header, size_t num_elements)
{
    // TODO optimize
    REALM_ASSERT(get_kind(header) == 'A');
    NodeHeader::set_wtype_in_header(wtype_Multiply, (char*)header);
    NodeHeader::set_size_in_header(num_elements, (char*)header);
}
template <>
inline void NodeHeader::set_num_elements<NodeHeader::Encoding::WTypIgn>(uint64_t* header, size_t num_elements)
{
    // TODO optimize
    REALM_ASSERT(get_kind(header) == 'A');
    NodeHeader::set_wtype_in_header(wtype_Ignore, (char*)header);
    NodeHeader::set_size_in_header(num_elements, (char*)header);
}
template <>
inline void NodeHeader::set_num_elements<NodeHeader::Encoding::AofP>(uint64_t* header, size_t num_elements)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::AofP);
    REALM_ASSERT(num_elements < 0x10000);
    ((uint16_t*)header)[3] = num_elements;
}
template <>
inline void NodeHeader::set_num_elements<NodeHeader::Encoding::PofA>(uint64_t* header, size_t num_elements)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::PofA);
    REALM_ASSERT(num_elements < 0x10000);
    ((uint16_t*)header)[3] = num_elements;
}


template <>
inline void NodeHeader::set_arrayA_num_elements<NodeHeader::Encoding::Flex>(uint64_t* header, size_t num_elements)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::Flex);
    REALM_ASSERT(num_elements < 0b10000000000); // 10 bits
    uint32_t word = ((uint32_t*)header)[1];
    word &= ~(0b1111111111 << 10);
    word |= num_elements << 10;
    ((uint32_t*)header)[1] = word;
}
template <>
inline void NodeHeader::set_arrayB_num_elements<NodeHeader::Encoding::Flex>(uint64_t* header, size_t num_elements)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::Flex);
    REALM_ASSERT(num_elements < 0b10000000000); // 10 bits
    uint32_t word = ((uint32_t*)header)[1];
    word &= ~(0b1111111111);
    word |= num_elements;
    ((uint32_t*)header)[1] = word;
}

template <>
inline size_t NodeHeader::get_num_elements<NodeHeader::Encoding::Packed>(uint64_t* header)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::Packed);
    return ((uint16_t*)header)[3];
}
template <>
inline size_t NodeHeader::get_num_elements<NodeHeader::Encoding::AofP>(uint64_t* header)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::AofP);
    return ((uint16_t*)header)[3];
}
template <>
inline size_t NodeHeader::get_num_elements<NodeHeader::Encoding::PofA>(uint64_t* header)
{
    REALM_ASSERT(get_kind(header) != 'A');
    REALM_ASSERT(get_encoding(header) == Encoding::PofA);
    return ((uint16_t*)header)[3];
}
template <>
inline size_t NodeHeader::get_num_elements<NodeHeader::Encoding::WTypBits>(uint64_t* header)
{
    REALM_ASSERT(get_kind(header) == 'A');
    return NodeHeader::get_size_from_header((const char*)header);
}
template <>
inline size_t NodeHeader::get_num_elements<NodeHeader::Encoding::WTypMult>(uint64_t* header)
{
    REALM_ASSERT(get_kind(header) == 'A');
    return NodeHeader::get_size_from_header((const char*)header);
}
template <>
inline size_t NodeHeader::get_num_elements<NodeHeader::Encoding::WTypIgn>(uint64_t* header)
{
    REALM_ASSERT(get_kind(header) == 'A');
    return NodeHeader::get_size_from_header((const char*)header);
}

template <>
inline size_t NodeHeader::get_arrayA_num_elements<NodeHeader::Encoding::Flex>(uint64_t* header)
{
    const auto kind = (char)get_kind(header);
    REALM_ASSERT(kind == 'B');
    const auto encoding = get_encoding(header);
    REALM_ASSERT(encoding == Encoding::Flex);
    uint32_t word = ((uint32_t*)header)[1];
    auto num_elements = (word >> 10) & 0b1111111111;
    return num_elements;
}

template <>
inline size_t NodeHeader::get_arrayB_num_elements<NodeHeader::Encoding::Flex>(uint64_t* header)
{
    REALM_ASSERT(get_kind(header) == 'B');
    REALM_ASSERT(get_encoding(header) == Encoding::Flex);
    uint32_t word = ((uint32_t*)header)[1];
    auto num_elements = word & 0b1111111111;
    return num_elements;
}


template <>
inline size_t NodeHeader::calc_size<NodeHeader::Encoding::Packed>(size_t num_elements, size_t element_size)
{
    return NodeHeader::header_size + align_bits_to8(num_elements * element_size);
}
template <>
inline size_t NodeHeader::calc_size<NodeHeader::Encoding::WTypBits>(size_t num_elements, size_t element_size)
{
    return calc_byte_size(wtype_Bits, num_elements, element_size);
    // return NodeHeader::header_size + align_bits_to8(num_elements * element_size);
}
template <>
inline size_t NodeHeader::calc_size<NodeHeader::Encoding::WTypMult>(size_t num_elements, size_t element_size)
{
    return calc_byte_size(wtype_Multiply, num_elements, element_size);
}


template <>
inline size_t NodeHeader::calc_size<NodeHeader::Encoding::WTypIgn>(size_t num_elements)
{
    return calc_byte_size(wtype_Ignore, num_elements, 0);
}
template <>
inline size_t NodeHeader::calc_size<NodeHeader::Encoding::AofP>(size_t num_elements, size_t elementA_size,
                                                                size_t elementB_size)
{
    return NodeHeader::header_size + align_bits_to8(num_elements * (elementA_size + elementB_size));
}
template <>
inline size_t NodeHeader::calc_size<NodeHeader::Encoding::PofA>(size_t num_elements, size_t elementA_size,
                                                                size_t elementB_size)
{
    return NodeHeader::header_size + align_bits_to8(num_elements * (elementA_size + elementB_size));
}
template <>
inline size_t NodeHeader::calc_size<NodeHeader::Encoding::Flex>(size_t arrayA_num_elements,
                                                                size_t arrayB_num_elements, size_t elementA_size,
                                                                size_t elementB_size)
{
    return NodeHeader::header_size +
           align_bits_to8(arrayA_num_elements * elementA_size + arrayB_num_elements * elementB_size);
}

size_t inline NodeHeader::get_byte_size_from_header(const char* header) noexcept
{
    auto h = (uint64_t*)header;
    auto kind = get_kind(h);
    if (kind == 'A') {
        auto encoding = get_encoding(h);
        REALM_ASSERT(encoding != Encoding::Flex);
        WidthType wtype = get_wtype_from_header(header);
        size_t width;
        size_t size;
        width = get_width_from_header(header);
        size = get_size_from_header(header);
        return calc_byte_size(wtype, size, width);
    }
    else {
        auto encoding = get_encoding(h);
        REALM_ASSERT(encoding == NodeHeader::Encoding::Flex); // this is the only encoding supported right now.
        switch (encoding) {
            case Encoding::Packed:
                return NodeHeader::header_size + align_bits_to8(get_num_elements<NodeHeader::Encoding::Packed>(h) *
                                                                get_element_size<NodeHeader::Encoding::Packed>(h));
            case Encoding::AofP:
                return NodeHeader::header_size + align_bits_to8(get_num_elements<NodeHeader::Encoding::AofP>(h) *
                                                                (get_elementA_size<NodeHeader::Encoding::AofP>(h) +
                                                                 get_elementB_size<NodeHeader::Encoding::AofP>(h)));
            case Encoding::PofA:
                return NodeHeader::header_size + align_bits_to8(get_num_elements<NodeHeader::Encoding::PofA>(h) *
                                                                (get_elementA_size<NodeHeader::Encoding::PofA>(h) +
                                                                 get_elementB_size<NodeHeader::Encoding::PofA>(h)));
            case Encoding::Flex:
                return NodeHeader::header_size +
                       align_bits_to8(get_arrayA_num_elements<NodeHeader::Encoding::Flex>(h) *
                                          get_elementA_size<NodeHeader::Encoding::Flex>(h) +
                                      get_arrayB_num_elements<NodeHeader::Encoding::Flex>(h) *
                                          get_elementB_size<NodeHeader::Encoding::Flex>(h));
            default:
                REALM_ASSERT(false && "unknown encoding");
        }
    }
}


} // namespace realm

#endif /* REALM_NODE_HEADER_HPP */
