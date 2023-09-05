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
        wtype_Bits = 0,     // width indicates how many bits every element occupies
        wtype_Multiply = 1, // width indicates how many bytes every element occupies
        wtype_Ignore = 2,   // each element is 1 byte
        // the following encodings use the width field (bits 0-2) of byte 4 to specify layouts.
        // byte 5 of the header holds one or two element sizes.
        // byte 6 and 7 holds one or two array sizes (element counts).
        //
        // the header stores enough data to a) compute the total size of a block,
        // and b) determine which part of a block may hold refs, which may need to be
        // scanned/updated for example during write to disk.
        //
        wtype_Packed = 3, // Array with better packed elements.
                          // Use for denser packaging of existing arrays up to 65535 elements
                          // bits 0-3 of byte 5 holds element size.
                          // byte 6 and 7 holds number of elements.
                          //
        wtype_AofP = 4,   // Array of pairs. Each pair has elements of two different sizes.
                          // Use for better spatial locality if you often access both members of a pair
                          // bits 0-3 of byte 5 holds size of first element in each pair
                          // bits 4-7 of byte 5 holds size of snd element in each pair
                          // byte 6 and 7 holds number of pairs
                          // if HasRefs is set, it applies to snd element.
                          //
        wtype_PofA = 5,   // Pair of arrays. Each arrays may hold elements of different sizes.
                          // Use for better spatial locality if you often access only one of the arrays,
                          // but want to represent stuff from two arrays in one memory block to
                          // save allocation and ref-translation overhead
                          // bits 0-3 of byte 5 holds size of elements in first array
                          // bits 4-7 of byte 5 holds size of elements in second array
                          // byte 6 and 7 holds number of elements in both arrays
                          // if HasRefs is set, it applies to snd array
                          //
        wtype_Flex = 6,   // Pair of arrays possibly of different length and with different sizes.
                          // Use for situations where array lengths may differ, for example
                          // if one array is used to index the other, or if one array is used
                          // for metadata which cannot hold refs. Note the number of elements
                          // is limited to 255.
                          // bits 0-3 of byte 5 holds size of elements in first array
                          // bits 4-7 of byte 5 holds size of elements in second array
                          // byte 6 holds number of elements in first array
                          // byte 7 holds number of elements in second array
                          // if HasRefs is set, it applies to snd array
        // possibly more....
    };
    static const int wtype_extend = 3; // value held in wtype field for wtypes after wtype_Wide
    static const int header_size = 8;  // Number of bytes used by header

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
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        return (int(h[4]) & 0x80) != 0;
    }

    static bool get_hasrefs_from_header(const char* header) noexcept
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

    static bool get_context_flag_from_header(const char* header) noexcept
    {
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        return (int(h[4]) & 0x20) != 0;
    }
    static void set_is_inner_bptree_node_in_header(bool value, char* header) noexcept
    {
        typedef unsigned char uchar;
        uchar* h = reinterpret_cast<uchar*>(header);
        h[4] = uchar((int(h[4]) & ~0x80) | int(value) << 7);
    }

    static void set_hasrefs_in_header(bool value, char* header) noexcept
    {
        typedef unsigned char uchar;
        uchar* h = reinterpret_cast<uchar*>(header);
        h[4] = uchar((int(h[4]) & ~0x40) | int(value) << 6);
    }

    static void set_context_flag_in_header(bool value, char* header) noexcept
    {
        typedef unsigned char uchar;
        uchar* h = reinterpret_cast<uchar*>(header);
        h[4] = uchar((int(h[4]) & ~0x20) | int(value) << 5);
    }



    // For wtype lower than wtype_extend, the element width is given by
    // bits 0-2 in byte 4 of the header and only powers of two is supported:
    //   0,1,2,4,8,16,32,64.
    // For new wtypes we support 16 different element sizes and in some
    // cases two of them -- for arrays of pairs or pairs of arrays. 
    // Element sizes of zero is not supported in the new format - pick an old format for that.
    // This is the extended encoding of the element widths (all widths in bits)
    //
    // Encoding:    Sizes:
    // 0            -> 1
    // 1            -> 2
    // 2            -> 3
    // 3            -> 4
    // 4            -> 5
    // 5            -> 6
    // 6            -> 8
    // 7            -> 10
    // 8            -> 12 (+2)
    // 9            -> 16 (+4)
    // 10           -> 20 (+4)
    // 11           -> 24 (+4)
    // 12           -> 32 (+8)
    // 13           -> 40 (+8)
    // 14           -> 52 (+12)
    // 15           -> 64 (+12)

    // Helpers for NodeHeader::WidthType:
    // handles all header formats
    static WidthType get_wtype_from_header(const char* header) noexcept
    {
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        int h4 = h[4];
        if ((h4 & 0x18) == 0x18) {
            return WidthType(wtype_extend + (h4 & 0x7));
        }
        else
            return WidthType((h4 & 0x18) >> 3);
    }

    static void set_wtype_in_header(WidthType value, char* header) noexcept
    {
        typedef unsigned char uchar;
        uchar* h = reinterpret_cast<uchar*>(header);
        auto h4 = h[4];
        if (value < wtype_extend) {
            h4 = (h4 & ~0x18) | int(value) << 3;
        }
        else {
            h4 = (h4 & ~0x1F) | (int(wtype_extend) << 3) | int(value - wtype_extend);
        }
        h[4] = h4;
    }

    // Helpers for the new width encoding (for wtype >= wtype_extend)
    static constexpr int width_enc_to_bits_table[16] = {1, 2, 3, 4, 5, 6, 8, 10, 12, 16, 20, 24, 32, 40, 52, 64};
    // from any number of bits to the encoding capable of holding them (0 should not be used)
    static constexpr int bits_to_width_enc[65] = {
        -1, 0,  1,  2,  // 0-3 bits
        3,  4,  5,  6,  // 4-7
        6,  7,  7,  8,  // 8-11
        8,  9,  9,  9,  // 12-15
        9,  10, 10, 10, // 16-19
        10, 11, 11, 11, // 20-23
        11, 12, 12, 12, // 24-27
        12, 12, 12, 12, // 28-31
        12, 13, 13, 13, // 32-35
        13, 13, 13, 13, // 36-39
        13, 14, 14, 14, // 40-43
        14, 14, 14, 14, // 44-47
        14, 14, 14, 14, // 48-51
        14, 15, 15, 15, // 52-25
        15, 15, 15, 15, // 56-59
        15, 15, 15, 15, // 60-63
        15              // 64
    };

    static int width_encoding_to_num_bits(int encoding)
    {
        return width_enc_to_bits_table[encoding];
    }

    static int num_bits_to_width_encoding(int num_bits)
    {
        return bits_to_width_enc[num_bits];
    }

    static int unsigned_to_num_bits(uint64_t value)
    {
        return log2(value);
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
        auto wtype = get_wtype_from_header(header);
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        REALM_ASSERT_RELEASE(wtype < wtype_extend);
        return uint_least8_t((1 << (int(h[4]) & 0x07)) >> 1);
    }

    static size_t get_size_from_header(const char* header) noexcept
    {
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        return (size_t(h[5]) << 16) + (size_t(h[6]) << 8) + h[7];
    }

    static void set_width_in_header(int value, char* header) noexcept
    {
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
        REALM_ASSERT_3(value, <=, max_array_size);
        typedef unsigned char uchar;
        uchar* h = reinterpret_cast<uchar*>(header);
        h[5] = uchar((value >> 16) & 0x000000FF);
        h[6] = uchar((value >> 8) & 0x000000FF);
        h[7] = uchar(value & 0x000000FF);
    }

    // Helper functions for layouts above wtype_extend:
    // Element width:
    static int get_width_A_from_header(const char* header) noexcept
    {
        auto wtype = get_wtype_from_header(header);
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        REALM_ASSERT_RELEASE(wtype >= wtype_extend);
        return width_encoding_to_num_bits(h[5] & 0xF);
    }

    // should not be used for wtype_Packed which has only one element size
    static int get_width_B_from_header(const char* header) noexcept
    {
        auto wtype = get_wtype_from_header(header);
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        REALM_ASSERT_RELEASE(wtype >= wtype_extend);
        return width_encoding_to_num_bits((h[5] >> 4) & 0xF);
    }



    // Helper functions for array sizes for layouts above wtype_extend:
    // should only be used for wtype_Flex
    static size_t get_size_A_from_header(const char* header) noexcept
    {
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        return size_t(h[6]);
    }

    // should only be used for wtype_Flex
    static size_t get_size_B_from_header(const char* header) noexcept
    {
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        return size_t(h[7]);
    }

    // shold be used for wtype_Packed, wtype_AofP, wtype_PofA
    static size_t get_size_AB_from_header(const char* header) noexcept
    {
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        return size_t(h[7] | (size_t(h[6]) << 8));
    }




    // Helpers shared for all formats:
    static size_t get_capacity_from_header(const char* header) noexcept
    {
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        return (size_t(h[0]) << 19) + (size_t(h[1]) << 11) + (h[2] << 3);
    }

    // Note: There is a copy of this function is test_alloc.cpp
    static void set_capacity_in_header(size_t value, char* header) noexcept
    {
        REALM_ASSERT_3(value, <=, (0xffffff << 3));
        typedef unsigned char uchar;
        uchar* h = reinterpret_cast<uchar*>(header);
        h[0] = uchar((value >> 19) & 0x000000FF);
        h[1] = uchar((value >> 11) & 0x000000FF);
        h[2] = uchar(value >> 3 & 0x000000FF);
    }

    static size_t get_byte_size_from_header(const char* header) noexcept
    {
        size_t size = get_size_from_header(header);
        uint_least8_t width = get_width_from_header(header);
        WidthType wtype = get_wtype_from_header(header);
        size_t num_bytes = calc_byte_size(wtype, size, width);

        return num_bytes;
    }


    // This one needs to be correct for all layouts, both old and new:
    static size_t calc_byte_size(WidthType wtype, size_t size, uint_least8_t width) noexcept
    {
        size_t num_bytes = 0;
        switch (wtype) {
            case wtype_Packed:
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
};
} // namespace realm

#endif /* REALM_NODE_HEADER_HPP */
