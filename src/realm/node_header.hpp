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
        // the following use the width field (bits 0-2) of byte 4 to specify layouts
        wtype_Wide = 3,     // smaller arrays with wider elements
        wtype_LocalDir = 4, // two combined arrays, second one indexes first one
        wtype_Sparse = 5,   // sparse array controlled by a bitvector
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

    static bool get_context_flag_from_header(const char* header) noexcept
    {
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        return (int(h[4]) & 0x20) != 0;
    }

    static WidthType get_wtype_from_header(const char* header) noexcept
    {
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        int h4 = h[4];
        if ((h4 & 0x18) == 0x18) {
            return WidthType(wtype_extend + (h4 & 0x18));
        }
        else
            return WidthType((h4 & 0x18) >> 3);
    }

    // For wtype lower than wtype_extend, the element width is given by
    // bits 0-2 in byte 4 of the header. For new wtypes these bits are already used to
    // extend the wtype and the element width is instead placed in bits 0-4 of
    // byte 3. These 5 bits encode the following element widths (all widths in bits)
    // Encoding:    Sizes: (in bits)
    // 0         -> 0
    // 1-4       -> 1,2,3,4
    // 5-8       -> 5,6,7,8
    // 9-12      -> 10,12,14,16
    // 13-16     -> 20,24,28,32
    // 17-20     -> 40,48,56,64
    // 21-24     -> 80,96,112,128
    // 25-28     -> 160,192,224,256
    // 29-31     reserved
    // Some layouts may not support all sizes. Or may support them without packing them.
    static int width_encoding_to_num_bits(int encoding)
    {
        int factor = 1;
        while (encoding >= 9) {
            factor <<= 1;
            encoding -= 4;
        }
        return factor * encoding;
    }
    static int num_bits_to_width_encoding(int num_bits)
    {
        int encoding_offset = 0;
        bool bit_lost = false;
        while (num_bits >= 9) {
            if (num_bits & 1)
                bit_lost = true;
            num_bits >>= 1;
            encoding_offset += 4;
        }
        int encoding_guess = encoding_offset + num_bits;
        // if a set bit was lost, pick the next higher encoding:
        return encoding_guess + bit_lost ? 1 : 0;
    }

    static uint_least8_t get_width_from_header(const char* header) noexcept
    {
        auto wtype = get_wtype_from_header(header);
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        if (wtype < wtype_extend) {
            return uint_least8_t((1 << (int(h[4]) & 0x07)) >> 1);
        }
        else {
            // handle new layouts, simply return the encoded width, caller can
            // map it to number of bits as desired
            return h[5] & 0x1F;
        }
    }

    static size_t get_size_from_header(const char* header) noexcept
    {
        REALM_ASSERT_RELEASE(get_wtype_from_header(header) != wtype_Dynamic);
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        return (size_t(h[5]) << 16) + (size_t(h[6]) << 8) + h[7];
    }

    static size_t get_capacity_from_header(const char* header) noexcept
    {
        typedef unsigned char uchar;
        const uchar* h = reinterpret_cast<const uchar*>(header);
        return (size_t(h[0]) << 19) + (size_t(h[1]) << 11) + (h[2] << 3);
    }

    static Type get_type_from_header(const char* header) noexcept
    {
        if (get_is_inner_bptree_node_from_header(header))
            return type_InnerBptreeNode;
        if (get_hasrefs_from_header(header))
            return type_HasRefs;
        return type_Normal;
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

    static void set_wtype_in_header(WidthType value, char* header) noexcept
    {
        // Indicates how to calculate size in bytes based on width
        // 0: bits      (width/8) * size
        // 1: multiply  width * size
        // 2: ignore    1 * size
        // 3: dynamic   requirec knowledge of specific type
        typedef unsigned char uchar;
        uchar* h = reinterpret_cast<uchar*>(header);
        h[4] = uchar((int(h[4]) & ~0x18) | int(value) << 3);
    }

    static void set_width_in_header(int value, char* header) noexcept
    {
        REALM_ASSERT_RELEASE(get_wtype_from_header(header) != wtype_Dynamic);
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
        REALM_ASSERT_RELEASE(get_wtype_from_header(header) != wtype_Dynamic);
        typedef unsigned char uchar;
        uchar* h = reinterpret_cast<uchar*>(header);
        h[5] = uchar((value >> 16) & 0x000000FF);
        h[6] = uchar((value >> 8) & 0x000000FF);
        h[7] = uchar(value & 0x000000FF);
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
            case wtype_Ignore: {
                num_bytes = size;
                break;
            }
            case wtype_Dynamic: {
                REALM_ASSERT_RELEASE(wtype != wtype_Dynamic);
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
