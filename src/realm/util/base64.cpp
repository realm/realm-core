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

#include <realm/util/base64.hpp>

#include <limits>
#include <vector>

#if defined(_MSC_VER)
#  define REALM_RESTRICT __restrict
#elif defined(__GNUC__) || defined(__clang__)
#  define REALM_RESTRICT __restrict__
#else
#  define REALM_RESTRICT
#endif


namespace {

static const char g_base64_encoding_chars[] = {
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
    'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
    'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
    'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
    'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
    'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
    'w', 'x', 'y', 'z', '0', '1', '2', '3',
    '4', '5', '6', '7', '8', '9', '+', '/'
};

// ASCII table -- 16 entries per row
static const unsigned char g_base64_chars[] = {
    66, 66, 66, 66, 66, 66, 66, 66, 66, 65, 65, 66, 66, 66, 66, 66,
    66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
    65, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 62, 66, 62, 66, 63,
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 66, 66, 66, 64, 66, 66,
    66,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 66, 66, 66, 66, 63,
    66, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 66, 66, 66, 66, 66,
    66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
    66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
    66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
    66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
    66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
    66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
    66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
    66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66
};

inline unsigned int index_of_base64_byte(unsigned char c)
{
    return g_base64_chars[static_cast<unsigned int>(c)];
}

} // unnamed namespace


namespace realm {
namespace util {

size_t base64_encode(const char *in_buffer, size_t in_buffer_size, char* out_buffer, size_t out_buffer_size) noexcept
{
    REALM_ASSERT_EX(in_buffer_size < std::numeric_limits<size_t>::max() - 2, in_buffer_size);
    REALM_ASSERT_EX(in_buffer_size < 3 * (std::numeric_limits<size_t>::max() / 4) - 2, in_buffer_size);
    size_t encoded_size = 4 * ((in_buffer_size + 2) / 3);
    REALM_ASSERT_EX(out_buffer_size >= encoded_size, out_buffer_size, encoded_size);
    static_cast<void>(out_buffer_size);

    for (size_t i = 0, j = 0; i < in_buffer_size;) {
        uint32_t octet_a = i < in_buffer_size ? static_cast<unsigned char>(in_buffer[i++]) : 0;
        uint32_t octet_b = i < in_buffer_size ? static_cast<unsigned char>(in_buffer[i++]) : 0;
        uint32_t octet_c = i < in_buffer_size ? static_cast<unsigned char>(in_buffer[i++]) : 0;

        uint32_t triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;

        out_buffer[j++] = g_base64_encoding_chars[(triple >> 3 * 6) & 0x3F];
        out_buffer[j++] = g_base64_encoding_chars[(triple >> 2 * 6) & 0x3F];
        out_buffer[j++] = g_base64_encoding_chars[(triple >> 1 * 6) & 0x3F];
        out_buffer[j++] = g_base64_encoding_chars[(triple >> 0 * 6) & 0x3F];
    }

    // The last zero, one or two characters must be set to '=';
    switch(in_buffer_size % 3) {
        case 0:
            break;
        case 1:
            out_buffer[encoded_size - 1] = '=';
            out_buffer[encoded_size - 2] = '=';
            break;
        case 2:
            out_buffer[encoded_size - 1] = '=';
            break;
        default:
            break;
    }

    return encoded_size;
}


Optional<size_t> base64_decode(StringData input, char* out_buffer, size_t out_buffer_len) noexcept
{
    REALM_ASSERT_EX(input.size() < std::numeric_limits<size_t>::max() / 3, input.size());
    size_t required_buffer_len = (input.size() * 3 + 3) / 4;
    REALM_ASSERT_EX(out_buffer_len >= required_buffer_len, out_buffer_len, required_buffer_len);
    static_cast<void>(out_buffer_len);
    static_cast<void>(required_buffer_len);

    // Guard against overlap (so that "restrict" works in the following)
    REALM_ASSERT(input.data() + input.size() <= out_buffer
                 || input.data() >= out_buffer + out_buffer_len);


    const char* REALM_RESTRICT p = input.data();
          char* REALM_RESTRICT o = out_buffer;

    enum b64_byte_type {
        equals     = 64,  // used as padding at the end
        whitespace = 65,
        invalid    = 66,
    };

    size_t bytes_written = 0;
    size_t num_trailing_equals = 0;
    unsigned int buffer = 0;
    size_t i, buffer_size; // i is input sequence, buffer_size is only incremented for valid characters
    for (i = 0, buffer_size = 0; i < input.size(); ++i) {
        // classify the base64 character
        unsigned int x = index_of_base64_byte(static_cast<unsigned char>(p[i]));
        switch (x) {
            case equals:     ++num_trailing_equals; continue;
            case whitespace: continue; // ignore whitespace
            case invalid:    return none;
        }

        if (num_trailing_equals > 0)
            return none; // data after the end-padding

        REALM_ASSERT_EX(x < 64, x);
        buffer = buffer << 6 | x;
        ++buffer_size;

        if (buffer_size == 4) {
            *o++ = (buffer >> 16) & 0xff;
            *o++ = (buffer >>  8) & 0xff;
            *o++ = (buffer >>  0) & 0xff;
            buffer = 0;
            buffer_size = 0;
            bytes_written += 3;
        }
    }

    // no padding
    size_t extra = input.size() % 4;
    if (num_trailing_equals == 0 && extra > 1) {
        num_trailing_equals = 4 - extra;
    }
    
    // trailing bytes
    if (num_trailing_equals == 0) {
        if (buffer_size != 0)
            return none; // stuff was left in buffer, so input was not sufficiently padded.
    }
    else if (num_trailing_equals == 1) {
        *o++ = (buffer >> 10) & 0xff;
        *o++ = (buffer >>  2) & 0xff;
        bytes_written += 2;
    }
    else if (num_trailing_equals == 2) {
        *o++ = (buffer >>  4) & 0xff;
        bytes_written += 1;
    }
    else {
        return none;
    }

    return bytes_written;
}

Optional<std::vector<char>> base64_decode_to_vector(StringData encoded)
{
    size_t max_size = base64_decoded_size(encoded.size());
    std::vector<char> decoded(max_size); // Throws
    Optional<size_t> actual_size = base64_decode(encoded, decoded.data(), decoded.size());
    if (!actual_size)
        return none;

    decoded.resize(*actual_size); // Throws
    return decoded;
}

} // namespace util
} // namespace realm
