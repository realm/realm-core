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

#include <realm/unicode.hpp>

#include <algorithm>
#include <clocale>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <ctype.h>
#endif

namespace realm {

// clang-format off
// Returns the number of bytes in a UTF-8 sequence whose leading byte is as specified.
size_t sequence_length(char lead)
{
    // keep 'static' else entire array will be pushed to stack at each call
    const static unsigned char lengths[256] = {
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 1, 1
    };

    return lengths[static_cast<unsigned char>(lead)];
}
// clang-format on

// Check if the next UTF-8 sequence in [begin, end) is identical to
// the one beginning at begin2. If it is, 'begin' is advanced
// accordingly.
bool equal_sequence(const char*& begin, const char* end, const char* begin2)
{
    if (begin[0] != begin2[0])
        return false;

    size_t i = 1;
    if (static_cast<int>(std::char_traits<char>::to_int_type(begin[0])) & 0x80) {
        // All following bytes matching '10xxxxxx' will be considered
        // as part of this character.
        while (begin + i != end) {
            if ((static_cast<int>(std::char_traits<char>::to_int_type(begin[i])) & (0x80 + 0x40)) != 0x80)
                break;
            if (begin[i] != begin2[i])
                return false;
            ++i;
        }
    }

    begin += i;
    return true;
}

// Translate from utf8 char to unicode. No check for invalid utf8; may read out of bounds! Caller must check.
uint32_t utf8value(const char* character)
{
    const unsigned char* c = reinterpret_cast<const unsigned char*>(character);
    size_t len = sequence_length(c[0]);
    uint32_t res = c[0];

    if (len == 1)
        return res;

    res &= (0x3f >> (len - 1));

    for (size_t i = 1; i < len; i++)
        res = ((res << 6) | (c[i] & 0x3f));

    return res;
}

// Converts UTF-8 source into upper or lower case. This function
// preserves the byte length of each UTF-8 character in following way:
// If an output character differs in size, it is simply substituded by
// the original character. This may of course give wrong search
// results in very special cases. Todo.
util::Optional<std::string> case_map(StringData source, bool upper)
{
    std::string result;
    result.resize(source.size());

#if defined(_WIN32)
    constexpr int tmp_buffer_size = 32;
    const char* begin = source.data();
    const char* end = begin + source.size();
    auto output = result.begin();
    while (begin != end) {
        auto n = end - begin;
        if (n > tmp_buffer_size) {
            // Break the input string into chunks - but don't break in the middle of a multibyte character
            const char* p = begin;
            const char* buffer_end = begin + tmp_buffer_size;
            while (p < buffer_end) {
                size_t len = sequence_length(*p);
                p += len;
                if (p > buffer_end) {
                    p -= len;
                    break;
                }
            }
            n = p - begin;
        }

        wchar_t tmp[tmp_buffer_size];

        int n2 = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, begin, int(n), tmp, tmp_buffer_size);
        if (n2 == 0)
            return util::none;

        if (n2 < tmp_buffer_size)
            tmp[n2] = 0;

        // Note: If tmp[0] == 0, it is because the string contains a
        // null-chacarcter, which is perfectly fine.

        wchar_t mapped_tmp[tmp_buffer_size];
        LCMapStringEx(LOCALE_NAME_INVARIANT, upper ? LCMAP_UPPERCASE : LCMAP_LOWERCASE, tmp, n2, mapped_tmp,
                      tmp_buffer_size, nullptr, nullptr, 0);

        // FIXME: The intention is to use flag 'WC_ERR_INVALID_CHARS'
        // to catch invalid UTF-8. Even though the documentation says
        // unambigously that it is supposed to work, it doesn't. When
        // the flag is specified, the function fails with error
        // ERROR_INVALID_FLAGS.
        DWORD flags = 0;
        auto m = static_cast<int>(end - begin);
        int n3 = WideCharToMultiByte(CP_UTF8, flags, mapped_tmp, n2, &*output, m, 0, 0);
        if (n3 == 0 && GetLastError() != ERROR_INSUFFICIENT_BUFFER)
            return util::none;

        if (n3 != n) {
            realm::safe_copy_n(begin, n, output); // Cannot handle different size, copy source
        }

        begin += n;
        output += n;
    }

    return result;
#else
    size_t sz = source.size();
    typedef std::char_traits<char> traits;
    for (size_t i = 0; i < sz; ++i) {
        char c = source[i];
        auto int_val = traits::to_int_type(c);

        auto copy_bytes = [&](size_t n) {
            if (i + n > sz) {
                return false;
            }
            for (size_t j = 1; j < n; j++) {
                result[i++] = c;
                c = source[i];
                if ((c & 0xC0) != 0x80) {
                    return false;
                }
            }
            return true;
        };

        if (int_val < 0x80) {
            // Handle ASCII
            if (upper && (c >= 'a' && c <= 'z')) {
                c -= 0x20;
            }
            else if (!upper && (c >= 'A' && c <= 'Z')) {
                c += 0x20;
            }
        }
        else {
            if ((int_val & 0xE0) == 0xc0) {
                // 2 byte utf-8
                if (i + 2 > sz) {
                    return {};
                }
                c = source[i + 1];
                if ((c & 0xC0) != 0x80) {
                    return {};
                }
                auto u = ((int_val << 6) + (traits::to_int_type(c) & 0x3F)) & 0x7FF;
                // Handle some Latin-1 supplement characters
                if (upper && (u >= 0xE0 && u <= 0xFE && u != 0xF7)) {
                    u -= 0x20;
                }
                else if (!upper && (u >= 0xC0 && u <= 0xDE && u != 0xD7)) {
                    u += 0x20;
                }

                result[i++] = static_cast<char>((u >> 6) | 0xC0);
                c = static_cast<char>((u & 0x3f) | 0x80);
            }
            else if ((int_val & 0xF0) == 0xE0) {
                // 3 byte utf-8
                if (!copy_bytes(3)) {
                    return {};
                }
            }
            else if ((int_val & 0xF8) == 0xF0) {
                // 4 byte utf-8
                if (!copy_bytes(4)) {
                    return {};
                }
            }
            else {
                return {};
            }
        }
        result[i] = c;
    }
    return result;
#endif
}

std::string case_map(StringData source, bool upper, IgnoreErrorsTag)
{
    return case_map(source, upper).value_or("");
}

// If needle == haystack, return true. NOTE: This function first
// performs a case insensitive *byte* compare instead of one whole
// UTF-8 character at a time. This is very fast, but not enough to
// guarantee that the strings are identical, so we need to finish off
// with a slower but rigorous comparison. The signature is similar in
// spirit to std::equal().
bool equal_case_fold(StringData haystack, const char* needle_upper, const char* needle_lower)
{
    for (size_t i = 0; i != haystack.size(); ++i) {
        char c = haystack[i];
        if (needle_lower[i] != c && needle_upper[i] != c)
            return false;
    }

    const char* begin = haystack.data();
    const char* end = begin + haystack.size();
    const char* i = begin;
    while (i != end) {
        if (!equal_sequence(i, end, needle_lower + (i - begin)) &&
            !equal_sequence(i, end, needle_upper + (i - begin)))
            return false;
    }
    return true;
}


// Test if needle is a substring of haystack. The signature is similar
// in spirit to std::search().
size_t search_case_fold(StringData haystack, const char* needle_upper, const char* needle_lower, size_t needle_size)
{
    // FIXME: This solution is very inefficient. Consider deploying the Boyer-Moore algorithm.
    size_t i = 0;
    while (needle_size <= haystack.size() - i) {
        if (equal_case_fold(haystack.substr(i, needle_size), needle_upper, needle_lower)) {
            return i;
        }
        ++i;
    }
    return haystack.size(); // Not found
}

/// This method takes an array that maps chars (both upper- and lowercase) to distance that can be moved
/// (and zero for chars not in needle), allowing the method to apply Boyer-Moore for quick substring search
/// The map is calculated in the StringNode<ContainsIns> class (so it can be reused across searches)
bool contains_ins(StringData haystack, const char* needle_upper, const char* needle_lower, size_t needle_size,
                  const std::array<uint8_t, 256>& charmap)
{
    if (needle_size == 0)
        return haystack.size() != 0;

    // Prepare vars to avoid lookups in loop
    size_t last_char_pos = needle_size - 1;
    unsigned char lastCharU = needle_upper[last_char_pos];
    unsigned char lastCharL = needle_lower[last_char_pos];

    // Do Boyer-Moore search
    size_t p = last_char_pos;
    while (p < haystack.size()) {
        unsigned char c = haystack.data()[p]; // Get candidate for last char

        if (c == lastCharU || c == lastCharL) {
            StringData candidate = haystack.substr(p - needle_size + 1, needle_size);
            if (equal_case_fold(candidate, needle_upper, needle_lower))
                return true; // text found!
        }

        // If we don't have a match, see how far we can move char_pos
        if (charmap[c] == 0)
            p += needle_size; // char was not present in search string
        else
            p += charmap[c];
    }

    return false;
}

bool string_like_ins(StringData text, StringData upper, StringData lower) noexcept
{
    if (text.is_null() || lower.is_null()) {
        return (text.is_null() && lower.is_null());
    }

    return StringData::matchlike_ins(text, lower, upper);
}

bool string_like_ins(StringData text, StringData pattern) noexcept
{
    if (text.is_null() || pattern.is_null()) {
        return (text.is_null() && pattern.is_null());
    }

    std::string upper = case_map(pattern, true, IgnoreErrors);
    std::string lower = case_map(pattern, false, IgnoreErrors);

    return StringData::matchlike_ins(text, lower.c_str(), upper.c_str());
}

} // namespace realm
