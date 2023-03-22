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

// Highest character currently supported for *sorting* strings in Realm, when using STRING_COMPARE_CPP11.
constexpr size_t last_latin_extended_2_unicode = 591;

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

// Returns bool(string1 < string2) for utf-8
bool utf8_compare(StringData string1, StringData string2)
{
    const char* s1 = string1.data();
    const char* s2 = string2.data();

    // This collation_order array has 592 entries; one entry per unicode character in the range 0...591
    // (upto and including 'Latin Extended 2'). The value tells what 'sorting order rank' the character
    // has, such that unichar1 < unichar2 implies collation_order[unichar1] < collation_order[unichar2]. The
    // array is generated from the table found at ftp://ftp.unicode.org/Public/UCA/latest/allkeys.txt. At the
    // bottom of unicode.cpp you can find source code that reads such a file and translates it into C++ that
    // you can copy/paste in case the official table should get updated.
    //
    // NOTE: Some numbers in the array are vere large. This is because the value is the *global* rank of the
    // almost full unicode set. An optimization could be to 'normalize' all values so they ranged from
    // 0...591 so they would fit in a uint16_t array instead of uint32_t.
    //
    // It groups all characters that look visually identical, that is, it puts `a, ‡, Â` together and before
    // `¯, o, ˆ`. Note that this sorting method is wrong in some countries, such as Denmark where `Â` must
    // come last. NOTE: This is a limitation of STRING_COMPARE_CORE until we get better such 'locale' support.

    // clang-format off
    static const uint32_t collation_order_core[last_latin_extended_2_unicode + 1] = {
        0, 2, 3, 4, 5, 6, 7, 8, 9, 33, 34, 35, 36, 37, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 31, 38, 39, 40, 41, 42, 43, 29, 44, 45, 46, 76, 47, 30, 48, 49, 128, 132, 134, 137, 139, 140, 143, 144, 145, 146, 50, 51, 77, 78, 79, 52, 53, 148, 182, 191, 208, 229, 263, 267, 285, 295, 325, 333, 341, 360, 363, 385, 429, 433, 439, 454, 473, 491, 527, 531, 537, 539, 557, 54, 55, 56, 57, 58, 59, 147, 181, 190, 207,
        228, 262, 266, 284, 294, 324, 332, 340, 359, 362, 384, 428, 432, 438, 453, 472, 490, 526, 530, 536, 538, 556, 60, 61, 62, 63, 28, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 32, 64, 72, 73, 74, 75, 65, 88, 66, 89, 149, 81, 90, 1, 91, 67, 92, 80, 136, 138, 68, 93, 94, 95, 69, 133, 386, 82, 129, 130, 131, 70, 153, 151, 157, 165, 575, 588, 570, 201, 233,
        231, 237, 239, 300, 298, 303, 305, 217, 371, 390, 388, 394, 402, 584, 83, 582, 495, 493, 497, 555, 541, 487, 470, 152, 150, 156, 164, 574, 587, 569, 200, 232, 230, 236, 238, 299, 297, 302, 304, 216, 370, 389, 387, 393, 401, 583, 84, 581, 494, 492, 496, 554, 540, 486, 544, 163, 162, 161, 160, 167, 166, 193, 192, 197, 196, 195, 194, 199, 198, 210, 209, 212, 211, 245, 244, 243, 242, 235, 234, 247, 246, 241, 240, 273, 272, 277, 276, 271, 270, 279, 278, 287, 286, 291, 290, 313, 312, 311, 310, 309,
        308, 315, 314, 301, 296, 323, 322, 328, 327, 337, 336, 434, 343, 342, 349, 348, 347, 346, 345, 344, 353, 352, 365, 364, 373, 372, 369, 368, 375, 383, 382, 400, 399, 398, 397, 586, 585, 425, 424, 442, 441, 446, 445, 444, 443, 456, 455, 458, 457, 462, 461, 460, 459, 477, 476, 475, 474, 489, 488, 505, 504, 503, 502, 501, 500, 507, 506, 549, 548, 509, 508, 533, 532, 543, 542, 545, 559, 558, 561, 560, 563, 562, 471, 183, 185, 187, 186, 189, 188, 206, 205, 204, 226, 215, 214, 213, 218, 257, 258, 259,
        265, 264, 282, 283, 292, 321, 316, 339, 338, 350, 354, 361, 374, 376, 405, 421, 420, 423, 422, 431, 430, 440, 468, 467, 466, 469, 480, 479, 478, 481, 524, 523, 525, 528, 553, 552, 565, 564, 571, 579, 578, 580, 135, 142, 141, 589, 534, 85, 86, 87, 71, 225, 224, 223, 357, 356, 355, 380, 379, 378, 159, 158, 307, 306, 396, 395, 499, 498, 518, 517, 512, 511, 516, 515, 514, 513, 256, 174, 173, 170, 169, 573, 572, 281, 280, 275, 274, 335, 334, 404, 403, 415, 414, 577, 576, 329, 222, 221, 220, 269,
        268, 293, 535, 367, 366, 172, 171, 180, 179, 411, 410, 176, 175, 178, 177, 253, 252, 255, 254, 318, 317, 320, 319, 417, 416, 419, 418, 450, 449, 452, 451, 520, 519, 522, 521, 464, 463, 483, 482, 261, 260, 289, 288, 377, 227, 427, 426, 567, 566, 155, 154, 249, 248, 409, 408, 413, 412, 392, 391, 407, 406, 547, 546, 358, 381, 485, 326, 219, 437, 168, 203, 202, 351, 484, 465, 568, 591, 590, 184, 510, 529, 251, 250, 331, 330, 436, 435, 448, 447, 551, 550
    };
    // clang-format on

    // Core-only method. Compares in us_EN locale (sorting may be slightly inaccurate in some countries). Will
    // return arbitrary return value for invalid utf8 (silent error treatment). If one or both strings have
    // unicodes beyond 'Latin Extended 2' (0...591), then the strings are compared by unicode value.
    uint32_t char1;
    uint32_t char2;
    do {
        size_t remaining1 = string1.size() - (s1 - string1.data());
        size_t remaining2 = string2.size() - (s2 - string2.data());

        if ((remaining1 == 0) != (remaining2 == 0)) {
            // exactly one of the strings have ended (not both or none; xor)
            return (remaining1 == 0);
        }
        else if (remaining2 == 0 && remaining1 == 0) {
            // strings are identical
            return false;
        }

        // invalid utf8
        if (remaining1 < sequence_length(s1[0]) || remaining2 < sequence_length(s2[0]))
            return false;

        char1 = utf8value(s1);
        char2 = utf8value(s2);

        if (char1 == char2) {
            // Go to next characters for both strings
            s1 += sequence_length(s1[0]);
            s2 += sequence_length(s2[0]);
        }
        else {
            // Test if above Latin Extended B
            if (char1 > last_latin_extended_2_unicode || char2 > last_latin_extended_2_unicode)
                return char1 < char2;

            const uint32_t* internal_collation_order = collation_order_core;
            uint32_t value1 = internal_collation_order[char1];
            uint32_t value2 = internal_collation_order[char2];

            return value1 < value2;
        }

    } while (true);
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


/*
// This is source code for generating the table in utf8_compare() from an allkey.txt file:

// Unicodes up to and including 'Latin Extended 2' (0...591)

std::vector<int64_t> order;
order.resize(last_latin_extended_2_unicode + 1);
std::string line;
std::ifstream myfile("d:/allkeys.txt");

// Read header text
for (size_t t = 0; t < 19; t++)
    getline(myfile, line);

// Read payload
for (size_t entry = 0; getline(myfile, line); entry++)
{
    string str = line.substr(0, 4);
    int64_t unicode = std::stoul(str, nullptr, 16);
    if (unicode < order.size())
    order[unicode] = entry;
}

// Emit something that you can copy/paste into the Core source code in unicode.cpp
cout << "static const uint32_t collation_order[] = {";
for (size_t t = 0; t < order.size(); t++) {
    if (t > 0 && t % 40 == 0)
        cout << "\n";
    cout << order[t] << (t + 1 < order.size() ? ", " : "");
}

cout << "};";
myfile.close();
*/
