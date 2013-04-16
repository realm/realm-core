/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_UTF8_HPP
#define TIGHTDB_UTF8_HPP

#include <stdint.h>
#include <string>

#include <tightdb/string_data.hpp>

namespace tightdb {

// FIXME: The current approach to case insensitive comparison requires
// that case mappings can be done in a way that does not change he
// number of bytes used to encode the individual Unicode
// character. This is not generally the case, so, as far as I can see,
// this approach has no future.
//
// FIXME: The current approach to case insensitive comparison relies
// on checking each "haystack" character against the corresponding
// character in both a lower cased and an upper cased version of the
// "needle". While this leads to efficient comparison, it ignores the
// fact that "case folding" is the only correct approach to case
// insensitive comparison in a locale agnostic Unicode
// environment.
//
// See
//   http://www.w3.org/International/wiki/Case_folding
//   http://userguide.icu-project.org/transforms/casemappings#TOC-Case-Folding.
//
// The ideal API would probably be something like this:
//
//   case_fold:        utf_8 -> case_folded
//   equal_case_fold:  (needle_case_folded, single_haystack_entry_utf_8) -> found
//   search_case_fold: (needle_case_folded, huge_haystack_string_utf_8) -> found_at_position
//
// The case folded form would probably be using UTF-32 or UTF-16.


/// If successfull, writes a string to \a target of the same size as
/// \a source, and returns true. Returns false if invalid UTF-8
/// encoding was encountered.
bool case_map(StringData source, char* target, bool upper);

/// Assumes that the sizes of \a needle_upper and \a needle_lower are
/// identical to the size of \a haystack. Returns false if the needle
/// is different from the haystack.
bool equal_case_fold(StringData haystack, const char* needle_upper, const char* needle_lower);

/// Assumes that the sizes of \a needle_upper and \a needle_lower are
/// both equal to \a needle_size. Returns haystack.size() if the
/// needle was not found.
std::size_t search_case_fold(StringData haystack, const char* needle_upper,
                             const char* needle_lower, std::size_t needle_size);



/// Transcode between UTF-8 and UTF-16.
///
/// \tparam Char16 Must be an integral type with at least 16 bits.
///
/// \tparam Traits16 Must define a suitable to_char_type() for \a
/// Char16.
template<class Char16, class Traits16 = std::char_traits<Char16> > struct Utf8x16 {
    /// Transcode as much as possible of the specified UTF-8 input, to
    /// UTF-16. Returns true if all input characters were transcoded,
    /// or transcoding stopped because the next character did not fit
    /// into the output buffer. Returns false if transcoding stopped
    /// due to invalid input. It is not specified whether this
    /// function returns true or false if invalid input occurs at the
    /// same time that the output buffer runs full. In any case, upon
    /// return, \a in_begin and \a out_begin are advanced to the
    /// position where the transcoding stopped.
    ///
    /// Throws only if Traits16::to_char_type() throws.
    static bool to_utf16(const char*& in_begin, const char* in_end,
                         Char16*& out_begin, Char16* out_end);
};


/// Shorthand for Utf8x16<Char16>::to_utf16(...)
template<class Char16> bool xcode_utf8_to_utf16(const char*& in_begin, const char* in_end,
                                                Char16*& out_begin, Char16* out_end);

/// Calculate the number of UTF-16 elements needed to hold the result
/// of transcoding the specified UTF-8 string. Upon return, if \a
/// in_begin != \a in_end, then the calculation stopped due to invalid
/// UTF-8 input. The returned size then reflects the number of UTF-16
/// elements needed to hold the result of transcoding the part of the
/// input that was examined. This function will only detect a few
/// UTF-8 validity issues, and can therefore not be used for UTF-8
/// validation.
std::size_t calc_buf_size_utf8_to_utf16(const char*& in_begin, const char* in_end);






// Implementation:

// Adapted from reference implementation.
// http://www.unicode.org/resources/utf8.html
// http://www.bsdua.org/files/unicode.tar.gz
template<class Char16, class Traits16>
inline bool Utf8x16<Char16, Traits16>::to_utf16(const char*& in_begin, const char* in_end,
                                                Char16*& out_begin, Char16* out_end)
{
    using namespace std;
    bool invalid = false;
    const char* in = in_begin;
    Char16* out = out_begin;
    while (in != in_end) {
        if (TIGHTDB_UNLIKELY(out == out_end)) {
            break; // Need space in output buffer
        }
        uint_fast16_t v1 = char_traits<char>::to_int_type(in[0]);
        if (TIGHTDB_LIKELY(v1 < 0x80)) { // One byte
            *out++ = Traits16::to_char_type(v1);
            in += 1;
            continue;
        }
        if (TIGHTDB_UNLIKELY(v1 < 0xC0)) {
            invalid = true;
            break; // Invalid first byte of UTF-8 sequence
        }
        if (TIGHTDB_LIKELY(v1 < 0xE0)) { // Two bytes
            if (TIGHTDB_UNLIKELY(in_end - in < 2)) {
                invalid = true;
                break; // Incomplete UTF-8 sequence
            }
            uint_fast16_t v2 = char_traits<char>::to_int_type(in[1]);
            if (TIGHTDB_UNLIKELY((v2 & 0xC0) != 0x80)) {
                invalid = true;
                break; // Invalid continuation byte
            }
            uint_fast16_t v =
                ((v1 & 0x1F) << 6) |
                ((v2 & 0x3F) << 0);
            if (TIGHTDB_UNLIKELY(v < 0x80)) {
                invalid = true;
                break; // Overlong encoding is invalid
            }
            *out++ = Traits16::to_char_type(v);
            in += 2;
            continue;
        }
        if (TIGHTDB_LIKELY(v1 < 0xF0)) { // Three bytes
            if (TIGHTDB_UNLIKELY(in_end - in < 3)) {
                invalid = true;
                break; // Incomplete UTF-8 sequence
            }
            uint_fast16_t v2 = char_traits<char>::to_int_type(in[1]);
            uint_fast16_t v3 = char_traits<char>::to_int_type(in[2]);
            if (TIGHTDB_UNLIKELY((v2 & 0xC0) != 0x80 || (v3 & 0xC0) != 0x80)) {
                invalid = true;
                break; // Invalid continuation byte
            }
            uint_fast16_t v =
                ((v1 & 0x0F) << 12) |
                ((v2 & 0x3F) <<  6) |
                ((v3 & 0x3F) <<  0);
            if (TIGHTDB_UNLIKELY(v < 0x800)) {
                invalid = true;
                break; // Overlong encoding is invalid
            }
            *out++ = Traits16::to_char_type(v);
            in += 3;
            continue;
        }
        if (TIGHTDB_UNLIKELY(out + 1 == out_end)) {
            break; // Need space in output buffer for surrogate pair
        }
        if (TIGHTDB_LIKELY(v1 < 0xF8)) { // Four bytes
            if (TIGHTDB_UNLIKELY(in_end - in < 4)) {
                invalid = true;
                break; // Incomplete UTF-8 sequence
            }
            uint_fast32_t w1 = v1; // 16 bit -> 32 bit
            uint_fast32_t v2 = char_traits<char>::to_int_type(in[1]); // 32 bit intended
            uint_fast16_t v3 = char_traits<char>::to_int_type(in[2]); // 16 bit intended
            uint_fast16_t v4 = char_traits<char>::to_int_type(in[3]); // 16 bit intended
            if (TIGHTDB_UNLIKELY((v2 & 0xC0) != 0x80 || (v3 & 0xC0) != 0x80 ||
                                 (v4 & 0xC0) != 0x80)) {
                invalid = true;
                break; // Invalid continuation byte
            }
            uint_fast32_t v =
                ((w1 & 0x07) << 18) | // Parenthesis is 32 bit partial result
                ((v2 & 0x3F) << 12) | // Parenthesis is 32 bit partial result
                ((v3 & 0x3F) <<  6) | // Parenthesis is 16 bit partial result
                ((v4 & 0x3F) <<  0);  // Parenthesis is 16 bit partial result
            if (TIGHTDB_UNLIKELY(v < 0x10000l)) {
                invalid = true;
                break; // Overlong encoding is invalid
            }
            if (TIGHTDB_UNLIKELY(0x110000l <= v)) {
                invalid = true;
                break; // Code point too big for UTF-16
            }
            v -= 0x10000l;
            *out++ = Traits16::to_char_type(0xD800 + (v / 0x400));
            *out++ = Traits16::to_char_type(0xDC00 + (v % 0x400));
            in += 4;
            continue;
        }
        // Invalid first byte of UTF-8 sequence, or code point too big for UTF-16
        invalid = true;
        break;
    }

    in_begin  = in;
    out_begin = out;
    return !invalid;
}


template<class Char16> inline bool xcode_utf8_to_utf16(const char*& in_begin, const char* in_end,
                                                       Char16*& out_begin, Char16* out_end)
{
    return Utf8x16<Char16>::to_utf16(in_begin, in_end, out_begin, out_end);
}


} // namespace tightdb

#endif // TIGHTDB_UTF8_HPP
