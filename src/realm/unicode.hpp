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

#ifndef REALM_UNICODE_HPP
#define REALM_UNICODE_HPP

#include <cstdint>
#include <string>

#include <realm/string_data.hpp>
#include <realm/util/features.h>
#include <realm/utilities.hpp>


namespace realm {

// Return size in bytes of utf8 character. No error checking
size_t sequence_length(char lead);

// Limitations for case insensitive string search
// Case insensitive search (equal, begins_with, ends_with, like and contains)
// only works for unicodes 0...0x7f which is the same as the 0...127
// ASCII character set (letters a-z and A-Z).

// In does *not* work for the 0...255 ANSI character set that contains
// characters from many European countries like Germany, France, Denmark,
// etc.

// It also does not work for characters from non-western countries like
// Japan, Russia, Arabia, etc.

// If there exists characters outside the ASCII range either in the text
// to be searched for, or in the Realm string column which is searched
// in, then the compare yields a random result such that the row may or
// may not be included in the result set.

// Return unicode value of character.
uint32_t utf8value(const char* character);

inline bool equal_sequence(const char*& begin, const char* end, const char* begin2);

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


/// If successful, returns a string of the same size as \a source.
/// Returns none if invalid UTF-8 encoding was encountered.
util::Optional<std::string> case_map(StringData source, bool upper);

enum IgnoreErrorsTag { IgnoreErrors };
std::string case_map(StringData source, bool upper, IgnoreErrorsTag);

/// Assumes that the sizes of \a needle_upper and \a needle_lower are
/// identical to the size of \a haystack. Returns false if the needle
/// is different from the haystack.
bool equal_case_fold(StringData haystack, const char* needle_upper, const char* needle_lower);

/// Assumes that the sizes of \a needle_upper and \a needle_lower are
/// both equal to \a needle_size. Returns haystack.size() if the
/// needle was not found.
size_t search_case_fold(StringData haystack, const char* needle_upper, const char* needle_lower, size_t needle_size);

/// Assumes that the sizes of \a needle_upper and \a needle_lower are
/// both equal to \a needle_size. Returns false if the
/// needle was not found.
bool contains_ins(StringData haystack, const char* needle_upper, const char* needle_lower, size_t needle_size,
                  const std::array<uint8_t, 256>& charmap);

/// Case insensitive wildcard matching ('?' for single char, '*' for zero or more chars)
bool string_like_ins(StringData text, StringData pattern) noexcept;
bool string_like_ins(StringData text, StringData upper, StringData lower) noexcept;

} // namespace realm

#endif // REALM_UNICODE_HPP
