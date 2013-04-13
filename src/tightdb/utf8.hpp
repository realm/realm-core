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


} // namespace tightdb

#endif // TIGHTDB_UTF8_HPP
