/*************************************************************************
*
* REALM CONFIDENTIAL
* __________________
*
*  [2011] - [2015] Realm Inc
*  All Rights Reserved.
*
* NOTICE:  All information contained herein is, and remains
* the property of Realm Incorporated and its suppliers,
* if any.  The intellectual and technical concepts contained
* herein are proprietary to Realm Incorporated
* and its suppliers and may be covered by U.S. and Foreign Patents,
* patents in process, and are protected by trade secret or copyright law.
* Dissemination of this information or reproduction of this material
* is strictly forbidden unless prior written permission is obtained
* from Realm Incorporated.
*
**************************************************************************/
#ifndef REALM_UNICODE_HPP
#define REALM_UNICODE_HPP

#include <locale>
#include <stdint.h>
#include <string>

#include <realm/util/safe_int_ops.hpp>
#include <realm/string_data.hpp>
#include <realm/util/features.h>
#include <realm/utilities.hpp>


namespace realm {

    enum string_compare_method_t { STRING_COMPARE_CORE, STRING_COMPARE_CPP11, STRING_COMPARE_CALLBACK } ;

    extern StringCompareCallback string_compare_callback;
    extern string_compare_method_t string_compare_method;

    // Description for set_string_compare_method():
    //
    // Short summary: iOS language binding: call
    //     set_string_compare_method() for fast but slightly inaccurate sort in some countries, or
    //     set_string_compare_method(2, callbackptr) for slow but precise sort (see callbackptr below)
    //
    // Different countries ('locales') have different sorting order for strings and letters. Because there unfortunatly
    // doesn't exist any unified standardized way to compare strings in C++ on multiple platforms, we need this method.
    //
    // It determins how sorting a TableView by a String column must take place. The 'method' argument can be:
    //
    // 0: Fast core-only compare (no OS/framework calls). LIMITATIONS: Works only upto 'Latin Extended 2' (unicodes
    // 0...591). Also, sorting order is according to 'en_US' so it may be slightly inaccurate for some countries.
    // 'callback' argument is ignored.
    //
    // Return value: Always 'true'
    //
    // 1: Native C++11 method if core is compiled as C++11. Gives precise sorting according
    // to user's current locale. LIMITATIONS: Currently works only on Windows and on Linux with clang. Does NOT work on
    // iOS (due to only 'C' locale being available in CoreFoundation, which puts 'Z' before 'a'). Unknown if works on
    // Windows Phone / Android. Furthermore it does NOT work on Linux with gcc 4.7 or 4.8 (lack of c++11 feature that
    // can convert utf8->wstring without calls to setlocale()).
    //
    // Return value: 'true' if supported, otherwise 'false' (if so, then previous setting, if any, is preserved).
    //
    // 2: Callback method. Language binding / C++ user must provide a utf-8 callback method of prototype:
    // bool callback(const char* string1, const char* string2) where 'callback' must return bool(string1 < string2).
    //
    // Return value: Always 'true'
    //
    // Default is method = 0 if the function is never called
    //
    // NOT THREAD SAFE! Call once during initialization or make sure it's not called simultaneously with different arguments
    // The setting is remembered per-process; it does NOT need to be called prior to each sort
    bool set_string_compare_method(string_compare_method_t method, StringCompareCallback callback);


    // Return size in bytes of utf8 character. No error checking
    size_t sequence_length(char lead);

    // Limitations for case insensitive string search
    // Case insensitive search (equal, begins_with, ends_with and contains)
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

    // Return bool(string1 < string2)
    bool utf8_compare(StringData string1, StringData string2);

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
    size_t search_case_fold(StringData haystack, const char* needle_upper,
        const char* needle_lower, size_t needle_size);

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
    // It groups all characters that look visually identical, that is, it puts `a, à, å` together and before 
    // `ø, o, ö`. Note that this sorting method is wrong in some countries, such as Denmark where `å` must
    // come last. NOTE: This is a limitation of STRING_COMPARE_CORE until we get better such 'locale' support.

    constexpr size_t last_latin_extended_2_unicode = 591;

    static const uint32_t collation_order[last_latin_extended_2_unicode + 1] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 456, 457, 458, 459, 460, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 461, 462, 463, 464, 8130, 465, 466, 467,
        468, 469, 470, 471, 472, 473, 474, 475, 8178, 8248, 8433, 8569, 8690, 8805, 8912, 9002, 9093, 9182, 476, 477, 478, 479, 480, 481, 482, 9290, 9446, 9511, 9595, 9690, 9818, 9882, 9965, 10051, 10156, 10211, 10342, 10408, 10492, 10588,
        10752, 10828, 10876, 10982, 11080, 11164, 11304, 11374, 11436, 11493, 11561, 483, 484, 485, 486, 487, 488, 9272, 9428, 9492, 9575, 9671, 9800, 9864, 9947, 10030, 10138, 10193, 10339, 10389, 10474, 10570, 10734, 10811, 10857, 10964, 11062, 11146, 11285, 11356,
        11417, 11476, 11543, 489, 490, 491, 492, 27, 28, 29, 30, 31, 32, 493, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58,
        494, 495, 8128, 8133, 8127, 8135, 496, 497, 498, 499, 9308, 500, 501, 59, 502, 503, 504, 505, 8533, 8669, 506, 12018, 507, 508, 509, 8351, 10606, 510, 8392, 8377, 8679, 511, 9317, 9315, 9329, 9353, 9348, 9341, 9383, 9545,
        9716, 9714, 9720, 9732, 10078, 10076, 10082, 10086, 9635, 10522, 10615, 10613, 10619, 10640, 10633, 512, 10652, 11190, 11188, 11194, 11202, 11515, 11624, 11038, 9316, 9314, 9328, 9352, 9345, 9340, 9381, 9543, 9715, 9713, 9719, 9731, 10077, 10075, 10081, 10085,
        9633, 10521, 10614, 10612, 10618, 10639, 10630, 513, 10651, 11189, 11187, 11193, 11199, 11514, 11623, 11521, 9361, 9360, 9319, 9318, 9359, 9358, 9536, 9535, 9538, 9537, 9542, 9541, 9540, 9539, 9620, 9619, 9626, 9625, 9744, 9743, 9718, 9717, 9736, 9735,
        9742, 9741, 9730, 9729, 9909, 9908, 9907, 9906, 9913, 9912, 9915, 9914, 9989, 9988, 10000, 9998, 10090, 10089, 10095, 10094, 10080, 10079, 10093, 10092, 10091, 10120, 10113, 10112, 10180, 10179, 10240, 10239, 10856, 10322, 10321, 10326, 10325, 10324, 10323, 10340,
        10337, 10328, 10327, 10516, 10515, 10526, 10525, 10520, 10519, 11663, 10567, 10566, 10660, 10659, 10617, 10616, 10638, 10637, 10689, 10688, 10901, 10900, 10907, 10906, 10903, 10902, 11006, 11005, 11010, 11009, 11018, 11017, 11012, 11011, 11109, 11108, 11104, 11103, 11132, 11131,
        11215, 11214, 11221, 11220, 11192, 11191, 11198, 11197, 11213, 11212, 11219, 11218, 11401, 11400, 11519, 11518, 11522, 11583, 11582, 11589, 11588, 11587, 11586, 11027, 9477, 9486, 9488, 9487, 11657, 11656, 10708, 9568, 9567, 9662, 9664, 9667, 9666, 11594, 9774, 9779,
        9784, 9860, 9859, 9937, 9943, 10014, 10135, 10129, 10266, 10265, 10363, 10387, 11275, 10554, 10556, 10723, 10673, 10672, 9946, 9945, 10802, 10801, 10929, 11653, 11652, 11054, 11058, 11136, 11139, 11138, 11141, 11232, 11231, 11282, 11347, 11537, 11536, 11597, 11596, 11613,
        11619, 11618, 11621, 11645, 11655, 11654, 11125, 11629, 11683, 11684, 11685, 11686, 9654, 9653, 9652, 10345, 10344, 10343, 10541, 10540, 10539, 9339, 9338, 10084, 10083, 10629, 10628, 11196, 11195, 11211, 11210, 11205, 11204, 11209, 11208, 11207, 11206, 9773, 9351, 9350,
        9357, 9356, 9388, 9387, 9934, 9933, 9911, 9910, 10238, 10237, 10656, 10655, 10658, 10657, 11616, 11615, 10181, 9651, 9650, 9648, 9905, 9904, 10015, 11630, 10518, 10517, 9344, 9343, 9386, 9385, 10654, 10653, 9365, 9364, 9367, 9366, 9752, 9751, 9754, 9753,
        10099, 10098, 10101, 10100, 10669, 10668, 10671, 10670, 10911, 10910, 10913, 10912, 11228, 11227, 11230, 11229, 11026, 11025, 11113, 11112, 11542, 11541, 9991, 9990, 10557, 9668, 10731, 10730, 11601, 11600, 9355, 9354, 9738, 9737, 10636, 10635, 10646, 10645, 10648, 10647,
        10650, 10649, 11528, 11527, 10382, 10563, 11142, 10182, 9641, 10848, 9409, 9563, 9562, 10364, 11134, 11048, 11606, 11660, 11659, 9478, 11262, 11354, 9769, 9768, 10186, 10185, 10855, 10854, 10936, 10935, 11535, 11534 };
} // namespace realm

#endif // REALM_UNICODE_HPP
