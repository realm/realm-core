/*************************************************************************
*
* REALM CONFIDENTIAL
* __________________
*
*  [2011] - [2014] Realm Inc
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

#include <algorithm>
#include <vector>
#include <iostream>

#ifdef _WIN32
    #define NOMINMAX
    #include <windows.h>
#else
    #include <ctype.h>
#endif

#include <realm/util/safe_int_ops.hpp>
#include <realm/unicode.hpp>

#if REALM_HAVE_CXX11
#include <clocale>

#ifdef _MSC_VER
    #include <codecvt>
#else
    #include <locale>
#endif

#endif


namespace realm {

bool set_string_compare_method(string_compare_method_t method, StringCompareCallback callback)
{
    if (method == STRING_COMPARE_CPP11) {
#if defined(REALM_HAVE_CXX11) && !defined(REALM_ANDROID)
        std::string l = std::locale("").name();

        // We cannot use C locale because it puts 'Z' before 'a'
        if (l == "C")
            return false;

#else

        // If Realm wasn't compiled as C++11, just return false.
        return false;

#endif
    }
    else if (method == STRING_COMPARE_CALLBACK) {
        string_compare_callback = callback;
    }

    // other success actions
    string_compare_method = method;
    return true;
}


// Returns the number of bytes in a UTF-8 sequence whose leading byte is as specified.
size_t sequence_length(char lead)
{
    // keep 'static' else entire array will be pushed to stack at each call
    const static unsigned char lengths[256] = {
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3,
        3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 1, 1
    };

    return lengths[static_cast<unsigned char>(lead)];
}

// Check if the next UTF-8 sequence in [begin, end) is identical to
// the one beginning at begin2. If it is, 'begin' is advanced
// accordingly.
inline bool equal_sequence(const char*& begin, const char* end, const char* begin2)
{
    if (begin[0] != begin2[0]) return false;

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

// Converts unicodes 0...0x6ff (up to Arabic) to their respective lower case characters using a popular UnicodeData.txt
// file (http://www.opensource.apple.com/source/Heimdal/Heimdal-247.9/lib/wind/UnicodeData.txt) that contains case
// conversion information. The conversion does not take your current locale in count; it can be slightly wrong in some
// countries! If the input is already lower case, or outside range 0...0x6ff, then input value is returned untouched.
uint32_t to_lower(uint32_t character)
{
    static const int16_t lowers[] = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x0061, 0x0062,
        0x0063, 0x0064, 0x0065, 0x0066, 0x0067, 0x0068, 0x0069, 0x006a, 0x006b, 0x006c, 0x006d, 0x006e, 0x006f,
        0x0070, 0x0071, 0x0072, 0x0073, 0x0074, 0x0075, 0x0076, 0x0077, 0x0078, 0x0079, 0x007a, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x03bc, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x00e0, 0x00e1, 0x00e2,
        0x00e3, 0x00e4, 0x00e5, 0x00e6, 0x00e7, 0x00e8, 0x00e9, 0x00ea, 0x00eb, 0x00ec, 0x00ed, 0x00ee, 0x00ef,
        0x00f0, 0x00f1, 0x00f2, 0x00f3, 0x00f4, 0x00f5, 0x00f6, 0, 0x00f8, 0x00f9, 0x00fa, 0x00fb, 0x00fc, 0x00fd,
        0x00fe, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0x0101, 0, 0x0103, 0, 0x0105, 0, 0x0107, 0, 0x0109, 0, 0x010b, 0, 0x010d, 0, 0x010f, 0, 0x0111, 0, 0x0113,
        0, 0x0115, 0, 0x0117, 0, 0x0119, 0, 0x011b, 0, 0x011d, 0, 0x011f, 0,
        0x0121, 0, 0x0123, 0, 0x0125, 0, 0x0127, 0, 0x0129, 0, 0x012b, 0, 0x012d, 0, 0x012f, 0, 0, 0, 0x0133, 0,
        0x0135, 0, 0x0137, 0, 0, 0x013a, 0, 0x013c, 0, 0x013e, 0, 0x0140, 0, 0x0142, 0, 0x0144, 0, 0x0146, 0,
        0x0148, 0, 0, 0x014b, 0, 0x014d, 0, 0x014f, 0, 0x0151, 0, 0x0153, 0, 0x0155, 0, 0x0157, 0, 0x0159, 0,
        0x015b, 0, 0x015d, 0, 0x015f, 0, 0x0161, 0, 0x0163, 0, 0x0165, 0, 0x0167, 0, 0x0169, 0, 0x016b, 0, 0x016d,
        0, 0x016f, 0, 0x0171, 0, 0x0173, 0, 0x0175, 0, 0x0177, 0, 0x00ff, 0x017a, 0, 0x017c, 0, 0x017e, 0, 0x0073,
        0, 0x0253, 0x0183, 0, 0x0185, 0, 0x0254, 0x0188, 0, 0x0256, 0x0257, 0x018c, 0, 0, 0x01dd, 0x0259, 0x025b,
        0x0192, 0, 0x0260, 0x0263, 0, 0x0269, 0x0268, 0x0199, 0, 0, 0, 0x026f, 0x0272, 0, 0x0275, 0x01a1, 0,
        0x01a3, 0, 0x01a5, 0, 0x0280, 0x01a8, 0, 0x0283, 0, 0, 0x01ad, 0, 0x0288, 0x01b0, 0, 0x028a, 0x028b,
        0x01b4, 0, 0x01b6, 0, 0x0292, 0x01b9, 0, 0, 0, 0x01bd, 0, 0, 0, 0, 0, 0, 0, 0x01c6, 0x01c6, 0, 0x01c9,
        0x01c9, 0, 0x01cc, 0x01cc, 0, 0x01ce, 0, 0x01d0, 0, 0x01d2, 0, 0x01d4, 0, 0x01d6, 0, 0x01d8, 0, 0x01da, 0,
        0x01dc, 0, 0, 0x01df, 0, 0x01e1, 0, 0x01e3, 0, 0x01e5, 0, 0x01e7, 0, 0x01e9, 0, 0x01eb, 0, 0x01ed, 0,
        0x01ef, 0, 0, 0x01f3, 0x01f3, 0, 0x01f5, 0, 0x0195, 0x01bf, 0x01f9, 0, 0x01fb, 0, 0x01fd, 0, 0x01ff, 0,
        0x0201, 0, 0x0203, 0, 0x0205, 0, 0x0207, 0, 0x0209, 0, 0x020b, 0, 0x020d, 0, 0x020f, 0, 0x0211, 0, 0x0213,
        0, 0x0215, 0, 0x0217, 0, 0x0219, 0, 0x021b, 0, 0x021d, 0, 0x021f, 0,
        0x019e, 0, 0x0223, 0, 0x0225, 0, 0x0227, 0, 0x0229, 0, 0x022b, 0, 0x022d, 0, 0x022f, 0, 0x0231, 0, 0x0233, 0,
        0, 0, 0, 0, 0, 0, 0x2c65, 0x023c, 0, 0x019a, 0x2c66, 0, 0, 0x0242, 0, 0x0180, 0x0289, 0x028c, 0x0247, 0,
        0x0249, 0, 0x024b, 0, 0x024d, 0, 0x024f, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x03b9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x0371, 0, 0x0373, 0, 0, 0, 0x0377,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x03ac, 0, 0x03ad, 0x03ae, 0x03af, 0, 0x03cc, 0, 0x03cd,
        0x03ce, 0, 0x03b1, 0x03b2, 0x03b3, 0x03b4, 0x03b5, 0x03b6, 0x03b7, 0x03b8, 0x03b9, 0x03ba, 0x03bb, 0x03bc,
        0x03bd, 0x03be, 0x03bf,
        0x03c0, 0x03c1, 0, 0x03c3, 0x03c4, 0x03c5, 0x03c6, 0x03c7, 0x03c8, 0x03c9, 0x03ca, 0x03cb, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x03c3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x03d7,
        0x03b2, 0x03b8, 0, 0, 0, 0x03c6, 0x03c0, 0, 0x03d9, 0, 0x03db, 0, 0x03dd, 0, 0x03df, 0, 0x03e1, 0, 0x03e3,
        0, 0x03e5, 0, 0x03e7, 0, 0x03e9, 0, 0x03eb, 0, 0x03ed, 0, 0x03ef, 0, 0x03ba, 0x03c1, 0, 0, 0x03b8, 0x03b5,
        0, 0x03f8, 0, 0x03f2, 0x03fb, 0, 0, 0x037b, 0x037c, 0x037d, 0x0450, 0x0451, 0x0452, 0x0453, 0x0454,
        0x0455, 0x0456, 0x0457, 0x0458, 0x0459, 0x045a, 0x045b, 0x045c, 0x045d, 0x045e, 0x045f, 0x0430, 0x0431,
        0x0432, 0x0433, 0x0434, 0x0435, 0x0436, 0x0437, 0x0438, 0x0439, 0x043a, 0x043b, 0x043c, 0x043d, 0x043e,
        0x043f, 0x0440, 0x0441, 0x0442, 0x0443, 0x0444, 0x0445, 0x0446, 0x0447, 0x0448, 0x0449, 0x044a, 0x044b,
        0x044c, 0x044d, 0x044e, 0x044f, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x0461, 0, 0x0463, 0, 0x0465, 0,
        0x0467, 0, 0x0469, 0, 0x046b, 0, 0x046d, 0, 0x046f, 0, 0x0471, 0, 0x0473, 0, 0x0475, 0, 0x0477, 0, 0x0479,
        0, 0x047b, 0, 0x047d, 0, 0x047f, 0, 0x0481, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x048b, 0, 0x048d, 0, 0x048f, 0,
        0x0491, 0, 0x0493, 0, 0x0495, 0, 0x0497, 0, 0x0499, 0, 0x049b, 0, 0x049d, 0, 0x049f, 0, 0x04a1, 0, 0x04a3,
        0, 0x04a5, 0, 0x04a7, 0, 0x04a9, 0, 0x04ab,
        0, 0x04ad, 0, 0x04af, 0, 0x04b1, 0, 0x04b3, 0, 0x04b5, 0, 0x04b7, 0, 0x04b9, 0, 0x04bb, 0, 0x04bd, 0, 0x04bf,
        0, 0x04cf, 0x04c2, 0, 0x04c4, 0, 0x04c6, 0, 0x04c8, 0, 0x04ca, 0, 0x04cc, 0, 0x04ce, 0, 0, 0x04d1, 0,
        0x04d3, 0, 0x04d5, 0, 0x04d7, 0, 0x04d9, 0, 0x04db, 0, 0x04dd, 0, 0x04df, 0, 0x04e1, 0, 0x04e3, 0, 0x04e5,
        0, 0x04e7, 0, 0x04e9, 0, 0x04eb, 0, 0x04ed, 0, 0x04ef, 0, 0x04f1, 0, 0x04f3, 0, 0x04f5, 0, 0x04f7, 0,
        0x04f9, 0, 0x04fb, 0, 0x04fd, 0, 0x04ff, 0, 0x0501, 0, 0x0503, 0, 0x0505, 0, 0x0507, 0, 0x0509, 0, 0x050b,
        0, 0x050d, 0, 0x050f, 0, 0x0511, 0, 0x0513, 0, 0x0515, 0, 0x0517, 0, 0x0519, 0, 0x051b, 0, 0x051d, 0,
        0x051f, 0, 0x0521, 0, 0x0523, 0, 0x0525, 0, 0x0527, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x0561, 0x0562, 0x0563,
        0x0564, 0x0565, 0x0566, 0x0567, 0x0568, 0x0569, 0x056a, 0x056b, 0x056c, 0x056d, 0x056e, 0x056f, 0x0570,
        0x0571, 0x0572, 0x0573, 0x0574, 0x0575, 0x0576, 0x0577, 0x0578, 0x0579, 0x057a, 0x057b, 0x057c, 0x057d,
        0x057e, 0x057f, 0x0580, 0x0581, 0x0582, 0x0583, 0x0584, 0x0585, 0x0586
    };

    uint32_t ret;

    if (character > sizeof(lowers) / sizeof(*lowers))
        ret = character;
    else
        ret = lowers[character] == 0 ? character : lowers[character];

    return ret;
}

std::wstring utf8_to_wstring(StringData str)
{
#if REALM_HAVE_CXX11 && defined(_MSC_VER)

    // __STDC_UTF_16__ seems not to work
    REALM_STATIC_ASSERT(sizeof(wchar_t) == 2, "Expected Windows to use utf16");
    std::wstring_convert<std::codecvt_utf8_utf16<wchar_t >> utf8conv;
    auto w_result = utf8conv.from_bytes(str.data(), str.data() + str.size());
    return w_result;

#else

    // gcc 4.7 and 4.8 do not yet support codecvt_utf8_utf16 and wstring_convert, and note that we can NOT just use
    // setlocale() + mbstowcs() because setlocale is extremely slow and may change locale of the entire user process
    static_cast<void>(str);
    REALM_ASSERT(false);
    return L"";

#endif
}

// Returns bool(string1 < string2) for utf-8
bool utf8_compare(StringData string1, StringData string2)
{
    const char* s1 = string1.data();
    const char* s2 = string2.data();

    // Array such that collation_order[unichar1] < collation_order[unichar2] if-and-only-if the compare operator
    // std::locale("us_EN")(unichar1, unichar2) == true. Table generated by code snippet in src/realm/tools/unicode.cpp.
    static const uint32_t collation_order[] = {
        0, 2, 3, 4, 5, 6, 7, 8, 9, 33, 34, 35, 36, 37, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
        26, 27, 31, 38, 39, 40, 41, 42, 43, 29, 44, 45, 46, 76, 47, 30, 48, 49, 128, 132, 134, 137, 139, 140, 143,
        144, 145, 146, 50, 51, 77, 78, 79, 52, 53, 148, 182, 191, 208, 229, 263, 267, 285, 295, 325, 333, 341,
        360, 363, 385, 429, 433, 439, 454, 473, 491, 527, 531, 537, 539, 557, 54, 55, 56, 57, 58, 59, 147, 181,
        190, 207
        , 228, 262, 266, 284, 294, 324, 332, 340, 359, 362, 384, 428, 432, 438, 453, 472, 490, 526, 530, 536, 538,
        556, 60, 61, 62, 63, 28, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112,
        113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 32, 64, 72, 73, 74, 75, 65, 88,
        66, 89, 149, 81, 90, 1, 91, 67, 92, 80, 136, 138, 68, 93, 94, 95, 69, 133, 386, 82, 129, 130, 131, 70,
        153, 151, 157, 165, 575, 588, 570, 201, 233
        , 231, 237, 239, 300, 298, 303, 305, 217, 371, 390, 388, 394, 402, 584, 83, 582, 495, 493, 497, 555, 541, 487,
        470, 152, 150, 156, 164, 574, 587, 569, 200, 232, 230, 236, 238, 299, 297, 302, 304, 216, 370, 389, 387,
        393, 401, 583, 84, 581, 494, 492, 496, 554, 540, 486, 544, 163, 162, 161, 160, 167, 166, 193, 192, 197,
        196, 195, 194, 199, 198, 210, 209, 212, 211, 245, 244, 243, 242, 235, 234, 247, 246, 241, 240, 273, 272,
        277, 276, 271, 270, 279, 278, 287, 286, 291, 290, 313, 312, 311, 310, 309
        , 308, 315, 314, 301, 296, 323, 322, 328, 327, 337, 336, 434, 343, 342, 349, 348, 347, 346, 345, 344, 353,
        352, 365, 364, 373, 372, 369, 368, 375, 383, 382, 400, 399, 398, 397, 586, 585, 425, 424, 442, 441, 446,
        445, 444, 443, 456, 455, 458, 457, 462, 461, 460, 459, 477, 476, 475, 474, 489, 488, 505, 504, 503, 502,
        501, 500, 507, 506, 549, 548, 509, 508, 533, 532, 543, 542, 545, 559, 558, 561, 560, 563, 562, 471, 183,
        185, 187, 186, 189, 188, 206, 205, 204, 226, 215, 214, 213, 218, 257, 258, 259
        , 265, 264, 282, 283, 292, 321, 316, 339, 338, 350, 354, 361, 374, 376, 405, 421, 420, 423, 422, 431, 430,
        440, 468, 467, 466, 469, 480, 479, 478, 481, 524, 523, 525, 528, 553, 552, 565, 564, 571, 579, 578, 580,
        135, 142, 141, 589, 534, 85, 86, 87, 71, 225, 224, 223, 357, 356, 355, 380, 379, 378, 159, 158, 307, 306,
        396, 395, 499, 498, 518, 517, 512, 511, 516, 515, 514, 513, 256, 174, 173, 170, 169, 573, 572, 281, 280,
        275, 274, 335, 334, 404, 403, 415, 414, 577, 576, 329, 222, 221, 220, 269
        , 268, 293, 535, 367, 366, 172, 171, 180, 179, 411, 410, 176, 175, 178, 177, 253, 252, 255, 254, 318, 317,
        320, 319, 417, 416, 419, 418, 450, 449, 452, 451, 520, 519, 522, 521, 464, 463, 483, 482, 261, 260, 289,
        288, 377, 227, 427, 426, 567, 566, 155, 154, 249, 248, 409, 408, 413, 412, 392, 391, 407, 406, 547, 546,
        358, 381, 485, 326, 219, 437, 168, 203, 202, 351, 484, 465, 568, 591, 590, 184, 510, 529, 251, 250, 331,
        330, 436, 435, 448, 447, 551, 550
    };

    if (string_compare_method == STRING_COMPARE_CORE) {
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
                return remaining1 == 0;
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
                size_t collators = sizeof(collation_order) / sizeof(collation_order[0]);
                if (char1 >= collators || char2 >= collators)
                    return char1 < char2;

                uint32_t value1 = collation_order[char1];
                uint32_t value2 = collation_order[char2];

                return value1 < value2;
            }

        } while (true);
    }
    else if (string_compare_method == STRING_COMPARE_CPP11) {
        // C++11. Precise sorting in user's current locale. Arbitrary return value (silent error) for invalid utf8
#if REALM_HAVE_CXX11
        std::wstring wstring1 = utf8_to_wstring(string1);
        std::wstring wstring2 = utf8_to_wstring(string2);
        std::locale l = std::locale("");
        bool ret = l(wstring1, wstring2);
        return ret;

#else
        REALM_ASSERT(false);
        return false;

#endif
    }
    else if (string_compare_method == STRING_COMPARE_CALLBACK) {
        // Callback method
        bool ret = string_compare_callback(s1, s2);
        return ret;
    }

    REALM_ASSERT(false);
    return false;
}


// Here is a version for Windows that may be closer to what is ultimately needed.
/*
bool case_map(const char* begin, const char* end, StringBuffer& dest, bool upper)
{
const int wide_buffer_size = 32;
wchar_t wide_buffer[wide_buffer_size];

dest.resize(end-begin);
size_t dest_offset = 0;

for (;;) {
int num_out;

// Decode
{
size_t num_in = end - begin;
if (size_t(32) <= num_in) {
num_out = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, begin, 32, wide_buffer, wide_buffer_size);
if (num_out != 0) {
begin += 32;
goto convert;
}
if (GetLastError() != ERROR_INSUFFICIENT_BUFFER) return false;
}
if (num_in == 0) break;
int n = num_in < size_t(8) ? int(num_in) : 8;
num_out = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, begin, n, wide_buffer, wide_buffer_size);
if (num_out != 0) {
begin += n;
goto convert;
}
return false;
}

convert:
if (upper) {
for (int i=0; i<num_out; ++i) {
CharUpperW(wide_buffer + i);
}
}
else {
for (int i=0; i<num_out; ++i) {
CharLowerW(wide_buffer + i);
}
}

encode:
{
size_t free = dest.size() - dest_offset;
if (int_less_than(std::numeric_limits<int>::max(), free)) free = std::numeric_limits<int>::max();
int n = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, wide_buffer, num_out,
dest.data() + dest_offset, int(free), 0, 0);
if (i != 0) {
dest_offset += n;
continue;
}
if (GetLastError() != ERROR_INSUFFICIENT_BUFFER) return false;
size_t dest_size = dest.size();
if (int_multiply_with_overflow_detect(dest_size, 2)) {
if (dest_size == std::numeric_limits<size_t>::max()) return false;
dest_size = std::numeric_limits<size_t>::max();
}
dest.resize(dest_size);
goto encode;
}
}

dest.resize(dest_offset);
return true;
}
*/


// Converts UTF-8 source into upper or lower case. This function
// preserves the byte length of each UTF-8 character in following way:
// If an output character differs in size, it is simply substituded by
// the original character. This may of course give wrong search
// results in very special cases. Todo.
std::string case_map(StringData source, bool upper)
{
    char* dst = new char[source.size()];
    case_map(source, dst, upper);
    std::string str(dst, source.size());
    delete[] dst;
    return str;
}

bool case_map(StringData source, char* target, bool upper)
{
#ifdef _WIN32
    const char* begin = source.data();
    const char* end = begin + source.size();
    while (begin != end) {
        int n = static_cast<int>(sequence_length(*begin));
        if (n == 0 || end - begin < n)
            return false;

        wchar_t tmp[2];     // FIXME: Why no room for UTF-16 surrogate


        int n2 = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, begin, n, tmp, 1);
        if (n2 == 0)
            return false;

        REALM_ASSERT(0 < n2 && n2 <= 1);
        tmp[n2] = 0;

        // Note: If tmp[0] == 0, it is because the string contains a
        // null-chacarcter, which is perfectly fine.

        if (upper)
            CharUpperW(static_cast<LPWSTR>(tmp));
        else
            CharLowerW(static_cast<LPWSTR>(tmp));

        // FIXME: The intention is to use flag 'WC_ERR_INVALID_CHARS'
        // to catch invalid UTF-8. Even though the documentation says
        // unambigously that it is supposed to work, it doesn't. When
        // the flag is specified, the function fails with error
        // ERROR_INVALID_FLAGS.
        DWORD flags = 0;
        int n3 = WideCharToMultiByte(CP_UTF8, flags, tmp, 1, target, static_cast<int>(end - begin), 0, 0);
        if (n3 == 0 && GetLastError() != ERROR_INSUFFICIENT_BUFFER)
            return false;

        if (n3 != n) {
            std::copy(begin, begin + n, target);     // Cannot handle different size, copy source
        }

        begin += n;
        target += n;
    }

    return true;

#else

    // FIXME: Implement this! Note that this is trivial in C++11 due
    // to its built-in support for UTF-8. In C++03 it is trivial when
    // __STDC_ISO_10646__ is defined. Also consider using ICU. Maybe
    // GNU has something to offer too.

    // For now we handle just the ASCII subset
    typedef std::char_traits<char> traits;
    if (upper) {
        size_t n = source.size();
        for (size_t i = 0; i<n; ++i) {
            char c = source[i];
            if (traits::lt(0x60, c) &&
                traits::lt(c, 0x7B))
                c = traits::to_char_type(traits::to_int_type(c) - 0x20);
            target[i] = c;
        }
    }
    else {     // lower
        size_t n = source.size();
        for (size_t i = 0; i<n; ++i) {
            char c = source[i];
            if (traits::lt(0x40, c) &&
                traits::lt(c, 0x5B))
                c = traits::to_char_type(traits::to_int_type(c) + 0x20);
            target[i] = c;
        }
    }

    return true;

#endif
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
size_t search_case_fold(StringData haystack, const char* needle_upper, const char* needle_lower,
                        size_t needle_size)
{
    // FIXME: This solution is terribly inefficient. Consider deploying the Boyer-Moore algorithm.
    size_t i = 0;
    while (needle_size <= haystack.size() - i) {
        if (equal_case_fold(haystack.substr(i, needle_size), needle_upper, needle_lower)) {
            return i;
        }
        ++i;
    }
    return haystack.size();     // Not found
}


} // namespace realm
