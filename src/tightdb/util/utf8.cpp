#include <cstring>
#include <algorithm>

#ifdef _WIN32
    #define NOMINMAX
    #include <windows.h>
#else
    #include <ctype.h>
#endif

#include <tightdb/util/safe_int_ops.hpp>
#include <tightdb/util/utf8.hpp>

using namespace std;

namespace {

#ifdef _WIN32
// Returns the number of bytes in a UTF-8 sequence whose leading byte
// is as specified.
inline int sequence_length(char lead)
{
    int lead2 = char_traits<char>::to_int_type(lead);
    if ((lead2 & 0x80) == 0) return 1;
    if ((lead2 & 0x40) == 0) return 0; // Error
    if ((lead2 & 0x20) == 0) return 2;
    if ((lead2 & 0x10) == 0) return 3;
    if ((lead2 & 0x08) == 0) return 4;
    if ((lead2 & 0x04) == 0) return 5;
    if ((lead2 & 0x02) == 0) return 6;
    return 0; // Error
}
#endif

// Check if the next UTF-8 sequence in [begin, end) is identical to
// the one beginning at begin2. If it is, 'begin' is advanced
// accordingly.
inline bool equal_sequence(const char*& begin, const char* end, const char* begin2)
{
    if (begin[0] != begin2[0]) return false;

    size_t i = 1;
    if (int(char_traits<char>::to_int_type(begin[0])) & 0x80) {
        // All following bytes matching '10xxxxxx' will be considered
        // as part of this character.
        while (begin + i != end) {
            if ((int(char_traits<char>::to_int_type(begin[i])) & (0x80 + 0x40)) != 0x80) break;
            if (begin[i] != begin2[i]) return false;
            ++i;
        }
    }

    begin += i;
    return true;
}

} // anonymous namespace


namespace tightdb {



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
            if (int_less_than(numeric_limits<int>::max(), free)) free = numeric_limits<int>::max();
            int n = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, wide_buffer, num_out,
                                        dest.data() + dest_offset, int(free), 0, 0);
            if (i != 0) {
                dest_offset += n;
                continue;
            }
            if (GetLastError() != ERROR_INSUFFICIENT_BUFFER) return false;
            size_t dest_size = dest.size();
            if (int_multiply_with_overflow_detect(dest_size, 2)) {
                if (dest_size == numeric_limits<size_t>::max()) return false;
                dest_size = numeric_limits<size_t>::max();
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
bool case_map(StringData source, char* target, bool upper)
{
#ifdef _WIN32
    const char* begin = source.data();
    const char* end = begin + source.size();
    while (begin != end) {
        int n = sequence_length(*begin);
        if (n == 0 || end-begin < n) return false;

        wchar_t tmp[2]; // FIXME: Why no room for UTF-16 surrogate?

        int n2 = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, begin, n, tmp, 1);
        if (n2 == 0) return false;

        TIGHTDB_ASSERT(0 < n2 && n2 <= 1);
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
        int n3 = WideCharToMultiByte(CP_UTF8, flags, tmp, 1, target, int(end-begin), 0, 0);
        if (n3 == 0 && GetLastError() != ERROR_INSUFFICIENT_BUFFER) return false;
        if (n3 != n) {
            copy(begin, begin+n, target); // Cannot handle different size, copy source
        }

        begin  += n;
        target += n;
    }

    return true;
#else
    // FIXME: Implement this! Note that this is trivial in C++11 due
    // to its built-in support for UTF-8. In C++03 it is trivial when
    // __STDC_ISO_10646__ is defined. Also consider using ICU. Maybe
    // GNU has something to offer too.

    // For now we handle just the ASCII subset
    typedef char_traits<char> traits;
    if (upper) {
        size_t n = source.size();
        for (size_t i=0; i<n; ++i) {
            char c = source[i];
            if (traits::lt(0x60, c) &&
                traits::lt(c, 0x7B)) c = traits::to_char_type(traits::to_int_type(c)-0x20);
            target[i] = c;
        }
    }
    else { // lower
        size_t n = source.size();
        for (size_t i=0; i<n; ++i) {
            char c = source[i];
            if (traits::lt(0x40, c) &&
                traits::lt(c, 0x5B)) c = traits::to_char_type(traits::to_int_type(c)+0x20);
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
    for (size_t i=0; i!=haystack.size(); ++i) {
        char c = haystack[i];
        if (needle_lower[i] != c && needle_upper[i] != c) return false;
    }

    const char* begin = haystack.data();
    const char* end   = begin + haystack.size();
    const char* i = begin;
    while (i != end) {
        if (!equal_sequence(i, end, needle_lower + (i - begin)) &&
            !equal_sequence(i, end, needle_upper + (i - begin))) return false;
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
    return haystack.size(); // Not found
}


} // namespace tightdb
