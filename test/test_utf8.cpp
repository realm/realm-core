#include "testsettings.hpp"
#ifdef TEST_UTF8

#include <cstddef>
#include <limits>
#include <stdexcept>
#include <string>
#include <iostream>

#include <tightdb/util/assert.hpp>
#include <memory>
#include <tightdb/util/utf8.hpp>
#include <tightdb/unicode.hpp>

#include "test.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::util;
using namespace tightdb::test_util;

// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment varible `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.


namespace {

//const char* uY = "\x0CE\x0AB";              // greek capital letter upsilon with dialytika (U+03AB)
//const char* uYd = "\x0CE\x0A5\x0CC\x088";    // decomposed form (Y followed by two dots)
//const char* uy = "\x0CF\x08B";              // greek small letter upsilon with dialytika (U+03AB)
//const char* uyd = "\x0cf\x085\x0CC\x088";    // decomposed form (Y followed by two dots)

const char* uA = "\x0c3\x085";         // danish capital A with ring above (as in BLAABAERGROED)
//const char* uAd = "\x041\x0cc\x08a";    // decomposed form (A (41) followed by ring)
const char* ua = "\x0c3\x0a5";         // danish lower case a with ring above (as in blaabaergroed)
//const char* uad = "\x061\x0cc\x08a";    // decomposed form (a (41) followed by ring)

const char* uAE = "\xc3\x86"; // danish upper case AE
const char* uae = "\xc3\xa6"; // danish lower case ae

const char* u16sur = "\xF0\xA0\x9C\x8E"; // chineese needing utf16 surrogate pair
const char* u16sur2 = "\xF0\xA0\x9C\xB1"; // same as above, with larger unicode

TEST(UTF_Fuzzy_utf8_to_utf16)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    const size_t size = 10;
    char in[size];
    int16_t out[size];

    for (size_t iter = 0; iter < 1000000; iter++) {
        for (size_t t = 0; t < size; t++) {
            in[t] = random.draw_int<char>();
        }

        const char* in2 = in;
        size_t needed = Utf8x16<int16_t>::find_utf16_buf_size(in2, in + size);
        size_t read = in2 - in;

        // number of utf16 codepoints should not exceed number of utf8 codepoints
        CHECK(needed <= size);
        
        // we should not read beyond input buffer
        CHECK(read <= size);

        int16_t* out2 = out;
        in2 = in;
        Utf8x16<int16_t>::to_utf16(in2, in2 + read, out2, out2 + needed);
        size_t read2 = in2 - in;
        size_t written = out2 - out;

        CHECK(read2 <= size);
        CHECK(written <= needed);
    }
}


TEST(UTF8_Compare_Core_ASCII) {
    // Useful line for creating new unit test cases:
    // bool ret = std::locale("us_EN")(string("a"), string("b"));

    set_string_compare_method(STRING_COMPARE_CORE, null_ptr);

    // simplest test
    CHECK_EQUAL(true, utf8_compare("a", "b"));
    CHECK_EQUAL(false, utf8_compare("b", "a"));
    CHECK_EQUAL(false, utf8_compare("a", "a"));

    // length makes a difference
    CHECK_EQUAL(true, utf8_compare("aaaa", "b"));
    CHECK_EQUAL(true, utf8_compare("a", "bbbb"));

    CHECK_EQUAL(true, utf8_compare("a", "aaaa"));
    CHECK_EQUAL(false, utf8_compare("aaaa", "a"));

    // change one letter to upper case; must sort the same
    CHECK_EQUAL(true, utf8_compare("A", "b"));
    CHECK_EQUAL(false, utf8_compare("b", "A"));
    CHECK_EQUAL(false, utf8_compare("A", "A"));

    CHECK_EQUAL(true, utf8_compare("AAAA", "b"));
    CHECK_EQUAL(true, utf8_compare("A", "b"));

    CHECK_EQUAL(false, utf8_compare("A", "aaaa"));
    CHECK_EQUAL(false, utf8_compare("AAAA", "a"));

    // change other letter to upper case; must still sort the same
    CHECK_EQUAL(true, utf8_compare("a", "B"));
    CHECK_EQUAL(false, utf8_compare("B", "a"));

    CHECK_EQUAL(true, utf8_compare("aaaa", "B"));
    CHECK_EQUAL(true, utf8_compare("a", "BBBB"));

    CHECK_EQUAL(true, utf8_compare("a", "AAAA"));
    CHECK_EQUAL(true, utf8_compare("aaaa", "A"));

    // now test casing for same letters
    CHECK_EQUAL(true, utf8_compare("a", "A"));
    CHECK_EQUAL(false, utf8_compare("A", "a"));

    // length is same, but string1 is lower case; string1 comes first
    CHECK_EQUAL(true, utf8_compare("aaaa", "AAAA"));
    CHECK_EQUAL(false, utf8_compare("AAAA", "aaaa"));

    // string2 is shorter, but string1 is lower case; lower case comes fist
    CHECK_EQUAL(true, utf8_compare("aaaa", "A"));
    CHECK_EQUAL(false, utf8_compare("A", "aaaa"));
}


TEST(UTF8_Compare_Core_utf8)
{
    // Useful line for creating new unit test cases:
    // bool ret = std::locale("us_EN")(string("a"), string("b"));

    set_string_compare_method(STRING_COMPARE_CORE, null_ptr);

    // single utf16 code points (tests mostly Windows)
    CHECK_EQUAL(false, utf8_compare(uae, uae));
    CHECK_EQUAL(false, utf8_compare(uAE, uAE));

    CHECK_EQUAL(true, utf8_compare(uae, ua));
    CHECK_EQUAL(false, utf8_compare(ua, uae));

    CHECK_EQUAL(false, utf8_compare(uAE, uae));

    CHECK_EQUAL(true, utf8_compare(uae, uA));
    CHECK_EQUAL(false, utf8_compare(uA, uAE));

    // char needing utf16 surrogate pair (tests mostly windows because *nix uses utf32 as wchar_t). These are symbols
    // that are beyond 'Latin Extended 2' (0...591), where 'compare_method 0' will sort them by unicode value instead.
    // Test where one char is surrogate, and other is non-surrogate
    CHECK_EQUAL(true, utf8_compare(uA, u16sur));
    CHECK_EQUAL(false, utf8_compare(u16sur, uA));
    CHECK_EQUAL(false, utf8_compare(u16sur, u16sur));

    // Test where both are surrogate
    CHECK_EQUAL(true, utf8_compare(u16sur, u16sur2));
    CHECK_EQUAL(false, utf8_compare(u16sur2, u16sur2));
    CHECK_EQUAL(false, utf8_compare(u16sur2, u16sur2));
}

TEST(UTF8_Compare_Core_utf8_invalid)
{
    // Test that invalid utf8 won't make decisions on data beyond Realm payload. Do that by placing an utf8 header that
    // indicate 5 octets will follow, and put spurious1 and spurious2 after them to see if Realm will access these too
    // and make sorting decisions on them. Todo: This does not guarantee that spurious data access does not happen;
    // todo: make unit test that attempts to trigger segfault near a page limit instead. 
    char invalid1[] = "\xfc";
    char spurious1[] = "aaaaaaaaaaaaaaaa";
    char invalid2[] = "\xfc";
    char spurious2[] = "bbbbbbbbbbbbbbbb";

    static_cast<void>(spurious1);
    static_cast<void>(spurious2);

    set_string_compare_method(STRING_COMPARE_CORE, null_ptr);
    StringData i1 = StringData(invalid1);
    StringData i2 = StringData(invalid2);

    // strings must be seen as 'equal' because they terminate when StringData::size is reached. Futhermore, we state
    // that return value is arbitrary for invalid utf8
    bool ret = utf8_compare(i1, i2);
    CHECK_EQUAL(ret, utf8_compare(i2, i1)); // must sort the same as before regardless of succeeding data
}
/* shows uninitialized data access i Valgrind (by design). Disabled until supressed or we find another way to test
TEST(Compare_Core_utf8_invalid_crash)
{
    // See if we can crash Realm with random data
    char str1[20];
    char str2[20];
    using namespace tightdb::test_util;
    Random r;

    set_string_compare_method(STRING_COMPARE_CORE, null_ptr);

    for (size_t t = 0; t < 10000; t++) {
        for (size_t i = 0; i < sizeof(str1); i++) {
            str1[i] = r.draw_int(0, 255);
            str2[i] = r.draw_int(0, 255);
        }
        utf8_compare(str1, str2);
        utf8_compare(str2, str1);
    }
}
*/
TEST(UTF8_Compare_Core_utf8_zero)
{
    // Realm must support 0 characters in utf8 strings
    CHECK_EQUAL(false, utf8_compare(StringData("\0", 1), StringData("\0", 1)));
    CHECK_EQUAL(true, utf8_compare(StringData("\0", 1), StringData("a")));
    CHECK_EQUAL(false, utf8_compare("a", StringData("\0", 1)));

    // 0 in middle of strings
    CHECK_EQUAL(true, utf8_compare(StringData("a\0a", 3), StringData("a\0b", 3)));
    CHECK_EQUAL(false, utf8_compare(StringData("a\0b", 3), StringData("a\0a", 3)));
    CHECK_EQUAL(false, utf8_compare(StringData("a\0a", 3), StringData("a\0a", 3)));

    // Number of trailing 0 makes a difference
    CHECK_EQUAL(true, utf8_compare(StringData("a\0", 2), StringData("a\0\0", 3)));
    CHECK_EQUAL(false, utf8_compare(StringData("a\0\0", 3), StringData("a\0", 2)));
}

template<class Int> struct IntChar {
    typedef Int int_type;
    Int m_value;
};

template<class Int> bool operator<(IntChar<Int> a, IntChar<Int> b)
{
    return a.m_value < b.m_value;
}

template<class Char, class Int> struct IntCharTraits: private char_traits<Char> {
    typedef Char char_type;
    typedef Int  int_type;
    typedef typename char_traits<Char>::off_type off_type;
    typedef typename char_traits<Char>::pos_type pos_type;
    static Int to_int_type(Char c)  { return c.m_value; }
    static Char to_char_type(Int i) { Char c; c.m_value = typename Char::int_type(i); return c; }
    static bool eq_int_type(Int i1, Int i2) { return i1 == i2; }
    static Int eof() { return numeric_limits<Int>::max(); }
    static Int not_eof(Int i) { return i != eof() ? i : Int(); }
    using char_traits<Char>::assign;
    using char_traits<Char>::eq;
    using char_traits<Char>::lt;
    using char_traits<Char>::move;
    using char_traits<Char>::copy;
    using char_traits<Char>::compare;
    using char_traits<Char>::length;
    using char_traits<Char>::find;
};


// Assumes that lower case 'a', 'b', 'c', 'd', 'e', and 'f' have
// consecutive integer values. Likewise for the corresponding capital
// letters. Note that is is not guaranteed by C++11.
int decode_hex_digit(char hex_digit)
{
    typedef char_traits<char> traits;
    int v = traits::to_int_type(hex_digit);
    if (traits::to_int_type('0') <= v && v <= traits::to_int_type('9'))
        return v - traits::to_int_type('0');
    if (traits::to_int_type('A') <= v && v <= traits::to_int_type('F'))
        return 10 + (v - traits::to_int_type('A'));
    if (traits::to_int_type('a') <= v && v <= traits::to_int_type('f'))
        return 10 + (v - traits::to_int_type('a'));
    throw runtime_error("Bad hex digit");
}

char encode_hex_digit(int value)
{
    typedef char_traits<char> traits;
    if (0 <= value) {
        if (value < 10) return traits::to_char_type(traits::to_int_type('0') + value);
        if (value < 16) return traits::to_char_type(traits::to_int_type('A') + (value-10));
    }
    throw runtime_error("Bad hex digit value");
}


string decode_8bit_hex(const string& hex)
{
    string s;
    s.reserve(hex.size() / 2);
    const char* begin = hex.data();
    const char* end = begin + hex.size();
    for (const char* i = begin; i != end; ++i) {
        char digit_1 = *i;
        if (++i == end) throw runtime_error("Incomplete 8-bit element");
        char digit_2 = *i;
        int value = 16 * decode_hex_digit(digit_1) + decode_hex_digit(digit_2);
        s += char_traits<char>::to_char_type(value);
    }
    return s;
}

string encode_8bit_hex(const string& bin)
{
    string s;
    s.reserve(bin.size() * 2);
    const char* begin = bin.data();
    const char* end = begin + bin.size();
    for (const char* i = begin; i != end; ++i) {
        int value = char_traits<char>::to_int_type(*i);
        s.push_back(encode_hex_digit(value / 16));
        s.push_back(encode_hex_digit(value % 16));
    }
    return s;
}


template<class String16> String16 decode_16bit_hex(const string& hex)
{
    String16 s;
    s.reserve(hex.size() / 4);
    const char* begin = hex.data();
    const char* end = begin + hex.size();
    for (const char* i = begin; i != end; ++i) {
        char digit_1 = *i;
        if (++i == end) throw runtime_error("Incomplete 16-bit element");
        char digit_2 = *i;
        if (++i == end) throw runtime_error("Incomplete 16-bit element");
        char digit_3 = *i;
        if (++i == end) throw runtime_error("Incomplete 16-bit element");
        char digit_4 = *i;
        long value =
            4096L * decode_hex_digit(digit_1) +
            256   * decode_hex_digit(digit_2) +
            16    * decode_hex_digit(digit_3) +
            1     * decode_hex_digit(digit_4);
        typedef typename String16::traits_type Traits16;
        s += Traits16::to_char_type(value);
    }

    return s;
}

template<class String16> string encode_16bit_hex(const String16& bin)
{
    string s;
    s.reserve(bin.size() * 4);
    typedef typename String16::traits_type Traits16;
    typedef typename Traits16::char_type   Char16;
    const Char16* begin = bin.data();
    const Char16* end = begin + bin.size();
    for (const Char16* i = begin; i != end; ++i) {
        long value = Traits16::to_int_type(*i);
        s.push_back(encode_hex_digit(int(value / 4096)));
        s.push_back(encode_hex_digit(int(value / 256) % 16));
        s.push_back(encode_hex_digit(int(value / 16) % 16));
        s.push_back(encode_hex_digit(int(value) % 16));
    }
    return s;
}


template<class String16> String16 utf8_to_utf16(const string& s)
{
    typedef typename String16::traits_type Traits16;
    typedef typename Traits16::char_type   Char16;
    typedef Utf8x16<Char16, Traits16> Xcode;
    const char* in_begin = s.data();
    const char* in_end = in_begin + s.size();
    size_t utf16_buf_size = Xcode::find_utf16_buf_size(in_begin, in_end);
    if (in_begin != in_end) throw runtime_error("Bad UTF-8");
    in_begin = s.data();
    std::unique_ptr<Char16[]> utf16_buf(new Char16[utf16_buf_size]);
    Char16* out_begin = utf16_buf.get();
    Char16* out_end = out_begin + utf16_buf_size;
    bool valid_utf8 = Xcode::to_utf16(in_begin, in_end, out_begin, out_end);
    TIGHTDB_ASSERT(valid_utf8);
    static_cast<void>(valid_utf8);
    TIGHTDB_ASSERT(in_begin == in_end);
    return String16(utf16_buf.get(), out_begin);
}

template<class String16> string utf16_to_utf8(const String16& s)
{
    typedef typename String16::traits_type Traits16;
    typedef typename Traits16::char_type   Char16;
    typedef Utf8x16<Char16, Traits16> Xcode;
    const Char16* in_begin = s.data();
    const Char16* in_end = in_begin + s.size();
    size_t utf8_buf_size = Xcode::find_utf8_buf_size(in_begin, in_end);
    if (in_begin != in_end) throw runtime_error("Bad UTF-16");
    in_begin = s.data();
    std::unique_ptr<char[]> utf8_buf(new char[utf8_buf_size]);
    char* out_begin = utf8_buf.get();
    char* out_end = out_begin + utf8_buf_size;
    bool valid_utf16 = Xcode::to_utf8(in_begin, in_end, out_begin, out_end);
    TIGHTDB_ASSERT(valid_utf16);
    static_cast<void>(valid_utf16);
    TIGHTDB_ASSERT(in_begin == in_end);
    return string(utf8_buf.get(), out_begin);
}


size_t find_buf_size_utf8_to_utf16(const string& s)
{
    typedef Utf8x16<char> Xcode;
    const char* in_begin = s.data();
    const char* in_end = in_begin + s.size();
    size_t size = Xcode::find_utf16_buf_size(in_begin, in_end);
    if (in_begin != in_end) throw runtime_error("Bad UTF-8");
    return size;
}

template<class String16> size_t find_buf_size_utf16_to_utf8(const String16& s)
{
    typedef typename String16::traits_type Traits16;
    typedef typename Traits16::char_type   Char16;
    typedef Utf8x16<Char16, Traits16> Xcode;
    const Char16* in_begin = s.data();
    const Char16* in_end = in_begin + s.size();
    size_t size = Xcode::find_utf8_buf_size(in_begin, in_end);
    if (in_begin != in_end) throw runtime_error("Bad UTF-16");
    return size;
}

} // anonymous namespace



// FIXME: For some reason, these tests do not compile under VisualStudio

#ifndef _WIN32

TEST(UTF8_TranscodeUtf16)
{
    typedef IntChar<int>                   Char16;
    typedef IntCharTraits<Char16, long>    Traits16;
    typedef basic_string<Char16, Traits16> String16;

    // Try a trivial string first
    {
        string utf8 = "Lorem ipsum. The quick brown fox jumps over the lazy dog.";
        const char* utf16_hex =
            "004C006F00720065006D00200069007000730075006D002E0020005400680065"
            "00200071007500690063006B002000620072006F0077006E00200066006F0078"
            "0020006A0075006D007000730020006F00760065007200200074006800650020"
            "006C0061007A007900200064006F0067002E";
        CHECK_EQUAL(char_traits<char>::length(utf16_hex),
                    find_buf_size_utf8_to_utf16(utf8) * 4);
        String16 utf16 = decode_16bit_hex<String16>(utf16_hex);
        CHECK_EQUAL(utf8.size(), find_buf_size_utf16_to_utf8(utf16));
        CHECK(utf16 == utf8_to_utf16<String16>(utf8));
        CHECK(utf8 == utf16_to_utf8(utf16));
    }

    // Now try a harder one (contains characters beyond U+FFFF)
    {
        const char* utf8_hex =
            "EFA4A5EFA49BF0A08080EFA4A7EFA491F0A08081EFA4A1C3A6C3B8C3A5EFA497"
            "EFA4A3F0A08082F0A08083666F6FF0A08084EFA495F0A08085F0A08086EFA493"
            "F0A08087F0A08088F0A08089F0A0808AEFA49DF0A0808BF0A0808CF0A0808DEF"
            "A49FF0A0808EF0A0808FEFA48F";
        const char* utf16_hex =
            "F925F91BD840DC00F927F911D840DC01F92100E600F800E5F917F923D840DC02"
            "D840DC030066006F006FD840DC04F915D840DC05D840DC06F913D840DC07D840"
            "DC08D840DC09D840DC0AF91DD840DC0BD840DC0CD840DC0DF91FD840DC0ED840"
            "DC0FF90F";
        string utf8 = decode_8bit_hex(utf8_hex);
        CHECK_EQUAL(char_traits<char>::length(utf16_hex),
                    find_buf_size_utf8_to_utf16(utf8) * 4);
        String16 utf16 = decode_16bit_hex<String16>(utf16_hex);
        CHECK_EQUAL(char_traits<char>::length(utf8_hex),
                    find_buf_size_utf16_to_utf8(utf16) * 2);
        CHECK(utf16 == utf8_to_utf16<String16>(utf8));
        CHECK(utf8 == utf16_to_utf8(utf16));
    }

    CHECK_EQUAL("41", encode_8bit_hex("A")); // Avoid 'unused function' warning
}

#endif // _WIN32

#endif // TEST_UTF8
