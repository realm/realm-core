#include "testsettings.hpp"
#ifdef TEST_UTIL_UTF8

#include <cstddef>
#include <limits>
#include <stdexcept>
#include <string>
#include <iostream>
#include <memory>

#include <realm/util/assert.hpp>
#include <realm/util/utf8.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;

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

template<class Int> struct IntChar {
    typedef Int int_type;
    Int m_value;
};

template<class Int> bool operator<(IntChar<Int> a, IntChar<Int> b)
{
    return a.m_value < b.m_value;
}

template<class Char, class Int> struct IntCharTraits: private std::char_traits<Char> {
    typedef Char char_type;
    typedef Int  int_type;
    typedef typename std::char_traits<Char>::off_type off_type;
    typedef typename std::char_traits<Char>::pos_type pos_type;
    static Int to_int_type(Char c)  { return c.m_value; }
    static Char to_char_type(Int i) { Char c; c.m_value = typename Char::int_type(i); return c; }
    static bool eq_int_type(Int i1, Int i2) { return i1 == i2; }
    static Int eof() { return std::numeric_limits<Int>::max(); }
    static Int not_eof(Int i) { return i != eof() ? i : Int(); }
    using std::char_traits<Char>::assign;
    using std::char_traits<Char>::eq;
    using std::char_traits<Char>::lt;
    using std::char_traits<Char>::move;
    using std::char_traits<Char>::copy;
    using std::char_traits<Char>::compare;
    using std::char_traits<Char>::length;
    using std::char_traits<Char>::find;
};


// Assumes that lower case 'a', 'b', 'c', 'd', 'e', and 'f' have
// consecutive integer values. Likewise for the corresponding capital
// letters. Note that is is not guaranteed by C++11.
int decode_hex_digit(char hex_digit)
{
    typedef std::char_traits<char> traits;
    int v = traits::to_int_type(hex_digit);
    if (traits::to_int_type('0') <= v && v <= traits::to_int_type('9'))
        return v - traits::to_int_type('0');
    if (traits::to_int_type('A') <= v && v <= traits::to_int_type('F'))
        return 10 + (v - traits::to_int_type('A'));
    if (traits::to_int_type('a') <= v && v <= traits::to_int_type('f'))
        return 10 + (v - traits::to_int_type('a'));
    throw std::runtime_error("Bad hex digit");
}

char encode_hex_digit(int value)
{
    typedef std::char_traits<char> traits;
    if (0 <= value) {
        if (value < 10) return traits::to_char_type(traits::to_int_type('0') + value);
        if (value < 16) return traits::to_char_type(traits::to_int_type('A') + (value-10));
    }
    throw std::runtime_error("Bad hex digit value");
}


std::string decode_8bit_hex(const std::string& hex)
{
    std::string s;
    s.reserve(hex.size() / 2);
    const char* begin = hex.data();
    const char* end = begin + hex.size();
    for (const char* i = begin; i != end; ++i) {
        char digit_1 = *i;
        if (++i == end) throw std::runtime_error("Incomplete 8-bit element");
        char digit_2 = *i;
        int value = 16 * decode_hex_digit(digit_1) + decode_hex_digit(digit_2);
        s += std::char_traits<char>::to_char_type(value);
    }
    return s;
}

std::string encode_8bit_hex(const std::string& bin)
{
    std::string s;
    s.reserve(bin.size() * 2);
    const char* begin = bin.data();
    const char* end = begin + bin.size();
    for (const char* i = begin; i != end; ++i) {
        int value = std::char_traits<char>::to_int_type(*i);
        s.push_back(encode_hex_digit(value / 16));
        s.push_back(encode_hex_digit(value % 16));
    }
    return s;
}


template<class String16> String16 decode_16bit_hex(const std::string& hex)
{
    String16 s;
    s.reserve(hex.size() / 4);
    const char* begin = hex.data();
    const char* end = begin + hex.size();
    for (const char* i = begin; i != end; ++i) {
        char digit_1 = *i;
        if (++i == end) throw std::runtime_error("Incomplete 16-bit element");
        char digit_2 = *i;
        if (++i == end) throw std::runtime_error("Incomplete 16-bit element");
        char digit_3 = *i;
        if (++i == end) throw std::runtime_error("Incomplete 16-bit element");
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

template<class String16> std::string encode_16bit_hex(const String16& bin)
{
    std::string s;
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


template<class String16> String16 utf8_to_utf16(const std::string& s)
{
    typedef typename String16::traits_type Traits16;
    typedef typename Traits16::char_type   Char16;
    typedef Utf8x16<Char16, Traits16> Xcode;
    const char* in_begin = s.data();
    const char* in_end = in_begin + s.size();
    size_t utf16_buf_size = Xcode::utf8_find_utf16_buf_size(in_begin, in_end);
    if (in_begin != in_end) throw std::runtime_error("Bad UTF-8");
    in_begin = s.data();
    std::unique_ptr<Char16[]> utf16_buf(new Char16[utf16_buf_size]);
    Char16* out_begin = utf16_buf.get();
    Char16* out_end = out_begin + utf16_buf_size;
    bool valid_utf8 = Xcode::utf8_to_utf16(in_begin, in_end, out_begin, out_end);
    REALM_ASSERT(valid_utf8);
    static_cast<void>(valid_utf8);
    REALM_ASSERT(in_begin == in_end);
    return String16(utf16_buf.get(), out_begin);
}

template<class String16> std::string utf16_to_utf8(const String16& s)
{
    typedef typename String16::traits_type Traits16;
    typedef typename Traits16::char_type   Char16;
    typedef Utf8x16<Char16, Traits16> Xcode;
    const Char16* in_begin = s.data();
    const Char16* in_end = in_begin + s.size();
    size_t utf8_buf_size = Xcode::utf16_find_utf8_buf_size(in_begin, in_end);
    if (in_begin != in_end) throw std::runtime_error("Bad UTF-16");
    in_begin = s.data();
    std::unique_ptr<char[]> utf8_buf(new char[utf8_buf_size]);
    char* out_begin = utf8_buf.get();
    char* out_end = out_begin + utf8_buf_size;
    bool valid_utf16 = Xcode::utf16_to_utf8(in_begin, in_end, out_begin, out_end);
    REALM_ASSERT(valid_utf16);
    static_cast<void>(valid_utf16);
    REALM_ASSERT(in_begin == in_end);
    return std::string(utf8_buf.get(), out_begin);
}


size_t find_buf_size_utf8_to_utf16(const std::string& s)
{
    typedef Utf8x16<char> Xcode;
    const char* in_begin = s.data();
    const char* in_end = in_begin + s.size();
    size_t size = Xcode::utf8_find_utf16_buf_size(in_begin, in_end);
    if (in_begin != in_end) throw std::runtime_error("Bad UTF-8");
    return size;
}

template<class String16> size_t find_buf_size_utf16_to_utf8(const String16& s)
{
    typedef typename String16::traits_type Traits16;
    typedef typename Traits16::char_type   Char16;
    typedef Utf8x16<Char16, Traits16> Xcode;
    const Char16* in_begin = s.data();
    const Char16* in_end = in_begin + s.size();
    size_t size = Xcode::utf16_find_utf8_buf_size(in_begin, in_end);
    if (in_begin != in_end) throw std::runtime_error("Bad UTF-16");
    return size;
}

} // anonymous namespace


TEST(Utf8_TranscodeUtf16)
{
    typedef IntChar<int>                        Char16;
    typedef IntCharTraits<Char16, long>         Traits16;
    typedef std::basic_string<Char16, Traits16> String16;

    // Try a trivial string first
    {
        std::string utf8 = "Lorem ipsum. The quick brown fox jumps over the lazy dog.";
        const char* utf16_hex =
            "004C006F00720065006D00200069007000730075006D002E0020005400680065"
            "00200071007500690063006B002000620072006F0077006E00200066006F0078"
            "0020006A0075006D007000730020006F00760065007200200074006800650020"
            "006C0061007A007900200064006F0067002E";
        CHECK_EQUAL(std::char_traits<char>::length(utf16_hex),
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
        std::string utf8 = decode_8bit_hex(utf8_hex);
        CHECK_EQUAL(std::char_traits<char>::length(utf16_hex),
                    find_buf_size_utf8_to_utf16(utf8) * 4);
        String16 utf16 = decode_16bit_hex<String16>(utf16_hex);
        CHECK_EQUAL(std::char_traits<char>::length(utf8_hex),
                    find_buf_size_utf16_to_utf8(utf16) * 2);
        CHECK(utf16 == utf8_to_utf16<String16>(utf8));
        CHECK(utf8 == utf16_to_utf8(utf16));
    }

    CHECK_EQUAL("41", encode_8bit_hex("A")); // Avoid 'unused function' warning
}


TEST(Utf8_FuzzyUtf8ToUtf16)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    const size_t size = 10;
    char in[size];
    int16_t out[size];

    for (size_t iter = 0; iter < 1000000; iter++) {
        for (size_t t = 0; t < size; t++) {
            in[t] = static_cast<char>(random.draw_int<int>());
        }

        const char* in2 = in;
        size_t needed = Utf8x16<int16_t>::utf8_find_utf16_buf_size(in2, in + size);
        size_t read = in2 - in;

        // number of utf16 codepoints should not exceed number of utf8 codepoints
        CHECK(needed <= size);

        // we should not read beyond input buffer
        CHECK(read <= size);

        int16_t* out2 = out;
        in2 = in;
        Utf8x16<int16_t>::utf8_to_utf16(in2, in2 + read, out2, out2 + needed);
        size_t read2 = in2 - in;
        size_t written = out2 - out;

        CHECK(read2 <= size);
        CHECK(written <= needed);
    }
}

#endif // TEST_UTIL_UTF8
