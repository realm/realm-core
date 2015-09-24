#include "testsettings.hpp"
#ifdef TEST_UNICODE

#include <cstddef>
#include <limits>
#include <stdexcept>
#include <string>
#include <iostream>
#include <memory>

#include <realm/unicode.hpp>

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

} // anonymous namespace


TEST(Unicode_Compare_Core_ASCII)
{
    // Useful line for creating new unit test cases:
    // bool ret = std::locale("us_EN")(string("a"), std::string("b"));

    set_string_compare_method(STRING_COMPARE_CORE, nullptr);

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


TEST(Unicode_Compare_Core_utf8)
{
    // Useful line for creating new unit test cases:
    // bool ret = std::locale("us_EN")(string("a"), std::string("b"));

    set_string_compare_method(STRING_COMPARE_CORE, nullptr);

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


TEST(Unicode_Compare_Core_utf8_invalid)
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

    set_string_compare_method(STRING_COMPARE_CORE, nullptr);
    StringData i1 = StringData(invalid1);
    StringData i2 = StringData(invalid2);

    // strings must be seen as 'equal' because they terminate when StringData::size is reached. Futhermore, we state
    // that return value is arbitrary for invalid utf8
    bool ret = utf8_compare(i1, i2);
    CHECK_EQUAL(ret, utf8_compare(i2, i1)); // must sort the same as before regardless of succeeding data
}


/* shows uninitialized data access i Valgrind (by design). Disabled until supressed or we find another way to test
TEST(Unicode_Compare_Core_utf8_invalid_crash)
{
    // See if we can crash Realm with random data
    char str1[20];
    char str2[20];
    using namespace realm::test_util;
    Random r;

    set_string_compare_method(STRING_COMPARE_CORE, nullptr);

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


TEST(Unicode_Compare_Core_utf8_zero)
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

#endif // TEST_UNICODE
