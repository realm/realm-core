#include "testsettings.hpp"
#ifdef TEST_UTIL_FROM_CHARS

#include <realm/util/from_chars.hpp>

#include <realm/util/string_view.hpp>

#include "test.hpp"

using namespace realm::util;

TEST(Util_FromChars_Base10)
{
    StringView okay_byte("254 is a fine byte");
    StringView overflow_byte("260 should overflow a byte");
    StringView negative_byte("-110 should be negative");
    StringView not_a_number("hello, world!\n");

    uint8_t byte = 0;
    FromCharsResult res = from_chars(okay_byte.begin(), okay_byte.end(), byte, 10);
    CHECK_EQUAL(byte, 254);
    CHECK(res.ec == std::errc{});
    CHECK_EQUAL(res.ptr, okay_byte.begin() + 3);

    res = from_chars(overflow_byte.begin(), overflow_byte.end(), byte, 10);
    CHECK_EQUAL(byte, 254);
    CHECK(res.ec == std::errc::result_out_of_range);
    CHECK_EQUAL(res.ptr, overflow_byte.begin() + 3);

    int16_t short_val;
    res = from_chars(overflow_byte.begin(), overflow_byte.end(), short_val, 10);
    CHECK_EQUAL(short_val, 260);
    CHECK(res.ec == std::errc{});
    CHECK_EQUAL(res.ptr, overflow_byte.begin() + 3);

    res = from_chars(not_a_number.begin(), not_a_number.end(), byte, 10);
    CHECK_EQUAL(byte, 254);
    CHECK(res.ec == std::errc::invalid_argument);
    CHECK_EQUAL(res.ptr, not_a_number.begin());

    int8_t signed_byte = 0;
    res = from_chars(okay_byte.begin(), okay_byte.end(), signed_byte, 10);
    CHECK_EQUAL(signed_byte, 0);
    CHECK(res.ec == std::errc::result_out_of_range);
    CHECK_EQUAL(res.ptr, okay_byte.begin() + 3);

    res = from_chars(negative_byte.begin(), negative_byte.end(), signed_byte, 10);
    CHECK_EQUAL(signed_byte, -110);
    CHECK(res.ec == std::errc{});
    CHECK_EQUAL(res.ptr, negative_byte.begin() + 4);
}

TEST(Util_FromChars_Base16)
{
    StringView okay_byte("ff");
    StringView overflow_byte("100");

    uint8_t byte = 0;
    FromCharsResult res = from_chars(okay_byte.begin(), okay_byte.end(), byte, 16);
    CHECK_EQUAL(byte, 255);
    CHECK(res.ec == std::errc{});
    CHECK_EQUAL(res.ptr, okay_byte.begin() + 2);

    res = from_chars(overflow_byte.begin(), overflow_byte.end(), byte, 16);
    CHECK_EQUAL(byte, 255);
    CHECK(res.ec == std::errc::result_out_of_range);
    CHECK_EQUAL(res.ptr, overflow_byte.begin() + 3);

    int16_t short_val;
    res = from_chars(overflow_byte.begin(), overflow_byte.end(), short_val, 16);
    CHECK_EQUAL(short_val, 256);
    CHECK(res.ec == std::errc{});
    CHECK_EQUAL(res.ptr, okay_byte.begin() + 2);
}

#endif
