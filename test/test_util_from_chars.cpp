#include "testsettings.hpp"
#ifdef TEST_UTIL_FROM_CHARS

#include <charconv>

#include <realm/util/from_chars.hpp>

#include "test.hpp"

using namespace realm::util;

TEST(Util_FromChars_Base10)
{
    const std::string_view okay_byte("254 is a fine byte");
    const std::string_view overflow_byte("260 should overflow a byte");
    const std::string_view negative_byte("-110 should be negative");
    const std::string_view not_a_number("hello, world!\n");

    uint8_t byte = 0;
    FromCharsResult res = from_chars(okay_byte.data(), okay_byte.data() + okay_byte.size(), byte, 10);
    CHECK_EQUAL(byte, 254);
    CHECK(res.ec == std::errc{});
    CHECK_EQUAL(res.ptr, okay_byte.data() + 3);

    res = from_chars(overflow_byte.data(), overflow_byte.data() + overflow_byte.size(), byte, 10);
    CHECK_EQUAL(byte, 254);
    CHECK(res.ec == std::errc::result_out_of_range);
    CHECK_EQUAL(res.ptr, overflow_byte.data() + 3);

    int16_t short_val;
    res = from_chars(overflow_byte.data(), overflow_byte.data() + overflow_byte.size(), short_val, 10);
    CHECK_EQUAL(short_val, 260);
    CHECK(res.ec == std::errc{});
    CHECK_EQUAL(res.ptr, overflow_byte.data() + 3);

    res = from_chars(not_a_number.data(), not_a_number.data() + not_a_number.size(), byte, 10);
    CHECK_EQUAL(byte, 254);
    CHECK(res.ec == std::errc::invalid_argument);
    CHECK_EQUAL(res.ptr, not_a_number.data());

    int8_t signed_byte = 0;
    res = from_chars(okay_byte.data(), okay_byte.data() + okay_byte.size(), signed_byte, 10);
    CHECK_EQUAL(signed_byte, 0);
    CHECK(res.ec == std::errc::result_out_of_range);
    CHECK_EQUAL(res.ptr, okay_byte.data() + 3);

    res = from_chars(negative_byte.data(), negative_byte.data() + negative_byte.size(), signed_byte, 10);
    CHECK_EQUAL(signed_byte, -110);
    CHECK(res.ec == std::errc{});
    CHECK_EQUAL(res.ptr, negative_byte.data() + 4);
}

TEST(Util_FromChars_Base16)
{
    const std::string_view okay_byte("ff");
    const std::string_view overflow_byte("100");

    uint8_t byte = 0;
    FromCharsResult res = from_chars(okay_byte.data(), okay_byte.data() + okay_byte.size(), byte, 16);
    CHECK_EQUAL(byte, 255);
    CHECK(res.ec == std::errc{});
    CHECK_EQUAL(res.ptr, okay_byte.data() + 2);

    res = from_chars(overflow_byte.data(), overflow_byte.data() + overflow_byte.size(), byte, 16);
    CHECK_EQUAL(byte, 255);
    CHECK(res.ec == std::errc::result_out_of_range);
    CHECK_EQUAL(res.ptr, overflow_byte.data() + 3);

    int16_t short_val;
    res = from_chars(overflow_byte.data(), overflow_byte.data() + overflow_byte.size(), short_val, 16);
    CHECK_EQUAL(short_val, 256);
    CHECK(res.ec == std::errc{});
    CHECK_EQUAL(res.ptr, okay_byte.data() + 2);
}

#endif
