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

#include "testsettings.hpp"
#ifdef TEST_UTIL_BASE64

#include <realm/util/base64.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;

TEST(Base64_Decode)
{
    std::vector<char> buffer;
    buffer.resize(1024);
    Optional<size_t> r;

    static const char* inputs[] = {
        "",
        "Zg==",
        "Zm8=",
        "Zm9v",
        "Zmxvbw==",
        "Zmxvb3I=",
        "SGVsb G8sIF\ndvc mxkIQ==", // contains whitespace
    };

    static const char* expected[] = {
        "",
        "f",
        "fo",
        "foo",
        "floo",
        "floor",
        "Hello, World!"
    };

    static const size_t num_tests = sizeof(inputs) / sizeof(inputs[0]);

    for (size_t i = 0; i < num_tests; ++i) {
        r = base64_decode(inputs[i], buffer.data(), buffer.size());
        CHECK(r);
        CHECK_EQUAL(StringData(buffer.data(), *r), expected[i]);
    }

    static const char* bad_inputs[] = {
        "!",       // invalid char
        ":",       // invalid char
        "Zg===",   // invalid length
        "====",    // only padding
        "()",      // invalid chars
        "Zm9v====", // wrong amount of padding
    };
    static const size_t num_bad_tests = sizeof(bad_inputs) / sizeof(bad_inputs[0]);

    for (size_t i = 0; i < num_bad_tests; ++i) {
        r = base64_decode(bad_inputs[i], buffer.data(), buffer.size());
        CHECK(!r);
    }
}


TEST(Base64_Decode_AdjacentBuffers)
{
    char buffer[10] = "Zg==\0"; // "f" + blank space + terminating zero
    const char expected[] = "f";
    Optional<size_t> r = base64_decode(buffer, buffer + 4, 3);
    CHECK(r);
    CHECK_EQUAL(*r, 1);
    CHECK_EQUAL(StringData{buffer + 4}, StringData{expected});
}

namespace {

struct TestBuffers {

    const char* decoded_buffer;
    size_t decoded_buffer_size;
    const char* encoded_buffer;
    size_t encoded_buffer_size;
};

} // namespace

TEST(Base64_Encode)
{
    std::vector<char> buffer;
    buffer.resize(100);

    TestBuffers tbs[] = {
        TestBuffers {"", 0, "", 0},
        TestBuffers {"\x00\x00\x00", 3, "AAAA", 4},
        TestBuffers {"\x00\x00\x01", 3, "AAAB", 4},
        TestBuffers {"\x80", 1, "gA==", 4},
        TestBuffers {"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10", 16, "AQIDBAUGBwgJCgsMDQ4PEA==", 24}

    };

    const size_t num_tests = sizeof(tbs) / sizeof(tbs[0]);

    for (size_t i = 0; i < num_tests; ++i) {
        size_t return_size = base64_encode(tbs[i].decoded_buffer, tbs[i].decoded_buffer_size, buffer.data(), buffer.size());
        CHECK_EQUAL(return_size, tbs[i].encoded_buffer_size);
        CHECK_EQUAL(StringData(buffer.data(), return_size), tbs[i].encoded_buffer);

        Optional<size_t> return_size_opt = base64_decode(StringData(tbs[i].encoded_buffer, tbs[i].encoded_buffer_size), buffer.data(), buffer.size());
        CHECK(return_size_opt);
        CHECK_EQUAL(*return_size_opt, tbs[i].decoded_buffer_size);
        for (size_t j = 0; j < *return_size_opt; ++j) {
            CHECK_EQUAL(buffer[j], tbs[i].decoded_buffer[j]);
        }
    }
}

TEST(Base64_DecodeToVector)
{
    {
        Optional<std::vector<char>> vec = base64_decode_to_vector("======");
        CHECK(!vec);
    }

    {
        Optional<std::vector<char>> vec = base64_decode_to_vector("SGVsb G8sIF\ndvc mxkIQ==");
        std::string str(vec->begin(), vec->end());
        CHECK_EQUAL("Hello, World!", str);
    }
}

#endif // TEST_UTIL_BASE64
