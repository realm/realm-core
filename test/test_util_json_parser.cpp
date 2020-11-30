#include "test.hpp"

#include <realm/util/json_parser.hpp>
#include <iostream>

using namespace realm;
using namespace realm::util;

static const char g_basic_object[] = "{\n"
                                     "    \"access\": [\"download\", \"upload\"],\n"
                                     "    \"timestamp\": 1455530614,\n"
                                     "    \"expires\": null,\n"
                                     "    \"app_id\": \"io.realm.Test\"\n"
                                     "}";

using ET = JSONParser::EventType;

namespace {
static const char g_events_test[] =
    "   {\"a\":\"b\",\t\"b\"    :[],\"c\": {\"d\":null,\"e\":123.13,\"f\": -199,\"g\":-2.3e9},\"h\":\"\\u00f8\"}";
static const JSONParser::EventType g_expected_events[] = {
    ET::object_begin, ET::string, ET::string,     ET::string, ET::array_begin, ET::array_end, ET::string,
    ET::object_begin, ET::string, ET::null,       ET::string, ET::number,      ET::string,    ET::number,
    ET::string,       ET::number, ET::object_end, ET::string, ET::string,      ET::object_end};
} // anonymous namespace


TEST(JSONParser_Basic)
{
    JSONParser parser{g_basic_object};
    enum {
        initial,
        in_object,
        get_access,
        access_elements,
        get_timestamp,
        get_expires,
        get_app_id,
    } state = initial;
    std::vector<char> buffer;
    util::Optional<double> timestamp;
    util::Optional<double> expires;
    util::Optional<std::string> app_id;
    std::vector<std::string> access;

    auto read_string_into_buffer = [&](const JSONParser::Event& event) -> StringData {
        CHECK(event.type == JSONParser::EventType::string);
        buffer.resize(std::max(buffer.size(), event.escaped_string_value().size()));
        return event.unescape_string(buffer.data());
    };

    auto ec = parser.parse([&](auto&& event) -> std::error_condition {
        switch (state) {
            case initial: {
                if (event.type == JSONParser::EventType::object_begin)
                    state = in_object;
                else
                    return JSONParser::Error::unexpected_token;
                break;
            }
            case in_object: {
                if (event.type == JSONParser::EventType::string) {
                    StringData key = read_string_into_buffer(event);
                    if (key == "access")
                        state = get_access;
                    else if (key == "timestamp")
                        state = get_timestamp;
                    else if (key == "expires")
                        state = get_expires;
                    else if (key == "app_id")
                        state = get_app_id;
                    else {
                        throw std::runtime_error("Unknown key");
                    }
                    break;
                }
                else if (event.type == JSONParser::EventType::object_end) {
                    break;
                }
                else
                    return JSONParser::Error::unexpected_token;
            }
            case get_access: {
                if (event.type == JSONParser::EventType::array_begin)
                    state = access_elements;
                else
                    return JSONParser::Error::unexpected_token;
                break;
            }
            case access_elements: {
                if (event.type == JSONParser::EventType::array_end)
                    state = in_object;
                else if (event.type == JSONParser::EventType::string)
                    access.push_back(read_string_into_buffer(event));
                else
                    return JSONParser::Error::unexpected_token;
                break;
            }
            case get_timestamp: {
                if (event.type == JSONParser::EventType::number) {
                    timestamp = event.number;
                    state = in_object;
                    break;
                }
                return JSONParser::Error::unexpected_token;
            }
            case get_expires: {
                if (event.type == JSONParser::EventType::null) {
                    state = in_object;
                    break;
                }
                return JSONParser::Error::unexpected_token;
            }
            case get_app_id: {
                if (event.type == JSONParser::EventType::string) {
                    app_id = std::string(read_string_into_buffer(event));
                    state = in_object;
                    break;
                }
                return JSONParser::Error::unexpected_token;
            }
        }
        return std::error_condition{};
    });

    CHECK(!ec);
    CHECK_EQUAL(state, in_object);
    CHECK_EQUAL(*timestamp, 1455530614);
    CHECK(!expires);
    CHECK_EQUAL(*app_id, "io.realm.Test");
    CHECK_EQUAL(access.size(), 2);
    CHECK_EQUAL(access[0], "download");
    CHECK_EQUAL(access[1], "upload");
}

TEST(JSONParser_UnescapeString)
{
    JSONParser::Event event(JSONParser::EventType::string);
    event.range = "\"Hello,\\\\ World.\\n8\\u00b0C\\u00F8\""; // includes surrounding double quotes
    std::vector<char> buffer;
    buffer.resize(event.escaped_string_value().size(), '\0');
    StringData unescaped = event.unescape_string(buffer.data());
    CHECK_EQUAL(unescaped, "Hello,\\ World.\n8°Cø");

    static const char* escaped[] = {
        "\"\\u0abg\"",        // invalid sequence
        "\"\\u0041\"",        // ASCII 'A'
        "\"\\u05d0\"",        // Hebrew 'alef'
        "\"\\u2f08\"",        // Kangxi (Chinese) 'man'
        "\"\\u4eba\"",        // CJK Unified Ideograph 'man'
        "\"\\ufffd\"",        // Replacement character
        "\"\\ud87e\\udd10\"", // Emoji 'zipper-mouth face' (surrogate pair)
    };
    static const char* expected[] = {
        "\\u0abg", "A",    "א", "⼈",
        "人", // NOTE! This character looks identical to the one above, but is a different codepoint.
        "�",       "🤐",
    };

    for (size_t i = 0; i != sizeof(escaped) / sizeof(escaped[0]); ++i) {
        event.range = escaped[i];
        unescaped = event.unescape_string(buffer.data());
        CHECK_EQUAL(unescaped, expected[i]);
    }

    static const char* invalid_surrogate_pairs[] = {
        "\"\\ud800a\"", // high surrogate followed by non-surrogate
        "\"\\udc00\"",  // low surrogate with no preceding high surrogate
    };

    for (size_t i = 0; i < sizeof(invalid_surrogate_pairs) / sizeof(invalid_surrogate_pairs[0]); ++i) {
        const char* str = invalid_surrogate_pairs[i];
        event.range = str;
        unescaped = event.unescape_string(buffer.data());
        CHECK_EQUAL(unescaped, StringData(str + 1, strlen(str) - 2));
    }
}

TEST(JSONParser_Events)
{
    JSONParser parser{g_events_test};
    size_t i = 0;
    parser.parse([&](auto&& event) noexcept {
        if (event.type != g_expected_events[i]) {
            CHECK(event.type == g_expected_events[i]);
            std::cerr << "Event did not match: " << event << " (at " << i << ")\n";
        }
        ++i;
        return std::error_condition{};
    });
    CHECK_EQUAL(i, sizeof(g_expected_events) / sizeof(g_expected_events[0]));
}

TEST(JSONParser_PropagateError)
{
    JSONParser parser{g_events_test};
    auto ec = parser.parse([&](auto&& event) noexcept {
        if (event.type == ET::null) {
            return std::error_condition{std::errc::argument_out_of_domain}; // just anything
        }
        return std::error_condition{};
    });
    CHECK(ec);
    CHECK(ec == std::errc::argument_out_of_domain);
}

TEST(JSONParser_Whitespace)
{
    auto dummy_callback = [](auto&&) noexcept {
        return std::error_condition{};
    };
    std::error_condition ec;

    static const char initial_whitespace[] = "  \t{}";
    JSONParser initial_whitespace_parser{initial_whitespace};
    ec = initial_whitespace_parser.parse(dummy_callback);
    CHECK(!ec);

    // std::isspace considers \f and \v whitespace, but the JSON standard doesn't.
    static const char invalid_whitespace_f[] = "{\"a\":\f1}";
    JSONParser invalid_whitespace_f_parser{invalid_whitespace_f};
    ec = invalid_whitespace_f_parser.parse(dummy_callback);
    CHECK(ec == JSONParser::Error::unexpected_token);

    static const char invalid_whitespace_v[] = "{\"a\":\v2}";
    JSONParser invalid_whitepsace_v_parser{invalid_whitespace_v};
    ec = invalid_whitepsace_v_parser.parse(dummy_callback);
    CHECK(ec == JSONParser::Error::unexpected_token);
}

TEST(JSONParser_PrimitiveDocuments)
{
    // JSON specifies that any object can be the document root.

    std::error_condition ec;

    static const char number_root[] = "123.0";
    JSONParser number_parser{number_root};
    ec = number_parser.parse([&](auto&& event) noexcept {
        CHECK_EQUAL(event.type, JSONParser::EventType::number);
        CHECK_EQUAL(event.number, 123);
        return std::error_condition{};
    });
    CHECK(!ec);

    static const char string_root[] = "\"\\u00f8\"";
    JSONParser string_parser{string_root};
    ec = string_parser.parse([&](auto&& event) noexcept {
        CHECK_EQUAL(event.type, JSONParser::EventType::string);
        char buffer[8] = {0};
        CHECK_EQUAL(event.unescape_string(buffer), "ø");
        return std::error_condition{};
    });
    CHECK(!ec);

    static const char bool_root[] = "false";
    JSONParser bool_parser{bool_root};
    ec = bool_parser.parse([&](auto&& event) noexcept {
        CHECK_EQUAL(event.type, JSONParser::EventType::boolean);
        CHECK(!event.boolean);
        return std::error_condition{};
    });
    CHECK(!ec);

    static const char null_root[] = "null";
    JSONParser null_parser{null_root};
    ec = null_parser.parse([&](auto&& event) noexcept {
        CHECK_EQUAL(event.type, JSONParser::EventType::null);
        return std::error_condition{};
    });
    CHECK(!ec);

    static const char invalid_root[] = "blah";
    JSONParser invalid_parser(invalid_root);
    ec = invalid_parser.parse([&](auto&&) noexcept {
        return std::error_condition{};
    });
    CHECK(ec == JSONParser::Error::unexpected_token);
}

TEST(JSONParser_ArrayDocument)
{
    std::error_condition ec;

    static const char array_root[] = "[]";
    JSONParser array_parser{array_root};
    ec = array_parser.parse([](auto&&) noexcept {
        return std::error_condition{};
    });
    CHECK(!ec);

    static const char invalid_array_root[] = "[";
    JSONParser invalid_array_parser{invalid_array_root};
    ec = invalid_array_parser.parse([](auto&&) noexcept {
        return std::error_condition{};
    });
    CHECK(ec == JSONParser::Error::unexpected_end_of_stream);
}

TEST(JSONParser_StringTermination)
{
    static const char string_root[] = "\"\\\\\\\"\"";
    JSONParser string_parser{string_root};
    std::error_condition ec;
    ec = string_parser.parse([&](auto&& event) noexcept {
        CHECK_EQUAL(event.type, JSONParser::EventType::string);
        CHECK_EQUAL(event.escaped_string_value(), "\\\\\\\"");
        return std::error_condition{};
    });
    CHECK(!ec);
}
