#include <realm/util/json_parser.hpp>
#include <sstream>

namespace {

int hexdigit_to_int(int digit) noexcept
{
    if (digit >= '0' && digit <= '9')
        return digit - '0';
    if (digit >= 'a' && digit <= 'f')
        return 10 + (digit - 'a');
    if (digit >= 'A' && digit <= 'F')
        return 10 + (digit - 'A');
    REALM_UNREACHABLE(); // LCOV_EXCL_LINE
}

// p must be a pointer to at least 6 chars.
// Returns number of UTF-8 bytes.
size_t convert_utf32_to_utf8(unsigned int utf32, char* p) noexcept
{
    REALM_ASSERT_EX(utf32 <= 0x7fffffff, utf32);

    size_t n;
    if (utf32 <= 0x7f) {
        p[0] = static_cast<char>(utf32);
        return 1;
    }
    else if (utf32 <= 0x07ff)
        n = 2;
    else if (utf32 <= 0xffff)
        n = 3;
    else if (utf32 <= 0x1fffff)
        n = 4;
    else if (utf32 <= 0x3ffffff)
        n = 5;
    else
        n = 6;

    unsigned int x = utf32;
    for (size_t i = 1; i < n; ++i) {
        p[n - i] = 0x80 | (x & 0x3f);
        x >>= 6;
    }
    p[0] = (0xfc << (6 - n)) | x;

    return n;
}

} // anonymous namespace

namespace realm {
namespace util {

const char* JSONParser::ErrorCategory::name() const noexcept
{
    return "realm::util::JSONParser::ErrorCategory";
}

std::string JSONParser::ErrorCategory::message(int ec) const
{
    Error e = static_cast<Error>(ec);
    switch (e) {
        case Error::unexpected_token:
            return "unexpected token";
        case Error::unexpected_end_of_stream:
            return "unexpected end of stream";
    }
    REALM_UNREACHABLE();
}

const JSONParser::ErrorCategory JSONParser::error_category{};

std::error_condition make_error_condition(JSONParser::Error e)
{
    return std::error_condition{static_cast<int>(e), JSONParser::error_category};
}

StringData JSONParser::Event::unescape_string(char* buffer) const noexcept
{
    REALM_ASSERT_EX(type == EventType::string, static_cast<int>(type));
    size_t i, o;
    bool escape = false;

    StringData escaped_string = escaped_string_value();
    for (i = 0, o = 0; i < escaped_string.size(); ++i) {
        char c = escaped_string[i];
        if (escape) {
            switch (c) {
                case '"':
                    buffer[o++] = '"';
                    break;
                case '\\':
                    buffer[o++] = '\\';
                    break;
                case '/':
                    buffer[o++] = '/';
                    break;
                case 'b':
                    buffer[o++] = '\b';
                    break;
                case 'f':
                    buffer[o++] = '\f';
                    break;
                case 'n':
                    buffer[o++] = '\n';
                    break;
                case 'r':
                    buffer[o++] = '\r';
                    break;
                case 't':
                    buffer[o++] = '\t';
                    break;
                case 'u': {
                    if (i + 4 < escaped_string.size()) {
                        const char* u = escaped_string.data() + i + 1;
                        if (std::all_of(u, u + 4, [](char d) {
                                return std::isxdigit(d);
                            })) {
                            unsigned int utf16_codepoint = 0;
                            for (size_t x = 0; x < 4; ++x) {
                                utf16_codepoint *= 16;
                                utf16_codepoint += hexdigit_to_int(u[x]);
                            }

                            if (utf16_codepoint >= 0xd800 && utf16_codepoint <= 0xdbff) {
                                // Surrogate UTF-16
                                unsigned int utf32_codepoint = (utf16_codepoint & 0x3ff) << 10;
                                u = escaped_string.data() + i + 5;
                                if (i + 10 < escaped_string.size() && u[0] == '\\' && u[1] == 'u') {
                                    u += 2;
                                    if (std::all_of(u, u + 4, [](char d) {
                                            return std::isxdigit(d);
                                        })) {
                                        unsigned int utf16_codepoint_2 = 0;
                                        for (size_t x = 0; x < 4; ++x) {
                                            utf16_codepoint_2 *= 16;
                                            utf16_codepoint_2 += hexdigit_to_int(u[x]);
                                        }
                                        if (utf16_codepoint_2 >= 0xdc00 && utf16_codepoint_2 <= 0xdfff) {
                                            utf32_codepoint |= utf16_codepoint_2 & 0x3ff;
                                            i += 10;
                                            o += convert_utf32_to_utf8(utf32_codepoint, &buffer[o]);
                                            break;
                                        }
                                        else {
                                            // High surrogate not followed by a low surrogate, invalid UTF-16.
                                        }
                                    }
                                    else {
                                        // High surrogate followed by invalid UTF-16 sequence, invalid UTF-16.
                                    }
                                }
                                else {
                                    // High surrogate followed by non-UTF16 sequence, invalid UTF-16.
                                }
                            }
                            else if (utf16_codepoint >= 0xdc00 && utf16_codepoint <= 0xdfff) {
                                // Low surrogate without high surrogate, invalid UTF-16.
                            }
                            else {
                                i += 4;
                                o += convert_utf32_to_utf8(utf16_codepoint, &buffer[o]);
                                break;
                            }
                        }
                    }
                    // Invalid Unicode hex sequence, so don't unescape.
                    buffer[o++] = '\\';
                    buffer[o++] = 'u';
                    break;
                }
                default: {
                    buffer[o++] = '\\';
                    buffer[o++] = c;
                    break;
                }
            }
            escape = false;
        }
        else {
            if (c == '\\') {
                escape = true;
            }
            else {
                buffer[o++] = c;
            }
        }
    }
    return StringData(buffer, o);
}

} // namespace util
} // namespace realm
