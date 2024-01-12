#include <realm/util/json_parser.hpp>
#include <sstream>
#include <charconv>

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
void convert_utf32_to_utf8(unsigned int utf32, std::vector<char>& buffer) noexcept
{
    REALM_ASSERT_EX(utf32 <= 0x7fffffff, utf32);
    static const uint8_t limits[] = {0x3f, 0x1f, 0x0f};

    struct out_utf8 {
        out_utf8(std::vector<char>& buffer)
            : buf(buffer)
        {
        }
        void operator()(unsigned int utf32)
        {
            if (i < 3 && utf32 > limits[i]) {
                ++i;
                (*this)(utf32 >> 6);
                buf.push_back(0x80 | (utf32 & 0x3f));
            }
            else {
                buf.push_back((0xf0 << (3 - i)) | utf32);
            }
        }
        std::vector<char>& buf;
        int i = 0;
    };

    if (utf32 <= 0x7f) {
        // Ascii
        buffer.push_back(utf32);
    }
    else {
        out_utf8{buffer}(utf32);
    }
}

} // anonymous namespace

namespace realm {
namespace util {


std::error_condition JSONParser::parse_object()
{
    Event event{EventType::object_begin};
    auto ec = expect_token(Token::object_begin, event.range);
    if (ec)
        return ec;
    ec = m_handler(event);
    if (ec)
        return ec;

    while (true) {
        ec = expect_token(Token::object_end, event.range);
        if (!ec) {
            // End of object
            event.type = EventType::object_end;
            ec = m_handler(event);
            if (ec)
                return ec;
            break;
        }

        if (ec != Error::unexpected_token)
            return ec;

        ec = parse_pair();
        if (ec)
            return ec;

        skip_whitespace();

        Token t;
        if (peek_token(t)) {
            if (t == Token::object_end) {
                // Fine, will terminate on next iteration
            }
            else if (t == Token::comma)
                ++m_current; // OK, because peek_char returned true
            else
                return Error::unexpected_token;
        }
        else {
            return Error::unexpected_end_of_stream;
        }
    }

    return std::error_condition{};
}

std::error_condition JSONParser::parse_pair()
{
    skip_whitespace();

    auto ec = parse_string();
    if (ec)
        return ec;

    skip_whitespace();

    Token t;
    if (peek_token(t)) {
        if (t == Token::colon) {
            ++m_current;
        }
        else {
            return Error::unexpected_token;
        }
    }

    return parse_value();
}

std::error_condition JSONParser::parse_array()
{
    Event event{EventType::array_begin};
    auto ec = expect_token(Token::array_begin, event.range);
    if (ec)
        return ec;
    ec = m_handler(event);
    if (ec)
        return ec;

    while (true) {
        ec = expect_token(Token::array_end, event.range);
        if (!ec) {
            // End of array
            event.type = EventType::array_end;
            ec = m_handler(event);
            if (ec)
                return ec;
            break;
        }

        if (ec != Error::unexpected_token)
            return ec;

        ec = parse_value();
        if (ec)
            return ec;

        skip_whitespace();

        Token t;
        if (peek_token(t)) {
            if (t == Token::array_end) {
                // Fine, will terminate next iteration.
            }
            else if (t == Token::comma)
                ++m_current; // OK, because peek_char returned true
            else
                return Error::unexpected_token;
        }
        else {
            return Error::unexpected_end_of_stream;
        }
    }

    return std::error_condition{};
}

std::error_condition JSONParser::parse_number()
{
    size_t bytes_left = m_end - m_current;
    if (bytes_left == 0)
        return Error::unexpected_end_of_stream;

    if (std::isspace(*m_current)) {
        // JSON has a different idea of what constitutes whitespace than isspace(),
        // but strtod() uses isspace() to skip initial whitespace. We have already
        // skipped whitespace that JSON considers valid, so if there is any whitespace
        // at m_current now, it is invalid according to JSON, and so is an error.
        return Error::unexpected_token;
    }

    switch (m_current[0]) {
        case 'N':
            // strtod() parses "NAN", JSON does not.
        case 'I':
            // strtod() parses "INF", JSON does not.
        case 'p':
        case 'P':
            // strtod() may parse exponent notation, JSON does not.
            return Error::unexpected_token;
        case '0':
            if (bytes_left > 2 && (m_current[1] == 'x' || m_current[1] == 'X')) {
                // strtod() parses hexadecimal, JSON does not.
                return Error::unexpected_token;
            }
    }

    const char* p = m_current;
    if (*p == '-')
        ++p;
    while (std::isdigit(*p))
        ++p;
    bool is_float = *p == '.';
    Event event{is_float ? EventType::number_float : EventType::number_integer};
    char* endp = nullptr;
    if (is_float) {
        // Float
        event.number = std::strtod(m_current, &endp);
    }
    else {
        event.integer = std::strtoll(m_current, &endp, 10);
    }
    if (endp == m_current) {
        return Error::unexpected_token;
    }
    m_current = endp;
    return m_handler(event);
}

std::error_condition JSONParser::parse_string()
{
    InputIterator p = m_current;
    if (p >= m_end)
        return Error::unexpected_end_of_stream;

    auto count_num_escapes_backwards = [](const char* p, const char* begin) -> size_t {
        size_t result = 0;
        for (; p > begin && *p == Token::escape; ++p)
            ++result;
        return result;
    };

    Token t = static_cast<Token>(*p);
    InputIterator inner_end;
    if (t == Token::dquote) {
        inner_end = m_current;
        do {
            inner_end = std::find(inner_end + 1, m_end, Token::dquote);
            if (inner_end == m_end)
                return Error::unexpected_end_of_stream;
        } while (count_num_escapes_backwards(inner_end - 1, m_current) % 2 == 1);

        Event event{EventType::string};
        event.range = Range(m_current, inner_end - m_current + 1);
        m_current = inner_end + 1;
        return m_handler(event);
    }
    return Error::unexpected_token;
}

std::error_condition JSONParser::parse_boolean()
{
    auto first_nonalpha = std::find_if_not(m_current, m_end, [](auto c) {
        return std::isalpha(c);
    });

    Event event{EventType::boolean};
    event.range = Range(m_current, first_nonalpha - m_current);
    if (event.range == "true") {
        event.boolean = true;
        m_current += 4;
        return m_handler(event);
    }
    else if (event.range == "false") {
        event.boolean = false;
        m_current += 5;
        return m_handler(event);
    }

    return Error::unexpected_token;
}

std::error_condition JSONParser::parse_null()
{
    auto first_nonalpha = std::find_if_not(m_current, m_end, [](auto c) {
        return std::isalpha(c);
    });

    Event event{EventType::null};
    event.range = Range(m_current, first_nonalpha - m_current);
    if (event.range == "null") {
        m_current += 4;
        return m_handler(event);
    }

    return Error::unexpected_token;
}

std::error_condition JSONParser::parse_value()
{
    skip_whitespace();

    if (m_current >= m_end)
        return Error::unexpected_end_of_stream;

    if (*m_current == Token::object_begin)
        return parse_object();

    if (*m_current == Token::array_begin)
        return parse_array();

    if (*m_current == 't' || *m_current == 'f')
        return parse_boolean();

    if (*m_current == 'n')
        return parse_null();

    if (*m_current == Token::dquote)
        return parse_string();

    return parse_number();
}

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

std::vector<char> JSONParser::Event::unescape_string() const noexcept
{
    std::vector<char> buffer;
    REALM_ASSERT_EX(type == EventType::string, static_cast<int>(type));
    const char* i = range.data() + 1;
    const char* end = i + range.size() - 2;
    buffer.reserve(end - i);
    while (i < end) {
        auto j = std::find(i, end, '\\');
        buffer.insert(buffer.end(), i, j);
        if (j == end) {
            // No more escapes
            break;
        }
        // skip '\'
        i = j + 1;
        if (i == end) {
            // end of string
            break;
        }
        char c = *i++;
        switch (c) {
            case '"':
                buffer.push_back('"');
                break;
            case '\\':
                buffer.push_back('\\');
                break;
            case '/':
                buffer.push_back('/');
                break;
            case 'b':
                buffer.push_back('\b');
                break;
            case 'f':
                buffer.push_back('\f');
                break;
            case 'n':
                buffer.push_back('\n');
                break;
            case 'r':
                buffer.push_back('\r');
                break;
            case 't':
                buffer.push_back('\t');
                break;
            case 'u': {
                if (i + 4 <= end) {
                    const char* u = i;
                    if (std::all_of(u, u + 4, [](char d) {
                            return std::isxdigit(d);
                        })) {
                        unsigned int utf16_codepoint = 0;
                        for (size_t x = 0; x < 4; ++x) {
                            utf16_codepoint *= 16;
                            utf16_codepoint += hexdigit_to_int(*u++);
                        }

                        if (utf16_codepoint >= 0xd800 && utf16_codepoint <= 0xdbff) {
                            // Surrogate UTF-16
                            unsigned int utf32_codepoint = (utf16_codepoint & 0x3ff) << 10;
                            if (u + 6 <= end && u[0] == '\\' && u[1] == 'u') {
                                u += 2;
                                if (std::all_of(u, u + 4, [](char d) {
                                        return std::isxdigit(d);
                                    })) {
                                    unsigned int utf16_codepoint_2 = 0;
                                    for (size_t x = 0; x < 4; ++x) {
                                        utf16_codepoint_2 *= 16;
                                        utf16_codepoint_2 += hexdigit_to_int(*u++);
                                    }
                                    if (utf16_codepoint_2 >= 0xdc00 && utf16_codepoint_2 <= 0xdfff) {
                                        utf32_codepoint |= utf16_codepoint_2 & 0x3ff;
                                        i = u;
                                        convert_utf32_to_utf8(utf32_codepoint, buffer);
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
                            i = u;
                            convert_utf32_to_utf8(utf16_codepoint, buffer);
                            break;
                        }
                    }
                }
                // Invalid Unicode hex sequence, so don't unescape.
                buffer.push_back('\\');
                buffer.push_back('u');
                break;
            }
            default: {
                buffer.push_back('\\');
                buffer.push_back(c);
                break;
            }
        }
    }
    return buffer;
}

} // namespace util
} // namespace realm
