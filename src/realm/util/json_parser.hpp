#ifndef REALM_UTIL_JSON_PARSER_HPP
#define REALM_UTIL_JSON_PARSER_HPP

#include <system_error>
#include <algorithm>
#include <cstdlib>
#include <cctype>

#include <realm/string_data.hpp>
#include <realm/util/span.hpp>
#include <realm/util/functional.hpp>

namespace realm {
namespace util {

/// A JSON parser that neither allocates heap memory nor throws exceptions.
///
/// The parser takes as input a range of characters, and emits a stream of events
/// representing the structure of the JSON document.
///
/// Parser errors are represented as `std::error_condition`s.
class JSONParser {
public:
    using InputIterator = const char*;

    enum class EventType {
        number_integer,
        number_float,
        string,
        boolean,
        null,
        array_begin,
        array_end,
        object_begin,
        object_end
    };

    using Range = StringData;

    struct Event {
        EventType type;
        Range range;
        Event(EventType type)
            : type(type)
        {
        }

        union {
            bool boolean;
            double number;
            int64_t integer;
        };

        StringData escaped_string_value() const noexcept;

        /// Unescape the string value into \a buffer.
        /// The type of this event must be EventType::string.
        ///
        /// \param buffer is a pointer to a buffer big enough to hold the
        /// unescaped string value. The unescaped string is guaranteed to be
        /// shorter than the escaped string, so escaped_string_value().size() can
        /// be used as an upper bound. Unicode sequences of the form "\uXXXX"
        /// will be converted to UTF-8 sequences. Note that the escaped form of
        /// a unicode point takes exactly 6 bytes, which is also the maximum
        /// possible length of a UTF-8 encoded codepoint.
        StringData unescape_string(char* buffer) const noexcept;
    };
    using EventHandler = util::UniqueFunction<std::error_condition(const Event&)>;

    enum class Error { unexpected_token = 1, unexpected_end_of_stream = 2 };

    JSONParser(EventHandler);

    /// Parse the input data, and call f repeatedly with an argument of type Event
    /// representing the token that the parser encountered.
    ///
    /// The stream of events is "flat", which is to say that it is the responsibility
    /// of the function f to keep track of any nested object structures as it deems
    /// appropriate.
    std::error_condition parse(util::Span<const char>);

    class ErrorCategory : public std::error_category {
    public:
        const char* name() const noexcept final;
        std::string message(int) const final;
    };
    static const ErrorCategory error_category;

private:
    enum Token : char {
        object_begin = '{',
        object_end = '}',
        array_begin = '[',
        array_end = ']',
        colon = ':',
        comma = ',',
        dquote = '"',
        escape = '\\',
        minus = '-',
        space = ' ',
        tab = '\t',
        cr = '\r',
        lf = '\n',
    };

    InputIterator m_current = nullptr;
    InputIterator m_end = nullptr;
    EventHandler m_handler;

    std::error_condition parse_object();
    std::error_condition parse_pair();
    std::error_condition parse_array();
    std::error_condition parse_number();
    std::error_condition parse_string();
    std::error_condition parse_value();
    std::error_condition parse_boolean();
    std::error_condition parse_null();

    std::error_condition expect_token(char, Range& out_range) noexcept;
    std::error_condition expect_token(Token, Range& out_range) noexcept;

    // Returns true unless EOF was reached.
    bool peek_char(char& out_c) noexcept;
    bool peek_token(Token& out_t) noexcept;
    bool is_whitespace(Token t) noexcept;
    void skip_whitespace() noexcept;
};

std::error_condition make_error_condition(JSONParser::Error e);

} // namespace util
} // namespace realm

namespace std {
template <>
struct is_error_condition_enum<realm::util::JSONParser::Error> {
    static const bool value = true;
};
} // namespace std

namespace realm {
namespace util {

/// Implementation:


inline JSONParser::JSONParser(EventHandler f)
    : m_handler(std::move(f))
{
}

inline std::error_condition JSONParser::parse(util::Span<const char> input)
{
    m_current = input.data();
    m_end = input.data() + input.size();
    return parse_value();
}

inline bool JSONParser::is_whitespace(Token t) noexcept
{
    switch (t) {
        case Token::space:
        case Token::tab:
        case Token::cr:
        case Token::lf:
            return true;
        default:
            return false;
    }
}

inline void JSONParser::skip_whitespace() noexcept
{
    while (m_current < m_end && is_whitespace(static_cast<Token>(*m_current)))
        ++m_current;
}

inline std::error_condition JSONParser::expect_token(char c, Range& out_range) noexcept
{
    skip_whitespace();
    if (m_current == m_end)
        return Error::unexpected_end_of_stream;
    if (*m_current == c) {
        out_range = Range(m_current, 1);
        ++m_current;
        return std::error_condition{};
    }
    return Error::unexpected_token;
}

inline std::error_condition JSONParser::expect_token(Token t, Range& out_range) noexcept
{
    return expect_token(static_cast<char>(t), out_range);
}

inline bool JSONParser::peek_char(char& out_c) noexcept
{
    if (m_current < m_end) {
        out_c = *m_current;
        return true;
    }
    return false;
}

inline bool JSONParser::peek_token(Token& out_t) noexcept
{
    if (m_current < m_end) {
        out_t = static_cast<Token>(*m_current);
        return true;
    }
    return false;
}

inline StringData JSONParser::Event::escaped_string_value() const noexcept
{
    REALM_ASSERT(type == EventType::string);
    REALM_ASSERT(range.size() >= 2);
    return StringData(range.data() + 1, range.size() - 2);
}

template <class OS>
OS& operator<<(OS& os, JSONParser::EventType type)
{
    switch (type) {
        case JSONParser::EventType::number_integer:
            os << "integer";
            return os;
        case JSONParser::EventType::number_float:
            os << "number";
            return os;
        case JSONParser::EventType::string:
            os << "string";
            return os;
        case JSONParser::EventType::boolean:
            os << "boolean";
            return os;
        case JSONParser::EventType::null:
            os << "null";
            return os;
        case JSONParser::EventType::array_begin:
            os << "[";
            return os;
        case JSONParser::EventType::array_end:
            os << "]";
            return os;
        case JSONParser::EventType::object_begin:
            os << "{";
            return os;
        case JSONParser::EventType::object_end:
            os << "}";
            return os;
    }
    REALM_UNREACHABLE();
}

template <class OS>
OS& operator<<(OS& os, const JSONParser::Event& e)
{
    os << e.type;
    switch (e.type) {
        case JSONParser::EventType::number_integer:
            return os << "(" << e.integer << ")";
        case JSONParser::EventType::number_float:
            return os << "(" << e.number << ")";
        case JSONParser::EventType::string:
            return os << "(" << e.range << ")";
        case JSONParser::EventType::boolean:
            return os << "(" << e.boolean << ")";
        default:
            return os;
    }
}

} // namespace util
} // namespace realm

#endif // REALM_UTIL_JSON_PARSER_HPP
