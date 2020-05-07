#include <locale>
#include <stdexcept>
#include <iomanip>

#include <realm/util/time.hpp>
#include <realm/util/timestamp_formatter.hpp>

using namespace realm;
using util::TimestampFormatter;


TimestampFormatter::TimestampFormatter(Config config)
    : m_utc_time{config.utc_time}
    , m_precision{config.precision}
    , m_format_segments{make_format_segments(config)} // Throws
{
    m_out.imbue(std::locale::classic());
    m_out.fill('0');
}


auto TimestampFormatter::format(std::time_t time, long nanoseconds) -> string_view_type
{
    std::tm time_2 = (m_utc_time ? util::gmtime(time) : util::localtime(time));
    m_out.set_buffer(m_buffer, m_buffer + sizeof m_buffer / sizeof m_buffer[0]);
    util::put_time(m_out, time_2, m_format_segments.first.c_str());
    switch (m_precision) {
        case Precision::seconds:
            break;
        case Precision::milliseconds:
            m_out << '.' << std::setw(3) << (nanoseconds / 1000000);
            break;
        case Precision::microseconds:
            m_out << '.' << std::setw(6) << (nanoseconds / 1000);
            break;
        case Precision::nanoseconds:
            m_out << '.' << std::setw(9) << nanoseconds;
            break;
    }
    util::put_time(m_out, time_2, m_format_segments.second);
    if (!m_out)
        throw std::runtime_error("Failed to format timestamp");
    return string_view_type{m_buffer, m_out.size()};
}


auto TimestampFormatter::make_format_segments(const Config& config) -> format_segments_type
{
    const char* i = config.format;
    for (;;) {
        char ch = *i++;
        if (REALM_UNLIKELY(ch == '%')) {
            ch = *i++;
            if (ch == 'S' || ch == 'T')
                break;
        }
        if (REALM_UNLIKELY(ch == '\0'))
            return {std::string{}, config.format};
    }
    return {std::string{config.format, i}, i}; // Throws
}
