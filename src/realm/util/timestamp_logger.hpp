
#ifndef REALM_UTIL_TIMESTAMP_LOGGER_HPP
#define REALM_UTIL_TIMESTAMP_LOGGER_HPP

#include <realm/util/logger.hpp>
#include <realm/util/timestamp_formatter.hpp>


namespace realm {
namespace util {

class TimestampStderrLogger : public RootLogger {
public:
    using Precision = TimestampFormatter::Precision;
    using Config = TimestampFormatter::Config;

    explicit TimestampStderrLogger(Config = {});

protected:
    void do_log(Logger::Level, std::string message) override;

private:
    TimestampFormatter m_formatter;
};


} // namespace util
} // namespace realm

#endif // REALM_UTIL_TIMESTAMP_LOGGER_HPP
