
#ifndef REALM_UTIL_TIMESTAMP_LOGGER_HPP
#define REALM_UTIL_TIMESTAMP_LOGGER_HPP

#include <realm/util/logger.hpp>
#include <realm/util/timestamp_formatter.hpp>


namespace realm {
namespace util {

class TimestampStderrLogger : public Logger {
public:
    using Precision = TimestampFormatter::Precision;
    using Config = TimestampFormatter::Config;

    explicit TimestampStderrLogger(Config = {}, Level = LogCategory::realm.get_default_level_threshold());

protected:
    void do_log(const LogCategory& category, Logger::Level, const std::string& message) final;

private:
    TimestampFormatter m_formatter;
};


} // namespace util
} // namespace realm

#endif // REALM_UTIL_TIMESTAMP_LOGGER_HPP
