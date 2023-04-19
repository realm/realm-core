#include <utility>
#include <iostream>

#include <realm/util/timestamp_logger.hpp>

using namespace realm;
using util::TimestampStderrLogger;


TimestampStderrLogger::TimestampStderrLogger(Config config, Level level)
    : Logger(level)
    , m_formatter{std::move(config)}
{
}


void TimestampStderrLogger::do_log(Logger::Level level, const std::string& message)
{
    auto now = std::chrono::system_clock::now();
    std::cerr << m_formatter.format(now) << ": " << get_level_prefix(level) << message << '\n'; // Throws
}
