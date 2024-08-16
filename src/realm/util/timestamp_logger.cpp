#include <utility>
#include <iostream>

#include <realm/util/timestamp_logger.hpp>

using namespace realm;
using util::TimestampStderrLogger;


TimestampStderrLogger::TimestampStderrLogger(Config config, Level level)
    : StderrLogger(level)
    , m_formatter{std::move(config)}
{
}


void TimestampStderrLogger::do_log(const LogCategory& category, Logger::Level level, const std::string& message)
{
    auto now = std::chrono::system_clock::now();
    do_write(util::format("%1: %2 - %3%4", m_formatter.format(now), category.get_name(), get_level_prefix(level),
                          message));
}
