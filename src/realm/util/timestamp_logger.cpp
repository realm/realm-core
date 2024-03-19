#include <utility>
#include <iostream>

#include <realm/util/timestamp_logger.hpp>

using namespace realm;
using util::TimestampStderrLogger;


TimestampStderrLogger::TimestampStderrLogger(Config config, Level level)
    : Logger()
    , m_formatter{std::move(config)}
{
    set_level_threshold(level);
}


void TimestampStderrLogger::do_log(const LogCategory& category, Logger::Level level, const std::string& message)
{
    auto now = std::chrono::system_clock::now();
    static Mutex mutex;
    LockGuard l(mutex);
    std::cerr << m_formatter.format(now) << ": " << category.get_name() << ": " << get_level_prefix(level) << message
              << '\n'; // Throws
}
