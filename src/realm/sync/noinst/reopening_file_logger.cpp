#include <utility>

#include <realm/sync/noinst/reopening_file_logger.hpp>

using namespace realm;
using _impl::ReopeningFileLogger;


ReopeningFileLogger::ReopeningFileLogger(std::string path, volatile std::sig_atomic_t& reopen_log_file,
                                         TimestampConfig config)
    : m_path(std::move(path))
    , m_file(m_path, util::File::mode_Append) // Throws
    , m_streambuf(&m_file)                    // Throws
    , m_out(&m_streambuf)                     // Throws
    , m_reopen_log_file(reopen_log_file)
    , m_timestamp_formatter{std::move(config)}
{
}


void ReopeningFileLogger::do_log(util::Logger::Level level, const std::string& message)
{
    if (REALM_UNLIKELY(m_reopen_log_file)) {
        do_log_2(level, "Reopening log file");                // Throws
        util::File new_file(m_path, util::File::mode_Append); // Throws
        m_file = std::move(new_file);
        m_reopen_log_file = 0;
    }
    do_log_2(level, std::move(message)); // Throws
}


void ReopeningFileLogger::do_log_2(util::Logger::Level level, const std::string& message)
{
    auto now = std::chrono::system_clock::now();
    m_out << m_timestamp_formatter.format(now) << ": " << get_level_prefix(level) << message << '\n'; // Throws
    m_out.flush();                                                                                    // Throws
}
