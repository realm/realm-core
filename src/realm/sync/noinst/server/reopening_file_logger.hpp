
#ifndef REALM_NOINST_FILE_LOGGER_HPP
#define REALM_NOINST_FILE_LOGGER_HPP

#include <csignal>
#include <chrono>
#include <ostream>

#include <realm/util/file.hpp>
#include <realm/util/logger.hpp>
#include <realm/util/timestamp_formatter.hpp>

namespace realm {
namespace _impl {

class ReopeningFileLogger : public util::RootLogger {
public:
    using Precision = util::TimestampFormatter::Precision;
    using TimestampConfig = util::TimestampFormatter::Config;

    explicit ReopeningFileLogger(std::string path, volatile std::sig_atomic_t& reopen_log_file, TimestampConfig = {});

protected:
    void do_log(util::Logger::Level, const std::string& message) override;

private:
    const std::string m_path;
    util::File m_file;
    util::File::Streambuf m_streambuf;
    std::ostream m_out;
    volatile std::sig_atomic_t& m_reopen_log_file;
    util::TimestampFormatter m_timestamp_formatter;

    void do_log_2(util::Logger::Level level, const std::string& message);
};

} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_FILE_LOGGER_HPP
