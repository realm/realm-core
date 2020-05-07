
#ifndef REALM_UTIL_DUPLICATING_LOGGER_HPP
#define REALM_UTIL_DUPLICATING_LOGGER_HPP

#include <realm/util/logger.hpp>


namespace realm {
namespace util {

/// The log level threshold of a logger of this type will be decided by the
/// associated base logger. Therefore, the log level threshold specified via the
/// auxiliary logger will be ignored.
///
/// Loggers of this type are thread-safe if the base logger and the auxiliary
/// loggers are both thread-safe.
class DuplicatingLogger : public Logger {
public:
    explicit DuplicatingLogger(Logger& base_logger, Logger& aux_logger) noexcept;

protected:
    void do_log(Logger::Level, std::string message) override;

private:
    Logger& m_base_logger;
    Logger& m_aux_logger;
};


// Implementation

inline DuplicatingLogger::DuplicatingLogger(Logger& base_logger, Logger& aux_logger) noexcept
    : Logger{base_logger.level_threshold}
    , m_base_logger{base_logger}
    , m_aux_logger{aux_logger}
{
}


} // namespace util
} // namespace realm

#endif // REALM_UTIL_DUPLICATING_LOGGER_HPP
