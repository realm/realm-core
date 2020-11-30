#ifndef REALM_TEST_UTIL_LOGGER_HPP
#define REALM_TEST_UTIL_LOGGER_HPP

#include <realm/util/logger.hpp>
#include <regex>
#include <deque>
#include <mutex>

namespace realm {
namespace test_util {

/// A thread-safe Logger implementation that allows testing whether a particular
/// log message was emitted.
class TestLogger : public util::Logger, private util::Logger::LevelThreshold {
public:
    using util::Logger::Level;

    /// Construct a TestLogger. If \a forward_to is non-null, a copy of each log
    /// message will be forwarded to that logger.
    TestLogger(util::Logger* forward_to = nullptr)
        : util::Logger(static_cast<const util::Logger::LevelThreshold&>(*this))
        , m_forward_to(forward_to)
    {
        if (forward_to == this)
            forward_to = nullptr;
    }

    /// Return true if a log message matching \a rx was emitted at the given log
    /// level. If \a at_level is `Level::all`, log messages at all levels are
    /// checked.
    ///
    /// The regular expression is matched using `std::regex_search()`, which
    /// means that a substring match will return true (the regular expression is
    /// not required to match the whole message).
    ///
    /// The level prefix ("INFO", "WARNING", etc.) is not part of the input to
    /// the regular expression match.
    ///
    /// This method is thread-safe.
    bool did_log(const std::regex& rx, Level at_level = Level::all,
                 std::regex_constants::match_flag_type = std::regex_constants::match_default) const;

    /// Return true if any message was emitted at (and only at) the given log
    /// level. If \a at_level is `Level::all`, returns true if any log message
    /// was emitted at any level.
    ///
    /// This method is thread-safe.
    bool did_log(Level at_level) const;

    /// Write the whole log to \a os as if the log was emitted with the given
    /// level. If \a at_level is `Level::all`, the full log is written out.
    ///
    /// This method is thread-safe.
    void write(std::ostream& os, Level at_level = Level::all) const;

protected:
    /// Logger implementation. This method is thread-safe.
    void do_log(Level, std::string message) override;

private:
    /// LevelThreshold implementation
    Level get() const noexcept override final
    {
        return Level::all;
    }

    struct Message {
        Level level;
        std::string message;
    };

    std::deque<Message> m_messages;
    mutable std::mutex m_mutex;
    util::Logger* m_forward_to;
};


/// Implementation:

inline bool TestLogger::did_log(const std::regex& rx, Level at_level,
                                std::regex_constants::match_flag_type flags) const
{
    std::lock_guard<std::mutex> l(m_mutex);
    for (const auto& message : m_messages) {
        if (at_level == Level::all || message.level == at_level) {
            if (std::regex_search(message.message, rx, flags))
                return true;
        }
    }
    return false;
}

inline bool TestLogger::did_log(Level at_level) const
{
    std::lock_guard<std::mutex> l(m_mutex);
    if (at_level == Level::all)
        return m_messages.size() != 0;

    return std::any_of(begin(m_messages), end(m_messages), [&](auto& message) {
        return message.level == at_level;
    });
}

inline void TestLogger::write(std::ostream& os, Level threshold) const
{
    std::lock_guard<std::mutex> l(m_mutex);
    for (const auto& message : m_messages) {
        if (message.level < threshold)
            continue;
        os << get_level_prefix(message.level);
        os << message.message << '\n';
    }
}

inline void TestLogger::do_log(Level level, std::string message)
{
    std::lock_guard<std::mutex> l(m_mutex);
    if (m_forward_to) {
        m_forward_to->log(level, message.c_str());
    }
    m_messages.emplace_back(Message{level, std::move(message)});
}


} // namespace test_util
} // namespace realm


#endif // REALM_TEST_UTIL_LOGGER_HPP
