/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_UTIL_LOGGER_HPP
#define REALM_UTIL_LOGGER_HPP

#include <realm/util/features.h>
#include <realm/util/thread.hpp>
#include <realm/util/file.hpp>

#include <cstring>
#include <ostream>
#include <string>
#include <utility>
#include <array>

namespace realm::util {

class Logger;

class LogCategory {
public:
    /// Specifies criticality when passed to log(). Functions as a criticality
    /// threshold when returned from LevelThreshold::get().
    ///
    ///     error   Be silent unless when there is an error.
    ///     warn    Be silent unless when there is an error or a warning.
    ///     info    Reveal information about what is going on, but in a
    ///             minimalistic fashion to avoid general overhead from logging
    ///             and to keep volume down.
    ///     detail  Same as 'info', but prioritize completeness over minimalism.
    ///     debug   Reveal information that can aid debugging, no longer paying
    ///             attention to efficiency.
    ///     trace   A version of 'debug' that allows for very high volume
    ///             output.
    // equivalent to realm_log_level_e in realm.h and must be kept in sync -
    // this is enforced in logging.cpp.
    enum class Level : int {
        all = 0,
        trace = 1,
        debug = 2,
        detail = 3,
        info = 4,
        warn = 5,
        error = 6,
        fatal = 7,
        off = 8
    };

    static LogCategory realm; // Top level category

    static LogCategory storage;           // Everything about mutating and querying the database
    static LogCategory /**/ transaction;  // Creating, advancing and committing transactions
    static LogCategory /**/ query;        // Query operations
    static LogCategory /**/ object;       // Mutations of the database
    static LogCategory /**/ notification; // Reporting changes to the database

    static LogCategory sync;               // Everything about device sync
    static LogCategory /**/ client;        // Catch-all category for client operations
    static LogCategory /*    */ session;   // Connection level activity
    static LogCategory /*    */ changeset; // Reception, upload and integration of changesets
    static LogCategory /*    */ network;   // Low level network activity
    static LogCategory /*    */ reset;     // Client reset operations
    static LogCategory /**/ server;        // All server activity (only relevant for test runs)

    static LogCategory app; // Activity at the app level

    static LogCategory sdk; // Tracing of SDK activity

    static constexpr size_t nb_categories = 15;

    LogCategory(std::string_view name, LogCategory* parent);

    // Set the default threshold level for category. All children
    // will be assigned the same level
    void set_default_level_threshold(Level);
    Level get_default_level_threshold() const noexcept;

    const std::string& get_name() const noexcept
    {
        return m_name;
    }

    // Find category from fully qualified name. Will throw if
    // name does not match a category
    static LogCategory& get_category(std::string_view name);
    static std::vector<std::string_view> get_category_names();

private:
    friend class Logger;
    static size_t s_next_index;
    size_t m_index = 0;
    std::string m_name;
    std::atomic<Level> m_default_level;
    std::vector<LogCategory*> m_children;

    size_t get_index() const noexcept
    {
        return m_index;
    }
    void set_level_threshold(Logger*, Level) const;
    void set_default_level_threshold(Logger*) const;
};

class LogCategoryRef {
public:
    LogCategoryRef(LogCategory& cat)
        : m_category(cat)
    {
    }
    LogCategory& operator*()
    {
        return m_category;
    }
    static LogCategoryRef get_category(std::string_view name)
    {
        return LogCategoryRef(LogCategory::get_category(name));
    }

private:
    LogCategory& m_category;
};

/// All messages logged with a level that is lower than the current threshold
/// will be dropped. For the sake of efficiency, this test happens before the
/// message is formatted. This class allows for the log level threshold to be
/// changed over time and any subclasses will share the same reference to a
/// log level threshold instance. The default log level threshold is
/// Logger::Level::info and is defined by Logger::default_log_level.
///
/// The Logger threshold level is intrinsically thread safe since it uses an
/// atomic to store the value. However, the do_log() operation is not, so it
/// is up to the subclass to ensure thread safety of the output operation.
///
/// Examples:
///
///    logger.error("Overlong message from master coordinator");
///    logger.info("Listening for peers on %1:%2", listen_address, listen_port);
class Logger {
public:
    template <class... Params>
    void trace(const LogCategory&, const char* message, Params&&...);
    template <class... Params>
    void trace(const char* message, Params&&... params)
    {
        trace(m_category, message, std::forward<Params>(params)...);
    }

    template <class... Params>
    void debug(const LogCategory&, const char* message, Params&&...);
    template <class... Params>
    void debug(const char* message, Params&&... params)
    {
        debug(m_category, message, std::forward<Params>(params)...);
    }

    template <class... Params>
    void detail(const LogCategory&, const char* message, Params&&...);
    template <class... Params>
    void detail(const char* message, Params&&... params)
    {
        detail(m_category, message, std::forward<Params>(params)...);
    }

    template <class... Params>
    void info(const LogCategory&, const char* message, Params&&...);
    template <class... Params>
    void info(const char* message, Params&&... params)
    {
        info(m_category, message, std::forward<Params>(params)...);
    }

    template <class... Params>
    void warn(const LogCategory&, const char* message, Params&&...);
    template <class... Params>
    void warn(const char* message, Params&&... params)
    {
        warn(m_category, message, std::forward<Params>(params)...);
    }

    template <class... Params>
    void error(const LogCategory&, const char* message, Params&&...);
    template <class... Params>
    void error(const char* message, Params&&... params)
    {
        error(m_category, message, std::forward<Params>(params)...);
    }

    template <class... Params>
    void fatal(const LogCategory&, const char* message, Params&&...);
    template <class... Params>
    void fatal(const char* message, Params&&... params)
    {
        fatal(m_category, message, std::forward<Params>(params)...);
    }

    using Level = LogCategory::Level;
    using ThresholdLevels = std::array<std::atomic<Level>, LogCategory::nb_categories>;

    static constexpr size_t max_width_of_value = 80;

    template <class... Params>
    void log(Level level, const char* message, Params&&... params)
    {
        log(m_category, level, message, std::forward<Params>(params)...);
    }

    template <class... Params>
    void log(const LogCategory&, Level, const char* message, Params&&...);

    // Get threshold level for the category this logger belongs to
    Level get_level_threshold() const noexcept
    {
        return get_level_threshold(m_category);
    }

    // Get threshold level for the specific category
    Level get_level_threshold(std::string_view cat_name) const noexcept
    {
        return get_level_threshold(LogCategory::get_category(cat_name));
    }
    Level get_level_threshold(const LogCategory& cat) const noexcept
    {
        return get_level_threshold(cat.get_index());
    }

    // Set threshold level for the category this logger belongs to
    void set_level_threshold(Level level) noexcept
    {
        set_level_threshold(m_category, level);
    }

    const LogCategory& get_category() const noexcept
    {
        return m_category;
    }

    // Set threshold level for the specific category
    void set_level_threshold(std::string_view cat_name, Level level) noexcept
    {
        set_level_threshold(LogCategory::get_category(cat_name), level);
    }
    void set_level_threshold(const LogCategory& cat, Level level) noexcept
    {
        cat.set_level_threshold(this, level);
    }

    /// Shorthand for `int(level) >= int(get_level_threshold())`.
    inline bool would_log(Level level) const noexcept
    {
        return static_cast<int>(level) >= static_cast<int>(get_level_threshold());
    }
    /// Shorthand for `int(level) >= int(get_level_threshold(cat))`.
    inline bool would_log(const LogCategory& cat, Level level) const noexcept
    {
        return static_cast<int>(level) >= static_cast<int>(get_level_threshold(cat.get_index()));
    }

    virtual inline ~Logger() noexcept = default;

    static void set_default_logger(std::shared_ptr<util::Logger>) noexcept;
    static std::shared_ptr<util::Logger>& get_default_logger() noexcept;
    static const std::string_view level_to_string(Level level) noexcept;

private:
    friend class LogCategory;
    // Only used by the base Logger class
    std::unique_ptr<ThresholdLevels> m_threshold_base;

protected:
    // Shared level threshold for subclasses that link to a base logger
    // See PrefixLogger and ThreadSafeLogger
    ThresholdLevels& m_level_thresholds;
    // Used when log() is called without a category.
    const LogCategory& m_category{LogCategory::realm};

    Logger() noexcept
        : m_threshold_base{new ThresholdLevels}
        , m_level_thresholds{*m_threshold_base}
    {
        m_category.set_default_level_threshold(this);
    }

    explicit Logger(const Logger& base_logger) noexcept
        : m_level_thresholds{base_logger.m_level_thresholds}
    {
    }

    Logger(const LogCategory& category, const Logger& base_logger) noexcept
        : m_level_thresholds{base_logger.m_level_thresholds}
        , m_category(category)
    {
    }

    Level get_level_threshold(size_t index) const noexcept
    {
        // Don't need strict ordering, mainly that the gets/sets are atomic
        return Level(m_level_thresholds[index].load(std::memory_order_relaxed));
    }
    static Level get_level_threshold(const Logger& logger, size_t index) noexcept
    {
        return logger.get_level_threshold(index);
    }
    void set_level_threshold(size_t index, Level level) noexcept
    {
        // Don't need strict ordering, mainly that the gets/sets are atomic
        m_level_thresholds[index].store(level, std::memory_order_relaxed);
    }

    static void do_log(Logger&, const LogCategory& category, Level, const std::string& message);

    virtual void do_log(const LogCategory& category, Level, const std::string& message) = 0;

    static const char* get_level_prefix(Level) noexcept;

private:
    template <class... Params>
    REALM_NOINLINE void do_log(const LogCategory& category, Level, const char* message, Params&&...);
};

template <class C, class T>
std::basic_ostream<C, T>& operator<<(std::basic_ostream<C, T>&, Logger::Level);

template <class C, class T>
std::basic_istream<C, T>& operator>>(std::basic_istream<C, T>&, Logger::Level&);

/// A logger that writes to STDERR, which is thread safe.
/// Since this class is a subclass of Logger, it maintains its own modifiable log
/// level threshold.
class StderrLogger : public Logger {
public:
    StderrLogger() noexcept = default;

    StderrLogger(Level level) noexcept
        : Logger()
    {
        set_level_threshold(level);
    }

protected:
    void do_log(const LogCategory& category, Level, const std::string&) final;
};


/// A logger that writes to a stream. This logger is not thread-safe.
///
/// Since this class is a subclass of Logger, it maintains its own modifiable log
/// level threshold.
class StreamLogger : public Logger {
public:
    explicit StreamLogger(std::ostream&) noexcept;

protected:
    void do_log(const LogCategory& category, Level, const std::string&) final;

private:
    std::ostream& m_out;
};


/// A logger that writes to a new file. This logger is not thread-safe.
///
/// Since this class is a subclass of Logger, it maintains its own thread safe log
/// level threshold.
class FileLogger : public StreamLogger {
public:
    explicit FileLogger(std::string path);
    explicit FileLogger(util::File);

private:
    util::File m_file;
    util::File::Streambuf m_streambuf;
    std::ostream m_out;
};

/// A logger that appends to a file. This logger is not thread-safe.
///
/// Since this class is a subclass of Logger, it maintains its own thread safe log
/// level threshold.
class AppendToFileLogger : public StreamLogger {
public:
    explicit AppendToFileLogger(std::string path);
    explicit AppendToFileLogger(util::File);

private:
    util::File m_file;
    util::File::Streambuf m_streambuf;
    std::ostream m_out;
};


/// A thread-safe logger where do_log() is thread safe. The log level is already
/// thread safe since Logger uses an atomic to store the log level threshold.
class ThreadSafeLogger : public Logger {
public:
    explicit ThreadSafeLogger(const std::shared_ptr<Logger>& base_logger) noexcept;

protected:
    void do_log(const LogCategory& category, Level, const std::string&) final;

private:
    Mutex m_mutex;
    std::shared_ptr<Logger> m_base_logger_ptr;
};


/// A logger that adds a fixed prefix to each message.
class PrefixLogger : public Logger {
public:
    // Construct a PrefixLogger from any Logger shared_ptr (PrefixLogger, StdErrLogger, etc.)
    // The first PrefixLogger must always be created from a Logger shared_ptr, subsequent PrefixLoggers
    // created, will point back to this logger shared_ptr for referencing the level_threshold when
    // logging output.
    PrefixLogger(std::string prefix, const std::shared_ptr<Logger>& base_logger) noexcept
        : Logger(*base_logger) // Save an alias of the passed in base_logger shared_ptr
        , m_prefix{std::move(prefix)}
        , m_owned_logger{base_logger}
        , m_chained_logger{*m_owned_logger} // do_log() writes to the chained logger
    {
    }

    // Same as above, but with a specific category
    PrefixLogger(const LogCategory& category, std::string prefix, const std::shared_ptr<Logger>& base_logger) noexcept
        : Logger(category, *base_logger) // Save an alias of the passed in base_logger shared_ptr
        , m_prefix{std::move(prefix)}
        , m_owned_logger{base_logger}
        , m_chained_logger{*m_owned_logger} // do_log() writes to the chained logger
    {
    }

    // Construct a PrefixLogger from another PrefixLogger object for chaining the prefixes on log output
    PrefixLogger(std::string prefix, PrefixLogger& prefix_logger) noexcept
        // Save an alias of the base_logger shared_ptr from the passed in PrefixLogger
        : Logger(prefix_logger)
        , m_prefix{std::move(prefix)}
        , m_chained_logger{prefix_logger} // do_log() writes to the chained logger
    {
    }

protected:
    void do_log(const LogCategory& category, Level, const std::string&) final;

private:
    const std::string m_prefix;
    // The next logger in the chain for chained PrefixLoggers or the base_logger
    std::shared_ptr<Logger> m_owned_logger;
    Logger& m_chained_logger;
};

/// A logger that uses a specific category for all log entries
class CategoryLogger : public util::Logger {
public:
    CategoryLogger(const LogCategory& category, const std::shared_ptr<Logger>& base_logger) noexcept
        : Logger(category, *base_logger)
        , m_base_logger_ptr(base_logger)
    {
    }

protected:
    void do_log(const util::LogCategory& category, Level level, const std::string& message) final
    {
        Logger::do_log(*m_base_logger_ptr, category, level, message);
    }

private:
    std::shared_ptr<Logger> m_base_logger_ptr;
};

// Logger with a local log level that is independent of the parent log level threshold
// The LocalThresholdLogger will define its own atomic log level threshold and
// will be unaffected by changes to the log level threshold of the parent.
// In addition, any changes to the log level threshold of this class or any
// subsequent linked loggers will not change the log level threshold of the
// parent. The parent will only be used for outputting log messages.
class LocalThresholdLogger : public Logger {
public:
    // A shared_ptr parent must be provided for this class for log output
    // Local log level is initialized with the current value from the provided logger
    LocalThresholdLogger(const std::shared_ptr<Logger>&);

    // A shared_ptr parent must be provided for this class for log output
    LocalThresholdLogger(const std::shared_ptr<Logger>&, Level);

    void do_log(const LogCategory& category, Logger::Level level, std::string const& message) override;

protected:
    std::shared_ptr<Logger> m_chained_logger;
};

/// A logger that performs a noop when logging functions are called
class NullLogger : public Logger {
    // Since we don't want to log anything, do_log() does nothing
    void do_log(const LogCategory&, Level, const std::string&) override {}
};


// Implementation

template <class... Params>
inline void Logger::trace(const LogCategory& cat, const char* message, Params&&... params)
{
    log(cat, Level::trace, message, std::forward<Params>(params)...); // Throws
}

template <class... Params>
inline void Logger::debug(const LogCategory& cat, const char* message, Params&&... params)
{
    log(cat, Level::debug, message, std::forward<Params>(params)...); // Throws
}

template <class... Params>
inline void Logger::detail(const LogCategory& cat, const char* message, Params&&... params)
{
    log(cat, Level::detail, message, std::forward<Params>(params)...); // Throws
}

template <class... Params>
inline void Logger::info(const LogCategory& cat, const char* message, Params&&... params)
{
    log(cat, Level::info, message, std::forward<Params>(params)...); // Throws
}

template <class... Params>
inline void Logger::warn(const LogCategory& cat, const char* message, Params&&... params)
{
    log(cat, Level::warn, message, std::forward<Params>(params)...); // Throws
}

template <class... Params>
inline void Logger::error(const LogCategory& cat, const char* message, Params&&... params)
{
    log(cat, Level::error, message, std::forward<Params>(params)...); // Throws
}

template <class... Params>
inline void Logger::fatal(const LogCategory& cat, const char* message, Params&&... params)
{
    log(cat, Level::fatal, message, std::forward<Params>(params)...); // Throws
}

template <class... Params>
inline void Logger::log(const LogCategory& cat, Level level, const char* message, Params&&... params)
{
    if (would_log(cat, level))
        do_log(cat, level, message, std::forward<Params>(params)...); // Throws
#if REALM_DEBUG
    else {
        // Do the string formatting even if it won't be logged to hopefully
        // catch invalid format strings
        static_cast<void>(format(message, std::forward<Params>(params)...)); // Throws
    }
#endif
}

inline void Logger::do_log(Logger& logger, const LogCategory& category, Level level, const std::string& message)
{
    logger.do_log(category, level, message); // Throws
}

template <class... Params>
void Logger::do_log(const LogCategory& category, Level level, const char* message, Params&&... params)
{
    do_log(category, level, format(message, std::forward<Params>(params)...)); // Throws
}

template <class C, class T>
std::basic_ostream<C, T>& operator<<(std::basic_ostream<C, T>& out, Logger::Level level)
{
    out << Logger::level_to_string(level);
    return out;
}

template <class C, class T>
std::basic_istream<C, T>& operator>>(std::basic_istream<C, T>& in, Logger::Level& level)
{
    std::basic_string<C, T> str;
    auto check = [&](const char* name) {
        size_t n = strlen(name);
        if (n != str.size())
            return false;
        for (size_t i = 0; i < n; ++i) {
            if (in.widen(name[i]) != str[i])
                return false;
        }
        return true;
    };
    if (in >> str) {
        if (check("all")) {
            level = Logger::Level::all;
        }
        else if (check("trace")) {
            level = Logger::Level::trace;
        }
        else if (check("debug")) {
            level = Logger::Level::debug;
        }
        else if (check("detail")) {
            level = Logger::Level::detail;
        }
        else if (check("info")) {
            level = Logger::Level::info;
        }
        else if (check("warn")) {
            level = Logger::Level::warn;
        }
        else if (check("error")) {
            level = Logger::Level::error;
        }
        else if (check("fatal")) {
            level = Logger::Level::fatal;
        }
        else if (check("off")) {
            level = Logger::Level::off;
        }
        else {
            in.setstate(std::ios_base::failbit);
        }
    }
    return in;
}

inline StreamLogger::StreamLogger(std::ostream& out) noexcept
    : m_out(out)
{
}

inline FileLogger::FileLogger(std::string path)
    : StreamLogger(m_out)
    , m_file(path, util::File::mode_Write) // Throws
    , m_streambuf(&m_file)                 // Throws
    , m_out(&m_streambuf)                  // Throws
{
}

inline FileLogger::FileLogger(util::File file)
    : StreamLogger(m_out)
    , m_file(std::move(file))
    , m_streambuf(&m_file) // Throws
    , m_out(&m_streambuf)  // Throws
{
}

inline AppendToFileLogger::AppendToFileLogger(std::string path)
    : StreamLogger(m_out)
    , m_file(path, util::File::mode_Append) // Throws
    , m_streambuf(&m_file)                  // Throws
    , m_out(&m_streambuf)                   // Throws
{
}

inline AppendToFileLogger::AppendToFileLogger(util::File file)
    : StreamLogger(m_out)
    , m_file(std::move(file))
    , m_streambuf(&m_file) // Throws
    , m_out(&m_streambuf)  // Throws
{
}

inline ThreadSafeLogger::ThreadSafeLogger(const std::shared_ptr<Logger>& base_logger) noexcept
    : Logger(*base_logger)
    , m_base_logger_ptr(base_logger)
{
}

// Construct a LocalThresholdLogger using the current log level value from the parent
inline LocalThresholdLogger::LocalThresholdLogger(const std::shared_ptr<Logger>& base_logger)
    : Logger()
    , m_chained_logger{base_logger}
{
    for (size_t i = 0; i < LogCategory::nb_categories; i++) {
        set_level_threshold(i, get_level_threshold(*base_logger, i));
    }
}

// Construct a LocalThresholdLogger using the provided log level threshold value
inline LocalThresholdLogger::LocalThresholdLogger(const std::shared_ptr<Logger>& base_logger, Level threshold)
    : Logger()
    , m_chained_logger{base_logger}
{
    // Verify the passed in shared ptr is not null
    REALM_ASSERT(m_chained_logger);
    set_level_threshold(threshold);
}

// Intended to be used to get a somewhat smaller number derived from 'this' pointer
inline unsigned gen_log_id(void* p)
{
    return (size_t(p) >> 4) & 0xffff;
}
} // namespace realm::util

#endif // REALM_UTIL_LOGGER_HPP
