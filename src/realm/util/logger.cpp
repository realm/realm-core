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

#include <realm/util/logger.hpp>

#include <iostream>
#include <mutex>

namespace realm::util {

namespace {
auto& s_logger_mutex = *new std::mutex;
std::shared_ptr<util::Logger> s_default_logger;
std::atomic<Logger::Level> s_default_level = Logger::Level::info;
} // anonymous namespace

void Logger::set_default_logger(std::shared_ptr<util::Logger> logger) noexcept
{
    std::lock_guard lock(s_logger_mutex);
    s_default_logger = logger;
}

std::shared_ptr<util::Logger>& Logger::get_default_logger() noexcept
{
    std::lock_guard lock(s_logger_mutex);
    if (!s_default_logger) {
        s_default_logger = std::make_shared<StderrLogger>();
        s_default_logger->set_level_threshold(s_default_level);
    }

    return s_default_logger;
}

void Logger::set_default_level_threshold(Level level) noexcept
{
    std::lock_guard lock(s_logger_mutex);
    s_default_level = level;
    if (s_default_logger)
        s_default_logger->set_level_threshold(level);
}

Logger::Level Logger::get_default_level_threshold() noexcept
{
    return s_default_level.load(std::memory_order_relaxed);
}

const char* Logger::get_level_prefix(Level level) noexcept
{
    switch (level) {
        case Level::off:
            [[fallthrough]];
        case Level::all:
            [[fallthrough]];
        case Level::trace:
            [[fallthrough]];
        case Level::debug:
            [[fallthrough]];
        case Level::detail:
            [[fallthrough]];
        case Level::info:
            break;
        case Level::warn:
            return "WARNING: ";
        case Level::error:
            return "ERROR: ";
        case Level::fatal:
            return "FATAL: ";
    }
    return "";
}

const std::string_view Logger::level_to_string(Level level) noexcept
{
    switch (level) {
        case Logger::Level::all:
            return "all";
        case Logger::Level::trace:
            return "trace";
        case Logger::Level::debug:
            return "debug";
        case Logger::Level::detail:
            return "detail";
        case Logger::Level::info:
            return "info";
        case Logger::Level::warn:
            return "warn";
        case Logger::Level::error:
            return "error";
        case Logger::Level::fatal:
            return "fatal";
        case Logger::Level::off:
            return "off";
    }
    REALM_ASSERT(false);
    return "";
}

void StderrLogger::do_log(Level level, const std::string& message)
{
    // std::cerr is unbuffered, so no need to flush
    std::cerr << get_level_prefix(level) << message << '\n'; // Throws
}

void StreamLogger::do_log(Level level, const std::string& message)
{
    m_out << get_level_prefix(level) << message << std::endl; // Throws
}

void ThreadSafeLogger::do_log(Level level, const std::string& message)
{
    LockGuard l(m_mutex);
    Logger::do_log(*m_base_logger_ptr, level, message); // Throws
}

void PrefixLogger::do_log(Level level, const std::string& message)
{
    Logger::do_log(m_chained_logger, level, m_prefix + message); // Throws
}

void LocalThresholdLogger::do_log(Logger::Level level, std::string const& message)
{
    Logger::do_log(*m_chained_logger, level, message); // Throws
}
} // namespace realm::util
