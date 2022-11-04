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

namespace realm::util {

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

Logger::Level ThreadSafeLogger::get_level_threshold() noexcept
{
    LockGuard l(m_mutex);
    return Logger::get_level_threshold();
}

void ThreadSafeLogger::set_level_threshold(Level level) noexcept
{
    LockGuard l(m_mutex);
    Logger::set_level_threshold(level);
}

void PrefixLogger::do_log(Level level, const std::string& message)
{
    Logger::do_log(m_base_logger, level, m_prefix + message); // Throws
}

} // namespace realm::util
