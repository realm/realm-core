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
        case Level::all:
        case Level::trace:
        case Level::debug:
        case Level::detail:
        case Level::info:
            break;
        case Level::warn:
            return "WARNING: ";
        case Level::error:
            return "ERROR: ";
        case Level::fatal:
            return "FATAL: ";
        case Level::off:
            break;
    }
    return "";
}

void StderrLogger::do_log(Level level, const std::string& message)
{
    std::cerr << get_level_prefix(level) << message << '\n'; // Throws
    std::cerr.flush();                                       // Throws
}

void StreamLogger::do_log(Level level, const std::string& message)
{
    m_out << get_level_prefix(level) << message << '\n'; // Throws
    m_out.flush();                                       // Throws
}

void ThreadSafeLogger::do_log(Level level, const std::string& message)
{
    LockGuard l(m_mutex);
    Logger::do_log(m_base_logger, level, message); // Throws
}

void PrefixLogger::do_log(Level level, const std::string& message)
{
    Logger::do_log(m_base_logger, level, m_prefix + message); // Throws
}

} // namespace realm::util
