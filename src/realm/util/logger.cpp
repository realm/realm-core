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

std::pair<std::string, size_t> Logger::subst_prepare(State& state)
{
    state.m_formatter << "%" << state.m_param_num;
    std::string key = state.m_formatter.str();
    state.m_formatter.str(std::string());
    size_t j = state.m_search.find(key);
    return std::make_pair(std::move(key), j);
}

void Logger::subst_finish(State& state, size_t j, const std::string& key)
{
    std::string str = state.m_formatter.str();
    state.m_formatter.str(std::string());
    state.m_message.replace(j, key.size(), str);
    state.m_search.replace(j, key.size(), std::string(str.size(), '\0'));
}

} // namespace realm::util