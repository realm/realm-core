////////////////////////////////////////////////////////////////////////////
//
// Copyright 2021 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or utilied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/object-store/c_api/util.hpp>

namespace realm::c_api {
namespace {
using Logger = realm::util::Logger;
static inline realm_log_level_e to_capi(Logger::Level level)
{
    switch (level) {
        case Logger::Level::all:
            return RLM_LOG_LEVEL_ALL;
        case Logger::Level::trace:
            return RLM_LOG_LEVEL_TRACE;
        case Logger::Level::debug:
            return RLM_LOG_LEVEL_DEBUG;
        case Logger::Level::detail:
            return RLM_LOG_LEVEL_DETAIL;
        case Logger::Level::info:
            return RLM_LOG_LEVEL_INFO;
        case Logger::Level::warn:
            return RLM_LOG_LEVEL_WARNING;
        case Logger::Level::error:
            return RLM_LOG_LEVEL_ERROR;
        case Logger::Level::fatal:
            return RLM_LOG_LEVEL_FATAL;
        case Logger::Level::off:
            return RLM_LOG_LEVEL_OFF;
    }
    REALM_TERMINATE("Invalid log level."); // LCOV_EXCL_LINE
}

static inline Logger::Level from_capi(realm_log_level_e level)
{
    switch (level) {
        case RLM_LOG_LEVEL_ALL:
            return Logger::Level::all;
        case RLM_LOG_LEVEL_TRACE:
            return Logger::Level::trace;
        case RLM_LOG_LEVEL_DEBUG:
            return Logger::Level::debug;
        case RLM_LOG_LEVEL_DETAIL:
            return Logger::Level::detail;
        case RLM_LOG_LEVEL_INFO:
            return Logger::Level::info;
        case RLM_LOG_LEVEL_WARNING:
            return Logger::Level::warn;
        case RLM_LOG_LEVEL_ERROR:
            return Logger::Level::error;
        case RLM_LOG_LEVEL_FATAL:
            return Logger::Level::fatal;
        case RLM_LOG_LEVEL_OFF:
            return Logger::Level::off;
    }
    REALM_TERMINATE("Invalid log level."); // LCOV_EXCL_LINE
}
} // namespace
} // namespace realm::c_api