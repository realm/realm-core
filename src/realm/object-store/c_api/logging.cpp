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

#include "types.hpp"
#include "util.hpp"
#include "logging.hpp"

namespace realm::c_api {
namespace {
using Logger = realm::util::Logger;

static_assert(realm_log_level_e(Logger::Level::all) == RLM_LOG_LEVEL_ALL);
static_assert(realm_log_level_e(Logger::Level::trace) == RLM_LOG_LEVEL_TRACE);
static_assert(realm_log_level_e(Logger::Level::debug) == RLM_LOG_LEVEL_DEBUG);
static_assert(realm_log_level_e(Logger::Level::detail) == RLM_LOG_LEVEL_DETAIL);
static_assert(realm_log_level_e(Logger::Level::info) == RLM_LOG_LEVEL_INFO);
static_assert(realm_log_level_e(Logger::Level::warn) == RLM_LOG_LEVEL_WARNING);
static_assert(realm_log_level_e(Logger::Level::error) == RLM_LOG_LEVEL_ERROR);
static_assert(realm_log_level_e(Logger::Level::fatal) == RLM_LOG_LEVEL_FATAL);
static_assert(realm_log_level_e(Logger::Level::off) == RLM_LOG_LEVEL_OFF);

class CLogger : public realm::util::RootLogger {
public:
    CLogger(SharedUserdata userdata, realm_log_func_t log_callback, Logger::Level level)
        : RootLogger()
        , m_userdata(std::move(userdata))
        , m_log_callback(log_callback)
    {
        this->set_level_threshold(level);
    }

protected:
    void do_log(Logger::Level level, const std::string& message) final
    {
        m_log_callback(m_userdata.get(), realm_log_level_e(level), message.c_str());
    }

private:
    SharedUserdata m_userdata;
    realm_log_func_t m_log_callback;
};
} // namespace

realm::SyncClientConfig::LoggerFactory make_logger_factory(realm_log_func_t logger, realm_userdata_t userdata,
                                                           realm_free_userdata_func_t free_userdata)
{
    return [logger, userdata = SharedUserdata{userdata, free_userdata}](Logger::Level level) {
        return std::make_unique<CLogger>(userdata, logger, level);
    };
}
} // namespace realm::c_api
