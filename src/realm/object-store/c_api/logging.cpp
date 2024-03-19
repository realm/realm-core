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

class CLogger : public realm::util::Logger {
public:
    CLogger(UserdataPtr userdata, realm_log_func_t log_callback)
        : Logger()
        , m_userdata(std::move(userdata))
        , m_log_callback(log_callback)
    {
    }

protected:
    void do_log(const util::LogCategory& category, Logger::Level level, const std::string& message) final
    {
        m_log_callback(m_userdata.get(), category.get_name().c_str(), realm_log_level_e(level), message.c_str());
    }

private:
    UserdataPtr m_userdata;
    realm_log_func_t m_log_callback;
};
} // namespace

RLM_API void realm_set_log_callback(realm_log_func_t callback, realm_userdata_t userdata,
                                    realm_free_userdata_func_t userdata_free) noexcept
{
    std::shared_ptr<util::Logger> logger;
    if (callback) {
        logger = std::make_shared<CLogger>(UserdataPtr{userdata, userdata_free}, callback);
    }
    util::Logger::set_default_logger(std::move(logger));
}

RLM_API void realm_set_log_level(realm_log_level_e level) noexcept
{
    util::LogCategory::realm.set_default_level_threshold(realm::util::LogCategory::Level(level));
}

RLM_API realm_log_level_e realm_set_log_level_category(const char* category_name, realm_log_level_e level) noexcept
{
    auto& cat = util::LogCategory::get_category(category_name);
    realm_log_level_e prev_level = realm_log_level_e(util::Logger::get_default_logger()->get_level_threshold(cat));
    cat.set_default_level_threshold(realm::util::LogCategory::Level(level));
    return prev_level;
}

RLM_API realm_log_level_e realm_get_log_level_category(const char* category_name) noexcept
{
    auto& cat = util::LogCategory::get_category(category_name);
    return realm_log_level_e(util::Logger::get_default_logger()->get_level_threshold(cat));
}

RLM_API size_t realm_get_category_names(size_t num_values, const char** out_values)
{
    auto vec = util::LogCategory::get_category_names();
    auto number_to_copy = vec.size();
    if (num_values > 0) {
        if (number_to_copy > num_values)
            number_to_copy = num_values;
        for (size_t n = 0; n < number_to_copy; n++) {
            out_values[n] = vec[n];
        }
    }
    return number_to_copy;
}

} // namespace realm::c_api
