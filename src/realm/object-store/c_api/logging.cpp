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

#include "logging.hpp"
#include <realm/object-store/c_api/types.hpp>

namespace realm::c_api {
namespace {
using Logger = realm::util::Logger;
class CLogger : public Logger::LevelThreshold, public Logger {
public:
    CLogger(UserdataPtr userdata, realm_logger_log_func_t log_callback,
            realm_logger_get_threshold_func_t get_threshold)
        : Logger::LevelThreshold()
        , Logger(static_cast<Logger::LevelThreshold&>(*this))
        , m_userdata(std::move(userdata))
        , m_log_callback(log_callback)
        , m_get_threshold(get_threshold)
    {
    }

protected:
    void do_log(Logger::Level level, const std::string& message) override final
    {
        m_log_callback(m_userdata.get(), to_capi(level), message.c_str());
    }

    Logger::Level get() const noexcept override final
    {
        return from_capi(m_get_threshold(m_userdata.get()));
    }

private:
    UserdataPtr m_userdata;
    realm_logger_log_func_t m_log_callback;
    realm_logger_get_threshold_func_t m_get_threshold;
};
} // namespace
} // namespace realm::c_api

using namespace realm::c_api;

RLM_API realm_logger_t* realm_logger_new(void* userdata, realm_logger_log_func_t log_func,
                                         realm_logger_get_threshold_func_t threshold_func,
                                         realm_free_userdata_func_t free_func)
{
    realm_logger_t* logger = new realm_logger_t;
    logger->logger.reset(new CLogger(UserdataPtr{userdata, free_func}, log_func, threshold_func));
    return logger;
}