////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include "sync_manager.hpp"

#include "impl/sync_client.hpp"
#include "impl/sync_session.hpp"

#include <thread>

using namespace realm;
using namespace realm::_impl;

SyncManager& SyncManager::shared()
{
    static SyncManager manager;
    return manager;
}

void SyncManager::set_log_level(util::Logger::Level level) noexcept
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_log_level = level;
}

void SyncManager::set_logger_factory(SyncLoggerFactory& factory) noexcept
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_logger_factory = &factory;
}

void SyncManager::set_error_handler(std::function<sync::Client::ErrorHandler> handler)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto wrapped_handler = [=](int error_code, std::string message) {
        // FIXME: If the sync team decides to route all errors through the session-level error handler, the client-level
        // error handler might go away altogether.
        switch (error_code) {
            case 100:       // Connection closed (no error)
            case 101:       // Unspecified non-critical error
                return;
            default:
                handler(error_code, message);
        }
    };
    m_error_handler = std::move(wrapped_handler);
}

void SyncManager::set_login_function(SyncLoginFunction login_function)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_login_function = std::move(login_function);
}

SyncLoginFunction& SyncManager::get_sync_login_function()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    // Precondition: binding must set a login callback before connecting any synced Realms.
    REALM_ASSERT(m_login_function);
    return m_login_function;
}

std::unique_ptr<SyncSession> SyncManager::create_session(std::string realm_path) const
{
    auto client = get_sync_client(); // Throws
    return std::make_unique<SyncSession>(std::move(client), std::move(realm_path)); // Throws
}

std::shared_ptr<SyncClient> SyncManager::get_sync_client() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_sync_client)
        m_sync_client = create_sync_client(); // Throws
    return m_sync_client;
}

std::shared_ptr<SyncClient> SyncManager::create_sync_client() const
{
    REALM_ASSERT(!m_mutex.try_lock());

    std::unique_ptr<util::Logger> logger;
    if (m_logger_factory) {
        logger = m_logger_factory->make_logger(m_log_level); // Throws
    }
    else {
        auto stderr_logger = std::make_unique<util::StderrLogger>(); // Throws
        stderr_logger->set_level_threshold(m_log_level);
        logger = std::move(stderr_logger);
    }
    return std::make_shared<SyncClient>(std::move(logger), std::move(m_error_handler));
}
