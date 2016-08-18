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

#include <thread>

using namespace realm;
using namespace realm::_impl;

namespace realm {
namespace _impl {

struct SyncClient {
    sync::Client client;

    SyncClient(std::unique_ptr<util::Logger> logger, std::function<sync::Client::ErrorHandler> handler)
    : client(make_client(*logger)) // Throws
    , m_logger(std::move(logger))
    , m_thread([this, handler=std::move(handler)] {
        client.set_error_handler(std::move(handler));
        client.run();
    }) // Throws
    {
    }

    ~SyncClient()
    {
        client.stop();
        m_thread.join();
    }

private:
    static sync::Client make_client(util::Logger& logger)
    {
        sync::Client::Config config;
        config.logger = &logger;
        return sync::Client(std::move(config)); // Throws
    }

    const std::unique_ptr<util::Logger> m_logger;
    std::thread m_thread;
};

} // namespace _impl
} // namespace realm


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
    m_error_handler = std::move(handler);
}

sync::Client& SyncManager::get_sync_client() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_sync_client)
        m_sync_client = create_sync_client(); // Throws
    return m_sync_client->client;
}

std::unique_ptr<SyncClient> SyncManager::create_sync_client() const
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
    return std::make_unique<SyncClient>(std::move(logger), std::move(m_error_handler));
}
