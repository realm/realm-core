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

#ifndef REALM_SYNC_MANAGER_HPP
#define REALM_SYNC_MANAGER_HPP

#include "shared_realm.hpp"

#include <realm/sync/client.hpp>
#include <realm/util/logger.hpp>

#include <memory>
#include <mutex>

namespace realm {

namespace _impl {
struct SyncClient;
struct SyncSession;
}

using SyncLoginFunction = std::function<void(const Realm::Config&)>;

class SyncLoggerFactory {
public:
    virtual std::unique_ptr<util::Logger> make_logger(util::Logger::Level) = 0;
};

class SyncManager {
public:
    static SyncManager& shared();

    void set_log_level(util::Logger::Level) noexcept;
    void set_logger_factory(SyncLoggerFactory&) noexcept;
    void set_error_handler(std::function<sync::Client::ErrorHandler>);
    void set_login_function(SyncLoginFunction);

    SyncLoginFunction& get_sync_login_function();
    std::unique_ptr<_impl::SyncSession> create_session(std::string realm_path) const;

private:
    SyncManager() = default;
    SyncManager(const SyncManager&) = delete;
    SyncManager& operator=(const SyncManager&) = delete;

    sync::Client& get_sync_client() const;
    std::unique_ptr<_impl::SyncClient> create_sync_client() const;

    mutable std::mutex m_mutex;

    SyncLoginFunction m_login_function;

    // FIXME: Should probably be util::Logger::Level::error
    util::Logger::Level m_log_level = util::Logger::Level::info;
    SyncLoggerFactory* m_logger_factory = nullptr;
    std::function<sync::Client::ErrorHandler> m_error_handler;

    mutable std::unique_ptr<_impl::SyncClient> m_sync_client;
};

} // namespace realm

#endif // REALM_SYNC_MANAGER_HPP
