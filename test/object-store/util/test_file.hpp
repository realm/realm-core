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

#ifndef REALM_TEST_UTIL_TEST_FILE_HPP
#define REALM_TEST_UTIL_TEST_FILE_HPP

#include <realm/object-store/shared_realm.hpp>
#include <realm/object-store/util/tagged_bool.hpp>

#include <realm/util/logger.hpp>
#include <realm/util/optional.hpp>

#include <thread>

#if REALM_ENABLE_SYNC
#include <realm/sync/config.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/app.hpp>
#include "test_utils.hpp"

#include <realm/sync/client.hpp>
#include <realm/sync/noinst/server/server.hpp>

#endif // REALM_ENABLE_SYNC


namespace realm {
struct AppSession;
class Schema;
enum class SyncSessionStopPolicy;
struct DBOptions;
struct SyncConfig;
} // namespace realm

class JoiningThread {
public:
    template <typename... Args>
    JoiningThread(Args&&... args)
        : m_thread(std::forward<Args>(args)...)
    {
    }
    ~JoiningThread()
    {
        if (m_thread.joinable())
            m_thread.join();
    }
    void join()
    {
        m_thread.join();
    }

private:
    std::thread m_thread;
};


struct TestFile : realm::Realm::Config {
    TestFile();
    ~TestFile();

    // The file should outlive the object, ie. should not be deleted in destructor
    void persist()
    {
        m_persist = true;
    }

    realm::DBOptions options() const;

private:
    bool m_persist = false;
    std::string m_temp_dir;
};

struct InMemoryTestFile : TestFile {
    InMemoryTestFile();
};

void advance_and_notify(realm::Realm& realm);
void on_change_but_no_notify(realm::Realm& realm);

#if REALM_ENABLE_SYNC

#ifndef TEST_ENABLE_SYNC_LOGGING
#define TEST_ENABLE_SYNC_LOGGING 0 // change to 1 to enable logging
#endif

struct TestLogger : realm::util::Logger::LevelThreshold, realm::util::Logger {
    void do_log(realm::util::Logger::Level, std::string const&) override {}
    Level get() const noexcept override
    {
        return Level::off;
    }
    TestLogger()
        : Logger::LevelThreshold()
        , Logger(static_cast<Logger::LevelThreshold&>(*this))
    {
    }
};

using StartImmediately = realm::util::TaggedBool<class StartImmediatelyTag>;

class SyncServer : private realm::sync::Clock {
public:
    struct Config {
        bool start_immediately = true;
        std::string local_dir;
    };

    ~SyncServer();

    void start();
    void stop();

    std::string url_for_realm(realm::StringData realm_name) const;
    std::string base_url() const
    {
        return m_url;
    }
    std::string local_root_dir() const
    {
        return m_local_root_dir;
    }

    template <class R, class P>
    void advance_clock(std::chrono::duration<R, P> duration = std::chrono::seconds(1)) noexcept
    {
        m_now += std::chrono::duration_cast<time_point::duration>(duration).count();
    }

private:
    friend struct TestSyncManager;
    SyncServer(const Config& config);
    std::string m_local_root_dir;
    std::unique_ptr<realm::util::Logger> m_logger;
    realm::sync::Server m_server;
    std::thread m_thread;
    std::string m_url;
    std::atomic<time_point::rep> m_now{0};

    time_point now() const noexcept override
    {
        return time_point{time_point::duration{m_now}};
    }
};

struct SyncTestFile : TestFile {
    template <typename ErrorHandler>
    SyncTestFile(const realm::SyncConfig& sync_config, realm::SyncSessionStopPolicy stop_policy,
                 ErrorHandler&& error_handler)
    {
        this->sync_config = std::make_shared<realm::SyncConfig>(sync_config);
        this->sync_config->stop_policy = stop_policy;
        this->sync_config->error_handler = std::forward<ErrorHandler>(error_handler);
        schema_mode = realm::SchemaMode::AdditiveExplicit;
    }

    SyncTestFile(std::shared_ptr<realm::app::App> app = nullptr, std::string name = "",
                 std::string user_name = "test");
    SyncTestFile(std::shared_ptr<realm::SyncUser> user, realm::bson::Bson partition, realm::Schema schema);
    SyncTestFile(std::shared_ptr<realm::app::App> app, realm::bson::Bson partition, realm::Schema schema);
    SyncTestFile(std::shared_ptr<realm::SyncUser> user, realm::Schema schema, realm::SyncConfig::FLXSyncEnabled);
};

struct TestSyncManager {
    struct Config {
#if TEST_ENABLE_SYNC_LOGGING
        static constexpr bool default_logging = true;
#else
        static constexpr bool default_logging = false;
#endif
        Config() {}
        Config(std::string, realm::SyncManager::MetadataMode = realm::SyncManager::MetadataMode::NoEncryption);
        Config(std::string, std::string,
               realm::SyncManager::MetadataMode = realm::SyncManager::MetadataMode::NoEncryption);
        Config(const realm::app::App::Config&, realm::AppSession* = nullptr);
        realm::app::App::Config app_config;
        std::string base_path;
        std::string base_url;
        realm::SyncManager::MetadataMode metadata_mode = realm::SyncManager::MetadataMode::NoEncryption;
        bool should_teardown_test_directory = true;
        bool verbose_sync_client_logging = default_logging;
        realm::AppSession* app_session = nullptr;
        bool override_sync_route = true;
    };

    TestSyncManager(const Config& = Config(), const SyncServer::Config& = {});
    ~TestSyncManager();

    std::shared_ptr<realm::app::App> app() const;
    realm::AppSession* app_session() const
    {
        return m_app_session;
    }
    SyncServer& sync_server()
    {
        return m_sync_server;
    }

    // Capture the token refresh callback so that we can invoke it later with
    // the desired result
    std::function<void(realm::app::Response)> network_callback;
    struct Transport : realm::app::GenericNetworkTransport {
        Transport(std::function<void(realm::app::Response)>* network_callback)
            : network_callback(network_callback)
        {
        }

        void send_request_to_server(realm::app::Request, std::function<void(realm::app::Response)> completion_block)
        {
            *network_callback = completion_block;
        }

        std::function<void(realm::app::Response)>* network_callback;
    };
    std::shared_ptr<realm::app::GenericNetworkTransport> transport = std::make_shared<Transport>(&network_callback);

private:
    std::shared_ptr<realm::app::App> m_app;
    SyncServer m_sync_server;
    std::string m_base_file_path;
    bool m_should_teardown_test_directory = true;
    realm::AppSession* m_app_session = nullptr;
};

inline TestSyncManager::Config::Config(std::string bp, realm::SyncManager::MetadataMode mdm)
    : base_path(bp)
    , metadata_mode(mdm)
{
}

inline TestSyncManager::Config::Config(std::string app_id, std::string bp, realm::SyncManager::MetadataMode mdm)
    : base_path(bp)
    , metadata_mode(mdm)
{
    realm::app::App::Config app_cfg;
    app_cfg.app_id = app_id;
    app_config = app_cfg;
}

inline TestSyncManager::Config::Config(const realm::app::App::Config& app_cfg, realm::AppSession* session)
    : app_config(app_cfg)
    , metadata_mode(realm::SyncManager::MetadataMode::NoEncryption)
    , should_teardown_test_directory(true)
    , app_session(session)
{
}

std::error_code wait_for_upload(realm::Realm& realm);
std::error_code wait_for_download(realm::Realm& realm);

#endif // REALM_ENABLE_SYNC

#endif
