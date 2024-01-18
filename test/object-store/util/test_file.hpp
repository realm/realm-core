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
#include <realm/util/tagged_bool.hpp>

#include <realm/util/logger.hpp>
#include <realm/util/optional.hpp>

#include <external/json/json.hpp>
#include "test_utils.hpp"

#include <memory>
#include <thread>

#if REALM_ENABLE_SYNC
#include <realm/sync/config.hpp>
#include <realm/object-store/sync/sync_manager.hpp>

#include <realm/sync/client.hpp>
#include <realm/sync/noinst/server/server.hpp>

#endif // REALM_ENABLE_SYNC

#if REALM_ENABLE_AUTH_TESTS || REALM_ENABLE_SYNC
#include <realm/object-store/sync/app.hpp>
#endif

#ifndef TEST_TIMEOUT_EXTRA
#define TEST_TIMEOUT_EXTRA 0
#endif

namespace realm {

#if REALM_ENABLE_AUTH_TESTS || REALM_ENABLE_SYNC
namespace app {
struct GenericNetworkTransport;
} // namespace app
struct AppSession;
#endif // REALM_ENABLE_AUTH_TESTS || REALM_ENABLE_SYNC

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

    TestFile(const TestFile&) = delete;
    TestFile& operator=(const TestFile&) = delete;

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

struct InMemoryTestFile : realm::Realm::Config {
    InMemoryTestFile();
    realm::DBOptions options() const;
};

void advance_and_notify(realm::Realm& realm);
void on_change_but_no_notify(realm::Realm& realm);

#ifndef TEST_ENABLE_LOGGING
#define TEST_ENABLE_LOGGING 0 // change to 1 to enable trace-level logging
#endif

#ifndef TEST_LOGGING_LEVEL
#if TEST_ENABLE_LOGGING
#define TEST_LOGGING_LEVEL all
#else
#define TEST_LOGGING_LEVEL off
#endif // TEST_ENABLE_LOGGING
#endif // TEST_LOGGING_LEVEL

#if REALM_ENABLE_AUTH_TESTS || REALM_ENABLE_SYNC

static const std::string profile_0_name = "Ursus americanus Ursus boeckhi";
static const std::string profile_0_first_name = "Ursus americanus";
static const std::string profile_0_last_name = "Ursus boeckhi";
static const std::string profile_0_email = "Ursus ursinus";
static const std::string profile_0_picture_url = "Ursus malayanus";
static const std::string profile_0_gender = "Ursus thibetanus";
static const std::string profile_0_birthday = "Ursus americanus";
static const std::string profile_0_min_age = "Ursus maritimus";
static const std::string profile_0_max_age = "Ursus arctos";

static const nlohmann::json profile_0 = {
    {"name", profile_0_name},         {"first_name", profile_0_first_name},   {"last_name", profile_0_last_name},
    {"email", profile_0_email},       {"picture_url", profile_0_picture_url}, {"gender", profile_0_gender},
    {"birthday", profile_0_birthday}, {"min_age", profile_0_min_age},         {"max_age", profile_0_max_age}};

nlohmann::json user_json(std::string access_token, std::string user_id = realm::random_string(15));
nlohmann::json user_profile_json(std::string user_id = realm::random_string(15),
                                 std::string identity_0_id = "Ursus arctos isabellinus",
                                 std::string identity_1_id = "Ursus arctos horribilis",
                                 std::string provider_type = "anon-user");
void set_app_config_defaults(realm::app::App::Config& app_config,
                             const std::shared_ptr<realm::app::GenericNetworkTransport>& transport);

static const std::string good_access_token =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJleHAiOjE1ODE1MDc3OTYsImlhdCI6MTU4MTUwNTk5NiwiaXNzIjoiNWU0M2RkY2M2MzZlZTEwNmVhYTEyYmRjIiwic3RpdGNoX2RldklkIjoi"
    "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwIiwic3RpdGNoX2RvbWFpbklkIjoiNWUxNDk5MTNjOTBiNGFmMGViZTkzNTI3Iiwic3ViIjoiNWU0M2Rk"
    "Y2M2MzZlZTEwNmVhYTEyYmRhIiwidHlwIjoiYWNjZXNzIn0.0q3y9KpFxEnbmRwahvjWU1v9y1T1s3r2eozu93vMc3s";

static const std::string good_access_token2 =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJleHAiOjE1ODkzMDE3MjAsImlhdCI6MTU4NDExODcyMCwiaXNzIjoiNWU2YmJiYzBhNmI3ZGZkM2UyNTA0OGI3Iiwic3RpdGNoX2RldklkIjoi"
    "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwIiwic3RpdGNoX2RvbWFpbklkIjoiNWUxNDk5MTNjOTBiNGFmMGViZTkzNTI3Iiwic3ViIjoiNWU2YmJi"
    "YzBhNmI3ZGZkM2UyNTA0OGIzIiwidHlwIjoiYWNjZXNzIn0.eSX4QMjIOLbdOYOPzQrD_racwLUk1HGFgxtx2a34k80";

static const std::string bad_access_token = "lolwut";
static const std::string dummy_device_id = "123400000000000000000000";

class OfflineAppSession {
public:
    struct Config {
        Config(std::shared_ptr<realm::app::GenericNetworkTransport>);
        std::shared_ptr<realm::app::GenericNetworkTransport> transport;
        bool delete_storage = true;
        std::optional<std::string> storage_path;
        realm::app::RealmBackingStoreConfig::MetadataMode metadata_mode =
            realm::app::RealmBackingStoreConfig::MetadataMode::NoMetadata;
    };
    OfflineAppSession(Config);
    ~OfflineAppSession();

    std::shared_ptr<realm::app::App> app() const noexcept
    {
        return m_app;
    }
    realm::app::GenericNetworkTransport* transport()
    {
        return m_transport.get();
    }
    std::shared_ptr<realm::app::BackingStore> const& backing_store() const noexcept
    {
        return m_app->backing_store();
    }

private:
    std::shared_ptr<realm::app::App> m_app;
    std::string m_base_file_path;
    std::shared_ptr<realm::app::GenericNetworkTransport> m_transport;
    bool m_delete_storage = true;
};

#endif // REALM_ENABLE_AUTH_TESTS || REALM_ENABLE_SYNC

#if REALM_ENABLE_AUTH_TESTS
using DeleteApp = realm::util::TaggedBool<struct DeleteAppTag>;
class TestAppSession {
public:
    struct Config {
        Config();
        Config(realm::AppSession, std::shared_ptr<realm::app::GenericNetworkTransport> = nullptr, DeleteApp = true,
#if REALM_ENABLE_SYNC
               realm::ReconnectMode mode = realm::ReconnectMode::normal,
               std::shared_ptr<realm::sync::SyncSocketProvider> socket_provider = nullptr,
#endif // REALM_ENABLE_SYNC
               std::optional<realm::app::App::StoreFactory> store = std::nullopt);
        ~Config();
        std::unique_ptr<realm::AppSession> app_session;
        std::shared_ptr<realm::app::GenericNetworkTransport> transport;
        DeleteApp delete_when_done;
        bool delete_storage = true;
        std::optional<std::string> storage_path;
#if REALM_ENABLE_SYNC
        realm::ReconnectMode reconnect_mode = realm::ReconnectMode::normal;
        std::shared_ptr<realm::sync::SyncSocketProvider> custom_socket_provider = nullptr;
#endif // REALM_SYNC
        std::optional<realm::app::App::StoreFactory> store_factory;
    };
    TestAppSession(Config config = {});
    ~TestAppSession();

    std::shared_ptr<realm::app::App> app() const noexcept
    {
        return m_app;
    }
    const realm::AppSession& app_session() const noexcept
    {
        return *m_app_session;
    }
    realm::app::GenericNetworkTransport* transport()
    {
        return m_transport.get();
    }
    std::shared_ptr<realm::SyncManager> sync_manager() const noexcept
    {
        return m_app->sync_manager();
    }
    std::shared_ptr<realm::app::BackingStore> const& backing_store() const noexcept
    {
        return m_app->backing_store();
    }

    std::vector<realm::bson::BsonDocument> get_documents(realm::SyncUser& user, const std::string& object_type,
                                                         size_t expected_count) const;

private:
    std::shared_ptr<realm::app::App> m_app;
    std::unique_ptr<realm::AppSession> m_app_session;
    std::string m_base_file_path;
    bool m_delete_app = true;
    std::shared_ptr<realm::app::GenericNetworkTransport> m_transport;
    bool m_delete_storage = true;
};

#endif // REALM_ENABLE_AUTH_TESTS

#if REALM_ENABLE_SYNC

using StartImmediately = realm::util::TaggedBool<class StartImmediatelyTag>;
using EnableSSL = realm::util::TaggedBool<class EnableSSLTag>;

class SyncServer : private realm::sync::Clock {
public:
    struct Config {
        StartImmediately start_immediately = true;
        EnableSSL ssl = false;
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
    friend class TestSyncManager;
    SyncServer(const Config& config);
    std::string m_local_root_dir;
    std::shared_ptr<realm::util::Logger> m_logger;
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
    SyncTestFile(std::shared_ptr<realm::SyncUser> user, realm::bson::Bson partition,
                 realm::util::Optional<realm::Schema> schema = realm::util::none);
    SyncTestFile(std::shared_ptr<realm::SyncUser> user, realm::bson::Bson partition,
                 realm::util::Optional<realm::Schema> schema,
                 std::function<realm::SyncSessionErrorHandler>&& error_handler);
    SyncTestFile(std::shared_ptr<realm::app::App> app, realm::bson::Bson partition, realm::Schema schema);
    SyncTestFile(std::shared_ptr<realm::SyncUser> user, realm::Schema schema, realm::SyncConfig::FLXSyncEnabled);
};

class TestSyncManager {
public:
    struct Config {
        Config() {}
        realm::app::App::Config app_config;
        std::string base_path;
        realm::SyncManager::MetadataMode metadata_mode = realm::SyncManager::MetadataMode::NoMetadata;
        bool should_teardown_test_directory = true;
        realm::util::Logger::Level log_level = realm::util::Logger::Level::TEST_LOGGING_LEVEL;
        bool override_sync_route = true;
        std::shared_ptr<realm::app::GenericNetworkTransport> transport;
        bool start_sync_client = true;
    };

    TestSyncManager(realm::SyncManager::MetadataMode mode);
    TestSyncManager(const Config& = Config(), const SyncServer::Config& = {});
    ~TestSyncManager();

    std::shared_ptr<realm::app::App> app() const noexcept
    {
        return m_app;
    }
    std::shared_ptr<realm::SyncManager> sync_manager() const
    {
        return m_app->sync_manager();
    }
    std::string base_file_path() const
    {
        return m_base_file_path;
    }
    SyncServer& sync_server()
    {
        return m_sync_server;
    }

    std::shared_ptr<realm::SyncUser> fake_user(const std::string& name = "test");

    // Capture the token refresh callback so that we can invoke it later with
    // the desired result
    realm::util::UniqueFunction<void(const realm::app::Response&)> network_callback;

    struct Transport : realm::app::GenericNetworkTransport {
        Transport(realm::util::UniqueFunction<void(const realm::app::Response&)>& network_callback)
            : network_callback(network_callback)
        {
        }

        void
        send_request_to_server(const realm::app::Request&,
                               realm::util::UniqueFunction<void(const realm::app::Response&)>&& completion) override
        {
            network_callback = std::move(completion);
        }

        realm::util::UniqueFunction<void(const realm::app::Response&)>& network_callback;
    };
    const std::shared_ptr<realm::app::GenericNetworkTransport> transport;

private:
    std::shared_ptr<realm::app::App> m_app;
    SyncServer m_sync_server;
    std::string m_base_file_path;
    bool m_should_teardown_test_directory = true;
};

inline TestSyncManager::TestSyncManager(realm::SyncManager::MetadataMode mode)
    : TestSyncManager([=] {
        Config config;
        config.metadata_mode = mode;
        return config;
    }())
{
}

bool wait_for_upload(realm::Realm& realm, std::chrono::seconds timeout = std::chrono::seconds(60));
bool wait_for_download(realm::Realm& realm, std::chrono::seconds timeout = std::chrono::seconds(60));

#endif // REALM_ENABLE_SYNC

#endif // REALM_TEST_UTIL_TEST_FILE_HPP
