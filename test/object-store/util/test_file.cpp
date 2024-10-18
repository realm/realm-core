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

#include "util/test_file.hpp"

#include "util/test_utils.hpp"
#include "util/sync/baas_admin_api.hpp"
#include "util/sync/sync_test_utils.hpp"
#include "../util/crypt_key.hpp"
#include "../util/test_path.hpp"
#include "util/sync/sync_test_utils.hpp"

#include <realm/db.hpp>
#include <realm/disable_sync_to_disk.hpp>
#include <realm/history.hpp>
#include <realm/string_data.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/file.hpp>

#if REALM_ENABLE_SYNC
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_database.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include <realm/object-store/schema.hpp>
#endif

#include <cstdlib>
#include <iostream>

#ifdef _WIN32
#include <io.h>
#include <fcntl.h>

inline static int mkstemp(char* _template)
{
    return _open(_mktemp(_template), _O_CREAT | _O_TEMPORARY, _S_IREAD | _S_IWRITE);
}
#else
#include <unistd.h>
#endif

#if REALM_HAVE_CLANG_FEATURE(thread_sanitizer)
#include <condition_variable>
#include <functional>
#include <thread>
#include <map>
#endif

using namespace realm;

TestFile::TestFile()
{
    disable_sync_to_disk();
    m_temp_dir = util::make_temp_dir();
    path = (fs::path(m_temp_dir) / "realm.XXXXXX").string();
    if (const char* crypt_key = test_util::crypt_key()) {
        encryption_key = std::vector<char>(crypt_key, crypt_key + 64);
    }
    int fd = mkstemp(path.data());
    if (fd < 0) {
        int err = errno;
        throw std::system_error(err, std::system_category());
    }
#ifdef _WIN32
    _close(fd);
    _unlink(path.c_str());
#else // POSIX
    close(fd);
    unlink(path.c_str());
#endif

    schema_version = 0;
}

TestFile::~TestFile()
{
    if (!m_persist) {
        try {
            util::Logger::get_default_logger()->debug("~TestFile() removing '%1' and '%2'", path, m_temp_dir);
            util::File::try_remove(path);
            util::try_remove_dir_recursive(m_temp_dir);
        }
        catch (const std::exception& e) {
            util::Logger::get_default_logger()->warn("~TestFile() cleanup failed for '%1': %2", path, e.what());
            // clean up is best effort, ignored.
        }
    }
}

DBOptions TestFile::options() const
{
    DBOptions options;
    options.durability = in_memory ? DBOptions::Durability::MemOnly : DBOptions::Durability::Full;
    return options;
}

InMemoryTestFile::InMemoryTestFile()
{
    in_memory = true;
    schema_version = 0;
    encryption_key = std::vector<char>();
}

DBOptions InMemoryTestFile::options() const
{
    DBOptions options;
    options.durability = DBOptions::Durability::MemOnly;
    return options;
}

#if REALM_ENABLE_SYNC

static const std::string fake_refresh_token = ENCODE_FAKE_JWT("not_a_real_token");
static const std::string fake_access_token = ENCODE_FAKE_JWT("also_not_real");
static const std::string fake_device_id = "123400000000000000000000";

SyncTestFile::SyncTestFile(TestSyncManager& tsm, std::string name, std::string user_name)
    : SyncTestFile(tsm.fake_user(user_name), bson::Bson(name))
{
}

#if REALM_APP_SERVICES
SyncTestFile::SyncTestFile(OfflineAppSession& oas, std::string name)
    : SyncTestFile(oas.make_user(), bson::Bson(name))
{
}
#endif // REALM_APP_SERVICES

SyncTestFile::SyncTestFile(std::shared_ptr<SyncUser> user, bson::Bson partition, util::Optional<Schema> schema)
{
    REALM_ASSERT(user);
    sync_config = std::make_shared<realm::SyncConfig>(user, partition);
    sync_config->stop_policy = SyncSessionStopPolicy::Immediately;
    sync_config->error_handler = [](std::shared_ptr<SyncSession>, SyncError error) {
        util::format(std::cerr, "An unexpected sync error was caught by the default SyncTestFile handler: '%1'\n",
                     error.status);
        abort();
    };
    schema_version = 1;
    this->schema = std::move(schema);
    schema_mode = SchemaMode::AdditiveExplicit;
}

SyncTestFile::SyncTestFile(std::shared_ptr<SyncUser> user, bson::Bson partition,
                           realm::util::Optional<realm::Schema> schema,
                           std::function<SyncSessionErrorHandler>&& error_handler)
{
    REALM_ASSERT(user);
    sync_config = std::make_shared<realm::SyncConfig>(user, partition);
    sync_config->stop_policy = SyncSessionStopPolicy::Immediately;
    sync_config->error_handler = std::move(error_handler);
    schema_version = 1;
    this->schema = std::move(schema);
    schema_mode = SchemaMode::AdditiveExplicit;
}

SyncTestFile::SyncTestFile(std::shared_ptr<realm::SyncUser> user, realm::Schema _schema, SyncConfig::FLXSyncEnabled)
{
    REALM_ASSERT(user);
    sync_config = std::make_shared<realm::SyncConfig>(user, SyncConfig::FLXSyncEnabled{});
    sync_config->stop_policy = SyncSessionStopPolicy::Immediately;
    sync_config->error_handler = [](std::shared_ptr<SyncSession> session, SyncError error) {
        util::format(std::cerr,
                     "An unexpected sync error was caught by the default SyncTestFile handler: '%1' for '%2'",
                     error.status, session->path());
        abort();
    };
    schema_version = 0;
    schema = _schema;
    schema_mode = SchemaMode::AdditiveExplicit;
}

SyncTestFile::SyncTestFile(TestSyncManager& tsm, bson::Bson partition, Schema schema)
    : SyncTestFile(tsm.fake_user("test"), std::move(partition), std::move(schema))
{
}

// MARK: - SyncServer
SyncServer::SyncServer(const SyncServer::Config& config)
    : m_local_root_dir(config.local_dir.empty() ? util::make_temp_dir() : config.local_dir)
    , m_server(m_local_root_dir, util::none, ([&] {
                   using namespace std::literals::chrono_literals;

                   m_logger = util::Logger::get_default_logger();

                   sync::Server::Config c;
                   c.logger = m_logger;
                   c.token_expiration_clock = this;
                   c.listen_address = "127.0.0.1";
                   c.disable_sync_to_disk = true;
                   c.ssl = config.ssl;
                   if (c.ssl) {
                       c.ssl_certificate_path = test_util::get_test_resource_path() + "test_util_network_ssl_ca.pem";
                       c.ssl_certificate_key_path =
                           test_util::get_test_resource_path() + "test_util_network_ssl_key.pem";
                   }

                   return c;
               })())
{
    m_server.start();
    m_url = util::format("%1://127.0.0.1:%2", config.ssl ? "wss" : "ws", m_server.listen_endpoint().port());
    if (config.start_immediately)
        start();
}

SyncServer::~SyncServer()
{
    stop();
}

void SyncServer::start()
{
    REALM_ASSERT(!m_thread.joinable());
    m_thread = std::thread([this] {
        m_server.run();
    });
}

void SyncServer::stop()
{
    m_server.stop();
    if (m_thread.joinable())
        m_thread.join();
}

std::string SyncServer::url_for_realm(StringData realm_name) const
{
    return util::format("%1/%2", m_url, realm_name);
}

int SyncServer::port() const
{
    return static_cast<int>(m_server.listen_endpoint().port());
}

struct WaitForSessionState {
    std::condition_variable cv;
    std::mutex mutex;
    bool complete = false;
    Status status = Status::OK();
};

static Status wait_for_session(Realm& realm, void (SyncSession::*fn)(util::UniqueFunction<void(Status)>&&),
                               std::chrono::seconds timeout)
{
    auto shared_state = std::make_shared<WaitForSessionState>();
    auto& session = *realm.sync_session();
    auto delay = TEST_TIMEOUT_EXTRA > 0 ? timeout + std::chrono::seconds(TEST_TIMEOUT_EXTRA) : timeout;
    (session.*fn)([weak_state = std::weak_ptr<WaitForSessionState>(shared_state)](Status s) {
        auto shared_state = weak_state.lock();
        if (!shared_state) {
            return;
        }
        std::lock_guard<std::mutex> lock(shared_state->mutex);
        shared_state->complete = true;
        shared_state->status = s;
        shared_state->cv.notify_one();
    });
    std::unique_lock<std::mutex> lock(shared_state->mutex);
    bool completed = shared_state->cv.wait_for(lock, delay, [&]() {
        return shared_state->complete == true;
    });
    if (!completed) {
        throw std::runtime_error(util::format("wait_for_session() exceeded %1 s", delay.count()));
    }
    return shared_state->status;
}

bool wait_for_upload(Realm& realm, std::chrono::seconds timeout)
{
    return !wait_for_session(realm, &SyncSession::wait_for_upload_completion, timeout).is_ok();
}

bool wait_for_download(Realm& realm, std::chrono::seconds timeout)
{
    return !wait_for_session(realm, &SyncSession::wait_for_download_completion, timeout).is_ok();
}

#if REALM_APP_SERVICES
void set_app_config_defaults(app::AppConfig& app_config,
                             const std::shared_ptr<app::GenericNetworkTransport>& transport)
{
    if (!app_config.transport)
        app_config.transport = transport;
    if (app_config.device_info.platform_version.empty())
        app_config.device_info.platform_version = "Object Store Test Platform Version";
    if (app_config.device_info.sdk_version.empty())
        app_config.device_info.sdk_version = "SDK Version";
    if (app_config.device_info.sdk.empty())
        app_config.device_info.sdk = "SDK Name";
    if (app_config.device_info.device_name.empty())
        app_config.device_info.device_name = "Device Name";
    if (app_config.device_info.device_version.empty())
        app_config.device_info.device_version = "Device Version";
    if (app_config.device_info.framework_name.empty())
        app_config.device_info.framework_name = "Framework Name";
    if (app_config.device_info.framework_version.empty())
        app_config.device_info.framework_version = "Framework Version";
    if (app_config.device_info.bundle_id.empty())
        app_config.device_info.bundle_id = "Bundle Id";
    if (app_config.app_id.empty())
        app_config.app_id = "app_id";
    app_config.metadata_mode = app::AppConfig::MetadataMode::InMemory;
}
#endif // REALM_APP_SERVICES

// MARK: - TestAppSession

#if REALM_ENABLE_AUTH_TESTS

TestAppSession::TestAppSession()
    // Don't delete the global runtime app session
    : TestAppSession(get_runtime_app_session(), {}, DeleteApp{false})
{
}

TestAppSession::TestAppSession(AppSession session)
    : TestAppSession(session, {}, DeleteApp{true})
{
}

TestAppSession::TestAppSession(AppSession session, Config config, DeleteApp delete_app, bool delete_storage)
    : m_app_session(std::make_unique<AppSession>(session))
    , m_config(config)
    , m_delete_app(delete_app)
    , m_delete_storage(delete_storage)
{
    if (!m_config.storage_path || m_config.storage_path->empty()) {
        m_config.storage_path.emplace(util::make_temp_dir() + random_string(10));
    }
    REALM_ASSERT(m_config.storage_path);
    util::try_make_dir(*m_config.storage_path);

    if (!m_config.transport) {
        m_config.transport = instance_of<SynchronousTestTransport>;
    }
    realm::app::AppConfig app_config = get_config(m_config.transport, *m_app_session);
    set_app_config_defaults(app_config, m_config.transport);
    // If a base URL was provided, set it in the app config
    if (m_config.base_url) {
        app_config.base_url = *m_config.base_url;
    }
    app_config.base_file_path = *m_config.storage_path;
    app_config.metadata_mode = m_config.metadata_mode;
    app_config.sync_client_config.reconnect_mode = m_config.reconnect_mode;
    // With multiplexing enabled, the linger time controls how long a
    // connection is kept open for reuse. In tests, we want to shut
    // down sync clients immediately.
    app_config.sync_client_config.timeouts.connection_linger_time = 0;
    app_config.sync_client_config.socket_provider = m_config.socket_provider;
    if (m_config.logger) {
        app_config.sync_client_config.logger_factory = [logger = m_config.logger](util::Logger::Level) {
            return logger;
        };
    }

    m_app = app::App::get_app(app::App::CacheMode::Disabled, app_config);

    // initialize sync client and save a local copy of the logger
    m_app->sync_manager()->get_sync_client();
    m_logger = m_app->sync_manager()->get_logger();
    // If no user creds are supplied, then create the user and log in
    if (!m_config.user_creds) {
        auto result = create_user_and_log_in();
        REALM_ASSERT(result.is_ok());
        m_config.user_creds = result.get_value();
    }
    // If creds are supplied, it is up to the caller to log in separately
}

TestAppSession::~TestAppSession()
{
    if (m_app) {
        m_app->sync_manager()->tear_down_for_testing();
        m_app.reset();
    }
    app::App::clear_cached_apps();
    // If the app session is being deleted or the config tells us to, delete the storage path
    if ((m_delete_app || m_delete_storage) && util::File::exists(*m_config.storage_path)) {
        try {
            util::try_remove_dir_recursive(*m_config.storage_path);
        }
        catch (const std::exception& ex) {
            std::cerr << "Error tearing down TestAppSession(" << m_app_session->config.app_name << "): " << ex.what()
                      << "\n";
        }
    }
    if (m_delete_app && m_app_session) {
        m_app_session->admin_api.delete_app(m_app_session->server_app_id);
    }
}

StatusWith<realm::app::AppCredentials> TestAppSession::create_user_and_log_in()
{
    REALM_ASSERT(m_app);
    AutoVerifiedEmailCredentials creds;
    auto pf = util::make_promise_future<void>();
    m_app->provider_client<app::App::UsernamePasswordProviderClient>().register_email(
        creds.email, creds.password,
        [this, &creds, promise = util::CopyablePromiseHolder<void>(std::move(pf.promise))](
            util::Optional<app::AppError> error) mutable {
            if (error) {
                if (m_logger)
                    m_logger->error("Failed to create user: %1", error->to_status());
                promise.get_promise().set_error(error->to_status());
                return;
            }
            auto result = log_in_user(creds);
            if (!result.is_ok()) {
                if (m_logger)
                    m_logger->error("User login failed: %1", result.get_status());
                promise.get_promise().set_error(result.get_status());
                return;
            }
            promise.get_promise().emplace_value();
        });
    auto result = pf.future.get_no_throw();
    if (!result.is_ok()) {
        return result;
    }
    return creds;
}

StatusWith<std::shared_ptr<realm::SyncUser>>
TestAppSession::log_in_user(std::optional<realm::app::AppCredentials> user_creds)
{
    REALM_ASSERT(m_app);
    REALM_ASSERT((user_creds || m_config.user_creds));
    auto pf = util::make_promise_future<std::shared_ptr<realm::SyncUser>>();
    m_app->log_in_with_credentials(
        *user_creds, [promise = util::CopyablePromiseHolder<std::shared_ptr<realm::SyncUser>>(std::move(pf.promise))](
                         std::shared_ptr<realm::SyncUser> user, util::Optional<app::AppError> error) mutable {
            if (error) {
                promise.get_promise().set_error(error->to_status());
                return;
            }
            promise.get_promise().emplace_value(user);
        });
    return pf.future.get_no_throw();
}

std::vector<bson::BsonDocument> TestAppSession::get_documents(app::User& user, const std::string& object_type,
                                                              size_t expected_count) const
{
    app::MongoClient remote_client = user.mongo_client("BackingDB");
    app::MongoDatabase db = remote_client.db(m_app_session->config.mongo_dbname);
    app::MongoCollection collection = db[object_type];
    int sleep_time = 10;
    timed_wait_for(
        [&] {
            uint64_t count = 0;
            collection.count({}, [&](uint64_t c, util::Optional<app::AppError> error) {
                REQUIRE(!error);
                count = c;
            });
            if (count < expected_count) {
                // querying the server too frequently makes it take longer to process the sync changesets we're
                // waiting for
                millisleep(sleep_time);
                if (sleep_time < 500) {
                    sleep_time *= 2;
                }
                return false;
            }
            return true;
        },
        std::chrono::minutes(5));

    std::vector<bson::BsonDocument> documents;
    collection.find({}, {}, [&](util::Optional<bson::BsonArray>&& result, util::Optional<app::AppError> error) {
        REQUIRE(result);
        REQUIRE(!error);
        REQUIRE(result->size() == expected_count);
        documents.reserve(result->size());
        for (auto&& bson : *result) {
            REQUIRE(bson.type() == bson::Bson::Type::Document);
            documents.push_back(std::move(static_cast<const bson::BsonDocument&>(bson)));
        }
    });
    return documents;
}
#endif // REALM_ENABLE_AUTH_TESTS

// MARK: - TestSyncManager

TestSyncManager::Config::Config() {}

TestSyncManager::TestSyncManager(const Config& config, const SyncServer::Config& sync_server_config)
    : m_sync_manager(SyncManager::create(SyncClientConfig()))
    , m_sync_server(sync_server_config)
    , m_base_file_path(config.base_path.empty() ? util::make_temp_dir() : config.base_path)
    , m_should_teardown_test_directory(config.should_teardown_test_directory)
{
    util::try_make_dir(m_base_file_path);

    m_sync_manager->set_sync_route(m_sync_server.base_url() + "/realm-sync", true);
    if (config.start_sync_client) {
        m_sync_manager->get_sync_client();
    }
}

TestSyncManager::~TestSyncManager()
{
    if (m_should_teardown_test_directory) {
        if (!m_base_file_path.empty() && util::File::exists(m_base_file_path)) {
            try {
                m_sync_manager->tear_down_for_testing();
                util::try_remove_dir_recursive(m_base_file_path);
            }
            catch (const std::exception& ex) {
                std::cerr << ex.what() << "\n";
            }
#if REALM_APP_SERVICES
            app::App::clear_cached_apps();
#endif // REALM_APP_SERVICES
        }
    }
}

std::shared_ptr<TestUser> TestSyncManager::fake_user(const std::string& name)
{
    auto user = std::make_shared<TestUser>(name, m_sync_manager);
    user->m_access_token = fake_access_token;
    user->m_refresh_token = fake_refresh_token;
    return user;
}

#if REALM_APP_SERVICES
OfflineAppSession::Config::Config(std::shared_ptr<realm::app::GenericNetworkTransport> t)
    : transport(t)
{
}

OfflineAppSession::OfflineAppSession(OfflineAppSession::Config config)
    : m_transport(std::move(config.transport))
    , m_delete_storage(config.delete_storage)
{
    REALM_ASSERT(m_transport);
    set_app_config_defaults(m_app_config, m_transport);

    if (config.storage_path) {
        m_base_file_path = *config.storage_path;
        util::try_make_dir(m_base_file_path);
    }
    else {
        m_base_file_path = util::make_temp_dir();
    }

    m_app_config.base_file_path = m_base_file_path;
    m_app_config.metadata_mode = config.metadata_mode;
    if (config.base_url) {
        m_app_config.base_url = *config.base_url;
    }
    if (config.app_id) {
        m_app_config.app_id = *config.app_id;
    }
    m_app_config.sync_client_config.socket_provider = config.socket_provider;
    m_app = app::App::get_app(app::App::CacheMode::Disabled, m_app_config);
}

OfflineAppSession::~OfflineAppSession()
{
    if (util::File::exists(m_base_file_path) && m_delete_storage) {
        try {
            m_app->sync_manager()->tear_down_for_testing();
            util::try_remove_dir_recursive(m_base_file_path);
        }
        catch (const std::exception& ex) {
            std::cerr << ex.what() << "\n";
        }
        app::App::clear_cached_apps();
    }
}

std::shared_ptr<realm::app::User> OfflineAppSession::make_user() const
{
    create_user_and_log_in(app());
    return app()->current_user();
}

#endif // REALM_APP_SERVICES
#endif // REALM_ENABLE_SYNC

#if REALM_HAVE_CLANG_FEATURE(thread_sanitizer)
// MARK: - TsanNotifyWorker
// A helper which synchronously runs on_change() on a fixed background thread
// so that ThreadSanitizer can potentially detect issues
// This deliberately uses an unsafe spinlock for synchronization to ensure that
// the code being tested has to supply all required safety
static class TsanNotifyWorker {
public:
    TsanNotifyWorker()
    {
        m_thread = std::thread([&] {
            work();
        });
    }

    void work()
    {
        while (true) {
            auto value = m_signal.load(std::memory_order_relaxed);
            if (value == 0 || value == 1)
                continue;
            if (value == 2)
                return;

            if (value & 1) {
                // Synchronize on the first handover of a given coordinator.
                value &= ~1;
                m_signal.load();
            }

            auto c = reinterpret_cast<_impl::RealmCoordinator*>(value);
            c->on_change();
            m_signal.store(1, std::memory_order_relaxed);
        }
    }

    ~TsanNotifyWorker()
    {
        m_signal = 2;
        m_thread.join();
    }

    void on_change(const std::shared_ptr<_impl::RealmCoordinator>& c)
    {
        auto& it = m_published_coordinators[c.get()];
        if (it.lock()) {
            m_signal.store(reinterpret_cast<uintptr_t>(c.get()), std::memory_order_relaxed);
        }
        else {
            // Synchronize on the first handover of a given coordinator.
            it = c;
            m_signal = reinterpret_cast<uintptr_t>(c.get()) | 1;
        }

        while (m_signal.load(std::memory_order_relaxed) != 1)
            ;
    }

private:
    std::atomic<uintptr_t> m_signal{0};
    std::thread m_thread;
    std::map<_impl::RealmCoordinator*, std::weak_ptr<_impl::RealmCoordinator>> m_published_coordinators;
} s_worker;

void on_change_but_no_notify(Realm& realm)
{
    s_worker.on_change(_impl::RealmCoordinator::get_existing_coordinator(realm.config().path));
}

void advance_and_notify(Realm& realm)
{
    on_change_but_no_notify(realm);
    realm.notify();
}

#else // REALM_HAVE_CLANG_FEATURE(thread_sanitizer)

void on_change_but_no_notify(Realm& realm)
{
    _impl::RealmCoordinator::get_coordinator(realm.config().path)->on_change();
}

void advance_and_notify(Realm& realm)
{
    on_change_but_no_notify(realm);
    realm.notify();
}
#endif
