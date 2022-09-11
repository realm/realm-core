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

#include "sync_test_utils.hpp"

#include <realm/object-store/object_store.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/sync/mongo_database.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/hex_dump.hpp>
#include <realm/util/sha_crypto.hpp>

#include <iostream>

namespace realm {

std::ostream& operator<<(std::ostream& os, util::Optional<app::AppError> error)
{
    if (!error) {
        os << "(none)";
    }
    else {
        os << "AppError(error_code=" << error->error_code
           << ", http_status_code=" << error->http_status_code.value_or(0) << ", message=\"" << error->message
           << "\", link_to_server_logs=\"" << error->link_to_server_logs << "\")";
    }
    return os;
}

bool results_contains_user(SyncUserMetadataResults& results, const std::string& identity,
                           const std::string& provider_type)
{
    for (size_t i = 0; i < results.size(); i++) {
        auto this_result = results.get(i);
        if (this_result.identity() == identity && this_result.provider_type() == provider_type) {
            return true;
        }
    }
    return false;
}

bool results_contains_original_name(SyncFileActionMetadataResults& results, const std::string& original_name)
{
    for (size_t i = 0; i < results.size(); i++) {
        if (results.get(i).original_name() == original_name) {
            return true;
        }
    }
    return false;
}

void timed_wait_for(util::FunctionRef<bool()> condition, std::chrono::milliseconds max_ms)
{
    const auto wait_start = std::chrono::steady_clock::now();
    util::EventLoop::main().run_until([&] {
        if (std::chrono::steady_clock::now() - wait_start > max_ms) {
            throw std::runtime_error(util::format("timed_wait_for exceeded %1 ms", max_ms.count()));
        }
        return condition();
    });
}

auto do_hash = [](const std::string& name) -> std::string {
    std::array<unsigned char, 32> hash;
    util::sha256(name.data(), name.size(), hash.data());
    return util::hex_dump(hash.data(), hash.size(), "");
};

ExpectedRealmPaths::ExpectedRealmPaths(const std::string& base_path, const std::string& app_id,
                                       const std::string& identity, const std::string& local_identity,
                                       const std::string& partition)
{
    // This is copied from SyncManager.cpp string_from_partition() in order to prevent
    // us changing that function and therefore breaking user's existing paths unknowingly.
    std::string cleaned_partition;
    bson::Bson partition_value = bson::parse(partition);
    switch (partition_value.type()) {
        case bson::Bson::Type::Int32:
            cleaned_partition = util::format("i_%1", static_cast<int32_t>(partition_value));
            break;
        case bson::Bson::Type::Int64:
            cleaned_partition = util::format("l_%1", static_cast<int64_t>(partition_value));
            break;
        case bson::Bson::Type::String:
            cleaned_partition = util::format("s_%1", static_cast<std::string>(partition_value));
            break;
        case bson::Bson::Type::ObjectId:
            cleaned_partition = util::format("o_%1", static_cast<ObjectId>(partition_value).to_string());
            break;
        case bson::Bson::Type::Uuid:
            cleaned_partition = util::format("u_%1", static_cast<UUID>(partition_value).to_string());
            break;
        case bson::Bson::Type::Null:
            cleaned_partition = "null";
            break;
        default:
            REALM_ASSERT(false);
    }

    std::string clean_name = cleaned_partition;
    std::string cleaned_app_id = util::make_percent_encoded_string(app_id);
    const auto manager_path = fs::path{base_path}.make_preferred() / "mongodb-realm" / cleaned_app_id;
    const auto preferred_name = manager_path / identity / clean_name;
    current_preferred_path = preferred_name.string() + ".realm";
    fallback_hashed_path = (manager_path / do_hash(preferred_name.string())).string() + ".realm";
    legacy_sync_directories_to_make.push_back((manager_path / local_identity).string());
    std::string encoded_partition = util::make_percent_encoded_string(partition);
    legacy_local_id_path = (manager_path / local_identity / encoded_partition).concat(".realm").string();
    auto dir_builder = manager_path / "realm-object-server";
    legacy_sync_directories_to_make.push_back(dir_builder.string());
    dir_builder /= local_identity;
    legacy_sync_directories_to_make.push_back(dir_builder.string());
    legacy_sync_path = (dir_builder / cleaned_partition).string();
}

class TestHelper {
public:
    static DBRef& get_db(SharedRealm const& shared_realm)
    {
        return Realm::Internal::get_db(*shared_realm);
    }
};

namespace reset_utils {

namespace {

TableRef get_table(Realm& realm, StringData object_type)
{
    return realm::ObjectStore::table_for_object_type(realm.read_group(), object_type);
}

// Run through the client reset steps manually without involving a sync server.
// Useful for speed and when integration testing is not available on a platform.
struct FakeLocalClientReset : public TestClientReset {
    FakeLocalClientReset(const Realm::Config& local_config, const Realm::Config& remote_config)
        : TestClientReset(local_config, remote_config)
    {
        REALM_ASSERT(m_local_config.sync_config);
        m_mode = m_local_config.sync_config->client_resync_mode;
        REALM_ASSERT(m_mode == ClientResyncMode::DiscardLocal || m_mode == ClientResyncMode::Recover);
        // Turn off real sync. But we still need a SyncClientHistory for recovery mode so fake it.
        m_local_config.sync_config = {};
        m_remote_config.sync_config = {};
        m_local_config.force_sync_history = true;
        m_remote_config.force_sync_history = true;
        m_local_config.in_memory = true;
        m_local_config.encryption_key = std::vector<char>();
        m_remote_config.in_memory = true;
        m_remote_config.encryption_key = std::vector<char>();
    }

    void run() override
    {
        m_did_run = true;
        auto local_realm = Realm::get_shared_realm(m_local_config);
        if (m_on_setup) {
            local_realm->begin_transaction();
            m_on_setup(local_realm);
            local_realm->commit_transaction();

            // Update the sync history to mark this initial setup state as if it
            // has been uploaded so that it doesn't replay during recovery.
            auto history_local =
                dynamic_cast<sync::ClientHistory*>(local_realm->read_group().get_replication()->_get_history_write());
            REALM_ASSERT(history_local);
            sync::version_type current_version;
            sync::SaltedFileIdent file_ident;
            sync::SyncProgress progress;
            history_local->get_status(current_version, file_ident, progress);
            progress.upload.client_version = current_version;
            progress.upload.last_integrated_server_version = current_version;
            sync::VersionInfo info_out;
            history_local->set_sync_progress(progress, nullptr, info_out);
        }
        {
            local_realm->begin_transaction();
            auto obj = create_object(*local_realm, "object", m_pk_driving_reset);
            auto col = obj.get_table()->get_column_key("value");
            obj.set(col, 1);
            obj.set(col, 2);
            obj.set(col, 3);
            local_realm->commit_transaction();

            local_realm->begin_transaction();
            obj.set(col, 4);
            if (m_make_local_changes) {
                m_make_local_changes(local_realm);
            }
            local_realm->commit_transaction();
            if (m_on_post_local) {
                m_on_post_local(local_realm);
            }
        }

        {
            auto remote_realm = Realm::get_shared_realm(m_remote_config);
            remote_realm->begin_transaction();
            if (m_on_setup) {
                m_on_setup(remote_realm);
            }

            // fake a sync by creating an object with the same pk
            create_object(*remote_realm, "object", m_pk_driving_reset);

            for (int i = 0; i < 2; ++i) {
                auto table = get_table(*remote_realm, "object");
                auto col = table->get_column_key("value");
                table->begin()->set(col, i + 5);
            }

            if (m_make_remote_changes) {
                m_make_remote_changes(remote_realm);
            }
            remote_realm->commit_transaction();

            TestLogger logger;
            sync::SaltedFileIdent fake_ident{1, 123456789};
            auto local_db = TestHelper::get_db(local_realm);
            auto remote_db = TestHelper::get_db(remote_realm);
            using _impl::client_reset::perform_client_reset_diff;
            constexpr bool recovery_is_allowed = true;
            perform_client_reset_diff(local_db, remote_db, fake_ident, logger, m_mode, recovery_is_allowed, nullptr,
                                      nullptr, nullptr);

            remote_realm->close();
            if (m_on_post_reset) {
                m_on_post_reset(local_realm);
            }
        }
    }

private:
    ClientResyncMode m_mode;
};
} // anonymous namespace

#if REALM_ENABLE_SYNC



#endif // REALM_ENABLE_SYNC


std::unique_ptr<TestClientReset> make_fake_local_client_reset(const Realm::Config& local_config,
                                                              const Realm::Config& remote_config)
{
    return std::make_unique<FakeLocalClientReset>(local_config, remote_config);
}

} // namespace reset_utils

} // namespace realm

using namespace realm;

#if REALM_ENABLE_SYNC

static const std::string fake_refresh_token = ENCODE_FAKE_JWT("not_a_real_token");
static const std::string fake_access_token = ENCODE_FAKE_JWT("also_not_real");
static const std::string fake_device_id = "123400000000000000000000";

SyncTestFile::SyncTestFile(std::shared_ptr<app::App> app, std::string name, std::string user_name)
    : SyncTestFile(app->sync_manager()->get_user(user_name, fake_refresh_token, fake_access_token, app->base_url(),
                                                 fake_device_id),
                   bson::Bson(name))
{
}

SyncTestFile::SyncTestFile(std::shared_ptr<SyncUser> user, bson::Bson partition, util::Optional<Schema> schema)
{
    REALM_ASSERT(user);
    sync_config = std::make_shared<realm::SyncConfig>(user, partition);
    sync_config->stop_policy = SyncSessionStopPolicy::Immediately;
    sync_config->error_handler = [](std::shared_ptr<SyncSession>, SyncError error) {
        std::cerr << util::format("An unexpected sync error was caught by the default SyncTestFile handler: '%1'",
                                  error.message)
                  << std::endl;
        abort();
    };
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
        std::cerr << util::format(
                         "An unexpected sync error was caught by the default SyncTestFile handler: '%1' for '%2'",
                         error.message, session->path())
                  << std::endl;
        abort();
    };
    schema_version = 1;
    schema = _schema;
    schema_mode = SchemaMode::AdditiveExplicit;
}

SyncTestFile::SyncTestFile(std::shared_ptr<app::App> app, bson::Bson partition, Schema schema)
    : SyncTestFile(app->current_user(), std::move(partition), std::move(schema))
{
}

// MARK: - SyncServer
SyncServer::SyncServer(const SyncServer::Config& config)
    : m_local_root_dir(config.local_dir.empty() ? util::make_temp_dir() : config.local_dir)
    , m_server(m_local_root_dir, util::none, ([&] {
                   using namespace std::literals::chrono_literals;

#if TEST_ENABLE_SYNC_LOGGING
                   auto logger = new util::StderrLogger();
                   logger->set_level_threshold(realm::util::Logger::Level::TEST_ENABLE_SYNC_LOGGING_LEVEL);
                   m_logger.reset(logger);
#else
                   m_logger.reset(new TestLogger());
#endif

                   sync::Server::Config config;
                   config.logger = m_logger.get();
                   config.token_expiration_clock = this;
                   config.listen_address = "127.0.0.1";
                   config.disable_sync_to_disk = true;

                   return config;
               })())
{
    m_server.start();
    m_url = util::format("ws://127.0.0.1:%1", m_server.listen_endpoint().port());
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

// MARK: - TestSyncManager

TestSyncManager::TestSyncManager(const Config& config, const SyncServer::Config& sync_server_config)
    : m_sync_server(sync_server_config)
    , m_should_teardown_test_directory(config.should_teardown_test_directory)
{
    if (config.transport)
        transport = config.transport;
    app::App::Config app_config = config.app_config;
    set_app_config_defaults(app_config, transport);

    SyncClientConfig sc_config;
    m_base_file_path = config.base_path.empty() ? util::make_temp_dir() + random_string(10) : config.base_path;
    util::try_make_dir(m_base_file_path);
    sc_config.base_file_path = m_base_file_path;
    sc_config.metadata_mode = config.metadata_mode;
    sc_config.log_level = config.sync_client_log_level;

    m_app = app::App::get_uncached_app(app_config, sc_config);
    if (config.override_sync_route) {
        m_app->sync_manager()->set_sync_route(m_sync_server.base_url() + "/realm-sync");
    }
    // initialize sync client
    m_app->sync_manager()->get_sync_client();
}

TestSyncManager::~TestSyncManager()
{
    if (m_should_teardown_test_directory) {
        if (!m_base_file_path.empty() && util::File::exists(m_base_file_path)) {
            try {
                m_app->sync_manager()->reset_for_testing();
                util::try_remove_dir_recursive(m_base_file_path);
            }
            catch (const std::exception& ex) {
                std::cerr << ex.what() << "\n";
            }
            app::App::clear_cached_apps();
        }
    }
}

#endif // REALM_ENABLE_SYNC
