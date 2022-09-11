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

#include "client_reset.hpp"

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

auto do_hash = [](const std::string& name) -> std::string {
    std::array<unsigned char, 32> hash;
    util::sha256(name.data(), name.size(), hash.data());
    return util::hex_dump(hash.data(), hash.size(), "");
};

namespace reset_utils {

Obj create_object(Realm& realm, StringData object_type, util::Optional<ObjectId> primary_key,
                  util::Optional<Partition> partition)
{
    auto table = realm::ObjectStore::table_for_object_type(realm.read_group(), object_type);
    REALM_ASSERT(table);
    FieldValues values = {};
    if (partition) {
        ColKey col = table->get_column_key(partition->property_name);
        REALM_ASSERT(col);
        values.insert(col, Mixed{partition->value});
    }
    return table->create_object_with_primary_key(primary_key ? *primary_key : ObjectId::gen(), std::move(values));
}

namespace {

TableRef get_table(Realm& realm, StringData object_type)
{
    return realm::ObjectStore::table_for_object_type(realm.read_group(), object_type);
}

namespace realm {
//class TestHelper {
//public:
//    static DBRef& get_db(SharedRealm const& shared_realm)
//    {
//        return Realm::Internal::get_db(*shared_realm);
//    }
//};
} // namespace realm

// Run through the client reset steps manually without involving a sync server.
// Useful for speed and when integration testing is not available on a platform.
struct FakeLocalClientReset : public TestClientReset {
    FakeLocalClientReset(const Realm::Config& local_config, const Realm::Config& remote_config)
        : TestClientReset(local_config, remote_config)
    {
//        REALM_ASSERT(m_local_config.sync_config);
        m_mode = m_local_config.sync_config->client_resync_mode;
//        REALM_ASSERT(m_mode == ClientResyncMode::DiscardLocal || m_mode == ClientResyncMode::Recover);
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
//            REALM_ASSERT(history_local);
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
            auto local_db = ::realm::reset_utils::realm::TestHelper::get_db(local_realm);
            auto remote_db = ::realm::reset_utils::realm::TestHelper::get_db(remote_realm);
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

std::unique_ptr<TestClientReset> make_fake_local_client_reset(const Realm::Config& local_config,
                                                              const Realm::Config& remote_config)
{
    return std::make_unique<FakeLocalClientReset>(local_config, remote_config);
}

} // namespace reset_utils

} // namespace realm

using namespace realm;

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

//#endif // REALM_ENABLE_SYNC
