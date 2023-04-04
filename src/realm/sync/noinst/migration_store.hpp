/*************************************************************************
 *
 * Copyright 2023 Realm, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#pragma once

#include <realm/db.hpp>
#include <realm/keys.hpp>
#include <realm/sync/config.hpp>
#include <realm/sync/subscriptions.hpp>

#include <memory>
#include <mutex>
#include <string>

namespace realm::sync {

class MigrationStore;
using MigrationStoreRef = std::shared_ptr<MigrationStore>;

// A MigrationStore manages the PBS -> FLX migration metadata table.
class MigrationStore : public std::enable_shared_from_this<MigrationStore> {
public:
    MigrationStore(const MigrationStore&) = delete;
    MigrationStore& operator=(const MigrationStore&) = delete;

    enum class MigrationState {
        NotMigrated,
        InProgress,
        Migrated,
    };

    static MigrationStoreRef create(DBRef db);

    // Converts the configuration from PBS to FLX if a migration is in progress or completed, otherwise returns the
    // passed in config object.
    std::shared_ptr<realm::SyncConfig> convert_sync_config(std::shared_ptr<realm::SyncConfig> config);

    // Called when the server responds with migrate to FLX and stores the FLX subscription RQL query string.
    void migrate_to_flx(std::string_view rql_query_string);

    // Clear the migrated state
    void cancel_migration();

    bool is_migration_in_progress();
    bool is_migrated();

    // Mark the migration complete and update the state. No-op if not in 'InProgress' state.
    void complete_migration();

    std::string get_query_string();

    // Create subscriptions for each table that does not have a subscription.
    // If subscriptions are created, they are commited and a change of query is sent to the server.
    void create_subscriptions(const SubscriptionStore& subs_store);
    void create_subscriptions(const SubscriptionStore& subs_store, const std::string& rql_query_string);

protected:
    explicit MigrationStore(DBRef db);

    // Read the data from the database - returns true if successful
    // Will return false if read_only is set and the metadata schema
    // versions info is not already set.
    bool load_data(bool read_only = false); // requires !m_mutex

    // Clear the migration store info
    void clear(std::unique_lock<std::mutex> lock);

private:
    // Generate a new subscription that can be added to the subscription store using
    // the query string returned from the server and a name that begins with "flx_migrated_"
    // followed by the class name.
    Subscription make_subscription(const std::string& object_class_name, const std::string& rql_query_string);

    DBRef m_db;

    TableKey m_migration_table;
    ColKey m_migration_started_at;
    ColKey m_migration_completed_at;
    ColKey m_migration_state;
    ColKey m_migration_query_str;

    std::mutex m_mutex;
    // Current migration state
    MigrationState m_state;
    // RQL query string received from the server
    std::string m_query_string;
};

} // namespace realm::sync