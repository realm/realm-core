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

#include <catch2/catch.hpp>

#include "util/event_loop.hpp"
#include "util/test_file.hpp"
#include "util/test_utils.hpp"

#include <realm/object-store/binding_context.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/keypath_helpers.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/thread_safe_reference.hpp>
#include <realm/object-store/util/scheduler.hpp>

#include <realm/db.hpp>

#if REALM_ENABLE_SYNC
#include <realm/object-store/sync/async_open_task.hpp>
#endif

#include <realm/util/fifo_helper.hpp>
#include <realm/util/scope_exit.hpp>

namespace realm {
class TestHelper {
public:
    static DBRef& get_db(SharedRealm const& shared_realm)
    {
        return Realm::Internal::get_db(*shared_realm);
    }

    static void begin_read(SharedRealm const& shared_realm, VersionID version)
    {
        Realm::Internal::begin_read(*shared_realm, version);
    }
};
} // namespace realm

using namespace realm;

TEST_CASE("Automated backup") {
    TestFile config;
    std::string copy_from_file_name = "test_backup-olden-and-golden.realm";
    config.path = "test_backup.realm";
    REQUIRE(util::File::exists(copy_from_file_name));
    util::File::copy(copy_from_file_name, config.path);
    REQUIRE(util::File::exists(config.path));
    // backup name must reflect version of old realm file (which is v6)
    std::string backup_path = "test_backup.v6.backup.realm";
    std::string backup_log = "test_backup.realm.backup-log";
    util::File::try_remove(backup_path);
    util::File::try_remove(backup_log);

    SECTION("Backup enabled will produce correctly named backup") {
        config.backup_at_file_format_change = true;
        auto realm = Realm::get_shared_realm(config);
        REQUIRE(util::File::exists(backup_path));
        REQUIRE(util::File::exists(backup_log));
    }

    SECTION("Backup disabled produces no backup") {
        config.backup_at_file_format_change = false;
        auto realm = Realm::get_shared_realm(config);
        REQUIRE(!util::File::exists(backup_path));
        REQUIRE(!util::File::exists(backup_log));
    }
}
