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

#include "util/event_loop.hpp"
#include "util/test_file.hpp"
#include "util/test_utils.hpp"
#include "../util/semaphore.hpp"

#include <realm/db.hpp>
#include <realm/history.hpp>

#include <realm/impl/simulated_failure.hpp>

#include <realm/object-store/binding_context.hpp>
#include <realm/object-store/keypath_helpers.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/class.hpp>
#include <realm/object-store/thread_safe_reference.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/util/event_loop_dispatcher.hpp>
#include <realm/object-store/util/scheduler.hpp>

#include <realm/util/base64.hpp>
#include <realm/util/fifo_helper.hpp>
#include <realm/util/scope_exit.hpp>

#if REALM_ENABLE_SYNC
#include <util/sync/flx_sync_harness.hpp>
#include <util/sync/sync_test_utils.hpp>
#include <util/test_file.hpp>
#ifdef REALM_ENABLE_AUTH_TESTS
#include <util/sync/baas_admin_api.hpp>
#endif // REALM_ENABLE_AUTH_TESTS

#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/sync/impl/app_metadata.hpp>
#include <realm/object-store/sync/sync_session.hpp>

#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/subscriptions.hpp>
#endif // REALM_ENABLE_SYNC

#include <catch2/catch_all.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <external/json/json.hpp>

#include <array>
#if REALM_HAVE_UV
#include <uv.h>
#endif // REALM_HAVE_UV

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

static bool operator==(IndexSet const& a, IndexSet const& b)
{
    return std::equal(a.as_indexes().begin(), a.as_indexes().end(), b.as_indexes().begin(), b.as_indexes().end());
}
} // namespace realm

using namespace realm;

namespace {
class Observer : public BindingContext {
public:
    Observer(Obj& obj)
    {
        m_result.push_back(ObserverState{obj.get_table()->get_key(), obj.get_key(), nullptr});
    }

    IndexSet array_change(size_t index, ColKey col_key) const noexcept
    {
        auto& changes = m_result[index].changes;
        auto col = changes.find(col_key.value);
        return col == changes.end() ? IndexSet{} : col->second.indices;
    }

private:
    std::vector<ObserverState> m_result;
    std::vector<void*> m_invalidated;

    std::vector<ObserverState> get_observed_rows() override
    {
        return m_result;
    }

    void did_change(std::vector<ObserverState> const& observers, std::vector<void*> const& invalidated, bool) override
    {
        m_invalidated = invalidated;
        m_result = observers;
    }
};
} // namespace

TEST_CASE("SharedRealm: get_shared_realm()") {
    TestFile config;
    config.schema_version = 1;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Int}}},
    };

    SECTION("should return the same instance when caching is enabled") {
        config.cache = true;
        auto realm1 = Realm::get_shared_realm(config);
        auto realm2 = Realm::get_shared_realm(config);
        REQUIRE(realm1.get() == realm2.get());
    }

    SECTION("should return different instances when caching is disabled") {
        config.cache = false;
        auto realm1 = Realm::get_shared_realm(config);
        auto realm2 = Realm::get_shared_realm(config);
        REQUIRE(realm1.get() != realm2.get());
    }

    SECTION("should validate that the config is sensible") {
        SECTION("bad encryption key") {
            config.encryption_key = std::vector<char>(2, 0);
            REQUIRE_EXCEPTION(Realm::get_shared_realm(config), InvalidEncryptionKey,
                              "Encryption key must be 64 bytes.");
        }

        SECTION("schema without schema version") {
            config.schema_version = ObjectStore::NotVersioned;
            REQUIRE_EXCEPTION(Realm::get_shared_realm(config), IllegalCombination,
                              "A schema version must be specified when the schema is specified");
        }

        SECTION("migration function for immutable") {
            config.schema_mode = SchemaMode::Immutable;
            config.migration_function = [](auto, auto, auto) {};
            REQUIRE_EXCEPTION(Realm::get_shared_realm(config), IllegalCombination,
                              "Realms opened in immutable mode do not use a migration function");
        }

        SECTION("migration function for read-only") {
            config.schema_mode = SchemaMode::ReadOnly;
            config.migration_function = [](auto, auto, auto) {};
            REQUIRE_EXCEPTION(Realm::get_shared_realm(config), IllegalCombination,
                              "Realms opened in read-only mode do not use a migration function");
        }

        SECTION("migration function for additive discovered") {
            config.schema_mode = SchemaMode::AdditiveDiscovered;
            config.migration_function = [](auto, auto, auto) {};
            REQUIRE_EXCEPTION(Realm::get_shared_realm(config), IllegalCombination,
                              "Realms opened in Additive-only schema mode do not use a migration function");
        }

        SECTION("migration function for additive explicit") {
            config.schema_mode = SchemaMode::AdditiveExplicit;
            config.migration_function = [](auto, auto, auto) {};
            REQUIRE_EXCEPTION(Realm::get_shared_realm(config), IllegalCombination,
                              "Realms opened in Additive-only schema mode do not use a migration function");
        }

        SECTION("initialization function for immutable") {
            config.schema_mode = SchemaMode::Immutable;
            config.initialization_function = [](auto) {};
            REQUIRE_EXCEPTION(Realm::get_shared_realm(config), IllegalCombination,
                              "Realms opened in immutable mode do not use an initialization function");
        }

        SECTION("initialization function for read-only") {
            config.schema_mode = SchemaMode::ReadOnly;
            config.initialization_function = [](auto) {};
            REQUIRE_EXCEPTION(Realm::get_shared_realm(config), IllegalCombination,
                              "Realms opened in read-only mode do not use an initialization function");
        }
        SECTION("in-memory encrypted realms are rejected") {
            config.in_memory = true;
            config.encryption_key = make_test_encryption_key();
            REQUIRE_EXCEPTION(Realm::get_shared_realm(config), IllegalCombination,
                              "Encryption is not supported for in-memory realms");
        }
    }

    SECTION("should reject mismatched config") {
        config.encryption_key.clear(); // may be set already when encrypting all

        SECTION("schema version") {
            auto realm = Realm::get_shared_realm(config);
            config.schema_version = 2;
            REQUIRE_EXCEPTION(
                Realm::get_shared_realm(config), MismatchedConfig,
                Catch::Matchers::Matches("Realm at path '.*' already opened with different schema version."));

            config.schema = util::none;
            config.schema_version = ObjectStore::NotVersioned;
            REQUIRE_NOTHROW(Realm::get_shared_realm(config));
        }

        SECTION("schema mode") {
            auto realm = Realm::get_shared_realm(config);
            config.schema_mode = SchemaMode::Manual;
            REQUIRE_EXCEPTION(
                Realm::get_shared_realm(config), MismatchedConfig,
                Catch::Matchers::Matches("Realm at path '.*' already opened with a different schema mode."));
        }

        SECTION("durability") {
            auto realm = Realm::get_shared_realm(config);
            config.in_memory = true;
            REQUIRE_EXCEPTION(
                Realm::get_shared_realm(config), MismatchedConfig,
                Catch::Matchers::Matches("Realm at path '.*' already opened with different inMemory settings."));
        }

        SECTION("schema") {
            auto realm = Realm::get_shared_realm(config);
            config.schema = Schema{
                {"object", {{"value", PropertyType::Int}, {"value2", PropertyType::Int}}},
            };
            REQUIRE_EXCEPTION(
                Realm::get_shared_realm(config), SchemaMismatch,
                Catch::Matchers::ContainsSubstring("Migration is required due to the following errors:"));
        }
    }

// Windows doesn't use fifos
#ifndef _WIN32
    SECTION("should be able to set a FIFO fallback path") {
        std::string fallback_dir = util::make_temp_dir() + "/fallback/";
        realm::util::try_make_dir(fallback_dir);
        TestFile config;
        config.fifo_files_fallback_path = fallback_dir;
        config.schema_version = 1;
        config.schema = Schema{
            {"object", {{"value", PropertyType::Int}}},
        };

        realm::util::make_dir(config.path + ".note");
        auto realm = Realm::get_shared_realm(config);
        auto fallback_file = util::format("%1realm_%2.note", fallback_dir,
                                          std::hash<std::string>()(config.path)); // Mirror internal implementation
        REQUIRE(util::File::exists(fallback_file));
        realm::util::remove_dir(config.path + ".note");
        REQUIRE(realm::util::try_remove_dir_recursive(fallback_dir));
    }

    SECTION("automatically append dir separator to end of fallback path") {
        std::string fallback_dir = util::make_temp_dir() + "/fallback";
        realm::util::try_make_dir(fallback_dir);
        TestFile config;
        config.fifo_files_fallback_path = fallback_dir;
        config.schema_version = 1;
        config.schema = Schema{
            {"object", {{"value", PropertyType::Int}}},
        };

        realm::util::make_dir(config.path + ".note");
        auto realm = Realm::get_shared_realm(config);
        auto fallback_file = util::format("%1/realm_%2.note", fallback_dir,
                                          std::hash<std::string>()(config.path)); // Mirror internal implementation
        REQUIRE(util::File::exists(fallback_file));
        realm::util::remove_dir(config.path + ".note");
        REQUIRE(realm::util::try_remove_dir_recursive(fallback_dir));
    }
#endif

    SECTION("should verify that the schema is valid") {
        config.schema =
            Schema{{"object",
                    {{"value", PropertyType::Int}},
                    {{"invalid backlink", PropertyType::LinkingObjects | PropertyType::Array, "object", "value"}}}};
        REQUIRE_THROWS_CONTAINING(Realm::get_shared_realm(config), "origin of linking objects property");
    }

    SECTION("should apply the schema if one is supplied") {
        Realm::get_shared_realm(config);

        {
            Group g(config.path, config.encryption_key.data());
            auto table = ObjectStore::table_for_object_type(g, "object");
            REQUIRE(table);
            REQUIRE(table->get_column_count() == 1);
            REQUIRE(table->get_column_name(*table->get_column_keys().begin()) == "value");
        }

        config.schema_version = 2;
        config.schema = Schema{
            {"object", {{"value", PropertyType::Int}, {"value2", PropertyType::Int}}},
        };
        bool migration_called = false;
        config.migration_function = [&](SharedRealm old_realm, SharedRealm new_realm, Schema&) {
            migration_called = true;
            REQUIRE_FALSE(old_realm->auto_refresh());
            REQUIRE(ObjectStore::table_for_object_type(old_realm->read_group(), "object")->get_column_count() == 1);
            REQUIRE(ObjectStore::table_for_object_type(new_realm->read_group(), "object")->get_column_count() == 2);
        };
        Realm::get_shared_realm(config);
        REQUIRE(migration_called);
    }

    SECTION("should properly roll back from migration errors") {
        Realm::get_shared_realm(config);

        config.schema_version = 2;
        config.schema = Schema{
            {"object", {{"value", PropertyType::Int}, {"value2", PropertyType::Int}}},
        };
        bool migration_called = false;
        config.migration_function = [&](SharedRealm old_realm, SharedRealm new_realm, Schema&) {
            REQUIRE_FALSE(old_realm->auto_refresh());
            REQUIRE(ObjectStore::table_for_object_type(old_realm->read_group(), "object")->get_column_count() == 1);
            REQUIRE(ObjectStore::table_for_object_type(new_realm->read_group(), "object")->get_column_count() == 2);
            if (!migration_called) {
                migration_called = true;
                throw "error";
            }
        };
        REQUIRE_THROWS_WITH(Realm::get_shared_realm(config), "error");
        REQUIRE(migration_called);
        REQUIRE_NOTHROW(Realm::get_shared_realm(config));
    }

    SECTION("should read the schema from the file if none is supplied") {
        Realm::get_shared_realm(config);

        config.schema = util::none;
        auto realm = Realm::get_shared_realm(config);
        REQUIRE(realm->schema().size() == 1);
        auto it = realm->schema().find("object");
        auto table = realm->read_group().get_table("class_object");
        REQUIRE(it != realm->schema().end());
        REQUIRE(it->table_key == table->get_key());
        REQUIRE(it->persisted_properties.size() == 1);
        REQUIRE(it->persisted_properties[0].name == "value");
        REQUIRE(it->persisted_properties[0].column_key == table->get_column_key("value"));
    }

    SECTION("should read the proper schema from the file if a custom version is supplied") {
        Realm::get_shared_realm(config);

        config.schema = util::none;
        config.schema_mode = SchemaMode::AdditiveExplicit;
        config.schema_version = 0;

        auto realm = Realm::get_shared_realm(config);
        REQUIRE(realm->schema().size() == 1);

        auto& db = TestHelper::get_db(realm);
        auto rt = db->start_read();
        VersionID old_version = rt->get_version_of_current_transaction();
        realm->close();

        config.schema = Schema{
            {"object", {{"value", PropertyType::Int}}},
            {"object1", {{"value", PropertyType::Int}}},
        };
        config.schema_version = 1;
        realm = Realm::get_shared_realm(config);
        REQUIRE(realm->schema().size() == 2);

        config.schema = util::none;
        auto old_realm = Realm::get_shared_realm(config);
        // must retain 'rt' until after opening for reading at that version
        TestHelper::begin_read(old_realm, old_version);
        rt = nullptr;
        REQUIRE(old_realm->schema().size() == 1);
    }

    SECTION("should sensibly handle opening an uninitialized file without a schema specified") {
        config.cache = GENERATE(false, true);

        // create an empty file
        util::File(config.path, util::File::mode_Write);

        // open the empty file, but don't initialize the schema
        Realm::Config config_without_schema = config;
        config_without_schema.schema = util::none;
        config_without_schema.schema_version = ObjectStore::NotVersioned;
        auto realm = Realm::get_shared_realm(config_without_schema);
        REQUIRE(realm->schema().empty());
        REQUIRE(realm->schema_version() == ObjectStore::NotVersioned);
        // verify that we can get another Realm instance
        REQUIRE_NOTHROW(Realm::get_shared_realm(config_without_schema));

        // verify that we can also still open the file with a proper schema
        auto realm2 = Realm::get_shared_realm(config);
        REQUIRE_FALSE(realm2->schema().empty());
        REQUIRE(realm2->schema_version() == 1);
    }

    SECTION("should populate the table columns in the schema when opening as immutable") {
        Realm::get_shared_realm(config);

        config.schema_mode = SchemaMode::Immutable;
        auto realm = Realm::get_shared_realm(config);
        auto it = realm->schema().find("object");
        auto table = realm->read_group().get_table("class_object");
        REQUIRE(it != realm->schema().end());
        REQUIRE(it->table_key == table->get_key());
        REQUIRE(it->persisted_properties.size() == 1);
        REQUIRE(it->persisted_properties[0].name == "value");
        REQUIRE(it->persisted_properties[0].column_key == table->get_column_key("value"));

        SECTION("refreshing an immutable Realm throws") {
            REQUIRE_THROWS_WITH(realm->refresh(), "Can't refresh an immutable Realm.");
        }
    }

    SECTION("should support using different table subsets on different threads") {
        auto realm1 = Realm::get_shared_realm(config);

        config.schema = Schema{
            {"object 2", {{"value", PropertyType::Int}}},
        };
        auto realm2 = Realm::get_shared_realm(config);

        config.schema = util::none;
        auto realm3 = Realm::get_shared_realm(config);

        config.schema = Schema{
            {"object", {{"value", PropertyType::Int}}},
        };
        auto realm4 = Realm::get_shared_realm(config);

        realm1->refresh();
        realm2->refresh();

        REQUIRE(realm1->schema().size() == 1);
        REQUIRE(realm1->schema().find("object") != realm1->schema().end());
        REQUIRE(realm2->schema().size() == 1);
        REQUIRE(realm2->schema().find("object 2") != realm2->schema().end());
        REQUIRE(realm3->schema().size() == 2);
        REQUIRE(realm3->schema().find("object") != realm3->schema().end());
        REQUIRE(realm3->schema().find("object 2") != realm3->schema().end());
        REQUIRE(realm4->schema().size() == 1);
        REQUIRE(realm4->schema().find("object") != realm4->schema().end());
    }

#ifndef _WIN32
    SECTION("should throw when creating the notification pipe fails") {
        // The ExternalCommitHelper implementation on Windows doesn't rely on FIFOs
        std::string expected_path = config.path + ".note";
        REQUIRE(util::try_make_dir(config.path + ".note"));
        if (auto tmp_dir = DBOptions::get_sys_tmp_dir(); !tmp_dir.empty()) {
            expected_path = util::format("%1realm_%2.note", util::normalize_dir(tmp_dir),
                                         std::hash<std::string>()(config.path)); // Mirror internal implementation
            REQUIRE(util::try_make_dir(expected_path));
        }
        REQUIRE_EXCEPTION(
            Realm::get_shared_realm(config), FileAlreadyExists,
            util::format("Cannot create fifo at path '%1': a non-fifo entry already exists at that path.",
                         expected_path));
        util::remove_dir(config.path + ".note");
        util::try_remove_dir(expected_path);
    }
#endif

#if !REALM_USE_UV && !TEST_SCHEDULER_UV // uv scheduler does not support background threads
    SECTION("should get different instances on different threads") {
        config.cache = true;
        auto realm1 = Realm::get_shared_realm(config);
        JoiningThread([&] {
            auto realm2 = Realm::get_shared_realm(config);
            REQUIRE(realm1 != realm2);
        });
    }
#endif

    SECTION("should detect use of Realm on incorrect thread") {
        auto realm = Realm::get_shared_realm(config);
        JoiningThread([&] {
            REQUIRE_THROWS_MATCHES(realm->verify_thread(), LogicError,
                                   Catch::Matchers::Message("Realm accessed from incorrect thread."));
        });
    }

    // Our test scheduler uses a simple integer identifier to allow cross thread scheduling
    class SimpleScheduler : public util::Scheduler {
    public:
        SimpleScheduler(size_t id)
            : Scheduler()
            , m_id(id)
        {
        }

        bool is_on_thread() const noexcept override
        {
            return true;
        }
        bool is_same_as(const Scheduler* other) const noexcept override
        {
            const SimpleScheduler* o = dynamic_cast<const SimpleScheduler*>(other);
            return (o && (o->m_id == m_id));
        }
        bool can_invoke() const noexcept override
        {
            return false;
        }
        void invoke(util::UniqueFunction<void()>&&) override {}

    protected:
        size_t m_id;
    };

    SECTION("should get different instances for different explicitly different schedulers") {
        config.cache = true;
        config.scheduler = std::make_shared<SimpleScheduler>(1);
        auto realm1 = Realm::get_shared_realm(config);
        config.scheduler = std::make_shared<SimpleScheduler>(2);
        auto realm2 = Realm::get_shared_realm(config);
        REQUIRE(realm1 != realm2);

        config.scheduler = nullptr;
        auto realm3 = Realm::get_shared_realm(config);
        REQUIRE(realm1 != realm3);
        REQUIRE(realm2 != realm3);
    }

    SECTION("can use Realm with explicit scheduler on different thread") {
        config.cache = true;
        config.scheduler = std::make_shared<SimpleScheduler>(1);
        auto realm = Realm::get_shared_realm(config);
        JoiningThread([&] {
            REQUIRE_NOTHROW(realm->verify_thread());
        });
    }

    SECTION("should get same instance for same explicit execution context on different thread") {
        config.cache = true;
        config.scheduler = std::make_shared<SimpleScheduler>(1);
        auto realm1 = Realm::get_shared_realm(config);
        JoiningThread([&] {
            auto realm2 = Realm::get_shared_realm(config);
            REQUIRE(realm1 == realm2);
        });
    }

    SECTION("should not modify the schema when fetching from the cache") {
        config.cache = true;
        auto realm = Realm::get_shared_realm(config);
        auto object_schema = &*realm->schema().find("object");
        Realm::get_shared_realm(config);
        REQUIRE(object_schema == &*realm->schema().find("object"));
    }

    SECTION("should reuse cached frozen Realm if versions match") {
        config.cache = true;
        auto realm = Realm::get_shared_realm(config);
        realm->read_group();
        auto frozen = realm->freeze();
        frozen->read_group();

        REQUIRE(frozen != realm);
        REQUIRE(realm->read_transaction_version() == frozen->read_transaction_version());

        REQUIRE(realm->freeze() == frozen);
        REQUIRE(Realm::get_frozen_realm(config, realm->read_transaction_version()) == frozen);
    }

    SECTION("should not use cached frozen Realm if versions don't match") {
        config.cache = true;
        auto realm = Realm::get_shared_realm(config);
        realm->read_group();
        auto frozen1 = realm->freeze();
        frozen1->read_group();

        REQUIRE(frozen1 != realm);
        REQUIRE(realm->read_transaction_version() == frozen1->read_transaction_version());

        auto table = realm->read_group().get_table("class_object");
        realm->begin_transaction();
        table->create_object();
        realm->commit_transaction();

        REQUIRE(realm->read_transaction_version() > frozen1->read_transaction_version());

        auto frozen2 = realm->freeze();
        frozen2->read_group();

        REQUIRE(frozen2 != frozen1);
        REQUIRE(frozen2 != realm);
        REQUIRE(realm->read_transaction_version() == frozen2->read_transaction_version());
        REQUIRE(frozen2->read_transaction_version() > frozen1->read_transaction_version());
    }

    SECTION("frozen realm should have the same schema as originating realm") {
        auto full_schema = Schema{
            {"object1", {{"value", PropertyType::Int}}},
            {"object2", {{"value", PropertyType::Int}}},
        };

        auto subset_schema = Schema{
            {"object1", {{"value", PropertyType::Int}}},
        };

        config.schema = full_schema;

        auto realm = Realm::get_shared_realm(config);
        realm->close();

        config.schema = subset_schema;

        realm = Realm::get_shared_realm(config);
        realm->read_group();
        auto frozen_realm = realm->freeze();
        auto frozen_schema = frozen_realm->schema();

        REQUIRE(full_schema != subset_schema);
        REQUIRE(realm->schema() == subset_schema);
        REQUIRE(frozen_schema == subset_schema);
    }

    SECTION("frozen realm should have the correct schema even if more properties are added later") {
        config.schema_mode = SchemaMode::AdditiveExplicit;
        auto full_schema = Schema{
            {"object", {{"value1", PropertyType::Int}, {"value2", PropertyType::Int}}},
        };

        auto subset_schema = Schema{
            {"object", {{"value1", PropertyType::Int}}},
        };

        config.schema = subset_schema;
        auto realm = Realm::get_shared_realm(config);
        realm->read_group();

        config.schema = full_schema;
        auto realm2 = Realm::get_shared_realm(config);
        realm2->read_group();

        auto frozen_realm = realm->freeze();
        REQUIRE(realm->schema() == subset_schema);
        REQUIRE(realm2->schema() == full_schema);
        REQUIRE(frozen_realm->schema() == subset_schema);
    }

    SECTION("freeze with orphaned embedded tables") {
        auto schema = Schema{
            {"object1", {{"value", PropertyType::Int}}},
            {"object2", ObjectSchema::ObjectType::Embedded, {{"value", PropertyType::Int}}},
        };
        config.schema = schema;
        config.schema_mode = SchemaMode::AdditiveDiscovered;
        auto realm = Realm::get_shared_realm(config);
        realm->read_group();
        auto frozen_realm = realm->freeze();
        REQUIRE(frozen_realm->schema() == schema);
    }
}

TEST_CASE("SharedRealm: schema_subset_mode") {
    TestFile config;
    config.schema_mode = SchemaMode::AdditiveExplicit;
    config.schema_version = 1;
    config.schema_subset_mode = SchemaSubsetMode::Complete;
    config.encryption_key.clear();

    // Use a DB directly to simulate changes made by another process
    auto db = DB::create(make_in_realm_history(), config.path);

    // Changing the schema version results in update_schema() hitting a very
    // different code path for Additive modes, so test both with the schema version
    // matching and not matching
    auto set_schema_version = GENERATE(false, true);
    INFO("Matching schema version: " << set_schema_version);
    if (set_schema_version) {
        auto tr = db->start_write();
        ObjectStore::set_schema_version(*tr, 1);
        tr->commit();
    }

    SECTION("additional properties are added at the end") {
        {
            auto tr = db->start_write();
            auto table = tr->add_table("class_object");
            for (int i = 0; i < 5; ++i) {
                table->add_column(type_Int, util::format("col %1", i));
            }
            tr->commit();
        }

        // missing col 0 and 4, and order is different from column order
        config.schema = Schema{{"object",
                                {
                                    {"col 2", PropertyType::Int},
                                    {"col 3", PropertyType::Int},
                                    {"col 1", PropertyType::Int},
                                }}};

        auto realm = Realm::get_shared_realm(config);
        auto& properties = realm->schema().find("object")->persisted_properties;
        REQUIRE(properties.size() == 5);
        REQUIRE(properties[0].name == "col 2");
        REQUIRE(properties[1].name == "col 3");
        REQUIRE(properties[2].name == "col 1");
        REQUIRE(properties[3].name == "col 0");
        REQUIRE(properties[4].name == "col 4");

        for (auto& property : properties) {
            REQUIRE(property.column_key != ColKey{});
        }

        config.schema_subset_mode.include_properties = false;
        realm = Realm::get_shared_realm(config);
        REQUIRE(realm->schema().find("object")->persisted_properties.size() == 3);
    }

    SECTION("additional tables are added in sorted order") {
        {
            auto tr = db->start_write();
            // In reverse order so that just using the table order doesn't
            // work accidentally
            tr->add_table("class_F")->add_column(type_Int, "value");
            tr->add_table("class_E")->add_column(type_Int, "value");
            tr->add_table("class_D")->add_column(type_Int, "value");
            tr->add_table("class_C")->add_column(type_Int, "value");
            tr->add_table("class_B")->add_column(type_Int, "value");
            tr->add_table("class_A")->add_column(type_Int, "value");
            tr->commit();
        }

        config.schema = Schema{
            {"A", {{"value", PropertyType::Int}}},
            {"E", {{"value", PropertyType::Int}}},
            {"D", {{"value", PropertyType::Int}}},
        };
        auto realm = Realm::get_shared_realm(config);
        auto& schema = realm->schema();
        REQUIRE(schema.size() == 6);
        REQUIRE(std::is_sorted(schema.begin(), schema.end(), [](auto& a, auto& b) {
            return a.name < b.name;
        }));

        config.schema_subset_mode.include_types = false;
        realm = Realm::get_shared_realm(config);
        REQUIRE(realm->schema().size() == 3);
    }

    SECTION("schema is updated when refreshing over a schema change") {
        config.schema = Schema{{"object", {{"value", PropertyType::Int}}}};
        auto realm = Realm::get_shared_realm(config);
        realm->read_group();
        auto& schema = realm->schema();

        {
            auto tr = db->start_write();
            tr->get_table("class_object")->add_column(type_Int, "value 2");
            tr->commit();
        }

        REQUIRE(schema.find("object")->persisted_properties.size() == 1);
        realm->refresh();
        REQUIRE(schema.find("object")->persisted_properties.size() == 2);

        {
            auto tr = db->start_write();
            tr->add_table("class_object 2")->add_column(type_Int, "value");
            tr->commit();
        }

        REQUIRE(schema.size() == 1);
        realm->refresh();
        REQUIRE(schema.size() == 2);
    }

    SECTION("schema is updated when schema is modified while not in a read transaction") {
        config.schema = Schema{{"object", {{"value", PropertyType::Int}}}};
        auto realm = Realm::get_shared_realm(config);
        auto& schema = realm->schema();

        {
            auto tr = db->start_write();
            tr->get_table("class_object")->add_column(type_Int, "value 2");
            tr->commit();
        }

        REQUIRE(schema.find("object")->persisted_properties.size() == 1);
        realm->read_group();
        REQUIRE(schema.find("object")->persisted_properties.size() == 2);
        realm->invalidate();

        {
            auto tr = db->start_write();
            tr->add_table("class_object 2")->add_column(type_Int, "value");
            tr->commit();
        }

        REQUIRE(schema.size() == 1);
        realm->read_group();
        REQUIRE(schema.size() == 2);
    }

    SECTION("frozen Realm sees the correct schema for each version") {
        config.schema = Schema{{"object", {{"value", PropertyType::Int}}}};
        std::vector<std::shared_ptr<Realm>> realms;
        for (int i = 0; i < 10; ++i) {
            realms.push_back(Realm::get_shared_realm(config));
            realms.back()->read_group();
            auto tr = db->start_write();
            tr->add_table(util::format("class_object %1", i))->add_column(type_Int, "value");
            tr->commit();
        }

        auto reset_schema = GENERATE(false, true);
        if (reset_schema) {
            config.schema.reset();
        }

        for (size_t i = 0; i < 10; ++i) {
            auto& r = *realms[i];
            REQUIRE(r.schema().size() == i + 1);
            auto frozen = r.freeze();
            REQUIRE(frozen->schema().size() == i + 1);
            REQUIRE(frozen->schema_version() == config.schema_version);
            frozen = Realm::get_frozen_realm(config, r.read_transaction_version());
            REQUIRE(frozen->schema().size() == i + 1);
            REQUIRE(frozen->schema_version() == config.schema_version);
        }

        SECTION("schema not set in config") {
            config.schema = std::nullopt;
            for (size_t i = 0; i < 10; ++i) {
                auto& r = *realms[i];
                REQUIRE(r.schema().size() == i + 1);
                REQUIRE(r.freeze()->schema().size() == i + 1);
                REQUIRE(Realm::get_frozen_realm(config, r.read_transaction_version())->schema().size() == i + 1);
            }
        }
    }

    SECTION("obtaining a frozen realm with an incompatible schema throws") {
        config.schema = Schema{{"object", {{"value", PropertyType::Int}}}};
        auto old_realm = Realm::get_shared_realm(config);
        {
            auto tr = db->start_write();
            auto table = tr->get_table("class_object");
            table->create_object();
            tr->commit();
        }
        old_realm->read_group();

        {
            auto tr = db->start_write();
            auto table = tr->add_table("class_object 2");
            ColKey val_col = table->add_column(type_Int, "value");
            table->create_object().set(val_col, 1);
            tr->commit();
        }

        config.schema = Schema{
            {"object", {{"value", PropertyType::Int}}},
            {"object 2", {{"value", PropertyType::Int}}},
        };
        auto new_realm = Realm::get_shared_realm(config);
        new_realm->read_group();

        REQUIRE(old_realm->freeze()->schema().size() == 1);
        REQUIRE(new_realm->freeze()->schema().size() == 2);
        REQUIRE(Realm::get_frozen_realm(config, new_realm->read_transaction_version())->schema().size() == 2);
        // An additive change is allowed, the unknown table is empty
        REQUIRE(Realm::get_frozen_realm(config, old_realm->read_transaction_version())->schema().size() == 2);

        config.schema = Schema{{"object", {{"value", PropertyType::String}}}}; // int -> string
        // Fails because the schema has an invalid breaking change
        REQUIRE_THROWS_AS(Realm::get_frozen_realm(config, new_realm->read_transaction_version()),
                          InvalidReadOnlySchemaChangeException);
        REQUIRE_THROWS_AS(Realm::get_frozen_realm(config, old_realm->read_transaction_version()),
                          InvalidReadOnlySchemaChangeException);
        config.schema = Schema{
            {"object", {{"value", PropertyType::Int}}},
            {"object 2", {{"value", PropertyType::String}}}, // int -> string
        };
        // fails due to invalid change on object 2 type
        REQUIRE_THROWS_AS(Realm::get_frozen_realm(config, new_realm->read_transaction_version()),
                          InvalidReadOnlySchemaChangeException);
        // opening the old state does not fail because the schema is an additive change
        auto frozen_old = Realm::get_frozen_realm(config, old_realm->read_transaction_version());
        REQUIRE(frozen_old->schema().size() == 2);
        {
            TableRef table = frozen_old->read_group().get_table("class_object");
            Results results(frozen_old, table);
            REQUIRE(results.is_frozen());
            REQUIRE(results.size() == 1);
        }
        {
            TableRef table = frozen_old->read_group().get_table("class_object 2");
            REQUIRE(!table);
            Results results(frozen_old, table);
            REQUIRE(results.is_frozen());
            REQUIRE(results.size() == 0);
        }
        config.schema = Schema{
            {"object", {{"value", PropertyType::Int}, {"value 2", PropertyType::String}}}, // add property
        };
        // fails due to additional property on object
        REQUIRE_THROWS_AS(Realm::get_frozen_realm(config, old_realm->read_transaction_version()),
                          InvalidReadOnlySchemaChangeException);
        REQUIRE_THROWS_AS(Realm::get_frozen_realm(config, new_realm->read_transaction_version()),
                          InvalidReadOnlySchemaChangeException);
    }
}

#if REALM_ENABLE_SYNC
TEST_CASE("Get Realm using Async Open", "[sync][pbs][async open]") {
    if (!util::EventLoop::has_implementation())
        return;

    TestSyncManager tsm;
    SyncTestFile config(tsm, "default");
    ObjectSchema object_schema = {"object",
                                  {
                                      {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                      {"value", PropertyType::Int},
                                  }};
    config.schema = Schema{object_schema};
    SyncTestFile config2(tsm, "default");
    config2.schema = config.schema;

    std::mutex mutex;

    SECTION("can open synced Realms that don't already exist") {
        auto realm = successfully_async_open_realm(config);
        REQUIRE(realm->read_group().get_table("class_object"));
    }

    SECTION("can write a realm file without client file id") {
        ThreadSafeReference realm_ref;
        SyncTestFile config3(tsm, "default");
        config3.schema = config.schema;
        uint64_t client_file_id;

        // Create some content
        auto origin = Realm::get_shared_realm(config);
        origin->begin_transaction();
        Class cls = origin->get_class("object");
        cls.create_object(0);
        origin->commit_transaction();
        wait_for_upload(*origin);

        // Create realm file without client file id
        {
            auto realm = successfully_async_open_realm(config);
            // Write some data
            realm->begin_transaction();
            realm->get_class("object").create_object(2);
            realm->commit_transaction();
            wait_for_upload(*realm);
            wait_for_download(*realm);
            client_file_id = realm->read_group().get_sync_file_id();

            realm->convert(config3);
        }

        // Create some more content on the server
        origin->begin_transaction();
        cls.create_object(7);
        origin->commit_transaction();
        wait_for_upload(*origin);

        // Now open a realm based on the realm file created above
        auto realm = Realm::get_shared_realm(config3);
        Class cls2 = realm->get_class("object");
        wait_for_download(*realm);
        wait_for_upload(*realm);

        // Make sure we have got a new client file id
        REQUIRE(realm->read_group().get_sync_file_id() != client_file_id);
        REQUIRE(cls.num_objects() == 3);

        // Check that we can continue committing to this realm
        realm->begin_transaction();
        cls2.create_object(5);
        realm->commit_transaction();
        wait_for_upload(*realm);

        // Check that this change is now in the original realm
        wait_for_download(*origin);
        origin->refresh();
        REQUIRE(cls.num_objects() == 4);
    }

    SECTION("downloads Realms which exist on the server") {
        {
            auto realm = Realm::get_shared_realm(config2);
            realm->begin_transaction();
            realm->read_group().get_table("class_object")->create_object_with_primary_key(0);
            realm->commit_transaction();
            wait_for_upload(*realm);
        }

        auto realm = successfully_async_open_realm(config);
        REQUIRE(realm->read_group().get_table("class_object"));
    }

    SECTION("progress notifiers of a task are cancelled if the task is cancelled") {
        bool progress_notifier1_called = false;
        bool task1_completed = false;
        bool progress_notifier2_called = false;
        bool task2_completed = false;
        {
            auto realm = Realm::get_shared_realm(config2);
            realm->begin_transaction();
            realm->read_group().get_table("class_object")->create_object_with_primary_key(0);
            realm->commit_transaction();
            wait_for_upload(*realm);
        }

        DBOptions options;
        options.encryption_key = config.encryption_key.data();
        auto db = DB::create(sync::make_client_replication(), config.path, options);
        auto write = db->start_write(); // block sync from writing until we cancel

        std::shared_ptr<AsyncOpenTask> task = Realm::get_synchronized_realm(config);
        std::shared_ptr<AsyncOpenTask> task2 = Realm::get_synchronized_realm(config);
        REQUIRE(task);
        REQUIRE(task2);
        task->register_download_progress_notifier([&](uint64_t, uint64_t, double) {
            std::lock_guard<std::mutex> guard(mutex);
            REQUIRE(!task1_completed);
            progress_notifier1_called = true;
        });
        task2->register_download_progress_notifier([&](uint64_t, uint64_t, double) {
            std::lock_guard<std::mutex> guard(mutex);
            REQUIRE(!task2_completed);
            progress_notifier2_called = true;
        });
        task->start([&](ThreadSafeReference realm_ref, std::exception_ptr err) {
            std::lock_guard<std::mutex> guard(mutex);
            REQUIRE(!err);
            REQUIRE(realm_ref);
            task1_completed = true;
        });
        task->cancel();
        ThreadSafeReference rref;
        task2->start([&](ThreadSafeReference realm_ref, std::exception_ptr err) {
            std::lock_guard<std::mutex> guard(mutex);
            REQUIRE(!err);
            REQUIRE(realm_ref);
            rref = std::move(realm_ref);
            task2_completed = true;
        });
        write = nullptr; // unblock sync
        util::EventLoop::main().run_until([&] {
            std::lock_guard<std::mutex> guard(mutex);
            return task2_completed;
        });
        std::lock_guard<std::mutex> guard(mutex);
        REQUIRE(!progress_notifier1_called);
        REQUIRE(!task1_completed);
        REQUIRE(progress_notifier2_called);
        REQUIRE(task2_completed);
        SharedRealm realm = Realm::get_shared_realm(std::move(rref));
        REQUIRE(realm);
    }

    SECTION("downloads latest state for Realms which already exist locally") {
        wait_for_upload(*Realm::get_shared_realm(config));

        {
            auto realm = Realm::get_shared_realm(config2);
            realm->begin_transaction();
            realm->read_group().get_table("class_object")->create_object_with_primary_key(0);
            realm->commit_transaction();
            wait_for_upload(*realm);
        }

        auto realm = successfully_async_open_realm(config);
        REQUIRE(realm->read_group().get_table("class_object")->size() == 1);
    }

    SECTION("can download multiple Realms at a time") {
        SyncTestFile config1(tsm, "realm1");
        SyncTestFile config2(tsm, "realm2");
        SyncTestFile config3(tsm, "realm3");
        SyncTestFile config4(tsm, "realm4");

        std::vector<std::shared_ptr<AsyncOpenTask>> tasks = {
            Realm::get_synchronized_realm(config1),
            Realm::get_synchronized_realm(config2),
            Realm::get_synchronized_realm(config3),
            Realm::get_synchronized_realm(config4),
        };

        std::atomic<int> completed{0};
        for (auto& task : tasks) {
            task->start([&](auto, auto) {
                ++completed;
            });
        }
        util::EventLoop::main().run_until([&] {
            return completed == 4;
        });
    }

    auto expired_token = encode_fake_jwt("", 123, 456);

    SECTION("can async open while waiting for a token refresh") {
        struct User : TestUser {
            using TestUser::TestUser;
            CompletionHandler stored_completion;
            void request_access_token(CompletionHandler&& completion) override
            {
                stored_completion = std::move(completion);
            }
            bool access_token_refresh_required() const override
            {
                return !stored_completion;
            }
        };
        auto user = std::make_shared<User>("realm", tsm.sync_manager());
        SyncTestFile config(user, "realm");
        auto valid_token = user->access_token();
        user->m_access_token = expired_token;

        REQUIRE_FALSE(user->stored_completion);
        std::atomic<bool> called{false};
        auto task = Realm::get_synchronized_realm(config);
        task->start([&](auto ref, auto error) {
            std::lock_guard<std::mutex> lock(mutex);
            REQUIRE(ref);
            REQUIRE(!error);
            called = true;
        });
        REQUIRE(user->stored_completion);
        user->m_access_token = valid_token;
        user->stored_completion({});
        user->stored_completion = {};

        util::EventLoop::main().run_until([&] {
            return called.load();
        });
        std::lock_guard<std::mutex> lock(mutex);
        REQUIRE(called);
    }

    SECTION("cancels download and reports an error on auth error") {
        struct User : TestUser {
            using TestUser::TestUser;
            void request_access_token(CompletionHandler&& completion) override
            {
                completion(app::AppError(ErrorCodes::HTTPError, "403 error", "", 403));
            }
            bool access_token_refresh_required() const override
            {
                return true;
            }
        };
        auto user = std::make_shared<User>("realm", tsm.sync_manager());
        user->m_access_token = expired_token;
        user->m_refresh_token = expired_token;
        SyncTestFile config(user, "realm");

        bool got_error = false;
        config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError) {
            got_error = true;
        };
        std::atomic<bool> called{false};
        auto task = Realm::get_synchronized_realm(config);
        task->start([&](auto ref, auto error) {
            std::lock_guard<std::mutex> lock(mutex);
            REQUIRE(error);
            REQUIRE_EXCEPTION(std::rethrow_exception(error), HTTPError,
                              "Unable to refresh the user access token: 403 error. Client Error: 403");
            REQUIRE(!ref);
            called = true;
        });
        util::EventLoop::main().run_until([&] {
            return called.load();
        });
        std::lock_guard<std::mutex> lock(mutex);
        REQUIRE(called);
        REQUIRE(got_error);
    }

#if REALM_APP_SERVICES

    SECTION("waiters are cancelled if cancel_waits_on_nonfatal_error") {
        auto logger = util::Logger::get_default_logger();
        auto transport = std::make_shared<HookedTransport<UnitTestTransport>>();
        auto socket_provider = std::make_shared<HookedSocketProvider>(logger, "some user agent");
        enum TestMode { expired_at_start, expired_by_websocket, websocket_fails };
        enum FailureMode { location_fails, token_fails, token_not_authorized };
        auto txt_test_mode = [](TestMode mode) {
            switch (mode) {
                case TestMode::expired_at_start:
                    return "access token expired when realm is opened";
                case TestMode::expired_by_websocket:
                    return "access token expired by websocket";
                case TestMode::websocket_fails:
                    return "websocket returns connection failed";
                default:
                    return "Unknown TestMode";
            }
        };
        auto txt_failure_mode = [](FailureMode mode) {
            switch (mode) {
                case FailureMode::location_fails:
                    return "location update fails";
                case FailureMode::token_fails:
                    return "access token refresh fails";
                case FailureMode::token_not_authorized:
                    return "websocket connect not authorized";
                default:
                    return "Unknown FailureMode";
            }
        };

        app::AppConfig app_config;
        set_app_config_defaults(app_config, transport);
        app_config.sync_client_config.socket_provider = socket_provider;
        app_config.base_file_path = util::make_temp_dir();
        app_config.metadata_mode = app::AppConfig::MetadataMode::NoEncryption;

        auto app = app::App::get_app(app::App::CacheMode::Disabled, app_config);
        create_user_and_log_in(app);
        auto user = app->current_user();
        // User should be logged in at this point
        REQUIRE(user->is_logged_in());

        bool not_authorized = false;
        bool token_refresh_called = false;
        bool location_refresh_called = false;

        TestMode test_mode = GENERATE(expired_at_start, expired_by_websocket, websocket_fails);
        FailureMode failure = GENERATE(location_fails, token_fails, token_not_authorized);

        logger->info("TEST: %1 - %2", txt_test_mode(test_mode), txt_failure_mode(failure));
        if (test_mode == TestMode::expired_at_start) {
            // invalidate the user's cached access token
            auto app_user = app->current_user();
            app_user->update_data_for_testing([&](app::UserData& data) {
                data.access_token = RealmJWT(expired_token);
            });
        }
        else if (test_mode == TestMode::expired_by_websocket) {
            // tell websocket to return not authorized to refresh access token
            not_authorized = true;
        }

        app.reset();

        auto err_handler = [](std::shared_ptr<SyncSession> session, SyncError error) {
            auto logger = util::Logger::get_default_logger();
            logger->debug("The sync error handler caught an error: '%1' for '%2'", error.status, session->path());
            // Ignore connection failed non-fatal errors and check for access token refresh unauthorized fatal errors
            if (error.status.code() == ErrorCodes::SyncConnectFailed) {
                REQUIRE_FALSE(error.is_fatal);
                return;
            }
            // If it's not SyncConnectFailed, then it should be AuthError
            REQUIRE(error.status.code() == ErrorCodes::AuthError);
            REQUIRE(error.is_fatal);
        };

        transport->request_hook = [&](const app::Request& req) -> std::optional<app::Response> {
            static constexpr int CURLE_OPERATION_TIMEDOUT = 28;
            std::lock_guard<std::mutex> lock(mutex);
            if (req.url.find("/auth/session") != std::string::npos) {
                token_refresh_called = true;
                if (failure == FailureMode::token_not_authorized) {
                    return app::Response{403, 0, {}, "403 not authorized"};
                }
                if (failure == FailureMode::token_fails) {
                    return app::Response{0, CURLE_OPERATION_TIMEDOUT, {}, "Operation timed out"};
                }
            }
            else if (req.url.find("/location") != std::string::npos) {
                location_refresh_called = true;
                if (failure == FailureMode::location_fails) {
                    // Fake "offline/request timed out" custom error response
                    return app::Response{0, CURLE_OPERATION_TIMEDOUT, {}, "Operation timed out"};
                }
            }
            return std::nullopt;
        };

        socket_provider->websocket_connect_func = [&]() -> std::optional<SocketProviderError> {
            if (not_authorized) {
                not_authorized = false; // one shot
                return SocketProviderError(sync::websocket::WebSocketError::websocket_unauthorized,
                                           "403 not authorized");
            }
            return SocketProviderError(sync::websocket::WebSocketError::websocket_connection_failed,
                                       "Operation timed out");
        };

        app = app::App::get_app(app::App::CacheMode::Disabled, app_config);
        SyncTestFile config(app->current_user(), "realm");
        config.sync_config->cancel_waits_on_nonfatal_error = true;
        config.sync_config->error_handler = err_handler;

        // User should be logged in at this point
        REQUIRE(config.sync_config->user->is_logged_in());

        auto status = async_open_realm(config);
        REQUIRE_FALSE(status.is_ok());

        {
            std::lock_guard lock(mutex);
            REQUIRE(location_refresh_called);
            if (failure != FailureMode::location_fails) {
                REQUIRE(token_refresh_called);
            }
        }

        app->sync_manager()->tear_down_for_testing();
    }

#endif // REALM_APP_SERVICES

    SECTION("read-only mode sets the schema version") {
        {
            SharedRealm realm = Realm::get_shared_realm(config);
            wait_for_upload(*realm);
            realm->close();
        }

        config2.schema_mode = SchemaMode::ReadOnly;
        auto realm = successfully_async_open_realm(config2);
        REQUIRE(realm->schema_version() == 1);
    }

    Schema with_added_object = Schema{object_schema,
                                      {"added",
                                       {
                                           {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                       }}};

    SECTION("read-only mode applies remote schema changes") {
        // Create the local file without "added"
        Realm::get_shared_realm(config2);

        // Add the table server-side
        config.schema = with_added_object;
        config2.schema = with_added_object;
        {
            SharedRealm realm = Realm::get_shared_realm(config);
            wait_for_upload(*realm);
            realm->close();
        }

        // Verify that the table gets added when reopening
        config2.schema_mode = SchemaMode::ReadOnly;
        auto realm = successfully_async_open_realm(config2);
        REQUIRE(realm->schema().find("added") != realm->schema().end());
        REQUIRE(realm->read_group().get_table("class_added"));
    }

    SECTION("read-only mode does not create tables not present on the server") {
        // Create the local file without "added"
        Realm::get_shared_realm(config2);

        config2.schema = with_added_object;
        config2.schema_mode = SchemaMode::ReadOnly;
        auto realm = successfully_async_open_realm(config2);
        REQUIRE(realm->schema().find("added") != realm->schema().end());
        REQUIRE_FALSE(realm->read_group().get_table("class_added"));
    }

    SECTION("adding a property to a newly downloaded read-only Realm reports an error") {
        // Create the Realm on the server
        wait_for_upload(*Realm::get_shared_realm(config2));

        config.schema_mode = SchemaMode::ReadOnly;
        config.schema = Schema{{"object",
                                {
                                    {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                    {"value", PropertyType::Int},
                                    {"value2", PropertyType::Int},
                                }}};

        auto status = async_open_realm(config);
        REQUIRE_FALSE(status.is_ok());
        REQUIRE_THAT(status.get_status().reason(),
                     Catch::Matchers::ContainsSubstring("Property 'object.value2' has been added."));
    }

    SECTION("adding a property to an existing read-only Realm reports an error") {
        Realm::get_shared_realm(config);

        config.schema_mode = SchemaMode::ReadOnly;
        config.schema = Schema{{"object",
                                {
                                    {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                    {"value", PropertyType::Int},
                                    {"value2", PropertyType::Int},
                                }}};
        REQUIRE_THROWS_CONTAINING(Realm::get_shared_realm(config), "Property 'object.value2' has been added.");

        auto status = async_open_realm(config);
        REQUIRE_FALSE(status.is_ok());
        REQUIRE_THAT(status.get_status().reason(),
                     Catch::Matchers::ContainsSubstring("Property 'object.value2' has been added."));
    }

    SECTION("removing a property from a newly downloaded read-only Realm leaves the column in place") {
        // Create the Realm on the server
        wait_for_upload(*Realm::get_shared_realm(config2));

        // Remove the "value" property from the schema
        config.schema_mode = SchemaMode::ReadOnly;
        config.schema = Schema{{"object",
                                {
                                    {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                }}};

        auto realm = successfully_async_open_realm(config);
        REQUIRE(realm->read_group().get_table("class_object")->get_column_key("value") != ColKey{});
    }

    SECTION("removing a property from a existing read-only Realm leaves the column in place") {
        Realm::get_shared_realm(config);

        // Remove the "value" property from the schema
        config.schema_mode = SchemaMode::ReadOnly;
        config.schema = Schema{{"object",
                                {
                                    {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                }}};

        auto realm = successfully_async_open_realm(config);
        REQUIRE(realm->read_group().get_table("class_object")->get_column_key("value") != ColKey{});
    }

    _impl::RealmCoordinator::assert_no_open_realms();
}

#if REALM_ENABLE_AUTH_TESTS

TEST_CASE("Synchronized realm: AutoOpen", "[sync][baas][pbs][async open]") {
    const auto partition = random_string(100);
    auto schema = get_default_schema();
    enum TestMode { expired_at_start, expired_by_websocket, websocket_fails };
    enum FailureMode { location_fails, token_fails, token_not_authorized };

    auto logger = util::Logger::get_default_logger();
    auto transport = std::make_shared<HookedTransport<>>();
    auto socket_provider = std::make_shared<HookedSocketProvider>(logger, "some user agent");
    std::mutex mutex;

    // Create the app session and get the logged in user identity
    auto app_session = create_app(minimal_app_config("autoopen-realm", schema));
    std::string identity;
    TestAppSession::Config tas_config;
    {
        // Keep the app and realm storage
        TestAppSession session(app_session, {transport, realm::ReconnectMode::normal, socket_provider},
                               DeleteApp{false}, false);
        auto user = session.current_user();
        REQUIRE(user);
        REQUIRE(user->is_logged_in());
        identity = user->user_id();
        tas_config = session.config(); // get config with storage path and user creds populated
    }
    REQUIRE_FALSE(identity.empty());
    TestAppSession session(app_session, tas_config);
    auto user = session.app()->get_existing_logged_in_user(identity);

    SyncTestFile config(user, partition, schema);
    config.sync_config->cancel_waits_on_nonfatal_error = true;
    config.sync_config->error_handler = [&logger](std::shared_ptr<SyncSession> session, SyncError error) {
        logger->debug("The sync error handler caught an error: '%1' for '%2'", error.status, session->path());
        // Ignore connection failed non-fatal errors and check for access token refresh unauthorized fatal errors
        if (error.status.code() == ErrorCodes::SyncConnectFailed) {
            REQUIRE_FALSE(error.is_fatal);
            return;
        }
        // If it's not SyncConnectFailed, then it should be AuthError
        REQUIRE(error.status.code() == ErrorCodes::AuthError);
        REQUIRE(error.is_fatal);
    };

    bool not_authorized = false;
    bool token_refresh_called = false;
    bool location_refresh_called = false;

    FailureMode failure = FailureMode::location_fails;

    transport->request_hook = [&](const app::Request& req) -> std::optional<app::Response> {
        static constexpr int CURLE_OPERATION_TIMEDOUT = 28;
        std::lock_guard<std::mutex> lock(mutex);
        if (req.url.find("/auth/session") != std::string::npos) {
            token_refresh_called = true;
            if (failure == FailureMode::token_not_authorized) {
                return app::Response{403, 0, {}, "403 not authorized"};
            }
            if (failure == FailureMode::token_fails) {
                return app::Response{0, CURLE_OPERATION_TIMEDOUT, {}, "Operation timed out"};
            }
        }
        else if (req.url.find("/location") != std::string::npos) {
            location_refresh_called = true;
            if (failure == FailureMode::location_fails) {
                // Fake "offline/request timed out" custom error response
                return app::Response{0, CURLE_OPERATION_TIMEDOUT, {}, "Operation timed out"};
            }
        }
        return std::nullopt;
    };

    socket_provider->websocket_connect_func = [&]() -> std::optional<SocketProviderError> {
        if (not_authorized) {
            not_authorized = false; // one shot
            return SocketProviderError(sync::websocket::WebSocketError::websocket_unauthorized, "403 not authorized");
        }
        return SocketProviderError(sync::websocket::WebSocketError::websocket_connection_failed,
                                   "Operation timed out");
    };

    auto task = Realm::get_synchronized_realm(config);
    auto pf = util::make_promise_future<std::exception_ptr>();
    task->start([&pf](auto ref, auto error) mutable {
        REQUIRE(!ref);
        REQUIRE(error);
        pf.promise.emplace_value(error);
    });

    auto result = pf.future.get_no_throw();
    REQUIRE(result.is_ok());
    REQUIRE(result.get_value());
    {
        std::lock_guard<std::mutex> lock(mutex);
        REQUIRE(location_refresh_called);
        if (failure != FailureMode::location_fails) {
            REQUIRE(token_refresh_called);
        }
    }

    transport->request_hook = nullptr;
    socket_provider->websocket_connect_func = nullptr;
    auto r = Realm::get_shared_realm(config);
    wait_for_download(*r);
}

#endif // REALM_ENABLE_AUTH_TESTS

TEST_CASE("SharedRealm: convert", "[sync][pbs][convert]") {
    TestSyncManager tsm;
    ObjectSchema object_schema = {"object",
                                  {
                                      {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                      {"value", PropertyType::Int},
                                  }};
    Schema schema{object_schema};

    SyncTestFile sync_config1(tsm, "default");
    sync_config1.schema = schema;
    TestFile local_config1;
    local_config1.schema = schema;
    local_config1.schema_version = sync_config1.schema_version;

    SECTION("can copy a synced realm to a synced realm") {
        auto sync_realm1 = Realm::get_shared_realm(sync_config1);
        sync_realm1->begin_transaction();
        sync_realm1->read_group().get_table("class_object")->create_object_with_primary_key(0);
        sync_realm1->commit_transaction();
        wait_for_upload(*sync_realm1);
        wait_for_download(*sync_realm1);

        // Copy to a new sync config
        SyncTestFile sync_config2(tsm, "default");
        sync_config2.schema = schema;

        sync_realm1->convert(sync_config2);

        auto sync_realm2 = Realm::get_shared_realm(sync_config2);

        // Check that the data also exists in the new realm
        REQUIRE(sync_realm2->read_group().get_table("class_object")->size() == 1);

        // Verify that sync works and objects created in the new copy will get
        // synchronized to the old copy
        sync_realm2->begin_transaction();
        sync_realm2->read_group().get_table("class_object")->create_object_with_primary_key(1);
        sync_realm2->commit_transaction();
        wait_for_upload(*sync_realm2);
        wait_for_download(*sync_realm1);

        sync_realm1->refresh();
        REQUIRE(sync_realm1->read_group().get_table("class_object")->size() == 2);
    }

    SECTION("can convert a synced realm to a local realm") {
        auto sync_realm = Realm::get_shared_realm(sync_config1);
        sync_realm->begin_transaction();
        sync_realm->read_group().get_table("class_object")->create_object_with_primary_key(0);
        sync_realm->commit_transaction();
        wait_for_upload(*sync_realm);
        wait_for_download(*sync_realm);

        sync_realm->convert(local_config1);

        auto local_realm = Realm::get_shared_realm(local_config1);

        // Check that the data also exists in the new realm
        REQUIRE(local_realm->read_group().get_table("class_object")->size() == 1);
    }

    SECTION("can convert a local realm to a synced realm") {
        auto local_realm = Realm::get_shared_realm(local_config1);
        local_realm->begin_transaction();
        local_realm->read_group().get_table("class_object")->create_object_with_primary_key(0);
        local_realm->commit_transaction();

        // Copy to a new sync config
        local_realm->convert(sync_config1);

        auto sync_realm = Realm::get_shared_realm(sync_config1);

        // Check that the data also exists in the new realm
        REQUIRE(sync_realm->read_group().get_table("class_object")->size() == 1);
    }

    SECTION("can copy a local realm to a local realm") {
        auto local_realm1 = Realm::get_shared_realm(local_config1);
        local_realm1->begin_transaction();
        local_realm1->read_group().get_table("class_object")->create_object_with_primary_key(0);
        local_realm1->commit_transaction();

        // Copy to a new local config
        TestFile local_config2;
        local_config2.schema = schema;
        local_config2.schema_version = local_config1.schema_version;
        local_realm1->convert(local_config2);

        auto local_realm2 = Realm::get_shared_realm(local_config2);

        // Check that the data also exists in the new realm
        REQUIRE(local_realm2->read_group().get_table("class_object")->size() == 1);
    }

    SECTION("synced realm must be fully uploaded") {
        auto realm = Realm::get_shared_realm(sync_config1);
        realm->sync_session()->pause();
        realm->begin_transaction();
        realm->read_group().get_table("class_object")->create_object_with_primary_key(0);
        realm->commit_transaction();

        SyncTestFile sync_config2(tsm, "default");
        sync_config2.schema = schema;
        REQUIRE_EXCEPTION(realm->convert(sync_config2), IllegalOperation,
                          "All client changes must be integrated in server before writing copy");

        realm->sync_session()->resume();
        wait_for_upload(*realm);
        REQUIRE_NOTHROW(realm->convert(sync_config2));
    }

    SECTION("can convert synced realm from within upload complete callback") {
        auto realm = Realm::get_shared_realm(sync_config1);
        realm->sync_session()->pause();
        realm->begin_transaction();
        realm->read_group().get_table("class_object")->create_object_with_primary_key(0);
        realm->commit_transaction();

        SyncTestFile sync_config2(tsm, "default");
        sync_config2.schema = schema;
        auto pf = util::make_promise_future();
        realm->sync_session()->wait_for_upload_completion([&](Status) {
            sync_config1.scheduler = util::Scheduler::make_dummy();
            auto realm = Realm::get_shared_realm(sync_config1);
            REQUIRE_NOTHROW(realm->convert(sync_config2));
            pf.promise.emplace_value();
        });
        realm->sync_session()->resume();
        pf.future.get();
    }
}

TEST_CASE("SharedRealm: convert - embedded objects", "[sync][pbs][convert][embedded objects]") {
    TestSyncManager tsm;
    ObjectSchema object_schema = {"object",
                                  {
                                      {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                      {"value", PropertyType::Int},
                                      {"embedded_link", PropertyType::Object | PropertyType::Nullable, "embedded"},
                                  }};
    ObjectSchema embedded_schema = {"embedded",
                                    ObjectSchema::ObjectType::Embedded,
                                    {
                                        {"name", PropertyType::String | PropertyType::Nullable},
                                    }};
    Schema schema{object_schema, embedded_schema};

    SyncTestFile sync_config1(tsm, "default");
    sync_config1.schema = schema;
    TestFile local_config1;
    local_config1.schema = schema;
    local_config1.schema_version = sync_config1.schema_version;

    SECTION("can copy a synced realm to a synced realm") {
        auto sync_realm1 = Realm::get_shared_realm(sync_config1);
        sync_realm1->begin_transaction();

        SECTION("null embedded object") {
            sync_realm1->read_group().get_table("class_object")->create_object_with_primary_key(0);
        }

        SECTION("embedded object") {
            auto obj = sync_realm1->read_group().get_table("class_object")->create_object_with_primary_key(0);
            auto col_key = sync_realm1->read_group().get_table("class_object")->get_column_key("embedded_link");
            obj.create_and_set_linked_object(col_key);
        }

        sync_realm1->commit_transaction();
        wait_for_upload(*sync_realm1);
        wait_for_download(*sync_realm1);

        // Copy to a new sync config
        SyncTestFile sync_config2(tsm, "default");
        sync_config2.schema = schema;

        sync_realm1->convert(sync_config2);

        auto sync_realm2 = Realm::get_shared_realm(sync_config2);

        // Check that the data also exists in the new realm
        REQUIRE(sync_realm2->read_group().get_table("class_object")->size() == 1);

        // Verify that sync works and objects created in the new copy will get
        // synchronized to the old copy
        sync_realm2->begin_transaction();
        sync_realm2->read_group().get_table("class_object")->create_object_with_primary_key(1);
        sync_realm2->commit_transaction();
        wait_for_upload(*sync_realm2);
        wait_for_download(*sync_realm1);

        sync_realm1->refresh();
        REQUIRE(sync_realm1->read_group().get_table("class_object")->size() == 2);
    }

    SECTION("can convert a synced realm to a local realm") {
        auto sync_realm = Realm::get_shared_realm(sync_config1);
        sync_realm->begin_transaction();

        SECTION("null embedded object") {
            sync_realm->read_group().get_table("class_object")->create_object_with_primary_key(0);
        }

        SECTION("embedded object") {
            auto obj = sync_realm->read_group().get_table("class_object")->create_object_with_primary_key(0);
            auto col_key = sync_realm->read_group().get_table("class_object")->get_column_key("embedded_link");
            obj.create_and_set_linked_object(col_key);
        }

        sync_realm->commit_transaction();
        wait_for_upload(*sync_realm);
        wait_for_download(*sync_realm);

        sync_realm->convert(local_config1);

        auto local_realm = Realm::get_shared_realm(local_config1);

        // Check that the data also exists in the new realm
        REQUIRE(local_realm->read_group().get_table("class_object")->size() == 1);
    }

    SECTION("can convert a local realm to a synced realm") {
        auto local_realm = Realm::get_shared_realm(local_config1);
        local_realm->begin_transaction();

        SECTION("null embedded object") {
            local_realm->read_group().get_table("class_object")->create_object_with_primary_key(0);
        }

        SECTION("embedded object") {
            auto obj = local_realm->read_group().get_table("class_object")->create_object_with_primary_key(0);
            auto col_key = local_realm->read_group().get_table("class_object")->get_column_key("embedded_link");
            obj.create_and_set_linked_object(col_key);
        }

        local_realm->commit_transaction();

        // Copy to a new sync config
        local_realm->convert(sync_config1);

        auto sync_realm = Realm::get_shared_realm(sync_config1);

        // Check that the data also exists in the new realm
        REQUIRE(sync_realm->read_group().get_table("class_object")->size() == 1);
    }

    SECTION("can copy a local realm to a local realm") {
        auto local_realm1 = Realm::get_shared_realm(local_config1);
        local_realm1->begin_transaction();

        SECTION("null embedded object") {
            local_realm1->read_group().get_table("class_object")->create_object_with_primary_key(0);
        }

        SECTION("embedded object") {
            auto obj = local_realm1->read_group().get_table("class_object")->create_object_with_primary_key(0);
            auto col_key = local_realm1->read_group().get_table("class_object")->get_column_key("embedded_link");
            obj.create_and_set_linked_object(col_key);
        }

        local_realm1->commit_transaction();

        // Copy to a new local config
        TestFile local_config2;
        local_config2.schema = schema;
        local_config2.schema_version = local_config1.schema_version;
        local_realm1->convert(local_config2);

        auto local_realm2 = Realm::get_shared_realm(local_config2);

        // Check that the data also exists in the new realm
        REQUIRE(local_realm2->read_group().get_table("class_object")->size() == 1);
    }
}
#endif // REALM_ENABLE_SYNC

TEST_CASE("SharedRealm: async writes") {
    _impl::RealmCoordinator::assert_no_open_realms();
    if (!util::EventLoop::has_implementation())
        return;

    TestFile config;
    config.schema_version = 0;
    config.schema = Schema{
        {"object",
         {
             {"value", PropertyType::Int},
             {"ints", PropertyType::Array | PropertyType::Int},
             {"int set", PropertyType::Set | PropertyType::Int},
             {"int dictionary", PropertyType::Dictionary | PropertyType::Int},
         }},
    };
    bool done = false;
    auto realm = Realm::get_shared_realm(config);
    auto table = realm->read_group().get_table("class_object");
    auto col = table->get_column_key("value");
    int write_nr = 0;
    int commit_nr = 0;

    auto wait_for_done = [&]() {
        util::EventLoop::main().run_until([&] {
            return done;
        });
        REQUIRE(done);
    };

    SECTION("async commit transaction") {
        realm->async_begin_transaction([&]() {
            REQUIRE(write_nr == 0);
            ++write_nr;
            table->create_object().set(col, 45);
            realm->async_commit_transaction([&](std::exception_ptr) {
                REQUIRE(commit_nr == 0);
                ++commit_nr;
            });
        });
        for (int expected = 1; expected < 1000; ++expected) {
            realm->async_begin_transaction([&, expected]() {
                REQUIRE(write_nr == expected);
                ++write_nr;
                auto o = table->get_object(0);
                o.set(col, o.get<int64_t>(col) + 37);
                realm->async_commit_transaction(
                    [&](auto) {
                        ++commit_nr;
                        done = commit_nr == 1000;
                    },
                    true);
            });
        }
        wait_for_done();
    }

    auto verify_persisted_count = [&](size_t expected) {
        if (realm)
            realm->close();
        _impl::RealmCoordinator::assert_no_open_realms();

        auto new_realm = Realm::get_shared_realm(config);
        auto table = new_realm->read_group().get_table("class_object");
        REQUIRE(table->size() == expected);
    };

    using RealmCloseFunction = void (Realm::*)();
    static RealmCloseFunction close_functions[] = {&Realm::close, &Realm::invalidate};
    static const char* close_function_names[] = {"close()", "invalidate()"};
    for (int i = 0; i < 2; ++i) {
        SECTION(close_function_names[i]) {
            bool persisted = false;
            SECTION("before write lock is acquired") {
                DBOptions options;
                options.encryption_key = config.encryption_key.data();
                // Acquire the write lock with a different DB instance so that we'll
                // be stuck in the Requesting stage
                realm::test_util::BowlOfStonesSemaphore sema;
                JoiningThread thread([&] {
                    auto db = DB::create(make_in_realm_history(), config.path, options);
                    auto write = db->start_write();
                    sema.add_stone();

                    // Wait until the main thread is waiting for the lock.
                    while (!db->other_writers_waiting_for_lock()) {
                        millisleep(1);
                    }
                    write->close();
                });

                // Wait for the background thread to have acquired the lock
                sema.get_stone();

                auto scheduler = realm->scheduler();
                realm->async_begin_transaction([&] {
                    // We should never get here as the realm is closed
                    FAIL();
                });

                // close() should block until we can acquire the write lock
                std::invoke(close_functions[i], *realm);

                {
                    // Verify that we released the write lock
                    auto db = DB::create(make_in_realm_history(), config.path, options);
                    REQUIRE(db->start_write(/* nonblocking */ true));
                }

                // Verify that the transaction callback never got enqueued
                scheduler->invoke([&] {
                    done = true;
                });
                wait_for_done();
            }
            SECTION("before async_begin_transaction() callback") {
                auto scheduler = realm->scheduler();
                realm->async_begin_transaction([&] {
                    // We should never get here as the realm is closed
                    FAIL();
                });
                std::invoke(close_functions[i], *realm);
                scheduler->invoke([&] {
                    done = true;
                });
                wait_for_done();
                verify_persisted_count(0);
            }
            SECTION("inside async_begin_transaction() callback before commit") {
                realm->async_begin_transaction([&] {
                    table->create_object().set(col, 45);
                    std::invoke(close_functions[i], *realm);
                    done = true;
                });
                wait_for_done();
                verify_persisted_count(0);
            }
            SECTION("inside async_begin_transaction() callback after sync commit") {
                realm->async_begin_transaction([&] {
                    table->create_object().set(col, 45);
                    realm->commit_transaction();
                    std::invoke(close_functions[i], *realm);
                    done = true;
                });
                wait_for_done();
                verify_persisted_count(1);
            }
            SECTION("inside async_begin_transaction() callback after async commit") {
                realm->async_begin_transaction([&] {
                    table->create_object().set(col, 45);
                    realm->async_commit_transaction([&](std::exception_ptr) {
                        persisted = true;
                    });
                    std::invoke(close_functions[i], *realm);
                    REQUIRE(persisted);
                    done = true;
                });
                wait_for_done();
                verify_persisted_count(1);
            }
            SECTION("inside async commit completion") {
                realm->async_begin_transaction([&] {
                    table->create_object().set(col, 45);
                    realm->async_commit_transaction([&](std::exception_ptr) {
                        done = true;
                        std::invoke(close_functions[i], *realm);
                    });
                });
                wait_for_done();
                verify_persisted_count(1);
            }
            SECTION("between commit and sync") {
                realm->async_begin_transaction([&] {
                    table->create_object().set(col, 45);
                    realm->async_commit_transaction([&](std::exception_ptr) {
                        persisted = true;
                    });
                    done = true;
                });
                wait_for_done();
                std::invoke(close_functions[i], *realm);
                REQUIRE(persisted);
                verify_persisted_count(1);
            }
            SECTION("with multiple pending commits") {
                int complete_count = 0;
                realm->async_begin_transaction([&] {
                    table->create_object().set(col, 45);
                    realm->async_commit_transaction([&](std::exception_ptr) {
                        ++complete_count;
                    });
                });
                realm->async_begin_transaction([&] {
                    table->create_object().set(col, 45);
                    realm->async_commit_transaction(
                        [&](auto) {
                            ++complete_count;
                        },
                        true);
                });
                realm->async_begin_transaction([&] {
                    table->create_object().set(col, 45);
                    realm->async_commit_transaction(
                        [&](auto) {
                            ++complete_count;
                        },
                        true);
                    done = true;
                });

                wait_for_done();
                std::invoke(close_functions[i], *realm);
                REQUIRE(complete_count == 3);
                verify_persisted_count(3);
            }
            SECTION("inside async_begin_transaction() with pending commits") {
                int complete_count = 0;
                realm->async_begin_transaction([&] {
                    table->create_object().set(col, 45);
                    realm->async_commit_transaction([&](std::exception_ptr) {
                        ++complete_count;
                    });
                });
                realm->async_begin_transaction([&] {
                    // This create should be discarded
                    table->create_object().set(col, 45);
                    std::invoke(close_functions[i], *realm);
                    done = true;
                });

                wait_for_done();
                std::invoke(close_functions[i], *realm);
                REQUIRE(complete_count == 1);
                verify_persisted_count(1);
            }

            struct Context : public BindingContext {
                int i;
                bool& called;
                Context(int i, bool& called)
                    : i(i)
                    , called(called)
                {
                }
                void did_change(std::vector<ObserverState> const&, std::vector<void*> const&, bool) override
                {
                    called = true;
                    std::invoke(close_functions[i], *realm.lock());
                }
            };
            SECTION("within did_change() after committing") {
                bool called = false;
                realm->m_binding_context.reset(new Context(i, called));
                realm->m_binding_context->realm = realm;

                realm->async_begin_transaction([&] {
                    table->create_object().set(col, 45);
                    CHECK_FALSE(called);
                    realm->async_commit_transaction([&](std::exception_ptr) {
                        CHECK(called);
                        done = true;
                    });
                });

                wait_for_done();
                verify_persisted_count(1);
            }

            SECTION("within did_change() when beginning") {
                realm->m_binding_context.reset(new Context(i, done));
                realm->m_binding_context->realm = realm;

                // Make a write on a different instance while autorefresh is
                // off to ensure that beginning the transaction advances the
                // read version and thus sends notifications
                realm->set_auto_refresh(false);
                auto realm2 = Realm::get_shared_realm(config);
                realm2->begin_transaction();
                realm2->commit_transaction();

                bool called = false;
                realm->async_begin_transaction([&] {
                    called = true;
                });
                wait_for_done();

                // close() inside a notification closes the Realm, but invalidate()
                // is a no-op. This means the write callback should be invoked
                // if we're testing invalidate() but not if we're testing close().
                REQUIRE(called == i);
            }
        }
    }

    SECTION("notify only with no further actions") {
        realm->async_begin_transaction(
            [&] {
                done = true;
            },
            true);
        wait_for_done();
        realm->cancel_transaction();
    }
    SECTION("notify only with synchronous commit") {
        realm->async_begin_transaction(
            [&] {
                done = true;
            },
            true);
        wait_for_done();
        table->create_object();
        realm->commit_transaction();
    }
    SECTION("schedule async commits after notify only") {
        realm->async_begin_transaction(
            [&] {
                done = true;
            },
            true);
        wait_for_done();
        done = false;
        realm->async_begin_transaction([&] {
            table->create_object();
            done = true;
            realm->commit_transaction();
        });
        table->create_object();
        realm->commit_transaction();
        REQUIRE(table->size() == 1);
        wait_for_done();
        REQUIRE(table->size() == 2);
    }
    SECTION("exception thrown during transaction with error handler") {
        Realm::AsyncHandle h = 7;
        bool called = false;
        realm->set_async_error_handler([&](Realm::AsyncHandle handle, std::exception_ptr error) {
            REQUIRE(error);
            REQUIRE_THROWS_CONTAINING(std::rethrow_exception(error), "an error");
            CHECK(handle == h);
            called = true;
        });
        h = realm->async_begin_transaction([&] {
            table->create_object();
            done = true;
            throw std::runtime_error("an error");
        });
        wait_for_done();

        // Transaction should have been rolled back
        REQUIRE_FALSE(realm->is_in_transaction());
        REQUIRE(table->size() == 0);
        REQUIRE(called);

        // Should be able to perform another write afterwards
        done = false;
        called = false;
        h = realm->async_begin_transaction([&] {
            table->create_object();
            realm->commit_transaction();
            done = true;
        });
        wait_for_done();
        REQUIRE(table->size() == 1);
        REQUIRE_FALSE(called);
    }
#ifndef _WIN32
    SECTION("exception thrown during transaction without error handler") {
        realm->set_async_error_handler(nullptr);
        realm->async_begin_transaction([&] {
            table->create_object();
            throw std::runtime_error("an error");
        });
        REQUIRE_THROWS_CONTAINING(util::EventLoop::main().run_until([&] {
            return false;
        }),
                                  "an error");

        // Transaction should have been rolled back
        REQUIRE_FALSE(realm->is_in_transaction());
        REQUIRE(table->size() == 0);

        // Should be able to perform another write afterwards
        realm->async_begin_transaction([&, realm] {
            table->create_object();
            realm->commit_transaction();
            done = true;
        });
        wait_for_done();
        REQUIRE(table->size() == 1);
    }
    SECTION("exception thrown during transaction without error handler after closing Realm") {
        realm->set_async_error_handler(nullptr);
        realm->async_begin_transaction([&] {
            realm->close();
            throw std::runtime_error("an error");
        });
        REQUIRE_THROWS_CONTAINING(util::EventLoop::main().run_until([&] {
            return false;
        }),
                                  "an error");
        REQUIRE(realm->is_closed());
    }
#endif
    SECTION("exception thrown from async commit completion callback with error handler") {
        Realm::AsyncHandle h;
        realm->set_async_error_handler([&](Realm::AsyncHandle handle, std::exception_ptr error) {
            REQUIRE(error);
            REQUIRE_THROWS_CONTAINING(std::rethrow_exception(error), "an error");
            CHECK(handle == h);
            done = true;
        });

        realm->begin_transaction();
        table->create_object();
        h = realm->async_commit_transaction([&](std::exception_ptr) {
            throw std::runtime_error("an error");
        });
        wait_for_done();
        verify_persisted_count(1);
    }
#ifndef _WIN32
    SECTION("exception thrown from async commit completion callback without error handler") {
        realm->begin_transaction();
        table->create_object();
        realm->async_commit_transaction([&](std::exception_ptr) {
            throw std::runtime_error("an error");
        });
        REQUIRE_THROWS_CONTAINING(util::EventLoop::main().run_until([&] {
            return false;
        }),
                                  "an error");
        REQUIRE(table->size() == 1);
    }
#endif

    if (_impl::SimulatedFailure::is_enabled()) {
        SECTION("error in the synchronous part of async commit") {
            realm->begin_transaction();
            table->create_object();

            using sf = _impl::SimulatedFailure;
            sf::OneShotPrimeGuard pg(sf::shared_group__grow_reader_mapping);
            REQUIRE_THROWS_AS(realm->async_commit_transaction([&](std::exception_ptr) {
                FAIL("should not call completion");
            }),
                              _impl::SimulatedFailure);
            REQUIRE_FALSE(realm->is_in_transaction());
        }
        SECTION("error in the async part of async commit") {
            realm->begin_transaction();
            table->create_object();

            using sf = _impl::SimulatedFailure;
            sf::set_thread_local(false);
            sf::OneShotPrimeGuard pg(sf::group_writer__commit);
            realm->async_commit_transaction([&](std::exception_ptr e) {
                REQUIRE(e);
                REQUIRE_THROWS_AS(std::rethrow_exception(e), _impl::SimulatedFailure);
                done = true;
            });
            wait_for_done();
            sf::set_thread_local(true);
        }
    }
    SECTION("throw exception from did_change()") {
        struct Context : public BindingContext {
            void did_change(std::vector<ObserverState> const&, std::vector<void*> const&, bool) override
            {
                throw std::runtime_error("expected error");
            }
        };
        realm->m_binding_context.reset(new Context);

        realm->begin_transaction();
        auto table = realm->read_group().get_table("class_object");
        table->create_object();
        REQUIRE_THROWS_WITH(realm->async_commit_transaction([&](std::exception_ptr) {
            done = true;
        }),
                            "expected error");
        wait_for_done();
    }
    SECTION("cancel scheduled async transaction") {
        auto handle = realm->async_begin_transaction([&, realm]() {
            table->create_object().set(col, 45);
            realm->async_commit_transaction(
                [&](auto) {
                    done = true;
                },
                true);
        });
        realm->async_begin_transaction([&, realm]() {
            table->create_object().set(col, 90);
            realm->async_commit_transaction(
                [&](auto) {
                    done = true;
                },
                true);
        });
        realm->async_cancel_transaction(handle);
        wait_for_done();
        auto table = realm->read_group().get_table("class_object");
        REQUIRE(table->size() == 1);
        REQUIRE(table->begin()->get<Int>("value") == 90);
    }
    SECTION("synchronous cancel inside async transaction") {
        realm->async_begin_transaction([&, realm]() {
            REQUIRE(table->size() == 0);
            table->create_object().set(col, 45);
            REQUIRE(table->size() == 1);
            realm->cancel_transaction();
            REQUIRE(table->size() == 0);
            done = true;
        });
        wait_for_done();
    }
    SECTION("synchronous commit of async transaction after async commit which allows grouping") {
        realm->async_begin_transaction([&, realm]() {
            table->create_object().set(col, 45);
            realm->async_commit_transaction(
                [&](auto) {
                    done = true;
                },
                true);
        });
        realm->async_begin_transaction([&, realm]() {
            table->create_object().set(col, 45);
            realm->commit_transaction();
        });
        wait_for_done();
        auto table = realm->read_group().get_table("class_object");
        REQUIRE(table->size() == 2);
    }
    SECTION("synchronous transaction after async transaction with no commit") {
        realm->async_begin_transaction([&]() {
            table->create_object().set(col, 80);
            done = true;
        });
        wait_for_done();
        realm->begin_transaction();
        table->create_object().set(col, 90);
        realm->commit_transaction();
        verify_persisted_count(1);
    }
    SECTION("synchronous transaction with scheduled async transaction with no commit") {
        realm->async_begin_transaction([&]() {
            table->create_object().set(col, 80);
            done = true;
        });
        realm->begin_transaction();
        table->create_object().set(col, 90);
        realm->commit_transaction();
        wait_for_done();
        verify_persisted_count(1);
    }
    SECTION("synchronous transaction with scheduled async transaction") {
        realm->async_begin_transaction([&, realm]() {
            table->create_object().set(col, 80);
            realm->commit_transaction();
            done = true;
        });
        realm->begin_transaction();
        table->create_object().set(col, 90);
        realm->commit_transaction();
        wait_for_done();
        REQUIRE(table->size() == 2);
        REQUIRE(table->get_object(0).get<Int>(col) == 90);
        REQUIRE(table->get_object(1).get<Int>(col) == 80);
    }
    SECTION("synchronous transaction with async write") {
        realm->begin_transaction();
        table->create_object().set(col, 45);
        realm->async_commit_transaction();

        realm->begin_transaction();
        table->create_object().set(col, 90);
        realm->async_commit_transaction([&](std::exception_ptr) {
            done = true;
        });
        wait_for_done();
        verify_persisted_count(2);
    }
    SECTION("synchronous transaction mixed with async transactions") {
        realm->async_begin_transaction([&, realm]() {
            table->create_object().set(col, 45);
            done = true;
            realm->async_commit_transaction();
        });
        realm->async_begin_transaction([&, realm]() {
            table->create_object().set(col, 45);
            realm->async_commit_transaction([&](std::exception_ptr) {
                done = true;
            });
        });
        wait_for_done();
        realm->begin_transaction(); // Here syncing of first async tr has not completed
        REQUIRE(table->size() == 1);
        table->create_object().set(col, 90);
        realm->commit_transaction(); // Will re-initiate async writes

        done = false;
        wait_for_done();
        verify_persisted_count(3);
    }
    SECTION("asynchronous transaction mixed with sync transaction that is cancelled") {
        bool persisted = false;
        realm->async_begin_transaction([&, realm]() {
            table->create_object().set(col, 45);
            done = true;
            realm->async_commit_transaction([&](std::exception_ptr) {
                persisted = true;
            });
        });
        realm->async_begin_transaction([&, realm]() {
            table->create_object().set(col, 45);
            auto handle = realm->async_commit_transaction([&](std::exception_ptr) {
                FAIL();
            });
            realm->async_cancel_transaction(handle);
        });
        wait_for_done();
        realm->begin_transaction();
        CHECK(persisted);
        persisted = false;
        REQUIRE(table->size() == 1);
        table->create_object().set(col, 90);
        realm->cancel_transaction();

        util::EventLoop::main().run_until([&] {
            return !realm->is_in_async_transaction();
        });

        REQUIRE(table->size() == 2);
        REQUIRE(!table->find_first_int(col, 90));
    }
    SECTION("cancelled sync transaction with pending async transaction") {
        realm->async_begin_transaction([&, realm]() {
            table->create_object().set(col, 45);
            realm->async_commit_transaction([&](std::exception_ptr) {
                done = true;
            });
        });
        realm->begin_transaction();
        REQUIRE(table->size() == 0);
        table->create_object();
        realm->cancel_transaction();
        REQUIRE(table->size() == 0);
        wait_for_done();
        verify_persisted_count(1);
    }
    SECTION("cancelled sync transaction with pending async commit") {
        bool persisted = false;
        realm->async_begin_transaction([&, realm]() {
            table->create_object().set(col, 45);
            done = true;
            realm->async_commit_transaction([&](std::exception_ptr) {
                persisted = true;
            });
        });
        wait_for_done();
        realm->begin_transaction();
        REQUIRE(table->size() == 1);
        table->create_object();
        realm->cancel_transaction();

        util::EventLoop::main().run_until([&] {
            return persisted;
        });
        verify_persisted_count(1);
    }
    SECTION("sync commit of async transaction with subsequent pending async transaction") {
        realm->async_begin_transaction([&, realm]() {
            table->create_object();
            realm->commit_transaction();
        });
        realm->async_begin_transaction([&, realm]() {
            table->create_object();
            realm->commit_transaction();
            done = true;
        });
        wait_for_done();
        REQUIRE(table->size() == 2);
    }
    SECTION("release reference to Realm after async begin") {
        std::weak_ptr<Realm> weak_realm = realm;
        realm->async_begin_transaction([&]() {
            table->create_object().set(col, 45);
            weak_realm.lock()->async_commit_transaction([&](std::exception_ptr) {
                done = true;
            });
        });
        realm = nullptr;
        wait_for_done();
        verify_persisted_count(1);
    }
    SECTION("object change information") {
        realm->begin_transaction();
        auto list_col = table->get_column_key("ints");
        auto set_col = table->get_column_key("int set");
        auto dict_col = table->get_column_key("int dictionary");
        auto obj = table->create_object();
        auto list = obj.get_list<Int>(list_col);
        for (int i = 0; i < 3; ++i)
            list.add(i);
        auto set = obj.get_set<Int>(set_col);
        set.insert(0);
        auto dict = obj.get_dictionary(dict_col);
        dict.insert("a", 0);
        realm->commit_transaction();

        Observer observer(obj);
        observer.realm = realm;
        realm->m_binding_context.reset(&observer);

        realm->async_begin_transaction([&]() {
            list.clear();
            set.clear();
            dict.clear();
            done = true;
        });
        wait_for_done();
        REQUIRE(observer.array_change(0, list_col) == IndexSet{0, 1, 2});
        REQUIRE(observer.array_change(0, set_col) == IndexSet{});
        REQUIRE(observer.array_change(0, dict_col) == IndexSet{});
        realm->m_binding_context.release();
    }

    SECTION("begin_transaction() from within did_change()") {
        struct Context : public BindingContext {
            void did_change(std::vector<ObserverState> const&, std::vector<void*> const&, bool) override
            {
                auto r = realm.lock();
                r->begin_transaction();
                auto table = r->read_group().get_table("class_object");
                table->create_object();
                if (++change_count == 1) {
                    r->commit_transaction();
                }
                else {
                    r->cancel_transaction();
                }
            }
            int change_count = 0;
        };

        realm->m_binding_context.reset(new Context());
        realm->m_binding_context->realm = realm;

        realm->begin_transaction();
        auto table = realm->read_group().get_table("class_object");
        table->create_object();
        bool persisted = false;
        realm->async_commit_transaction([&persisted](auto) {
            persisted = true;
        });
        REQUIRE(table->size() == 2);
        REQUIRE(persisted);
    }

    SECTION("async write grouping") {
        size_t completion_calls = 0;
        for (size_t i = 0; i < 41; ++i) {
            realm->async_begin_transaction([&, i, realm] {
                // The top ref in the Realm file should only be updated once every 20 commits
                CHECK(Group(config.path, config.encryption_key.data()).get_table("class_object")->size() ==
                      (i / 20) * 20);

                table->create_object();
                realm->async_commit_transaction(
                    [&](std::exception_ptr) {
                        ++completion_calls;
                    },
                    true);
            });
        }
        util::EventLoop::main().run_until([&] {
            return completion_calls == 41;
        });
    }

    SECTION("async write grouping with manual barriers") {
        size_t completion_calls = 0;
        for (size_t i = 0; i < 41; ++i) {
            realm->async_begin_transaction([&, i, realm] {
                // The top ref in the Realm file should only be updated once every 6 commits
                CHECK(Group(config.path, config.encryption_key.data()).get_table("class_object")->size() ==
                      (i / 6) * 6);

                table->create_object();
                realm->async_commit_transaction(
                    [&](std::exception_ptr) {
                        ++completion_calls;
                    },
                    (i + 1) % 6 != 0);
            });
        }
        util::EventLoop::main().run_until([&] {
            return completion_calls == 41;
        });
    }

    SECTION("async writes scheduled inside sync write") {
        realm->begin_transaction();
        realm->async_begin_transaction([&] {
            REQUIRE(table->size() == 1);
            table->create_object();
            realm->async_commit_transaction();
        });
        realm->async_begin_transaction([&] {
            REQUIRE(table->size() == 2);
            table->create_object();
            realm->async_commit_transaction([&](std::exception_ptr) {
                done = true;
            });
        });
        REQUIRE(table->size() == 0);
        table->create_object();
        realm->commit_transaction();
        wait_for_done();
        REQUIRE(table->size() == 3);
    }

    SECTION("async writes scheduled inside multiple sync write") {
        realm->begin_transaction();
        realm->async_begin_transaction([&] {
            REQUIRE(table->size() == 2);
            table->create_object();
            realm->async_commit_transaction();
        });
        realm->async_begin_transaction([&] {
            REQUIRE(table->size() == 3);
            table->create_object();
            realm->async_commit_transaction();
        });
        REQUIRE(table->size() == 0);
        table->create_object();
        realm->commit_transaction();

        realm->begin_transaction();
        realm->async_begin_transaction([&] {
            REQUIRE(table->size() == 4);
            table->create_object();
            realm->async_commit_transaction();
        });
        realm->async_begin_transaction([&] {
            REQUIRE(table->size() == 5);
            table->create_object();
            realm->async_commit_transaction([&](std::exception_ptr) {
                done = true;
            });
        });
        REQUIRE(table->size() == 1);
        table->create_object();
        realm->commit_transaction();


        wait_for_done();
        REQUIRE(table->size() == 6);
    }

    SECTION("async writes which would run inside sync writes are deferred") {
        realm->async_begin_transaction([&] {
            done = true;
        });

        // Wait for the background thread to hold the write lock (without letting
        // the event loop run so that the scheduled task isn't run)
        DBOptions options;
        options.encryption_key = config.encryption_key.data();
        auto db = DB::create(make_in_realm_history(), config.path, options);
        while (db->start_write(true))
            millisleep(1);

        realm->begin_transaction();

        // Invoke the pending callback
        util::EventLoop::main().run_pending();
        // Should not have run the async write block
        REQUIRE(done == false);

        // Should run the async write block once the synchronous transaction is done
        realm->cancel_transaction();
        REQUIRE(done == false);
        util::EventLoop::main().run_pending();
        REQUIRE(done == true);
    }

    util::EventLoop::main().run_until([&] {
        return !realm || !realm->has_pending_async_work();
    });

    _impl::RealmCoordinator::clear_all_caches();
}

TEST_CASE("Call run_async_completions after realm has been closed") {
    // This requires a special scheduler as we have to call Realm::close
    // just after DB::AsyncCommitHelper has made a callback to the function
    // that asks the scheduler to invoke run_async_completions()

    struct ManualScheduler : util::Scheduler {
        std::mutex mutex;
        std::condition_variable cv;
        std::vector<util::UniqueFunction<void()>> callbacks;

        void invoke(util::UniqueFunction<void()>&& cb) override
        {
            {
                std::lock_guard lock(mutex);
                callbacks.push_back(std::move(cb));
            }
            cv.notify_all();
        }

        bool is_on_thread() const noexcept override
        {
            return true;
        }
        bool is_same_as(const Scheduler*) const noexcept override
        {
            return false;
        }
        bool can_invoke() const noexcept override
        {
            return true;
        }
    };

    auto scheduler = std::make_shared<ManualScheduler>();

    TestFile config;
    config.schema_version = 0;
    config.schema = Schema{{"object", {{"value", PropertyType::Int}}}};
    config.scheduler = scheduler;
    config.automatic_change_notifications = false;

    auto realm = Realm::get_shared_realm(config);

    realm->begin_transaction();
    realm->async_commit_transaction([](std::exception_ptr) {});

    std::vector<util::UniqueFunction<void()>> callbacks;
    {
        std::unique_lock lock(scheduler->mutex);
        // Wait for scheduler to be invoked
        scheduler->cv.wait(lock, [&] {
            return !scheduler->callbacks.empty();
        });
        callbacks.swap(scheduler->callbacks);
    }
    realm->close();
    // Call whatever functions that was added to scheduler.
    for (auto& cb : callbacks)
        cb();
}

// Our libuv scheduler currently does not support background threads, so we can
// only run this on apple platforms
#if REALM_PLATFORM_APPLE
TEST_CASE("SharedRealm: async writes on multiple threads") {
    _impl::RealmCoordinator::assert_no_open_realms();

    TestFile config;
    config.cache = true;
    config.schema_version = 0;
    config.schema = Schema{{"object", {{"value", PropertyType::Int}}}};
    auto realm = Realm::get_shared_realm(config);
    auto table_key = realm->read_group().get_table("class_object")->get_key();
    realm->close();

    struct QueueState {
        dispatch_queue_t queue;
        Realm::Config config;
    };
    std::vector<QueueState> queues;
    for (int i = 0; i < 10; ++i) {
        auto queue = dispatch_queue_create(util::format("queue %1", i).c_str(), 0);
        Realm::Config queue_config = config;
        queue_config.scheduler = util::Scheduler::make_dispatch(static_cast<void*>(queue));
        queues.push_back({queue, std::move(queue_config)});
    }

    std::atomic<size_t> completions = 0;
    // Capturing by reference when mixing lambda and blocks is weird, so capture
    // a pointer instead
    auto completions_ptr = &completions;

    auto async_write_and_async_commit = [=](const Realm::Config& config) {
        Realm::get_shared_realm(config)->async_begin_transaction([=] {
            auto realm = Realm::get_shared_realm(config);
            realm->read_group().get_table(table_key)->create_object();
            realm->async_commit_transaction([=](std::exception_ptr) {
                ++*completions_ptr;
            });
        });
    };
    auto async_write_and_sync_commit = [=](const Realm::Config& config) {
        Realm::get_shared_realm(config)->async_begin_transaction([=] {
            auto realm = Realm::get_shared_realm(config);
            realm->read_group().get_table(table_key)->create_object();
            realm->commit_transaction();
            ++*completions_ptr;
        });
    };
    auto sync_write_and_async_commit = [=](const Realm::Config& config) {
        auto realm = Realm::get_shared_realm(config);
        realm->begin_transaction();
        realm->read_group().get_table(table_key)->create_object();
        realm->async_commit_transaction([=](std::exception_ptr) {
            ++*completions_ptr;
        });
    };
    auto sync_write_and_sync_commit = [=](const Realm::Config& config) {
        auto realm = Realm::get_shared_realm(config);
        realm->begin_transaction();
        realm->read_group().get_table(table_key)->create_object();
        realm->commit_transaction();
        ++*completions_ptr;
    };

    SECTION("async begin and async commit") {
        for (auto& queue : queues) {
            dispatch_async(queue.queue, ^{
                for (int i = 0; i < 10; ++i) {
                    async_write_and_async_commit(queue.config);
                }
            });
        }
        util::EventLoop::main().run_until([&] {
            return completions == 100;
        });
    }
    SECTION("async begin and sync commit") {
        for (auto& queue : queues) {
            dispatch_async(queue.queue, ^{
                for (int i = 0; i < 10; ++i) {
                    async_write_and_sync_commit(queue.config);
                }
            });
        }
        util::EventLoop::main().run_until([&] {
            return completions == 100;
        });
    }
    SECTION("sync begin and async commit") {
        for (auto& queue : queues) {
            dispatch_async(queue.queue, ^{
                for (int i = 0; i < 10; ++i) {
                    sync_write_and_async_commit(queue.config);
                }
            });
        }
        util::EventLoop::main().run_until([&] {
            return completions == 100;
        });
    }
    SECTION("sync begin and sync commit") {
        for (auto& queue : queues) {
            dispatch_async(queue.queue, ^{
                for (int i = 0; i < 10; ++i) {
                    sync_write_and_sync_commit(queue.config);
                }
            });
        }
        util::EventLoop::main().run_until([&] {
            return completions == 100;
        });
    }
    SECTION("mixed sync and async") {
        // Test every permutation of each of the variants
        struct IndexedOp {
            int index;
            std::function<void(const Realm::Config& config)> fn;
        };
        std::array<IndexedOp, 4> functions{{
            {0, async_write_and_async_commit},
            {1, sync_write_and_async_commit},
            {2, async_write_and_sync_commit},
            {3, sync_write_and_sync_commit},
        }};
        size_t i = 0;
        size_t expected_completions = 0;
        do {
            auto& queue = queues[i++ % 10];
            auto functions_copy = functions;
            dispatch_async(queue.queue, ^{
                for (auto& fn : functions_copy) {
                    fn.fn(queue.config);
                }
            });
            expected_completions += 4;
        } while (std::next_permutation(functions.begin(), functions.end(), [](auto& a, auto& b) {
            return a.index < b.index;
        }));

        util::EventLoop::main().run_until([&] {
            return completions == expected_completions;
        });
    }


    realm = Realm::get_shared_realm(config);
    REQUIRE(realm->read_group().get_table(table_key)->size() == completions);

    for (auto& queue : queues) {
        dispatch_sync(queue.queue, ^{
                      });
    }
}
#endif

class LooperDelegate {
public:
    LooperDelegate() {}
    void run_once()
    {
        for (auto it = m_tasks.begin(); it != m_tasks.end(); ++it) {
            if (it->may_run && *it->may_run) {
                it->the_job();
                m_tasks.erase(it);
                return;
            }
        }
    }
    std::shared_ptr<bool> add_task(util::UniqueFunction<void()>&& the_job)
    {
        m_tasks.push_back(Task{std::make_shared<bool>(false), std::move(the_job)});
        return m_tasks.back().may_run;
    }
    bool has_tasks()
    {
        return !m_tasks.empty();
    }

private:
    struct Task {
        std::shared_ptr<bool> may_run;
        util::UniqueFunction<void()> the_job;
    };
    std::vector<Task> m_tasks;
};

#ifndef _WIN32
TEST_CASE("SharedRealm: async_writes_2") {
    _impl::RealmCoordinator::assert_no_open_realms();
    if (!util::EventLoop::has_implementation())
        return;

    TestFile config;
    config.schema_version = 0;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Int}}},
    };
    bool done = false;
    auto realm = Realm::get_shared_realm(config);
    int write_nr = 0;
    int commit_nr = 0;
    auto table = realm->read_group().get_table("class_object");
    auto col = table->get_column_key("value");
    LooperDelegate ld;
    std::shared_ptr<bool> t1_rdy = ld.add_task([&, realm]() {
        REQUIRE(write_nr == 0);
        ++write_nr;
        table->create_object().set(col, 45);
        realm->cancel_transaction();
    });
    std::shared_ptr<bool> t2_rdy = ld.add_task([&, realm]() {
        REQUIRE(write_nr == 1);
        ++write_nr;
        table->create_object().set(col, 45);
        realm->async_commit_transaction([&](std::exception_ptr) {
            REQUIRE(commit_nr == 0);
            ++commit_nr;
        });
    });
    std::shared_ptr<bool> t3_rdy = ld.add_task([&, realm]() {
        ++write_nr;
        auto o = table->get_object(0);
        o.set(col, o.get<int64_t>(col) + 37);
        realm->async_commit_transaction([&](std::exception_ptr) {
            ++commit_nr;
            done = true;
        });
    });

    // Make some notify_only transactions
    realm->async_begin_transaction(
        [&]() {
            *t1_rdy = true;
        },
        true);
    realm->async_begin_transaction(
        [&]() {
            *t2_rdy = true;
        },
        true);
    realm->async_begin_transaction(
        [&]() {
            *t3_rdy = true;
        },
        true);

    util::EventLoop::main().run_until([&, realm] {
        ld.run_once();
        return done;
    });
    REQUIRE(done);
}
#endif

TEST_CASE("SharedRealm: notifications") {
    if (!util::EventLoop::has_implementation())
        return;

    TestFile config;
    config.schema_version = 0;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Int}}},
    };

    struct Context : BindingContext {
        size_t* change_count;
        util::UniqueFunction<void()> did_change_fn;
        util::UniqueFunction<void()> changes_available_fn;

        Context(size_t* out)
            : change_count(out)
        {
        }

        void did_change(std::vector<ObserverState> const&, std::vector<void*> const&, bool) override
        {
            ++*change_count;
            if (did_change_fn)
                did_change_fn();
        }

        void changes_available() override
        {
            if (changes_available_fn)
                changes_available_fn();
        }
    };

    size_t change_count = 0;
    auto realm = Realm::get_shared_realm(config);
    realm->read_group();
    auto context = new Context{&change_count};
    realm->m_binding_context.reset(context);
    realm->m_binding_context->realm = realm;

    SECTION("local notifications are sent synchronously") {
        realm->begin_transaction();
        REQUIRE(change_count == 0);
        realm->commit_transaction();
        REQUIRE(change_count == 1);
    }
#ifndef _WIN32
    SECTION("remote notifications are sent asynchronously") {
        auto r2 = Realm::get_shared_realm(config);
        r2->begin_transaction();
        r2->commit_transaction();
        REQUIRE(change_count == 0);
        util::EventLoop::main().run_until([&] {
            return change_count > 0;
        });
        REQUIRE(change_count == 1);
    }

    SECTION("notifications created in async transaction are sent synchronously") {
        realm->async_begin_transaction([&] {
            REQUIRE(change_count == 0);
            realm->async_commit_transaction();
            REQUIRE(change_count == 1);
        });
        REQUIRE(change_count == 0);
        util::EventLoop::main().run_until([&] {
            return change_count > 0;
        });
        REQUIRE(change_count == 1);
        util::EventLoop::main().run_until([&] {
            return !realm->has_pending_async_work();
        });
    }
#endif
    SECTION("refresh() from within changes_available() refreshes") {
        context->changes_available_fn = [&] {
            REQUIRE(realm->refresh());
        };
        realm->set_auto_refresh(false);

        auto r2 = Realm::get_shared_realm(config);
        r2->begin_transaction();
        r2->commit_transaction();
        realm->notify();
        // Should return false as the realm was already advanced
        REQUIRE_FALSE(realm->refresh());
    }

    SECTION("refresh() from within did_change() is a no-op") {
        context->did_change_fn = [&] {
            if (change_count > 1)
                return;

            // Create another version so that refresh() advances the version
            auto r2 = Realm::get_shared_realm(realm->config());
            r2->begin_transaction();
            r2->commit_transaction();

            REQUIRE_FALSE(realm->refresh());
        };

        auto r2 = Realm::get_shared_realm(config);
        r2->begin_transaction();
        r2->commit_transaction();

        REQUIRE(realm->refresh());
        REQUIRE(change_count == 1);

        REQUIRE(realm->refresh());
        REQUIRE(change_count == 2);
        REQUIRE_FALSE(realm->refresh());
    }

    SECTION("begin_write() from within did_change() produces recursive notifications") {
        context->did_change_fn = [&] {
            if (realm->is_in_transaction())
                realm->cancel_transaction();
            if (change_count > 3)
                return;

            // Create another version so that begin_write() advances the version
            auto r2 = Realm::get_shared_realm(realm->config());
            r2->begin_transaction();
            r2->commit_transaction();

            realm->begin_transaction();
            REQUIRE(change_count == 4);
        };

        auto r2 = Realm::get_shared_realm(config);
        r2->begin_transaction();
        r2->commit_transaction();
        REQUIRE(realm->refresh());
        REQUIRE(change_count == 4);
        REQUIRE_FALSE(realm->refresh());
    }

#if REALM_ENABLE_SYNC
    SECTION("SubscriptionStore writes produce notifications") {
        auto subscription_store = sync::SubscriptionStore::create(TestHelper::get_db(realm));
        REQUIRE(change_count == 0);
        util::EventLoop::main().run_until([&] {
            return change_count > 0;
        });
        REQUIRE(change_count == 1);

        subscription_store->get_active().make_mutable_copy().commit();
        REQUIRE(change_count == 1);
        util::EventLoop::main().run_until([&] {
            return change_count > 1;
        });
        REQUIRE(change_count == 2);
    }
#endif
}

TEST_CASE("SharedRealm: schema updating from external changes") {
    TestFile config;
    config.schema_version = 0;
    config.schema_mode = SchemaMode::AdditiveExplicit;
    config.schema = Schema{
        {"object",
         {
             {"value", PropertyType::Int, Property::IsPrimary{true}},
             {"value 2", PropertyType::Int, Property::IsPrimary{false}, Property::IsIndexed{true}},
         }},
    };

    SECTION("newly added columns update table columns but are not added to properties") {
        // Does this test add any value when column keys are stable?
        auto r1 = Realm::get_shared_realm(config);
        auto r2 = Realm::get_shared_realm(config);
        auto test = [&] {
            r2->begin_transaction();
            r2->read_group().get_table("class_object")->add_column(type_String, "new col");
            r2->commit_transaction();

            auto& object_schema = *r1->schema().find("object");
            REQUIRE(object_schema.persisted_properties.size() == 2);
            ColKey col = object_schema.persisted_properties[0].column_key;
            r1->refresh();
            REQUIRE(object_schema.persisted_properties[0].column_key == col);
        };
        SECTION("with an active read transaction") {
            r1->read_group();
            test();
        }
        SECTION("without an active read transaction") {
            r1->invalidate();
            test();
        }
    }

    SECTION("beginning a read transaction checks for incompatible changes") {
        auto r = Realm::get_shared_realm(config);
        r->invalidate();

        auto& db = TestHelper::get_db(r);
        WriteTransaction wt(db);
        auto& table = *wt.get_table("class_object");

        SECTION("removing a property") {
            table.remove_column(table.get_column_key("value"));
            wt.commit();
            REQUIRE_THROWS_CONTAINING(r->refresh(), "Property 'object.value' has been removed.");
        }

        SECTION("change property type") {
            table.remove_column(table.get_column_key("value 2"));
            table.add_column(type_Float, "value 2");
            wt.commit();
            REQUIRE_THROWS_CONTAINING(r->refresh(),
                                      "Property 'object.value 2' has been changed from 'int' to 'float'");
        }

        SECTION("make property optional") {
            table.remove_column(table.get_column_key("value 2"));
            table.add_column(type_Int, "value 2", true);
            wt.commit();
            REQUIRE_THROWS_CONTAINING(r->refresh(), "Property 'object.value 2' has been made optional");
        }

        SECTION("recreate column with no changes") {
            table.remove_column(table.get_column_key("value 2"));
            table.add_column(type_Int, "value 2");
            wt.commit();
            REQUIRE_NOTHROW(r->refresh());
        }

        SECTION("remove index from non-PK") {
            table.remove_search_index(table.get_column_key("value 2"));
            wt.commit();
            REQUIRE_NOTHROW(r->refresh());
        }
    }
}

TEST_CASE("SharedRealm: close()") {
    TestFile config;
    config.schema_version = 1;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Int}}},
        {"list", {{"list", PropertyType::Object | PropertyType::Array, "object"}}},
    };

    auto realm = Realm::get_shared_realm(config);

    SECTION("all functions throw ClosedRealmException after close") {
        const char* msg = "Cannot access realm that has been closed.";

        realm->close();
        REQUIRE(realm->is_closed());
        REQUIRE_EXCEPTION(realm->verify_open(), ClosedRealm, msg);

        REQUIRE_EXCEPTION(realm->update_schema(Schema{}), ClosedRealm, msg);
        REQUIRE_EXCEPTION(realm->rename_property(Schema{}, "", "", ""), ClosedRealm, msg);
        REQUIRE_EXCEPTION(realm->set_schema_subset(Schema{}), ClosedRealm, msg);

        REQUIRE_EXCEPTION(realm->begin_transaction(), ClosedRealm, msg);
        REQUIRE_EXCEPTION(realm->commit_transaction(), ClosedRealm, msg);
        REQUIRE_EXCEPTION(realm->cancel_transaction(), ClosedRealm, msg);
        REQUIRE(!realm->is_in_transaction());

        REQUIRE_EXCEPTION(realm->async_begin_transaction(nullptr), ClosedRealm, msg);
        REQUIRE_EXCEPTION(realm->async_commit_transaction(nullptr), ClosedRealm, msg);
        REQUIRE_EXCEPTION(realm->async_cancel_transaction(0), ClosedRealm, msg);
        REQUIRE_FALSE(realm->is_in_async_transaction());

        REQUIRE_EXCEPTION(realm->freeze(), ClosedRealm, msg);
        REQUIRE_FALSE(realm->is_frozen());
        REQUIRE_EXCEPTION(realm->get_number_of_versions(), ClosedRealm, msg);
        REQUIRE_EXCEPTION(realm->read_transaction_version(), ClosedRealm, msg);
        REQUIRE_EXCEPTION(realm->duplicate(), ClosedRealm, msg);

        REQUIRE_EXCEPTION(realm->enable_wait_for_change(), ClosedRealm, msg);
        REQUIRE_EXCEPTION(realm->wait_for_change(), ClosedRealm, msg);
        REQUIRE_EXCEPTION(realm->wait_for_change_release(), ClosedRealm, msg);

        REQUIRE_NOTHROW(realm->notify());
        REQUIRE_EXCEPTION(realm->refresh(), ClosedRealm, msg);
        REQUIRE_EXCEPTION(realm->invalidate(), ClosedRealm, msg);
        REQUIRE_EXCEPTION(realm->compact(), ClosedRealm, msg);
        REQUIRE_EXCEPTION(realm->convert(realm->config()), ClosedRealm, msg);
        REQUIRE_EXCEPTION(realm->write_copy(), ClosedRealm, msg);

#if REALM_ENABLE_SYNC
        REQUIRE_FALSE(realm->sync_session());
        msg = "Flexible sync is not enabled";
        REQUIRE_EXCEPTION(realm->get_latest_subscription_set(), IllegalOperation, msg);
        REQUIRE_EXCEPTION(realm->get_active_subscription_set(), IllegalOperation, msg);
#endif
    }

    SECTION("fully closes database file even with live notifiers") {
        auto& group = realm->read_group();
        realm->begin_transaction();
        auto obj = ObjectStore::table_for_object_type(group, "list")->create_object();
        realm->commit_transaction();

        Results results(realm, ObjectStore::table_for_object_type(group, "object"));
        List list(realm, obj.get_linklist("list"));
        Object object(realm, obj);

        auto obj_token = object.add_notification_callback([](CollectionChangeSet) {});
        auto list_token = list.add_notification_callback([](CollectionChangeSet) {});
        auto results_token = results.add_notification_callback([](CollectionChangeSet) {});

        // Perform a dummy transaction to ensure the notifiers actually acquire
        // resources that need to be closed
        realm->begin_transaction();
        realm->commit_transaction();

        realm->close();

        // Verify that we're able to acquire an exclusive lock
        REQUIRE(DB::call_with_lock(config.path, [](auto) {}));
    }
}

TEST_CASE("Realm::delete_files()") {
    TestFile config;
    config.schema_version = 1;
    config.schema = Schema{{"object", {{"value", PropertyType::Int}}}};
    auto realm = Realm::get_shared_realm(config);
    auto path = config.path;

    // We need to create some additional files that might not be present
    // for a freshly opened realm but need to be tested for as the will
    // be created during a Realm's life cycle.
    (void)util::File(path + ".log", util::File::mode_Write);

    SECTION("Deleting files of a closed Realm succeeds.") {
        realm->close();
        bool did_delete = false;
        Realm::delete_files(path, &did_delete);
        REQUIRE(did_delete);
        REQUIRE_FALSE(util::File::exists(path));
        REQUIRE_FALSE(util::File::exists(path + ".management"));
        REQUIRE_FALSE(util::File::exists(path + ".note"));
        REQUIRE_FALSE(util::File::exists(path + ".log"));

        // Deleting the .lock file is not safe. It must still exist.
        REQUIRE(util::File::exists(path + ".lock"));
    }

    SECTION("Trying to delete files of an open Realm fails.") {
        REQUIRE_EXCEPTION(Realm::delete_files(path), ErrorCodes::DeleteOnOpenRealm,
                          util::format("Cannot delete files of an open Realm: '%1' is still in use.", path));
        REQUIRE(util::File::exists(path + ".lock"));
        REQUIRE(util::File::exists(path));
        REQUIRE(util::File::exists(path + ".management"));
#ifndef _WIN32
        REQUIRE(util::File::exists(path + ".note"));
#endif
        REQUIRE(util::File::exists(path + ".log"));
    }

    SECTION("Deleting the same Realm multiple times.") {
        realm->close();
        Realm::delete_files(path);
        Realm::delete_files(path);
        Realm::delete_files(path);
    }

    SECTION("Calling delete on a folder that does not exist.") {
        auto fake_path = "/tmp/doesNotExist/realm.424242";
        bool did_delete = false;
        Realm::delete_files(fake_path, &did_delete);
        REQUIRE_FALSE(did_delete);
    }

    SECTION("passing did_delete is optional") {
        realm->close();
        Realm::delete_files(path, nullptr);
    }

    SECTION("Deleting a Realm which does not exist does not set did_delete") {
        TestFile new_config;
        bool did_delete = false;
        Realm::delete_files(new_config.path, &did_delete);
        REQUIRE_FALSE(did_delete);
    }
}

TEST_CASE("ShareRealm: in-memory mode from buffer") {
    TestFile config;
    config.schema_version = 1;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Int}}},
    };

    SECTION("Save and open Realm from in-memory buffer") {
        // Write in-memory copy of Realm to a buffer
        auto realm = Realm::get_shared_realm(config);
        OwnedBinaryData realm_buffer = realm->write_copy();

        // Open the buffer as a new (immutable in-memory) Realm
        realm::Realm::Config config2;
        config2.in_memory = true;
        config2.schema_mode = SchemaMode::Immutable;
        config2.realm_data = realm_buffer.get();

        auto realm2 = Realm::get_shared_realm(config2);

        // Verify that it can read the schema and that it is the same
        REQUIRE(realm->schema().size() == 1);
        auto it = realm->schema().find("object");
        auto table = realm->read_group().get_table("class_object");
        REQUIRE(it != realm->schema().end());
        REQUIRE(it->table_key == table->get_key());
        REQUIRE(it->persisted_properties.size() == 1);
        REQUIRE(it->persisted_properties[0].name == "value");
        REQUIRE(it->persisted_properties[0].column_key == table->get_column_key("value"));

        // Test invalid configs
        realm::Realm::Config config3;
        config3.realm_data = realm_buffer.get();
        REQUIRE_EXCEPTION(Realm::get_shared_realm(config3), IllegalCombination,
                          "In-memory realms initialized from memory buffers can only be opened in read-only mode");

        config3.in_memory = true;
        config3.schema_mode = SchemaMode::Immutable;
        config3.path = "path";
        REQUIRE_EXCEPTION(Realm::get_shared_realm(config3), IllegalCombination,
                          "Specifying both memory buffer and path is invalid");

        config3.path = "";
        config3.encryption_key = std::vector<char>(64, 'a');
        REQUIRE_EXCEPTION(Realm::get_shared_realm(config3), IllegalCombination,
                          "Memory buffers do not support encryption");
    }
}

TEST_CASE("ShareRealm: realm closed in did_change callback") {
    TestFile config;
    config.schema_version = 1;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Int}}},
    };
    config.automatic_change_notifications = false;
    auto r1 = Realm::get_shared_realm(config);

    r1->begin_transaction();
    auto table = r1->read_group().get_table("class_object");
    table->create_object();
    r1->commit_transaction();

    struct Context : public BindingContext {
        Context(std::shared_ptr<Realm>& realm)
            : realm(&realm)
        {
        }
        std::shared_ptr<Realm>* realm;
        void did_change(std::vector<ObserverState> const&, std::vector<void*> const&, bool) override
        {
            auto realm = this->realm; // close() will delete `this`
            (*realm)->close();
            realm->reset();
        }
    };

    SECTION("did_change") {
        r1->m_binding_context.reset(new Context(r1));
        r1->invalidate();

        auto r2 = Realm::get_shared_realm(config);
        r2->begin_transaction();
        r2->read_group().get_table("class_object")->create_object();
        r2->commit_transaction();
        r2.reset();

        r1->notify();
    }

    SECTION("did_change with async results") {
        r1->m_binding_context.reset(new Context(r1));
        Results results(r1, table->where());
        auto token = results.add_notification_callback([&](CollectionChangeSet) {
            // Should not be called.
            REQUIRE(false);
        });

        auto r2 = Realm::get_shared_realm(config);
        r2->begin_transaction();
        r2->read_group().get_table("class_object")->create_object();
        r2->commit_transaction();
        r2.reset();

        auto coordinator = _impl::RealmCoordinator::get_coordinator(config.path);
        coordinator->on_change();

        r1->notify();
    }

    SECTION("refresh") {
        r1->m_binding_context.reset(new Context(r1));

        auto r2 = Realm::get_shared_realm(config);
        r2->begin_transaction();
        r2->read_group().get_table("class_object")->create_object();
        r2->commit_transaction();
        r2.reset();

        REQUIRE_FALSE(r1->refresh());
    }
}

TEST_CASE("RealmCoordinator: schema cache") {
    TestFile config;
    auto coordinator = _impl::RealmCoordinator::get_coordinator(config.path);

    Schema cache_schema;
    uint64_t cache_sv = -1, cache_tv = -1;

    Schema schema{
        {"object", {{"value", PropertyType::Int}}},
    };
    Schema schema2{
        {"object",
         {
             {"value", PropertyType::Int},
         }},
        {"object 2",
         {
             {"value", PropertyType::Int},
         }},
    };

    SECTION("valid initial schema sets cache") {
        coordinator->cache_schema(schema, 5, 10);
        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_schema == schema);
        REQUIRE(cache_sv == 5);
        REQUIRE(cache_tv == 10);
    }

    SECTION("cache can be updated with newer schema") {
        coordinator->cache_schema(schema, 5, 10);
        coordinator->cache_schema(schema2, 6, 11);
        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_schema == schema2);
        REQUIRE(cache_sv == 6);
        REQUIRE(cache_tv == 11);
    }

    SECTION("empty schema is ignored") {
        coordinator->cache_schema(Schema{}, 5, 10);
        REQUIRE_FALSE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));

        coordinator->cache_schema(schema, 5, 10);
        coordinator->cache_schema(Schema{}, 5, 10);
        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_schema == schema);
        REQUIRE(cache_sv == 5);
        REQUIRE(cache_tv == 10);
    }

    SECTION("schema for older transaction is ignored") {
        coordinator->cache_schema(schema, 5, 10);
        coordinator->cache_schema(schema2, 4, 8);

        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_schema == schema);
        REQUIRE(cache_sv == 5);
        REQUIRE(cache_tv == 10);

        coordinator->advance_schema_cache(10, 20);
        coordinator->cache_schema(schema, 6, 15);
        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_tv == 20); // should not have dropped to 15
    }

    SECTION("advance_schema() from transaction version bumps transaction version") {
        coordinator->cache_schema(schema, 5, 10);
        coordinator->advance_schema_cache(10, 12);
        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_schema == schema);
        REQUIRE(cache_sv == 5);
        REQUIRE(cache_tv == 12);
    }

    SECTION("advance_schema() ending before transaction version does nothing") {
        coordinator->cache_schema(schema, 5, 10);
        coordinator->advance_schema_cache(8, 9);
        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_schema == schema);
        REQUIRE(cache_sv == 5);
        REQUIRE(cache_tv == 10);
    }

    SECTION("advance_schema() extending over transaction version bumps version") {
        coordinator->cache_schema(schema, 5, 10);
        coordinator->advance_schema_cache(3, 15);
        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_schema == schema);
        REQUIRE(cache_sv == 5);
        REQUIRE(cache_tv == 15);
    }

    SECTION("advance_schema() with no cahced schema does nothing") {
        coordinator->advance_schema_cache(3, 15);
        REQUIRE_FALSE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
    }
}

TEST_CASE("SharedRealm: coordinator schema cache") {
    TestFile config;
    auto r = Realm::get_shared_realm(config);
    auto coordinator = _impl::RealmCoordinator::get_coordinator(config.path);

    Schema cache_schema;
    uint64_t cache_sv = -1, cache_tv = -1;

    Schema schema{
        {"object", {{"value", PropertyType::Int}}},
    };
    Schema schema2{
        {"object",
         {
             {"value", PropertyType::Int},
         }},
        {"object 2",
         {
             {"value", PropertyType::Int},
         }},
    };

    class ExternalWriter {
    private:
        std::shared_ptr<Realm> m_realm;

    public:
        WriteTransaction wt;
        ExternalWriter(Realm::Config const& config)
            : m_realm([&] {
                auto c = config;
                c.scheduler = util::Scheduler::make_frozen(VersionID());
                return _impl::RealmCoordinator::get_coordinator(c.path)->get_realm(c, util::none);
            }())
            , wt(TestHelper::get_db(m_realm))
        {
        }
    };

    auto external_write = [&](Realm::Config const& config, auto&& fn) {
        ExternalWriter wt(config);
        fn(wt.wt);
        wt.wt.commit();
    };

    SECTION("is initially empty for uninitialized file") {
        REQUIRE_FALSE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
    }
    r->update_schema(schema);

    SECTION("is populated after calling update_schema()") {
        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_sv == 0);
        REQUIRE(cache_schema == schema);
        REQUIRE(cache_schema.begin()->persisted_properties[0].column_key != ColKey{});
    }

    coordinator = nullptr;
    r = nullptr;
    r = Realm::get_shared_realm(config);
    coordinator = _impl::RealmCoordinator::get_coordinator(config.path);
    REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));

    SECTION("is populated after opening an initialized file") {
        REQUIRE(cache_sv == 0);
        REQUIRE(cache_tv == 2); // with in-realm history the version doesn't reset
        REQUIRE(cache_schema == schema);
        REQUIRE(cache_schema.begin()->persisted_properties[0].column_key != ColKey{});
    }

    SECTION("transaction version is bumped after a local write") {
        auto tv = cache_tv;
        r->begin_transaction();
        r->commit_transaction();
        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_tv == tv + 1);
    }

    SECTION("notify() without a read transaction does not bump transaction version") {
        auto tv = cache_tv;

        SECTION("non-schema change") {
            external_write(config, [](auto& wt) {
                wt.get_table("class_object")->create_object();
            });
        }
        SECTION("schema change") {
            external_write(config, [](auto& wt) {
                wt.add_table("class_object 2");
            });
        }

        r->notify();
        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_tv == tv);
        REQUIRE(cache_schema == schema);
    }

    SECTION("notify() with a read transaction bumps transaction version") {
        r->read_group();
        external_write(config, [](auto& wt) {
            wt.get_table("class_object")->create_object();
        });

        r->notify();
        auto tv = cache_tv;
        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_tv == tv + 1);
    }

    SECTION("notify() with a read transaction updates schema folloing external schema change") {
        r->read_group();
        external_write(config, [](auto& wt) {
            wt.add_table("class_object 2");
        });

        r->notify();
        auto tv = cache_tv;
        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_tv == tv + 1);
        REQUIRE(cache_schema.size() == 2);
        REQUIRE(cache_schema.find("object 2") != cache_schema.end());
    }

    SECTION("transaction version is bumped after refresh() following external non-schema write") {
        external_write(config, [](auto& wt) {
            wt.get_table("class_object")->create_object();
        });

        r->refresh();
        auto tv = cache_tv;
        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_tv == tv + 1);
    }

    SECTION("schema is reread following refresh() over external schema change") {
        external_write(config, [](auto& wt) {
            wt.add_table("class_object 2");
        });

        r->refresh();
        auto tv = cache_tv;
        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_tv == tv + 1);
        REQUIRE(cache_schema.size() == 2);
        REQUIRE(cache_schema.find("object 2") != cache_schema.end());
    }

    SECTION("update_schema() to version already on disk updates cache") {
        r->read_group();
        external_write(config, [](auto& wt) {
            auto table = wt.add_table("class_object 2");
            table->add_column(type_Int, "value");
        });

        auto tv = cache_tv;
        r->update_schema(schema2);

        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_tv == tv + 1); // only +1 because update_schema() did not perform a write
        REQUIRE(cache_schema.size() == 2);
        REQUIRE(cache_schema.find("object 2") != cache_schema.end());
    }

    SECTION("update_schema() to version already on disk updates cache") {
        r->read_group();
        external_write(config, [](auto& wt) {
            auto table = wt.add_table("class_object 2");
            table->add_column(type_Int, "value");
        });

        auto tv = cache_tv;
        r->update_schema(schema2);

        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_tv == tv + 1); // only +1 because update_schema() did not perform a write
        REQUIRE(cache_schema.size() == 2);
        REQUIRE(cache_schema.find("object 2") != cache_schema.end());
    }

    SECTION("update_schema() to version populated on disk while waiting for the write lock updates cache") {
        r->read_group();

        // We want to commit the write while we're waiting on the write lock on
        // this thread, which can't really be done in a properly synchronized manner
        std::chrono::microseconds wait_time{5000};
#if REALM_ANDROID
        // When running on device or in an emulator we need to wait longer due
        // to them being slow
        wait_time *= 10;
#endif

        bool did_run = false;
        JoiningThread thread([&] {
            ExternalWriter writer(config);
            if (writer.wt.get_table("class_object 2"))
                return;
            did_run = true;

            auto table = writer.wt.add_table("class_object 2");
            table->add_column(type_Int, "value");
            std::this_thread::sleep_for(wait_time * 2);
            writer.wt.commit();
        });
        std::this_thread::sleep_for(wait_time);

        auto tv = cache_tv;
        r->update_schema(Schema{
            {"object", {{"value", PropertyType::Int}}},
            {"object 2", {{"value", PropertyType::Int}}},
        });

        // just skip the test if the timing was wrong to avoid spurious failures
        if (!did_run)
            return;

        REQUIRE(coordinator->get_cached_schema(cache_schema, cache_sv, cache_tv));
        REQUIRE(cache_tv == tv + 1); // only +1 because update_schema()'s write was rolled back
        REQUIRE(cache_schema.size() == 2);
        REQUIRE(cache_schema.find("object 2") != cache_schema.end());
    }
}

TEST_CASE("SharedRealm: dynamic schema mode doesn't invalidate object schema pointers when schema hasn't changed") {
    TestFile config;

    // Prepopulate the Realm with the schema.
    Realm::Config config_with_schema = config;
    config_with_schema.schema_version = 1;
    config_with_schema.schema_mode = SchemaMode::Automatic;
    config_with_schema.schema =
        Schema{{"object",
                {
                    {"value", PropertyType::Int, Property::IsPrimary{true}},
                    {"value 2", PropertyType::Int, Property::IsPrimary{false}, Property::IsIndexed{true}},
                }}};
    auto r1 = Realm::get_shared_realm(config_with_schema);

    // Retrieve the object schema in dynamic mode.
    auto r2 = Realm::get_shared_realm(config);
    auto* object_schema = &*r2->schema().find("object");

    // Perform an empty write to create a new version, resulting in the other Realm needing to re-read the schema.
    r1->begin_transaction();
    r1->commit_transaction();

    // Advance to the latest version, and verify the object schema is at the same location in memory.
    r2->read_group();
    REQUIRE(object_schema == &*r2->schema().find("object"));
}

TEST_CASE("SharedRealm: declaring an object as embedded results in creating an embedded table") {
    TestFile config;

    // Prepopulate the Realm with the schema.
    config.schema = Schema{{"object1",
                            ObjectSchema::ObjectType::Embedded,
                            {
                                {"value", PropertyType::Int},
                            }},
                           {"object2",
                            {
                                {"value", PropertyType::Object | PropertyType::Nullable, "object1"},
                            }}};
    auto r1 = Realm::get_shared_realm(config);

    Group& g = r1->read_group();
    auto t = g.get_table("class_object1");
    REQUIRE(t->is_embedded());
}

TEST_CASE("SharedRealm: SchemaChangedFunction") {
    struct Context : BindingContext {
        size_t* change_count;
        Schema* schema;
        Context(size_t* count_out, Schema* schema_out)
            : change_count(count_out)
            , schema(schema_out)
        {
        }

        void schema_did_change(Schema const& changed_schema) override
        {
            ++*change_count;
            *schema = changed_schema;
        }
    };

    size_t schema_changed_called = 0;
    Schema changed_fixed_schema;
    TestFile config;
    RealmConfig dynamic_config = config;

    config.schema = Schema{{"object1",
                            {
                                {"value", PropertyType::Int},
                            }},
                           {"object2",
                            {
                                {"value", PropertyType::Int},
                            }}};
    config.schema_version = 1;
    auto r1 = Realm::get_shared_realm(config);
    r1->read_group();
    r1->m_binding_context.reset(new Context(&schema_changed_called, &changed_fixed_schema));

    SECTION("Fixed schema") {
        SECTION("update_schema") {
            auto new_schema = Schema{{"object3",
                                      {
                                          {"value", PropertyType::Int},
                                      }}};
            r1->update_schema(new_schema, 2);
            REQUIRE(schema_changed_called == 1);
            REQUIRE(changed_fixed_schema.find("object3")->property_for_name("value")->column_key != ColKey{});
        }

        SECTION("Open a new Realm instance with same config won't trigger") {
            auto r2 = Realm::get_shared_realm(config);
            REQUIRE(schema_changed_called == 0);
        }

        SECTION("Non schema related transaction doesn't trigger") {
            auto r2 = Realm::get_shared_realm(config);
            r2->begin_transaction();
            r2->commit_transaction();
            r1->refresh();
            REQUIRE(schema_changed_called == 0);
        }

        SECTION("Schema is changed by another Realm") {
            auto r2 = Realm::get_shared_realm(config);
            r2->begin_transaction();
            r2->read_group().get_table("class_object1")->add_column(type_String, "new col");
            r2->commit_transaction();
            r1->refresh();
            REQUIRE(schema_changed_called == 1);
            REQUIRE(changed_fixed_schema.find("object1")->property_for_name("value")->column_key != ColKey{});
        }

        // This is not a valid use case. m_schema won't be refreshed.
        SECTION("Schema is changed by this Realm won't trigger") {
            r1->begin_transaction();
            r1->read_group().get_table("class_object1")->add_column(type_String, "new col");
            r1->commit_transaction();
            REQUIRE(schema_changed_called == 0);
        }
    }

    SECTION("Dynamic schema") {
        size_t dynamic_schema_changed_called = 0;
        Schema changed_dynamic_schema;
        auto r2 = Realm::get_shared_realm(dynamic_config);
        r2->m_binding_context.reset(new Context(&dynamic_schema_changed_called, &changed_dynamic_schema));

        SECTION("set_schema_subset") {
            auto new_schema = Schema{{"object1",
                                      {
                                          {"value", PropertyType::Int},
                                      }}};
            r2->set_schema_subset(new_schema);
            REQUIRE(schema_changed_called == 0);
            REQUIRE(dynamic_schema_changed_called == 1);
            REQUIRE(changed_dynamic_schema.find("object1")->property_for_name("value")->column_key != ColKey{});
        }

        SECTION("Non schema related transaction will always trigger in dynamic mode") {
            auto r1 = Realm::get_shared_realm(config);
            // An empty transaction will trigger the schema changes always in dynamic mode.
            r1->begin_transaction();
            r1->commit_transaction();
            r2->refresh();
            REQUIRE(dynamic_schema_changed_called == 1);
            REQUIRE(changed_dynamic_schema.find("object1")->property_for_name("value")->column_key != ColKey{});
        }

        SECTION("Schema is changed by another Realm") {
            r1->begin_transaction();
            r1->read_group().get_table("class_object1")->add_column(type_String, "new col");
            r1->commit_transaction();
            r2->refresh();
            REQUIRE(dynamic_schema_changed_called == 1);
            REQUIRE(changed_dynamic_schema.find("object1")->property_for_name("value")->column_key != ColKey{});
        }
    }
}

TEST_CASE("SharedRealm: compact on launch") {
    // Make compactable Realm
    TestFile config;
    config.automatic_change_notifications = false;
    int num_opens = 0;
    config.should_compact_on_launch_function = [&](uint64_t total_bytes, uint64_t used_bytes) {
        REQUIRE(total_bytes > used_bytes);
        num_opens++;
        return num_opens != 2;
    };
    config.schema = Schema{
        {"object", {{"value", PropertyType::String}}},
    };
    REQUIRE(num_opens == 0);
    auto r = Realm::get_shared_realm(config);
    REQUIRE(num_opens == 1);
    r->begin_transaction();
    auto table = r->read_group().get_table("class_object");
    size_t count = 1000;
    for (size_t i = 0; i < count; ++i)
        table->create_object().set_all(util::format("Foo_%1", i % 10).c_str());
    r->commit_transaction();
    REQUIRE(table->size() == count);
    r->close();

    SECTION("compact reduces the file size") {
#ifndef _WIN32
        // Confirm expected sizes before and after opening the Realm
        size_t size_before = size_t(util::File(config.path).get_size());
        r = Realm::get_shared_realm(config);
        REQUIRE(num_opens == 2);
        r->close();
        REQUIRE(size_t(util::File(config.path).get_size()) == size_before); // File size after returning false
        r = Realm::get_shared_realm(config);
        REQUIRE(num_opens == 3);
        REQUIRE(size_t(util::File(config.path).get_size()) < size_before); // File size after returning true

        // Validate that the file still contains what it should
        REQUIRE(r->read_group().get_table("class_object")->size() == count);

        // Registering for a collection notification shouldn't crash when compact on launch is used.
        Results results(r, r->read_group().get_table("class_object"));
        results.add_notification_callback([](CollectionChangeSet const&) {});
        r->close();
#endif
    }

    SECTION("compact function does not get invoked if realm is open on another thread") {
        config.scheduler = util::Scheduler::make_frozen(VersionID());
        r = Realm::get_shared_realm(config);
        REQUIRE(num_opens == 2);
        JoiningThread([&] {
            auto r2 = Realm::get_shared_realm(config);
            REQUIRE(num_opens == 2);
        });
        r->close();
        JoiningThread([&] {
            auto r3 = Realm::get_shared_realm(config);
            REQUIRE(num_opens == 3);
        });
    }
}

struct ModeAutomatic {
    static constexpr SchemaMode mode = SchemaMode::Automatic;
    static constexpr bool should_call_init_on_version_bump = false;
};
struct ModeAdditive {
    static constexpr SchemaMode mode = SchemaMode::AdditiveExplicit;
    static constexpr bool should_call_init_on_version_bump = false;
};
struct ModeManual {
    static constexpr SchemaMode mode = SchemaMode::Manual;
    static constexpr bool should_call_init_on_version_bump = false;
};
struct ModeSoftResetFile {
    static constexpr SchemaMode mode = SchemaMode::SoftResetFile;
    static constexpr bool should_call_init_on_version_bump = true;
};
struct ModeHardResetFile {
    static constexpr SchemaMode mode = SchemaMode::HardResetFile;
    static constexpr bool should_call_init_on_version_bump = true;
};

TEMPLATE_TEST_CASE("SharedRealm: update_schema with initialization_function", "[init][update schema]", ModeAutomatic,
                   ModeAdditive, ModeManual, ModeSoftResetFile, ModeHardResetFile)
{
    TestFile config;
    config.schema_mode = TestType::mode;
    bool initialization_function_called = false;
    uint64_t schema_version_in_callback = -1;
    Schema schema_in_callback;
    auto initialization_function = [&initialization_function_called, &schema_version_in_callback,
                                    &schema_in_callback](auto shared_realm) {
        REQUIRE(shared_realm->is_in_transaction());
        initialization_function_called = true;
        schema_version_in_callback = shared_realm->schema_version();
        schema_in_callback = shared_realm->schema();
    };

    Schema schema{
        {"object", {{"value", PropertyType::String}}},
    };

    SECTION("call initialization function directly by update_schema") {
        // Open in dynamic mode with no schema specified
        auto realm = Realm::get_shared_realm(config);
        REQUIRE_FALSE(initialization_function_called);

        realm->update_schema(schema, 0, nullptr, initialization_function);
        REQUIRE(initialization_function_called);
        REQUIRE(schema_version_in_callback == 0);
        REQUIRE(schema_in_callback.compare(schema).size() == 0);
    }

    config.schema_version = 0;
    config.schema = schema;

    SECTION("initialization function should be called for unversioned realm") {
        config.initialization_function = initialization_function;
        Realm::get_shared_realm(config);
        REQUIRE(initialization_function_called);
        REQUIRE(schema_version_in_callback == 0);
        REQUIRE(schema_in_callback.compare(schema).size() == 0);
    }

    SECTION("initialization function for versioned realm") {
        // Initialize v0
        Realm::get_shared_realm(config);

        config.schema_version = 1;
        config.initialization_function = initialization_function;
        Realm::get_shared_realm(config);
        REQUIRE(initialization_function_called == TestType::should_call_init_on_version_bump);
        if (TestType::should_call_init_on_version_bump) {
            REQUIRE(schema_version_in_callback == 1);
            REQUIRE(schema_in_callback.compare(schema).size() == 0);
        }
    }
}

TEST_CASE("BindingContext is notified about delivery of change notifications") {
    _impl::RealmCoordinator::assert_no_open_realms();
    InMemoryTestFile config;
    config.automatic_change_notifications = false;

    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"object", {{"value", PropertyType::Int}}},
    });

    auto coordinator = _impl::RealmCoordinator::get_coordinator(config.path);
    auto table = r->read_group().get_table("class_object");

    SECTION("BindingContext notified even if no callbacks are registered") {
        static int binding_context_start_notify_calls = 0;
        static int binding_context_end_notify_calls = 0;
        struct Context : BindingContext {
            void will_send_notifications() override
            {
                ++binding_context_start_notify_calls;
            }

            void did_send_notifications() override
            {
                ++binding_context_end_notify_calls;
            }
        };
        r->m_binding_context.reset(new Context());

        SECTION("local commit") {
            binding_context_start_notify_calls = 0;
            binding_context_end_notify_calls = 0;
            coordinator->on_change();
            r->begin_transaction();
            REQUIRE(binding_context_start_notify_calls == 1);
            REQUIRE(binding_context_end_notify_calls == 1);
            r->cancel_transaction();
        }

        SECTION("remote commit") {
            binding_context_start_notify_calls = 0;
            binding_context_end_notify_calls = 0;
            JoiningThread([&] {
                auto r2 = coordinator->get_realm(util::Scheduler::make_frozen(VersionID()));
                r2->begin_transaction();
                auto table2 = r2->read_group().get_table("class_object");
                table2->create_object();
                r2->commit_transaction();
            });
            advance_and_notify(*r);
            REQUIRE(binding_context_start_notify_calls == 1);
            REQUIRE(binding_context_end_notify_calls == 1);
        }
    }

    SECTION("notify BindingContext before and after sending notifications") {
        static int binding_context_start_notify_calls = 0;
        static int binding_context_end_notify_calls = 0;
        static int notification_calls = 0;

        auto col = table->get_column_key("value");
        Results results1(r, table->where().greater_equal(col, 0));
        Results results2(r, table->where().less(col, 10));

        auto token1 = results1.add_notification_callback([&](CollectionChangeSet) {
            ++notification_calls;
        });

        auto token2 = results2.add_notification_callback([&](CollectionChangeSet) {
            ++notification_calls;
        });

        struct Context : BindingContext {
            void will_send_notifications() override
            {
                REQUIRE(notification_calls == 0);
                REQUIRE(binding_context_end_notify_calls == 0);
                ++binding_context_start_notify_calls;
            }

            void did_send_notifications() override
            {
                REQUIRE(notification_calls == 2);
                REQUIRE(binding_context_start_notify_calls == 1);
                ++binding_context_end_notify_calls;
            }
        };
        r->m_binding_context.reset(new Context());

        SECTION("local commit") {
            binding_context_start_notify_calls = 0;
            binding_context_end_notify_calls = 0;
            notification_calls = 0;
            coordinator->on_change();
            r->begin_transaction();
            table->create_object();
            r->commit_transaction();
            REQUIRE(binding_context_start_notify_calls == 1);
            REQUIRE(binding_context_end_notify_calls == 1);
        }

        SECTION("remote commit") {
            binding_context_start_notify_calls = 0;
            binding_context_end_notify_calls = 0;
            notification_calls = 0;
            JoiningThread([&] {
                auto r2 = coordinator->get_realm(util::Scheduler::make_frozen(VersionID()));
                r2->begin_transaction();
                auto table2 = r2->read_group().get_table("class_object");
                table2->create_object();
                r2->commit_transaction();
            });
            advance_and_notify(*r);
            REQUIRE(binding_context_start_notify_calls == 1);
            REQUIRE(binding_context_end_notify_calls == 1);
        }
    }

    SECTION("did_send() is skipped if the Realm is closed first") {
        Results results(r, table->where());
        bool do_close = true;
        auto token = results.add_notification_callback([&](CollectionChangeSet) {
            if (do_close)
                r->close();
        });

        struct FailOnDidSend : BindingContext {
            void did_send_notifications() override
            {
                FAIL("did_send_notifications() should not have been called");
            }
        };
        struct CloseOnWillChange : FailOnDidSend {
            Realm& realm;
            CloseOnWillChange(Realm& realm)
                : realm(realm)
            {
            }

            void will_send_notifications() override
            {
                realm.close();
            }
        };

        SECTION("closed in notification callback for notify()") {
            r->m_binding_context.reset(new FailOnDidSend);
            coordinator->on_change();
            r->notify();
        }

        SECTION("closed in notification callback for refresh()") {
            do_close = false;
            coordinator->on_change();
            r->notify();
            do_close = true;

            JoiningThread([&] {
                auto r = coordinator->get_realm(util::Scheduler::make_frozen(VersionID()));
                r->begin_transaction();
                r->read_group().get_table("class_object")->create_object();
                r->commit_transaction();
            });

            r->m_binding_context.reset(new FailOnDidSend);
            coordinator->on_change();
            r->refresh();
        }

        SECTION("closed in will_send() for notify()") {
            r->m_binding_context.reset(new CloseOnWillChange(*r));
            coordinator->on_change();
            r->notify();
        }

        SECTION("closed in will_send() for refresh()") {
            do_close = false;
            coordinator->on_change();
            r->notify();
            do_close = true;

            JoiningThread([&] {
                auto r = coordinator->get_realm(util::Scheduler::make_frozen(VersionID()));
                r->begin_transaction();
                r->read_group().get_table("class_object")->create_object();
                r->commit_transaction();
            });

            r->m_binding_context.reset(new CloseOnWillChange(*r));
            coordinator->on_change();
            r->refresh();
        }
    }
#ifdef _WIN32
    _impl::RealmCoordinator::clear_all_caches();
#endif
}

TEST_CASE("RealmCoordinator: get_unbound_realm()") {
    TestFile config;
    config.cache = true;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Int}}},
    };

    ThreadSafeReference ref;
    JoiningThread([&] {
        ref = _impl::RealmCoordinator::get_coordinator(config)->get_unbound_realm();
    });

    SECTION("checks thread after being resolved") {
        auto realm = Realm::get_shared_realm(std::move(ref));
        REQUIRE_NOTHROW(realm->verify_thread());
        JoiningThread([&] {
            REQUIRE_EXCEPTION(realm->verify_thread(), WrongThread, "Realm accessed from incorrect thread.");
        });
    }

    SECTION("delivers notifications to the thread it is resolved on") {
#ifndef _WIN32
        if (!util::EventLoop::has_implementation())
            return;
        auto realm = Realm::get_shared_realm(std::move(ref));
        Results results(realm, ObjectStore::table_for_object_type(realm->read_group(), "object")->where());
        bool called = false;
        auto token = results.add_notification_callback([&](CollectionChangeSet) {
            called = true;
        });
        util::EventLoop::main().run_until([&] {
            return called;
        });
#endif
    }

    SECTION("resolves to existing cached Realm for the thread if caching is enabled") {
        auto r1 = Realm::get_shared_realm(config);
        auto r2 = Realm::get_shared_realm(std::move(ref));
        REQUIRE(r1 == r2);
    }

    SECTION("resolves to a new Realm if caching is disabled") {
        config.cache = false;
        auto r1 = Realm::get_shared_realm(config);
        auto r2 = Realm::get_shared_realm(std::move(ref));
        REQUIRE(r1 != r2);

        // New unbound with cache disabled
        JoiningThread([&] {
            ref = _impl::RealmCoordinator::get_coordinator(config)->get_unbound_realm();
        });
        auto r3 = Realm::get_shared_realm(std::move(ref));
        REQUIRE(r1 != r3);
        REQUIRE(r2 != r3);

        // New local with cache enabled should grab the resolved unbound
        config.cache = true;
        auto r4 = Realm::get_shared_realm(config);
        REQUIRE(r4 == r2);
    }
}

TEST_CASE("Immutable Realms") {
    TestFile config; // can't be in-memory because we have to write a file to open in immutable mode
    config.schema_version = 1;
    config.schema = Schema{{"object", {{"value", PropertyType::Int}}}};

    {
        auto realm = Realm::get_shared_realm(config);
        realm->begin_transaction();
        realm->read_group().get_table("class_object")->create_object();
        realm->commit_transaction();
    }

    config.schema_mode = SchemaMode::Immutable;
    auto realm = Realm::get_shared_realm(config);
    realm->read_group();

    SECTION("unsupported functions") {
        SECTION("update_schema()") {
            REQUIRE_THROWS_AS(realm->compact(), WrongTransactionState);
        }
        SECTION("begin_transaction()") {
            REQUIRE_THROWS_AS(realm->begin_transaction(), WrongTransactionState);
        }
        SECTION("async_begin_transaction()") {
            REQUIRE_THROWS_AS(realm->async_begin_transaction(nullptr), WrongTransactionState);
        }
        SECTION("refresh()") {
            REQUIRE_THROWS_AS(realm->refresh(), WrongTransactionState);
        }
        SECTION("compact()") {
            REQUIRE_THROWS_AS(realm->compact(), WrongTransactionState);
        }
    }

    SECTION("supported functions") {
        SECTION("is_in_transaction()") {
            REQUIRE_FALSE(realm->is_in_transaction());
        }
        SECTION("is_in_async_transaction()") {
            REQUIRE_FALSE(realm->is_in_transaction());
        }
        SECTION("freeze()") {
            std::shared_ptr<Realm> frozen;
            REQUIRE_NOTHROW(frozen = realm->freeze());
            REQUIRE(frozen->read_group().get_table("class_object")->size() == 1);
            REQUIRE_NOTHROW(frozen = Realm::get_frozen_realm(config, realm->read_transaction_version()));
            REQUIRE(frozen->read_group().get_table("class_object")->size() == 1);
        }
        SECTION("notify()") {
            REQUIRE_NOTHROW(realm->notify());
        }
        SECTION("is_in_read_transaction()") {
            REQUIRE(realm->is_in_read_transaction());
        }
        SECTION("last_seen_transaction_version()") {
            REQUIRE(realm->last_seen_transaction_version() == 1);
        }
        SECTION("get_number_of_versions()") {
            REQUIRE(realm->get_number_of_versions() == 1);
        }
        SECTION("read_transaction_version()") {
            REQUIRE(realm->read_transaction_version() == VersionID{1, 0});
        }
        SECTION("current_transaction_version()") {
            REQUIRE(realm->current_transaction_version() == VersionID{1, 0});
        }
        SECTION("latest_snapshot_version()") {
            REQUIRE(realm->latest_snapshot_version() == 1);
        }
        SECTION("duplicate()") {
            auto duplicate = realm->duplicate();
            REQUIRE(duplicate->get_table("class_object")->size() == 1);
        }
        SECTION("invalidate()") {
            REQUIRE_NOTHROW(realm->invalidate());
            REQUIRE_FALSE(realm->is_in_read_transaction());
            REQUIRE(realm->read_group().get_table("class_object")->size() == 1);
        }
        SECTION("close()") {
            REQUIRE_NOTHROW(realm->close());
            REQUIRE(realm->is_closed());
        }
        SECTION("has_pending_async_work()") {
            REQUIRE_FALSE(realm->has_pending_async_work());
        }
        SECTION("wait_for_change()") {
            REQUIRE_FALSE(realm->wait_for_change());
        }
    }
}

TEST_CASE("KeyPathMapping generation") {
    TestFile config;
    realm::query_parser::KeyPathMapping mapping;

    SECTION("class aliasing") {
        Schema schema = {
            {"PersistedName", {{"age", PropertyType::Int}}, {}, "AlternativeName"},
            {"class_with_policy",
             {{"value", PropertyType::Int},
              {"child", PropertyType::Object | PropertyType::Nullable, "class_with_policy"}},
             {{"parents", PropertyType::LinkingObjects | PropertyType::Array, "class_with_policy", "child"}},
             "ClassWithPolicy"},
        };
        schema.validate();
        config.schema = schema;
        auto realm = Realm::get_shared_realm(config);
        realm::populate_keypath_mapping(mapping, *realm);
        REQUIRE(mapping.has_table_mapping("AlternativeName"));
        REQUIRE("class_PersistedName" == mapping.get_table_mapping("AlternativeName"));

        auto table = realm->read_group().get_table("class_class_with_policy");
        std::vector<Mixed> args{0};
        auto q = table->query("parents.value = $0", args, mapping);
        REQUIRE(q.count() == 0);
    }
}

TEST_CASE("Concurrent operations") {
    SECTION("Async commits together with online compaction") {
        // This is a reproduction test for issue https://github.com/realm/realm-dart/issues/1396
        // First create a relatively large realm, then delete the content and do some more
        // commits using async commits. If a compaction is started when doing an async commit
        // then the subsequent committing done in the helper thread will illegally COW the
        // top array. When the next mutation is done, the top array will be reported as being
        // already freed.
        TestFile config;
        config.schema_version = 1;
        config.schema = Schema{{"object", {{"value", PropertyType::Int}}}};

        auto realm_1 = Realm::get_shared_realm(config);
        Results res(realm_1, realm_1->read_group().get_table("class_object")->where());
        auto realm_2 = Realm::get_shared_realm(config);

        {
            // Create a lot of objects
            realm_2->begin_transaction();
            auto table = realm_2->read_group().get_table("class_object");
            for (int i = 0; i < 400000; i++) {
                table->create_object().set("value", i);
            }
            realm_2->commit_transaction();
        }

        int commit_1 = 0;
        int commit_2 = 0;

        for (int i = 0; i < 4; i++) {
            realm_1->async_begin_transaction([&]() {
                // Clearing the DB will reduce the need for space
                // This will trigger an online compaction
                // Before the fix, the probram would crash here next time around.
                res.clear();
                realm_1->async_commit_transaction([&](std::exception_ptr) {
                    commit_1++;
                });
            });
            realm_2->async_begin_transaction([&]() {
                // Make sure we will continue to have something to delete
                auto table = realm_2->read_group().get_table("class_object");
                for (int i = 0; i < 100; i++) {
                    table->create_object().set("value", i);
                }
                realm_2->async_commit_transaction([&](std::exception_ptr) {
                    commit_2++;
                });
            });
        }

        util::EventLoop::main().run_until([&] {
            return commit_1 == 4 && commit_2 == 4;
        });
    }

    SECTION("No open realms") {
        // This is just to check that the section above did not leave any realms open
        _impl::RealmCoordinator::assert_no_open_realms();
    }
}

TEST_CASE("Notification logging") {
    using namespace std::chrono_literals;
    TestFile config;
    // util::LogCategory::realm.set_default_level_threshold(util::Logger::Level::all);
    config.schema_version = 1;
    config.schema = Schema{{"object", {{"value", PropertyType::Int}}}};

    auto realm = Realm::get_shared_realm(config);
    auto table = realm->read_group().get_table("class_object");
    int changed = 0;
    Results res(realm, table->query("value == 5"));
    auto token = res.add_notification_callback([&changed](CollectionChangeSet const&) {
        changed++;
    });

    int commit_nr = 0;
    util::EventLoop::main().run_until([&] {
        for (int64_t i = 0; i < 10; i++) {
            realm->begin_transaction();
            table->create_object().set("value", i);
            realm->commit_transaction();
            std::this_thread::sleep_for(2ms);
        }
        return ++commit_nr == 10;
    });
}
