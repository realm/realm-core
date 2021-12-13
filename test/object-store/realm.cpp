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

#if REALM_ENABLE_SYNC
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/sync/impl/sync_metadata.hpp>
#endif

#include <realm/db.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/fifo_helper.hpp>
#include <realm/util/scope_exit.hpp>

#include <external/json/json.hpp>

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
    config.cache = true;
    config.schema_version = 1;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Int}}},
    };

    SECTION("should return the same instance when caching is enabled") {
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
            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }

        SECTION("schema without schema version") {
            config.schema_version = ObjectStore::NotVersioned;
            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }

        SECTION("migration function for immutable") {
            config.schema_mode = SchemaMode::Immutable;
            config.migration_function = [](auto, auto, auto) {};
            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }

        SECTION("migration function for read-only") {
            config.schema_mode = SchemaMode::ReadOnly;
            config.migration_function = [](auto, auto, auto) {};
            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }

        SECTION("migration function for additive discovered") {
            config.schema_mode = SchemaMode::AdditiveDiscovered;
            config.migration_function = [](auto, auto, auto) {};
            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }

        SECTION("migration function for additive explicit") {
            config.schema_mode = SchemaMode::AdditiveExplicit;
            config.migration_function = [](auto, auto, auto) {};
            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }

        SECTION("initialization function for immutable") {
            config.schema_mode = SchemaMode::Immutable;
            config.initialization_function = [](auto) {};
            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }

        SECTION("initialization function for read-only") {
            config.schema_mode = SchemaMode::ReadOnly;
            config.initialization_function = [](auto) {};
            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }
    }

    SECTION("should reject mismatched config") {
        SECTION("cached") {
        }
        SECTION("uncached") {
            config.cache = false;
        }

        SECTION("schema version") {
            auto realm = Realm::get_shared_realm(config);
            config.schema_version = 2;
            REQUIRE_THROWS(Realm::get_shared_realm(config));

            config.schema = util::none;
            config.schema_version = ObjectStore::NotVersioned;
            REQUIRE_NOTHROW(Realm::get_shared_realm(config));
        }

        SECTION("schema mode") {
            auto realm = Realm::get_shared_realm(config);
            config.schema_mode = SchemaMode::Manual;
            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }

        SECTION("durability") {
            auto realm = Realm::get_shared_realm(config);
            config.in_memory = true;
            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }

        SECTION("schema") {
            auto realm = Realm::get_shared_realm(config);
            config.schema = Schema{
                {"object", {{"value", PropertyType::Int}, {"value2", PropertyType::Int}}},
            };
            REQUIRE_THROWS(Realm::get_shared_realm(config));
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
        realm::util::remove_dir_recursive(fallback_dir);
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
        realm::util::remove_dir_recursive(fallback_dir);
    }
#endif

    SECTION("should verify that the schema is valid") {
        config.schema =
            Schema{{"object",
                    {{"value", PropertyType::Int}},
                    {{"invalid backlink", PropertyType::LinkingObjects | PropertyType::Array, "object", "value"}}}};
        REQUIRE_THROWS_WITH(Realm::get_shared_realm(config),
                            Catch::Matchers::Contains("origin of linking objects property"));
    }

    SECTION("should apply the schema if one is supplied") {
        Realm::get_shared_realm(config);

        {
            Group g(config.path);
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
        config.cache = false;
        config.schema_mode = SchemaMode::AdditiveExplicit;
        config.schema_version = 0;

        auto realm = Realm::get_shared_realm(config);
        REQUIRE(realm->schema().size() == 1);

        auto& db = TestHelper::get_db(realm);
        auto rt = db->start_read();
        VersionID old_version = rt->get_version_of_current_transaction();
        rt = nullptr;
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
        TestHelper::begin_read(old_realm, old_version);
        REQUIRE(old_realm->schema().size() == 1);
    }

    SECTION("should sensibly handle opening an uninitialized file without a schema specified") {
        SECTION("cached") {
        }
        SECTION("uncached") {
            config.cache = false;
        }

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
            REQUIRE_THROWS_WITH(realm->refresh(), "Can't refresh a read-only Realm.");
        }
    }

    SECTION("should support using different table subsets on different threads") {
        config.cache = false;
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

// The ExternalCommitHelper implementation on Windows doesn't rely on files
#ifndef _WIN32
    SECTION("should throw when creating the notification pipe fails") {
        REQUIRE(util::try_make_dir(config.path + ".note"));
        auto sys_fallback_file =
            util::format("%1realm_%2.note", util::normalize_dir(DBOptions::get_sys_tmp_dir()),
                         std::hash<std::string>()(config.path)); // Mirror internal implementation
        REQUIRE(util::try_make_dir(sys_fallback_file));
        REQUIRE_THROWS(Realm::get_shared_realm(config));
        util::remove_dir(config.path + ".note");
        util::remove_dir(sys_fallback_file);
    }
#endif

    SECTION("should get different instances on different threads") {
        auto realm1 = Realm::get_shared_realm(config);
        std::thread([&] {
            auto realm2 = Realm::get_shared_realm(config);
            REQUIRE(realm1 != realm2);
        }).join();
    }

    SECTION("should detect use of Realm on incorrect thread") {
        auto realm = Realm::get_shared_realm(config);
        std::thread([&] {
            REQUIRE_THROWS_AS(realm->verify_thread(), IncorrectThreadException);
        }).join();
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
        bool can_deliver_notifications() const noexcept override
        {
            return false;
        }
        void set_notify_callback(std::function<void()>) override {}
        void notify() override {}

    protected:
        size_t m_id;
    };

    SECTION("should get different instances for different explicitly different schedulers") {
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
        config.scheduler = std::make_shared<SimpleScheduler>(1);
        auto realm = Realm::get_shared_realm(config);
        std::thread([&] {
            REQUIRE_NOTHROW(realm->verify_thread());
        }).join();
    }

    SECTION("should get same instance for same explicit execution context on different thread") {
        config.scheduler = std::make_shared<SimpleScheduler>(1);
        auto realm1 = Realm::get_shared_realm(config);
        std::thread([&] {
            auto realm2 = Realm::get_shared_realm(config);
            REQUIRE(realm1 == realm2);
        }).join();
    }

    SECTION("should not modify the schema when fetching from the cache") {
        auto realm = Realm::get_shared_realm(config);
        auto object_schema = &*realm->schema().find("object");
        Realm::get_shared_realm(config);
        REQUIRE(object_schema == &*realm->schema().find("object"));
    }

    SECTION("should not use cached frozen Realm if versions don't match") {
        auto realm = Realm::get_shared_realm(config);
        realm->read_group();
        auto frozen1 = realm->freeze();
        frozen1->read_group();

        REQUIRE(frozen1 != realm);
        REQUIRE(realm->read_transaction_version() == frozen1->read_transaction_version());

        auto table = realm->read_group().get_table("class_object");
        realm->begin_transaction();
        Obj obj = table->create_object();
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
}

#if REALM_ENABLE_SYNC
TEST_CASE("Get Realm using Async Open", "[asyncOpen]") {
    if (!util::EventLoop::has_implementation())
        return;

    TestSyncManager init_sync_manager;
    SyncTestFile config(init_sync_manager.app(), "default");
    config.cache = false;
    ObjectSchema object_schema = {"object",
                                  {
                                      {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                      {"value", PropertyType::Int},
                                  }};
    config.schema = Schema{object_schema};
    SyncTestFile config2(init_sync_manager.app(), "default");
    config2.schema = config.schema;
    config2.cache = false;

    std::mutex mutex;
    SECTION("can open synced Realms that don't already exist") {
        ThreadSafeReference realm_ref;
        std::atomic<bool> called{false};
        auto task = Realm::get_synchronized_realm(config);
        task->start([&](auto ref, auto error) {
            std::lock_guard<std::mutex> lock(mutex);
            REQUIRE(!error);
            called = true;
            realm_ref = std::move(ref);
        });
        util::EventLoop::main().run_until([&] {
            return called.load();
        });
        std::lock_guard<std::mutex> lock(mutex);
        REQUIRE(called);
        REQUIRE(realm_ref);
        REQUIRE(Realm::get_shared_realm(std::move(realm_ref))->read_group().get_table("class_object"));
    }

    SECTION("can write a realm file without client file id") {
        ThreadSafeReference realm_ref;
        SyncTestFile config3(init_sync_manager.app(), "default");
        config3.schema = config.schema;
        config3.cache = false;
        uint64_t client_file_id;

        // Create some content
        auto origin = Realm::get_shared_realm(config);
        origin->begin_transaction();
        origin->read_group().get_table("class_object")->create_object_with_primary_key(0);
        origin->commit_transaction();
        wait_for_upload(*origin);

        // Create realm file without client file id
        {
            auto task = Realm::get_synchronized_realm(config2);
            task->start([&](ThreadSafeReference ref, std::exception_ptr error) {
                std::lock_guard<std::mutex> lock(mutex);
                REQUIRE(!error);
                realm_ref = std::move(ref);
            });
            util::EventLoop::main().run_until([&] {
                std::lock_guard<std::mutex> lock(mutex);
                return bool(realm_ref);
            });
            // Write some data
            SharedRealm realm = Realm::get_shared_realm(std::move(realm_ref));
            realm->begin_transaction();
            realm->read_group().get_table("class_object")->create_object_with_primary_key(2);
            realm->commit_transaction();
            wait_for_upload(*realm);
            wait_for_download(*realm);
            client_file_id = realm->read_group().get_sync_file_id();
            realm->write_copy(config3.path, BinaryData());
        }

        // Create some more content on the server
        origin->begin_transaction();
        origin->read_group().get_table("class_object")->create_object_with_primary_key(7);
        origin->commit_transaction();
        wait_for_upload(*origin);

        // Now open a realm based on the realm file created above
        auto realm = Realm::get_shared_realm(config3);
        wait_for_download(*realm);
        // Make sure we have got a new client file id
        REQUIRE(realm->read_group().get_sync_file_id() != client_file_id);
        REQUIRE(realm->read_group().get_table("class_object")->size() == 3);

        // Check that we can continue committing to this realm
        realm->begin_transaction();
        realm->read_group().get_table("class_object")->create_object_with_primary_key(5);
        realm->commit_transaction();
        wait_for_upload(*realm);

        // Check that this change is now in the original realm
        wait_for_download(*origin);
        origin->refresh();
        REQUIRE(origin->read_group().get_table("class_object")->size() == 4);
    }

    SECTION("downloads Realms which exist on the server") {
        {
            auto realm = Realm::get_shared_realm(config2);
            realm->begin_transaction();
            realm->read_group().get_table("class_object")->create_object_with_primary_key(0);
            realm->commit_transaction();
            wait_for_upload(*realm);
        }

        ThreadSafeReference realm_ref;
        std::atomic<bool> called{false};
        auto task = Realm::get_synchronized_realm(config);
        task->start([&](auto ref, auto error) {
            std::lock_guard<std::mutex> lock(mutex);
            REQUIRE(!error);
            called = true;
            realm_ref = std::move(ref);
        });
        util::EventLoop::main().run_until([&] {
            return called.load();
        });
        std::lock_guard<std::mutex> lock(mutex);
        REQUIRE(called);
        REQUIRE(Realm::get_shared_realm(std::move(realm_ref))->read_group().get_table("class_object"));
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

        std::shared_ptr<AsyncOpenTask> task = Realm::get_synchronized_realm(config);
        std::shared_ptr<AsyncOpenTask> task2 = Realm::get_synchronized_realm(config);
        REQUIRE(task);
        REQUIRE(task2);
        auto realm = Realm::get_shared_realm(config);
        realm->begin_transaction(); // block sync from writing until we cancel
        task->register_download_progress_notifier([&](uint64_t, uint64_t) {
            std::lock_guard<std::mutex> guard(mutex);
            REQUIRE(!task1_completed);
            progress_notifier1_called = true;
        });
        task2->register_download_progress_notifier([&](uint64_t, uint64_t) {
            std::lock_guard<std::mutex> guard(mutex);
            REQUIRE(!task2_completed);
            progress_notifier2_called = true;
        });
        task->start([&](ThreadSafeReference realm_ref, std::exception_ptr err) {
            REQUIRE(!err);
            SharedRealm realm = Realm::get_shared_realm(std::move(realm_ref));
            REQUIRE(realm);
            std::lock_guard<std::mutex> guard(mutex);
            task1_completed = true;
        });
        task->cancel();
        task2->start([&](ThreadSafeReference realm_ref, std::exception_ptr err) {
            REQUIRE(!err);
            SharedRealm realm = Realm::get_shared_realm(std::move(realm_ref));
            REQUIRE(realm);
            std::lock_guard<std::mutex> guard(mutex);
            task2_completed = true;
        });
        realm->cancel_transaction(); // unblock sync
        util::EventLoop::main().run_until([&] {
            std::lock_guard<std::mutex> guard(mutex);
            return task2_completed;
        });
        std::lock_guard<std::mutex> guard(mutex);
        REQUIRE(!progress_notifier1_called);
        REQUIRE(!task1_completed);
        REQUIRE(progress_notifier2_called);
        REQUIRE(task2_completed);
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

        std::atomic<bool> called{false};
        auto task = Realm::get_synchronized_realm(config);
        task->start([&](auto ref, auto error) {
            std::lock_guard<std::mutex> lock(mutex);
            REQUIRE(!error);
            called = true;

            REQUIRE(Realm::get_shared_realm(std::move(ref))->read_group().get_table("class_object")->size() == 1);
        });
        util::EventLoop::main().run_until([&] {
            return called.load();
        });
        std::lock_guard<std::mutex> lock(mutex);
        REQUIRE(called);
    }

    SECTION("can download multiple Realms at a time") {
        SyncTestFile config1(init_sync_manager.app(), "realm1");
        SyncTestFile config2(init_sync_manager.app(), "realm2");
        SyncTestFile config3(init_sync_manager.app(), "realm3");
        SyncTestFile config4(init_sync_manager.app(), "realm4");

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

    // Create a token which can be parsed as a JWT but is not valid
    std::string unencoded_body = nlohmann::json({{"exp", 123}, {"iat", 456}}).dump();
    std::string encoded_body;
    encoded_body.resize(util::base64_encoded_size(unencoded_body.size()));
    util::base64_encode(unencoded_body.data(), unencoded_body.size(), &encoded_body[0], encoded_body.size());
    auto invalid_token = "." + encoded_body + ".";

    // Token refreshing requires that we have app metadata and we can't fetch
    // it normally, so just stick some fake values in
    init_sync_manager.app()->sync_manager()->perform_metadata_update([&](const SyncMetadataManager& manager) {
        manager.set_app_metadata("GLOBAL", "location", "hostname", "ws_hostname");
    });

    SECTION("can async open while waiting for a token refresh") {
        SyncTestFile config(init_sync_manager.app(), "realm");
        auto valid_token = config.sync_config->user->access_token();
        config.sync_config->user->update_access_token(std::move(invalid_token));

        std::atomic<bool> called{false};
        auto task = Realm::get_synchronized_realm(config);
        task->start([&](auto ref, auto error) {
            std::lock_guard<std::mutex> lock(mutex);
            REQUIRE(ref);
            REQUIRE(!error);
            called = true;
        });

        auto body = nlohmann::json({{"access_token", valid_token}}).dump();
        init_sync_manager.network_callback(app::Response{200, 0, {}, body});
        util::EventLoop::main().run_until([&] {
            return called.load();
        });
        std::lock_guard<std::mutex> lock(mutex);
        REQUIRE(called);
    }

    SECTION("cancels download and reports an error on auth error") {
        SyncTestFile config(init_sync_manager.app(), "realm");
        config.sync_config->user->update_refresh_token(std::string(invalid_token));
        config.sync_config->user->update_access_token(std::move(invalid_token));

        bool got_error = false;
        config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError) {
            got_error = true;
        };
        std::atomic<bool> called{false};
        auto task = Realm::get_synchronized_realm(config);
        task->start([&](auto ref, auto error) {
            std::lock_guard<std::mutex> lock(mutex);
            REQUIRE(error);
            REQUIRE(!ref);
            called = true;
        });
        init_sync_manager.network_callback(app::Response{403});
        util::EventLoop::main().run_until([&] {
            return called.load();
        });
        std::lock_guard<std::mutex> lock(mutex);
        REQUIRE(called);
        REQUIRE(got_error);
    }

    SECTION("can observe an added class in read-only mode") {
        {
            SharedRealm realm = Realm::get_shared_realm(config);
            wait_for_upload(*realm);
            realm->close();
        }

        Schema with_added_object = Schema{object_schema,
                                          {"added",
                                           {
                                               {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                           }}};

        {
            config2.schema = with_added_object;
            auto realm = Realm::get_shared_realm(config2);
            realm->begin_transaction();
            realm->read_group().get_table("class_object")->create_object_with_primary_key(0);
            realm->read_group().get_table("class_added")->create_object_with_primary_key(0);
            realm->commit_transaction();
            wait_for_upload(*realm);
        }

        {
            config.schema = with_added_object;
            config.schema_mode = SchemaMode::ReadOnly;
            SharedRealm realm = Realm::get_shared_realm(config);
            REQUIRE(!realm->read_group().get_table("class_added"));
            wait_for_upload(*realm);
            wait_for_download(*realm);
            realm->refresh();
            TableRef added = realm->read_group().get_table("class_added");
            REQUIRE(added);
            Results results(realm, added);
            NotificationToken token =
                results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr) {});
            realm->close();
        }
    }
}
#endif

TEST_CASE("SharedRealm: async_writes") {
    if (!util::EventLoop::has_implementation())
        return;

    TestFile config;
    config.cache = false;
    config.schema_version = 0;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Int}, {"ints", PropertyType::Array | PropertyType::Int}}},
    };
    bool done = false;
    auto realm = Realm::get_shared_realm(config);
    int write_nr = 0;
    int commit_nr = 0;

    SECTION("async commit transaction") {
        realm->async_begin_transaction([&]() {
            REQUIRE(write_nr == 0);
            ++write_nr;
            auto table = realm->read_group().get_table("class_object");
            auto col = table->get_column_key("value");
            table->create_object().set(col, 45);
            realm->async_commit_transaction([&]() {
                REQUIRE(commit_nr == 0);
                ++commit_nr;
            });
        });
        for (int expected = 1; expected < 1000; ++expected) {
            realm->async_begin_transaction([&, expected]() {
                REQUIRE(write_nr == expected);
                ++write_nr;
                auto table = realm->read_group().get_table("class_object");
                auto col = table->get_column_key("value");
                auto o = table->get_object(0);
                o.set(col, o.get<int64_t>(col) + 37);
                realm->async_commit_transaction(
                    [&]() {
                        ++commit_nr;
                        done = commit_nr == 1000;
                    },
                    true);
            });
        }
        util::EventLoop::main().run_until([&] {
            return done;
        });
        REQUIRE(done);
    }
    SECTION("realm closed") {
        bool timeout = false;
        auto has_timer = realm->scheduler()->set_timeout_callback(100, [&timeout]() {
            timeout = true;
        });
        if (has_timer) {
            realm->async_begin_transaction([&] {
                // We should never get here as the realm is closed
                done = true;
            });
            realm->close();
            util::EventLoop::main().run_until([&] {
                return done || timeout;
            });
            REQUIRE(!done);
        }
    }
    SECTION("realm closed when sync in progress") {
        bool persisted = false;
        realm->async_begin_transaction([&] {
            auto table = realm->read_group().get_table("class_object");
            auto col = table->get_column_key("value");
            table->create_object().set(col, 45);
            realm->async_commit_transaction([&]() {
                persisted = true;
            });
            done = true;
        });
        util::EventLoop::main().run_until([&] {
            return done;
        });
        realm->close();
        REQUIRE(persisted);
    }
    SECTION("notify only with no further actions") {
        realm->async_begin_transaction(
            [&] {
                done = true;
            },
            true);
        util::EventLoop::main().run_until([&] {
            return done;
        });
    }
    SECTION("exception thrown during transaction") {
        Realm::AsyncHandle h = 7;
        bool called = false;
        realm->set_async_error_handler([&](Realm::AsyncHandle handle, std::exception_ptr error) {
            CHECK(error);
            CHECK(handle == h);
            called = true;
        });
        h = realm->async_begin_transaction([&] {
            done = true;
            auto table = realm->read_group().get_table("class_object");
            table->create_object_with_primary_key(45); // Will throw
            realm->async_commit_transaction();
        });
        util::EventLoop::main().run_until([&] {
            return done;
        });
        realm->close();
        REQUIRE(called);
    }
    SECTION("Canceling async transaction") {
        auto handle = realm->async_begin_transaction([&]() {
            auto table = realm->read_group().get_table("class_object");
            auto col = table->get_column_key("value");
            table->create_object().set(col, 45);
            realm->async_commit_transaction(
                [&]() {
                    done = true;
                },
                true);
        });
        realm->async_begin_transaction([&]() {
            auto table = realm->read_group().get_table("class_object");
            auto col = table->get_column_key("value");
            table->create_object().set(col, 90);
            realm->async_commit_transaction(
                [&]() {
                    done = true;
                },
                true);
        });
        realm->async_cancel_transaction(handle);
        util::EventLoop::main().run_until([&] {
            return done;
        });
        auto table = realm->read_group().get_table("class_object");
        REQUIRE(table->size() == 1);
        REQUIRE(table->begin()->get<Int>("value") == 90);
    }
    SECTION("synchronous commit") {
        realm->async_begin_transaction([&]() {
            auto table = realm->read_group().get_table("class_object");
            auto col = table->get_column_key("value");
            table->create_object().set(col, 45);
            realm->async_commit_transaction(
                [&]() {
                    done = true;
                },
                true);
        });
        realm->async_begin_transaction([&]() {
            auto table = realm->read_group().get_table("class_object");
            auto col = table->get_column_key("value");
            table->create_object().set(col, 45);
            realm->commit_transaction();
        });
        util::EventLoop::main().run_until([&] {
            return done;
        });
        auto table = realm->read_group().get_table("class_object");
        REQUIRE(table->size() == 2);
    }
    SECTION("synchronous transaction after async transaction with no commit") {
        realm->async_begin_transaction([&]() {});
        realm->begin_transaction();
        auto table = realm->read_group().get_table("class_object");
        auto col = table->get_column_key("value");
        table->create_object().set(col, 90);
        realm->commit_transaction();
    }
    SECTION("synchronous transaction with async write") {
        realm->begin_transaction();
        auto table = realm->read_group().get_table("class_object");
        auto col = table->get_column_key("value");
        table->create_object().set(col, 45);
        realm->async_commit_transaction();

        realm->begin_transaction();
        table->create_object().set(col, 90);
        realm->async_commit_transaction([&]() {
            done = true;
        });

        util::EventLoop::main().run_until([&] {
            return done;
        });
        REQUIRE(table->size() == 2);
    }
    SECTION("synchronous transaction mixed with async transactions") {
        realm->async_begin_transaction([&]() {
            auto table = realm->read_group().get_table("class_object");
            auto col = table->get_column_key("value");
            table->create_object().set(col, 45);
            done = true;
            realm->async_commit_transaction();
        });
        realm->async_begin_transaction([&]() {
            auto table = realm->read_group().get_table("class_object");
            auto col = table->get_column_key("value");
            table->create_object().set(col, 45);
            realm->async_commit_transaction([&]() {
                done = true;
            });
        });
        util::EventLoop::main().run_until([&] {
            return done;
        });
        realm->begin_transaction(); // Here syncing of first async tr has not completed
        auto table = realm->read_group().get_table("class_object");
        REQUIRE(table->size() == 1);
        auto col = table->get_column_key("value");
        table->create_object().set(col, 90);
        realm->commit_transaction(); // Will re-initiate async writes

        done = false;
        util::EventLoop::main().run_until([&] {
            return done;
        });
        REQUIRE(table->size() == 3);
    }
    SECTION("asynchronous transaction mixed with sync transaction that is cancelled") {
        bool persisted = false;
        realm->async_begin_transaction([&]() {
            auto table = realm->read_group().get_table("class_object");
            auto col = table->get_column_key("value");
            table->create_object().set(col, 45);
            done = true;
            realm->async_commit_transaction([&]() {
                persisted = true;
            });
        });
        realm->async_begin_transaction([&]() {
            auto table = realm->read_group().get_table("class_object");
            auto col = table->get_column_key("value");
            table->create_object().set(col, 45);
            auto handle = realm->async_commit_transaction([&]() {
                throw std::runtime_error("Should not go here");
            });
            realm->async_cancel_transaction(handle);
        });
        util::EventLoop::main().run_until([&] {
            return done;
        });
        realm->begin_transaction();
        CHECK(persisted);
        persisted = false;
        auto table = realm->read_group().get_table("class_object");
        REQUIRE(table->size() == 1);
        auto col = table->get_column_key("value");
        table->create_object().set(col, 90);
        realm->cancel_transaction();

        util::EventLoop::main().run_until([&] {
            return !realm->is_in_async_transaction();
        });

        REQUIRE(table->size() == 2);
        REQUIRE(!table->find_first_int(col, 90));
    }
    SECTION("object change information") {
        realm->begin_transaction();
        auto table = realm->read_group().get_table("class_object");
        auto col = table->get_column_key("ints");
        auto obj = table->create_object();
        auto list = obj.get_list<Int>(col);
        for (int i = 0; i < 3; ++i)
            list.add(i);
        realm->commit_transaction();

        Observer observer(obj);
        observer.realm = realm;
        realm->m_binding_context.reset(&observer);

        realm->async_begin_transaction([&]() {
            auto table = realm->read_group().get_table("class_object");
            auto col = table->get_column_key("ints");
            auto l = table->get_object(0).get_list<Int>(col);
            l.clear();
            done = true;
        });
        util::EventLoop::main().run_until([&] {
            return done;
        });
        REQUIRE(observer.array_change(0, col) == IndexSet{0, 1, 2});
        realm->m_binding_context.release();
    }

    SECTION("begin_transaction() from within did_change()") {
        struct Context : public BindingContext {
            void did_change(std::vector<ObserverState> const&, std::vector<void*> const&, bool) override
            {
                auto r = realm.lock();
                r->begin_transaction();
                auto table = r->read_group().get_table("class_object");
                auto obj = table->create_object();
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
        auto obj = table->create_object();
        bool persisted = false;
        realm->async_commit_transaction([&persisted]() {
            persisted = true;
        });
        REQUIRE(table->size() == 2);
        REQUIRE(persisted);
    }

    SECTION("close realm from within did_change()") {
        struct Context : public BindingContext {
            void did_change(std::vector<ObserverState> const&, std::vector<void*> const&, bool) override
            {
                realm.lock()->close();
            }
        };

        auto r2 = Realm::get_shared_realm(config);
        r2->m_binding_context.reset(new Context());
        r2->m_binding_context->realm = r2;

        r2->begin_transaction();
        auto table = r2->read_group().get_table("class_object");
        auto obj = table->create_object();
        r2->commit_transaction();
    }
}

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
    std::shared_ptr<bool> add_task(const std::function<void()>& the_job)
    {
        Task t{std::make_shared<bool>(false), the_job};

        m_tasks.push_back(t);
        return t.may_run;
    }
    bool has_tasks()
    {
        return !m_tasks.empty();
    }

private:
    struct Task {
        std::shared_ptr<bool> may_run;
        std::function<void()> the_job;
    };
    std::vector<Task> m_tasks;
};

TEST_CASE("SharedRealm: async_writes_2") {
    _impl::RealmCoordinator::assert_no_open_realms();

    if (!util::EventLoop::has_implementation())
        return;

    TestFile config;
    config.cache = false;
    config.schema_version = 0;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Int}}},
    };
    bool done = false;
    auto realm = Realm::get_shared_realm(config);
    int write_nr = 0;
    int commit_nr = 0;
    LooperDelegate ld;
    std::shared_ptr<bool> t1_rdy = ld.add_task([&]() {
        REQUIRE(write_nr == 0);
        ++write_nr;
        auto table = realm->read_group().get_table("class_object");
        auto col = table->get_column_key("value");
        table->create_object().set(col, 45);
        realm->cancel_transaction();
    });
    std::shared_ptr<bool> t2_rdy = ld.add_task([&]() {
        REQUIRE(write_nr == 1);
        ++write_nr;
        auto table = realm->read_group().get_table("class_object");
        auto col = table->get_column_key("value");
        table->create_object().set(col, 45);
        realm->async_commit_transaction([&]() {
            REQUIRE(commit_nr == 0);
            ++commit_nr;
        });
    });
    std::shared_ptr<bool> t3_rdy = ld.add_task([&]() {
        ++write_nr;
        auto table = realm->read_group().get_table("class_object");
        auto col = table->get_column_key("value");
        auto o = table->get_object(0);
        o.set(col, o.get<int64_t>(col) + 37);
        realm->async_commit_transaction([&]() {
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
    util::EventLoop::main().run_until([&] {
        ld.run_once();
        return done;
    });
    REQUIRE(done);
}

TEST_CASE("SharedRealm: notifications") {
    if (!util::EventLoop::has_implementation())
        return;

    TestFile config;
    config.cache = false;
    config.schema_version = 0;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Int}}},
    };

    struct Context : BindingContext {
        size_t* change_count;
        std::function<void()> did_change_fn;
        std::function<void()> changes_available_fn;

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

    SECTION("notifications created in async transaction are sent asynchronously") {
        realm->async_begin_transaction([&] {
            realm->async_commit_transaction();
        });
        REQUIRE(change_count == 0);
        util::EventLoop::main().run_until([&] {
            return change_count > 0;
        });
        REQUIRE(change_count == 1);
    }

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
            REQUIRE_THROWS_WITH(r->refresh(), Catch::Matchers::Contains("Property 'object.value' has been removed."));
        }

        SECTION("change property type") {
            table.remove_column(table.get_column_key("value 2"));
            table.add_column(type_Float, "value 2");
            wt.commit();
            REQUIRE_THROWS_WITH(
                r->refresh(),
                Catch::Matchers::Contains("Property 'object.value 2' has been changed from 'int' to 'float'"));
        }

        SECTION("make property optional") {
            table.remove_column(table.get_column_key("value 2"));
            table.add_column(type_Int, "value 2", true);
            wt.commit();
            REQUIRE_THROWS_WITH(r->refresh(),
                                Catch::Matchers::Contains("Property 'object.value 2' has been made optional"));
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
        realm->close();

        REQUIRE(realm->is_closed());

        REQUIRE_THROWS_AS(realm->read_group(), ClosedRealmException);
        REQUIRE_THROWS_AS(realm->begin_transaction(), ClosedRealmException);
        REQUIRE(!realm->is_in_transaction());
        REQUIRE_THROWS_AS(realm->commit_transaction(), InvalidTransactionException);
        REQUIRE_THROWS_AS(realm->cancel_transaction(), InvalidTransactionException);

        REQUIRE_THROWS_AS(realm->refresh(), ClosedRealmException);
        REQUIRE_THROWS_AS(realm->invalidate(), ClosedRealmException);
        REQUIRE_THROWS_AS(realm->compact(), ClosedRealmException);
    }

    SECTION("fully closes database file even with live notifiers") {
        auto& group = realm->read_group();
        realm->begin_transaction();
        auto obj = ObjectStore::table_for_object_type(group, "list")->create_object();
        realm->commit_transaction();

        Results results(realm, ObjectStore::table_for_object_type(group, "object"));
        List list(realm, obj.get_linklist("list"));
        Object object(realm, obj);

        auto obj_token = object.add_notification_callback([](CollectionChangeSet, std::exception_ptr) {});
        auto list_token = list.add_notification_callback([](CollectionChangeSet, std::exception_ptr) {});
        auto results_token = results.add_notification_callback([](CollectionChangeSet, std::exception_ptr) {});

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
    util::File(path + ".log", util::File::mode_Write);

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
        REQUIRE_THROWS_AS(Realm::delete_files(path), DeleteOnOpenRealmException);
        REQUIRE(util::File::exists(path + ".lock"));
        REQUIRE(util::File::exists(path));
        REQUIRE(util::File::exists(path + ".management"));
        REQUIRE(util::File::exists(path + ".note"));
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
        REQUIRE_THROWS(Realm::get_shared_realm(config3)); // missing in_memory and immutable

        config3.in_memory = true;
        config3.schema_mode = SchemaMode::Immutable;
        config3.path = "path";
        REQUIRE_THROWS(Realm::get_shared_realm(config3)); // both buffer and path

        config3.path = "";
        config3.encryption_key = {'a'};
        REQUIRE_THROWS(Realm::get_shared_realm(config3)); // both buffer and encryption
    }
}

TEST_CASE("ShareRealm: realm closed in did_change callback") {
    TestFile config;
    config.schema_version = 1;
    config.cache = false;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Int}}},
    };
    config.automatic_change_notifications = false;
    auto r1 = Realm::get_shared_realm(config);

    r1->begin_transaction();
    auto table = r1->read_group().get_table("class_object");
    table->create_object();
    r1->commit_transaction();

    // Cannot be a member var of Context since Realm.close will free the context.
    static SharedRealm* shared_realm;
    shared_realm = &r1;
    struct Context : public BindingContext {
        void did_change(std::vector<ObserverState> const&, std::vector<void*> const&, bool) override
        {
            (*shared_realm)->close();
            (*shared_realm).reset();
        }
    };

    SECTION("did_change") {
        r1->m_binding_context.reset(new Context());
        r1->invalidate();

        auto r2 = Realm::get_shared_realm(config);
        r2->begin_transaction();
        r2->read_group().get_table("class_object")->create_object();
        r2->commit_transaction();
        r2.reset();

        r1->notify();
    }

    SECTION("did_change with async results") {
        r1->m_binding_context.reset(new Context());
        Results results(r1, table->where());
        auto token = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr) {
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
        r1->m_binding_context.reset(new Context());

        auto r2 = Realm::get_shared_realm(config);
        r2->begin_transaction();
        r2->read_group().get_table("class_object")->create_object();
        r2->commit_transaction();
        r2.reset();

        REQUIRE_FALSE(r1->refresh());
    }

    shared_realm = nullptr;
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
                            ObjectSchema::IsEmbedded{true},
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
    config.cache = false;
    auto dynamic_config = config;

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

#ifndef _WIN32
TEST_CASE("SharedRealm: compact on launch") {
    // Make compactable Realm
    TestFile config;
    config.automatic_change_notifications = false;
    int num_opens = 0;
    config.should_compact_on_launch_function = [&](size_t total_bytes, size_t used_bytes) {
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
        results.add_notification_callback([](CollectionChangeSet const&, std::exception_ptr) {});
        r->close();
    }

    SECTION("compact function does not get invoked if realm is open on another thread") {
        config.scheduler = util::Scheduler::make_frozen(VersionID());
        r = Realm::get_shared_realm(config);
        REQUIRE(num_opens == 2);
        std::thread([&] {
            auto r2 = Realm::get_shared_realm(config);
            REQUIRE(num_opens == 2);
        }).join();
        r->close();
        std::thread([&] {
            auto r3 = Realm::get_shared_realm(config);
            REQUIRE(num_opens == 3);
        }).join();
    }
}
#endif

struct ModeAutomatic {
    static SchemaMode mode()
    {
        return SchemaMode::Automatic;
    }
    static bool should_call_init_on_version_bump()
    {
        return false;
    }
};
struct ModeAdditive {
    static SchemaMode mode()
    {
        return SchemaMode::AdditiveExplicit;
    }
    static bool should_call_init_on_version_bump()
    {
        return false;
    }
};
struct ModeManual {
    static SchemaMode mode()
    {
        return SchemaMode::Manual;
    }
    static bool should_call_init_on_version_bump()
    {
        return false;
    }
};
struct ModeResetFile {
    static SchemaMode mode()
    {
        return SchemaMode::ResetFile;
    }
    static bool should_call_init_on_version_bump()
    {
        return true;
    }
};

TEMPLATE_TEST_CASE("SharedRealm: update_schema with initialization_function", "[init][update_schema]", ModeAutomatic,
                   ModeAdditive, ModeManual, ModeResetFile)
{
    TestFile config;
    config.schema_mode = TestType::mode();
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
        REQUIRE(initialization_function_called == TestType::should_call_init_on_version_bump());
        if (TestType::should_call_init_on_version_bump()) {
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

        auto token1 = results1.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            ++notification_calls;
        });

        auto token2 = results2.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
            REQUIRE_FALSE(err);
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
        auto token = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr) {
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
}

TEST_CASE("Statistics on Realms") {
    _impl::RealmCoordinator::assert_no_open_realms();

    InMemoryTestFile config;
    // config.cache = false;
    config.automatic_change_notifications = false;

    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"object", {{"value", PropertyType::Int}}},
    });

    SECTION("compute_size") {
        auto s = r->read_group().compute_aggregated_byte_size();
        REQUIRE(s > 0);
    }
}

#if REALM_PLATFORM_APPLE && NOTIFIER_BACKGROUND_ERRORS
TEST_CASE("BindingContext is notified in case of notifier errors") {
    _impl::RealmCoordinator::assert_no_open_realms();

    class OpenFileLimiter {
    public:
        OpenFileLimiter()
        {
            // Set the max open files to zero so that opening new files will fail
            getrlimit(RLIMIT_NOFILE, &m_old);
            rlimit rl = m_old;
            rl.rlim_cur = 0;
            setrlimit(RLIMIT_NOFILE, &rl);
        }

        ~OpenFileLimiter()
        {
            setrlimit(RLIMIT_NOFILE, &m_old);
        }

    private:
        rlimit m_old;
    };

    InMemoryTestFile config;
    config.automatic_change_notifications = false;

    auto r = Realm::get_shared_realm(config);
    r->update_schema({
        {"object", {{"value", PropertyType::Int}}},
    });

    auto coordinator = _impl::RealmCoordinator::get_coordinator(config.path);
    auto table = r->read_group().get_table("class_object");
    Results results(r, *r->read_group().get_table("class_object"));
    static int binding_context_start_notify_calls = 0;
    static int binding_context_end_notify_calls = 0;
    static bool error_called = false;
    struct Context : BindingContext {
        void will_send_notifications() override
        {
            REQUIRE_FALSE(error_called);
            ++binding_context_start_notify_calls;
        }

        void did_send_notifications() override
        {
            REQUIRE(error_called);
            ++binding_context_end_notify_calls;
        }
    };
    r->m_binding_context.reset(new Context());

    SECTION("realm on background thread could not be opened") {
        OpenFileLimiter limiter;

        auto token = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr err) {
            REQUIRE(err);
            REQUIRE_FALSE(error_called);
            error_called = true;
        });
        advance_and_notify(*r);
        REQUIRE(error_called);
        REQUIRE(binding_context_start_notify_calls == 1);
        REQUIRE(binding_context_end_notify_calls == 1);
    }
}
#endif

TEST_CASE("RealmCoordinator: get_unbound_realm()") {
    TestFile config;
    config.cache = true;
    config.schema = Schema{
        {"object", {{"value", PropertyType::Int}}},
    };

    ThreadSafeReference ref;
    std::thread([&] {
        ref = _impl::RealmCoordinator::get_coordinator(config)->get_unbound_realm();
    }).join();

    SECTION("checks thread after being resolved") {
        auto realm = Realm::get_shared_realm(std::move(ref));
        REQUIRE_NOTHROW(realm->verify_thread());
        std::thread([&] {
            REQUIRE_THROWS(realm->verify_thread());
        }).join();
    }

    SECTION("delivers notifications to the thread it is resolved on") {
        if (!util::EventLoop::has_implementation())
            return;
        auto realm = Realm::get_shared_realm(std::move(ref));
        Results results(realm, ObjectStore::table_for_object_type(realm->read_group(), "object")->where());
        bool called = false;
        auto token = results.add_notification_callback([&](CollectionChangeSet, std::exception_ptr) {
            called = true;
        });
        util::EventLoop::main().run_until([&] {
            return called;
        });
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
        std::thread([&] {
            ref = _impl::RealmCoordinator::get_coordinator(config)->get_unbound_realm();
        }).join();
        auto r3 = Realm::get_shared_realm(std::move(ref));
        REQUIRE(r1 != r3);
        REQUIRE(r2 != r3);

        // New local with cache enabled should grab the resolved unbound
        config.cache = true;
        auto r4 = Realm::get_shared_realm(config);
        REQUIRE(r4 == r2);
    }
}

TEST_CASE("KeyPathMapping generation") {
    TestFile config;
    config.cache = true;
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
