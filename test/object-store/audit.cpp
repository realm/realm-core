////////////////////////////////////////////////////////////////////////////
//
// Copyright 2022 Realm Inc.
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

#include <catch2/catch_all.hpp>

#include "sync/flx_sync_harness.hpp"
#include "util/event_loop.hpp"
#include "util/test_file.hpp"
#include "util/test_utils.hpp"
#include "util/baas_admin_api.hpp"

#include <realm/object-store/audit.hpp>
#include <realm/object-store/audit_serializer.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/object.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/shared_realm.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>

#include <realm/object-store/sync/sync_user.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_database.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>

#include <realm/set.hpp>
#include <realm/list.hpp>
#include <realm/dictionary.hpp>

#include <external/json/json.hpp>

using namespace realm;
using namespace std::string_literals;
using nlohmann::json;

#ifndef AUDIT_LOG_LEVEL
#define AUDIT_LOG_LEVEL util::Logger::Level::off
#endif

namespace {

struct AuditEvent {
    std::string activity;
    util::Optional<std::string> event;
    json data;
    util::Optional<std::string> raw_data;
    Timestamp timestamp;
    std::map<std::string, std::string> metadata;
};

util::Optional<std::string> to_optional_string(StringData sd)
{
    return sd ? util::Optional<std::string>(sd) : none;
}

std::vector<AuditEvent> get_audit_events(TestSyncManager& manager, bool parse_events = true)
{
    // Wait for all sessions to be fully uploaded and then tear them down
    auto sync_manager = manager.app()->sync_manager();
    REALM_ASSERT(sync_manager);
    auto sessions = sync_manager->get_all_sessions();
    for (auto& session : sessions) {
        // The realm user session has been manually closed, don't try to wait for it to sync
        // If the session is still active (in this case the audit session) wait for audit to complete
        if (session->state() == SyncSession::State::Active) {
            auto [promise, future] = util::make_promise_future<void>();
            session->wait_for_upload_completion([promise = std::move(promise)](std::error_code) mutable {
                // Don't care if error occurred, just finish operation
                promise.emplace_value();
            });
            future.get();
        }
        session->shutdown_and_wait();
    }
    sync_manager->wait_for_sessions_to_terminate();

    // Stop the sync server so that we can safely inspect its Realm files
    auto& server = manager.sync_server();
    server.stop();

    std::vector<AuditEvent> events;

    // Iterate over all of the audit Realm files in the server's storage
    // directory, opening them in read-only mode (as they use Server history),
    // and slurp the audit events out of them.
    auto root = server.local_root_dir();
    std::string file_name;
    util::DirScanner dir(root);
    while (dir.next(file_name)) {
        StringData sd(file_name);
        if (!sd.begins_with("audit-") || !sd.ends_with(".realm"))
            continue;

        Group g(root + "/" + file_name);
        auto table = g.get_table("class_AuditEvent");
        if (!table)
            continue;

        ColKey activity_key, event_key, data_key, timestamp_key;
        std::vector<std::pair<std::string, ColKey>> metadata_keys;
        for (auto col_key : table->get_column_keys()) {
            auto name = table->get_column_name(col_key);
            if (name == "activity")
                activity_key = col_key;
            else if (name == "event")
                event_key = col_key;
            else if (name == "data")
                data_key = col_key;
            else if (name == "timestamp")
                timestamp_key = col_key;
            else if (name != "_id")
                metadata_keys.push_back({name, col_key});
        }

        for (auto& obj : *table) {
            AuditEvent event;
            event.activity = obj.get<StringData>(activity_key);
            event.event = to_optional_string(obj.get<StringData>(event_key));
            event.timestamp = obj.get<Timestamp>(timestamp_key);
            for (auto& [name, key] : metadata_keys) {
                if (auto sd = obj.get<StringData>(key))
                    event.metadata[name] = sd;
            }
            if (parse_events) {
                if (auto data = obj.get<StringData>(data_key))
                    event.data = json::parse(data.data(), data.data() + data.size());
            }
            else {
                if (auto data = obj.get<StringData>(data_key))
                    event.raw_data = std::string(data);
            }
            REALM_ASSERT(!event.timestamp.is_null() && event.timestamp != Timestamp(0, 0));
            events.push_back(std::move(event));
        }
    }

    return events;
}

void sort_events(std::vector<AuditEvent>& events)
{
    std::sort(events.begin(), events.end(), [](auto& a, auto& b) {
        return a.timestamp < b.timestamp;
    });
}

#if REALM_ENABLE_AUTH_TESTS
static std::vector<AuditEvent> get_audit_events_from_baas(TestAppSession& session, SyncUser& user,
                                                          size_t expected_count)
{
    auto& app_session = session.app_session();
    app::MongoClient remote_client = user.mongo_client("BackingDB");
    app::MongoDatabase db = remote_client.db(app_session.config.mongo_dbname);
    app::MongoCollection collection = db["AuditEvent"];
    std::vector<AuditEvent> events;
    static const std::set<std::string> nonmetadata_fields = {"activity", "event", "data", "realm_id"};

    timed_wait_for(
        [&] {
            uint64_t count = 0;
            collection.count({}, [&](uint64_t c, util::Optional<app::AppError> error) {
                REQUIRE(!error);
                count = c;
            });
            if (count < expected_count) {
                millisleep(500); // slow down the number of retries
                return false;
            }
            return true;
        },
        std::chrono::minutes(5));

    collection.find({}, {},
                    [&](util::Optional<std::vector<bson::Bson>>&& result, util::Optional<app::AppError> error) {
                        REQUIRE(!error);
                        REQUIRE(result->size() >= expected_count);
                        events.reserve(result->size());
                        for (auto bson : *result) {
                            auto doc = static_cast<const bson::BsonDocument&>(bson).entries();
                            AuditEvent event;
                            event.activity = static_cast<std::string>(doc["activity"]);
                            event.timestamp = static_cast<Timestamp>(doc["timestamp"]);
                            if (auto it = doc.find("event"); it != doc.end() && it->second != bson::Bson()) {
                                event.event = static_cast<std::string>(it->second);
                            }
                            if (auto it = doc.find("data"); it != doc.end() && it->second != bson::Bson()) {
                                event.data = json::parse(static_cast<std::string>(it->second));
                            }
                            for (auto& [key, value] : doc) {
                                if (value.type() == bson::Bson::Type::String && !nonmetadata_fields.count(key))
                                    event.metadata.insert({key, static_cast<std::string>(value)});
                            }
                            events.push_back(event);
                        }
                    });
    sort_events(events);
    return events;
}
#endif

// Check that the given key is present and the value is null
#define REQUIRE_NULL(v, k)                                                                                           \
    do {                                                                                                             \
        REQUIRE(v.contains(k));                                                                                      \
        REQUIRE(v[k] == nullptr);                                                                                    \
    } while (0)

#define REQUIRE_SET_EQUAL(a, ...)                                                                                    \
    do {                                                                                                             \
        json actual = (a);                                                                                           \
        json expected = __VA_ARGS__;                                                                                 \
        std::sort(actual.begin(), actual.end());                                                                     \
        std::sort(expected.begin(), expected.end());                                                                 \
        REQUIRE(actual == expected);                                                                                 \
    } while (0)

class CustomSerializer : public AuditObjectSerializer {
public:
    const Obj* expected_obj = nullptr;
    bool error = false;
    size_t completion_count = 0;

    void to_json(json& out, const Obj& obj) override
    {
        if (error) {
            throw std::runtime_error("custom serialization error");
        }
        if (expected_obj) {
            REQUIRE(expected_obj->get_key() == obj.get_key());
            REQUIRE(expected_obj->get_table()->get_key() == obj.get_table()->get_key());
            out["obj"] = obj.get_key().value;
            out["table"] = obj.get_table()->get_key().value;
        }
        else {
            AuditObjectSerializer::to_json(out, obj);
        }
    }

    void scope_complete() override
    {
        ++completion_count;
    }
};

void assert_no_error(std::exception_ptr e)
{
    REALM_ASSERT(!e);
}

struct TestClock {
    std::atomic<int32_t> timestamp = 1000;
    TestClock()
    {
        audit_test_hooks::set_clock([&] {
            auto now = timestamp.fetch_add(1);
            return Timestamp(now, now);
        });
    }
    ~TestClock()
    {
        audit_test_hooks::set_clock(nullptr);
    }
};

} // namespace

TEST_CASE("audit object serialization") {
    TestSyncManager test_session;
    SyncTestFile config(test_session.app(), "parent");
    config.cache = false;
    config.schema_version = 1;
    config.schema = Schema{
        {"object",
         {{"_id", PropertyType::Int, Property::IsPrimary{true}},

          {"int", PropertyType::Int | PropertyType::Nullable},
          {"bool", PropertyType::Bool | PropertyType::Nullable},
          {"string", PropertyType::String | PropertyType::Nullable},
          {"data", PropertyType::Data | PropertyType::Nullable},
          {"date", PropertyType::Date | PropertyType::Nullable},
          {"float", PropertyType::Float | PropertyType::Nullable},
          {"double", PropertyType::Double | PropertyType::Nullable},
          {"mixed", PropertyType::Mixed | PropertyType::Nullable},
          {"objectid", PropertyType::ObjectId | PropertyType::Nullable},
          {"decimal", PropertyType::Decimal | PropertyType::Nullable},
          {"uuid", PropertyType::UUID | PropertyType::Nullable},

          {"int list", PropertyType::Int | PropertyType::Nullable | PropertyType::Array},
          {"int set", PropertyType::Int | PropertyType::Nullable | PropertyType::Set},
          {"int dictionary", PropertyType::Int | PropertyType::Nullable | PropertyType::Dictionary},

          {"object", PropertyType::Object | PropertyType::Nullable, "target"},
          {"object list", PropertyType::Object | PropertyType::Array, "target"},
          {"object set", PropertyType::Object | PropertyType::Set, "target"},
          {"object dictionary", PropertyType::Object | PropertyType::Nullable | PropertyType::Dictionary, "target"},

          {"embedded object", PropertyType::Object | PropertyType::Nullable, "embedded target"},
          {"embedded object list", PropertyType::Object | PropertyType::Array, "embedded target"},
          {"embedded object dictionary", PropertyType::Object | PropertyType::Nullable | PropertyType::Dictionary,
           "embedded target"}}},
        {"target", {{"_id", PropertyType::Int, Property::IsPrimary{true}}, {"value", PropertyType::Int}}},
        {"embedded target", ObjectSchema::ObjectType::Embedded, {{"value", PropertyType::Int}}}};
    config.audit_config = std::make_shared<AuditConfig>();
    auto serializer = std::make_shared<CustomSerializer>();
    config.audit_config->serializer = serializer;
    config.audit_config->logger =
        std::make_shared<util::ThreadSafeLogger>(std::make_shared<util::StderrLogger>(AUDIT_LOG_LEVEL));
    auto realm = Realm::get_shared_realm(config);
    auto audit = realm->audit_context();
    REQUIRE(audit);

    // We open in proper sync mode to let the audit context initialize from that,
    // but we don't actually want the realm to be synchronizing
    realm->sync_session()->close();

    auto table = realm->read_group().get_table("class_object");
    auto target_table = realm->read_group().get_table("class_target");
    CppContext context(realm);

    auto populate_object = [&](Obj& obj) {
        obj.set("int", 1);
        obj.set("bool", true);
        obj.set("string", "abc");
        obj.set("data", BinaryData("abc", 3));
        obj.set("date", Timestamp(123, 456));
        obj.set("float", 1.1f);
        obj.set("double", 2.2);
        obj.set("mixed", Mixed(10));
        obj.set("objectid", ObjectId("000000000000000000000001"));
        obj.set("uuid", UUID("00000000-0000-0000-0000-000000000001"));

        auto int_list = obj.get_list<util::Optional<int64_t>>("int list");
        int_list.add(1);
        int_list.add(2);
        int_list.add(3);
        int_list.add(none);

        auto int_set = obj.get_set<util::Optional<int64_t>>("int set");
        int_set.insert(1);
        int_set.insert(2);
        int_set.insert(3);
        int_set.insert(none);

        auto int_dictionary = obj.get_dictionary("int dictionary");
        int_dictionary.insert("1", 1);
        int_dictionary.insert("2", 2);
        int_dictionary.insert("3", 3);
        int_dictionary.insert("4", none);

        auto obj_list = obj.get_linklist("object list");
        obj_list.add(target_table->create_object_with_primary_key(1).set_all(1).get_key());
        obj_list.add(target_table->create_object_with_primary_key(2).set_all(2).get_key());
        obj_list.add(target_table->create_object_with_primary_key(3).set_all(3).get_key());

        auto obj_set = obj.get_linkset(obj.get_table()->get_column_key("object set"));
        obj_set.insert(target_table->create_object_with_primary_key(4).set_all(4).get_key());
        obj_set.insert(target_table->create_object_with_primary_key(5).set_all(5).get_key());
        obj_set.insert(target_table->create_object_with_primary_key(6).set_all(6).get_key());

        auto obj_dict = obj.get_dictionary("object dictionary");
        obj_dict.insert("a", target_table->create_object_with_primary_key(7).set_all(7).get_key());
        obj_dict.insert("b", target_table->create_object_with_primary_key(8).set_all(8).get_key());
        obj_dict.insert("c", target_table->create_object_with_primary_key(9).set_all(9).get_key());

        auto embedded_list = obj.get_linklist("embedded object list");
        embedded_list.create_and_insert_linked_object(0).set_all(1);
        embedded_list.create_and_insert_linked_object(1).set_all(2);
        embedded_list.create_and_insert_linked_object(2).set_all(3);

        auto embedded_dict = obj.get_dictionary("embedded object dictionary");
        embedded_dict.create_and_insert_linked_object("d").set_all(4);
        embedded_dict.create_and_insert_linked_object("e").set_all(5);
        embedded_dict.create_and_insert_linked_object("f").set_all(6);

        return obj;
    };

    auto validate_default_values = [](auto& value) {
        REQUIRE(value.size() == 21);
        REQUIRE(value["_id"] == 2);
        REQUIRE(value["int"] == 1);
        REQUIRE(value["bool"] == true);
        REQUIRE(value["string"] == "abc");
        REQUIRE_FALSE(value.contains("data"));
        REQUIRE(value["date"] == "1970-01-01T00:02:03.000Z");
        REQUIRE(value["float"] == 1.1f);
        REQUIRE(value["double"] == 2.2);
        REQUIRE(value["mixed"] == 10);
        REQUIRE(value["objectid"] == "000000000000000000000001");
        REQUIRE(value["uuid"] == "00000000-0000-0000-0000-000000000001");
        REQUIRE_NULL(value, "object");
        REQUIRE_NULL(value, "embedded object");
        REQUIRE(value["int list"] == json({1, 2, 3, nullptr}));
        REQUIRE_SET_EQUAL(value["int set"], {1, 2, 3, nullptr});
        REQUIRE(value["int dictionary"] == json({{"1", 1}, {"2", 2}, {"3", 3}, {"4", nullptr}}));
        REQUIRE(value["object list"] == json({1, 2, 3}));
        REQUIRE_SET_EQUAL(value["object set"], {4, 5, 6});
    };

    SECTION("default object serialization") {
        realm->begin_transaction();
        auto obj = table->create_object_with_primary_key(2);
        populate_object(obj);
        realm->commit_transaction();

        audit->begin_scope("scope");
        Object object(realm, obj);
        audit->end_scope(assert_no_error);
        audit->wait_for_completion();

        auto events = get_audit_events(test_session);
        REQUIRE(events.size() == 1);
        auto& event = events[0];
        REQUIRE(event.event == "read");
        REQUIRE(event.activity == "scope");
        REQUIRE(!event.timestamp.is_null());

        REQUIRE(event.data["type"] == "object");
        auto value = event.data["value"];
        REQUIRE(value.size() == 1);
        validate_default_values(value[0]);
    }

    SECTION("custom object serialization") {
        realm->begin_transaction();
        auto obj1 = table->create_object_with_primary_key(2);
        auto obj2 = table->create_object_with_primary_key(3);
        realm->commit_transaction();

        serializer->expected_obj = &obj1;

        audit->begin_scope("scope 1");
        Object(realm, obj1);
        audit->end_scope(assert_no_error);
        audit->wait_for_completion();
        REQUIRE(serializer->completion_count == 1);

        audit->begin_scope("empty scope");
        audit->end_scope(assert_no_error);
        audit->wait_for_completion();
        REQUIRE(serializer->completion_count == 2);

        serializer->expected_obj = &obj2;

        audit->begin_scope("scope 2");
        Object(realm, obj2);
        audit->end_scope(assert_no_error);
        audit->wait_for_completion();
        REQUIRE(serializer->completion_count == 3);

        auto events = get_audit_events(test_session);
        REQUIRE(events.size() == 2);

        REQUIRE(events[0].activity == "scope 1");
        REQUIRE(events[1].activity == "scope 2");
        REQUIRE(events[0].data ==
                json({{"type", "object"},
                      {"value", json::array({{{"obj", obj1.get_key().value}, {"table", table->get_key().value}}})}}));
        REQUIRE(events[1].data ==
                json({{"type", "object"},
                      {"value", json::array({{{"obj", obj2.get_key().value}, {"table", table->get_key().value}}})}}));
    }

    SECTION("custom serialization error reporting") {
        serializer->error = true;

        realm->begin_transaction();
        auto obj = table->create_object_with_primary_key(2);
        realm->commit_transaction();
        audit->begin_scope("scope");
        Object(realm, obj);
        audit->end_scope([](auto error) {
            REQUIRE(error);
            REQUIRE_THROWS_CONTAINING(std::rethrow_exception(error), "custom serialization error");
        });
        audit->wait_for_completion();
    }

    SECTION("write transaction serialization") {
        SECTION("create object") {
            audit->begin_scope("scope");
            realm->begin_transaction();
            auto obj = table->create_object_with_primary_key(2);
            populate_object(obj);
            realm->commit_transaction();
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 1);
            auto& event = events[0];
            REQUIRE(event.event == "write");
            REQUIRE(event.activity == "scope");
            REQUIRE(!event.timestamp.is_null());

            REQUIRE(event.data.size() == 2);
            auto& object_changes = event.data["object"];
            REQUIRE(object_changes.size() == 1);
            REQUIRE(object_changes["insertions"].size() == 1);
            validate_default_values(object_changes["insertions"][0]);

            // target table should have 9 insertions with _id == value
            REQUIRE(event.data["target"]["insertions"].size() == 9);
            for (int i = 0; i < 9; ++i) {
                REQUIRE(event.data["target"]["insertions"][i] == json({{"_id", i + 1}, {"value", i + 1}}));
            }
        }

        SECTION("modify object") {
            realm->begin_transaction();
            auto obj = table->create_object_with_primary_key(2);
            populate_object(obj);
            realm->commit_transaction();

            audit->begin_scope("scope");
            realm->begin_transaction();
            obj.set("int", 3);
            obj.set("bool", true);
            realm->commit_transaction();
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 1);
            auto& event = events[0];
            REQUIRE(event.data.size() == 1);
            REQUIRE(event.data["object"].size() == 1);
            REQUIRE(event.data["object"]["modifications"].size() == 1);
            auto& modifications = event.data["object"]["modifications"][0];
            REQUIRE(modifications.size() == 2);
            REQUIRE(modifications["newValue"].size() == 1);
            REQUIRE(modifications["newValue"]["int"] == 3);
            // note: bool is not reported because it was assigned to itself
            validate_default_values(modifications["oldValue"]);
        }

        SECTION("delete object") {
            realm->begin_transaction();
            auto obj = table->create_object_with_primary_key(2);
            populate_object(obj);
            realm->commit_transaction();

            audit->begin_scope("scope");
            realm->begin_transaction();
            obj.remove();
            realm->commit_transaction();
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 1);
            auto& event = events[0];
            REQUIRE(event.data.size() == 1);
            REQUIRE(event.data["object"].size() == 1);
            REQUIRE(event.data["object"]["deletions"].size() == 1);
            validate_default_values(event.data["object"]["deletions"][0]);
        }

        SECTION("delete embedded object") {
            realm->begin_transaction();
            auto obj = table->create_object_with_primary_key(2);
            obj.create_and_set_linked_object(obj.get_table()->get_column_key("embedded object")).set_all(100);
            realm->commit_transaction();

            audit->begin_scope("scope");
            realm->begin_transaction();
            obj.get_linked_object("embedded object").remove();
            realm->commit_transaction();
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 1);
            REQUIRE(events[0].data.size() == 1);
            REQUIRE(events[0].data["object"].size() == 1);
            REQUIRE(events[0].data["object"]["modifications"].size() == 1);
            auto& modification = events[0].data["object"]["modifications"][0];
            REQUIRE(modification["newValue"] == json({{"embedded object", nullptr}}));
            REQUIRE(modification["oldValue"]["embedded object"] == json({{"value", 100}}));
        }

        SECTION("mixed changes") {
            realm->begin_transaction();
            std::vector<Obj> objects;
            for (int i = 0; i < 5; ++i)
                objects.push_back(target_table->create_object_with_primary_key(i).set_all(i));
            realm->commit_transaction();

            audit->begin_scope("scope");
            realm->begin_transaction();

            // Mutate then delete should not report the mutate
            objects[0].set("value", 100);
            objects[1].set("value", 100);
            objects[2].set("value", 100);
            objects[1].remove();

            // Insert then mutate should not report the mutate
            auto obj = target_table->create_object_with_primary_key(20);
            obj.set("value", 100);

            // Insert then delete should not report the insert or delete
            auto obj2 = target_table->create_object_with_primary_key(21);
            obj2.remove();

            realm->commit_transaction();
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 1);
            auto& event = events[0];
            REQUIRE(event.data.size() == 1);
            auto& data = event.data["target"];
            REQUIRE(data.size() == 3);
            REQUIRE(data["deletions"] == json({{{"_id", 1}, {"value", 1}}}));
            REQUIRE(data["insertions"] == json({{{"_id", 20}, {"value", 100}}}));
            REQUIRE(data["modifications"] ==
                    json({{{"oldValue", {{"_id", 0}, {"value", 0}}}, {"newValue", {{"value", 100}}}},
                          {{"oldValue", {{"_id", 2}, {"value", 2}}}, {"newValue", {{"value", 100}}}}}));
        }

        SECTION("empty write transactions do not produce an event") {
            audit->begin_scope("scope");
            realm->begin_transaction();
            realm->commit_transaction();
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            REQUIRE(get_audit_events(test_session).empty());
        }
    }

    SECTION("empty query") {
        audit->begin_scope("scope");
        Results(realm, table->where()).snapshot();
        audit->end_scope(assert_no_error);
        audit->wait_for_completion();
        REQUIRE(get_audit_events(test_session).empty());
    }

    SECTION("non-empty query") {
        realm->begin_transaction();
        for (int64_t i = 0; i < 10; ++i) {
            table->create_object_with_primary_key(i);
            target_table->create_object_with_primary_key(i);
        }
        realm->commit_transaction();

        SECTION("query counts as a read on all objects matching the query") {
            audit->begin_scope("scope");
            Results(realm, table->where().less(table->get_column_key("_id"), 5)).snapshot();
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();
            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 1);
            REQUIRE(events[0].data["value"].size() == 5);
        }

        SECTION("subsequent reads on the same table are folded into the query") {
            audit->begin_scope("scope");
            Results(realm, table->where().less(table->get_column_key("_id"), 5)).snapshot();
            Object(realm, table->get_object(3)); // does not produce any new audit data
            Object(realm, table->get_object(7)); // adds this object to the query's event
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();
            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 1);
            REQUIRE(events[0].data["value"].size() == 6);
        }

        SECTION("reads on different tables are not folded into query") {
            audit->begin_scope("scope");
            Results(realm, table->where().less(table->get_column_key("_id"), 5)).snapshot();
            Object(realm, target_table->get_object(3));
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();
            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 2);
            REQUIRE(events[0].data["value"].size() == 5);
            REQUIRE(events[1].data["value"].size() == 1);
        }

        SECTION("reads on same table following a read on a different table are not folded into query") {
            audit->begin_scope("scope");
            Results(realm, table->where().less(table->get_column_key("_id"), 5)).snapshot();
            Object(realm, target_table->get_object(3));
            Object(realm, table->get_object(3));
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();
            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 3);
            REQUIRE(events[0].data["value"].size() == 5);
            REQUIRE(events[1].data["value"].size() == 1);
            REQUIRE(events[2].data["value"].size() == 1);
        }

        SECTION("reads with intervening writes are not combined") {
            audit->begin_scope("scope");
            Results(realm, table->where().less(table->get_column_key("_id"), 5)).snapshot();
            realm->begin_transaction();
            realm->commit_transaction();
            Object(realm, table->get_object(3));
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();
            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 2);
            REQUIRE(events[0].data["value"].size() == 5);
            REQUIRE(events[1].data["value"].size() == 1);
        }
    }

    SECTION("query on list of objects") {
        realm->begin_transaction();
        auto obj = table->create_object_with_primary_key(2);
        auto list = obj.get_linklist("object list");
        for (int64_t i = 0; i < 10; ++i)
            list.add(target_table->create_object_with_primary_key(i).set_all(i * 2).get_key());
        realm->commit_transaction();

        audit->begin_scope("scope");
        Object object(realm, obj);
        auto obj_list = util::any_cast<List>(object.get_property_value<std::any>(context, "object list"));
        obj_list.filter(target_table->where().greater(target_table->get_column_key("value"), 10)).snapshot();
        audit->end_scope(assert_no_error);
        audit->wait_for_completion();

        auto events = get_audit_events(test_session);
        REQUIRE(events.size() == 2);
        REQUIRE(events[0].data["type"] == "object");
        REQUIRE(events[0].data["value"][0]["object list"] == json({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
        REQUIRE(events[1].data["type"] == "target");
        REQUIRE(events[1].data["value"] == json({
                                               {{"_id", 6}, {"value", 12}},
                                               {{"_id", 7}, {"value", 14}},
                                               {{"_id", 8}, {"value", 16}},
                                               {{"_id", 9}, {"value", 18}},
                                           }));
    }

    SECTION("link access tracking") {
        realm->begin_transaction();
        table->create_object_with_primary_key(1);
        target_table->create_object_with_primary_key(0);
        auto obj = table->create_object_with_primary_key(2);
        obj.set("object", target_table->create_object_with_primary_key(1).set_all(1).get_key());
        obj.create_and_set_linked_object(table->get_column_key("embedded object")).set_all(200);

        auto obj_list = obj.get_linklist("object list");
        obj_list.add(target_table->create_object_with_primary_key(3).set_all(10).get_key());
        obj_list.add(target_table->create_object_with_primary_key(4).set_all(20).get_key());
        obj_list.add(target_table->create_object_with_primary_key(5).set_all(30).get_key());

        auto obj_set = obj.get_linkset(obj.get_table()->get_column_key("object set"));
        obj_set.insert(target_table->create_object_with_primary_key(6).set_all(40).get_key());
        obj_set.insert(target_table->create_object_with_primary_key(7).set_all(50).get_key());
        obj_set.insert(target_table->create_object_with_primary_key(8).set_all(60).get_key());

        auto obj_dict = obj.get_dictionary("object dictionary");
        obj_dict.insert("a", target_table->create_object_with_primary_key(9).set_all(90).get_key());
        obj_dict.insert("b", target_table->create_object_with_primary_key(10).set_all(100).get_key());
        obj_dict.insert("c", target_table->create_object_with_primary_key(11).set_all(110).get_key());
        realm->commit_transaction();

        SECTION("objects are serialized as just primary key by default") {
            audit->begin_scope("scope");
            Object object(realm, obj);
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 1);
            auto& value = events[0].data["value"][0];
            REQUIRE(value["object"] == 1);
            REQUIRE(value["object list"] == json({3, 4, 5}));
            REQUIRE_SET_EQUAL(value["object set"], {6, 7, 8});
            REQUIRE(value["object dictionary"] == json({{"a", 9}, {"b", 10}, {"c", 11}}));
        }

        SECTION("embedded objects are always full object") {
            audit->begin_scope("scope");
            Object object(realm, obj);
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 1);
            REQUIRE(events[0].data["value"][0]["embedded object"] == json({{"value", 200}}));
        }

        SECTION("links followed serialize the full object") {
            audit->begin_scope("scope");
            Object object(realm, obj);
            object.get_property_value<std::any>(context, "object");
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 2);
            auto& value = events[0].data["value"][0];
            REQUIRE(value["object"] == json({{"_id", 1}, {"value", 1}}));
            REQUIRE(events[1].data["value"][0] == json({{"_id", 1}, {"value", 1}}));

            // Other fields are left in pk form
            REQUIRE(value["object list"] == json({3, 4, 5}));
            REQUIRE_SET_EQUAL(value["object set"], {6, 7, 8});
            REQUIRE(value["object dictionary"] == json({{"a", 9}, {"b", 10}, {"c", 11}}));
        }

        SECTION("instantiating a collection accessor does not count as a read") {
            audit->begin_scope("scope");
            Object object(realm, obj);
            util::any_cast<List>(object.get_property_value<std::any>(context, "object list"));
            util::any_cast<object_store::Set>(object.get_property_value<std::any>(context, "object set"));
            util::any_cast<object_store::Dictionary>(
                object.get_property_value<std::any>(context, "object dictionary"));
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 1);
            auto& value = events[0].data["value"][0];
            REQUIRE(value["object list"] == json({3, 4, 5}));
            REQUIRE_SET_EQUAL(value["object set"], {6, 7, 8});
            REQUIRE(value["object dictionary"] == json({{"a", 9}, {"b", 10}, {"c", 11}}));
        }

        SECTION("accessing any value from a collection serializes full objects for the entire collection") {
            SECTION("list") {
                audit->begin_scope("scope");
                Object object(realm, obj);
                auto list = util::any_cast<List>(object.get_property_value<std::any>(context, "object list"));
                SECTION("get()") {
                    list.get(1);
                }
                SECTION("get_any()") {
                    list.get_any(1);
                }
                audit->end_scope(assert_no_error);
                audit->wait_for_completion();

                auto events = get_audit_events(test_session);
                REQUIRE(events.size() == 2);
                auto& value = events[0].data["value"][0];
                REQUIRE(
                    value["object list"] ==
                    json({{{"_id", 3}, {"value", 10}}, {{"_id", 4}, {"value", 20}}, {{"_id", 5}, {"value", 30}}}));
                REQUIRE_SET_EQUAL(value["object set"], {6, 7, 8});
                REQUIRE(value["object dictionary"] == json({{"a", 9}, {"b", 10}, {"c", 11}}));
            }

            SECTION("set") {
                audit->begin_scope("scope");
                Object object(realm, obj);
                auto set =
                    util::any_cast<object_store::Set>(object.get_property_value<std::any>(context, "object set"));
                SECTION("get()") {
                    set.get(1);
                }
                SECTION("get_any()") {
                    set.get_any(1);
                }
                audit->end_scope(assert_no_error);
                audit->wait_for_completion();

                auto events = get_audit_events(test_session);
                REQUIRE(events.size() == 2);
                auto& value = events[0].data["value"][0];
                REQUIRE_SET_EQUAL(
                    value["object set"],
                    json({{{"_id", 6}, {"value", 40}}, {{"_id", 7}, {"value", 50}}, {{"_id", 8}, {"value", 60}}}));
                REQUIRE(value["object list"] == json({3, 4, 5}));
                REQUIRE(value["object dictionary"] == json({{"a", 9}, {"b", 10}, {"c", 11}}));
            }

            SECTION("dictionary") {
                audit->begin_scope("scope");
                Object object(realm, obj);
                auto dict = util::any_cast<object_store::Dictionary>(
                    object.get_property_value<std::any>(context, "object dictionary"));
                SECTION("get_object()") {
                    dict.get_object("b");
                }
                SECTION("get_any(string)") {
                    dict.get_any("b");
                }
                SECTION("get_any(index)") {
                    const_cast<const object_store::Dictionary&>(dict).get_any(size_t(1));
                }
                SECTION("try_get_any()") {
                    dict.try_get_any("b");
                }
                audit->end_scope(assert_no_error);
                audit->wait_for_completion();

                auto events = get_audit_events(test_session);
                REQUIRE(events.size() == 2);
                auto& value = events[0].data["value"][0];
                REQUIRE(value["object list"] == json({3, 4, 5}));
                REQUIRE_SET_EQUAL(value["object set"], {6, 7, 8});
                REQUIRE(value["object dictionary"] == json({{"a", {{"_id", 9}, {"value", 90}}},
                                                            {"b", {{"_id", 10}, {"value", 100}}},
                                                            {"c", {{"_id", 11}, {"value", 110}}}}));
            }
        }

        SECTION(
            "link access on an object read outside of a scope does not produce a read on the parent in the scope") {
            Object object(realm, obj);
            audit->begin_scope("scope");
            object.get_property_value<std::any>(context, "object");
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 1);
            auto& event = events[0];
            REQUIRE(event.event == "read");
            REQUIRE(event.data["type"] == "target");
        }

        SECTION("link access in a different scope from the object do not expand linked object in parent read") {
            audit->begin_scope("scope 1");
            Object object(realm, obj);
            audit->end_scope(assert_no_error);

            audit->begin_scope("scope 2");
            object.get_property_value<std::any>(context, "object");
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 2);
            REQUIRE(events[0].activity == "scope 1");
            REQUIRE(events[0].data["type"] == "object");
            REQUIRE(events[1].activity == "scope 2");
            REQUIRE(events[1].data["type"] == "target");
            REQUIRE(events[0].data["value"][0]["object"] == 1);
        }

        SECTION("link access tracking is reset between scopes") {
            audit->begin_scope("scope 1");
            Object object(realm, obj);
            object.get_property_value<std::any>(context, "object");
            audit->end_scope(assert_no_error);

            audit->begin_scope("scope 2");
            // Perform two unrelated events so that the read on `obj` is at
            // an event index after the link access in the previous scope
            Object(realm, target_table->get_object(obj_set.get(0)));
            Object(realm, target_table->get_object(obj_set.get(1)));
            Object(realm, obj);
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 5);
            REQUIRE(events[0].activity == "scope 1");
            REQUIRE(events[1].activity == "scope 1");
            REQUIRE(events[2].activity == "scope 2");
            REQUIRE(events[3].activity == "scope 2");
            REQUIRE(events[4].activity == "scope 2");

            REQUIRE(events[0].data["type"] == "object");
            REQUIRE(events[1].data["type"] == "target");
            REQUIRE(events[2].data["type"] == "target");
            REQUIRE(events[3].data["type"] == "target");
            REQUIRE(events[4].data["type"] == "object");

            // First link should be expanded, second, should not
            REQUIRE(events[0].data["value"][0]["object"] == json({{"_id", 1}, {"value", 1}}));
            REQUIRE(events[4].data["value"][0]["object"] == 1);
        }

        SECTION("read on the parent after the link access do not expand the linked object") {
            Object object(realm, obj);

            audit->begin_scope("scope");
            object.get_property_value<std::any>(context, "object");
            Object(realm, obj);
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 2);
            REQUIRE(events[1].data["value"][0]["object"] == 1);
        }
    }

    SECTION("read on newly created object") {
        realm->begin_transaction();
        audit->begin_scope("scope");
        Object object(realm, table->create_object_with_primary_key(100));
        Results(realm, table->where()).snapshot();
        audit->end_scope(assert_no_error);
        realm->commit_transaction();
        audit->wait_for_completion();

        auto events = get_audit_events(test_session);
        REQUIRE(events.empty());
    }

    SECTION("query matching both new and existing objects") {
        realm->begin_transaction();
        table->create_object_with_primary_key(1);
        realm->commit_transaction();

        realm->begin_transaction();
        table->create_object_with_primary_key(2);
        audit->begin_scope("scope");
        Results(realm, table->where()).snapshot();
        audit->end_scope(assert_no_error);
        realm->commit_transaction();
        audit->wait_for_completion();

        auto events = get_audit_events(test_session);
        REQUIRE(events.size() == 1);
        REQUIRE(events[0].data["value"].size() == 1);
    }

    SECTION("reads mixed with deletions") {
        realm->begin_transaction();
        table->create_object_with_primary_key(1);
        auto obj2 = table->create_object_with_primary_key(2);
        auto obj3 = table->create_object_with_primary_key(3);
        realm->commit_transaction();

        SECTION("reads of objects that are subsequently deleted are still reported") {
            audit->begin_scope("scope");
            realm->begin_transaction();
            Object(realm, obj2);
            obj2.remove();
            realm->commit_transaction();
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 2);
            REQUIRE(events[0].event == "read");
            REQUIRE(events[1].event == "write");
            REQUIRE(events[0].data["value"][0]["_id"] == 2);
        }

        SECTION("reads after deletions report the correct object") {
            audit->begin_scope("scope");
            realm->begin_transaction();
            obj2.remove();
            // In the pre-core-6 version of the code this would incorrectly
            // report a read on obj2
            Object(realm, obj3);
            realm->commit_transaction();
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 2);
            REQUIRE(events[0].event == "read");
            REQUIRE(events[1].event == "write");
            REQUIRE(events[0].data["value"][0]["_id"] == 3);
        }
    }
}

TEST_CASE("audit management") {
    TestClock clock;

    TestSyncManager test_session;
    SyncTestFile config(test_session.app(), "parent");
    config.cache = false;
    config.schema_version = 1;
    config.schema = Schema{
        {"object", {{"_id", PropertyType::Int, Property::IsPrimary{true}}, {"value", PropertyType::Int}}},
    };
    config.audit_config = std::make_shared<AuditConfig>();
    auto realm = Realm::get_shared_realm(config);
    auto audit = realm->audit_context();
    REQUIRE(audit);
    auto table = realm->read_group().get_table("class_object");

    // We open in proper sync mode to let the audit context initialize from that,
    // but we don't actually want the realm to be synchronizing
    realm->sync_session()->close();

    SECTION("cannot nest scopes") {
        audit->begin_scope("name");
        REQUIRE_THROWS(audit->begin_scope("name"));
    }
    SECTION("cannot end nonexistent scope") {
        REQUIRE_THROWS(audit->end_scope());
    }

    SECTION("config validation") {
        SyncTestFile config(test_session.app(), "parent2");
        config.audit_config = std::make_shared<AuditConfig>();
        SECTION("invalid prefix") {
            config.audit_config->partition_value_prefix = "";
            REQUIRE_THROWS(Realm::get_shared_realm(config));
            config.audit_config->partition_value_prefix = "/audit";
            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }
        SECTION("invalid metadata") {
            config.audit_config->metadata = {{"", "a"}};
            REQUIRE_THROWS(Realm::get_shared_realm(config));
            std::string long_name('a', 64);
            config.audit_config->metadata = {{long_name, "b"}};
            REQUIRE_THROWS(Realm::get_shared_realm(config));
            config.audit_config->metadata = {{"activity", "c"}};
            REQUIRE_THROWS(Realm::get_shared_realm(config));
            config.audit_config->metadata = {{"a", "d"}, {"a", "e"}};
            REQUIRE_THROWS(Realm::get_shared_realm(config));
        }
    }

    SECTION("scope names") {
        realm->begin_transaction();
        auto obj = table->create_object_with_primary_key(1);
        realm->commit_transaction();

        audit->begin_scope("scope 1");
        Object(realm, obj);
        audit->end_scope(assert_no_error);

        audit->begin_scope("scope 2");
        Object(realm, obj);
        audit->end_scope(assert_no_error);
        audit->wait_for_completion();

        auto events = get_audit_events(test_session);
        REQUIRE(events.size() == 2);
        REQUIRE(events[0].activity == "scope 1");
        REQUIRE(events[1].activity == "scope 2");
    }

    SECTION("event timestamps") {
        std::vector<Obj> objects;
        realm->begin_transaction();
        for (int i = 0; i < 10; ++i)
            objects.push_back(table->create_object_with_primary_key(i));
        realm->commit_transaction();

        audit->begin_scope("scope");
        for (int i = 0; i < 10; ++i) {
            Object(realm, objects[i]);
            Object(realm, objects[i]);
        }
        audit->end_scope(assert_no_error);
        audit->wait_for_completion();

        auto events = get_audit_events(test_session);
        REQUIRE(events.size() == 10);
        for (int i = 0; i < 10; ++i) {
            // i * 2 because we generate two reads on each object and the second
            // is dropped, but still should have called now().
            REQUIRE(events[i].timestamp == Timestamp(1000 + i * 2, 1000 + i * 2));
        }
    }

    SECTION("metadata updating") {
        realm->begin_transaction();
        auto obj1 = realm->read_group().get_table("class_object")->create_object_with_primary_key(1);
        realm->read_group().get_table("class_object")->create_object_with_primary_key(2);
        realm->read_group().get_table("class_object")->create_object_with_primary_key(3);
        realm->commit_transaction();

        SECTION("update before scope") {
            audit->update_metadata({{"a", "aa"}});
            audit->begin_scope("scope 1");
            Object(realm, obj1);
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 1);
            auto event = events[0];
            REQUIRE(event.metadata.size() == 1);
            REQUIRE(event.metadata["a"] == "aa");
        }

        SECTION("update during scope") {
            audit->begin_scope("scope 1");
            audit->update_metadata({{"a", "aa"}});
            Object(realm, obj1);
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 1);
            auto event = events[0];
            REQUIRE(event.metadata.size() == 0);
        }

        SECTION("one metadata field at a time") {
            for (int i = 0; i < 100; ++i) {
                audit->update_metadata({{util::format("name %1", i), util::format("value %1", i)}});
                audit->begin_scope(util::format("scope %1", i));
                Object(realm, obj1);
                audit->end_scope(assert_no_error);
            }
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 100);
            for (size_t i = 0; i < 100; ++i) {
                REQUIRE(events[i].metadata.size() == 1);
                REQUIRE(events[i].metadata[util::format("name %1", i)] == util::format("value %1", i));
            }
        }

        SECTION("many metadata fields") {
            std::vector<std::pair<std::string, std::string>> metadata;
            for (int i = 0; i < 100; ++i) {
                metadata.push_back({util::format("name %1", i), util::format("value %1", i)});
                audit->update_metadata(std::vector(metadata));
                audit->begin_scope(util::format("scope %1", i));
                Object(realm, obj1);
                audit->end_scope(assert_no_error);
            }
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 100);
            for (size_t i = 0; i < 100; ++i) {
                REQUIRE(events[i].metadata.size() == i + 1);
            }
        }

        SECTION("update via opening new realm") {
            config.audit_config->metadata = {{"a", "aa"}};
            auto realm2 = Realm::get_shared_realm(config);
            auto obj2 = realm2->read_group().get_table("class_object")->get_object(1);

            audit->begin_scope("scope 1");
            Object(realm, obj1);
            Object(realm2, obj2);
            audit->end_scope(assert_no_error);

            config.audit_config->metadata = {{"a", "aaa"}, {"b", "bb"}};
            auto realm3 = Realm::get_shared_realm(config);
            auto obj3 = realm3->read_group().get_table("class_object")->get_object(2);

            audit->begin_scope("scope 2");
            Object(realm, obj1);
            Object(realm2, obj2);
            Object(realm3, obj3);
            audit->end_scope(assert_no_error);
            audit->wait_for_completion();

            auto events = get_audit_events(test_session);
            REQUIRE(events.size() == 5);
            REQUIRE(events[0].activity == "scope 1");
            REQUIRE(events[1].activity == "scope 1");
            REQUIRE(events[2].activity == "scope 2");
            REQUIRE(events[3].activity == "scope 2");
            REQUIRE(events[4].activity == "scope 2");
            REQUIRE(events[0].metadata.size() == 1);
            REQUIRE(events[1].metadata.size() == 1);
            REQUIRE(events[2].metadata.size() == 2);
            REQUIRE(events[3].metadata.size() == 2);
            REQUIRE(events[4].metadata.size() == 2);
        }
    }

    SECTION("custom audit event") {
        // Verify that each of the completion handlers is called in the expected order
        std::atomic<size_t> completions = 0;
        auto expect_completion = [&](size_t expected) {
            return [&completions, expected](std::exception_ptr e) {
                REQUIRE_FALSE(e);
                REQUIRE(completions++ == expected);
            };
        };

        audit->record_event("event 1", "event"s, "data"s, expect_completion(0));
        audit->record_event("event 2", none, "data"s, expect_completion(1));
        audit->begin_scope("scope");
        // note: does not use the scope's activity
        audit->record_event("event 3", none, none, expect_completion(2));
        audit->end_scope(expect_completion(3));
        audit->record_event("event 4", none, none, expect_completion(4));

        util::EventLoop::main().run_until([&] {
            return completions == 5;
        });

        auto events = get_audit_events(test_session, false);
        REQUIRE(events.size() == 4);
        REQUIRE(events[0].activity == "event 1");
        REQUIRE(events[1].activity == "event 2");
        REQUIRE(events[2].activity == "event 3");
        REQUIRE(events[3].activity == "event 4");
        REQUIRE(events[0].event == "event"s);
        REQUIRE(events[1].event == none);
        REQUIRE(events[2].event == none);
        REQUIRE(events[3].event == none);
        REQUIRE(events[0].raw_data == "data"s);
        REQUIRE(events[1].raw_data == "data"s);
        REQUIRE(events[2].raw_data == none);
        REQUIRE(events[3].raw_data == none);
    }

    SECTION("read transaction version management") {
        realm->begin_transaction();
        auto obj = table->create_object_with_primary_key(1);
        realm->commit_transaction();

        auto realm2 = Realm::get_shared_realm(config);
        auto obj2 = realm2->read_group().get_table("class_object")->get_object(0);
        auto realm3 = Realm::get_shared_realm(config);
        auto obj3 = realm3->read_group().get_table("class_object")->get_object(0);

        realm2->begin_transaction();
        obj2.set_all(1);
        realm2->commit_transaction();

        realm3->begin_transaction();
        obj3.set_all(2);
        realm3->commit_transaction();

        audit->begin_scope("scope");
        Object(realm3, obj3); // value 2
        Object(realm2, obj2); // value 1
        Object(realm, obj);   // value 0
        realm->refresh();
        Object(realm, obj);   // value 2
        Object(realm2, obj2); // value 1
        realm2->refresh();
        Object(realm3, obj3); // value 2
        Object(realm2, obj2); // value 2
        Object(realm, obj);   // value 2
        audit->end_scope(assert_no_error);
        audit->wait_for_completion();

        auto events = get_audit_events(test_session);
        REQUIRE(events.size() == 6);
        std::string str = events[0].data.dump();
        // initial
        REQUIRE(events[0].data["value"][0]["value"] == 2);
        REQUIRE(events[1].data["value"][0]["value"] == 1);
        REQUIRE(events[2].data["value"][0]["value"] == 0);

        // realm->refresh()
        REQUIRE(events[3].data["value"][0]["value"] == 2);
        REQUIRE(events[4].data["value"][0]["value"] == 1);

        // realm2->refresh()
        REQUIRE(events[5].data["value"][0]["value"] == 2);
    }

#if !REALM_DEBUG // This test is unreasonably slow in debug mode
    SECTION("large audit scope") {
        realm->begin_transaction();
        auto obj1 = table->create_object_with_primary_key(1);
        auto obj2 = table->create_object_with_primary_key(2);
        realm->commit_transaction();

        audit->begin_scope("large");
        for (int i = 0; i < 150'000; ++i) {
            Object(realm, obj1);
            Object(realm, obj2);
        }
        audit->end_scope(assert_no_error);
        audit->wait_for_completion();

        auto events = get_audit_events(test_session);
        REQUIRE(events.size() == 300'000);
    }
#endif
}

TEST_CASE("audit realm sharding") {
    // Don't start the server immediately so that we're forced to accumulate
    // a lot of local unuploaded data.
    TestSyncManager test_session{{}, {.start_immediately = false}};

    SyncTestFile config(test_session.app(), "parent");
    config.cache = false;
    config.schema_version = 1;
    config.schema = Schema{
        {"object", {{"_id", PropertyType::Int, Property::IsPrimary{true}}, {"value", PropertyType::Int}}},
    };
    config.audit_config = std::make_shared<AuditConfig>();
    auto logger = std::make_shared<util::StderrLogger>(AUDIT_LOG_LEVEL);
    config.audit_config->logger = std::make_shared<util::ThreadSafeLogger>(logger);
    auto realm = Realm::get_shared_realm(config);
    auto audit = realm->audit_context();
    REQUIRE(audit);
    auto table = realm->read_group().get_table("class_object");

    // We open in proper sync mode to let the audit context initialize from that,
    // but we don't actually want the realm to be synchronizing
    realm->sync_session()->close();

    // Set a small shard size so that we don't have to write an absurd
    // amount of data to test this
    audit_test_hooks::set_maximum_shard_size(32 * 1024);
    auto cleanup = util::make_scope_exit([]() noexcept {
        audit_test_hooks::set_maximum_shard_size(256 * 1024 * 1024);
    });

    realm->begin_transaction();
    std::vector<Obj> objects;
    for (int i = 0; i < 2000; ++i)
        objects.push_back(table->create_object_with_primary_key(i));
    realm->commit_transaction();

    // Write a lot of audit scopes while unable to sync
    for (int i = 0; i < 50; ++i) {
        audit->begin_scope(util::format("scope %1", i));
        Results(realm, table->where()).snapshot();
        audit->end_scope(assert_no_error);
    }
    audit->wait_for_completion();

    // There should now be several unuploaded Realms in the local client
    // directory
    auto root = test_session.base_file_path() + "/realm-audit/app_id/test/audit";
    std::string file_name;
    util::DirScanner dir(root);
    size_t file_count = 0;
    size_t unlocked_file_count = 0;
    while (dir.next(file_name)) {
        if (!StringData(file_name).ends_with(".realm"))
            continue;
        ++file_count;
        // The upper limit is a soft cap, so files might be a bit bigger
        // than it. 1 MB errs on the side of never getting spurious failures.
        REQUIRE(util::File::get_size_static(root + "/" + file_name) < 1024 * 1024);
        if (DB::call_with_lock(root + "/" + file_name, [](auto&) {})) {
            ++unlocked_file_count;
        }
    }
    // The exact number of shards is fuzzy due to the combination of the
    // soft cap on size and the fact that changesets are compressed, but
    // there definitely should be more than one.
    REQUIRE(file_count > 2);
    // There should be exactly two files open still: the one we're currently
    // writing to, and the first one which we wrote and are waiting for the
    // upload to complete.
    REQUIRE(unlocked_file_count == file_count - 2);

    auto get_sorted_events = [&] {
        auto events = get_audit_events(test_session, false);
        // The events might be out of order because there's no guaranteed order
        // for both uploading the Realms and for opening the uploaded Realms.
        // Once sorted by timestamp the scopes should be in order, though.
        sort_events(events);
        return events;
    };
    auto close_all_sessions = [&] {
        realm->close();
        realm = nullptr;
        auto sync_manager = test_session.app()->sync_manager();
        for (auto& session : sync_manager->get_all_sessions()) {
            session->shutdown_and_wait();
        }
    };

    SECTION("start server with existing session open") {
        test_session.sync_server().start();
        audit->wait_for_uploads();

        auto events = get_sorted_events();
        REQUIRE(events.size() == 50);
        for (int i = 0; i < 50; ++i) {
            REQUIRE(events[i].activity == util::format("scope %1", i));
        }

        // There should be exactly one remaining local Realm file (the currently
        // open one that hasn't hit the size limit yet)
        size_t remaining_realms = 0;
        util::DirScanner dir(root);
        while (dir.next(file_name)) {
            if (StringData(file_name).ends_with(".realm"))
                ++remaining_realms;
        }
        REQUIRE(remaining_realms == 1);
    }

    SECTION("trigger uploading by opening a new Realm") {
        close_all_sessions();
        test_session.sync_server().start();

        // Open a different Realm with the same user and audit prefix
        SyncTestFile config(test_session.app(), "other");
        config.audit_config = std::make_shared<AuditConfig>();
        config.audit_config->logger = std::make_shared<util::ThreadSafeLogger>(logger);
        auto realm = Realm::get_shared_realm(config);
        auto audit2 = realm->audit_context();
        REQUIRE(audit2);
        audit2->wait_for_uploads();

        auto events = get_sorted_events();
        REQUIRE(events.size() == 50);
        for (int i = 0; i < 50; ++i) {
            REQUIRE(events[i].activity == util::format("scope %1", i));
        }

        // There should be no remaining local Realm files because we haven't
        // made the new audit context open a Realm yet
        util::DirScanner dir(root);
        while (dir.next(file_name)) {
            REQUIRE_FALSE(StringData(file_name).ends_with(".realm"));
        }
    }

    SECTION("uploading is per audit prefix") {
        close_all_sessions();
        test_session.sync_server().start();

        // Open the same Realm with a different audit prefix
        SyncTestFile config(test_session.app(), "parent");
        config.audit_config = std::make_shared<AuditConfig>();
        config.audit_config->logger = std::make_shared<util::ThreadSafeLogger>(logger);
        config.audit_config->partition_value_prefix = "other";
        auto realm = Realm::get_shared_realm(config);
        auto audit2 = realm->audit_context();
        REQUIRE(audit2);
        audit2->wait_for_uploads();

        // Should not have uploaded any of the old events
        auto events = get_sorted_events();
        REQUIRE(events.size() == 0);
    }
}

#if REALM_ENABLE_AUTH_TESTS
static void generate_event(std::shared_ptr<Realm> realm, int call = 0)
{
    auto table = realm->read_group().get_table("class_object");
    auto audit = realm->audit_context();

    realm->begin_transaction();
    table->create_object_with_primary_key(call + 1).set_all(2);
    realm->commit_transaction();

    audit->begin_scope("scope");
    Object(realm, table->get_object(call));
    audit->end_scope(assert_no_error);
}

TEST_CASE("audit integration tests") {
    // None of these tests need a deterministic clock, but the server rounding
    // timestamps to milliseconds can result in events not having monotonically
    // increasing timestamps with an actual clock.
    TestClock clock;

    const Schema schema{
        {"object", {{"_id", PropertyType::Int, Property::IsPrimary{true}}, {"value", PropertyType::Int}}},
        {"AuditEvent",
         {
             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"timestamp", PropertyType::Date},
             {"activity", PropertyType::String},
             {"event", PropertyType::String | PropertyType::Nullable},
             {"data", PropertyType::String | PropertyType::Nullable},
             {"metadata 1", PropertyType::String | PropertyType::Nullable},
             {"metadata 2", PropertyType::String | PropertyType::Nullable},
         }}};
    const Schema no_audit_event_schema{
        {"object", {{"_id", PropertyType::Int, Property::IsPrimary{true}}, {"value", PropertyType::Int}}}};

    auto app_create_config = default_app_config(get_base_url());
    app_create_config.schema = schema;
    app_create_config.dev_mode_enabled = false;
    TestAppSession session = create_app(app_create_config);

    SyncTestFile config(session.app()->current_user(), bson::Bson("default"));
    config.schema = schema;
    config.audit_config = std::make_shared<AuditConfig>();
    config.audit_config->logger =
        std::make_shared<util::ThreadSafeLogger>(std::make_shared<util::StderrLogger>(AUDIT_LOG_LEVEL));

    auto expect_error = [&](auto&& config, auto&& fn) -> SyncError {
        std::mutex mutex;
        util::Optional<SyncError> error;
        config.audit_config->sync_error_handler = [&](SyncError e) {
            std::lock_guard lock(mutex);
            error = e;
        };

        auto realm = Realm::get_shared_realm(config);
        fn(realm, 0);

        timed_wait_for(
            [&] {
                std::lock_guard lock(mutex);
                return (bool)error;
            },
            std::chrono::seconds(30));
        REQUIRE(bool(error));
        return *error;
    };

    SECTION("basic functionality") {
        auto realm = Realm::get_shared_realm(config);
        realm->sync_session()->close();
        generate_event(realm);

        auto events = get_audit_events_from_baas(session, *config.sync_config->user, 1);
        REQUIRE(events.size() == 1);
        REQUIRE(events[0].activity == "scope");
        REQUIRE(events[0].event == "read");
        REQUIRE(!events[0].timestamp.is_null()); // FIXME
        REQUIRE(events[0].data == json({{"type", "object"}, {"value", {{{"_id", 1}, {"value", 2}}}}}));
    }

    SECTION("different user from parent Realm") {
        create_user_and_log_in(session.app());
        config.audit_config->audit_user = session.app()->current_user();
        auto realm = Realm::get_shared_realm(config);
        // If audit uses the sync user this'll make it fail as that user is logged out
        config.sync_config->user->log_out();

        generate_event(realm);
        REQUIRE(get_audit_events_from_baas(session, *config.audit_config->audit_user, 1).size() == 1);
    }

    SECTION("different app from parent Realm") {
        auto audit_user = config.sync_config->user;

        // Create an app which does not include AuditEvent in the schema so that
        // things will break if audit tries to use it
        app_create_config.schema = no_audit_event_schema;
        TestAppSession session_2 = create_app(app_create_config);
        SyncTestFile config(session_2.app()->current_user(), bson::Bson("default"));
        config.schema = no_audit_event_schema;
        config.audit_config = std::make_shared<AuditConfig>();
        config.audit_config->audit_user = audit_user;

        auto realm = Realm::get_shared_realm(config);
        generate_event(realm);
        REQUIRE(get_audit_events_from_baas(session, *audit_user, 1).size() == 1);
    }

    SECTION("valid metadata properties") {
        auto realm = Realm::get_shared_realm(config);
        generate_event(realm, 0);
        realm->audit_context()->update_metadata({{"metadata 1", "value 1"}});
        generate_event(realm, 1);
        realm->audit_context()->update_metadata({{"metadata 2", "value 2"}});
        generate_event(realm, 2);
        realm->audit_context()->update_metadata({{"metadata 1", "value 3"}, {"metadata 2", "value 4"}});
        generate_event(realm, 3);

        using Metadata = std::map<std::string, std::string>;
        auto events = get_audit_events_from_baas(session, *config.sync_config->user, 4);
        REQUIRE(events[0].metadata.empty());
        REQUIRE(events[1].metadata == Metadata({{"metadata 1", "value 1"}}));
        REQUIRE(events[2].metadata == Metadata({{"metadata 2", "value 2"}}));
        REQUIRE(events[3].metadata == Metadata({{"metadata 1", "value 3"}, {"metadata 2", "value 4"}}));
    }

    SECTION("invalid metadata properties") {
        config.audit_config->metadata = {{"invalid key", "value"}};
        auto error = expect_error(config, generate_event);
        REQUIRE(error.message.find("Invalid schema change") == 0);
        REQUIRE(error.is_fatal);
    }

    SECTION("removed sync user") {
        create_user_and_log_in(session.app());
        auto audit_user = session.app()->current_user();
        config.audit_config->audit_user = audit_user;
        auto realm = Realm::get_shared_realm(config);
        session.app()->sync_manager()->remove_user(audit_user->identity());

        auto audit = realm->audit_context();
        audit->begin_scope("scope");
        realm->begin_transaction();
        auto table = realm->read_group().get_table("class_object");
        table->create_object_with_primary_key(1).set_all(2);
        realm->commit_transaction();

        audit->end_scope([&](auto error) {
            REQUIRE(error);
            REQUIRE_THROWS_CONTAINING(std::rethrow_exception(error), "user has been removed");
        });
        audit->wait_for_completion();
    }

    SECTION("AuditEvent missing from server schema") {
        app_create_config.schema = no_audit_event_schema;
        TestAppSession session_2 = create_app(app_create_config);
        SyncTestFile config(session_2.app()->current_user(), bson::Bson("default"));
        config.schema = no_audit_event_schema;
        config.audit_config = std::make_shared<AuditConfig>();

        auto error = expect_error(config, generate_event);
        REQUIRE(error.message.find("Invalid schema change") == 0);
        REQUIRE(error.is_fatal);
    }

    SECTION("incoming changesets are discarded") {
        app::MongoClient remote_client = config.sync_config->user->mongo_client("BackingDB");
        app::MongoDatabase db = remote_client.db(session.app_session().config.mongo_dbname);
        app::MongoCollection collection = db["AuditEvent"];

        SECTION("objects deleted on server") {
            // Because EraseObject is idempotent, this case actually just works
            // without any special logic.
            auto delete_one = [&] {
                uint64_t deleted = 0;
                while (deleted == 0) {
                    collection.delete_one({},
                                          [&](util::Optional<uint64_t>&& count, util::Optional<app::AppError> error) {
                                              REQUIRE_FALSE(error);
                                              deleted = *count;
                                          });
                    if (deleted == 0) {
                        millisleep(100); // slow down the number of retries
                    }
                }
            };

            auto realm = Realm::get_shared_realm(config);
            for (int i = 0; i < 10; ++i) {
                generate_event(realm, i);
                delete_one();
            }
        }

        SECTION("objects modified on server") {
            // UpdateObject throws bad_transaction_log() if the object doesn't
            // exist locally, so this will break if we try to apply the changesets
            // from the server.
            const bson::BsonDocument filter{{"event", "read"}};
            const bson::BsonDocument update{{"$set", bson::BsonDocument{{"event", "processed"}}}};
            auto update_one = [&] {
                int32_t count = 0;
                while (count == 0) {
                    collection.update_one(
                        filter, update,
                        [&](app::MongoCollection::UpdateResult result, util::Optional<app::AppError> error) {
                            REQUIRE_FALSE(error);
                            count = result.modified_count;
                        });
                    if (count == 0) {
                        millisleep(100); // slow down the number of retries
                    }
                }
            };

            auto realm = Realm::get_shared_realm(config);
            for (int i = 0; i < 10; ++i) {
                generate_event(realm, i);
                update_one();
            }
        }
    }

    SECTION("flexible sync") {
        app::FLXSyncTestHarness harness("audit");
        create_user_and_log_in(harness.app());

        SECTION("auditing a flexible sync realm without specifying an audit user throws an exception") {
            SyncTestFile config(harness.app()->current_user(), {}, SyncConfig::FLXSyncEnabled{});
            config.audit_config = std::make_shared<AuditConfig>();
            REQUIRE_THROWS_CONTAINING(Realm::get_shared_realm(config), "partition-based sync");
        }

        SECTION("auditing with a flexible sync user reports a sync error") {
            config.audit_config->audit_user = harness.app()->current_user();
            auto error = expect_error(config, generate_event);
            REQUIRE(error.message.find(
                        "client connected using partition based sync when app is using flexible sync") == 0);
            REQUIRE(error.is_fatal);
        }

        SECTION("auditing a flexible sync realm with a pbs audit user works") {
            config.audit_config->audit_user = config.sync_config->user;
            config.sync_config->user = harness.app()->current_user();
            config.sync_config->flx_sync_requested = true;
            config.sync_config->partition_value.clear();

            auto realm = Realm::get_shared_realm(config);
            {
                auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
                mut_subs.insert_or_assign(Query(realm->read_group().get_table("class_object")));
                std::move(mut_subs).commit();
            }

            realm->sync_session()->log_out();
            generate_event(realm, 0);
            get_audit_events_from_baas(session, *config.audit_config->audit_user, 1);
        }
    }

#if 0 // This test takes ~10 minutes to run
    SECTION("large audit scope") {
        auto realm = Realm::get_shared_realm(config);
        auto table = realm->read_group().get_table("class_object");
        auto audit = realm->audit_context();

        realm->begin_transaction();
        auto obj1 = table->create_object_with_primary_key(1);
        auto obj2 = table->create_object_with_primary_key(2);
        realm->commit_transaction();

        audit->begin_scope("large");
        for (int i = 0; i < 150'000; ++i) {
            Object(realm, obj1);
            Object(realm, obj2);
        }
        audit->end_scope(assert_no_error);

        REQUIRE(get_audit_events_from_baas(session, *session.app()->current_user(), 300'000).size() == 300'000);
    }
#endif
}
#endif // REALM_ENABLE_AUTH_TESTS
