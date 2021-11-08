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

#include "util/test_file.hpp"
#include "util/test_utils.hpp"

#include <realm/object-store/list.hpp>
#include <realm/object-store/object.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/thread_safe_reference.hpp>
#include <realm/object-store/util/scheduler.hpp>

#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>

#include <realm/db.hpp>
#include <realm/history.hpp>
#include <realm/string_data.hpp>
#include <realm/util/optional.hpp>

using namespace realm;

static TableRef get_table(Realm& realm, StringData object_name)
{
    return ObjectStore::table_for_object_type(realm.read_group(), object_name);
}

static Object create_object(SharedRealm const& realm, StringData object_type, AnyDict value)
{
    CppContext ctx(realm);
    return Object::create(ctx, realm, object_type, util::Any(value));
}

TEST_CASE("thread safe reference") {
    using namespace std::string_literals;

    Schema schema{
        {"string object",
         {
             {"value", PropertyType::String | PropertyType::Nullable},
         }},
        {"int object",
         {
             {"value", PropertyType::Int},
         }},
        {"int array object", {{"value", PropertyType::Array | PropertyType::Object, "int object"}}},
        {"int array", {{"value", PropertyType::Array | PropertyType::Int}}},
    };

    InMemoryTestFile config;
    config.automatic_change_notifications = false;
    config.cache = false;
    config.schema = schema;
    auto r = Realm::get_shared_realm(config);

    const auto int_obj_col = r->schema().find("int object")->persisted_properties[0].column_key;

    SECTION("version pinning") {
        CppContext ctx(r);
        r->begin_transaction();
        auto int_obj = create_object(r, "int object", {{"value", INT64_C(7)}});
        auto string_obj = create_object(r, "string object", {{"value", "a"s}});
        auto int_list_obj = create_object(r, "int array", {{"value", AnyVector{INT64_C(7)}}});
        auto int_obj_list_obj = create_object(r, "int array object", {{"value", AnyVector{int_obj}}});
        auto int_list = List(int_list_obj, int_list_obj.get_object_schema().property_for_name("value"));
        auto int_obj_list = List(int_obj_list_obj, int_obj_list_obj.get_object_schema().property_for_name("value"));
        r->commit_transaction();

        auto history = make_in_realm_history();
        auto db = DB::create(*history, config.path, config.options());
        auto initial_version = db->get_version_id_of_latest_snapshot();
        REQUIRE(db->get_number_of_versions() == 2);

        SECTION("Results pins the source version") {
            auto table = r->read_group().get_table("class_int object");
            auto results = Results(r, table->where());
            auto ref = util::make_optional(ThreadSafeReference(results));

            // This commit does not increase the number of versions because it
            // cleans up the version *before* the one we pinned
            r->begin_transaction();
            r->commit_transaction();
            REQUIRE(db->get_number_of_versions() == 2);

            // This commit would clean up the pinned version if it was not pinned
            r->begin_transaction();
            r->commit_transaction();
            REQUIRE(db->get_number_of_versions() == 3);
            REQUIRE_NOTHROW(db->start_read(initial_version));

            // Once we release the TSR the next write should clean up the version
            ref = {};

            REQUIRE(db->get_number_of_versions() == 3);
            r->begin_transaction();
            r->commit_transaction();
            REQUIRE(db->get_number_of_versions() == 2);
            REQUIRE_THROWS(db->start_read(initial_version));
        }

        SECTION("other types do not") {
            auto obj_ref = ThreadSafeReference(int_obj);
            auto list_ref = ThreadSafeReference(int_list);
            auto list_obj_ref = ThreadSafeReference(int_obj_list);

            // Cleans up the commit before initial_version
            r->begin_transaction();
            r->commit_transaction();
            REQUIRE(db->get_number_of_versions() == 2);

            // Cleans up initial_version
            r->begin_transaction();
            r->commit_transaction();
            REQUIRE(db->get_number_of_versions() == 2);
            REQUIRE_THROWS(db->start_read(initial_version));

            // Should still be resolvable
            auto obj_2 = obj_ref.resolve<Object>(r);
            auto list_2 = list_ref.resolve<List>(r);
            auto list_obj_2 = list_obj_ref.resolve<List>(r);
            REQUIRE(obj_2.obj().get<int64_t>("value") == 7);
            REQUIRE(list_2.size() == 1);
            REQUIRE(list_obj_2.size() == 1);
        }
    }

    SECTION("coordinator pinning") {
        CppContext ctx(r);
        r->begin_transaction();
        auto int_obj = create_object(r, "int object", {{"value", INT64_C(7)}});
        auto string_obj = create_object(r, "string object", {{"value", "a"s}});
        auto int_list_obj = create_object(r, "int array", {{"value", AnyVector{INT64_C(7)}}});
        auto int_obj_list_obj = create_object(r, "int array object", {{"value", AnyVector{int_obj}}});
        auto int_list = List(int_list_obj, int_list_obj.get_object_schema().property_for_name("value"));
        auto int_obj_list = List(int_obj_list_obj, int_obj_list_obj.get_object_schema().property_for_name("value"));
        r->commit_transaction();

        SECTION("Results retains the source RealmCoordinator") {
            auto coordinator = _impl::RealmCoordinator::get_existing_coordinator(config.path).get();
            auto table = r->read_group().get_table("class_int object");
            auto results = Results(r, table->where());
            auto ref = util::make_optional(ThreadSafeReference(results));

            r->close();
            REQUIRE(coordinator == _impl::RealmCoordinator::get_existing_coordinator(config.path).get());
            ref = {};
            REQUIRE_FALSE(_impl::RealmCoordinator::get_existing_coordinator(config.path).get());
        }

        SECTION("other types do not") {
            auto obj_ref = ThreadSafeReference(int_obj);
            auto list_ref = ThreadSafeReference(int_list);
            auto list_obj_ref = ThreadSafeReference(int_obj_list);

            r->close();
            REQUIRE_FALSE(_impl::RealmCoordinator::get_existing_coordinator(config.path).get());
        }
    }

    SECTION("version mismatch") {
        CppContext ctx(r);
        r->begin_transaction();
        auto int_obj = create_object(r, "int object", {{"value", INT64_C(7)}});
        r->commit_transaction();
        ColKey col = int_obj.get_object_schema().property_for_name("value")->column_key;
        ObjKey k = int_obj.obj().get_key();
        REQUIRE(int_obj.obj().get<Int>(col) == 7);

        SECTION("resolves at older version") {
            ThreadSafeReference ref;
            {
                SharedRealm r2 = Realm::get_shared_realm(config);
                Object num = Object(r2, "int object", k);
                REQUIRE(num.obj().get<Int>(col) == 7);

                r2->begin_transaction();
                num.obj().set(col, 9);
                r2->commit_transaction();

                ref = num;
            };

            REQUIRE(int_obj.obj().get<Int>(col) == 7);
            Object obj_2 = ref.resolve<Object>(r);
            REQUIRE(obj_2.obj().get<Int>(col) == 9);
            REQUIRE(int_obj.obj().get<Int>(col) == 9);

            r->begin_transaction();
            int_obj.obj().set(col, 11);
            r->commit_transaction();

            REQUIRE(obj_2.obj().get<Int>(col) == 11);
            REQUIRE(int_obj.obj().get<Int>(col) == 11);
        }

        SECTION("resolve at newer version") {
            auto ref = ThreadSafeReference(int_obj);
            {
                SharedRealm r2 = Realm::get_shared_realm(config);
                Obj obj_2 = Object(r2, "int object", k).obj();

                r2->begin_transaction();
                obj_2.set(col, 9);
                r2->commit_transaction();
                REQUIRE(obj_2.get<Int>(col) == 9);

                Obj obj_resolved = ref.resolve<Object>(r2).obj();
                REQUIRE(obj_resolved.get<Int>(col) == 9);

                r2->begin_transaction();
                obj_resolved.set(col, 11);
                r2->commit_transaction();

                REQUIRE(obj_2.get<Int>(col) == 11);
                REQUIRE(obj_resolved.get<Int>(col) == 11);
            }

            REQUIRE(int_obj.obj().get<Int>(col) == 7);
            r->refresh();
            REQUIRE(int_obj.obj().get<Int>(col) == 11);
        }

        SECTION("resolve references with multiple source versions") {
            auto commit_new_num = [&](int64_t value) -> Object {
                r->begin_transaction();
                Object num = create_object(r, "int object", {{"value", value}});
                r->commit_transaction();
                return num;
            };

            auto ref1 = ThreadSafeReference(commit_new_num(1));
            auto ref2 = ThreadSafeReference(commit_new_num(2));
            {
                SharedRealm r2 = Realm::get_shared_realm(config);
                Object num1 = ref1.resolve<Object>(r2);
                Object num2 = ref2.resolve<Object>(r2);

                ColKey col = num1.get_object_schema().property_for_name("value")->column_key;
                REQUIRE(num1.obj().get<Int>(col) == 1);
                REQUIRE(num2.obj().get<Int>(col) == 2);
            }
        }
    }

    SECTION("same thread") {
        r->begin_transaction();
        Object num = create_object(r, "int object", {{"value", INT64_C(7)}});
        r->commit_transaction();

        ColKey col = num.get_object_schema().property_for_name("value")->column_key;
        REQUIRE(num.obj().get<Int>(col) == 7);
        auto ref = ThreadSafeReference(num);
        bool did_run_section = false;

        SECTION("same realm") {
            did_run_section = true;
            {
                Object num = ref.resolve<Object>(r);
                REQUIRE(num.obj().get<Int>(col) == 7);
                r->begin_transaction();
                num.obj().set(col, 9);
                r->commit_transaction();
                REQUIRE(num.obj().get<Int>(col) == 9);
            }
            REQUIRE(num.obj().get<Int>(col) == 9);
        }
        SECTION("different realm") {
            did_run_section = true;
            {
                SharedRealm r = Realm::get_shared_realm(config);
                Object num = ref.resolve<Object>(r);
                REQUIRE(num.obj().get<Int>(col) == 7);
                r->begin_transaction();
                num.obj().set(col, 9);
                r->commit_transaction();
                REQUIRE(num.obj().get<Int>(col) == 9);
            }
            REQUIRE(num.obj().get<Int>(col) == 7);
        }
        catch2_ensure_section_run_workaround(did_run_section, "same thread", [&]() {
            r->begin_transaction(); // advance to latest version by starting a write
            REQUIRE(num.obj().get<Int>(col) == 9);
            r->cancel_transaction();
        });
    }

    SECTION("passing over") {
        SECTION("read-only `ThreadSafeReference`") {
            // We need to create a new `configuration` for the read-only tests since the `InMemoryTestFile` will be
            // gone as soon as we `close()` it which we need to do so we can re-open it in read-only after preparing /
            // writing data to it.
            TestFile config;
            config.schema = schema;
            SharedRealm realm = Realm::get_shared_realm(config);
            realm->begin_transaction();
            create_object(realm, "int object", {{"value", INT64_C(42)}});
            realm->commit_transaction();
            realm->close();
            config.schema_mode = SchemaMode::Immutable;
            SharedRealm read_only_realm = Realm::get_shared_realm(config);
            auto table = read_only_realm->read_group().get_table("class_int object");
            Results results(read_only_realm, table);
            REQUIRE(results.size() == 1);
            REQUIRE(results.get(0).get<int64_t>(int_obj_col) == 42);

            SECTION("read-only `ThreadSafeReference` to `Results`") {
                auto thread_safe_results = ThreadSafeReference(results);
                JoiningThread([thread_safe_results = std::move(thread_safe_results), config, int_obj_col]() mutable {
                    SharedRealm realm_in_thread = Realm::get_shared_realm(config);
                    Results resolved_results = thread_safe_results.resolve<Results>(realm_in_thread);
                    REQUIRE(resolved_results.size() == 1);
                    REQUIRE(resolved_results.get(0).get<int64_t>(int_obj_col) == 42);
                });
            }

            SECTION("read-only `ThreadSafeReference` to an `Object`") {
                Object object(read_only_realm, results.get(0));
                auto thread_safe_object = ThreadSafeReference(object);
                JoiningThread([thread_safe_object = std::move(thread_safe_object), config, int_obj_col]() mutable {
                    SharedRealm realm_in_thread = Realm::get_shared_realm(config);
                    auto resolved_object = thread_safe_object.resolve<Object>(realm_in_thread);
                    REQUIRE(resolved_object.is_valid());
                    REQUIRE(resolved_object.obj().get<int64_t>(int_obj_col) == 42);
                });
            }
        }

        SECTION("objects") {
            r->begin_transaction();
            auto str = create_object(r, "string object", {});
            auto num = create_object(r, "int object", {{"value", INT64_C(0)}});
            r->commit_transaction();

            ColKey col_num = num.get_object_schema().property_for_name("value")->column_key;
            ColKey col_str = str.get_object_schema().property_for_name("value")->column_key;
            auto ref_str = ThreadSafeReference(str);
            auto ref_num = ThreadSafeReference(num);
            {
                SharedRealm r2 = Realm::get_shared_realm(config);
                Object str = ref_str.resolve<Object>(r2);
                Object num = ref_num.resolve<Object>(r2);

                REQUIRE(str.obj().get<String>(col_str).is_null());
                REQUIRE(num.obj().get<Int>(col_num) == 0);

                r2->begin_transaction();
                str.obj().set(col_str, "the meaning of life");
                num.obj().set(col_num, 42);
                r2->commit_transaction();
            }

            REQUIRE(str.obj().get<String>(col_str).is_null());
            REQUIRE(num.obj().get<Int>(col_num) == 0);

            r->refresh();

            REQUIRE(str.obj().get<String>(col_str) == "the meaning of life");
            REQUIRE(num.obj().get<Int>(col_num) == 42);
        }

        SECTION("object list") {
            r->begin_transaction();
            auto zero = create_object(r, "int object", {{"value", INT64_C(0)}});
            auto obj = create_object(r, "int array object", {{"value", AnyVector{zero}}});
            auto col = get_table(*r, "int array object")->get_column_key("value");
            List list(r, obj.obj(), col);
            r->commit_transaction();

            REQUIRE(list.size() == 1);
            REQUIRE(list.get(0).get<int64_t>(int_obj_col) == 0);
            auto ref = ThreadSafeReference(list);
            {
                SharedRealm r2 = Realm::get_shared_realm(config);
                List list = ref.resolve<List>(r2);
                REQUIRE(list.size() == 1);
                REQUIRE(list.get(0).get<int64_t>(int_obj_col) == 0);

                r2->begin_transaction();
                list.remove_all();
                auto one = create_object(r2, "int object", {{"value", INT64_C(1)}});
                auto two = create_object(r2, "int object", {{"value", INT64_C(2)}});
                list.add(one.obj());
                list.add(two.obj());
                r2->commit_transaction();

                REQUIRE(list.size() == 2);
                REQUIRE(list.get(0).get<int64_t>(int_obj_col) == 1);
                REQUIRE(list.get(1).get<int64_t>(int_obj_col) == 2);
            }

            REQUIRE(list.size() == 1);
            REQUIRE(list.get(0).get<int64_t>(int_obj_col) == 0);

            r->refresh();

            REQUIRE(list.size() == 2);
            REQUIRE(list.get(0).get<int64_t>(int_obj_col) == 1);
            REQUIRE(list.get(1).get<int64_t>(int_obj_col) == 2);
        }

        SECTION("sorted object results") {
            auto& table = *get_table(*r, "string object");
            auto col = table.get_column_key("value");
            auto results = Results(r, table.where().not_equal(col, "C")).sort({{{col}}, {false}});

            r->begin_transaction();
            create_object(r, "string object", {{"value", "A"s}});
            create_object(r, "string object", {{"value", "B"s}});
            create_object(r, "string object", {{"value", "C"s}});
            create_object(r, "string object", {{"value", "D"s}});
            r->commit_transaction();

            REQUIRE(results.size() == 3);
            REQUIRE(results.get(0).get<StringData>(col) == "D");
            REQUIRE(results.get(1).get<StringData>(col) == "B");
            REQUIRE(results.get(2).get<StringData>(col) == "A");
            auto ref = ThreadSafeReference(results);
            {
                SharedRealm r2 = Realm::get_shared_realm(config);
                Results results = ref.resolve<Results>(r2);

                REQUIRE(results.size() == 3);
                REQUIRE(results.get(0).get<StringData>(col) == "D");
                REQUIRE(results.get(1).get<StringData>(col) == "B");
                REQUIRE(results.get(2).get<StringData>(col) == "A");

                r2->begin_transaction();
                results.get(2).remove();
                results.get(0).remove();
                create_object(r2, "string object", {{"value", "E"s}});
                r2->commit_transaction();

                REQUIRE(results.size() == 2);
                REQUIRE(results.get(0).get<StringData>(col) == "E");
                REQUIRE(results.get(1).get<StringData>(col) == "B");
            }

            REQUIRE(results.size() == 3);
            REQUIRE(results.get(0).get<StringData>(col) == "D");
            REQUIRE(results.get(1).get<StringData>(col) == "B");
            REQUIRE(results.get(2).get<StringData>(col) == "A");

            r->refresh();

            REQUIRE(results.size() == 2);
            REQUIRE(results.get(0).get<StringData>(col) == "E");
            REQUIRE(results.get(1).get<StringData>(col) == "B");
        }

        SECTION("distinct object results") {
            auto& table = *get_table(*r, "string object");
            auto col = table.get_column_key("value");
            auto results = Results(r, table.where()).distinct({{{col}}}).sort({{"value", true}});

            r->begin_transaction();
            create_object(r, "string object", {{"value", "A"s}});
            create_object(r, "string object", {{"value", "A"s}});
            create_object(r, "string object", {{"value", "B"s}});
            r->commit_transaction();

            REQUIRE(results.size() == 2);
            REQUIRE(results.get(0).get<StringData>(col) == "A");
            REQUIRE(results.get(1).get<StringData>(col) == "B");
            auto ref = ThreadSafeReference(results);
            {
                SharedRealm r2 = Realm::get_shared_realm(config);
                Results results = ref.resolve<Results>(r2);

                REQUIRE(results.size() == 2);
                REQUIRE(results.get(0).get<StringData>(col) == "A");
                REQUIRE(results.get(1).get<StringData>(col) == "B");

                r2->begin_transaction();
                results.get(0).remove();
                create_object(r2, "string object", {{"value", "C"s}});
                r2->commit_transaction();

                REQUIRE(results.size() == 3);
                REQUIRE(results.get(0).get<StringData>(col) == "A");
                REQUIRE(results.get(1).get<StringData>(col) == "B");
                REQUIRE(results.get(2).get<StringData>(col) == "C");
            }

            REQUIRE(results.size() == 2);
            REQUIRE(results.get(0).get<StringData>(col) == "A");
            REQUIRE(results.get(1).get<StringData>(col) == "B");

            r->refresh();

            REQUIRE(results.size() == 3);
            REQUIRE(results.get(0).get<StringData>(col) == "A");
            REQUIRE(results.get(1).get<StringData>(col) == "B");
            REQUIRE(results.get(2).get<StringData>(col) == "C");
        }

        SECTION("int list") {
            r->begin_transaction();
            auto obj = create_object(r, "int array", {{"value", AnyVector{INT64_C(0)}}});
            auto col = get_table(*r, "int array")->get_column_key("value");
            List list(r, obj.obj(), col);
            r->commit_transaction();

            auto ref = ThreadSafeReference(list);
            {
                SharedRealm r2 = Realm::get_shared_realm(config);
                List list = ref.resolve<List>(r2);
                REQUIRE(list.size() == 1);
                REQUIRE(list.get<int64_t>(0) == 0);

                r2->begin_transaction();
                list.remove_all();
                list.add(int64_t(1));
                list.add(int64_t(2));
                r2->commit_transaction();

                REQUIRE(list.size() == 2);
                REQUIRE(list.get<int64_t>(0) == 1);
                REQUIRE(list.get<int64_t>(1) == 2);
            };

            REQUIRE(list.size() == 1);
            REQUIRE(list.get<int64_t>(0) == 0);

            r->refresh();

            REQUIRE(list.size() == 2);
            REQUIRE(list.get<int64_t>(0) == 1);
            REQUIRE(list.get<int64_t>(1) == 2);
        }

        SECTION("sorted int results") {
            r->begin_transaction();
            auto obj = create_object(r, "int array", {{"value", AnyVector{INT64_C(0), INT64_C(2), INT64_C(1)}}});
            auto col = get_table(*r, "int array")->get_column_key("value");
            List list(r, obj.obj(), col);
            r->commit_transaction();

            auto results = list.sort({{"self", true}});

            REQUIRE(results.size() == 3);
            REQUIRE(results.get<int64_t>(0) == 0);
            REQUIRE(results.get<int64_t>(1) == 1);
            REQUIRE(results.get<int64_t>(2) == 2);
            auto ref = ThreadSafeReference(results);
            JoiningThread([ref = std::move(ref), config]() mutable {
                config.scheduler = util::Scheduler::make_frozen(VersionID());
                SharedRealm r = Realm::get_shared_realm(config);
                Results results = ref.resolve<Results>(r);

                REQUIRE(results.size() == 3);
                REQUIRE(results.get<int64_t>(0) == 0);
                REQUIRE(results.get<int64_t>(1) == 1);
                REQUIRE(results.get<int64_t>(2) == 2);

                r->begin_transaction();
                auto table = get_table(*r, "int array");
                List list(r, *table->begin(), table->get_column_key("value"));
                list.remove(1);
                list.add(int64_t(-1));
                r->commit_transaction();

                REQUIRE(results.size() == 3);
                REQUIRE(results.get<int64_t>(0) == -1);
                REQUIRE(results.get<int64_t>(1) == 0);
                REQUIRE(results.get<int64_t>(2) == 1);
            });

            REQUIRE(results.size() == 3);
            REQUIRE(results.get<int64_t>(0) == 0);
            REQUIRE(results.get<int64_t>(1) == 1);
            REQUIRE(results.get<int64_t>(2) == 2);

            r->refresh();

            REQUIRE(results.size() == 3);
            REQUIRE(results.get<int64_t>(0) == -1);
            REQUIRE(results.get<int64_t>(1) == 0);
            REQUIRE(results.get<int64_t>(2) == 1);
        }

        SECTION("distinct int results") {
            r->begin_transaction();
            auto obj = create_object(
                r, "int array", {{"value", AnyVector{INT64_C(3), INT64_C(2), INT64_C(1), INT64_C(1), INT64_C(2)}}});
            auto col = get_table(*r, "int array")->get_column_key("value");
            List list(r, obj.obj(), col);
            r->commit_transaction();

            auto results = list.as_results().distinct({"self"}).sort({{"self", true}});

            REQUIRE(results.size() == 3);
            REQUIRE(results.get<int64_t>(0) == 1);
            REQUIRE(results.get<int64_t>(1) == 2);
            REQUIRE(results.get<int64_t>(2) == 3);

            auto ref = ThreadSafeReference(results);
            JoiningThread([ref = std::move(ref), config]() mutable {
                config.scheduler = util::Scheduler::make_frozen(VersionID());
                SharedRealm r = Realm::get_shared_realm(config);
                Results results = ref.resolve<Results>(r);

                REQUIRE(results.size() == 3);
                REQUIRE(results.get<int64_t>(0) == 1);
                REQUIRE(results.get<int64_t>(1) == 2);
                REQUIRE(results.get<int64_t>(2) == 3);

                r->begin_transaction();
                auto table = get_table(*r, "int array");
                List list(r, *table->begin(), table->get_column_key("value"));
                list.remove(1);
                list.remove(0);
                r->commit_transaction();

                REQUIRE(results.size() == 2);
                REQUIRE(results.get<int64_t>(0) == 1);
                REQUIRE(results.get<int64_t>(1) == 2);
            });

            REQUIRE(results.size() == 3);
            REQUIRE(results.get<int64_t>(0) == 1);
            REQUIRE(results.get<int64_t>(1) == 2);
            REQUIRE(results.get<int64_t>(2) == 3);

            r->refresh();

            REQUIRE(results.size() == 2);
            REQUIRE(results.get<int64_t>(0) == 1);
            REQUIRE(results.get<int64_t>(1) == 2);
        }

        SECTION("multiple types") {
            auto results = Results(r, get_table(*r, "int object")->where().equal(int_obj_col, 5));

            r->begin_transaction();
            auto num = create_object(r, "int object", {{"value", INT64_C(5)}});
            auto obj = create_object(r, "int array object", {{"value", AnyVector{}}});
            auto col = get_table(*r, "int array object")->get_column_key("value");
            List list(r, obj.obj(), col);
            r->commit_transaction();

            REQUIRE(list.size() == 0);
            REQUIRE(results.size() == 1);
            REQUIRE(results.get(0).get<int64_t>(int_obj_col) == 5);
            auto ref_num = ThreadSafeReference(num);
            auto ref_list = ThreadSafeReference(list);
            auto ref_results = ThreadSafeReference(results);
            {
                SharedRealm r2 = Realm::get_shared_realm(config);
                auto num = ref_num.resolve<Object>(r2);
                auto list = ref_list.resolve<List>(r2);
                auto results = ref_results.resolve<Results>(r2);

                REQUIRE(list.size() == 0);
                REQUIRE(results.size() == 1);
                REQUIRE(results.get(0).get<int64_t>(int_obj_col) == 5);

                r2->begin_transaction();
                num.obj().set_all(6);
                list.add(num.obj().get_key());
                r2->commit_transaction();

                REQUIRE(list.size() == 1);
                REQUIRE(list.get(0).get<int64_t>(int_obj_col) == 6);
                REQUIRE(results.size() == 0);
            }

            REQUIRE(list.size() == 0);
            REQUIRE(results.size() == 1);
            REQUIRE(results.get(0).get<int64_t>(int_obj_col) == 5);

            r->refresh();

            REQUIRE(list.size() == 1);
            REQUIRE(list.get(0).get<int64_t>(int_obj_col) == 6);
            REQUIRE(results.size() == 0);
        }
    }

    SECTION("resolve at version where handed over thing has been deleted") {
        Object obj;
        auto delete_and_resolve = [&](auto&& list) {
            auto ref = ThreadSafeReference(list);

            r->begin_transaction();
            obj.obj().remove();
            r->commit_transaction();

            return ref.resolve<typename std::remove_reference<decltype(list)>::type>(r);
        };

        SECTION("object") {
            r->begin_transaction();
            obj = create_object(r, "int object", {{"value", INT64_C(7)}});
            r->commit_transaction();

            REQUIRE(!delete_and_resolve(obj).is_valid());
        }

        SECTION("object list") {
            r->begin_transaction();
            obj = create_object(r, "int array object", {{"value", AnyVector{AnyDict{{"value", INT64_C(0)}}}}});
            auto col = get_table(*r, "int array object")->get_column_key("value");
            List list(r, obj.obj(), col);
            r->commit_transaction();

            REQUIRE(!delete_and_resolve(list).is_valid());
        }

        SECTION("int list") {
            r->begin_transaction();
            obj = create_object(r, "int array", {{"value", AnyVector{{INT64_C(1)}}}});
            auto col = get_table(*r, "int array")->get_column_key("value");
            List list(r, obj.obj(), col);
            r->commit_transaction();

            REQUIRE(!delete_and_resolve(list).is_valid());
        }

        SECTION("object results") {
            r->begin_transaction();
            obj = create_object(r, "int array object", {{"value", AnyVector{AnyDict{{"value", INT64_C(0)}}}}});
            auto col = get_table(*r, "int array object")->get_column_key("value");
            List list(r, obj.obj(), col);
            r->commit_transaction();

            auto results = delete_and_resolve(list.sort({{"value", true}}));
            REQUIRE(results.is_valid());
            REQUIRE(results.size() == 0);
        }

        SECTION("int results") {
            r->begin_transaction();
            obj = create_object(r, "int array", {{"value", AnyVector{{INT64_C(1)}}}});
            List list(r, obj.obj(), get_table(*r, "int array")->get_column_key("value"));
            r->commit_transaction();

            REQUIRE(!delete_and_resolve(list).is_valid());
        }
    }

    SECTION("resolve at version before where handed over thing was created") {
        auto create_ref = [&](auto&& fn) -> ThreadSafeReference {
            ThreadSafeReference ref;
            {
                SharedRealm r2 = Realm::get_shared_realm(config);
                r2->begin_transaction();
                auto obj = fn(r2);
                r2->commit_transaction();
                ref = obj;
            };
            return ref;
        };

        SECTION("object") {
            auto obj = create_ref([](auto& r) {
                           return create_object(r, "int object", {{"value", INT64_C(7)}});
                       }).resolve<Object>(r);
            REQUIRE(obj.is_valid());
            REQUIRE(obj.get_column_value<int64_t>("value") == 7);
        }

        SECTION("object list") {
            auto list = create_ref([](auto& r) {
                            auto obj = create_object(r, "int array object",
                                                     {{"value", AnyVector{AnyDict{{"value", INT64_C(0)}}}}});
                            return List(r, obj.obj(), get_table(*r, "int array object")->get_column_key("value"));
                        }).resolve<List>(r);
            REQUIRE(list.is_valid());
            REQUIRE(list.size() == 1);
        }

        SECTION("int list") {
            auto list = create_ref([](auto& r) {
                            auto obj = create_object(r, "int array", {{"value", AnyVector{{INT64_C(1)}}}});
                            return List(r, obj.obj(), get_table(*r, "int array")->get_column_key("value"));
                        }).resolve<List>(r);
            REQUIRE(list.is_valid());
            REQUIRE(list.size() == 1);
        }

        SECTION("object results") {
            auto results =
                create_ref([](auto& r) {
                    auto obj =
                        create_object(r, "int array object", {{"value", AnyVector{AnyDict{{"value", INT64_C(0)}}}}});
                    Results results = List(r, obj.obj(), get_table(*r, "int array object")->get_column_key("value"))
                                          .sort({{"value", true}});
                    REQUIRE(results.size() == 1);
                    return results;
                }).resolve<Results>(r);
            REQUIRE(results.is_valid());
            REQUIRE(results.size() == 1);
        }

        SECTION("int results") {
            auto results = create_ref([](auto& r) {
                               auto obj = create_object(r, "int array", {{"value", AnyVector{{INT64_C(1)}}}});
                               return List(r, obj.obj(), get_table(*r, "int array")->get_column_key("value"))
                                   .sort({{"self", true}});
                           }).resolve<Results>(r);
            REQUIRE(results.is_valid());
            REQUIRE(results.size() == 1);
        }
    }

    SECTION("create TSR inside the write transaction which created the object being handed over") {
        auto create_ref = [&](auto&& fn) -> ThreadSafeReference {
            ThreadSafeReference ref;
            {
                SharedRealm r2 = Realm::get_shared_realm(config);
                r2->begin_transaction();
                ref = fn(r2);
                r2->commit_transaction();
            };
            return ref;
        };

        SECTION("object") {
            auto obj = create_ref([](auto& r) {
                           return create_object(r, "int object", {{"value", INT64_C(7)}});
                       }).resolve<Object>(r);
            REQUIRE(obj.is_valid());
            REQUIRE(obj.get_column_value<int64_t>("value") == 7);
        }

        SECTION("object list") {
            auto list = create_ref([](auto& r) {
                            auto obj = create_object(r, "int array object",
                                                     {{"value", AnyVector{AnyDict{{"value", INT64_C(0)}}}}});
                            return List(r, obj.obj(), get_table(*r, "int array object")->get_column_key("value"));
                        }).resolve<List>(r);
            REQUIRE(list.is_valid());
            REQUIRE(list.size() == 1);
        }

        SECTION("int list") {
            auto list = create_ref([](auto& r) {
                            auto obj = create_object(r, "int array", {{"value", AnyVector{{INT64_C(1)}}}});
                            return List(r, obj.obj(), get_table(*r, "int array")->get_column_key("value"));
                        }).resolve<List>(r);
            REQUIRE(list.is_valid());
            REQUIRE(list.size() == 1);
        }

        SECTION("object results") {
            REQUIRE_THROWS(create_ref([](auto& r) {
                auto obj =
                    create_object(r, "int array object", {{"value", AnyVector{AnyDict{{"value", INT64_C(0)}}}}});
                Results results = List(r, obj.obj(), get_table(*r, "int array object")->get_column_key("value"))
                                      .sort({{"value", true}});
                REQUIRE(results.size() == 1);
                return results;
            }));
        }

        SECTION("int results") {
            auto results = create_ref([](auto& r) {
                               auto obj = create_object(r, "int array", {{"value", AnyVector{{INT64_C(1)}}}});
                               return List(r, obj.obj(), get_table(*r, "int array")->get_column_key("value"))
                                   .sort({{"self", true}});
                           }).resolve<Results>(r);
            REQUIRE(results.is_valid());
            REQUIRE(results.size() == 1);
        }
    }

    SECTION("create TSR to pre-existing objects inside write transaction") {
        auto create_ref = [&](auto&& fn) -> ThreadSafeReference {
            ThreadSafeReference ref;
            {
                SharedRealm r2 = Realm::get_shared_realm(config);
                r2->begin_transaction();
                auto obj = fn(r2);
                r2->commit_transaction();
                r2->begin_transaction();
                ref = obj;
                r2->commit_transaction();
            };
            return ref;
        };

        SECTION("object") {
            auto obj = create_ref([](auto& r) {
                           return create_object(r, "int object", {{"value", INT64_C(7)}});
                       }).resolve<Object>(r);
            REQUIRE(obj.is_valid());
            REQUIRE(obj.get_column_value<int64_t>("value") == 7);
        }

        SECTION("object list") {
            auto list = create_ref([](auto& r) {
                            auto obj = create_object(r, "int array object",
                                                     {{"value", AnyVector{AnyDict{{"value", INT64_C(0)}}}}});
                            return List(r, obj.obj(), get_table(*r, "int array object")->get_column_key("value"));
                        }).resolve<List>(r);
            REQUIRE(list.is_valid());
            REQUIRE(list.size() == 1);
        }

        SECTION("int list") {
            auto list = create_ref([](auto& r) {
                            auto obj = create_object(r, "int array", {{"value", AnyVector{{INT64_C(1)}}}});
                            return List(r, obj.obj(), get_table(*r, "int array")->get_column_key("value"));
                        }).resolve<List>(r);
            REQUIRE(list.is_valid());
            REQUIRE(list.size() == 1);
        }

        SECTION("object results") {
            auto results =
                create_ref([](auto& r) {
                    auto obj =
                        create_object(r, "int array object", {{"value", AnyVector{AnyDict{{"value", INT64_C(0)}}}}});
                    Results results = List(r, obj.obj(), get_table(*r, "int array object")->get_column_key("value"))
                                          .sort({{"value", true}});
                    REQUIRE(results.size() == 1);
                    return results;
                }).resolve<Results>(r);
            REQUIRE(results.is_valid());
            REQUIRE(results.size() == 1);
        }

        SECTION("int results") {
            auto results = create_ref([](auto& r) {
                               auto obj = create_object(r, "int array", {{"value", AnyVector{{INT64_C(1)}}}});
                               return List(r, obj.obj(), get_table(*r, "int array")->get_column_key("value"))
                                   .sort({{"self", true}});
                           }).resolve<Results>(r);
            REQUIRE(results.is_valid());
            REQUIRE(results.size() == 1);
        }
    }

    SECTION("create TSR inside cancelled write transaction") {
        auto create_ref = [&](auto&& fn) -> ThreadSafeReference {
            ThreadSafeReference ref;
            {
                SharedRealm r2 = Realm::get_shared_realm(config);
                r2->begin_transaction();
                ref = fn(r2);
                r2->cancel_transaction();
            };
            return ref;
        };

        SECTION("object") {
            auto obj = create_ref([](auto& r) {
                           return create_object(r, "int object", {{"value", INT64_C(7)}});
                       }).resolve<Object>(r);
            REQUIRE_FALSE(obj.is_valid());
        }

        SECTION("object list") {
            auto list = create_ref([](auto& r) {
                            auto obj = create_object(r, "int array object",
                                                     {{"value", AnyVector{AnyDict{{"value", INT64_C(0)}}}}});
                            return List(r, obj.obj(), get_table(*r, "int array object")->get_column_key("value"));
                        }).resolve<List>(r);
            REQUIRE_FALSE(list.is_valid());
        }

        SECTION("int list") {
            auto list = create_ref([](auto& r) {
                            auto obj = create_object(r, "int array", {{"value", AnyVector{{INT64_C(1)}}}});
                            return List(r, obj.obj(), get_table(*r, "int array")->get_column_key("value"));
                        }).resolve<List>(r);
            REQUIRE_FALSE(list.is_valid());
        }

        SECTION("object results") {
            REQUIRE_THROWS(create_ref([](auto& r) {
                auto obj =
                    create_object(r, "int array object", {{"value", AnyVector{AnyDict{{"value", INT64_C(0)}}}}});
                Results results = List(r, obj.obj(), get_table(*r, "int array object")->get_column_key("value"))
                                      .sort({{"value", true}});
                REQUIRE(results.size() == 1);
                return results;
            }));
        }

        SECTION("int results") {
            auto results = create_ref([](auto& r) {
                               auto obj = create_object(r, "int array", {{"value", AnyVector{{INT64_C(1)}}}});
                               return List(r, obj.obj(), get_table(*r, "int array")->get_column_key("value"))
                                   .sort({{"self", true}});
                           }).resolve<Results>(r);
            REQUIRE_FALSE(results.is_valid());
        }
    }

    SECTION("allow multiple resolves") {
        r->begin_transaction();
        auto int_obj = create_object(r, "int object", {{"value", INT64_C(7)}});
        r->commit_transaction();
        auto ref = ThreadSafeReference(int_obj);
        REQUIRE_NOTHROW(ref.resolve<Object>(r));
        REQUIRE_NOTHROW(ref.resolve<Object>(r));
    }

    SECTION("resolve after source version has been cleaned up") {
        auto create_ref = [&](auto&& fn) -> ThreadSafeReference {
            ThreadSafeReference ref;
            {
                SharedRealm r2 = Realm::get_shared_realm(config);
                r2->begin_transaction();
                auto obj = fn(r2);
                r2->commit_transaction();
                ref = obj;

                // Perform additional writes until the TSR's source version
                // no longer exists
                auto history = make_in_realm_history();
                auto db = DB::create(*history, config.path, config.options());
                auto tsr_version = db->get_version_id_of_latest_snapshot();
                while (true) {
                    REQUIRE(db->get_number_of_versions() < 5);
                    r2->begin_transaction();
                    r2->commit_transaction();
                    try {
                        db->start_read(tsr_version);
                    }
                    catch (const DB::BadVersion&) {
                        break;
                    }
                }
            };
            return ref;
        };

        r->invalidate();

        SECTION("object") {
            auto ref = create_ref([](auto& r) {
                return create_object(r, "int object", {{"value", INT64_C(7)}});
            });

            SECTION("target Realm already in a read transaction") {
                r->read_group();
            }
            SECTION("target Realm not in a read transaction") {
            }

            REQUIRE(ref.resolve<Object>(r).obj().get<Int>("value") == 7);
        }

        SECTION("object list") {
            auto ref = create_ref([](auto& r) {
                auto obj =
                    create_object(r, "int array object", {{"value", AnyVector{AnyDict{{"value", INT64_C(0)}}}}});
                auto col = get_table(*r, "int array object")->get_column_key("value");
                return List(r, obj.obj(), col);
            });

            SECTION("target Realm already in a read transaction") {
                r->read_group();
            }
            SECTION("target Realm not in a read transaction") {
            }

            REQUIRE(ref.resolve<List>(r).size() == 1);
        }

        SECTION("int list") {
            auto ref = create_ref([](auto& r) {
                auto obj = create_object(r, "int array", {{"value", AnyVector{{INT64_C(1)}}}});
                auto col = get_table(*r, "int array")->get_column_key("value");
                return List(r, obj.obj(), col);
            });

            SECTION("target Realm already in a read transaction") {
                r->read_group();
            }
            SECTION("target Realm not in a read transaction") {
            }

            REQUIRE(ref.resolve<List>(r).size() == 1);
        }

        // no object results because that pins the source version so this
        // test isn't applicable (and would be an infintie loop)
    }
}
