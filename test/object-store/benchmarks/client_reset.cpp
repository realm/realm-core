////////////////////////////////////////////////////////////////////////////
//
// Copyright 2021 Realm Inc.
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

#define CATCH_CONFIG_ENABLE_BENCHMARKING

#include "util/test_file.hpp"
#include "util/test_utils.hpp"

#include "sync/sync_test_utils.hpp"

#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/sync/noinst/client_reset.hpp>

namespace realm {

TableRef get_table(Realm& realm, StringData object_type)
{
    return ObjectStore::table_for_object_type(realm.read_group(), object_type);
}

Obj create_object(Realm& realm, StringData object_type, util::Optional<int64_t> primary_key = util::none)
{
    auto table = get_table(realm, object_type);
    REQUIRE(table);
    static int64_t pk = 0;
    FieldValues values = {};
    return table->create_object_with_primary_key(primary_key ? *primary_key : pk++, std::move(values));
}

struct BenchmarkLocalClientReset : public reset_utils::TestClientReset {
    BenchmarkLocalClientReset(realm::Realm::Config local_config, realm::Realm::Config remote_config)
        : reset_utils::TestClientReset(local_config, remote_config)
    {
        REALM_ASSERT(m_local_config.sync_config);
        REALM_ASSERT(m_local_config.sync_config->client_resync_mode == ClientResyncMode::DiscardLocal);
        // turn off sync, we only fake it
        m_local_config.sync_config = {};
        m_remote_config.sync_config = {};
    }

    void prepare()
    {
        m_local = Realm::get_shared_realm(m_local_config);
        m_local->begin_transaction();
        if (m_on_setup) {
            m_on_setup(m_local);
        }
        m_local->commit_transaction();
        constexpr int64_t shared_pk = -42;
        {
            m_local->begin_transaction();
            auto obj = create_object(*m_local, "object", shared_pk);
            auto col = obj.get_table()->get_column_key("value");
            obj.set(col, 1);
            obj.set(col, 2);
            obj.set(col, 3);
            m_local->commit_transaction();

            m_local->begin_transaction();
            obj.set(col, 4);
            if (m_make_local_changes) {
                m_make_local_changes(m_local);
            }
            m_local->commit_transaction();
            if (m_on_post_local) {
                m_on_post_local(m_local);
            }
        }

        m_remote_config.schema = m_local_config.schema;
        m_remote = Realm::get_shared_realm(m_remote_config);
        m_remote->begin_transaction();
        if (m_on_setup) {
            m_on_setup(m_remote);
        }

        // fake a sync by creating an object with the same pk
        create_object(*m_remote, "object", shared_pk);

        for (int i = 0; i < 2; ++i) {
            auto table = get_table(*m_remote, "object");
            auto col = table->get_column_key("value");
            table->begin()->set(col, i + 5);
        }

        if (m_make_remote_changes) {
            m_make_remote_changes(m_remote);
        }
        m_remote->commit_transaction();
        m_did_setup = true;
    }

    // as a Catch2 Benchmark must be repeatable, the following does the reset without committing anything
    void run() override
    {
        m_did_run = true;
        REALM_ASSERT(m_did_setup);
        m_local->begin_transaction();
        m_remote->begin_transaction();

        TestLogger logger;
        _impl::client_reset::transfer_group((Transaction&)m_remote->read_group(), (Transaction&)m_local->read_group(),
                                            logger);
        if (m_on_post_reset) {
            m_on_post_reset(m_local);
        }
        m_local->cancel_transaction();
        m_remote->cancel_transaction();
    }
    bool m_did_setup = false;
    SharedRealm m_local;
    SharedRealm m_remote;
};

TEST_CASE("client reset: discard local", "[benchmark]") {
    const std::string valid_pk_name = "_id";
    const std::string partition_value = "partition_foo";
    Property partition_prop = {"realm_id", PropertyType::String | PropertyType::Nullable};
    Schema schema = {
        {"source",
         {{valid_pk_name, PropertyType::Int | PropertyType::Nullable, true},
          {"link", PropertyType::Object | PropertyType::Nullable, "object"},
          {"link_list", PropertyType::Array | PropertyType::Object, "object"},
          {"mixed", PropertyType::Mixed | PropertyType::Nullable},
          {"mixed_list", PropertyType::Array | PropertyType::Mixed | PropertyType::Nullable},
          partition_prop}},
        {"empty table",
         {
             {valid_pk_name, PropertyType::Int | PropertyType::Nullable, true},
             partition_prop,
         }},
        {"object",
         {
             {valid_pk_name, PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
             {"value_double", PropertyType::Double},
             {"value_float", PropertyType::Float},
             {"value_decimal", PropertyType::Decimal},
             {"value_bool", PropertyType::Bool},
             {"value_mixed", PropertyType::Mixed | PropertyType::Nullable},
             {"value_string", PropertyType::String | PropertyType::Nullable},
             {"mixed_list", PropertyType::Array | PropertyType::Mixed | PropertyType::Nullable},
             partition_prop,
         }},
    };

    TestSyncManager init_sync_manager;
    SyncTestFile config(init_sync_manager.app(), "default");
    config.cache = false;
    config.automatic_change_notifications = false;
    config.schema = schema;
    config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;

    SyncTestFile config2(init_sync_manager.app(), "default");

    BenchmarkLocalClientReset test_reset(config, config2);

    SECTION("empty") {
        test_reset.prepare();
        BENCHMARK("no changes") {
            test_reset.run();
        };
    }

    auto populate_objects = [&](SharedRealm realm, size_t num_objects) {
        TableRef table = get_table(*realm, "object");
        REQUIRE(table);
        ColKey partition_col_key = table->get_column_key(partition_prop.name);
        ColKey value_col_key = table->get_column_key("value");
        ColKey value_str_col_key = table->get_column_key("value_string");
        ColKey double_col_key = table->get_column_key("value_double");
        ColKey float_col_key = table->get_column_key("value_float");
        ColKey decimal_col_key = table->get_column_key("value_decimal");
        ColKey bool_col_key = table->get_column_key("value_bool");
        ColKey mixed_col_key = table->get_column_key("value_mixed");
        ColKey mixed_list_col_key = table->get_column_key("mixed_list");
        int64_t pk = 1; // TestClientReset creates an object with pk 0 so start with something else
        for (size_t i = 0; i < num_objects; ++i) {
            FieldValues values = {
                {partition_col_key, partition_value},
                {value_col_key, int64_t(i)},
                {value_str_col_key, util::format("string_value_%1", i)},
                {double_col_key, double(i) + 0.5},
                {float_col_key, float(i) + .333f},
                {decimal_col_key, Decimal128{int64_t(i)}},
                {bool_col_key, i % 2 == 0},
                {mixed_col_key, Mixed{int64_t(i)}},
            };
            auto obj = table->create_object_with_primary_key(pk++, std::move(values));
            auto mixed_list = obj.get_list<Mixed>(mixed_list_col_key);
            mixed_list.add(int64_t(i));
            mixed_list.add(util::format("mixed_list_value_%1", i));
            mixed_list.add(float(i));
        }
    };

    auto populate_source_objects_with_links = [&](SharedRealm realm) {
        TableRef table = get_table(*realm, "source");
        TableRef dest = get_table(*realm, "object");
        REQUIRE(table);
        REQUIRE(dest);
        ColKey partition_col_key = table->get_column_key(partition_prop.name);
        ColKey link_col_key = table->get_column_key("link");
        ColKey mixed_col_key = table->get_column_key("mixed");
        ColKey link_list_col_key = table->get_column_key("link_list");
        ColKey mixed_list_col_key = table->get_column_key("mixed_list");
        int64_t pk = 1;
        for (auto it = dest->begin(); it != dest->end(); ++it) {
            Mixed mixed_link = ObjLink{dest->get_key(), it->get_key()};
            FieldValues values = {
                {partition_col_key, partition_value},
                {link_col_key, it->get_key()},
                {mixed_col_key, mixed_link},
            };
            auto obj = table->create_object_with_primary_key(pk++, std::move(values));
            auto lnk_list = obj.get_linklist(link_list_col_key);
            lnk_list.add(it->get_key());
            lnk_list.add(it->get_key());
            lnk_list.add(it->get_key());
            auto mixed_list = obj.get_list<Mixed>(mixed_list_col_key);
            mixed_list.add(mixed_link);
            mixed_list.add(mixed_link);
            mixed_list.add(mixed_link);
        }
    };

    auto remove_odd_objects = [](TableRef table) {
        bool do_remove = false;
        for (auto it = table->begin(); it != table->end(); ++it) {
            if (do_remove) {
                it->remove();
            }
            do_remove = !do_remove;
        }
    };


    constexpr size_t num_objects = 10000;
    SECTION(util::format("populated with %1 simple objects", num_objects)) {
        test_reset.setup([&](SharedRealm realm) {
            populate_objects(realm, num_objects);
        });

        SECTION("no change") {
            test_reset.prepare();
            BENCHMARK("reset with no changes") {
                test_reset.run();
            };
        }
        SECTION("remote removes half the local data") {
            test_reset.make_remote_changes([&](SharedRealm remote) {
                remove_odd_objects(get_table(*remote, "object"));
            });
            test_reset.prepare();
            BENCHMARK("reset will remove half the local data") {
                test_reset.run();
            };
        }
        SECTION("remote will double the amount of local data ") {
            test_reset.make_local_changes([&](SharedRealm local) {
                remove_odd_objects(get_table(*local, "object"));
            });
            test_reset.prepare();
            BENCHMARK("reset will double the amount of local data") {
                test_reset.run();
            };
        }
    }

    SECTION(util::format("with %1 source objects linked to %1 dest objects", num_objects / 2)) {
        test_reset.setup([&](SharedRealm realm) {
            populate_objects(realm, num_objects / 2);
            populate_source_objects_with_links(realm);
        });

        SECTION("no change") {
            test_reset.prepare();
            BENCHMARK("reset with no changes") {
                test_reset.run();
            };
        }
        SECTION("remote removes half the local data") {
            test_reset.make_remote_changes([&](SharedRealm remote) {
                remove_odd_objects(get_table(*remote, "object"));
                remove_odd_objects(get_table(*remote, "source"));
            });
            test_reset.prepare();
            BENCHMARK("reset will remove half the local data") {
                test_reset.run();
            };
        }
        SECTION("remote will double the amount of local data ") {
            test_reset.make_local_changes([&](SharedRealm local) {
                remove_odd_objects(get_table(*local, "object"));
                remove_odd_objects(get_table(*local, "source"));
            });
            test_reset.prepare();
            BENCHMARK("reset will double the amount of local data") {
                test_reset.run();
            };
        }
    }
}

} // namespace realm
