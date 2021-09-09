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

namespace realm {

TableRef get_table(Realm& realm, StringData object_type)
{
    return ObjectStore::table_for_object_type(realm.read_group(), object_type);
}

TEST_CASE("client reset seamless loss", "[benchmark]") {
    const std::string valid_pk_name = "_id";
    const std::string partition_value = "partition_foo";
    Property partition_prop = {"realm_id", PropertyType::String | PropertyType::Nullable};
    Schema schema = {
        {"source", {{valid_pk_name, PropertyType::Int | PropertyType::Nullable, true}, partition_prop}},
        {"dest",
         {
             {valid_pk_name, PropertyType::Int | PropertyType::Nullable, true},
             partition_prop,
         }},
        {"object",
         {
             {valid_pk_name, PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
             {"value_string", PropertyType::String | PropertyType::Nullable},
             partition_prop,
         }},
    };

    TestSyncManager init_sync_manager;
    SyncTestFile config(init_sync_manager.app(), "default");
    config.cache = false;
    config.automatic_change_notifications = false;
    config.schema = schema;
    config.sync_config->client_resync_mode = ClientResyncMode::SeamlessLoss;

    SyncTestFile config2(init_sync_manager.app(), "default");

    std::unique_ptr<reset_utils::TestClientReset> test_reset =
        reset_utils::make_fake_local_client_reset(config, config2);

    SECTION("empty") {
        BENCHMARK("no changes") {
            test_reset->run();
        };
    }

    SECTION("populated without links") {
        constexpr size_t num_objects = 10000;

        test_reset->setup([&](SharedRealm realm) {
            auto table = get_table(*realm, "object");
            ColKey partition_col_key = table->get_column_key(partition_prop.name);
            ColKey value_col_key = table->get_column_key("value");
            ColKey value_str_col_key = table->get_column_key("value_string");
            REQUIRE(table);
            int64_t pk = 1; // TestClientReset creates an object with pk 0 so start with something else
            for (size_t i = 0; i < num_objects; ++i) {
                FieldValues values = {
                    {partition_col_key, partition_value},
                    {value_col_key, int64_t(i)},
                    {value_str_col_key, util::format("string_value_%1", i)},
                };
                table->create_object_with_primary_key(pk++, std::move(values));
            }
        });

        BENCHMARK("no changes") {
            test_reset->run();
        };
    }
}

} // namespace realm
