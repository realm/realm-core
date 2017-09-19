////////////////////////////////////////////////////////////////////////////
//
// Copyright 20167 Realm Inc.
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

#include "list.hpp"
#include "shared_realm.hpp"
#include "object_schema.hpp"
#include "object_store.hpp"
#include "schema.hpp"

#include "sync/sync_config.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_session.hpp"

#include "util/event_loop.hpp"
#include "util/test_file.hpp"
#include <realm/util/optional.hpp>

using namespace realm;

using TypeATuple = std::tuple<size_t, size_t, std::string>;

namespace {

Schema partial_sync_schema()
{
	return Schema{
        {"partial_sync_object_a", {
            {"first_number", PropertyType::Int},
            {"second_number", PropertyType::Int},
            {"string", PropertyType::String}
        }},
        {"partial_sync_object_b", {
            {"number", PropertyType::Int},
            {"first_string", PropertyType::String},
            {"second_string", PropertyType::String},
        }}
    };
}

void populate_realm_with_type_a_objects(Realm::Config& config, std::vector<TypeATuple> values)
{	
	auto r = Realm::get_shared_realm(config);
	const auto& object_schema = *r->schema().find("partial_sync_object_a");
    const auto& first_number_prop = *object_schema.property_for_name("first_number");
    const auto& second_number_prop = *object_schema.property_for_name("second_number");
    const auto& string_prop = *object_schema.property_for_name("string");
    TableRef table = ObjectStore::table_for_object_type(r->read_group(), "partial_sync_object_a");
	r->begin_transaction();
	for (size_t i = 0; i < values.size(); ++i) {
		auto current = values[i];
#if REALM_HAVE_SYNC_STABLE_IDS
        size_t row_idx = sync::create_object(r->read_group(), *table);
#else
        size_t row_idx = table->add_empty_row();
#endif // REALM_HAVE_SYNC_STABLE_IDS
        table->set_int(first_number_prop.table_column, row_idx, std::get<0>(current));
        table->set_int(second_number_prop.table_column, row_idx, std::get<1>(current));
        table->set_string(string_prop.table_column, row_idx, std::get<2>(current));
    }
	r->commit_transaction();
}

}

TEST_CASE("Partial sync", "[sync-foobar]") {
	if (!EventLoop::has_implementation())
        return;

	SyncServer server;
	SyncTestFile config(server, "test", partial_sync_schema());
	SyncTestFile partial_config(server, "test/__partial/123456", partial_sync_schema(), true);

	SECTION("works for the basic case") {
		// Add objects to the Realm
        {
            populate_realm_with_type_a_objects(config,
                {{1, 0, "realm"}, {2, 0, "partial"}, {3, 0, "sync"}});
            std::atomic<bool> upload_done(false);
            auto r = Realm::get_shared_realm(config);
            auto session = SyncManager::shared().get_existing_active_session(config.path);
            session->wait_for_download_completion([&](auto) { upload_done = true; });
            EventLoop::main().run_until([&] { return upload_done.load(); });
        }
		
        // Open the partially synced Realm and run a query.
        std::atomic<bool> partial_sync_done(false);
        auto r = Realm::get_shared_realm(partial_config);
        util::Optional<List> results;
        r->register_partial_sync_query(
            "partial_sync_object_a",
            "first_number > 1", 
            [&](List list, std::exception_ptr) {
                partial_sync_done = true;
                results = list;
        });
        EventLoop::main().run_until([&] { return partial_sync_done.load(); });
        REQUIRE(bool(results));
        CHECK(results->size() == 2);
	}
}
