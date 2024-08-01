#include <realm/exceptions.hpp>
#include <realm/object_id.hpp>
#include <realm/transaction.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/sync_metadata_schema.hpp>
#include <realm/sync/subscriptions.hpp>

#include "test.hpp"
#include "util/test_path.hpp"

#include <filesystem>

namespace realm::sync {

struct SubscriptionStoreFixture {
    SubscriptionStoreFixture(const test_util::DBTestPathGuard& path)
        : db(DB::create(make_client_replication(), path))
    {
        auto write = db->start_write();
        auto a_table = write->get_or_add_table_with_primary_key("class_a", type_Int, "_id");
        a_table_key = a_table->get_key();
        if (foo_col = a_table->get_column_key("foo"); !foo_col) {
            foo_col = a_table->add_column(type_String, "foo");
        }
        if (bar_col = a_table->get_column_key("bar"); !bar_col) {
            bar_col = a_table->add_column(type_Int, "bar");
        }
        write->commit();
    }

    DBRef db;
    TableKey a_table_key;
    ColKey foo_col;
    ColKey bar_col;
};

TEST(Sync_SubscriptionStoreBasic)
{
    ObjectId anon_sub_id;
    SHARED_GROUP_TEST_PATH(sub_store_path);
    {
        SubscriptionStoreFixture fixture(sub_store_path);
        auto store = SubscriptionStore::create(fixture.db);
        // Because there are no subscription sets yet, get_latest should point to an empty object
        auto latest = store->get_latest();
        CHECK(latest.begin() == latest.end());
        CHECK_EQUAL(latest.size(), 0);
        CHECK(latest.find("a sub") == nullptr);
        CHECK_EQUAL(latest.version(), 0);
        CHECK(latest.error_str().is_null());
        // The "0" query is "Pending" from beginning since it gets created in the initial constructor
        // of SubscriptionStore
        CHECK_EQUAL(latest.state(), SubscriptionSet::State::Pending);

        // By making a mutable copy of `latest` we should create an actual object that we can modify.
        auto out = latest.make_mutable_copy();
        CHECK_EQUAL(out.state(), SubscriptionSet::State::Uncommitted);
        CHECK(out.error_str().is_null());
        CHECK_EQUAL(out.version(), 1);
        auto read_tr = fixture.db->start_read();
        Query query_a(read_tr->get_table("class_a"));
        query_a.equal(fixture.foo_col, StringData("JBR")).greater_equal(fixture.bar_col, int64_t(1));
        auto&& [it, inserted] = out.insert_or_assign("a sub", query_a);
        CHECK(inserted);
        CHECK_NOT(it == out.end());
        CHECK_EQUAL(it->name, "a sub");
        CHECK_EQUAL(it->object_class_name, "a");
        CHECK_EQUAL(it->query_string, query_a.get_description());

        std::tie(it, inserted) =
            out.insert_or_assign(Query(read_tr->get_table(fixture.a_table_key)).equal(fixture.foo_col, "bizz"));
        CHECK_NOT(it == out.end());
        CHECK(inserted);

        CHECK_EQUAL(it->name, util::Optional<std::string>());
        StringData name(it->name);
        CHECK(name.is_null());
        anon_sub_id = it->id;

        out.commit();
    }

    // Destroy the DB and reload it and make sure we can get the subscriptions we set in the previous block.
    {
        SubscriptionStoreFixture fixture(sub_store_path);
        auto store = SubscriptionStore::create(fixture.db);

        auto read_tr = fixture.db->start_read();
        Query query_a(read_tr->get_table(fixture.a_table_key));
        query_a.equal(fixture.foo_col, StringData("JBR")).greater_equal(fixture.bar_col, int64_t(1));

        auto set = store->get_latest();
        CHECK_EQUAL(set.version(), 1);
        CHECK_EQUAL(set.size(), 2);
        auto ptr = set.find(query_a);
        CHECK(ptr);
        CHECK_EQUAL(ptr->name, "a sub");
        CHECK_EQUAL(ptr->object_class_name, "a");
        CHECK_EQUAL(ptr->query_string, query_a.get_description());

        // Make sure we can't get a subscription set that doesn't exist.
        CHECK(set.find("b subs") == nullptr);

        auto anon_sub_it = std::find_if(set.begin(), set.end(), [&](const Subscription& sub) {
            return sub.id == anon_sub_id;
        });
        CHECK_NOT(anon_sub_it == set.end());
        CHECK_EQUAL(anon_sub_it->name, util::Optional<std::string>());
    }
}

TEST(Sync_SubscriptionStoreStateUpdates)
{
    SHARED_GROUP_TEST_PATH(sub_store_path);
    SubscriptionStoreFixture fixture(sub_store_path);
    auto store = SubscriptionStore::create(fixture.db);

    auto read_tr = fixture.db->start_read();
    Query query_a(read_tr->get_table("class_a"));
    query_a.equal(fixture.foo_col, StringData("JBR")).greater_equal(fixture.bar_col, int64_t(1));
    Query query_b(read_tr->get_table(fixture.a_table_key));
    query_b.equal(fixture.foo_col, "Realm");

    // Create a new subscription set, insert a subscription into it, and mark it as complete.
    {
        auto out = store->get_latest().make_mutable_copy();
        auto&& [it, inserted] = out.insert_or_assign("a sub", query_a);
        CHECK(inserted);
        CHECK_NOT(it == out.end());

        out.set_state(SubscriptionSet::State::Complete);
        out.commit();
    }

    // Clone the completed set and update it to have a new query.
    {
        auto new_set = store->get_latest().make_mutable_copy();
        auto new_set_copy = new_set;
        CHECK_EQUAL(new_set.version(), 2);
        new_set.clear();
        new_set.insert_or_assign("b sub", query_b);
        new_set.commit();

        // Mutating a MutableSubscriptionSet that's already been committed should throw a LogicError
        CHECK_THROW(new_set_copy.clear(), LogicError);
        CHECK_THROW(new_set_copy.erase(new_set_copy.begin()), LogicError);
        CHECK_THROW(new_set_copy.insert_or_assign(query_b), LogicError);
    }

    // There should now be two subscription sets, version 1 is complete with query a and version 2 is pending with
    // query b.
    {
        auto active = store->get_active();
        auto latest = store->get_latest();
        CHECK_NOT_EQUAL(active.version(), latest.version());
        CHECK_EQUAL(active.state(), SubscriptionSet::State::Complete);
        CHECK_EQUAL(latest.state(), SubscriptionSet::State::Pending);

        auto it_a = active.begin();
        CHECK_EQUAL(it_a->query_string, query_a.get_description());
        CHECK_EQUAL(it_a->name, "a sub");
        auto it_b = latest.begin();
        CHECK_EQUAL(it_b->name, "b sub");
        CHECK_EQUAL(it_b->query_string, query_b.get_description());
    }

    // Mark the version 2 set as complete.
    {
        auto tr = fixture.db->start_write();
        store->begin_bootstrap(*tr, 2);
        store->complete_bootstrap(*tr, 2);
        tr->commit();
        store->download_complete();
    }

    // There should now only be one set, version 2, that is complete. Trying to
    // get version 1 should report that it was superseded
    {
        auto active = store->get_active();
        auto latest = store->get_latest();
        CHECK(active.version() == latest.version());
        CHECK(active.state() == SubscriptionSet::State::Complete);

        // By marking version 2 as complete version 1 will get superseded and removed.
        CHECK_EQUAL(store->get_by_version(1).state(), SubscriptionSet::State::Superseded);
    }

    // Trying to begin a bootstrap on the superseded version should report that
    // the server did something invalid
    {
        auto tr = fixture.db->start_write();
        CHECK_THROW_ERROR(store->begin_bootstrap(*tr, 1), SyncProtocolInvariantFailed, "nonexistent query version 1");
        CHECK_THROW_ERROR(store->begin_bootstrap(*tr, 4), SyncProtocolInvariantFailed, "nonexistent query version 4");
    }
    CHECK_THROW_ERROR(store->set_error(2, "err1"), SyncProtocolInvariantFailed,
                      "Received error 'err1' for already-completed query version 2");
    CHECK_THROW_ERROR(store->set_error(4, "err2"), SyncProtocolInvariantFailed,
                      "Invalid state update for nonexistent query version 4");

    {
        auto set = store->get_latest().make_mutable_copy();
        CHECK_EQUAL(set.size(), 1);
        // This is just to create a unique name for this sub so we can verify that the iterator returned by
        // insert_or_assign is pointing to the subscription that was just created.
        std::string new_sub_name = ObjectId::gen().to_string();
        auto&& [inserted_it, inserted] = set.insert_or_assign(new_sub_name, query_a);
        CHECK(inserted);
        CHECK_EQUAL(inserted_it->name, new_sub_name);
        CHECK_EQUAL(set.size(), 2);
        auto it = set.begin();
        CHECK_EQUAL(it->name, "b sub");
        it = set.erase(it);
        CHECK_NOT(it == set.end());
        CHECK_EQUAL(set.size(), 1);
        CHECK_EQUAL(it->name, new_sub_name);
        it = set.erase(it);
        CHECK(it == set.end());
        CHECK_EQUAL(set.size(), 0);
    }
}

TEST(Sync_SubscriptionStoreUpdateExisting)
{
    SHARED_GROUP_TEST_PATH(sub_store_path);
    SubscriptionStoreFixture fixture(sub_store_path);
    auto store = SubscriptionStore::create(fixture.db);

    auto read_tr = fixture.db->start_read();
    Query query_a(read_tr->get_table("class_a"));
    query_a.equal(fixture.foo_col, StringData("JBR")).greater_equal(fixture.bar_col, int64_t(1));
    Query query_b(read_tr->get_table(fixture.a_table_key));
    query_b.equal(fixture.foo_col, "Realm");
    ObjectId id_of_inserted;
    auto sub_name = ObjectId::gen().to_string();
    {
        auto out = store->get_latest().make_mutable_copy();
        auto [it, inserted] = out.insert_or_assign(sub_name, query_a);
        CHECK(inserted);
        CHECK_NOT(it == out.end());
        id_of_inserted = it->id;
        CHECK_NOT_EQUAL(id_of_inserted, ObjectId{});

        std::tie(it, inserted) = out.insert_or_assign(sub_name, query_b);
        CHECK(!inserted);
        CHECK_NOT(it == out.end());
        CHECK_EQUAL(it->object_class_name, "a");
        CHECK_EQUAL(it->query_string, query_b.get_description());
        CHECK_EQUAL(it->id, id_of_inserted);
        out.commit();
    }
    {
        auto set = store->get_latest().make_mutable_copy();
        CHECK_EQUAL(set.size(), 1);
        auto it = std::find_if(set.begin(), set.end(), [&](const Subscription& sub) {
            return sub.id == id_of_inserted;
        });
        CHECK_NOT(it == set.end());
        CHECK_EQUAL(it->name, sub_name);
    }
}

TEST(Sync_SubscriptionStoreAssignAnonAndNamed)
{
    SHARED_GROUP_TEST_PATH(sub_store_path);
    SubscriptionStoreFixture fixture(sub_store_path);
    auto store = SubscriptionStore::create(fixture.db);

    auto read_tr = fixture.db->start_read();
    Query query_a(read_tr->get_table("class_a"));
    query_a.equal(fixture.foo_col, StringData("JBR")).greater_equal(fixture.bar_col, int64_t(1));
    Query query_b(read_tr->get_table(fixture.a_table_key));
    query_b.equal(fixture.foo_col, "Realm");

    {
        auto out = store->get_latest().make_mutable_copy();
        auto [it, inserted] = out.insert_or_assign("a sub", query_a);
        CHECK(inserted);
        auto named_id = it->id;

        std::tie(it, inserted) = out.insert_or_assign(query_a);
        CHECK(inserted);
        CHECK_NOT_EQUAL(it->id, named_id);
        CHECK_EQUAL(out.size(), 2);

        std::tie(it, inserted) = out.insert_or_assign(query_b);
        CHECK(inserted);
        named_id = it->id;

        std::tie(it, inserted) = out.insert_or_assign("", query_b);
        CHECK(inserted);
        CHECK(it->name);
        CHECK_EQUAL(it->name, "");
        CHECK_NOT_EQUAL(it->id, named_id);
        CHECK_EQUAL(out.size(), 4);
    }
}

TEST(Sync_SubscriptionStoreNotifications_Basics)
{
    SHARED_GROUP_TEST_PATH(sub_store_path);
    SubscriptionStoreFixture fixture(sub_store_path);
    auto store = SubscriptionStore::create(fixture.db);

    auto sub_set = store->get_latest().make_mutable_copy();
    auto v1_pending = sub_set.get_state_change_notification(SubscriptionSet::State::Pending);
    auto v1_bootstrapping = sub_set.get_state_change_notification(SubscriptionSet::State::Bootstrapping);
    auto v1_awaiting_mark = sub_set.get_state_change_notification(SubscriptionSet::State::AwaitingMark);
    auto v1_complete = sub_set.get_state_change_notification(SubscriptionSet::State::Complete);
    auto v1_error = sub_set.get_state_change_notification(SubscriptionSet::State::Error);
    auto v1_superseded = sub_set.get_state_change_notification(SubscriptionSet::State::Superseded);
    sub_set = sub_set.commit().make_mutable_copy();
    auto v2_pending = sub_set.get_state_change_notification(SubscriptionSet::State::Pending);
    auto v2_bootstrapping = sub_set.get_state_change_notification(SubscriptionSet::State::Bootstrapping);
    auto v2_complete = sub_set.get_state_change_notification(SubscriptionSet::State::Complete);

    // This should complete immediately because transitioning to the Pending state happens when you commit.
    CHECK(v1_pending.is_ready());
    CHECK_EQUAL(v1_pending.get(), SubscriptionSet::State::Pending);

    // This should also return immediately with a ready future because the subset is in the correct state.
    CHECK_EQUAL(store->get_by_version(1).get_state_change_notification(SubscriptionSet::State::Pending).get(),
                SubscriptionSet::State::Pending);

    // v2 hasn't been committed yet
    CHECK_NOT(v2_pending.is_ready());
    sub_set.commit();
    CHECK(v2_pending.is_ready());
    CHECK_EQUAL(v2_pending.get(), SubscriptionSet::State::Pending);

    // v1 bootstrapping hasn't begun yet
    CHECK_NOT(v1_bootstrapping.is_ready());

    {
        auto tr = fixture.db->start_write();
        store->begin_bootstrap(*tr, 1);
        tr->commit_and_continue_as_read();
        store->report_progress(tr);
    }

    // v1 is now in the bootstrapping state but v2 isn't
    CHECK(v1_bootstrapping.is_ready());
    CHECK_EQUAL(v1_bootstrapping.get(), SubscriptionSet::State::Bootstrapping);
    CHECK_NOT(v2_bootstrapping.is_ready());

    // Requesting a new notification for the bootstrapping state completes immediately
    CHECK_EQUAL(store->get_by_version(1).get_state_change_notification(SubscriptionSet::State::Bootstrapping).get(),
                SubscriptionSet::State::Bootstrapping);

    // Completing the bootstrap advances us to the awaiting mark state
    {
        auto tr = fixture.db->start_write();
        store->complete_bootstrap(*tr, 1);
        tr->commit_and_continue_as_read();
        store->report_progress(tr);
    }

    CHECK(v1_awaiting_mark.is_ready());
    CHECK_EQUAL(v1_awaiting_mark.get(), SubscriptionSet::State::AwaitingMark);
    CHECK_NOT(v1_complete.is_ready());
    CHECK_NOT(v2_bootstrapping.is_ready());

    // Receiving download completion after the bootstrap application adavances us to Complete
    store->download_complete();
    store->report_progress();

    CHECK(v1_complete.is_ready());
    CHECK_EQUAL(v1_complete.get(), SubscriptionSet::State::Complete);
    CHECK_NOT(v1_error.is_ready());
    CHECK_NOT(v1_superseded.is_ready());
    CHECK_NOT(v2_bootstrapping.is_ready());

    // Completing v2's bootstrap supersedes v1, completing both that and the error notification
    {
        auto tr = fixture.db->start_write();
        store->begin_bootstrap(*tr, 2);
        store->complete_bootstrap(*tr, 2);
        tr->commit_and_continue_as_read();
        store->report_progress(tr);
    }

    CHECK(v1_error.is_ready());
    CHECK(v1_superseded.is_ready());
    CHECK(v2_bootstrapping.is_ready());
    CHECK_EQUAL(v1_error.get(), SubscriptionSet::State::Superseded);
    CHECK_EQUAL(v1_superseded.get(), SubscriptionSet::State::Superseded);
    CHECK_EQUAL(v2_bootstrapping.get(), SubscriptionSet::State::AwaitingMark);

    // v2 is not actually complete until we get download completion
    CHECK_NOT(v2_complete.is_ready());
    store->download_complete();
    store->report_progress();
    CHECK(v2_complete.is_ready());
    CHECK_EQUAL(v2_complete.get(), SubscriptionSet::State::Complete);
}

TEST(Sync_SubscriptionStoreNotifications_Errors)
{
    SHARED_GROUP_TEST_PATH(sub_store_path);
    SubscriptionStoreFixture fixture(sub_store_path);
    auto store = SubscriptionStore::create(fixture.db);

    const auto states = {SubscriptionSet::State::Bootstrapping, SubscriptionSet::State::AwaitingMark,
                         SubscriptionSet::State::Complete, SubscriptionSet::State::Error};

    auto sub_set = store->get_latest().make_mutable_copy();
    std::vector<util::Future<SubscriptionSet::State>> v1_futures;
    for (auto state : states)
        v1_futures.push_back(sub_set.get_state_change_notification(state));
    auto v1_superseded = sub_set.get_state_change_notification(SubscriptionSet::State::Superseded);
    auto v1_sub_set = sub_set.commit();
    sub_set = v1_sub_set.make_mutable_copy();
    std::vector<util::Future<SubscriptionSet::State>> v2_futures;
    for (auto state : states)
        v2_futures.push_back(sub_set.get_state_change_notification(state));
    auto v2_sub_set = sub_set.commit();
    sub_set = v2_sub_set.make_mutable_copy();
    std::vector<util::Future<SubscriptionSet::State>> v3_futures;
    for (auto state : states)
        v3_futures.push_back(sub_set.get_state_change_notification(state));
    auto v3_sub_set = sub_set.commit();

    // Error state completes notifications for all states except superseded
    store->set_error(1, "v1 error message");
    store->report_progress();
    for (auto& fut : v1_futures) {
        CHECK(fut.is_ready());
        auto status = fut.get_no_throw();
        CHECK_NOT(status.is_ok());
        CHECK_EQUAL(status.get_status().code(), ErrorCodes::SubscriptionFailed);
        CHECK_EQUAL(status.get_status().reason(), "v1 error message");
    }
    CHECK_NOT(v1_superseded.is_ready());

    // Sub set is not updated until refreshing
    CHECK_EQUAL(v1_sub_set.state(), SubscriptionSet::State::Pending);

    v1_sub_set.refresh();
    CHECK_EQUAL(v1_sub_set.state(), SubscriptionSet::State::Error);
    CHECK_EQUAL(v1_sub_set.error_str(), "v1 error message");

    // Directly fetching while in an error state works too
    v1_sub_set = store->get_by_version(1);
    CHECK_EQUAL(v1_sub_set.state(), SubscriptionSet::State::Error);
    CHECK_EQUAL(v1_sub_set.error_str(), "v1 error message");

    // v1 failing does not update v2 or v3
    for (auto& fut : v2_futures)
        CHECK_NOT(fut.is_ready());
    for (auto& fut : v3_futures)
        CHECK_NOT(fut.is_ready());

    // v3 failing does not update v2
    store->set_error(3, "v3 error message");
    store->report_progress();

    CHECK_NOT(v1_superseded.is_ready());
    for (auto& fut : v2_futures)
        CHECK_NOT(fut.is_ready());
    for (auto& fut : v3_futures) {
        CHECK(fut.is_ready());
        auto status = fut.get_no_throw();
        CHECK_NOT(status.is_ok());
        CHECK_EQUAL(status.get_status().code(), ErrorCodes::SubscriptionFailed);
        CHECK_EQUAL(status.get_status().reason(), "v3 error message");
    }

    // Trying to begin a bootstrap on the errored version should report that
    // the server did something invalid
    {
        auto tr = fixture.db->start_write();
        CHECK_THROW_ERROR(store->begin_bootstrap(*tr, 3), SyncProtocolInvariantFailed,
                          "Received bootstrap for query version 3 after receiving the error 'v3 error message'");
    }
}

TEST(Sync_SubscriptionStoreNotifications_MultipleStores)
{
    SHARED_GROUP_TEST_PATH(sub_store_path);
    SubscriptionStoreFixture fixture(sub_store_path);
    auto store_1 = SubscriptionStore::create(fixture.db);
    auto db_2 = DB::create(make_client_replication(), sub_store_path);
    auto store_2 = SubscriptionStore::create(db_2);

    store_1->get_latest().make_mutable_copy().commit();

    auto sub_set = store_2->get_latest();
    auto future = sub_set.get_state_change_notification(SubscriptionSet::State::Pending);
    CHECK(future.is_ready());

    future = sub_set.get_state_change_notification(SubscriptionSet::State::Bootstrapping);
    CHECK_NOT(future.is_ready());
    {
        auto tr = fixture.db->start_write();
        store_1->begin_bootstrap(*tr, 1);
        tr->commit();
    }
    CHECK_NOT(future.is_ready());
    store_2->report_progress();
    CHECK(future.is_ready());

    future = sub_set.get_state_change_notification(SubscriptionSet::State::AwaitingMark);
    CHECK_NOT(future.is_ready());
    {
        auto tr = fixture.db->start_write();
        store_1->complete_bootstrap(*tr, 1);
        tr->commit();
    }
    CHECK_NOT(future.is_ready());
    store_2->report_progress();
    CHECK(future.is_ready());

    future = sub_set.get_state_change_notification(SubscriptionSet::State::Complete);
    CHECK_NOT(future.is_ready());
    store_1->download_complete();
    CHECK_NOT(future.is_ready());
    store_2->report_progress();
    CHECK(future.is_ready());

    future = sub_set.get_state_change_notification(SubscriptionSet::State::Superseded);
    CHECK_NOT(future.is_ready());
    store_1->get_active().make_mutable_copy().commit();
    {
        auto tr = fixture.db->start_write();
        store_1->begin_bootstrap(*tr, 2);
        store_1->complete_bootstrap(*tr, 2);
        tr->commit();
    }
    CHECK_NOT(future.is_ready());
    store_2->report_progress();
    CHECK(future.is_ready());
}

TEST(Sync_SubscriptionStoreRefreshSubscriptionSetInvalid)
{
    SHARED_GROUP_TEST_PATH(sub_store_path)
    SubscriptionStoreFixture fixture(sub_store_path);
    auto store = SubscriptionStore::create(fixture.db);
    // Because there are no subscription sets yet, get_latest should point to an empty object
    auto latest = std::make_unique<SubscriptionSet>(store->get_latest());
    CHECK(latest->begin() == latest->end());
    // The SubscriptionStore gets destroyed.
    store.reset();

    // Throws since the SubscriptionStore is gone.
    CHECK_THROW(latest->refresh(), RuntimeError);
}

// Only runs if REALM_MAX_BPNODE_SIZE is 1000 since that's what the pre-created
// test realm files were created with
TEST_IF(Sync_SubscriptionStoreInternalSchemaMigration, REALM_MAX_BPNODE_SIZE == 1000)
{
    SHARED_GROUP_TEST_PATH(sub_store_path)

    // This test file was created using the FLXSyncTestHarness in the object store tests like this:
    //   FLXSyncTestHarness harness("flx_generate_meta_tables");
    //     harness.load_initial_data([&](SharedRealm realm) {
    //     auto config = realm->config();
    //     config.path = "test_flx_metadata_tables_v1.realm";
    //     config.cache = false;
    //     realm->convert(config, false);
    //   });
    auto path = std::filesystem::path(test_util::get_test_resource_path()) / "test_flx_metadata_tables_v1.realm";
    CHECK(util::File::exists(path.string()));
    util::File::copy(path.string(), sub_store_path);
    SubscriptionStoreFixture fixture(sub_store_path);
    auto store = SubscriptionStore::create(fixture.db);
    auto [active_version, latest_version, pending_mark_version] = store->get_version_info();
    CHECK_EQUAL(active_version, latest_version);
    auto active = store->get_active();
    CHECK_EQUAL(active.version(), 1);
    CHECK_EQUAL(active.state(), SubscriptionSet::State::Complete);
    CHECK_EQUAL(active.size(), 1);
    auto sub = active.at(0);
    CHECK_EQUAL(sub.id, ObjectId("62742ab959d7f2e48f59f75d"));
    CHECK_EQUAL(sub.object_class_name, "TopLevel");

    auto tr = fixture.db->start_read();
    SyncMetadataSchemaVersions versions(tr);
    auto flx_sub_store_version = versions.get_version_for(tr, sync::internal_schema_groups::c_flx_subscription_store);
    CHECK(flx_sub_store_version);
    CHECK_EQUAL(*flx_sub_store_version, 2);

    CHECK(!versions.get_version_for(tr, "non_existent_table"));
}

TEST(Sync_SubscriptionStoreNextPendingVersion)
{
    SHARED_GROUP_TEST_PATH(sub_store_path)
    SubscriptionStoreFixture fixture(sub_store_path);
    auto store = SubscriptionStore::create(fixture.db);

    auto mut_sub_set = store->get_latest().make_mutable_copy();
    auto sub_set = mut_sub_set.commit();
    auto complete_set = sub_set.version();

    mut_sub_set = sub_set.make_mutable_copy();
    sub_set = mut_sub_set.commit();
    auto bootstrapping_set = sub_set.version();

    mut_sub_set = sub_set.make_mutable_copy();
    sub_set = mut_sub_set.commit();
    auto pending_set = sub_set.version();

    {
        auto tr = fixture.db->start_write();
        store->begin_bootstrap(*tr, complete_set);
        store->complete_bootstrap(*tr, complete_set);
        tr->commit();
        store->download_complete();
        tr = fixture.db->start_write();
        store->begin_bootstrap(*tr, bootstrapping_set);
        tr->commit();
    }

    auto pending_version = store->get_next_pending_version(0);
    CHECK(pending_version);
    CHECK_EQUAL(pending_version->query_version, bootstrapping_set);

    pending_version = store->get_next_pending_version(bootstrapping_set);
    CHECK(pending_set);
    CHECK_EQUAL(pending_version->query_version, pending_set);

    pending_version = store->get_next_pending_version(pending_set);
    CHECK(!pending_version);
}

TEST(Sync_SubscriptionStoreSubSetHasTable)
{
    SHARED_GROUP_TEST_PATH(sub_store_path)
    SubscriptionStoreFixture fixture(sub_store_path);
    auto store = SubscriptionStore::create(fixture.db);

    auto read_tr = fixture.db->start_read();
    // We should have no subscriptions yet so this should return false.
    auto table_set = store->get_tables_for_latest(*read_tr);
    CHECK(table_set.empty());

    Query query_a(read_tr->get_table(fixture.a_table_key));
    query_a.equal(fixture.foo_col, StringData("JBR")).greater_equal(fixture.bar_col, int64_t(1));
    Query query_b(read_tr->get_table(fixture.a_table_key));
    query_b.equal(fixture.foo_col, "Realm");

    auto mut_sub_set = store->get_latest().make_mutable_copy();
    mut_sub_set.insert_or_assign(query_a);
    mut_sub_set.insert_or_assign(query_b);
    auto sub_set = mut_sub_set.commit();

    read_tr->advance_read();
    table_set = store->get_tables_for_latest(*read_tr);
    CHECK(table_set.find("a") != table_set.end());
    CHECK(table_set.find("fake_table_that_doesnt_exist") == table_set.end());

    mut_sub_set = sub_set.make_mutable_copy();
    mut_sub_set.erase(query_a);
    sub_set = mut_sub_set.commit();

    read_tr->advance_read();
    table_set = store->get_tables_for_latest(*read_tr);
    CHECK(table_set.find("a") != table_set.end());
    CHECK(table_set.find("fake_table_that_doesnt_exist") == table_set.end());

    mut_sub_set = sub_set.make_mutable_copy();
    mut_sub_set.erase(query_b);
    sub_set = mut_sub_set.commit();

    read_tr->advance_read();
    table_set = store->get_tables_for_latest(*read_tr);
    CHECK(table_set.empty());
}

TEST(Sync_SubscriptionStoreNotifyAll)
{
    SHARED_GROUP_TEST_PATH(sub_store_path)
    SubscriptionStoreFixture fixture(sub_store_path);
    auto store = SubscriptionStore::create(fixture.db);

    const Status status_abort(ErrorCodes::OperationAborted, "operation aborted");

    size_t hit_count = 0;

    auto state_handler = [this, &hit_count, &status_abort](StatusWith<SubscriptionSet::State> state) {
        CHECK(!state.is_ok());
        CHECK_EQUAL(state, status_abort);
        hit_count++;
    };

    auto read_tr = fixture.db->start_read();
    // We should have no subscriptions yet so this should return false.
    auto table_set = store->get_tables_for_latest(*read_tr);
    CHECK(table_set.empty());

    Query query_a(read_tr->get_table(fixture.a_table_key));
    query_a.equal(fixture.foo_col, StringData("JBR")).greater_equal(fixture.bar_col, int64_t(1));
    Query query_b(read_tr->get_table(fixture.a_table_key));
    query_b.equal(fixture.foo_col, "Realm");

    // Create multiple pending subscriptions and notify all of them
    {
        auto mut_sub_set1 = store->get_latest().make_mutable_copy();
        mut_sub_set1.insert_or_assign(query_a);
        auto sub_set1 = mut_sub_set1.commit();

        sub_set1.get_state_change_notification(SubscriptionSet::State::Complete)
            .get_async([&state_handler](StatusWith<SubscriptionSet::State> state) {
                state_handler(state);
            });
    }
    {
        auto mut_sub_set2 = store->get_latest().make_mutable_copy();
        mut_sub_set2.insert_or_assign(query_b);
        auto sub_set2 = mut_sub_set2.commit();

        sub_set2.get_state_change_notification(SubscriptionSet::State::Complete)
            .get_async([&state_handler](StatusWith<SubscriptionSet::State> state) {
                state_handler(state);
            });
    }
    {
        auto mut_sub_set3 = store->get_latest().make_mutable_copy();
        mut_sub_set3.insert_or_assign(query_a);
        auto sub_set3 = mut_sub_set3.commit();

        sub_set3.get_state_change_notification(SubscriptionSet::State::Complete)
            .get_async([&state_handler](StatusWith<SubscriptionSet::State> state) {
                state_handler(state);
            });
    }

    auto pending_subs = store->get_pending_subscriptions();
    CHECK_EQUAL(pending_subs.size(), 3);
    for (auto& sub : pending_subs) {
        CHECK_EQUAL(sub.state(), SubscriptionSet::State::Pending);
    }

    store->notify_all_state_change_notifications(status_abort);
    CHECK_EQUAL(hit_count, 3);

    // Any pending subscriptions should still be in the pending state after notify()
    pending_subs = store->get_pending_subscriptions();
    CHECK_EQUAL(pending_subs.size(), 3);
    for (auto& sub : pending_subs) {
        CHECK_EQUAL(sub.state(), SubscriptionSet::State::Pending);
    }
}

TEST(Sync_SubscriptionStoreTerminate)
{
    SHARED_GROUP_TEST_PATH(sub_store_path)
    SubscriptionStoreFixture fixture(sub_store_path);
    auto store = SubscriptionStore::create(fixture.db);

    size_t hit_count = 0;

    auto state_handler = [this, &hit_count](StatusWith<SubscriptionSet::State> state) {
        CHECK(state.is_ok());
        CHECK_EQUAL(state, SubscriptionSet::State::Superseded);
        hit_count++;
    };

    auto read_tr = fixture.db->start_read();
    // We should have no subscriptions yet so this should return false.
    auto table_set = store->get_tables_for_latest(*read_tr);
    CHECK(table_set.empty());

    Query query_a(read_tr->get_table(fixture.a_table_key));
    query_a.equal(fixture.foo_col, StringData("JBR")).greater_equal(fixture.bar_col, int64_t(1));
    Query query_b(read_tr->get_table(fixture.a_table_key));
    query_b.equal(fixture.foo_col, "Realm");

    // Create multiple pending subscriptions and "terminate" all of them
    {
        auto mut_sub_set1 = store->get_latest().make_mutable_copy();
        mut_sub_set1.insert_or_assign(query_a);
        auto sub_set1 = mut_sub_set1.commit();

        sub_set1.get_state_change_notification(SubscriptionSet::State::Complete)
            .get_async([&state_handler](StatusWith<SubscriptionSet::State> state) {
                state_handler(state);
            });
    }
    {
        auto mut_sub_set2 = store->get_latest().make_mutable_copy();
        mut_sub_set2.insert_or_assign(query_b);
        auto sub_set2 = mut_sub_set2.commit();

        sub_set2.get_state_change_notification(SubscriptionSet::State::Complete)
            .get_async([&state_handler](StatusWith<SubscriptionSet::State> state) {
                state_handler(state);
            });
    }
    {
        auto mut_sub_set3 = store->get_latest().make_mutable_copy();
        mut_sub_set3.insert_or_assign(query_a);
        auto sub_set3 = mut_sub_set3.commit();

        sub_set3.get_state_change_notification(SubscriptionSet::State::Complete)
            .get_async([&state_handler](StatusWith<SubscriptionSet::State> state) {
                state_handler(state);
            });
    }

    CHECK_EQUAL(store->get_latest().version(), 3);
    CHECK_EQUAL(store->get_pending_subscriptions().size(), 3);

    auto write_tr = fixture.db->start_write();
    store->reset(*write_tr); // notifications are called on this thread
    write_tr->commit();

    CHECK_EQUAL(hit_count, 3);
    CHECK_EQUAL(store->get_latest().version(), 0);
    CHECK_EQUAL(store->get_pending_subscriptions().size(), 0);
}

// Copied from sync_metadata_schema.cpp
constexpr static std::string_view c_flx_metadata_table("flx_metadata");
constexpr static std::string_view c_meta_schema_version_field("schema_version");

static void create_legacy_metadata_schema(DBRef db, int64_t version)
{
    // Create the legacy table
    TableKey legacy_table_key;
    ColKey legacy_version_key;
    std::vector<SyncMetadataTable> legacy_table_def{
        {&legacy_table_key, c_flx_metadata_table, {{&legacy_version_key, c_meta_schema_version_field, type_Int}}}};
    auto tr = db->start_write();
    create_sync_metadata_schema(tr, &legacy_table_def);
    tr->commit_and_continue_writing();
    auto legacy_meta_table = tr->get_table(legacy_table_key);
    auto legacy_object = legacy_meta_table->create_object();
    // Set the legacy version, which will be converted to the flx subscription store version
    legacy_object.set(legacy_version_key, version);
    tr->commit();
}

TEST(Sync_SyncMetadataSchemaVersionsReader)
{
    SHARED_GROUP_TEST_PATH(sub_store_path)
    DBRef db = DB::create(make_client_replication(), sub_store_path);
    std::string schema_group_name = "schema_group_name";
    int64_t version = 123;
    int64_t legacy_version = 345;

    {
        auto tr = db->start_read();
        // Verify opening a reader on an unitialized versions table returns uninitialized
        SyncMetadataSchemaVersionsReader reader(tr);
        auto schema_version = reader.get_version_for(tr, schema_group_name);
        CHECK(!schema_version);
    }

    {
        auto tr = db->start_read();
        // Initialize the schema versions table and set a schema version
        SyncMetadataSchemaVersions schema_versions(tr);
        tr->promote_to_write();
        schema_versions.set_version_for(tr, schema_group_name, version);
        tr->commit_and_continue_as_read();
        auto schema_version = schema_versions.get_version_for(tr, schema_group_name);
        CHECK(schema_version);
        CHECK(*schema_version == version);
    }

    {
        auto tr = db->start_read();
        // Verify opening a reader on an initialized versions table returns initialized
        SyncMetadataSchemaVersionsReader reader(tr);
        auto schema_version = reader.get_version_for(tr, schema_group_name);
        CHECK(schema_version);
        CHECK(*schema_version == version);
    }

    // Create the legacy metadata schema table
    create_legacy_metadata_schema(db, legacy_version);
    {
        auto tr = db->start_read();
        // Verify opening a reader with legacy data returns uninitialized
        SyncMetadataSchemaVersionsReader reader(tr);
        auto schema_version = reader.get_version_for(tr, schema_group_name);
        CHECK(!schema_version);
    }

    // Test case where both tables exist in database
    {
        auto tr = db->start_read();
        // Initialize the schema versions table and verify the converted flx subscription store version
        SyncMetadataSchemaVersions schema_versions(tr);
        auto schema_version = schema_versions.get_version_for(tr, internal_schema_groups::c_flx_subscription_store);
        CHECK(schema_version);
        CHECK(*schema_version == legacy_version);
        // Verify the legacy table has been deleted after the conversion
        CHECK(!tr->has_table(c_flx_metadata_table));
    }
}

TEST(Sync_SyncMetadataSchemaVersions)
{
    SHARED_GROUP_TEST_PATH(sub_store_path)
    DBRef db = DB::create(make_client_replication(), sub_store_path);
    int64_t flx_version = 234, flx_version2 = 77777;
    int64_t btstrp_version = 567, btstrp_version2 = 888888;
    int64_t mig_version = 890, mig_version2 = 9999999;

    auto check_version = [this, &db](SyncMetadataSchemaVersionsReader& schema_versions,
                                     const std::string_view& group_name, int64_t expected_version) {
        auto tr = db->start_read();
        auto schema_version = schema_versions.get_version_for(tr, group_name);
        CHECK(schema_version);
        CHECK(*schema_version == expected_version);
    };

    {
        // Initialize the table and write values
        auto tr = db->start_read();
        SyncMetadataSchemaVersions schema_versions(tr);
        tr->promote_to_write();
        schema_versions.set_version_for(tr, internal_schema_groups::c_flx_subscription_store, flx_version);
        schema_versions.set_version_for(tr, internal_schema_groups::c_pending_bootstraps, btstrp_version);
        schema_versions.set_version_for(tr, internal_schema_groups::c_flx_migration_store, mig_version);
        tr->commit();

        check_version(schema_versions, internal_schema_groups::c_flx_subscription_store, flx_version);
        check_version(schema_versions, internal_schema_groups::c_pending_bootstraps, btstrp_version);
        check_version(schema_versions, internal_schema_groups::c_flx_migration_store, mig_version);
    }

    {
        // Re-read the data and verify the values
        auto tr = db->start_read();
        SyncMetadataSchemaVersions schema_versions(tr);

        check_version(schema_versions, internal_schema_groups::c_flx_subscription_store, flx_version);
        check_version(schema_versions, internal_schema_groups::c_pending_bootstraps, btstrp_version);
        check_version(schema_versions, internal_schema_groups::c_flx_migration_store, mig_version);
    }

    {
        // Write new values and verify the values
        auto tr = db->start_read();
        SyncMetadataSchemaVersions schema_versions(tr);
        tr->promote_to_write();
        schema_versions.set_version_for(tr, internal_schema_groups::c_flx_subscription_store, flx_version2);
        tr->commit_and_continue_writing();
        schema_versions.set_version_for(tr, internal_schema_groups::c_pending_bootstraps, btstrp_version2);
        tr->commit_and_continue_writing();
        schema_versions.set_version_for(tr, internal_schema_groups::c_flx_migration_store, mig_version2);
        tr->commit();

        check_version(schema_versions, internal_schema_groups::c_flx_subscription_store, flx_version2);
        check_version(schema_versions, internal_schema_groups::c_pending_bootstraps, btstrp_version2);
        check_version(schema_versions, internal_schema_groups::c_flx_migration_store, mig_version2);
    }

    {
        // Re-read the data and verify the new values with a reader
        auto tr = db->start_read();
        SyncMetadataSchemaVersionsReader schema_versions(tr);

        check_version(schema_versions, internal_schema_groups::c_flx_subscription_store, flx_version2);
        check_version(schema_versions, internal_schema_groups::c_pending_bootstraps, btstrp_version2);
        check_version(schema_versions, internal_schema_groups::c_flx_migration_store, mig_version2);
    }
}

TEST(Sync_SyncMetadataSchemaVersions_LegacyTable)
{
    SHARED_GROUP_TEST_PATH(sub_store_path)
    DBRef db = DB::create(make_client_replication(), sub_store_path);
    int64_t version = 678;

    // Create the legacy metadata schema table
    create_legacy_metadata_schema(db, version);
    {
        auto tr = db->start_read();
        // Converts the legacy table to the unified table
        SyncMetadataSchemaVersions schema_versions(tr);
        auto schema_version = schema_versions.get_version_for(tr, internal_schema_groups::c_flx_subscription_store);
        CHECK(schema_version);
        CHECK(*schema_version == version);
        // Verify the legacy table has been deleted after the conversion
        CHECK(!tr->has_table(c_flx_metadata_table));
    }
}

TEST(Sync_MutableSubscriptionSetOperations)
{
    SHARED_GROUP_TEST_PATH(sub_store_path);
    SubscriptionStoreFixture fixture(sub_store_path);
    auto store = SubscriptionStore::create(fixture.db);

    auto read_tr = fixture.db->start_read();
    Query query_a(read_tr->get_table("class_a"));
    query_a.greater_equal(fixture.bar_col, int64_t(1));
    Query query_b(read_tr->get_table(fixture.a_table_key));
    query_b.equal(fixture.foo_col, "Realm");
    Query query_c(read_tr->get_table(fixture.a_table_key));

    // insert_or_assign
    {
        auto out = store->get_latest().make_mutable_copy();
        auto [it, inserted] = out.insert_or_assign("a sub", query_a);
        CHECK(inserted);
        auto named_id = it->id;
        out.insert_or_assign("b sub", query_b);
        CHECK_EQUAL(out.size(), 2);
        std::tie(it, inserted) = out.insert_or_assign("a sub", query_a);
        CHECK_NOT(inserted);
        CHECK_EQUAL(it->id, named_id);
        CHECK_EQUAL(out.size(), 2);
    }

    // find
    {
        auto out = store->get_latest().make_mutable_copy();
        auto [it, inserted] = out.insert_or_assign("a sub", query_a);
        std::tie(it, inserted) = out.insert_or_assign("b sub", query_b);
        CHECK(out.find(query_b));
        CHECK(out.find("a sub"));
    }

    // erase
    {
        auto out = store->get_latest().make_mutable_copy();
        out.insert_or_assign("a sub", query_a);
        out.insert_or_assign("b sub", query_b);
        out.insert_or_assign("c sub", query_c);
        CHECK_EQUAL(out.size(), 3);
        auto it = out.erase(out.begin());
        // Iterator points to last query inserted due do "swap and pop" idiom.
        CHECK_EQUAL(it->query_string, query_c.get_description());
        CHECK_EQUAL(out.size(), 2);
        CHECK_NOT(out.erase("a sub"));
        CHECK_EQUAL(out.size(), 2);
        CHECK(out.erase(query_b));
        CHECK_EQUAL(out.size(), 1);
        CHECK(out.erase("c sub"));
        CHECK_EQUAL(out.size(), 0);
    }

    // erase_by_class_name
    {
        auto out = store->get_latest().make_mutable_copy();
        out.insert_or_assign("a sub", query_a);
        out.insert_or_assign("b sub", query_b);
        out.insert_or_assign("c sub", query_c);
        // Nothing to erase.
        CHECK_NOT(out.erase_by_class_name("foo"));
        // Erase all queries for the class type of the first query.
        CHECK(out.erase_by_class_name(out.begin()->object_class_name));
        // No queries left.
        CHECK_EQUAL(out.size(), 0);
    }

    // erase_by_id
    {
        auto out = store->get_latest().make_mutable_copy();
        out.insert_or_assign("a sub", query_a);
        out.insert_or_assign("b sub", query_b);
        // Nothing to erase.
        CHECK_NOT(out.erase_by_id(ObjectId::gen()));
        // Erase first query.
        CHECK(out.erase_by_id(out.begin()->id));
        CHECK_EQUAL(out.size(), 1);
    }

    // clear
    {
        auto out = store->get_latest().make_mutable_copy();
        out.insert_or_assign("a sub", query_a);
        out.insert_or_assign("b sub", query_b);
        out.insert_or_assign("c sub", query_c);
        CHECK_EQUAL(out.size(), 3);
        out.clear();
        CHECK_EQUAL(out.size(), 0);
    }

    // import
    {
        auto out = store->get_latest().make_mutable_copy();
        out.insert_or_assign("a sub", query_a);
        out.insert_or_assign("b sub", query_b);
        auto subs = out.commit();

        // This is an empty subscription set.
        auto out2 = store->get_active().make_mutable_copy();
        out2.insert_or_assign("c sub", query_c);
        out2.import(std::move(subs));
        // "c sub" is erased when 'import' is used.
        CHECK_EQUAL(out2.size(), 2);
        // insert "c sub" again.
        out2.insert_or_assign("c sub", query_c);
        CHECK_EQUAL(out2.size(), 3);
    }
}

} // namespace realm::sync
