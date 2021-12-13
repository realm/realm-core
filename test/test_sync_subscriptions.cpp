#include "realm/sync/subscriptions.hpp"
#include "realm/sync/noinst/client_history_impl.hpp"

#include "test.hpp"
#include "util/test_path.hpp"

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
    SHARED_GROUP_TEST_PATH(sub_store_path);
    {
        SubscriptionStoreFixture fixture(sub_store_path);
        SubscriptionStore store(fixture.db, [](int64_t) {});
        // Because there are no subscription sets yet, get_latest should point to an invalid object
        // and all the property accessors should return dummy values.
        auto latest = store.get_latest();
        CHECK_EQUAL(latest.begin(), latest.end());
        CHECK_EQUAL(latest.size(), 0);
        CHECK_EQUAL(latest.find("a sub"), latest.end());
        CHECK_EQUAL(latest.version(), 0);
        CHECK(latest.error_str().is_null());
        CHECK_EQUAL(latest.state(), SubscriptionSet::State::Uncommitted);

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
        CHECK_NOT_EQUAL(it, out.end());
        CHECK_EQUAL(it->name(), "a sub");
        CHECK_EQUAL(it->object_class_name(), "a");
        CHECK_EQUAL(it->query_string(), query_a.get_description());
        out.commit();
    }

    // Destroy the DB and reload it and make sure we can get the subscriptions we set in the previous block.
    {
        SubscriptionStoreFixture fixture(sub_store_path);
        SubscriptionStore store(fixture.db, [](int64_t) {});

        auto read_tr = fixture.db->start_read();
        Query query_a(read_tr->get_table(fixture.a_table_key));
        query_a.equal(fixture.foo_col, StringData("JBR")).greater_equal(fixture.bar_col, int64_t(1));

        auto set = store.get_latest();
        CHECK_EQUAL(set.version(), 1);
        CHECK_EQUAL(set.size(), 1);
        auto it = set.find(query_a);
        CHECK_NOT_EQUAL(it, set.end());
        CHECK_EQUAL(it->name(), "a sub");
        CHECK_EQUAL(it->object_class_name(), "a");
        CHECK_EQUAL(it->query_string(), query_a.get_description());

        // Make sure we can't get a subscription set that doesn't exist.
        auto it_end = set.find("b subs");
        CHECK_EQUAL(it_end, set.end());
    }
}

TEST(Sync_SubscriptionStoreStateUpdates)
{
    SHARED_GROUP_TEST_PATH(sub_store_path);
    SubscriptionStoreFixture fixture(sub_store_path);
    SubscriptionStore store(fixture.db, [](int64_t) {});

    auto read_tr = fixture.db->start_read();
    Query query_a(read_tr->get_table("class_a"));
    query_a.equal(fixture.foo_col, StringData("JBR")).greater_equal(fixture.bar_col, int64_t(1));
    Query query_b(read_tr->get_table(fixture.a_table_key));
    query_b.equal(fixture.foo_col, "Realm");

    // Create a new subscription set, insert a subscription into it, and mark it as complete.
    {
        auto out = store.get_latest().make_mutable_copy();
        auto read_tr = fixture.db->start_read();
        auto&& [it, inserted] = out.insert_or_assign("a sub", query_a);
        CHECK(inserted);
        CHECK_NOT_EQUAL(it, out.end());

        out.update_state(SubscriptionSet::State::Complete);
        out.commit();
    }

    // Clone the completed set and update it to have a new query.
    {
        auto new_set = store.get_latest().make_mutable_copy();
        CHECK_EQUAL(new_set.version(), 2);
        new_set.clear();
        new_set.insert_or_assign("b sub", query_b);
        new_set.commit();
    }

    // There should now be two subscription sets, version 1 is complete with query a and version 2 is pending with
    // query b.
    {
        auto active = store.get_active();
        auto latest = store.get_latest();
        CHECK_NOT_EQUAL(active.version(), latest.version());
        CHECK_EQUAL(active.state(), SubscriptionSet::State::Complete);
        CHECK_EQUAL(latest.state(), SubscriptionSet::State::Pending);

        auto it_a = active.begin();
        CHECK_EQUAL(it_a->query_string(), query_a.get_description());
        CHECK_EQUAL(it_a->name(), "a sub");
        auto it_b = latest.begin();
        CHECK_EQUAL(it_b->name(), "b sub");
        CHECK_EQUAL(it_b->query_string(), query_b.get_description());
    }

    // Mark the version 2 set as complete.
    {
        auto latest_mutable = store.get_mutable_by_version(2);
        latest_mutable.update_state(SubscriptionSet::State::Complete);
        latest_mutable.commit();
    }

    // There should now only be one set, version 2, that is complete. Trying to get version 1 should throw an error.
    {
        auto active = store.get_active();
        auto latest = store.get_latest();
        CHECK(active.version() == latest.version());
        CHECK(active.state() == SubscriptionSet::State::Complete);

        // By marking version 2 as complete version 1 will get superceded and removed.
        CHECK_THROW(store.get_mutable_by_version(1), KeyNotFound);
    }

    {
        auto set = store.get_latest().make_mutable_copy();
        CHECK_EQUAL(set.size(), 1);
        // This is just to create a unique name for this sub so we can verify that the iterator returned by
        // insert_or_assign is pointing to the subscription that was just created.
        std::string new_sub_name = ObjectId::gen().to_string();
        auto&& [inserted_it, inserted] = set.insert_or_assign(new_sub_name, query_a);
        CHECK(inserted);
        CHECK_EQUAL(inserted_it->name(), new_sub_name);
        CHECK_EQUAL(set.size(), 2);
        auto it = set.begin();
        CHECK_EQUAL(it->name(), "b sub");
        it = set.erase(it);
        CHECK_NOT_EQUAL(it, set.end());
        CHECK_EQUAL(set.size(), 1);
        CHECK_EQUAL(it->name(), new_sub_name);
        it = set.erase(it);
        CHECK_EQUAL(it, set.end());
        CHECK_EQUAL(set.size(), 0);
    }
}

TEST(Sync_SubscriptionStoreUpdateExisting)
{
    SHARED_GROUP_TEST_PATH(sub_store_path);
    SubscriptionStoreFixture fixture(sub_store_path);
    SubscriptionStore store(fixture.db, [](int64_t) {});

    auto read_tr = fixture.db->start_read();
    Query query_a(read_tr->get_table("class_a"));
    query_a.equal(fixture.foo_col, StringData("JBR")).greater_equal(fixture.bar_col, int64_t(1));
    Query query_b(read_tr->get_table(fixture.a_table_key));
    query_b.equal(fixture.foo_col, "Realm");
    ObjectId id_of_inserted;
    auto sub_name = ObjectId::gen().to_string();
    {
        auto out = store.get_latest().make_mutable_copy();
        auto [it, inserted] = out.insert_or_assign(sub_name, query_a);
        CHECK(inserted);
        CHECK_NOT_EQUAL(it, out.end());
        id_of_inserted = it->id();
        CHECK_NOT_EQUAL(id_of_inserted, ObjectId{});

        std::tie(it, inserted) = out.insert_or_assign(sub_name, query_b);
        CHECK(!inserted);
        CHECK_NOT_EQUAL(it, out.end());
        CHECK_EQUAL(it->object_class_name(), "a");
        CHECK_EQUAL(it->query_string(), query_b.get_description());
        CHECK_EQUAL(it->id(), id_of_inserted);
        out.commit();
    }
    {
        auto set = store.get_latest().make_mutable_copy();
        CHECK_EQUAL(set.size(), 1);
        auto it = std::find_if(set.begin(), set.end(), [&](const Subscription& sub) {
            return sub.id() == id_of_inserted;
        });
        CHECK_NOT_EQUAL(it, set.end());
        CHECK_EQUAL(it->name(), sub_name);
    }
}

TEST(Sync_SubscriptionStoreAssignAnonAndNamed)
{
    SHARED_GROUP_TEST_PATH(sub_store_path);
    SubscriptionStoreFixture fixture(sub_store_path);
    SubscriptionStore store(fixture.db, [](int64_t) {});

    auto read_tr = fixture.db->start_read();
    Query query_a(read_tr->get_table("class_a"));
    query_a.equal(fixture.foo_col, StringData("JBR")).greater_equal(fixture.bar_col, int64_t(1));
    Query query_b(read_tr->get_table(fixture.a_table_key));
    query_b.equal(fixture.foo_col, "Realm");

    {
        auto out = store.get_latest().make_mutable_copy();
        auto [it, inserted] = out.insert_or_assign("a sub", query_a);
        CHECK(inserted);
        auto named_id = it->id();

        std::tie(it, inserted) = out.insert_or_assign(query_a);
        CHECK(inserted);
        CHECK_NOT_EQUAL(it->id(), named_id);
        CHECK_EQUAL(out.size(), 2);

        std::tie(it, inserted) = out.insert_or_assign(query_b);
        CHECK(inserted);
        named_id = it->id();

        std::tie(it, inserted) = out.insert_or_assign("b sub", query_b);
        CHECK(inserted);
        CHECK_NOT_EQUAL(it->id(), named_id);
        CHECK_EQUAL(out.size(), 4);
    }
}

TEST(Sync_SubscriptionStoreNotifications)
{
    SHARED_GROUP_TEST_PATH(sub_store_path);
    SubscriptionStoreFixture fixture(sub_store_path);
    SubscriptionStore store(fixture.db, [](int64_t) {});

    std::vector<util::Future<SubscriptionSet::State>> notification_futures;
    auto sub_set = store.get_latest().make_mutable_copy();
    notification_futures.push_back(sub_set.get_state_change_notification(SubscriptionSet::State::Pending));
    sub_set.commit();
    sub_set = store.get_latest().make_mutable_copy();
    notification_futures.push_back(sub_set.get_state_change_notification(SubscriptionSet::State::Bootstrapping));
    sub_set.commit();
    sub_set = store.get_latest().make_mutable_copy();
    notification_futures.push_back(sub_set.get_state_change_notification(SubscriptionSet::State::Bootstrapping));
    sub_set.commit();
    sub_set = store.get_latest().make_mutable_copy();
    notification_futures.push_back(sub_set.get_state_change_notification(SubscriptionSet::State::Complete));
    sub_set.commit();
    sub_set = store.get_latest().make_mutable_copy();
    notification_futures.push_back(sub_set.get_state_change_notification(SubscriptionSet::State::Complete));
    sub_set.commit();
    sub_set = store.get_latest().make_mutable_copy();
    notification_futures.push_back(sub_set.get_state_change_notification(SubscriptionSet::State::Complete));
    sub_set.commit();

    // This should complete immediately because transitioning to the Pending state happens when you commit.
    CHECK_EQUAL(notification_futures[0].get(), SubscriptionSet::State::Pending);

    // This should also return immediately with a ready future because the subset is in the correct state.
    CHECK_EQUAL(store.get_mutable_by_version(1).get_state_change_notification(SubscriptionSet::State::Pending).get(),
                SubscriptionSet::State::Pending);

    // This should not be ready yet because we haven't updated its state.
    CHECK_NOT(notification_futures[1].is_ready());

    sub_set = store.get_mutable_by_version(2);
    sub_set.update_state(SubscriptionSet::State::Bootstrapping);
    sub_set.commit();

    // Now we should be able to get the future result because we updated the state.
    CHECK_EQUAL(notification_futures[1].get(), SubscriptionSet::State::Bootstrapping);

    // This should not be ready yet because we haven't updated its state.
    CHECK_NOT(notification_futures[2].is_ready());

    // Update the state to complete - skipping the bootstrapping phase entirely.
    sub_set = store.get_mutable_by_version(3);
    sub_set.update_state(SubscriptionSet::State::Complete);
    sub_set.commit();

    // Now we should be able to get the future result because we updated the state and skipped the bootstrapping
    // phase.
    CHECK_EQUAL(notification_futures[2].get(), SubscriptionSet::State::Complete);

    // Update one of the subscription sets to have an error state along with an error message.
    std::string error_msg = "foo bar bizz buzz. i'm an error string for this test!";
    CHECK_NOT(notification_futures[3].is_ready());
    sub_set = store.get_mutable_by_version(4);
    sub_set.update_state(SubscriptionSet::State::Bootstrapping);
    sub_set.update_state(SubscriptionSet::State::Error, std::string_view(error_msg));
    sub_set.commit();

    // This should return a non-OK Status with the error message we set on the subscription set.
    auto err_res = notification_futures[3].get_no_throw();
    CHECK_NOT(err_res.is_ok());
    CHECK_EQUAL(err_res.get_status().code(), ErrorCodes::RuntimeError);
    CHECK_EQUAL(err_res.get_status().reason(), error_msg);

    // Getting a ready future on a set that's already in the error state should also return immediately with an error.
    err_res = store.get_by_version(4).get_state_change_notification(SubscriptionSet::State::Complete).get_no_throw();
    CHECK_NOT(err_res.is_ok());
    CHECK_EQUAL(err_res.get_status().code(), ErrorCodes::RuntimeError);
    CHECK_EQUAL(err_res.get_status().reason(), error_msg);

    // When a higher version supercedes an older one - i.e. you send query sets for versions 5/6 and the server starts
    // bootstrapping version 6 - we expect the notifications for both versions to be fulfilled when the latest one
    // completes bootstrapping.
    CHECK_NOT(notification_futures[4].is_ready());
    CHECK_NOT(notification_futures[5].is_ready());

    auto old_sub_set = store.get_by_version(5);

    sub_set = store.get_mutable_by_version(6);
    sub_set.update_state(SubscriptionSet::State::Complete);
    sub_set.commit();

    CHECK_EQUAL(notification_futures[4].get(), SubscriptionSet::State::Superceded);
    CHECK_EQUAL(notification_futures[5].get(), SubscriptionSet::State::Complete);

    // Also check that new requests for the superceded sub set get filled immediately.
    CHECK_EQUAL(old_sub_set.get_state_change_notification(SubscriptionSet::State::Complete).get(),
                SubscriptionSet::State::Superceded);

    // Check that asking for a state change that is less than the current state of the sub set gets filled
    // immediately.
    CHECK_EQUAL(sub_set.get_state_change_notification(SubscriptionSet::State::Bootstrapping).get(),
                SubscriptionSet::State::Complete);
}

} // namespace realm::sync
