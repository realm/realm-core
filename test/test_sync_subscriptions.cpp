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
        SubscriptionStore store(fixture.db);
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
        auto&& [it, inserted] = out.insert(query_a, std::string{"a sub"});
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
        SubscriptionStore store(fixture.db);

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
    SubscriptionStore store(fixture.db);

    auto read_tr = fixture.db->start_read();
    Query query_a(read_tr->get_table("class_a"));
    query_a.equal(fixture.foo_col, StringData("JBR")).greater_equal(fixture.bar_col, int64_t(1));
    Query query_b(read_tr->get_table(fixture.a_table_key));
    query_b.equal(fixture.foo_col, "Realm");

    // Create a new subscription set, insert a subscription into it, and mark it as complete.
    {

        auto out = store.get_latest().make_mutable_copy();
        auto read_tr = fixture.db->start_read();
        auto&& [it, inserted] = out.insert(query_a, std::string{"a sub"});
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
        new_set.insert(query_b, std::string{"b sub"});
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
        CHECK_THROW(store.get_mutable_by_version(1), std::out_of_range);
    }
}

TEST(Sync_SubscriptionStoreUpdateExisting)
{
    SHARED_GROUP_TEST_PATH(sub_store_path);
    SubscriptionStoreFixture fixture(sub_store_path);
    SubscriptionStore store(fixture.db);

    auto read_tr = fixture.db->start_read();
    Query query_a(read_tr->get_table("class_a"));
    query_a.equal(fixture.foo_col, StringData("JBR")).greater_equal(fixture.bar_col, int64_t(1));
    Query query_b(read_tr->get_table(fixture.a_table_key));
    query_b.equal(fixture.foo_col, "Realm");
    {

        auto out = store.get_latest().make_mutable_copy();
        auto read_tr = fixture.db->start_read();
        auto&& [it, inserted] = out.insert(query_a, std::string{"a sub"});
        CHECK(inserted);
        CHECK_NOT_EQUAL(it, out.end());

        it->update_query(query_b);
        CHECK_EQUAL(it->object_class_name(), "a");
        CHECK_EQUAL(it->query_string(), query_b.get_description());
    }
}

} // namespace realm::sync
