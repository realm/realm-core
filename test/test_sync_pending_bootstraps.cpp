#include "realm/db.hpp"
#include "realm/sync/noinst/client_history_impl.hpp"
#include "realm/sync/noinst/pending_bootstrap_store.hpp"
#include "realm/sync/subscriptions.hpp"

#include "test.hpp"
#include "util/test_path.hpp"

namespace realm::sync {

TEST(Sync_PendingBootstrapStoreBatching)
{
    SHARED_GROUP_TEST_PATH(db_path);
    SyncProgress progress;
    progress.download = {5, 5};
    progress.latest_server_version = {5, 123456789};
    progress.upload = {5, 5};

    {
        auto db = DB::create(make_client_replication(), db_path);
        auto sub_store = sync::SubscriptionStore::create(db);
        sync::PendingBootstrapStore store(db, *test_context.logger, sub_store);
        int64_t query_version = sub_store->get_latest().make_mutable_copy().commit().version();
        CHECK_EQUAL(sub_store->get_by_version(query_version).state(), SubscriptionSet::State::Pending);

        CHECK(!store.has_pending());
        std::vector<RemoteChangeset> changesets;
        std::vector<std::string> changeset_data;

        changeset_data.emplace_back(1024, 'a');
        changesets.emplace_back(1, 6, BinaryData(changeset_data.back()), 1, 1);
        changesets.back().original_changeset_size = 1024;
        changeset_data.emplace_back(1024, 'b');
        changesets.emplace_back(2, 7, BinaryData(changeset_data.back()), 2, 1);
        changesets.back().original_changeset_size = 1024;
        changeset_data.emplace_back(1024, 'c');
        changesets.emplace_back(3, 8, BinaryData(changeset_data.back()), 3, 1);
        changesets.back().original_changeset_size = 1024;

        store.add_batch(query_version, util::none, 0, changesets);

        CHECK(store.has_pending());
        CHECK_EQUAL(sub_store->get_by_version(query_version).state(), SubscriptionSet::State::Bootstrapping);

        changesets.clear();
        changeset_data.clear();
        changeset_data.emplace_back(1024, 'd');
        changesets.emplace_back(4, 9, BinaryData(changeset_data.back()), 4, 2);
        changesets.back().original_changeset_size = 1024;
        changeset_data.emplace_back(1024, 'e');
        changesets.emplace_back(5, 10, BinaryData(changeset_data.back()), 5, 3);
        changesets.back().original_changeset_size = 1024;

        store.add_batch(query_version, progress, 1, changesets);
        CHECK_EQUAL(sub_store->get_by_version(query_version).state(), SubscriptionSet::State::Bootstrapping);
    }

    {
        auto db = DB::create(make_client_replication(), db_path);
        auto sub_store = sync::SubscriptionStore::create(db);
        sync::PendingBootstrapStore store(db, *test_context.logger, sub_store);
        CHECK(store.has_pending());

        auto stats = store.pending_stats();
        CHECK_EQUAL(stats.pending_changeset_bytes, 1024 * 5);
        CHECK_EQUAL(stats.pending_changesets, 5);
        CHECK_EQUAL(stats.query_version, 1);

        auto pending_batch = store.peek_pending(*db->start_read(), (1024 * 3) - 1);
        CHECK_EQUAL(pending_batch.changesets.size(), 3);
        CHECK_EQUAL(pending_batch.remaining_changesets, 2);
        CHECK_EQUAL(pending_batch.query_version, 1);
        CHECK(pending_batch.progress);

        auto validate_changeset = [&](size_t idx, version_type rv, version_type lv, char val, timestamp_type ts,
                                      file_ident_type ident) {
            auto& changeset = pending_batch.changesets[idx];
            CHECK_EQUAL(changeset.remote_version, rv);
            CHECK_EQUAL(changeset.last_integrated_local_version, lv);
            CHECK_EQUAL(changeset.origin_timestamp, ts);
            CHECK_EQUAL(changeset.origin_file_ident, ident);
            CHECK_EQUAL(changeset.original_changeset_size, 1024);
            util::Span<const char> data(changeset.data.get_first_chunk().data(),
                                        changeset.data.get_first_chunk().size());
            CHECK(std::all_of(data.begin(), data.end(), [&](char ch) {
                return ch == val;
            }));
        };

        validate_changeset(0, 1, 6, 'a', 1, 1);
        validate_changeset(1, 2, 7, 'b', 2, 1);
        validate_changeset(2, 3, 8, 'c', 3, 1);

        auto tr = db->start_write();
        store.pop_front_pending(*tr, pending_batch.changesets.size());
        tr->commit();
        CHECK(store.has_pending());

        pending_batch = store.peek_pending(*db->start_read(), 1024 * 2);
        CHECK_EQUAL(pending_batch.changesets.size(), 2);
        CHECK_EQUAL(pending_batch.remaining_changesets, 0);
        CHECK_EQUAL(pending_batch.query_version, 1);
        CHECK(pending_batch.progress);
        validate_changeset(0, 4, 9, 'd', 4, 2);
        validate_changeset(1, 5, 10, 'e', 5, 3);

        tr = db->start_write();
        store.pop_front_pending(*tr, pending_batch.changesets.size());
        tr->commit();
        CHECK(!store.has_pending());
        CHECK_EQUAL(sub_store->get_latest().state(), SubscriptionSet::State::AwaitingMark);
    }
}

TEST(Sync_PendingBootstrapStoreClear)
{
    SHARED_GROUP_TEST_PATH(db_path);
    SyncProgress progress;
    progress.download = {5, 5};
    progress.latest_server_version = {5, 123456789};
    progress.upload = {5, 5};
    auto db = DB::create(make_client_replication(), db_path);
    auto sub_store = sync::SubscriptionStore::create(db);
    sync::PendingBootstrapStore store(db, *test_context.logger, sub_store);

    CHECK(!store.has_pending());
    std::vector<RemoteChangeset> changesets;
    std::vector<std::string> changeset_data;

    changeset_data.emplace_back(1024, 'a');
    changesets.emplace_back(1, 6, BinaryData(changeset_data.back()), 1, 1);
    changesets.back().original_changeset_size = 1024;
    changeset_data.emplace_back(1024, 'b');
    changesets.emplace_back(2, 7, BinaryData(changeset_data.back()), 2, 1);
    changesets.back().original_changeset_size = 1024;

    int64_t query_version = sub_store->get_latest().make_mutable_copy().commit().version();
    store.add_batch(query_version, progress, 1, changesets);
    CHECK(store.has_pending());
    CHECK_EQUAL(SubscriptionSet::State::Bootstrapping, sub_store->get_latest().state());

    auto pending_batch = store.peek_pending(*db->start_read(), 1025);
    CHECK_EQUAL(pending_batch.remaining_changesets, 0);
    CHECK_EQUAL(pending_batch.query_version, query_version);
    CHECK(pending_batch.progress);
    CHECK_EQUAL(pending_batch.changesets.size(), 2);

    auto tr = db->start_write();
    store.clear(*tr, query_version);
    tr->commit();

    CHECK_EQUAL(SubscriptionSet::State::Pending, sub_store->get_latest().state());
    CHECK_NOT(store.has_pending());
}

} // namespace realm::sync
