#include "realm/db.hpp"
#include "realm/sync/noinst/client_history_impl.hpp"
#include "realm/sync/noinst/pending_bootstrap_store.hpp"

#include "test.hpp"
#include "util/test_path.hpp"

namespace realm::sync {

namespace {

std::vector<RemoteChangeset> create_changesets(int64_t remote_version, int count, int& last_count,
                                               std::vector<std::string>& changeset_data)
{
    std::vector<RemoteChangeset> changesets;
    while (count-- > 0) {
        changeset_data.emplace_back(1024, (char)('a' + last_count));
        changesets.emplace_back(remote_version, 5 + last_count, BinaryData(changeset_data.back()), 1 + last_count,
                                1 + (last_count / 3));
        changesets.back().original_changeset_size = 1024;
        ++last_count;
    }
    return changesets;
}

void verify_store_state(test_util::unit_test::TestContext& test_context, sync::PendingBootstrapStore& store,
                        bool pending, int count = 0, int64_t query_version = 0, int64_t remote_version = 0,
                        bool complete = false)
{
    CHECK_EQUAL(store.has_pending(), pending);
    if (pending) {
        CHECK(store.query_version());
        CHECK(store.remote_version());
        CHECK_EQUAL(*store.query_version(), query_version);
        CHECK_EQUAL(*store.remote_version(), remote_version);
    }
    else {
        CHECK_NOT(store.query_version());
        CHECK_NOT(store.remote_version());
    }
    CHECK_EQUAL(store.bootstrap_complete(), complete);
    auto stats = store.pending_stats();
    CHECK_EQUAL(stats.pending_changesets, count);
    CHECK_EQUAL(stats.query_version, query_version);
    CHECK_EQUAL(stats.remote_version, remote_version);
    CHECK_EQUAL(stats.complete, complete);
    CHECK_EQUAL(stats.pending_changeset_bytes, 1024 * count);
}

void validate_changesets(test_util::unit_test::TestContext& test_context, std::vector<RemoteChangeset> changesets,
                         version_type remote_version, int count, int start)
{
    version_type last_integrated = 5 + start;
    timestamp_type timestamp = 1 + start;
    file_ident_type file_ident = 1;
    char val = 'a' + start;
    for (int idx = 0; idx < count; idx++) {
        auto& changeset = changesets[idx];
        CHECK_EQUAL(changeset.remote_version, remote_version);
        CHECK_EQUAL(changeset.last_integrated_local_version, last_integrated + idx);
        CHECK_EQUAL(changeset.origin_timestamp, timestamp + idx);
        CHECK_EQUAL(changeset.origin_file_ident, file_ident + ((idx + start) / 3));
        CHECK_EQUAL(changeset.original_changeset_size, 1024);
        util::Span<const char> data(changeset.data.get_first_chunk().data(), changeset.data.get_first_chunk().size());
        CHECK(std::all_of(data.begin(), data.end(), [&](char ch) {
            return ch == (val + idx);
        }));
    }
}

} // namespace

TEST(Sync_PendingBootstrapStoreBatching)
{
    SHARED_GROUP_TEST_PATH(db_path);
    SyncProgress progress;
    progress.download = {3, 5};
    progress.latest_server_version = {5, 123456789};
    progress.upload = {5, 5};
    {
        auto db = DB::create(make_client_replication(), db_path);
        sync::PendingBootstrapStore store(db, *test_context.logger);

        // Initial state
        verify_store_state(test_context, store, false);

        std::vector<RemoteChangeset> changesets;
        std::vector<std::string> changeset_data;
        int last_count = 0;
        bool created_new_batch = false;

        // Create 3 changesets with remote_version of 3
        changesets = create_changesets(3, 3, last_count, changeset_data);

        store.add_batch(1, 3, util::none, changesets, &created_new_batch);

        // Verify store has a new bootstrap with 3 changesets and versions: query 1 / remote 3
        CHECK(created_new_batch);
        verify_store_state(test_context, store, true, 3, 1, 3);

        // Create 2 more changesets with remote_version of 3
        changesets = create_changesets(3, 2, last_count, changeset_data);

        store.add_batch(1, 3, progress, changesets, &created_new_batch);

        // Verify store has a complete bootstrap with 5 changesets and versions: query 1 / remote 3
        CHECK_NOT(created_new_batch);
        verify_store_state(test_context, store, true, 5, 1, 3, true);
    }

    {
        auto db = DB::create(make_client_replication(), db_path);
        sync::PendingBootstrapStore store(db, *test_context.logger);

        // Verify re-opened store has a complete bootstrap with 5 changesets and versions: query 1 / remote 3
        verify_store_state(test_context, store, true, 5, 1, 3, true);

        auto pending_batch = store.peek_pending((1024 * 3) - 1);
        CHECK_EQUAL(pending_batch.changesets.size(), 3);
        CHECK_EQUAL(pending_batch.remaining_changesets, 2);
        CHECK_EQUAL(pending_batch.query_version, 1);
        CHECK_EQUAL(pending_batch.remote_version, 3);
        CHECK(pending_batch.progress);

        // Validate the 3 changesets just retrieved from the bootstrap store
        validate_changesets(test_context, pending_batch.changesets, 3, 3, 0);

        auto tr = db->start_write();
        store.pop_front_pending(tr, pending_batch.changesets.size());
        tr->commit();

        // Verify re-opened store still has a complete bootstrap with 2 changesets and versions: query 1 / remote 3
        verify_store_state(test_context, store, true, 2, 1, 3, true);

        pending_batch = store.peek_pending(1024 * 2);
        CHECK_EQUAL(pending_batch.changesets.size(), 2);
        CHECK_EQUAL(pending_batch.remaining_changesets, 0);
        CHECK_EQUAL(pending_batch.query_version, 1);
        CHECK_EQUAL(pending_batch.remote_version, 3);
        CHECK(pending_batch.progress);

        // Validate the last 2 changesets just retrieved from the bootstrap store
        validate_changesets(test_context, pending_batch.changesets, 3, 2, 3);

        tr = db->start_write();
        store.pop_front_pending(tr, pending_batch.changesets.size());
        tr->commit();

        // Verify store is back to uninitialised state with no pending bootstrap
        verify_store_state(test_context, store, false);
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
    sync::PendingBootstrapStore store(db, *test_context.logger);

    // Initial state
    verify_store_state(test_context, store, false);

    std::vector<RemoteChangeset> changesets;
    std::vector<std::string> changeset_data;
    int last_count = 0;
    bool created_new_batch = false;

    // Create 2 changesets with remote_version of 5
    changesets = create_changesets(5, 2, last_count, changeset_data);

    store.add_batch(2, 5, progress, changesets, &created_new_batch);

    // Verify store has a complete bootstrap with 2 changesets and versions: query 2 / remote 5
    CHECK(created_new_batch);
    verify_store_state(test_context, store, true, 2, 2, 5, true);

    auto pending_batch = store.peek_pending(1025);
    CHECK_EQUAL(pending_batch.changesets.size(), 2);
    CHECK_EQUAL(pending_batch.remaining_changesets, 0);
    CHECK_EQUAL(pending_batch.query_version, 2);
    CHECK_EQUAL(pending_batch.remote_version, 5);
    CHECK(pending_batch.progress);

    store.clear();

    // Verify store is back to uninitialised state with no pending bootstrap
    verify_store_state(test_context, store, false);

    // Verify the pending stats return empty as well
    pending_batch = store.peek_pending(1024);
    CHECK_EQUAL(pending_batch.changesets.size(), 0);
    CHECK_EQUAL(pending_batch.remaining_changesets, 0);
    CHECK_EQUAL(pending_batch.query_version, 0);
    CHECK_EQUAL(pending_batch.remote_version, 0);
    CHECK_NOT(pending_batch.progress);
}

TEST(Sync_PendingBootstrapStoreDifferentVersions)
{
    SHARED_GROUP_TEST_PATH(db_path);
    auto db = DB::create(make_client_replication(), db_path);
    sync::PendingBootstrapStore store(db, *test_context.logger);

    // Initial state
    verify_store_state(test_context, store, false);

    std::vector<RemoteChangeset> changesets;
    std::vector<std::string> changeset_data;
    int last_count = 0;
    bool created_new_batch = false;

    // Create 4 changesets with remote_version of 4
    changesets = create_changesets(4, 4, last_count, changeset_data);

    store.add_batch(3, 4, util::none, changesets, &created_new_batch);

    // Verify store has a pending bootstrap with 4 changesets and versions: query 3 / remote 4
    CHECK(created_new_batch);
    verify_store_state(test_context, store, true, 4, 3, 4);

    // Create 3 changesets with remote_version of 4
    changesets = create_changesets(4, 3, last_count, changeset_data);

    created_new_batch = false;
    store.add_batch(4, 4, util::none, changesets, &created_new_batch);

    // Verify store has a new bootstrap entry with 3 changesets and versions: query 4 / remote 4
    CHECK(created_new_batch);
    verify_store_state(test_context, store, true, 3, 4, 4);

    // Create 5 changesets with remote_version of 6
    changesets = create_changesets(6, 5, last_count, changeset_data);

    created_new_batch = false;
    store.add_batch(4, 6, util::none, changesets, &created_new_batch);

    // Verify store has a new bootstrap entry with 5 changesets and versions: query 4 / remote 6
    CHECK(created_new_batch);
    verify_store_state(test_context, store, true, 5, 4, 6);

    SyncProgress progress;
    progress.download = {6, 5};
    progress.latest_server_version = {5, 123456789};
    progress.upload = {5, 5};

    // Create 2 changesets with remote_version of 6
    changesets = create_changesets(6, 2, last_count, changeset_data);

    created_new_batch = false;
    store.add_batch(4, 6, progress, changesets, &created_new_batch);

    // Verify store completed the bootstrap entry with 7 changesets and versions: query 4 / remote 6
    CHECK_NOT(created_new_batch);
    verify_store_state(test_context, store, true, 7, 4, 6, true);

    // Create 1 changeset with remote_version of 6
    changesets = create_changesets(6, 1, last_count, changeset_data);

    created_new_batch = false;
    store.add_batch(4, 6, util::none, changesets, &created_new_batch);

    // Verify store created new bootstrap entry with 1 changesets and versions: query 4 / remote 6
    CHECK(created_new_batch);
    verify_store_state(test_context, store, true, 1, 4, 6);

    // Reset the bootstrap store
    store.clear();

    // Initial state
    verify_store_state(test_context, store, false);

    // Add a bootstrap message with no progress or changesets
    created_new_batch = false;
    store.add_batch(5, 9, util::none, {}, &created_new_batch);

    // Verify store created pending bootstrap entry with 0 changesets and versions: query 5 / remote 9
    CHECK(created_new_batch);
    verify_store_state(test_context, store, true, 0, 5, 9);

    // Create 2 changesets with remote_version of 10
    changesets = create_changesets(10, 2, last_count, changeset_data);

    created_new_batch = false;
    store.add_batch(5, 10, util::none, changesets, &created_new_batch);

    // Verify store reused pending bootstrap entry with 2 changesets and versions: query 5 / remote 10
    CHECK_NOT(created_new_batch);
    verify_store_state(test_context, store, true, 2, 5, 10);
}

TEST(Sync_PendingBootstrapStoreRestart)
{
    SHARED_GROUP_TEST_PATH(db_path);
    {
        auto db = DB::create(make_client_replication(), db_path);
        sync::PendingBootstrapStore store(db, *test_context.logger);

        // Initial state
        verify_store_state(test_context, store, false);

        std::vector<RemoteChangeset> changesets;
        std::vector<std::string> changeset_data;
        int last_count = 0;
        bool created_new_batch = false;

        // Create 4 changesets with remote_version of 4
        changesets = create_changesets(4, 4, last_count, changeset_data);

        store.add_batch(3, 4, util::none, changesets, &created_new_batch);

        // Verify store has a pending bootstrap with 4 changesets and versions: query 3 / remote 4
        CHECK(created_new_batch);
        verify_store_state(test_context, store, true, 4, 3, 4);
    }
    {
        // Reopen the store - since there was only a partial bootstrap, it should be cleared
        auto db = DB::create(make_client_replication(), db_path);
        sync::PendingBootstrapStore store(db, *test_context.logger);

        // Initial state
        verify_store_state(test_context, store, false);

        std::vector<RemoteChangeset> changesets;
        std::vector<std::string> changeset_data;
        int last_count = 0;
        bool created_new_batch = false;

        SyncProgress progress;
        progress.download = {10, 5};
        progress.latest_server_version = {5, 123456789};
        progress.upload = {5, 5};

        // Create 2 changesets with remote_version of 10
        changesets = create_changesets(10, 2, last_count, changeset_data);

        store.add_batch(5, 10, progress, changesets, &created_new_batch);

        // Verify store has a complete bootstrap with 2 changesets and versions: query 5 / remote 10
        CHECK(created_new_batch);
        verify_store_state(test_context, store, true, 2, 5, 10, true);
    }
    {
        // Reopen the store - complete bootstrap should be reloaded
        auto db = DB::create(make_client_replication(), db_path);
        sync::PendingBootstrapStore store(db, *test_context.logger);

        // Verify store has a complete bootstrap with 2 changesets and versions: query 5 / remote 10
        verify_store_state(test_context, store, true, 2, 5, 10, true);
    }
}

} // namespace realm::sync
