#include <string>
#include <sstream>

#include <realm/util/file.hpp>
#include <realm/db.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/server/server_history.hpp>

#include "test.hpp"
#include "util/compare_groups.hpp"
#include "sync_fixtures.hpp"

using namespace realm;
using namespace realm::test_util;


// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment variable `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.


namespace {

// FIXME: Disabled because of a migration bug in Core re: embedded objects support.
TEST_IF(Sync_HistoryMigration, false)
{
    // Set to true to produce new versions of client and server-side files in
    // `resources/history_migration/` as needed. This should be done whenever
    // the client or server-side schema versions are bumped. Do this, and rerun
    // the test before you add new versions to `client_schema_versions` and
    // `server_schema_versions`.
    //
    // Be careful, however, not to produce the new files until the new history
    // schema version is finalized.
    //
    // You need to manually rename (remove the `_new` suffix) and commit the new
    // files. When you have done that, add corresponding new versions to
    // `client_schema_versions` and `server_schema_versions`.
    bool produce_new_files = false;

    // The lists of history schema versions that are upgradable to the current
    // version, and for which corresponding files exist in
    // `resources/history_migration/`. See the `produce_new_files` above for an
    // easy way to generate new files.
    std::vector<int> client_schema_versions = {1, 2, 10};
    std::vector<int> server_schema_versions = {7, 8, 9, 10, 20};

    // Before bootstrapping, there can be no client or server files. After
    // bootstrapping, there must be at least one client, and one server file.
    if (client_schema_versions.empty() != server_schema_versions.empty())
        throw std::runtime_error("Bootstrapping inconsistency");

    bool bootstrapped = !server_schema_versions.empty();

    int latest_client_schema_version = -1;
    if (bootstrapped) {
        auto i = std::max_element(client_schema_versions.begin(), client_schema_versions.end());
        latest_client_schema_version = *i;
        REALM_ASSERT(latest_client_schema_version <= sync::get_client_history_schema_version());
    }

    int latest_server_schema_version = -1;
    if (bootstrapped) {
        auto i = std::max_element(server_schema_versions.begin(), server_schema_versions.end());
        latest_server_schema_version = *i;
        REALM_ASSERT(latest_server_schema_version <= _impl::get_server_history_schema_version());
    }

    // Fail the test if there are no files corresponding to the current client
    // and server-side history schema versions (see `produce_new_files` above
    // for an easy way to produce the missing files).
    if (CHECK(bootstrapped)) {
        // FIXME: produce new files
        // CHECK_EQUAL(sync::get_client_history_schema_version(), latest_client_schema_version);
        // CHECK_EQUAL(_impl::get_server_history_schema_version(), latest_server_schema_version);
    }

    // Create reference contents
    //
    // CAUTION: This cannot be changed without also purging all the accumulated
    // test files.
    auto reference_initialize = [&](const std::string& client_path) {
        DBRef sg = DB::create(sync::make_client_replication(), client_path);
        WriteTransaction wt{sg};
        TableRef table = wt.get_group().add_table_with_primary_key("class_Table", type_String, "label");
        ColKey col_key = table->add_column(type_Int, "value");
        table->create_object_with_primary_key("Banach").set(col_key, 88);
        table->create_object_with_primary_key("Hausdorff").set(col_key, 99);
        table->create_object_with_primary_key("Hilbert").set(col_key, 77);
        wt.commit();
    };

    auto modify = [&](const std::string& client_path, StringData label, int old_value, int new_value) {
        DBRef sg = DB::create(sync::make_client_replication(), client_path);
        WriteTransaction wt{sg};
        Group& group = wt.get_group();
        TableRef table = group.get_table("class_Table");
        REALM_ASSERT(table);
        ColKey col_key_label = table->get_column_key("label");
        ColKey col_key_value = table->get_column_key("value");
        REALM_ASSERT(col_key_label);
        REALM_ASSERT(col_key_value);
        ObjKey key = table->find_first_string(col_key_label, label);
        Obj obj = table->get_object(key);
        REALM_ASSERT(key);
        CHECK_EQUAL(old_value, obj.get<Int>(col_key_value));
        obj.set(col_key_value, new_value);
        wt.commit();
    };

    // Modify reference contents as by local client.
    //
    // CAUTION: This cannot be changed without also purging all the accumulated
    // test files.
    auto reference_local_modify = [&](const std::string& client_path) {
        modify(client_path, "Hausdorff", 99, 66);
    };

    // Modify reference contents as by remote client.
    //
    // CAUTION: This cannot be changed without also purging all the accumulated
    // test files.
    auto reference_remote_modify = [&](const std::string& client_path) {
        modify(client_path, "Hilbert", 77, 55);
    };

    SHARED_GROUP_TEST_PATH(no_changes_reference_path);
    SHARED_GROUP_TEST_PATH(local_changes_reference_path);
    SHARED_GROUP_TEST_PATH(remote_changes_reference_path);
    SHARED_GROUP_TEST_PATH(all_changes_reference_path);
    reference_initialize(no_changes_reference_path);
    reference_initialize(local_changes_reference_path);
    reference_initialize(remote_changes_reference_path);
    reference_initialize(all_changes_reference_path);
    reference_local_modify(local_changes_reference_path);
    reference_local_modify(all_changes_reference_path);
    reference_remote_modify(remote_changes_reference_path);
    reference_remote_modify(all_changes_reference_path);

    class ServerHistoryContext : public _impl::ServerHistory::Context {
    public:
        std::mt19937_64& server_history_get_random() noexcept override final
        {
            return m_random;
        }

    private:
        std::mt19937_64 m_random;
    };
    ServerHistoryContext server_history_context;
    _impl::ServerHistory::DummyCompactionControl compaction_control;

    // Accesses file without migrating it
    auto get_history_info = [&](const std::string& path, int& history_type, int& history_schema_version) {
        Group group{path};
        using gf = _impl::GroupFriend;
        Allocator& alloc = gf::get_alloc(group);
        ref_type top_ref = gf::get_top_ref(group);
        _impl::History::version_type version; // Dummy
        gf::get_version_and_history_info(alloc, top_ref, version, history_type, history_schema_version);
    };

    auto verify_client_file = [&](const std::string& client_path) {
        DBRef sg = DB::create(sync::make_client_replication(), client_path);
        ReadTransaction rt{sg};
        rt.get_group().verify();
    };

    auto verify_server_file = [&](const std::string& server_path) {
        _impl::ServerHistory history{server_history_context, compaction_control};
        DBRef sg = DB::create(history, server_path);
        ReadTransaction rt{sg};
        rt.get_group().verify();
    };

    auto compare_client_files = [&](const std::string& client_path_1, const std::string& client_path_2) {
        DBRef sg_1 = DB::create(sync::make_client_replication(), client_path_1);
        DBRef sg_2 = DB::create(sync::make_client_replication(), client_path_2);
        ReadTransaction rt_1{sg_1};
        ReadTransaction rt_2{sg_2};
        return compare_groups(rt_1, rt_2, test_context.logger);
    };

    auto compare_client_and_server_files = [&](const std::string& client_path, const std::string& server_path) {
        auto history_1 = sync::make_client_replication();
        _impl::ServerHistory history_2{server_history_context, compaction_control};
        DBRef sg_1 = DB::create(*history_1, client_path);
        DBRef sg_2 = DB::create(history_2, server_path);
        ReadTransaction rt_1{sg_1};
        ReadTransaction rt_2{sg_2};
        return compare_groups(rt_1, rt_2, test_context.logger);
    };

    std::string resources_dir = "resources";
    std::string history_migration_dir = util::File::resolve("history_migration", resources_dir);

    std::ostringstream formatter;
    formatter.fill('0');

    auto get_name = [&](const char* prefix, int history_schema_version, bool with_new) {
        formatter.str({});
        formatter << prefix << "_schema_version_" << std::setw(3) << history_schema_version;
        if (with_new)
            formatter << "_new";
        formatter << ".realm";
        return std::move(formatter).str();
    };

    auto fetch_file = [&](const char* prefix, int history_schema_version, const std::string& path) {
        bool with_new = false;
        std::string fetch_name = get_name(prefix, history_schema_version, with_new);
        std::string fetch_path = util::File::resolve(fetch_name, history_migration_dir);
        log("Fetching %1", fetch_path);
        util::File::copy(fetch_path, path);
    };

    auto stash_file = [&](const std::string& path, const char* prefix, int history_schema_version) {
        bool with_new = true;
        std::string stash_name = get_name(prefix, history_schema_version, with_new);
        std::string stash_path = util::File::resolve(stash_name, history_migration_dir);
        util::try_make_dir(history_migration_dir);
        log("Stashing %1", stash_path);
        util::File::copy(path, stash_path);
    };

    auto fetch_and_migrate_client_file = [&](int client_schema_version, const std::string& client_path) {
        fetch_file("client", client_schema_version, client_path);
        // Verify that it is a client-side file and that it uses the specified
        // history schema version
        int history_type = 0, history_schema_version = 0;
        try {
            get_history_info(client_path, history_type, history_schema_version);
            if (history_type != Replication::hist_SyncClient)
                throw std::runtime_error{"Bad history type for client-side file"};
            if (history_schema_version != client_schema_version)
                throw std::runtime_error{"Bad history schema version for client-side file"};
        }
        catch (const FileFormatUpgradeRequired&) {
            // File formats prior to 10 cannot be opened in read-only mode
        }
        // History migration is a side-effect of verification
        verify_client_file(client_path);
        if (!compare_client_files(local_changes_reference_path, client_path))
            throw std::runtime_error{"Bad contents in fetched client-side file"};
    };

    auto fetch_and_migrate_server_file = [&](int server_schema_version, const std::string& server_path) {
        fetch_file("server", server_schema_version, server_path);
        // Verify that it is a server-side file and that it uses the specified
        // history schema version
        int history_type = 0, history_schema_version = 0;
        try {
            get_history_info(server_path, history_type, history_schema_version);
            if (history_type != Replication::hist_SyncServer)
                throw std::runtime_error{"Bad history type for server-side file"};
            if (history_schema_version != server_schema_version)
                throw std::runtime_error{"Bad history schema version for server-side file"};
        }
        catch (const FileFormatUpgradeRequired&) {
            // File formats prior to 10 cannot be opened in read-only mode
        }
        // History migration is a side-effect of verification
        verify_server_file(server_path);
        if (!compare_client_and_server_files(remote_changes_reference_path, server_path))
            throw std::runtime_error{"Bad contents in fetched server-side file"};
    };

    // Save a copy in `resources/history_migration/` if the current client-side
    // history schema version is newer than that of the latest available test
    // file
    auto stash_client_file_if_new = [&](const std::string& client_path) {
        verify_client_file(client_path);
        int history_type = 0, history_schema_version = 0;
        get_history_info(client_path, history_type, history_schema_version);
        REALM_ASSERT(history_type == Replication::hist_SyncClient);
        if (bootstrapped) {
            if (latest_client_schema_version == sync::get_client_history_schema_version())
                return; // Latest file is current
        }
        stash_file(client_path, "client", history_schema_version);
    };

    // Save a copy in `resources/history_migration/` if the current server-side
    // history schema version is newer than that of the latest available test
    // file
    auto stash_server_file_if_new = [&](const std::string& server_path) {
        verify_server_file(server_path);
        int history_type = 0, history_schema_version = 0;
        get_history_info(server_path, history_type, history_schema_version);
        REALM_ASSERT(history_type == Replication::hist_SyncServer);
        if (bootstrapped) {
            if (latest_server_schema_version == _impl::get_server_history_schema_version())
                return; // Latest file is current
        }
        stash_file(server_path, "server", history_schema_version);
    };

    std::string virtual_path = "/test";

    auto get_server_path = [&](const std::string& server_dir) {
        fixtures::ClientServerFixture fixture{server_dir, test_context};
        return fixture.map_virtual_to_real_path(virtual_path);
    };

    auto synchronize = [&](const std::string& client_path, const std::string& server_dir) {
        fixtures::ClientServerFixture fixture{server_dir, test_context};
        fixture.start();
        auto db = DB::create(sync::make_client_replication(), client_path);
        sync::Session session = fixture.make_bound_session(db, virtual_path);
        session.wait_for_upload_complete_or_client_stopped();
        session.wait_for_download_complete_or_client_stopped();
    };

    auto test = [&](int client_schema_version, int server_schema_version) {
        log("Test: client_schema_version=%1, server_schema_version=%2", client_schema_version, server_schema_version);

        SHARED_GROUP_TEST_PATH(local_client_path);
        SHARED_GROUP_TEST_PATH(remote_client_path);
        TEST_DIR(server_dir);
        std::string server_path = get_server_path(server_dir);

        // Verify that the server's contents can be faithfully pushed to a new
        // client-side file after the server file has gone through history
        // migration
        fetch_and_migrate_server_file(server_schema_version, server_path);
        synchronize(remote_client_path, server_dir);
        CHECK(compare_client_files(remote_changes_reference_path, remote_client_path));
        verify_client_file(remote_client_path);

        // Fetch the client-side file to be tested, and check that it can be
        // resynchronized after having gone through history migration
        fetch_and_migrate_client_file(client_schema_version, local_client_path);
        synchronize(local_client_path, server_dir);
        CHECK(compare_client_files(all_changes_reference_path, local_client_path));
        verify_client_file(local_client_path);

        // Make a modification through one file, and check that it arrives
        // faithfully in the other, and keep doing this fow a while with
        // alternating directions
        std::string client_path_in = local_client_path;
        std::string client_path_out = remote_client_path;
        int prior_new_value = 55;
        int n = 5;
        for (int i = 0; i < n; ++i) {
            int old_value = prior_new_value;
            int new_value = 1000 + i;
            modify(client_path_in, "Hilbert", old_value, new_value);
            prior_new_value = new_value;
            synchronize(client_path_in, server_dir);
            synchronize(client_path_out, server_dir);
            verify_client_file(client_path_in);
            verify_client_file(client_path_out);
            swap(client_path_in, client_path_out);
        }
    };

    // Test all client-side schema versions using latest server-side schema
    // version
    if (bootstrapped) {
        for (int client_schema_version : client_schema_versions)
            test(client_schema_version, latest_server_schema_version);
    }

    // Test all server-side schema versions using latest client-side schema
    // version
    if (bootstrapped) {
        for (int server_schema_version : server_schema_versions)
            test(latest_client_schema_version, server_schema_version);
    }

    if (produce_new_files) {
        SHARED_GROUP_TEST_PATH(local_client_path);
        TEST_DIR(server_dir);
        std::string server_path = get_server_path(server_dir);
        if (!bootstrapped) {
            // Bootstrapping case

            // The following deliberately constructs a pair of files (client and
            // server-side) that are only partially synchronized. Both have
            // changes that are not in the other.
            SHARED_GROUP_TEST_PATH(init_client_path);
            reference_initialize(init_client_path);
            synchronize(init_client_path, server_dir);
            synchronize(local_client_path, server_dir);

            // Make local changes that will not be uploaded until after migration
            reference_local_modify(local_client_path);

            // Make remote changes that will not be downloaded until after migration
            SHARED_GROUP_TEST_PATH(remote_client_path);
            synchronize(remote_client_path, server_dir);
            reference_remote_modify(remote_client_path);
            synchronize(remote_client_path, server_dir);
        }
        else {
            // Migration case
            fetch_and_migrate_client_file(latest_client_schema_version, local_client_path);
            fetch_and_migrate_server_file(latest_server_schema_version, server_path);
        }

        stash_client_file_if_new(local_client_path);
        stash_server_file_if_new(server_path);

        // Sanity check
        if (!compare_client_files(local_changes_reference_path, local_client_path))
            throw std::runtime_error("Bad 'local changes' contents in client file");
        if (!compare_client_and_server_files(remote_changes_reference_path, server_path))
            throw std::runtime_error("Bad 'remote changes' contents in server file");
        synchronize(local_client_path, server_dir);
        if (!compare_client_files(all_changes_reference_path, local_client_path))
            throw std::runtime_error("Bad 'all changes' contents in client file");
        if (!compare_client_and_server_files(all_changes_reference_path, server_path))
            throw std::runtime_error("Bad 'all changes' contents in server file");
    }

    CHECK_NOT(produce_new_files); // Should not be enabled under normal circumstances
}
} // unnamed namespace
