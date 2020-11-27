#include <string>

#include <realm/util/random.hpp>
#include <realm/util/logger.hpp>
#include <realm/db.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/protocol.hpp>

#include "test.hpp"
#include "util/compare_groups.hpp"

using namespace realm;
using namespace realm::sync;
using namespace realm::_impl::client_reset;
using namespace realm::test_util;

namespace {

const util::Optional<std::array<char, 64>> encryption_key_none;

void check_common(test_util::unit_test::TestContext& test_context, util::Logger& logger, const std::string& path_1,
                  const std::string& path_2, const util::Optional<std::array<char, 64>>& encryption_key,
                  sync::SaltedFileIdent client_file_ident, uint_fast64_t downloaded_bytes)
{
    DBOptions options{encryption_key ? encryption_key->data() : nullptr};
    std::unique_ptr<ClientReplication> history_1 = make_client_replication(path_1);
    DBRef sg_1 = DB::create(*history_1, options);
    std::unique_ptr<ClientReplication> history_2 = make_client_replication(path_2);
    DBRef sg_2 = DB::create(*history_2, options);

    // Check client_file_ident.
    {
        version_type current_client_version;
        SaltedFileIdent client_file_ident_2;
        SyncProgress progress;
        history_2->get_status(current_client_version, client_file_ident_2, progress);

        CHECK_EQUAL(client_file_ident_2.ident, client_file_ident.ident);
        CHECK_EQUAL(client_file_ident_2.salt, client_file_ident.salt);
    }

    // Check downloaded bytes.
    {
        uint_fast64_t downloaded_bytes_2;
        uint_fast64_t downloadable_bytes;
        uint_fast64_t uploaded_bytes;
        uint_fast64_t uploadable_bytes_2;
        uint_fast64_t snapshot_version;
        history_2->get_upload_download_bytes(downloaded_bytes_2, downloadable_bytes, uploaded_bytes,
                                             uploadable_bytes_2, snapshot_version);
        CHECK_EQUAL(downloaded_bytes_2, downloaded_bytes);
        CHECK_EQUAL(downloadable_bytes, 0);
        CHECK_EQUAL(uploaded_bytes, 0);
    }

    // Check state equality.
    {
        ReadTransaction rt_1(sg_1);
        ReadTransaction rt_2(sg_2);
        CHECK(compare_groups(rt_1, rt_2, logger));
    }

    // Verify the history.
    {
        ReadTransaction rt_2(sg_2);
        const Group& group = rt_2.get_group();
        group.verify();
    }
}

TEST(ClientResetDiff_TransferGroup)
{
    // This tests checks the function client_reset::transfer_group().
    SHARED_GROUP_TEST_PATH(path_src);
    SHARED_GROUP_TEST_PATH(path_dst);

    util::Logger& logger = test_context.logger;

    // Populate the source Realm.
    {
        std::unique_ptr<ClientReplication> history = make_client_replication(path_src);
        DBRef sg = DB::create(*history);
        WriteTransaction wt{sg};

        {
            TableRef table = create_table(wt, "class_table_1");
            table->add_column(col_type_Int, "integer");
            table->add_column(col_type_String, "string");
            sync::create_object(wt, *table).set_all(456, "abc");
        }

        {
            TableRef table = create_table_with_primary_key(wt, "class_table_2", type_Int, "pk_int");
            auto col_ndx_0 = table->add_column(col_type_Bool, "bool");
            sync::create_object_with_primary_key(wt, *table, 111).set(col_ndx_0, true);
        }

        {
            TableRef table = create_table_with_primary_key(wt, "class_table_5", type_Int, "pk_int");
            {
                auto col_ndx = table->add_column_list(col_type_Int, "array_int");
                auto list = sync::create_object_with_primary_key(wt, *table, 666).get_list<Int>(col_ndx);
                list.add(10);
                list.add(11);
                list.add(12);
                list.add(13);
                list.add(14);
            }
        }

        {
            TableRef table = create_table_with_primary_key(wt, "class_table_6", type_String, "pk_string");
            auto col_ndx = table->add_column_list(*table, "target<ObjKey>");
            table->add_column(col_type_Bool, "something");

            Obj obj_a = sync::create_object_with_primary_key(wt, *table, "aaa");
            Obj obj_b = sync::create_object_with_primary_key(wt, *table, "bbb");
            Obj obj_c = sync::create_object_with_primary_key(wt, *table, "ccc");
            Obj obj_d = sync::create_object_with_primary_key(wt, *table, "ddd");
            Obj obj_e = sync::create_object_with_primary_key(wt, *table, "eee");
            Obj obj_f = sync::create_object_with_primary_key(wt, *table, "fff");

            auto ll = obj_b.get_linklist(col_ndx);
            ll.add(obj_a.get_key());
            ll.add(obj_b.get_key());
            ll.add(obj_c.get_key());
            ll.add(obj_d.get_key());
            ll.add(obj_e.get_key());
            ll.add(obj_f.get_key());
        }

        wt.commit();
    }

    // Populate the destination Realm.
    {
        std::unique_ptr<ClientReplication> history = make_client_replication(path_dst);
        DBRef sg = DB::create(*history);
        WriteTransaction wt{sg};
        Group& group = wt.get_group();

        {
            TableRef table = create_table(wt, "class_table_0");
            table->add_column(col_type_Int, "integer");
            sync::create_object(wt, *table).set_all(123);
        }

        {
            TableRef table = create_table(wt, "class_table_1");
            table->add_column(col_type_Int, "integer");
            table->add_column(col_type_String, "string");
            sync::create_object(wt, *table).set_all(123, "def");
        }

        {
            TableRef table = create_table(wt, "class_table_2");
            table->add_column(col_type_Int, "integer");
            sync::create_object(wt, *table).set_all(123);
        }

        create_table_with_primary_key(wt, "class_table_3", type_Int, "pk_int");
        create_table_with_primary_key(wt, "class_table_4", type_String, "pk_string");
        {
            TableRef table_3 = group.get_table("class_table_3");
            TableRef table_4 = group.get_table("class_table_4");
            auto col_3 = table_3->add_column_list(*table_4, "target_link3");
            auto col_4 = table_4->add_column_list(*table_3, "target_link4");
            auto col_4a = table_4->add_column_list(*table_4, "target_link4a");

            Obj obj_3 = sync::create_object_with_primary_key(wt, *table_3, 111);
            Obj obj_4 = sync::create_object_with_primary_key(wt, *table_4, StringData{"abc"});
            auto ll_3 = obj_3.get_linklist(col_3);
            ll_3.insert(0, obj_4.get_key());
            ll_3.insert(1, obj_4.get_key());

            auto ll_4 = obj_4.get_linklist(col_4);
            ll_4.insert(0, obj_3.get_key());

            auto ll_4a = obj_4.get_linklist(col_4a);
            ll_4a.insert(0, obj_4.get_key());
        }

        {
            TableRef table = create_table_with_primary_key(wt, "class_table_5", type_Int, "pk_int");
            {
                auto col_ndx = table->add_column_list(col_type_Int, "array_int");
                auto array = sync::create_object_with_primary_key(wt, *table, 666).get_list<Int>(col_ndx);
                array.add(10);
                array.add(8888);
                array.add(8888);
                array.add(12);
                array.add(13);
                array.add(14);
            }
        }

        {
            TableRef table = create_table_with_primary_key(wt, "class_table_6", type_String, "pk_string");
            table->add_column(col_type_Int, "something");
            auto col_ndx = table->add_column_list(*table, "target_link");

            // Opposite order such that the row indices are different.
            Obj obj_f = sync::create_object_with_primary_key(wt, *table, "fff");
            Obj obj_e = sync::create_object_with_primary_key(wt, *table, "eee");
            Obj obj_d = sync::create_object_with_primary_key(wt, *table, "ddd");
            sync::create_object_with_primary_key(wt, *table, "ccc");
            Obj obj_b = sync::create_object_with_primary_key(wt, *table, "bbb");
            Obj obj_a = sync::create_object_with_primary_key(wt, *table, "aaa");

            auto ll = obj_b.get_linklist(col_ndx);
            ll.add(obj_a.get_key());
            ll.add(obj_b.get_key());
            ll.add(obj_a.get_key());
            ll.add(obj_d.get_key());
            ll.add(obj_e.get_key());
            ll.add(obj_f.get_key());
        }

        wt.commit();
    }

    {
        std::unique_ptr<ClientReplication> history_src = make_client_replication(path_src);
        DBRef sg_src = DB::create(*history_src);
        ReadTransaction rt{sg_src};
        TableInfoCache table_info_cache_src{rt};

        std::unique_ptr<ClientReplication> history_dst = make_client_replication(path_dst);
        DBRef sg_dst = DB::create(*history_dst);
        WriteTransaction wt{sg_dst};
        TableInfoCache table_info_cache_dst{wt};

        transfer_group(rt, table_info_cache_src, wt, table_info_cache_dst, logger);

        wt.commit();
    }

    {
        std::unique_ptr<ClientReplication> history_src = make_client_replication(path_src);
        DBRef sg_src = DB::create(*history_src);
        ReadTransaction rt_src{sg_src};

        std::unique_ptr<ClientReplication> history_dst = make_client_replication(path_dst);
        DBRef sg_dst = DB::create(*history_dst);
        ReadTransaction rt_dst{sg_dst};

        CHECK(compare_groups(rt_src, rt_dst, logger));
    }
}

TEST(ClientResetDiff_1)
{
    SHARED_GROUP_TEST_PATH(path_1); // The remote
    SHARED_GROUP_TEST_PATH(path_2); // The local

    util::Logger& logger = test_context.logger;
    SaltedFileIdent client_file_ident = {123, 456}; // Anything.
    sync::SaltedVersion server_version{1, 1234};
    uint_fast64_t downloaded_bytes = 98765; // Anything.
    version_type client_version = 0;


    {
        std::unique_ptr<ClientReplication> history = make_client_replication(path_1);
        DBRef sg = DB::create(*history);
        WriteTransaction wt{sg};
        {
            TableRef table = create_table(wt, "class_table_0");
            auto col_ndx_0 = table->add_column(col_type_Int, "integer");
            auto col_ndx_1 = table->add_column(col_type_Bool, "bool");
            auto col_ndx_2 = table->add_column_list(col_type_String, "array_string");
            auto col_ndx_3 = table->add_column_list(col_type_Double, "array_double");
            auto col_ndx_4 = table->add_column(col_type_Float, "float");
            auto col_ndx_5 = table->add_column(col_type_Timestamp, "timestamp");
            auto col_ndx_6 = table->add_column_list(col_type_Int, "array_integer", true);

            Obj obj_0 = sync::create_object(wt, *table);
            Obj obj_1 = sync::create_object(wt, *table);

            obj_0.set(col_ndx_0, 123);
            obj_0.set(col_ndx_1, true);
            obj_0.get_list<String>(col_ndx_2).add("Hello");

            auto array_double = obj_0.get_list<double>(col_ndx_3);
            array_double.add(1234.5678);
            array_double.add(-0.01);

            obj_0.set(col_ndx_4, -34.56f);

            Timestamp timestamp{1234, 5678};
            obj_1.set(col_ndx_5, timestamp);

            auto array_int = obj_1.get_list<util::Optional<Int>>(col_ndx_6);
            for (int i = 0; i < 5; ++i) {
                array_int.insert_null(i);
            }
            for (int i = 0; i < 20; ++i) {
                array_int.add(i);
            }
        }

        create_table_with_primary_key(wt, "class_table_1", type_Int, "pk_int");
        create_table_with_primary_key(wt, "class_table_2", type_String, "pk_string");
        wt.commit();
    }

    bool recover_local_changes = false;
    bool should_commit_remote = true;
    perform_client_reset_diff(path_1, path_2, encryption_key_none, client_file_ident, server_version,
                              downloaded_bytes, client_version, recover_local_changes, logger, should_commit_remote);

    check_common(test_context, logger, path_1, path_2, encryption_key_none, client_file_ident, downloaded_bytes);
}

TEST(ClientResetDiff_2)
{
    SHARED_GROUP_TEST_PATH(path_1); // The remote
    SHARED_GROUP_TEST_PATH(path_2); // The local

    util::Logger& logger = test_context.logger;
    SaltedFileIdent client_file_ident = {123, 456}; // Anything.
    sync::SaltedVersion server_version{1, 1234};
    uint_fast64_t downloaded_bytes = 98765; // Anything.
    version_type client_version = 0;

    // The remote.
    {
        _impl::ClientHistoryImpl history{path_1};
        DBRef sg = DB::create(history);
        WriteTransaction wt{sg};

        TableRef table = create_table_with_primary_key(wt, "class_table_0", type_String, "pk_string");
        auto col_ndx = table->add_column(col_type_Int, "int", true);
        sync::create_object_with_primary_key(wt, *table, "aaa").set(col_ndx, 1);
        sync::create_object_with_primary_key(wt, *table, "bbb").set(col_ndx, 2);

        wt.commit();
    }

    // The local.
    {
        _impl::ClientHistoryImpl history{path_2};
        DBRef sg = DB::create(history);
        WriteTransaction wt{sg};

        TableRef table = create_table_with_primary_key(wt, "class_table_0", type_String, "pk_string");
        auto col_ndx = table->add_column(col_type_Int, "int", true);
        auto obj_a = sync::create_object_with_primary_key(wt, *table, "aaa").set<util::Optional<Int>>(col_ndx, 6);
        sync::create_object_with_primary_key(wt, *table, "ccc").set<util::Optional<Int>>(col_ndx, 3);
        sync::create_object_with_primary_key(wt, *table, "ddd").set_null(col_ndx);
        obj_a.add_int(col_ndx, 4);

        wt.commit();
    }

    bool recover_local_changes = true;
    bool should_commit_remote = true;
    perform_client_reset_diff(path_1, path_2, encryption_key_none, client_file_ident, server_version,
                              downloaded_bytes, client_version, recover_local_changes, logger, should_commit_remote);

    check_common(test_context, logger, path_1, path_2, encryption_key_none, client_file_ident, downloaded_bytes);

    // Check the content.
    {
        _impl::ClientHistoryImpl history{path_2};
        DBRef sg = DB::create(history);
        {
            ReadTransaction rt{sg};
            TableInfoCache table_info_cache{rt};
            const Group& group = rt.get_group();

            ConstTableRef table = group.get_table("class_table_0");
            CHECK(table);
            auto col_ndx = table->get_column_key("int");
            CHECK(col_ndx);
            CHECK_EQUAL(table->size(), 4);

            auto get_val = [&](StringData pk) -> int_fast64_t {
                GlobalKey oid(pk);
                int_fast64_t val =
                    *sync::obj_for_object_id(table_info_cache, *table, oid).get<util::Optional<Int>>(col_ndx);
                return val;
            };

            CHECK_EQUAL(get_val("aaa"), 6 + 4);
            CHECK_EQUAL(get_val("bbb"), 2);
            CHECK_EQUAL(get_val("ccc"), 3);
            GlobalKey oid("ddd");
            const Obj obj = obj_for_object_id(table_info_cache, *table, oid);
            CHECK(obj.is_null(col_ndx));
        }

        {
            version_type current_client_version;
            SaltedFileIdent client_file_ident_2;
            SyncProgress progress;
            history.get_status(current_client_version, client_file_ident_2, progress);
            CHECK_EQUAL(current_client_version, 3);
            CHECK_EQUAL(client_file_ident.ident, client_file_ident_2.ident);
            CHECK_EQUAL(client_file_ident.salt, client_file_ident_2.salt);
            CHECK_EQUAL(progress.latest_server_version.version, server_version.version);
            CHECK_EQUAL(progress.latest_server_version.salt, server_version.salt);
            CHECK_EQUAL(progress.download.server_version, server_version.version);
            CHECK_EQUAL(progress.download.last_integrated_client_version, client_version);
            CHECK_EQUAL(progress.upload.client_version, 0);
            CHECK_EQUAL(progress.upload.last_integrated_server_version, 0);
        }

        uint_fast64_t uploadable_bytes;
        {
            uint_fast64_t downloaded_bytes;
            uint_fast64_t downloadable_bytes;
            uint_fast64_t uploaded_bytes;
            uint_fast64_t snapshot_version;
            history.get_upload_download_bytes(downloaded_bytes, downloadable_bytes, uploaded_bytes, uploadable_bytes,
                                              snapshot_version);
            CHECK_EQUAL(downloaded_bytes, downloaded_bytes);
            CHECK_EQUAL(downloadable_bytes, 0);
            CHECK_EQUAL(uploaded_bytes, 0);
            CHECK_NOT_EQUAL(uploadable_bytes, 0);
            CHECK_EQUAL(snapshot_version, 3);
        }

        {
            UploadCursor upload_progress{2, server_version.version};
            version_type end_version = 3;
            std::vector<sync::ClientReplication::UploadChangeset> changesets;
            version_type locked_server_version; // Dummy
            history.find_uploadable_changesets(upload_progress, end_version, changesets, locked_server_version);

            CHECK_EQUAL(upload_progress.client_version, 3);
            CHECK_EQUAL(upload_progress.last_integrated_server_version, server_version.version);
            CHECK_EQUAL(changesets.size(), 1);
            const sync::ClientReplication::UploadChangeset& changeset = changesets[0];
            CHECK_EQUAL(changeset.origin_file_ident, 0);
            CHECK_EQUAL(changeset.progress.client_version, 3);
            CHECK_EQUAL(changeset.progress.last_integrated_server_version, server_version.version);
            CHECK_EQUAL(changeset.changeset.size(), uploadable_bytes);
        }

        {
            ReadTransaction rt{sg};
            version_type current_version = rt.get_version();

            {
                version_type begin_version = 1;
                Optional<_impl::ClientHistoryImpl::LocalChangeset> lc =
                    history.get_next_local_changeset(current_version, begin_version);
                CHECK(lc);
                CHECK_EQUAL(lc->version, 2);
                CHECK_EQUAL(lc->changeset.size(), uploadable_bytes);
            }

            {
                version_type begin_version = 2;
                Optional<_impl::ClientHistoryImpl::LocalChangeset> lc =
                    history.get_next_local_changeset(current_version, begin_version);
                CHECK(lc);
                CHECK_EQUAL(lc->version, 2);
                CHECK_EQUAL(lc->changeset.size(), uploadable_bytes);
            }

            {
                version_type begin_version = 3;
                Optional<_impl::ClientHistoryImpl::LocalChangeset> lc =
                    history.get_next_local_changeset(current_version, begin_version);
                CHECK(!lc);
            }
        }
    }
}

TEST(ClientResetDiff_FailedLocalRecovery)
{
    SHARED_GROUP_TEST_PATH(path_1); // The remote
    SHARED_GROUP_TEST_PATH(path_2); // The local

    util::Logger& logger = test_context.logger;
    SaltedFileIdent client_file_ident = {123, 456}; // Anything.
    sync::SaltedVersion server_version{1, 1234};
    uint_fast64_t downloaded_bytes = 98765; // Anything.
    version_type client_version = 0;

    // The remote.
    {
        std::unique_ptr<ClientReplication> history = make_client_replication(path_1);
        DBRef sg = DB::create(*history);
        WriteTransaction wt{sg};

        TableRef table_0 = create_table_with_primary_key(wt, "class_table_0", type_String, "pk_string");
        TableRef table_1 = create_table_with_primary_key(wt, "class_table_1", type_Int, "pk_int");

        table_0->add_column_list(*table_1, "linklist");

        wt.commit();
    }

    // The local.
    {
        std::unique_ptr<ClientReplication> history = make_client_replication(path_2);
        DBRef sg = DB::create(*history);
        WriteTransaction wt{sg};

        TableRef table_0 = create_table_with_primary_key(wt, "class_table_0", type_String, "pk_string");

        TableRef table_2 = create_table(wt, "class_table_2");
        table_2->add_column(col_type_Int, "int");
        CHECK_EQUAL(table_2->get_column_count(), 1);

        TableRef table_3 = create_table_with_primary_key(wt, "class_table_3", type_String, "pk_string");
        table_3->add_column(*table_0, "links");
        table_3->add_column_list(col_type_Int, "array_int");

        // The target table differs for the same column in remote and local.
        table_0->add_column_list(*table_2, "linklist");

        create_object_with_primary_key(wt, *table_0, "aaa");
        CHECK_EQUAL(table_0->size(), 1);

        wt.commit();
    }

    bool recover_local_changes = true;
    bool should_commit_remote = true;
    perform_client_reset_diff(path_1, path_2, encryption_key_none, client_file_ident, server_version,
                              downloaded_bytes, client_version, recover_local_changes, logger, should_commit_remote);

    check_common(test_context, logger, path_1, path_2, encryption_key_none, client_file_ident, downloaded_bytes);

    // Check the content.
    {
        std::unique_ptr<ClientReplication> history = make_client_replication(path_2);
        DBRef sg = DB::create(*history);
        {
            ReadTransaction rt{sg};
            TableInfoCache table_info_cache{rt};
            const Group& group = rt.get_group();

            CHECK_EQUAL(group.size(), 4);

            ConstTableRef table_0 = group.get_table("class_table_0");
            CHECK(table_0);
            CHECK_EQUAL(table_0->size(), 0);
            ConstTableRef table_1 = group.get_table("class_table_1");
            CHECK(table_1);
            ConstTableRef table_2 = group.get_table("class_table_2");
            CHECK(table_2);
            CHECK_EQUAL(table_2->get_column_count(), 1);
            auto col_ndx = table_2->get_column_key("int");
            DataType col_type = table_2->get_column_type(col_ndx);
            CHECK_EQUAL(col_type, type_Int);
            ConstTableRef table_3 = group.get_table("class_table_3");
            CHECK(table_3);
            CHECK_EQUAL(table_3->get_column_count(), 3);
            col_ndx = table_3->get_column_key("links");
            col_type = table_3->get_column_type(col_ndx);
            CHECK_EQUAL(col_type, type_Link);
            col_ndx = table_3->get_column_key("array_int");
            col_type = table_3->get_column_type(col_ndx);
            CHECK_EQUAL(col_type, type_Int);
            CHECK(table_3->is_list(col_ndx));
        }
    }
}

TEST(ClientResetDiff_ClientVersion)
{
    SHARED_GROUP_TEST_PATH(path_1); // The remote
    SHARED_GROUP_TEST_PATH(path_2); // The local

    util::Logger& logger = test_context.logger;
    SaltedFileIdent client_file_ident = {123, 456}; // Anything.
    sync::SaltedVersion server_version{1, 1234};
    uint_fast64_t downloaded_bytes = 98765; // Anything.

    auto create_schema_and_objects = [&](Transaction& wt) {
        TableRef table = create_table_with_primary_key(wt, "class_table", type_String, "pk_string");
        auto col_int = table->add_column(col_type_Int, "int");
        auto col_ll = table->add_column_list(*table, "linklist");
        table->add_column_list(col_type_String, "array");

        Obj obj_a = create_object_with_primary_key(wt, *table, "aaa").set(col_int, 100);
        Obj obj_b = create_object_with_primary_key(wt, *table, "bbb").set(col_int, 200);

        auto ll = obj_a.get_linklist(col_ll);
        ll.add(obj_a.get_key());
        ll.add(obj_b.get_key());
    };

    // The remote.
    {
        std::unique_ptr<ClientReplication> history = make_client_replication(path_1);
        DBRef sg = DB::create(*history);
        WriteTransaction wt{sg};

        create_schema_and_objects(wt);

        TableRef table = wt.get_table("class_table");
        CHECK(table);

        auto col_list = table->get_column_key("array");
        GlobalKey oid_a("aaa");
        auto array = obj_for_object_id(wt, *table, oid_a).get_list<String>(col_list);
        array.add("A");

        wt.commit();
    }

    // The local.
    {
        std::unique_ptr<ClientReplication> history = make_client_replication(path_2);
        DBRef sg = DB::create(*history);
        ObjKeys obj_keys;

        {
            WriteTransaction wt{sg};

            create_schema_and_objects(wt);
            version_type version = wt.commit();
            CHECK_EQUAL(version, 2);
        }

        {
            WriteTransaction wt{sg};

            TableRef table = wt.get_table("class_table");
            CHECK(table);
            auto col_pk = table->get_column_key("pk_string");
            auto col_int = table->get_column_key("int");
            auto col_ll = table->get_column_key("linklist");
            auto col_list = table->get_column_key("array");

            obj_keys.push_back(table->find_first_string(col_pk, "aaa"));
            obj_keys.push_back(table->find_first_string(col_pk, "bbb"));
            obj_keys.push_back(create_object_with_primary_key(wt, *table, "ccc").get_key());

            auto obj0 = table->get_object(obj_keys[0]);
            obj0.set(col_int, 300);

            obj0.get_linklist(col_ll).add(obj_keys[0]);

            auto array = obj0.get_list<String>(col_list);
            array.add("B");
            array.add("C");

            version_type version = wt.commit();
            CHECK_EQUAL(version, 3);
        }

        {
            WriteTransaction wt{sg};
            Group& group = wt.get_group();

            TableRef table = group.get_table("class_table");
            CHECK(table);
            auto col_int = table->get_column_key("int");
            auto col_ll = table->get_column_key("linklist");
            auto col_list = table->get_column_key("array");

            auto obj0 = table->get_object(obj_keys[0]);
            auto obj1 = table->get_object(obj_keys[1]);

            obj1.set(col_int, 400);

            auto ll = obj1.get_linklist(col_ll);
            ll.add(obj_keys[1]);
            ll.add(obj_keys[0]);
            ll.add(obj_keys[1]);
            ll.add(obj_keys[0]);

            auto array = obj0.get_list<String>(col_list);
            array.insert(1, "D");
            array.add("E");

            version_type version = wt.commit();
            CHECK_EQUAL(version, 4);
        }

        {
            WriteTransaction wt{sg};
            Group& group = wt.get_group();

            TableRef table = group.get_table("class_table");
            CHECK(table);
            auto col_int = table->get_column_key("int");
            auto col_ll = table->get_column_key("linklist");
            auto col_list = table->get_column_key("array");

            auto obj0 = table->get_object(obj_keys[0]);
            auto obj1 = table->get_object(obj_keys[1]);
            auto obj2 = table->get_object(obj_keys[2]);

            obj0.set(col_int, 500);
            obj2.set(col_int, 600);

            {
                auto ll = obj1.get_linklist(col_ll);
                CHECK_EQUAL(ll.size(), 4);
                ll.set(0, obj_keys[2]);
            }

            {
                auto ll = obj2.get_linklist(col_ll);
                ll.add(obj_keys[1]);
            }

            auto array = obj0.get_list<String>(col_list);
            array.remove(3);
            array.remove(0);
            CHECK_EQUAL(array.size(), 2);

            version_type version = wt.commit();
            CHECK_EQUAL(version, 5);
        }
    }

    bool recover_local_changes = true;
    bool should_commit_remote = true;
    // The first two local changesets are known by the remote.
    version_type client_version = 2;
    perform_client_reset_diff(path_1, path_2, encryption_key_none, client_file_ident, server_version,
                              downloaded_bytes, client_version, recover_local_changes, logger, should_commit_remote);

    check_common(test_context, logger, path_1, path_2, encryption_key_none, client_file_ident, downloaded_bytes);

    // Check the content.
    {
        std::unique_ptr<ClientReplication> history = make_client_replication(path_2);
        DBRef sg = DB::create(*history);
        {
            ReadTransaction rt{sg};
            TableInfoCache table_info_cache{rt};

            CHECK_EQUAL(rt.get_group().size(), 1);

            ConstTableRef table = rt.get_table("class_table");

            CHECK_EQUAL(table->get_column_count(), 4);
            CHECK_EQUAL(table->size(), 2);

            auto col_int = table->get_column_key("int");
            auto col_ll = table->get_column_key("linklist");
            auto col_list = table->get_column_key("array");

            GlobalKey oid_a("aaa");
            GlobalKey oid_b("bbb");
            const Obj obj_a = obj_for_object_id(table_info_cache, *table, oid_a);
            const Obj obj_b = obj_for_object_id(table_info_cache, *table, oid_b);

            CHECK_EQUAL(obj_a.get<Int>(col_int), 500);
            CHECK_EQUAL(obj_b.get<Int>(col_int), 400);

            {
                auto ll = obj_a.get_linklist(col_ll);
                CHECK_EQUAL(ll.size(), 2);
                CHECK_EQUAL(ll.get(0), obj_a.get_key());
                CHECK_EQUAL(ll.get(1), obj_b.get_key());
            }

            {
                auto ll = obj_b.get_linklist(col_ll);
                CHECK_EQUAL(ll.size(), 4);
                CHECK_EQUAL(ll.get(0), obj_b.get_key());
                CHECK_EQUAL(ll.get(1), obj_a.get_key());
                CHECK_EQUAL(ll.get(0), obj_b.get_key());
                CHECK_EQUAL(ll.get(1), obj_a.get_key());
            }

            {
                auto array = obj_a.get_list<String>(col_list);
                CHECK_EQUAL(array.size(), 1);
                CHECK_EQUAL(array.get(0), "D");
            }
        }
    }
}

TEST(ClientResetDiff_PrimitiveArrays)
{
    SHARED_GROUP_TEST_PATH(path_1); // The remote
    SHARED_GROUP_TEST_PATH(path_2); // The local

    util::Logger& logger = test_context.logger;
    SaltedFileIdent client_file_ident = {123, 456}; // Anything.
    sync::SaltedVersion server_version{10, 1234};
    uint_fast64_t downloaded_bytes = 987654; // Anything.

    auto create_schema = [&](Transaction& wt) {
        TableRef table = create_table_with_primary_key(wt, "class_table", type_String, "pk_string");

        table->add_column_list(col_type_Int, "array_int");
        table->add_column_list(col_type_String, "array_string", true);

        sync::create_object_with_primary_key(wt, *table, "abc");
    };

    // The remote.
    {
        std::unique_ptr<ClientReplication> history = make_client_replication(path_1);
        DBRef sg = DB::create(*history);
        WriteTransaction wt{sg};

        create_schema(wt);

        TableRef table = wt.get_table("class_table");
        CHECK(table);

        {
            auto array = table->begin()->get_list<Int>(table->get_column_key("array_int"));
            array.add(11);
            array.add(12);
            array.add(13);
            array.add(14);
        }

        {
            auto array = table->begin()->get_list<String>(table->get_column_key("array_string"));
            array.add("11");
            array.add("12");
            array.add("13");
            array.add("14");
        }

        wt.commit();
    }

    // The local.
    {
        std::unique_ptr<ClientReplication> history = make_client_replication(path_2);
        DBRef sg = DB::create(*history);

        {
            WriteTransaction wt{sg};

            create_schema(wt);

            TableRef table = wt.get_table("class_table");
            CHECK(table);

            {
                auto array = table->begin()->get_list<Int>(table->get_column_key("array_int"));
                array.add(15);
                array.add(11);
                array.add(12);
                array.add(16);
            }

            {
                auto array = table->begin()->get_list<String>(table->get_column_key("array_string"));
                array.add("15");
                array.add("11");
                array.add("12");
                array.add("16");
            }

            version_type version = wt.commit();
            CHECK_EQUAL(version, 2);
        }

        {
            WriteTransaction wt{sg};
            Group& group = wt.get_group();

            TableRef table = group.get_table("class_table");
            CHECK(table);

            {
                auto array = table->begin()->get_list<Int>(table->get_column_key("array_int"));
                array.insert(1, 17);
                array.insert(1, 13);
                array.insert(5, 18);
                array.set(0, 13);
                array.move(5, 3);
            }

            {
                auto array = table->begin()->get_list<String>(table->get_column_key("array_string"));
                array.insert(1, "17");
                array.insert(1, "13");
                array.insert(5, "18");
                array.set_null(0);
            }

            version_type version = wt.commit();
            CHECK_EQUAL(version, 3);
        }
    }

    bool recover_local_changes = true;
    bool should_commit_remote = true;
    version_type client_version = 1;

    perform_client_reset_diff(path_1, path_2, encryption_key_none, client_file_ident, server_version,
                              downloaded_bytes, client_version, recover_local_changes, logger, should_commit_remote);

    check_common(test_context, logger, path_1, path_2, encryption_key_none, client_file_ident, downloaded_bytes);

    // Check the content.
    {
        std::unique_ptr<ClientReplication> history = make_client_replication(path_2);
        DBRef sg = DB::create(*history);
        {
            ReadTransaction rt{sg};
            TableInfoCache table_info_cache{rt};
            const Group& group = rt.get_group();

            CHECK_EQUAL(group.size(), 1);

            ConstTableRef table = group.get_table("class_table");
            CHECK(table);

            {
                auto array = table->begin()->get_list<Int>(table->get_column_key("array_int"));
                CHECK_EQUAL(array.size(), 7);
                CHECK_EQUAL(array.get(0), 13);
                CHECK_EQUAL(array.get(1), 13);
                CHECK_EQUAL(array.get(2), 17);
                CHECK_EQUAL(array.get(3), 18);
                CHECK_EQUAL(array.get(4), 12);
                CHECK_EQUAL(array.get(5), 13);
                CHECK_EQUAL(array.get(6), 14);
            }

            {
                auto array = table->begin()->get_list<String>(table->get_column_key("array_string"));
                CHECK_EQUAL(array.size(), 7);
                CHECK_EQUAL(array.get(0), StringData{});
                CHECK_EQUAL(array.get(1), "13");
                CHECK_EQUAL(array.get(2), "17");
                CHECK_EQUAL(array.get(3), "12");
                CHECK_EQUAL(array.get(4), "13");
                CHECK_EQUAL(array.get(5), "18");
                CHECK_EQUAL(array.get(6), "14");
            }
        }
    }
}

TEST(ClientResetDiff_NonSyncTables)
{
    SHARED_GROUP_TEST_PATH(path_1); // The remote
    SHARED_GROUP_TEST_PATH(path_2); // The local

    util::Logger& logger = test_context.logger;
    SaltedFileIdent client_file_ident = {123, 456}; // Anything.
    sync::SaltedVersion server_version{10, 1234};
    uint_fast64_t downloaded_bytes = 987654; // Anything.

    // The remote.
    {
        std::unique_ptr<ClientReplication> history = make_client_replication(path_1);
        DBRef sg = DB::create(*history);
        WriteTransaction wt{sg};

        TableRef table = create_table_with_primary_key(wt, "class_table", type_String, "pk_string");
        auto col_ndx = table->add_column(*table, "link");

        Obj obj_a = sync::create_object_with_primary_key(wt, *table, "aaa");
        Obj obj_b = sync::create_object_with_primary_key(wt, *table, "bbb");
        sync::create_object_with_primary_key(wt, *table, "ccc");

        obj_a.set(col_ndx, obj_b.get_key());
        obj_b.set(col_ndx, obj_a.get_key());

        wt.commit();
    }

    // The local
    {
        std::unique_ptr<ClientReplication> history = make_client_replication(path_2);
        DBRef sg = DB::create(*history);
        WriteTransaction wt{sg};
        Group& group = wt.get_group();

        TableRef table = group.add_table("nonsync_table");
        table->add_column(col_type_Int, "integer");
        table->create_object();
        table->create_object();
        wt.commit();
    }

    bool recover_local_changes = true;
    bool should_commit_remote = true;
    version_type client_version = 0;

    perform_client_reset_diff(path_1, path_2, encryption_key_none, client_file_ident, server_version,
                              downloaded_bytes, client_version, recover_local_changes, logger, should_commit_remote);

    // Check the content.
    {
        std::unique_ptr<ClientReplication> history = make_client_replication(path_2);
        DBRef sg = DB::create(*history);
        {
            ReadTransaction rt{sg};
            TableInfoCache table_info_cache{rt};
            const Group& group = rt.get_group();

            CHECK_EQUAL(group.size(), 2);

            {
                ConstTableRef table = group.get_table("class_table");
                CHECK(table);
                CHECK_EQUAL(table->get_column_count(), 2);
                auto col_link = table->get_column_key("link");
                DataType col_type = table->get_column_type(col_link);
                CHECK_EQUAL(col_type, type_Link);
                CHECK_EQUAL(table->size(), 3);
                GlobalKey oid_a("aaa");
                GlobalKey oid_b("bbb");
                GlobalKey oid_c("ccc");
                const Obj obj_a = sync::obj_for_object_id(table_info_cache, *table, oid_a);
                const Obj obj_b = sync::obj_for_object_id(table_info_cache, *table, oid_b);
                const Obj obj_c = sync::obj_for_object_id(table_info_cache, *table, oid_c);

                CHECK_EQUAL(obj_a.get<ObjKey>(col_link), obj_b.get_key());
                CHECK_EQUAL(obj_b.get<ObjKey>(col_link), obj_a.get_key());
                CHECK(obj_c.is_null(col_link));
            }
            {
                ConstTableRef table = group.get_table("nonsync_table");
                CHECK(table);
                CHECK_EQUAL(table->get_column_count(), 1);
                DataType col_type = table->get_column_type(table->get_column_key("integer"));
                CHECK_EQUAL(col_type, type_Int);
                CHECK_EQUAL(table->size(), 2);
            }
        }
    }
}

TEST(ClientResetDiff_Links)
{
    SHARED_GROUP_TEST_PATH(path_1); // The remote
    SHARED_GROUP_TEST_PATH(path_2); // The local

    util::Logger& logger = test_context.logger;
    SaltedFileIdent remote_client_file_ident = {10, 100}; // Anything.
    SaltedFileIdent local_client_file_ident = {20, 200};  // Anything.
    SaltedFileIdent new_client_file_ident = {30, 300};    // Anything.
    sync::SaltedVersion server_version{10, 1234};
    uint_fast64_t downloaded_bytes = 98765; // Anything.

    // The remote.
    {
        _impl::ClientHistoryImpl history{path_1};
        DBRef sg = DB::create(history);
        WriteTransaction wt{sg};

        version_type current_version = wt.get_version();
        history.set_client_file_ident_in_wt(current_version, remote_client_file_ident);

        TableRef table_0 = create_table(wt, "class_table_0");
        TableRef table_1 = create_table_with_primary_key(wt, "class_table_1", type_String, "pk_string");
        TableRef table_2 = create_table_with_primary_key(wt, "class_table_2", type_Int, "pk_int");

        auto col_link_00 = table_0->add_column(*table_0, "link_0");
        auto col_link_01 = table_0->add_column(*table_1, "link_1");
        auto col_link_02 = table_0->add_column(*table_2, "link_2");
        auto col_str_0 = table_0->add_column(col_type_String, "string");

        auto col_link_10 = table_1->add_column(*table_0, "link_0");
        auto col_link_11 = table_1->add_column(*table_1, "link_1");
        auto col_link_12 = table_1->add_column(*table_2, "link_2");

        auto col_link_20 = table_2->add_column(*table_0, "link_0");
        auto col_link_21 = table_2->add_column(*table_1, "link_1");
        auto col_link_22 = table_2->add_column(*table_2, "link_2");

        Obj remote_0 = create_object(wt, *table_0).set(col_str_0, "remote_0");
        Obj remote_1 = create_object(wt, *table_0).set(col_str_0, "remote_1");
        Obj remote_2 = create_object(wt, *table_0).set(col_str_0, "remote_2");

        Obj aaa = create_object_with_primary_key(wt, *table_1, "aaa");
        Obj bbb = create_object_with_primary_key(wt, *table_1, "bbb");
        Obj ccc = create_object_with_primary_key(wt, *table_1, "ccc");

        Obj obj_51 = create_object_with_primary_key(wt, *table_2, 51);
        Obj obj_52 = create_object_with_primary_key(wt, *table_2, 52);
        Obj obj_53 = create_object_with_primary_key(wt, *table_2, 53);

        // Links in table_0.
        remote_0.set(col_link_00, remote_1.get_key());       // remote_0 -> remote_1
        remote_1.set(col_link_00, remote_2.get_key(), true); // remote_1 -> remote_2
        remote_0.set(col_link_01, bbb.get_key());            // remote_0 -> bbb
        remote_2.set(col_link_01, ccc.get_key());            // remote_2 -> ccc
        remote_0.set(col_link_02, obj_52.get_key());         // remote_0 -> 52
        remote_1.set(col_link_02, obj_52.get_key());         // remote_1 -> 52
        remote_2.set(col_link_02, obj_51.get_key());         // remote_2 -> 51

        // Links in table_1.

        aaa.set(col_link_10, remote_1.get_key());       // aaa -> remote_1
        ccc.set(col_link_10, remote_2.get_key(), true); // ccc -> remote_2
        aaa.set(col_link_11, bbb.get_key());            // aaa -> bbb
        bbb.set(col_link_11, aaa.get_key());            // bbb -> aaa
        ccc.set(col_link_11, aaa.get_key());            // ccc -> aaa
        aaa.set(col_link_12, obj_53.get_key());         // aaa -> 53
        bbb.set(col_link_12, obj_53.get_key());         // bbb -> 53

        // Links in table_2.
        obj_51.set(col_link_20, remote_1.get_key()); // 51 -> remote_1
        obj_51.set(col_link_21, bbb.get_key());      // 51 -> bbb
        obj_51.set(col_link_22, obj_52.get_key());   // 51 -> 52
        //            table_2->set_link(1, 2, 2, true);
        //            table_2->set_link(2, 0, 1, false);
        //            table_2->set_link(2, 1, 0, false);
        //            table_2->set_link(2, 2, 0, false);
        //            table_2->set_link(3, 0, 2, true);
        //            table_2->set_link(3, 1, 1, false);
        //            table_2->set_link(3, 2, 0, false);
        //
        wt.commit();
    }

    // The local.
    {
        _impl::ClientHistoryImpl history{path_2};
        DBRef sg = DB::create(history);
        WriteTransaction wt{sg};

        version_type current_version = wt.get_version();
        history.set_client_file_ident_in_wt(current_version, local_client_file_ident);

        // Same tables.
        TableRef table_0 = create_table(wt, "class_table_0");
        TableRef table_1 = create_table_with_primary_key(wt, "class_table_1", type_String, "pk_string");
        TableRef table_2 = create_table_with_primary_key(wt, "class_table_2", type_Int, "pk_int");

        // Same columns in different order.
        auto col_link_01 = table_0->add_column(*table_1, "link_1");
        auto col_link_00 = table_0->add_column(*table_0, "link_0");
        auto col_str_0 = table_0->add_column(col_type_String, "string");
        auto col_link_02 = table_0->add_column(*table_2, "link_2");

        auto col_link_11 = table_1->add_column(*table_1, "link_1");
        auto col_link_12 = table_1->add_column(*table_2, "link_2");
        auto col_link_10 = table_1->add_column(*table_0, "link_0");

        auto col_link_22 = table_2->add_column(*table_2, "link_2");
        auto col_link_21 = table_2->add_column(*table_1, "link_1");
        auto col_link_20 = table_2->add_column(*table_0, "link_0");

        // Objects.
        Obj local_0 = create_object(wt, *table_0).set(col_str_0, "local_0");
        Obj local_1 = create_object(wt, *table_0).set(col_str_0, "local_1");
        Obj local_2 = create_object(wt, *table_0).set(col_str_0, "local_2");

        // Primary key objects have overlap but also new objects.
        Obj ddd = sync::create_object_with_primary_key(wt, *table_1, "ddd");
        Obj aaa = sync::create_object_with_primary_key(wt, *table_1, "aaa");
        Obj bbb = sync::create_object_with_primary_key(wt, *table_1, "bbb");

        Obj obj_51 = sync::create_object_with_primary_key(wt, *table_2, 51);
        Obj obj_62 = sync::create_object_with_primary_key(wt, *table_2, 62);
        Obj obj_63 = sync::create_object_with_primary_key(wt, *table_2, 63);

        // Links in table_0.
        local_0.set(col_link_01, aaa.get_key());     // local_0 -> aaa
        local_0.set(col_link_00, local_1.get_key()); // local_0 -> local_1
        local_2.set(col_link_00, local_2.get_key()); // local_2 -> local_2
        local_1.set_null(col_link_02);

        // Links in table_1.
        ddd.set(col_link_11, aaa.get_key());     // ddd -> aaa
        ddd.set(col_link_10, local_0.get_key()); // ddd -> local_0
        aaa.set(col_link_12, obj_63.get_key());  // aaa -> 63
        bbb.set(col_link_10, local_1.get_key()); // bbb -> local_1

        // Links in table_2.
        obj_51.set(col_link_20, local_2.get_key()); // 51 -> local_2
        obj_51.set(col_link_22, obj_63.get_key());  // 51 -> 63
        obj_62.set(col_link_21, aaa.get_key());     // 62 -> aaa
        obj_63.set(col_link_22, obj_51.get_key());  // 63 -> 51

        wt.commit();
    }

    bool recover_local_changes = true;
    bool should_commit_remote = true;
    version_type client_version = 0;
    perform_client_reset_diff(path_1, path_2, encryption_key_none, new_client_file_ident, server_version,
                              downloaded_bytes, client_version, recover_local_changes, logger, should_commit_remote);

    check_common(test_context, logger, path_1, path_2, encryption_key_none, new_client_file_ident, downloaded_bytes);

    // Check the content.
    {
        _impl::ClientHistoryImpl history{path_2};
        DBRef sg = DB::create(history);
        {
            ReadTransaction rt{sg};
            TableInfoCache table_info_cache{rt};
            const Group& group = rt.get_group();
            TableInfoCache table_info_cache_src{rt};

            ConstTableRef table_0 = group.get_table("class_table_0");
            CHECK(table_0);
            CHECK_EQUAL(table_0->get_column_count(), 4);
            auto col_ndx_0_0 = table_0->get_column_key("link_0");
            CHECK(col_ndx_0_0);
            auto col_ndx_0_1 = table_0->get_column_key("link_1");
            CHECK(col_ndx_0_1);
            auto col_ndx_0_2 = table_0->get_column_key("link_2");
            CHECK(col_ndx_0_2);
            auto col_ndx_0_str = table_0->get_column_key("string");
            CHECK(col_ndx_0_str);
            CHECK_EQUAL(table_0->size(), 6);


            ConstTableRef table_1 = group.get_table("class_table_1");
            CHECK(table_1);
            const TableInfoCache::TableInfo& table_info_1 = table_info_cache.get_table_info(*table_1);
            ColKey pk_ndx_1 = table_info_1.primary_key_col;
            CHECK_EQUAL(table_1->get_column_count(), 4);
            auto col_ndx_1_0 = table_1->get_column_key("link_0");
            CHECK(col_ndx_1_0);
            auto col_ndx_1_1 = table_1->get_column_key("link_1");
            CHECK(col_ndx_1_1);
            auto col_ndx_1_2 = table_1->get_column_key("link_2");
            CHECK(col_ndx_1_2);
            CHECK_EQUAL(table_1->size(), 4);

            ConstTableRef table_2 = group.get_table("class_table_2");
            CHECK(table_2);
            const TableInfoCache::TableInfo& table_info_2 = table_info_cache.get_table_info(*table_2);
            ColKey pk_ndx_2 = table_info_2.primary_key_col;
            CHECK_EQUAL(table_2->get_column_count(), 4);
            auto col_ndx_2_0 = table_2->get_column_key("link_0");
            CHECK(col_ndx_2_0);
            auto col_ndx_2_1 = table_2->get_column_key("link_1");
            CHECK(col_ndx_2_1);
            auto col_ndx_2_2 = table_2->get_column_key("link_2");
            CHECK(col_ndx_2_2);
            CHECK_EQUAL(table_2->size(), 5);

            // Check links in table_0.
            for (auto& obj : *table_0) {
                StringData str = obj.get<String>(col_ndx_0_str);
                ObjKey row_ndx_0 = obj.get<ObjKey>(col_ndx_0_0);
                ObjKey row_ndx_1 = obj.get<ObjKey>(col_ndx_0_1);
                ObjKey row_ndx_2 = obj.get<ObjKey>(col_ndx_0_2);
                if (str == "remote_0") {
                    StringData str_link = table_0->get_object(row_ndx_0).get<String>(col_ndx_0_str);
                    CHECK_EQUAL(str_link, "remote_1");
                    CHECK_EQUAL(table_1->get_object(row_ndx_1).get<String>(pk_ndx_1), "bbb");
                    CHECK_EQUAL(table_2->get_object(row_ndx_2).get<Int>(pk_ndx_2), 52);
                }
                else if (str == "remote_1") {
                    StringData str_link = table_0->get_object(row_ndx_0).get<String>(col_ndx_0_str);
                    CHECK_EQUAL(str_link, "remote_2");
                    CHECK(obj.is_null(col_ndx_0_1));
                    CHECK_EQUAL(table_2->get_object(row_ndx_2).get<Int>(pk_ndx_2), 52);
                }
                else if (str == "remote_2") {
                    CHECK(obj.is_null(col_ndx_0_0));
                    CHECK_EQUAL(table_1->get_object(row_ndx_1).get<String>(pk_ndx_1), "ccc");
                    CHECK_EQUAL(table_2->get_object(row_ndx_2).get<Int>(pk_ndx_2), 51);
                }
                else if (str == "local_0") {
                    StringData str_link = table_0->get_object(row_ndx_0).get<String>(col_ndx_0_str);
                    CHECK_EQUAL(str_link, "local_1");
                    CHECK_EQUAL(table_1->get_object(row_ndx_1).get<String>(pk_ndx_1), "aaa");
                    CHECK(obj.is_null(col_ndx_0_2));
                }
                else if (str == "local_1") {
                    CHECK(obj.is_null(col_ndx_0_0));
                    CHECK(obj.is_null(col_ndx_0_1));
                    CHECK(obj.is_null(col_ndx_0_2));
                }
                else if (str == "local_2") {
                    StringData str_link = table_0->get_object(row_ndx_0).get<String>(col_ndx_0_str);
                    CHECK_EQUAL(str_link, "local_2");
                    CHECK(obj.is_null(col_ndx_0_1));
                    CHECK(obj.is_null(col_ndx_0_2));
                }
            }

            // Check links in table_1.
            for (auto& obj : *table_1) {
                StringData pk = obj.get<String>(pk_ndx_1);
                ObjKey row_ndx_0 = obj.get<ObjKey>(col_ndx_1_0);
                ObjKey row_ndx_1 = obj.get<ObjKey>(col_ndx_1_1);
                ObjKey row_ndx_2 = obj.get<ObjKey>(col_ndx_1_2);
                if (pk == "aaa") {
                    StringData str_link = table_0->get_object(row_ndx_0).get<String>(col_ndx_0_str);
                    CHECK_EQUAL(str_link, "remote_1");
                    CHECK_EQUAL(table_1->get_object(row_ndx_1).get<String>(pk_ndx_1), "bbb");
                    CHECK_EQUAL(table_2->get_object(row_ndx_2).get<Int>(pk_ndx_2), 63);
                }
                else if (pk == "bbb") {
                    StringData str_link = table_0->get_object(row_ndx_0).get<String>(col_ndx_0_str);
                    CHECK_EQUAL(str_link, "local_1");
                    CHECK_EQUAL(table_1->get_object(row_ndx_1).get<String>(pk_ndx_1), "aaa");
                    CHECK_EQUAL(table_2->get_object(row_ndx_2).get<Int>(pk_ndx_2), 53);
                }
                else if (pk == "ccc") {
                    StringData str_link = table_0->get_object(row_ndx_0).get<String>(col_ndx_0_str);
                    CHECK_EQUAL(str_link, "remote_2");
                    CHECK_EQUAL(table_1->get_object(row_ndx_1).get<String>(pk_ndx_1), "aaa");
                    CHECK(obj.is_null(col_ndx_1_2));
                }
                else if (pk == "ddd") {
                    StringData str_link = table_0->get_object(row_ndx_0).get<String>(col_ndx_0_str);
                    CHECK_EQUAL(str_link, "local_0");
                    CHECK_EQUAL(table_1->get_object(row_ndx_1).get<String>(pk_ndx_1), "aaa");
                    CHECK(obj.is_null(col_ndx_1_2));
                }
            }

            // Check links in table_2.
            for (auto& obj : *table_2) {
                int_fast64_t pk = obj.get<Int>(pk_ndx_2);
                ObjKey row_ndx_0 = obj.get<ObjKey>(col_ndx_2_0);
                ObjKey row_ndx_1 = obj.get<ObjKey>(col_ndx_2_1);
                ObjKey row_ndx_2 = obj.get<ObjKey>(col_ndx_2_2);
                if (pk == 51) {
                    StringData str_link = table_0->get_object(row_ndx_0).get<String>(col_ndx_0_str);
                    CHECK_EQUAL(str_link, "local_2");
                    CHECK_EQUAL(table_1->get_object(row_ndx_1).get<String>(pk_ndx_1), "bbb");
                    CHECK_EQUAL(table_2->get_object(row_ndx_2).get<Int>(pk_ndx_2), 63);
                }
                else if (pk == 52) {
                    CHECK(obj.is_null(col_ndx_2_0));
                    CHECK(obj.is_null(col_ndx_2_1));
                    CHECK(obj.is_null(col_ndx_2_2));
                }
                else if (pk == 53) {
                    CHECK(obj.is_null(col_ndx_2_0));
                    CHECK(obj.is_null(col_ndx_2_1));
                    CHECK(obj.is_null(col_ndx_2_2));
                }
                else if (pk == 62) {
                    CHECK(obj.is_null(col_ndx_2_0));
                    CHECK_EQUAL(table_1->get_object(row_ndx_1).get<String>(pk_ndx_1), "aaa");
                    CHECK(obj.is_null(col_ndx_2_2));
                }
                else if (pk == 63) {
                    CHECK(obj.is_null(col_ndx_2_0));
                    CHECK(obj.is_null(col_ndx_2_1));
                    CHECK_EQUAL(table_2->get_object(row_ndx_2).get<Int>(pk_ndx_2), 51);
                }
            }
        }
    }
}

} // unnamed namespace
