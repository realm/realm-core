/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include "testsettings.hpp"
#ifdef TEST_REPLICATION

#include <algorithm>
#include <memory>

#include <realm.hpp>
#include <realm/util/features.h>
#include <realm/util/file.hpp>
#include <realm/replication.hpp>
#include <realm/history.hpp>

#include "test.hpp"
#include "test_table_helper.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
using unit_test::TestContext;


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
// environment varible `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.

TEST(Replication_HistorySchemaVersionNormal)
{
    SHARED_GROUP_TEST_PATH(path);
    ReplSyncClient repl(1);
    DBRef sg_1 = DB::create(repl, path);
    // it should be possible to have two open shared groups on the same thread
    // without any read/write transactions in between
    DBRef sg_2 = DB::create(repl, path);
}

TEST(Replication_HistorySchemaVersionDuringWT)
{
    SHARED_GROUP_TEST_PATH(path);

    ReplSyncClient repl(1);
    DBRef sg_1 = DB::create(repl, path);
    {
        // Do an empty commit to force the file format version to be established.
        WriteTransaction wt(sg_1);
        wt.commit();
    }

    auto wt = sg_1->start_write();
    wt->set_sync_file_id(2);

    // It should be possible to open a second db at the same path
    // while a WriteTransaction is active via another SharedGroup.
    DBRef sg_2 = DB::create(repl, path);
    wt->commit();

    auto rt = sg_2->start_read();
    CHECK_EQUAL(rt->get_sync_file_id(), 2);
}


// This is to test that the exported file has no memory leaks
TEST(Replication_GroupWriteWithoutHistory)
{
    SHARED_GROUP_TEST_PATH(path);
    SHARED_GROUP_TEST_PATH(out1);
    SHARED_GROUP_TEST_PATH(out2);

    ReplSyncClient repl(1);
    DBRef sg_1 = DB::create(repl, path);
    {
        WriteTransaction wt(sg_1);
        auto table = wt.add_table("Table");
        auto col = table->add_column(type_String, "strings");
        auto obj = table->create_object();
        obj.set(col, "Hello");
        wt.commit();
    }
    {
        ReadTransaction rt(sg_1);
        // Export file without history
        rt.get_group().write(out1);
    }

    {
        // Open without history
        DBRef sg_2 = DB::create(out1);
        ReadTransaction rt(sg_2);
        rt.get_group().verify();
    }

    {
        ReadTransaction rt(sg_1);
        // Export file with history
        rt.get_group().write(out2, nullptr, 1);
    }

    {
        // Open with history
        ReplSyncClient repl2(1);
        DBRef sg_2 = DB::create(repl2, out2);
        ReadTransaction rt(sg_2);
        rt.get_group().verify();
    }
}

TEST(Replication_HistorySchemaVersionUpgrade)
{
    SHARED_GROUP_TEST_PATH(path);

    {
        ReplSyncClient repl(1);
        DBRef sg = DB::create(repl, path);
        {
            // Do an empty commit to force the file format version to be established.
            WriteTransaction wt(sg);
            wt.commit();
        }
    }

    ReplSyncClient repl(2);
    DBRef sg_1 = DB::create(repl, path); // This will be the session initiator
    CHECK(repl.is_upgraded());
    WriteTransaction wt(sg_1);
    // When this one is opened, the file should have been upgraded
    // If this was not the case we would have triggered another upgrade
    // and the test would hang
    DBRef sg_2 = DB::create(repl, path);
}

TEST(Replication_WriteWithoutHistory)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    ReplSyncClient repl(1);
    DBRef sg = DB::create(repl, path_1);
    {
        // Do an empty commit to force the file format version to be established.
        WriteTransaction wt(sg);
        wt.add_table("Table");
        wt.commit();
    }

    {
        ReadTransaction rt(sg);
        rt.get_group().write(path_2, nullptr, rt.get_version(), false);
    }
    // Make sure the realm can be opened without history
    DBRef sg_2 = DB::create(path_2);
    {
        WriteTransaction wt(sg_2);
        auto table = wt.get_table("Table");
        CHECK(table);
        table->add_column(type_Int, "int");
        wt.commit();
    }
}

#endif // TEST_REPLICATION
