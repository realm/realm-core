#include "testsettings.hpp"
#ifdef TEST_SHARED

#include <UnitTest++.h>

#include <tightdb.hpp>
#include <tightdb/file.hpp>
#include <tightdb/thread.hpp>
#include <tightdb/bind.hpp>
#include <tightdb/terminate.hpp>

#include "testsettings.hpp"

// Need fork() and waitpid() for Shared_RobustAgainstDeathDuringWrite
#ifndef _WIN32
#  include <unistd.h>
#  include <sys/wait.h>
#  define ENABLE_ROBUST_AGAINST_DEATH_DURING_WRITE
#endif

using namespace std;
using namespace tightdb;


TEST(Shared_Unattached)
{
    SharedGroup sg((SharedGroup::unattached_tag()));
}


namespace {

TIGHTDB_TABLE_4(TestTableShared,
                first,  Int,
                second, Int,
                third,  Bool,
                fourth, String)

} // anonymous namespace


TEST(Shared_Initial)
{
    // Delete old files if there
    File::try_remove("test_shared.tightdb");
    File::try_remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup sg("test_shared.tightdb");

        // Verify that new group is empty
        {
            ReadTransaction rt(sg);
            CHECK(rt.get_group().is_empty());
        }

#ifdef TIGHTDB_DEBUG
        // Also do a basic ringbuffer test
        sg.test_ringbuf();
#endif
    }

    // Verify that lock file was deleted after use
#ifndef _WIN32 // GroupShared cannot clean lock file on Windows
    CHECK(!File::exists("test_shared.tightdb.lock"));
#endif
}

TEST(Shared_Stale_Lock_File)
{
    // Delete old files if there
    File::try_remove("test_shared.tightdb");
    File::try_remove("test_shared.tightdb.lock"); // also the info file
#ifndef _WIN32
    {
        // create lock file
        SharedGroup sg("test_shared.tightdb", false, SharedGroup::durability_Full);
        system("cp test_shared.tightdb.lock test_shared.tightdb.lock.backup");
    }
    rename("test_shared.tightdb.lock.backup","test_shared.tightdb.lock");
    {
        SharedGroup sg("test_shared.tightdb", false, SharedGroup::durability_Full);
    }
#endif
    {
        // create lock file
        SharedGroup sg("test_shared.tightdb", false, SharedGroup::durability_Full);
        rename("test_shared.tightdb.lock","test_shared.tightdb.lock.backup");
    }
    rename("test_shared.tightdb.lock.backup","test_shared.tightdb.lock");
    {
        SharedGroup sg("test_shared.tightdb", false, SharedGroup::durability_Full);
    }
}

TEST(Shared_Initial_Mem)
{
    // Delete old files if there
    File::try_remove("test_shared.tightdb");
    File::try_remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup sg("test_shared.tightdb", false, SharedGroup::durability_MemOnly);

        // Verify that new group is empty
        {
            ReadTransaction rt(sg);
            CHECK(rt.get_group().is_empty());
        }

#ifdef TIGHTDB_DEBUG
        // Also do a basic ringbuffer test
        sg.test_ringbuf();
#endif
    }

    // Verify that both db and lock file was deleted after use
#ifndef _WIN32 // GroupShared cannot clean lock/db file on Windows
    CHECK(!File::exists("test_shared.tightdb"));
    CHECK(!File::exists("test_shared.tightdb.lock"));
#endif

}


TEST(Shared_Initial2)
{
    // Delete old files if there
    File::try_remove("test_shared.tightdb");
    File::try_remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup sg("test_shared.tightdb");

        {
            // Open the same db again (in empty state)
            SharedGroup sg2("test_shared.tightdb");

            // Verify that new group is empty
            {
                ReadTransaction rt(sg2);
                CHECK(rt.get_group().is_empty());
            }

            // Add a new table
            {
                WriteTransaction wt(sg2);
                TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
                t1->add(1, 2, false, "test");
                wt.commit();
            }
        }

        // Verify that the new table has been added
        {
            ReadTransaction rt(sg);
            TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
            CHECK_EQUAL(1, t1->size());
            CHECK_EQUAL(1, t1[0].first);
            CHECK_EQUAL(2, t1[0].second);
            CHECK_EQUAL(false, t1[0].third);
            CHECK_EQUAL("test", t1[0].fourth);
        }
    }

    // Verify that lock file was deleted after use
#ifndef _WIN32 // GroupShared cannot clean lock file on Windows
    CHECK(!File::exists("test_shared.tightdb.lock"));
#endif
}


TEST(Shared_Initial2_Mem)
{
    // Delete old files if there
    File::try_remove("test_shared.tightdb");
    File::try_remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup sg("test_shared.tightdb", false, SharedGroup::durability_MemOnly);

        {
            // Open the same db again (in empty state)
            SharedGroup sg2("test_shared.tightdb", false, SharedGroup::durability_MemOnly);

            // Verify that new group is empty
            {
                ReadTransaction rt(sg2);
                CHECK(rt.get_group().is_empty());
            }

            // Add a new table
            {
                WriteTransaction wt(sg2);
                TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
                t1->add(1, 2, false, "test");
                wt.commit();
            }
        }

        // Verify that the new table has been added
        {
            ReadTransaction rt(sg);
            TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
            CHECK_EQUAL(1, t1->size());
            CHECK_EQUAL(1, t1[0].first);
            CHECK_EQUAL(2, t1[0].second);
            CHECK_EQUAL(false, t1[0].third);
            CHECK_EQUAL("test", t1[0].fourth);
        }
    }

    // Verify that both db and lock file was deleted after use
#ifndef _WIN32 // GroupShared cannot clean lock/db file on Windows
    CHECK(!File::exists("test_shared.tightdb"));
    CHECK(!File::exists("test_shared.tightdb.lock"));
#endif
}

TEST(Shared1)
{
    // Delete old files if there
    File::try_remove("test_shared.tightdb");
    File::try_remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup sg("test_shared.tightdb");

        // Create first table in group
        {
            WriteTransaction wt(sg);
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1->add(1, 2, false, "test");
            wt.commit();
        }

        // Open same db again
        SharedGroup sg2("test_shared.tightdb");
        {
            ReadTransaction rt(sg2);

            // Verify that last set of changes are commited
            TestTableShared::ConstRef t2 = rt.get_table<TestTableShared>("test");
            CHECK(t2->size() == 1);
            CHECK_EQUAL(1, t2[0].first);
            CHECK_EQUAL(2, t2[0].second);
            CHECK_EQUAL(false, t2[0].third);
            CHECK_EQUAL("test", t2[0].fourth);

            // Do a new change while stil having current read transaction open
            {
                WriteTransaction wt(sg);
                TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
                t1->add(2, 3, true, "more test");
                wt.commit();
            }

            // Verify that that the read transaction does not see
            // the change yet (is isolated)
            CHECK(t2->size() == 1);
            CHECK_EQUAL(1, t2[0].first);
            CHECK_EQUAL(2, t2[0].second);
            CHECK_EQUAL(false, t2[0].third);
            CHECK_EQUAL("test", t2[0].fourth);

            // Do one more new change while stil having current read transaction open
            // so we know that it does not overwrite data held by
            {
                WriteTransaction wt(sg);
                TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
                t1->add(0, 1, false, "even more test");
                wt.commit();
            }

            // Verify that that the read transaction does still not see
            // the change yet (is isolated)
            CHECK(t2->size() == 1);
            CHECK_EQUAL(1, t2[0].first);
            CHECK_EQUAL(2, t2[0].second);
            CHECK_EQUAL(false, t2[0].third);
            CHECK_EQUAL("test", t2[0].fourth);
        }

        // Start a new read transaction and verify that it can now see the changes
        {
            ReadTransaction rt(sg2);
            TestTableShared::ConstRef t3 = rt.get_table<TestTableShared>("test");

            CHECK(t3->size() == 3);
            CHECK_EQUAL(1, t3[0].first);
            CHECK_EQUAL(2, t3[0].second);
            CHECK_EQUAL(false, t3[0].third);
            CHECK_EQUAL("test", t3[0].fourth);
            CHECK_EQUAL(2, t3[1].first);
            CHECK_EQUAL(3, t3[1].second);
            CHECK_EQUAL(true, t3[1].third);
            CHECK_EQUAL("more test", t3[1].fourth);
            CHECK_EQUAL(0, t3[2].first);
            CHECK_EQUAL(1, t3[2].second);
            CHECK_EQUAL(false, t3[2].third);
            CHECK_EQUAL("even more test", t3[2].fourth);
        }
    }

    // Verify that lock file was deleted after use
#ifndef _WIN32 // GroupShared cannot clean lock file on Windows
    CHECK(!File::exists("test_shared.tightdb.lock"));
#endif
}

TEST(Shared_rollback)
{
    // Delete old files if there
    File::try_remove("test_shared.tightdb");
    File::try_remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup sg("test_shared.tightdb");

        // Create first table in group (but rollback)
        {
            WriteTransaction wt(sg);
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1->add(1, 2, false, "test");
            // Note: Implicit rollback
        }

        // Verify that no changes were made
        {
            ReadTransaction rt(sg);
            CHECK_EQUAL(false, rt.get_group().has_table("test"));
        }

        // Really create first table in group
        {
            WriteTransaction wt(sg);
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1->add(1, 2, false, "test");
            wt.commit();
        }

        // Verify that the changes were made
        {
            ReadTransaction rt(sg);
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");
            CHECK(t->size() == 1);
            CHECK_EQUAL(1, t[0].first);
            CHECK_EQUAL(2, t[0].second);
            CHECK_EQUAL(false, t[0].third);
            CHECK_EQUAL("test", t[0].fourth);
        }

        // Greate more changes (but rollback)
        {
            WriteTransaction wt(sg);
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1->add(0, 0, true, "more test");
            // Note: Implicit rollback
        }

        // Verify that no changes were made
        {
            ReadTransaction rt(sg);
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");
            CHECK(t->size() == 1);
            CHECK_EQUAL(1, t[0].first);
            CHECK_EQUAL(2, t[0].second);
            CHECK_EQUAL(false, t[0].third);
            CHECK_EQUAL("test", t[0].fourth);
        }
    }

    // Verify that lock file was deleted after use
#ifndef _WIN32 // GroupShared cannot clean lock file on Windows
    CHECK(!File::exists("test_shared.tightdb.lock"));
#endif
}

TEST(Shared_Writes)
{
    // Delete old files if there
    File::try_remove("test_shared.tightdb");
    File::try_remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup sg("test_shared.tightdb");

        // Create first table in group
        {
            WriteTransaction wt(sg);
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1->add(0, 2, false, "test");
            wt.commit();
        }

        // Do a lot of repeated write transactions
        for (size_t i = 0; i < 100; ++i) {
            WriteTransaction wt(sg);
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1[0].first += 1;
            wt.commit();
        }

        // Verify that the changes were made
        {
            ReadTransaction rt(sg);
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");
            const int64_t v = t[0].first;
            CHECK_EQUAL(100, v);
        }
    }

    // Verify that lock file was deleted after use
#ifndef _WIN32 // GroupShared cannot clean lock file on Windows
    CHECK(!File::exists("test_shared.tightdb.lock"));
#endif
}


TEST(Shared_ManyReaders)
{
    // This test was written primarily to expose a former bug in
    // SharedGroup::end_read(), where the lock-file was not remapped
    // after ring-buffer expansion.

    const int chunk_1_size = 251;
    char chunk_1[chunk_1_size];
    for (int i = 0; i < chunk_1_size; ++i)
        chunk_1[i] = (i + 3) % 251;
    const int chunk_2_size = 123;
    char chunk_2[chunk_2_size];
    for (int i = 0; i < chunk_2_size; ++i)
        chunk_2[i] = (i + 11) % 241;

#if TEST_DURATION < 1
    // Mac OS X 10.8 cannot handle more than 15 due to its default ulimit settings.
    int rounds[] = { 3, 5, 7, 9, 11, 13, 15 };
#else
    int rounds[] = { 3, 5, 11, 17, 23, 27, 31, 47, 59 };
#endif
    const int num_rounds = sizeof rounds / sizeof *rounds;

    const int max_N = 64;
    CHECK(max_N >= rounds[num_rounds-1]);
    UniquePtr<SharedGroup> shared_groups[8 * max_N];
    UniquePtr<ReadTransaction> read_transactions[8 * max_N];

    for (int round = 0; round < num_rounds; ++round) {
        int N = rounds[round];

        File::try_remove("test.tightdb");
        File::try_remove("test.tightdb.lock");

        SharedGroup root_sg("test.tightdb", false, SharedGroup::durability_MemOnly);

        // Add two tables
        {
            WriteTransaction wt(root_sg);
            TableRef test_1 = wt.get_table("test_1");
            test_1->add_column(type_Int, "i");
            test_1->insert_int(0,0,0);
            test_1->insert_done();
            TableRef test_2 = wt.get_table("test_2");
            test_2->add_column(type_Binary, "b");
            wt.commit();
        }


        // Create 8*N shared group accessors
        for (int i = 0; i < 8*N; ++i)
            shared_groups[i].reset(new SharedGroup("test.tightdb", false, SharedGroup::durability_MemOnly));

        // Initiate 2*N read transactions with progressive changes
        for (int i = 0; i < 2*N; ++i) {
            read_transactions[i].reset(new ReadTransaction(*shared_groups[i]));
            {
                ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
                CHECK_EQUAL(1u, test_1->size());
                CHECK_EQUAL(i, test_1->get_int(0,0));
                ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
                int n_1 = i *  1;
                int n_2 = i * 18;
                CHECK_EQUAL(n_1+n_2, test_2->size());
                for (int j = 0; j < n_1; ++j)
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0,j));
                for (int j = n_1; j < n_1+n_2; ++j)
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0,j));
            }
            {
                WriteTransaction wt(root_sg);
                TableRef test_1 = wt.get_table("test_1");
                test_1->add_int(0,1);
                TableRef test_2 = wt.get_table("test_2");
                test_2->insert_binary(0, 0, BinaryData(chunk_1));
                test_2->insert_done();
                wt.commit();
            }
            {
                WriteTransaction wt(root_sg);
                TableRef test_2 = wt.get_table("test_2");
                for (int j = 0; j < 18; ++j) {
                    test_2->insert_binary(0, test_2->size(), BinaryData(chunk_2));
                    test_2->insert_done();
                }
                wt.commit();
            }
        }

        // Check isolation between read transactions
        for (int i = 0; i < 2*N; ++i) {
            ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
            CHECK_EQUAL(1, test_1->size());
            CHECK_EQUAL(i, test_1->get_int(0,0));
            ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
            int n_1 = i *  1;
            int n_2 = i * 18;
            CHECK_EQUAL(n_1+n_2, test_2->size());
            for (int j = 0; j < n_1; ++j)
                CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0,j));
            for (int j = n_1; j < n_1+n_2; ++j)
                CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0,j));
        }

        // End the first half of the read transactions during further
        // changes
        for (int i = N-1; i >= 0; --i) {
            {
                WriteTransaction wt(root_sg);
                TableRef test_1 = wt.get_table("test_1");
                test_1->add_int(0,2);
                wt.commit();
            }
            {
                ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
                CHECK_EQUAL(1, test_1->size());
                CHECK_EQUAL(i, test_1->get_int(0,0));
                ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
                int n_1 = i *  1;
                int n_2 = i * 18;
                CHECK_EQUAL(n_1+n_2, test_2->size());
                for (int j = 0; j < n_1; ++j)
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0,j));
                for (int j = n_1; j < n_1+n_2; ++j)
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0,j));
            }
            read_transactions[i].reset();
        }

        // Initiate 6*N extra read transactionss with further progressive changes
        for (int i = 2*N; i < 8*N; ++i) {
            read_transactions[i].reset(new ReadTransaction(*shared_groups[i]));
            {
                ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
                CHECK_EQUAL(1u, test_1->size());
                int i_2 = 2*N + i;
                CHECK_EQUAL(i_2, test_1->get_int(0,0));
                ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
                int n_1 = i *  1;
                int n_2 = i * 18;
                CHECK_EQUAL(n_1+n_2, test_2->size());
                for (int j = 0; j < n_1; ++j)
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0,j));
                for (int j = n_1; j < n_1+n_2; ++j)
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0,j));
            }
            {
                WriteTransaction wt(root_sg);
                TableRef test_1 = wt.get_table("test_1");
                test_1->add_int(0,1);
                TableRef test_2 = wt.get_table("test_2");
                test_2->insert_binary(0, 0, BinaryData(chunk_1));
                test_2->insert_done();
                wt.commit();
            }
            {
                WriteTransaction wt(root_sg);
                TableRef test_2 = wt.get_table("test_2");
                for (int j = 0; j < 18; ++j) {
                    test_2->insert_binary(0, test_2->size(), BinaryData(chunk_2));
                    test_2->insert_done();
                }
                wt.commit();
            }
        }

        // End all remaining read transactions during further changes
        for (int i = 1*N; i < 8*N; ++i) {
            {
                WriteTransaction wt(root_sg);
                TableRef test_1 = wt.get_table("test_1");
                test_1->add_int(0,2);
                wt.commit();
            }
            {
                ConstTableRef test_1 = read_transactions[i]->get_table("test_1");
                CHECK_EQUAL(1, test_1->size());
                int i_2 = i<2*N ? i : 2*N + i;
                CHECK_EQUAL(i_2, test_1->get_int(0,0));
                ConstTableRef test_2 = read_transactions[i]->get_table("test_2");
                int n_1 = i *  1;
                int n_2 = i * 18;
                CHECK_EQUAL(n_1+n_2, test_2->size());
                for (int j = 0; j < n_1; ++j)
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0,j));
                for (int j = n_1; j < n_1+n_2; ++j)
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0,j));
            }
            read_transactions[i].reset();
        }

        // Check final state via each shared group, then destroy it
        for (int i=0; i<8*N; ++i) {
            {
                ReadTransaction rt(*shared_groups[i]);
                ConstTableRef test_1 = rt.get_table("test_1");
                CHECK_EQUAL(1, test_1->size());
                CHECK_EQUAL(3*8*N, test_1->get_int(0,0));
                ConstTableRef test_2 = rt.get_table("test_2");
                int n_1 = 8*N *  1;
                int n_2 = 8*N * 18;
                CHECK_EQUAL(n_1+n_2, test_2->size());
                for (int j = 0; j < n_1; ++j)
                    CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0,j));
                for (int j = n_1; j < n_1+n_2; ++j)
                    CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0,j));
            }
            shared_groups[i].reset();
        }

        // Check final state via new shared group
        {
            SharedGroup sg("test.tightdb", false, SharedGroup::durability_MemOnly);
            ReadTransaction rt(sg);
            ConstTableRef test_1 = rt.get_table("test_1");
            CHECK_EQUAL(1, test_1->size());
            CHECK_EQUAL(3*8*N, test_1->get_int(0,0));
            ConstTableRef test_2 = rt.get_table("test_2");
            int n_1 = 8*N *  1;
            int n_2 = 8*N * 18;
            CHECK_EQUAL(n_1+n_2, test_2->size());
            for (int j = 0; j < n_1; ++j)
                CHECK_EQUAL(BinaryData(chunk_1), test_2->get_binary(0,j));
            for (int j = n_1; j < n_1+n_2; ++j)
                CHECK_EQUAL(BinaryData(chunk_2), test_2->get_binary(0,j));
        }
    }
}


namespace {

TIGHTDB_TABLE_1(MyTable_SpecialOrder, first,  Int)

} // anonymous namespace

TEST(Shared_Writes_SpecialOrder)
{
    File::try_remove("test.tightdb");
    File::try_remove("test.tightdb.lock");

    SharedGroup sg("test.tightdb");

    const int num_rows = 5; // FIXME: Should be strictly greater than TIGHTDB_MAX_LIST_SIZE, but that takes a loooooong time!
    const int num_reps = 25;

    {
        WriteTransaction wt(sg);
        MyTable_SpecialOrder::Ref table = wt.get_table<MyTable_SpecialOrder>("test");
        for (int i=0; i<num_rows; ++i) {
            table->add(0);
        }
        wt.commit();
    }

    for (int i=0; i<num_rows; ++i) {
        for (int j=0; j<num_reps; ++j) {
            {
                WriteTransaction wt(sg);
                MyTable_SpecialOrder::Ref table = wt.get_table<MyTable_SpecialOrder>("test");
                CHECK_EQUAL(j, table[i].first);
                ++table[i].first;
                wt.commit();
            }
        }
    }

    {
        ReadTransaction rt(sg);
        MyTable_SpecialOrder::ConstRef table = rt.get_table<MyTable_SpecialOrder>("test");
        for (int i=0; i<num_rows; ++i) {
            CHECK_EQUAL(num_reps, table[i].first);
        }
    }
}

namespace  {

void increment_entry_thread(size_t row_ndx)
{
    // Open shared db
    SharedGroup sg("test_shared.tightdb");

    for (size_t i = 0; i < 100; ++i) {
        // Increment cell
        {
            WriteTransaction wt(sg);
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1[row_ndx].first += 1;
            // FIXME: For some reason this takes ages when running
            // inside valgrind, it is probably due to the "extreme
            // overallocation" bug. The 1000 transactions performed
            // here can produce a final database file size of more
            // than 1 GiB. Really! And that is a table with only 10
            // rows. It is about 1 MiB per transaction.
            wt.commit();
        }

        // Verify in new transaction so that we interleave
        // read and write transactions
        {
            ReadTransaction rt(sg);
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

            int64_t v = t[row_ndx].first;
            int64_t expected = i+1;
            CHECK_EQUAL(expected, v);
        }
    }
}

} // anonymous namespace

TEST(Shared_WriterThreads)
{
    // Delete old files if there
    File::try_remove("test_shared.tightdb");
    File::try_remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup sg("test_shared.tightdb");

        const size_t thread_count = 10;

        // Create first table in group
        {
            WriteTransaction wt(sg);
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            for (size_t i = 0; i < thread_count; ++i) {
                t1->add(0, 2, false, "test");
            }
            wt.commit();
        }

        Thread threads[thread_count];

        // Create all threads
        for (size_t i = 0; i < thread_count; ++i) {
            threads[i].start(util::bind(&increment_entry_thread, i));
        }

        // Wait for all threads to complete
        for (size_t i = 0; i < thread_count; ++i) {
            threads[i].join();
        }

        // Verify that the changes were made
        {
            ReadTransaction rt(sg);
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

            for (size_t i = 0; i < thread_count; ++i) {
                int64_t v = t[i].first;
                CHECK_EQUAL(100, v);
            }
        }
    }

    // Verify that lock file was deleted after use
#ifndef _WIN32 // GroupShared cannot clean lock file on Windows
    CHECK(!File::exists("test_shared.tightdb.lock"));
#endif
}


#if defined TEST_ROBUSTNESS && defined ENABLE_ROBUST_AGAINST_DEATH_DURING_WRITE

TEST(Shared_RobustAgainstDeathDuringWrite)
{
    // Abort if robust mutexes are not supported on the current
    // platform. Otherwise we would probably get into a dead-lock.
    if (!RobustMutex::is_robust_on_this_platform())
        return;

    // This test can only be conducted by spawning independent
    // processes which can then be terminated individually.

    File::try_remove("test.tightdb");

    for (int i = 0; i < 10; ++i) {
        pid_t pid = fork();
        if (pid == pid_t(-1))
            TIGHTDB_TERMINATE("fork() failed");
        if (pid == 0) {
            // Child
            SharedGroup sg("test.tightdb");
            WriteTransaction wt(sg);
            TableRef table = wt.get_table("alpha");
            _exit(0); // Die with an active write transaction
        }
        else {
            // Parent
            int stat_loc = 0;
            int options = 0;
            pid = waitpid(pid, &stat_loc, options);
            if (pid == pid_t(-1))
                TIGHTDB_TERMINATE("waitpid() failed");
            bool child_exited_normaly = WIFEXITED(stat_loc);
            CHECK(child_exited_normaly);
            int child_exit_status = WEXITSTATUS(stat_loc);
            CHECK_EQUAL(0, child_exit_status);
        }

        // Check that we can continue without dead-locking
        {
            SharedGroup sg("test.tightdb");
            WriteTransaction wt(sg);
            TableRef table = wt.get_table("beta");
            if (table->is_empty()) {
                table->add_column(type_Int, "i");
                table->insert_int(0,0,0);
                table->insert_done();
            }
            table->add_int(0,1);
            wt.commit();
        }
    }

    {
        SharedGroup sg("test.tightdb");
        ReadTransaction rt(sg);
        CHECK(!rt.has_table("alpha"));
        CHECK(rt.has_table("beta"));
        ConstTableRef table = rt.get_table("beta");
        CHECK_EQUAL(10, table->get_int(0,0));
    }

    File::remove("test.tightdb");
}

#endif // defined TEST_ROBUSTNESS && defined ENABLE_ROBUST_AGAINST_DEATH_DURING_WRITE


TEST(Shared_FormerErrorCase1)
{
    File::try_remove("test_shared.tightdb");
    File::try_remove("test_shared.tightdb.lock");
    SharedGroup sg("test_shared.tightdb");
    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("my_table");
        {
            Spec& spec = table->get_spec();
            spec.add_column(type_Int, "alpha");
            spec.add_column(type_Bool, "beta");
            spec.add_column(type_Int, "gamma");
            spec.add_column(type_DateTime, "delta");
            spec.add_column(type_String, "epsilon");
            spec.add_column(type_Binary, "zeta");
            {
                Spec subspec = spec.add_subtable_column("eta");
                subspec.add_column(type_Int, "foo");
                {
                    Spec subsubspec = subspec.add_subtable_column("bar");
                    subsubspec.add_column(type_Int, "value");
                }
            }
            spec.add_column(type_Mixed, "theta");
        }
        table->update_from_spec();
        table->insert_empty_row(0, 1);
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        {
            TableRef table = wt.get_table("my_table");
            table->set_int(0, 0, 1);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        {
            TableRef table = wt.get_table("my_table");
            table->set_int(0, 0, 2);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        {
            TableRef table = wt.get_table("my_table");
            TableRef table2 = table->get_subtable(6, 0);
            table2->insert_int(0, 0, 0);
            table2->insert_subtable(1, 0);
            table2->insert_done();
        }
        {
            TableRef table = wt.get_table("my_table");
            table->set_int(0, 0, 3);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        {
            TableRef table = wt.get_table("my_table");
            table->set_int(0, 0, 4);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        {
            TableRef table = wt.get_table("my_table");
            TableRef table2 = table->get_subtable(6, 0);
            TableRef table3 = table2->get_subtable(1, 0);
            table3->insert_empty_row(0, 1);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        {
            TableRef table = wt.get_table("my_table");
            TableRef table2 = table->get_subtable(6, 0);
            TableRef table3 = table2->get_subtable(1, 0);
            table3->insert_empty_row(1, 1);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        {
            TableRef table = wt.get_table("my_table");
            TableRef table2 = table->get_subtable(6, 0);
            TableRef table3 = table2->get_subtable(1, 0);
            table3->set_int(0, 0, 0);
        }
        {
            TableRef table = wt.get_table("my_table");
            table->set_int(0, 0, 5);
        }
        {
            TableRef table = wt.get_table("my_table");
            TableRef table2 = table->get_subtable(6, 0);
            table2->set_int(0, 0, 1);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("my_table");
        table = table->get_subtable(6, 0);
        table = table->get_subtable(1, 0);
        table->set_int(0, 1, 1);
        table = wt.get_table("my_table");
        table->set_int(0, 0, 6);
        table = wt.get_table("my_table");
        table = table->get_subtable(6, 0);
        table->set_int(0, 0, 2);
        wt.commit();
    }
}



namespace {

TIGHTDB_TABLE_1(FormerErrorCase2_Subtable,
                value,  Int)

TIGHTDB_TABLE_1(FormerErrorCase2_Table,
                bar, Subtable<FormerErrorCase2_Subtable>)

} // namespace

TEST(Shared_FormerErrorCase2)
{
    File::try_remove("test_shared.tightdb");
    File::try_remove("test_shared.tightdb.lock");

    for (int i=0; i<10; ++i) {
        SharedGroup sg("test_shared.tightdb");
        {
            WriteTransaction wt(sg);
            FormerErrorCase2_Table::Ref table = wt.get_table<FormerErrorCase2_Table>("table");
            table->add();
            table->add();
            table->add();
            table->add();
            table->add();
            table->clear();
            table->add();
            table[0].bar->add();
            wt.commit();
        }
    }
}

namespace {

TIGHTDB_TABLE_1(OverAllocTable,
                text, String)

} // namespace

TEST(Shared_SpaceOveruse)
{
#if TEST_DURATION < 1
    const int n_outer = 300;
    const int n_inner = 21;
#else
    const int n_outer = 3000;
    const int n_inner = 42;
#endif

    // Many transactions
    {
        File::try_remove("over_alloc_1.tightdb");
        File::try_remove("over_alloc_1.tightdb.lock");
        SharedGroup sg("over_alloc_1.tightdb");

        // Do a lot of sequential transactions
        for (int i = 0; i < n_outer; ++i) {
            WriteTransaction wt(sg);
            OverAllocTable::Ref table = wt.get_table<OverAllocTable>("my_table");
            for (int j = 0; j < n_inner; ++j) {
                table->add("x");
            }
            wt.commit();
        }

        // Verify that all was added correctly
        {
            ReadTransaction rt(sg);
            OverAllocTable::ConstRef table = rt.get_table<OverAllocTable>("my_table");

            const size_t count = table->size();
            CHECK_EQUAL(n_outer * n_inner, count);

            for (size_t i = 0; i < count; ++i) {
                CHECK_EQUAL("x", table[i].text);
            }

#ifdef TIGHTDB_DEBUG
            table->Verify();
#endif
        }
    }
}


TEST(Shared_Notifications)
{
    // Delete old files if there
    File::try_remove("test_shared.tightdb");
    File::try_remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup sg("test_shared.tightdb");

        // No other instance have changed db since last transaction
        CHECK(!sg.has_changed());

        {
            // Open the same db again (in empty state)
            SharedGroup sg2("test_shared.tightdb");

            // Verify that new group is empty
            {
                ReadTransaction rt(sg2);
                CHECK(rt.get_group().is_empty());
            }

            // No other instance have changed db since last transaction
            CHECK(!sg2.has_changed());

            // Add a new table
            {
                WriteTransaction wt(sg2);
                TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
                t1->add(1, 2, false, "test");
                wt.commit();
            }
        }

        // Db has been changed by other instance
        CHECK(sg.has_changed());

        // Verify that the new table has been added
        {
            ReadTransaction rt(sg);
            TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
            CHECK_EQUAL(1, t1->size());
            CHECK_EQUAL(1, t1[0].first);
            CHECK_EQUAL(2, t1[0].second);
            CHECK_EQUAL(false, t1[0].third);
            CHECK_EQUAL("test", t1[0].fourth);
        }

        // No other instance have changed db since last transaction
        CHECK(!sg.has_changed());
    }
}

TEST(Shared_FromSerialized)
{
    // Delete old files if there
    File::try_remove("test_shared.tightdb");
    File::try_remove("test_shared.tightdb.lock"); // also the info file

    // Create new group and serialize to disk
    {
        Group g1;
        TestTableShared::Ref t1 = g1.get_table<TestTableShared>("test");
        t1->add(1, 2, false, "test");
        g1.write("test_shared.tightdb");
    }

    // Open same file as shared group
    SharedGroup sg("test_shared.tightdb");

    // Verify that contents is there when shared
    {
        ReadTransaction rt(sg);
        TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
        CHECK_EQUAL(1, t1->size());
        CHECK_EQUAL(1, t1[0].first);
        CHECK_EQUAL(2, t1[0].second);
        CHECK_EQUAL(false, t1[0].third);
        CHECK_EQUAL("test", t1[0].fourth);
    }
}

#ifndef _WIN32
TEST(StringIndex_Bug1)
{
    File::try_remove("test.tightdb");
    File::try_remove("test.tightdb.lock");
    SharedGroup db("test.tightdb");

    {
        Group& group = db.begin_write();
        TableRef table = group.get_table("users");
        table->add_column(type_String, "username");
        table->set_index(0);
        for (int i = 0; i < TIGHTDB_MAX_LIST_SIZE + 1; ++i)
            table->add_empty_row();
        for (int i = 0; i < TIGHTDB_MAX_LIST_SIZE + 1; ++i)
            table->remove(0);
        db.commit();
    }

    {
        Group& group = db.begin_write();
        TableRef table = group.get_table("users");
        table->add_empty_row();
        db.commit();
    }

    File::try_remove("test.tightdb");
}
#endif

TEST(StringIndex_Bug2)
{
    File::try_remove("test.tightdb");
    File::try_remove("test.tightdb.lock");
    SharedGroup sg("test.tightdb");

    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("a");
        table->add_column(type_String, "b");
        table->set_index(0);  // Not adding index makes it work
        table->add_empty_row();
        wt.commit();
    }

    {
        ReadTransaction rt(sg);
    }
}


namespace {
void rand_str(char* res, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        res[i] = 'a' + rand() % 10;
    }
}
} // anonymous namespace

TEST(StringIndex_Bug3)
{
    File::try_remove("indexbug.tightdb");
    File::try_remove("indexbug.tightdb.lock");
    SharedGroup db("indexbug.tightdb");

    {
        Group& group = db.begin_write();
        TableRef table = group.get_table("users");
        table->add_column(type_String, "username");
        table->set_index(0);  // Disabling index makes it work
        db.commit();
    }

    size_t transactions = 0;

    for (size_t n = 0; n < 100; ++n) {
        const uint64_t action = rand() % 1000;

        transactions++;

        if (action <= 500) {
            // delete random user
            Group& group = db.begin_write();
            TableRef table = group.get_table("users");
            if (table->size() > 0) {
                size_t del = rand() % table->size();
                //cerr << "-" << del << ": " << table->get_string(0, del) << endl;
                table->remove(del);
#ifdef TIGHTDB_DEBUG
                table->Verify();
#endif
            }
            db.commit();
        }
        else {
            // add new user
            Group& group = db.begin_write();
            TableRef table = group.get_table("users");
            table->add_empty_row();
            char txt[100];
            rand_str(txt, 8);
            txt[8] = 0;
            //cerr << "+" << txt << endl;
            table->set_string(0, table->size() - 1, txt);
#ifdef TIGHTDB_DEBUG
            table->Verify();
#endif
            db.commit();
        }
    }
}


// disable shared async on windows
#ifndef _WIN32

TEST(Shared_Async)
{
    // Clean up old state
    File::try_remove("asynctest.tightdb");
    File::try_remove("asynctest.tightdb.lock");

    // Do some changes in a async db
    {
        SharedGroup db("asynctest.tightdb", false, SharedGroup::durability_Async);

        for (size_t n = 0; n < 100; ++n) {
            //printf("t %d\n", (int)n);
            WriteTransaction wt(db);
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1->add(1, n, false, "test");
            wt.commit();
        }
    }

    // Wait for async_commit process to shutdown
    while (File::exists("asynctest.tightdb.lock")) {
        sleep(1);
    }

    // Read the db again in normal mode to verify
    {
        SharedGroup db("asynctest.tightdb");

        for (size_t n = 0; n < 100; ++n) {
            ReadTransaction rt(db);
            TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
            CHECK(t1->size() == 100);
        }
    }
}


namespace  {

#define INCREMENTS 100

void* IncrementEntry(void* arg)
{
    try 
    {
        const size_t row_ndx = (size_t)arg;

        // Open shared db
        SharedGroup sg("test_shared.tightdb", 
                       false, SharedGroup::durability_Async );

        for (size_t i = 0; i < INCREMENTS; ++i) {

            // Increment cell
            {

                WriteTransaction wt(sg);
                TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
                t1[row_ndx].first += 1;
                // FIXME: For some reason this takes ages when running
                // inside valgrind, it is probably due to the "extreme
                // overallocation" bug. The 1000 transactions performed
                // here can produce a final database file size of more
                // than 1 GiB. Really! And that is a table with only 10
                // rows. It is about 1 MiB per transaction.
                wt.commit();
            }
            // Verify in new transaction so that we interleave
            // read and write transactions

            {
                ReadTransaction rt(sg);
                TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

                const int64_t v = t[row_ndx].first;
                const int64_t expected = i+1;
                CHECK_EQUAL(expected, v);
            }

        }

    } catch (runtime_error e) {
        printf("Thread exiting due to runtime exception\n");
        printf("what(): %s\n", e.what());
    } catch (...) {
        printf("Thread exiting for unknown reason\n");
        printf("\n");
    }
    return 0;
}


void make_table(size_t rows) 
{
    File::try_remove("test_shared.tightdb");
    File::try_remove("test_shared.tightdb.log");
    File::try_remove("test_alone.tightdb");
    // Create first table in group
#if 1
#if 0
    {
        SharedGroup sgr("test_shared.tightdb");
        SharedGroup sgw("test_shared.tightdb");
        {
            ReadTransaction rt0(sgr);
            WriteTransaction wt0(sgw);
            wt0.commit();
        }
        ReadTransaction rt(sgr);
        {
        }
        WriteTransaction wt(sgw);
        TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
        for (size_t i = 0; i < rows; ++i) {
            t1->add(0, 2, false, "test");
        }
        wt.commit();
        WriteTransaction wt2(sgw);
        TestTableShared::Ref t2 = wt2.get_table<TestTableShared>("test");
        for (size_t i = 0; i < rows; ++i) {
            t2->add(0, 2, false, "test");
        }
        wt2.commit();
    }
#else
#if 0
    {
        SharedGroup sg("test_shared.tightdb");
        WriteTransaction wt(sg);
        TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
        for (size_t i = 0; i < rows; ++i) {
            t1->add(0, 2, false, "test");
        }
        wt.commit();
    }
#else
    {
        SharedGroup sg("test_shared.tightdb", 
                       false, SharedGroup::durability_Async);
        WriteTransaction wt(sg);
        TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
        for (size_t i = 0; i < rows; ++i) {
            t1->add(0, 2, false, "test");
        }
        wt.commit();
    }
#endif
#endif
    // Wait for async_commit process to shutdown
    while (File::exists("test_shared.tightdb.lock")) {
        usleep(100);
    }
#else
    {
        Group g("test_alone.tightdb", Group::mode_ReadWrite);
        TestTableShared::Ref t1 = g.get_table<TestTableShared>("test");
        for (size_t i = 0; i < rows; ++i) {
            t1->add(0, 2, false, "test");
        }
        printf("Writing db\n");
        g.commit();
    }
#endif
}

void multi_threaded(size_t thread_count, size_t base) 
{
    // Do some changes in a async db
    {

        pthread_t* threads = new pthread_t[thread_count];

        // Create all threads
        for (size_t i = 0; i < thread_count; ++i) {
            const int rc = pthread_create(&threads[i], NULL, IncrementEntry, (void*)(i+base));
            CHECK_EQUAL(0, rc);
        }

        // Wait for all threads to complete
        for (size_t i = 0; i < thread_count; ++i) {
            const int rc = pthread_join(threads[i], NULL);
            CHECK_EQUAL(0, rc);
        }

        delete[] threads;

        // Verify that the changes were made
        {
            SharedGroup sg("test_shared.tightdb", 
                           false, SharedGroup::durability_Async);
            ReadTransaction rt(sg);
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

            for (size_t i = 0; i < thread_count; ++i) {
                const int64_t v = t[i+base].first;
                CHECK_EQUAL(INCREMENTS, v);
            }
        }
    }
}

void validate_and_clear(size_t rows, int result)
{
    // Wait for async_commit process to shutdown
    while (File::exists("test_shared.tightdb.lock")) {
        usleep(100);
    }
    // Verify - once more, in sync mode - that the changes were made
    {
        SharedGroup sg("test_shared.tightdb");
        WriteTransaction wt(sg);
        TestTableShared::Ref t = wt.get_table<TestTableShared>("test");
        
        for (size_t i = 0; i < rows; ++i) {
            const int64_t v = t[i].first;
            t[i].first = 0;
            CHECK_EQUAL(result, v);
        }
        wt.commit();
    }
}

void multi_process(int numprocs, size_t numthreads) 
{
    for (int i=0; i<numprocs; i++) {
        if (fork()==0) {
            multi_threaded(numthreads, i*numthreads);
            exit(0);
        }
    }
    int status = 0;
    for (int i=0; i<numprocs; i++) wait(&status);
}

} // anonymous namespace


TEST(Shared_Multiprocess)
{
    // wait for any daemon hanging around to exit
    File::try_remove("test_shared.tightdb.lock");
    usleep(100);

#if TEST_DURATION < 1
    make_table(4);

    multi_threaded(2,0);
    validate_and_clear(2, INCREMENTS);

    for (int k=1; k<3; k++) {
        multi_process(2,2);
        validate_and_clear(4,INCREMENTS);
    }
#else
    make_table(100);

    multi_threaded(10,0);
    validate_and_clear(10, INCREMENTS);

    for (int k=1; k<10; k++) {
        multi_process(10,10);
        validate_and_clear(100,INCREMENTS);
    }
#endif
}

#endif // endif not on windows


TEST(Shared_MixedWithNonShared)
{
    File::try_remove("test.tightdb");
    {
        // Create empty file without free-space tracking
        Group g;
        g.write("test.tightdb");
    }
    {
        // See if we can modify with non-shared group
        Group g("test.tightdb", Group::mode_ReadWrite);
        g.get_table("foo"); // Add table "foo"
        g.commit();
    }

    File::try_remove("test.tightdb");
    {
        // Create non-empty file without free-space tracking
        Group g;
        g.get_table("x");
        g.write("test.tightdb");
    }
    {
        // See if we can modify with non-shared group
        Group g("test.tightdb", Group::mode_ReadWrite);
        g.get_table("foo"); // Add table "foo"
        g.commit();
    }

    File::try_remove("test.tightdb");
    {
        // Create empty file without free-space tracking
        Group g;
        g.write("test.tightdb");
    }
    {
        // See if we can read and modify with shared group
        SharedGroup sg("test.tightdb");
        {
            ReadTransaction rt(sg);
            CHECK(!rt.has_table("foo"));
        }
        {
            WriteTransaction wt(sg);
            wt.get_table("foo"); // Add table "foo"
            wt.commit();
        }
    }

    File::try_remove("test.tightdb");
    {
        // Create non-empty file without free-space tracking
        Group g;
        g.get_table("x");
        g.write("test.tightdb");
    }
    {
        // See if we can read and modify with shared group
        SharedGroup sg("test.tightdb");
        {
            ReadTransaction rt(sg);
            CHECK(!rt.has_table("foo"));
        }
        {
            WriteTransaction wt(sg);
            wt.get_table("foo"); // Add table "foo"
            wt.commit();
        }
    }
    {
        SharedGroup sg("test.tightdb");
        {
            ReadTransaction rt(sg);
            CHECK(rt.has_table("foo"));
        }
    }
    {
        // Access using non-shared group
        Group g("test.tightdb", Group::mode_ReadWrite);
        g.commit();
    }
    {
        // Modify using non-shared group
        Group g("test.tightdb", Group::mode_ReadWrite);
        g.get_table("bar"); // Add table "bar"
        g.commit();
    }
    {
        // See if we can still acces using shared group
        SharedGroup sg("test.tightdb");
        {
            ReadTransaction rt(sg);
            CHECK(rt.has_table("foo"));
            CHECK(rt.has_table("bar"));
            CHECK(!rt.has_table("baz"));
        }
        {
            WriteTransaction wt(sg);
            wt.get_table("baz"); // Add table "baz"
            wt.commit();
        }
    }
    {
        SharedGroup sg("test.tightdb");
        {
            ReadTransaction rt(sg);
            CHECK(rt.has_table("baz"));
        }
    }
    File::remove("test.tightdb");
}


TEST(GroupShared_MultipleRollbacks)
{
    File::try_remove("test.tightdb");
    {
        SharedGroup sg("test.tightdb");
        sg.begin_write();
        sg.rollback();
        sg.rollback();
    }
    File::remove("test.tightdb");
}

TEST(GroupShared_MultipleEndReads)
{
    File::try_remove("test.tightdb");
    {
        SharedGroup sg("test.tightdb");
        sg.begin_read();
        sg.end_read();
        sg.end_read();
    }
    File::remove("test.tightdb");
}


TEST(GroupShared_ReserveDiskSpace)
{
    File::try_remove("test.tightdb");
    {
        SharedGroup sg("test.tightdb");
        File::SizeType orig_file_size = File("test.tightdb").get_size();

        // Check that reserve() does not change the file size if the
        // specified size is less than the actual file size.
        File::SizeType reserve_size_1 = orig_file_size / 2;        
        //FIXME:VS2012(32bit) warning warning C4244: 'argument' : conversion from 'tightdb::File::SizeType' to 'size_t', possible loss of data
        sg.reserve(reserve_size_1);
        File::SizeType new_file_size_1 = File("test.tightdb").get_size();
        CHECK_EQUAL(orig_file_size, new_file_size_1);

        // Check that reserve() does not change the file size if the
        // specified size is equal to the actual file size.
        File::SizeType reserve_size_2 = orig_file_size;
        sg.reserve(reserve_size_2);
        File::SizeType new_file_size_2 = File("test.tightdb").get_size();
        CHECK_EQUAL(orig_file_size, new_file_size_2);

        // Check that reserve() does change the file size if the
        // specified size is greater than the actual file size, and
        // that the new size is at least as big as the requested size.
        File::SizeType reserve_size_3 = orig_file_size + 1;
        sg.reserve(reserve_size_3);
        File::SizeType new_file_size_3 = File("test.tightdb").get_size();
        CHECK(new_file_size_3 >= reserve_size_3);

        // Check that disk space reservation is independent of transactions
        {
            WriteTransaction wt(sg);
            wt.get_table<TestTableShared>("table_1")->add_empty_row(2000);
            wt.commit();
        }
        orig_file_size = File("test.tightdb").get_size();
        File::SizeType reserve_size_4 = 2 * orig_file_size + 1;
        sg.reserve(reserve_size_4);
        File::SizeType new_file_size_4 = File("test.tightdb").get_size();
        CHECK(new_file_size_4 >= reserve_size_4);
        WriteTransaction wt(sg);
        wt.get_table<TestTableShared>("table_2")->add_empty_row(2000);
        orig_file_size = File("test.tightdb").get_size();
        File::SizeType reserve_size_5 = orig_file_size + 333;
        sg.reserve(reserve_size_5);
        File::SizeType new_file_size_5 = File("test.tightdb").get_size();
        CHECK(new_file_size_5 >= reserve_size_5);
        wt.get_table<TestTableShared>("table_3")->add_empty_row(2000);
        wt.commit();
        orig_file_size = File("test.tightdb").get_size();
        File::SizeType reserve_size_6 = orig_file_size + 459;
        sg.reserve(reserve_size_6);
        File::SizeType new_file_size_6 = File("test.tightdb").get_size();
        CHECK(new_file_size_6 >= reserve_size_6);
        {
            WriteTransaction wt(sg);
            wt.commit();
        }
    }
    File::remove("test.tightdb");
}

#endif // TEST_SHARED
