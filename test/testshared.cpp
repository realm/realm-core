#include <pthread.h>

#include <UnitTest++.h>

#include <tightdb/file.hpp>
#include <tightdb.hpp>

using namespace std;
using namespace tightdb;

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

void* IncrementEntry(void* arg)
{
    const size_t row_ndx = (size_t)arg;

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

            const int64_t v = t[row_ndx].first;
            const int64_t expected = i+1;
            CHECK_EQUAL(expected, v);
        }
    }
    return 0;
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

        pthread_t threads[thread_count];

        // Create all threads
        for (size_t i = 0; i < thread_count; ++i) {
            const int rc = pthread_create(&threads[i], NULL, IncrementEntry, (void*)i);
            CHECK_EQUAL(0, rc);
        }

        // Wait for all threads to complete
        for (size_t i = 0; i < thread_count; ++i) {
            const int rc = pthread_join(threads[i], NULL);
            CHECK_EQUAL(0, rc);
        }

        // Verify that the changes were made
        {
            ReadTransaction rt(sg);
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

            for (size_t i = 0; i < thread_count; ++i) {
                const int64_t v = t[i].first;
                CHECK_EQUAL(100, v);
            }
        }
    }

    // Verify that lock file was deleted after use
#ifndef _WIN32 // GroupShared cannot clean lock file on Windows
    CHECK(!File::exists("test_shared.tightdb.lock"));
#endif
}


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
            spec.add_column(type_Date, "delta");
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
    const int n_outer = 3000;
    const int n_inner = 42;

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

namespace {
void randstr(char* res, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        res[i] = 'a' + rand() % 10;
    }
}
}

TEST(StringIndex_Bug)
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
            randstr(txt, 8);
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
