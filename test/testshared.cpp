#include <UnitTest++.h>
#include "tightdb.hpp"
#include "tightdb/group_shared.hpp"

// Does not work for windows yet
#ifndef _MSC_VER

#include <unistd.h>

using namespace tightdb;

namespace {

TIGHTDB_TABLE_4(TestTableShared,
                first,  Int,
                second, Int,
                third,  Bool,
                fourth, String)

TEST(Shared_Initial)
{
    // Delete old files if there
    remove("test_shared.tightdb");
    remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup shared("test_shared.tightdb");
        CHECK(shared.is_valid());

        // Verify that new group is empty
        {
            const Group& g1 = shared.begin_read();
            CHECK(g1.is_valid());
            CHECK(g1.is_empty());
            shared.end_read();
        }

#ifdef TIGHTDB_DEBUG
        // Also do a basic ringbuffer test
        shared.test_ringbuf();
#endif
    }

    // Verify that lock file was deleted after use
    const int rc = access("test_shared.tightdb.lock", F_OK);
    CHECK_EQUAL(-1, rc);
}

} // anonymous namespace

TEST(Shared_Initial2)
{
    // Delete old files if there
    remove("test_shared.tightdb");
    remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup shared("test_shared.tightdb");
        CHECK(shared.is_valid());

        {
            // Open the same db again (in empty state)
            SharedGroup shared2("test_shared.tightdb");
            CHECK(shared2.is_valid());

            // Verify that new group is empty
            {
                const Group& g1 = shared2.begin_read();
                CHECK(g1.is_valid());
                CHECK(g1.is_empty());
                shared2.end_read();
            }

            // Add a new table
            {
                Group& g1 = shared2.begin_write();
                TestTableShared::Ref t1 = g1.get_table<TestTableShared>("test");
                t1->add(1, 2, false, "test");
                shared2.commit();
            }
        }

        // Verify that the new table has been added
        {
            const Group& g1 = shared.begin_read();
            TestTableShared::ConstRef t1 = g1.get_table<TestTableShared>("test");
            CHECK_EQUAL(1, t1->size());
            CHECK_EQUAL(1, t1[0].first);
            CHECK_EQUAL(2, t1[0].second);
            CHECK_EQUAL(false, t1[0].third);
            CHECK_EQUAL("test", (const char*)t1[0].fourth);
            shared.end_read();
        }
    }

    // Verify that lock file was deleted after use
    const int rc = access("test_shared.tightdb.lock", F_OK);
    CHECK_EQUAL(-1, rc);
}

TEST(Shared1)
{
    // Delete old files if there
    remove("test_shared.tightdb");
    remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup shared("test_shared.tightdb");
        CHECK(shared.is_valid());

        // Create first table in group
        {
            Group& g1 = shared.begin_write();
            TestTableShared::Ref t1 = g1.get_table<TestTableShared>("test");
            t1->add(1, 2, false, "test");
            shared.commit();
        }

        // Open same db again
        SharedGroup shared2("test_shared.tightdb");
        CHECK(shared2.is_valid());
        {
            const Group& g2 = shared2.begin_read();

            // Verify that last set of changes are commited
            TestTableShared::ConstRef t2 = g2.get_table<TestTableShared>("test");
            CHECK(t2->size() == 1);
            CHECK_EQUAL(1, t2[0].first);
            CHECK_EQUAL(2, t2[0].second);
            CHECK_EQUAL(false, t2[0].third);
            CHECK_EQUAL("test", (const char*)t2[0].fourth);
            // don't end_read yet

            // Do a new change while stil having current read transaction open
            {
                Group& g1 = shared.begin_write();
                TestTableShared::Ref t1 = g1.get_table<TestTableShared>("test");
                t1->add(2, 3, true, "more test");
                shared.commit();
            }

            // Verify that that the read transaction does not see
            // the change yet (is isolated)
            CHECK(t2->size() == 1);
            CHECK_EQUAL(1, t2[0].first);
            CHECK_EQUAL(2, t2[0].second);
            CHECK_EQUAL(false, t2[0].third);
            CHECK_EQUAL("test", (const char*)t2[0].fourth);

            // Do one more new change while stil having current read transaction open
            // so we know that it does not overwrite data held by
            {
                Group& g1 = shared.begin_write();
                TestTableShared::Ref t1 = g1.get_table<TestTableShared>("test");
                t1->add(0, 1, false, "even more test");
                shared.commit();
            }

            // Verify that that the read transaction does still not see
            // the change yet (is isolated)
            CHECK(t2->size() == 1);
            CHECK_EQUAL(1, t2[0].first);
            CHECK_EQUAL(2, t2[0].second);
            CHECK_EQUAL(false, t2[0].third);
            CHECK_EQUAL("test", (const char*)t2[0].fourth);

            // Close read transaction
            shared2.end_read();
        }

        // Start a new read transaction and verify that it can now see the changes
        {
            const Group& g3 = shared2.begin_read();
            TestTableShared::ConstRef t3 = g3.get_table<TestTableShared>("test");

            CHECK(t3->size() == 3);
            CHECK_EQUAL(1, t3[0].first);
            CHECK_EQUAL(2, t3[0].second);
            CHECK_EQUAL(false, t3[0].third);
            CHECK_EQUAL("test", (const char*)t3[0].fourth);
            CHECK_EQUAL(2, t3[1].first);
            CHECK_EQUAL(3, t3[1].second);
            CHECK_EQUAL(true, t3[1].third);
            CHECK_EQUAL("more test", (const char*)t3[1].fourth);
            CHECK_EQUAL(0, t3[2].first);
            CHECK_EQUAL(1, t3[2].second);
            CHECK_EQUAL(false, t3[2].third);
            CHECK_EQUAL("even more test", (const char*)t3[2].fourth);

            shared2.end_read();
        }
    }

    // Verify that lock file was deleted after use
    const int rc = access("test_shared.tightdb.lock", F_OK);
    CHECK_EQUAL(-1, rc);
}

TEST(Shared_rollback)
{
    // Delete old files if there
    remove("test_shared.tightdb");
    remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup shared("test_shared.tightdb");
        CHECK(shared.is_valid());

        // Create first table in group (but rollback)
        {
            Group& g1 = shared.begin_write();
            TestTableShared::Ref t1 = g1.get_table<TestTableShared>("test");
            t1->add(1, 2, false, "test");
            shared.rollback();
        }

        // Verify that no changes were made
        {
            const Group& g1 = shared.begin_read();
            CHECK_EQUAL(false, g1.has_table("test"));
            shared.end_read();
        }

        // Really create first table in group
        {
            Group& g1 = shared.begin_write();
            TestTableShared::Ref t1 = g1.get_table<TestTableShared>("test");
            t1->add(1, 2, false, "test");
            shared.commit();
        }

        // Verify that the changes were made
        {
            const Group& g1 = shared.begin_read();
            TestTableShared::ConstRef t = g1.get_table<TestTableShared>("test");
            CHECK(t->size() == 1);
            CHECK_EQUAL(1, t[0].first);
            CHECK_EQUAL(2, t[0].second);
            CHECK_EQUAL(false, t[0].third);
            CHECK_EQUAL("test", (const char*)t[0].fourth);
            shared.end_read();
        }

        // Greate more changes (but rollback)
        {
            Group& g1 = shared.begin_write();
            TestTableShared::Ref t1 = g1.get_table<TestTableShared>("test");
            t1->add(0, 0, true, "more test");
            shared.rollback();
        }

        // Verify that no changes were made
        {
            const Group& g1 = shared.begin_read();
            TestTableShared::ConstRef t = g1.get_table<TestTableShared>("test");
            CHECK(t->size() == 1);
            CHECK_EQUAL(1, t[0].first);
            CHECK_EQUAL(2, t[0].second);
            CHECK_EQUAL(false, t[0].third);
            CHECK_EQUAL("test", (const char*)t[0].fourth);
            shared.end_read();
        }
    }

    // Verify that lock file was deleted after use
    const int rc = access("test_shared.tightdb.lock", F_OK);
    CHECK_EQUAL(-1, rc);
}

TEST(Shared_Writes)
{
    // Delete old files if there
    remove("test_shared.tightdb");
    remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup shared("test_shared.tightdb");
        CHECK(shared.is_valid());

        // Create first table in group
        {
            Group& g1 = shared.begin_write();
            TestTableShared::Ref t1 = g1.get_table<TestTableShared>("test");
            t1->add(0, 2, false, "test");
            shared.commit();
        }

        // Do a lot of repeated write transactions
        for (size_t i = 0; i < 100; ++i) {
            Group& g1 = shared.begin_write();
            TestTableShared::Ref t1 = g1.get_table<TestTableShared>("test");
            t1[0].first += 1;
            shared.commit();
        }

        // Verify that the changes were made
        {
            const Group& g1 = shared.begin_read();
            TestTableShared::ConstRef t = g1.get_table<TestTableShared>("test");
            const int64_t v = t[0].first;
            CHECK_EQUAL(100, v);
            shared.end_read();
        }
    }

    // Verify that lock file was deleted after use
    const int rc = access("test_shared.tightdb.lock", F_OK);
    CHECK_EQUAL(-1, rc);
}

namespace {

TIGHTDB_TABLE_1(MyTable_SpecialOrder, first,  Int)

} // anonymous namespace

TEST(Shared_Writes_SpecialOrder)
{
    remove("test.tightdb");
    remove("test.tightdb.lock");

    SharedGroup db("test.tightdb");
    CHECK(db.is_valid());

    const int num_rows = 5; // FIXME: Should be strictly greater than MAX_LIST_SIZE, but that takes a loooooong time!
    const int num_reps = 25;

    {
        Group& group = db.begin_write();
        MyTable_SpecialOrder::Ref table = group.get_table<MyTable_SpecialOrder>("test");
        for (int i=0; i<num_rows; ++i) {
            table->add(0);
        }
    }
    db.commit();

    for (int i=0; i<num_rows; ++i) {
        for (int j=0; j<num_reps; ++j) {
            {
                Group& group = db.begin_write();
                MyTable_SpecialOrder::Ref table = group.get_table<MyTable_SpecialOrder>("test");
                CHECK_EQUAL(j, table[i].first);
                ++table[i].first;
            }
            db.commit();
        }
    }

    {
        const Group& group = db.begin_read();
        MyTable_SpecialOrder::ConstRef table = group.get_table<MyTable_SpecialOrder>("test");
        for (int i=0; i<num_rows; ++i) {
            CHECK_EQUAL(num_reps, table[i].first);
        }
    }
    db.end_read();
}

namespace  {

void* IncrementEntry(void* arg);

void* IncrementEntry(void* arg )
{
    const size_t row_id = (size_t)arg;

    // Open shared db
    SharedGroup shared("test_shared.tightdb");
    CHECK(shared.is_valid());

    for (size_t i = 0; i < 100; ++i) {
        // Increment cell
        {
            Group& g1 = shared.begin_write();
            TestTableShared::Ref t1 = g1.get_table<TestTableShared>("test");
            t1[row_id].first += 1;
            shared.commit();
        }

        // Verify in new transaction so that we interleave
        // read and write transactions
        {
            const Group& g1 = shared.begin_read();
            TestTableShared::ConstRef t = g1.get_table<TestTableShared>("test");

            const int64_t v = t[row_id].first;
            const int64_t expected = i+1;
            CHECK_EQUAL(expected, v);

            shared.end_read();
        }
    }
    return NULL;
}

} // anonymous namespace

TEST(Shared_WriterThreads)
{
    // Delete old files if there
    remove("test_shared.tightdb");
    remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup shared("test_shared.tightdb");
        CHECK(shared.is_valid());

        const size_t thread_count = 10;

        // Create first table in group
        {
            Group& g1 = shared.begin_write();
            TestTableShared::Ref t1 = g1.get_table<TestTableShared>("test");
            for (size_t i = 0; i < thread_count; ++i) {
                t1->add(0, 2, false, "test");
            }
            shared.commit();
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
            const Group& g1 = shared.begin_read();
            TestTableShared::ConstRef t = g1.get_table<TestTableShared>("test");

            for (size_t i = 0; i < thread_count; ++i) {
                const int64_t v = t[i].first;
                CHECK_EQUAL(100, v);
            }
            shared.end_read();
        }
    }

    // Verify that lock file was deleted after use
    const int rc = access("test_shared.tightdb.lock", F_OK);
    CHECK_EQUAL(-1, rc);
}


TEST(Shared_FormerErrorCase1)
{
    remove("test_shared.tightdb");
    remove("test_shared.tightdb.lock");
    SharedGroup db("test_shared.tightdb");
    CHECK(db.is_valid());
    {
        Group& group = db.begin_write();
        TableRef table = group.get_table("my_table");
        {
            Spec& spec = table->get_spec();
            spec.add_column(COLUMN_TYPE_INT, "alpha");
            spec.add_column(COLUMN_TYPE_BOOL, "beta");
            spec.add_column(COLUMN_TYPE_INT, "gamma");
            spec.add_column(COLUMN_TYPE_DATE, "delta");
            spec.add_column(COLUMN_TYPE_STRING, "epsilon");
            spec.add_column(COLUMN_TYPE_BINARY, "zeta");
            {
                Spec subspec = spec.add_subtable_column("eta");
                subspec.add_column(COLUMN_TYPE_INT, "foo");
                {
                    Spec subsubspec = subspec.add_subtable_column("bar");
                    subsubspec.add_column(COLUMN_TYPE_INT, "value");
                }
            }
            spec.add_column(COLUMN_TYPE_MIXED, "theta");
        }
        table->update_from_spec();
        table->insert_empty_row(0, 1);
    }
    db.commit();

    {
        Group& group = db.begin_write();
        static_cast<void>(group);
    }
    db.commit();

    {
        Group& group = db.begin_write();
        {
            TableRef table = group.get_table("my_table");
            table->set_int(0, 0, 1);
        }
    }
    db.commit();

    {
        Group& group = db.begin_write();
        {
            TableRef table = group.get_table("my_table");
            table->set_int(0, 0, 2);
        }
    }
    db.commit();

    {
        Group& group = db.begin_write();
        {
            TableRef table = group.get_table("my_table");
            TableRef table2 = table->get_subtable(6, 0);
            table2->insert_int(0, 0, 0);
            table2->insert_subtable(1, 0);
            table2->insert_done();
        }
        {
            TableRef table = group.get_table("my_table");
            table->set_int(0, 0, 3);
        }
    }
    db.commit();

    {
        Group& group = db.begin_write();
        {
            TableRef table = group.get_table("my_table");
            table->set_int(0, 0, 4);
        }
    }
    db.commit();

    {
        Group& group = db.begin_write();
        {
            TableRef table = group.get_table("my_table");
            TableRef table2 = table->get_subtable(6, 0);
            TableRef table3 = table2->get_subtable(1, 0);
            table3->insert_empty_row(0, 1);
        }
    }
    db.commit();

    {
        Group& group = db.begin_write();
        {
            TableRef table = group.get_table("my_table");
            TableRef table2 = table->get_subtable(6, 0);
            TableRef table3 = table2->get_subtable(1, 0);
            table3->insert_empty_row(1, 1);
        }
    }
    db.commit();

    {
        Group& group = db.begin_write();
        {
            TableRef table = group.get_table("my_table");
            TableRef table2 = table->get_subtable(6, 0);
            TableRef table3 = table2->get_subtable(1, 0);
            table3->set_int(0, 0, 0);
        }
        {
            TableRef table = group.get_table("my_table");
            table->set_int(0, 0, 5);
        }
        {
            TableRef table = group.get_table("my_table");
            TableRef table2 = table->get_subtable(6, 0);
            table2->set_int(0, 0, 1);
        }
    }
    db.commit();

    {
        Group& group = db.begin_write();
        TableRef table = group.get_table("my_table");
        table = table->get_subtable(6, 0);
        table = table->get_subtable(1, 0);
        table->set_int(0, 1, 1);
        table = group.get_table("my_table");
        table->set_int(0, 0, 6);
        table = group.get_table("my_table");
        table = table->get_subtable(6, 0);
        table->set_int(0, 0, 2);
    }
    db.commit();
}



namespace {

TIGHTDB_TABLE_1(FormerErrorCase2_Subtable,
                value,  Int)

TIGHTDB_TABLE_1(FormerErrorCase2_Table,
                bar, Subtable<FormerErrorCase2_Subtable>)

} // namespace

TEST(Shared_FormerErrorCase2)
{
    remove("test_shared.tightdb");
    remove("test_shared.tightdb.lock");

    for (int i=0; i<10; ++i) {
        SharedGroup db("test_shared.tightdb");
        CHECK(db.is_valid());
        {
            Group& group = db.begin_write();
            FormerErrorCase2_Table::Ref table = group.get_table<FormerErrorCase2_Table>("table");
            table->add();
            table->add();
            table->add();
            table->add();
            table->add();
            table->clear();
            table->add();
            table[0].bar->add();
        }
        db.commit();
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
        remove("over_alloc_1.tightdb");
        remove("over_alloc_1.tightdb.lock");
        SharedGroup db("over_alloc_1.tightdb");
        CHECK(db.is_valid());

        // Do a lot of sequential transactions
        for (int i = 0; i < n_outer; ++i) {
            {
                Group& group = db.begin_write();
                OverAllocTable::Ref table = group.get_table<OverAllocTable>("my_table");
                for (int j = 0; j < n_inner; ++j) {
                    table->add("x");
                }
            }
            db.commit();
        }

        // Verify that all was added correctly
        {
            const Group& group = db.begin_read();
            OverAllocTable::ConstRef table = group.get_table<OverAllocTable>("my_table");

            const size_t count = table->size();
            CHECK_EQUAL(n_outer * n_inner, count);

            for (size_t i = 0; i < count; ++i) {
                CHECK_EQUAL("x", table[i].text);
            }

#ifdef TIGHTDB_DEBUG
            table->Verify();
#endif

            db.end_read();
        }
    }
}

TEST(Shared_Notifications)
{
    // Delete old files if there
    remove("test_shared.tightdb");
    remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup shared("test_shared.tightdb");
        CHECK(shared.is_valid());

        // No other instance have changed db since last transaction
        CHECK(!shared.has_changed());

        {
            // Open the same db again (in empty state)
            SharedGroup shared2("test_shared.tightdb");
            CHECK(shared2.is_valid());

            // Verify that new group is empty
            {
                const Group& g1 = shared2.begin_read();
                CHECK(g1.is_valid());
                CHECK(g1.is_empty());
                shared2.end_read();
            }

            // No other instance have changed db since last transaction
            CHECK(!shared2.has_changed());

            // Add a new table
            {
                Group& g1 = shared2.begin_write();
                CHECK(g1.is_valid());
                TestTableShared::Ref t1 = g1.get_table<TestTableShared>("test");
                t1->add(1, 2, false, "test");
                shared2.commit();
            }
        }

        // Db has been changed by other instance
        CHECK(shared.has_changed());

        // Verify that the new table has been added
        {
            const Group& g1 = shared.begin_read();
            CHECK(g1.is_valid());

            TestTableShared::ConstRef t1 = g1.get_table<TestTableShared>("test");
            CHECK_EQUAL(1, t1->size());
            CHECK_EQUAL(1, t1[0].first);
            CHECK_EQUAL(2, t1[0].second);
            CHECK_EQUAL(false, t1[0].third);
            CHECK_EQUAL("test", (const char*)t1[0].fourth);
            shared.end_read();
        }

        // No other instance have changed db since last transaction
        CHECK(!shared.has_changed());
    }
}

TEST(Shared_FromSerialized)
{
    // Delete old files if there
    remove("test_shared.tdb");
    remove("test_shared.tdb.lock"); // also the info file

    // Create new group and serialize to disk
    {
        Group g1;
        TestTableShared::Ref t1 = g1.get_table<TestTableShared>("test");
        t1->add(1, 2, false, "test");
        g1.write("test_shared.tdb");
    }

    // Open same file as shared group
    SharedGroup shared("test_shared.tdb");
    CHECK(shared.is_valid());

    // Verify that contents is there when shared
    {
        const Group& g1 = shared.begin_read();
        CHECK(g1.is_valid());

        TestTableShared::ConstRef t1 = g1.get_table<TestTableShared>("test");
        CHECK_EQUAL(1, t1->size());
        CHECK_EQUAL(1, t1[0].first);
        CHECK_EQUAL(2, t1[0].second);
        CHECK_EQUAL(false, t1[0].third);
        CHECK_EQUAL("test", (const char*)t1[0].fourth);
        shared.end_read();
    }
}


#endif // !_MSV_VER
