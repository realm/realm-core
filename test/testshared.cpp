#include <UnitTest++.h>
#include "tightdb.hpp"
#include "tightdb/group_shared.hpp"

// Does not work for windows yet
#ifndef _MSC_VER

using namespace tightdb;

TIGHTDB_TABLE_4(TestTableShared,
                first,  Int,
                second, Int,
                third,  Bool,
                fourth, String)

TEST(Shared1)
{
    // Delete old files if there
    remove("test_shared.tdb");
    remove("test_shared.tdb.lock"); // also the info file
    
    // Create a new shared db
    SharedGroup shared("test_shared.tdb");
    CHECK(shared.is_valid());
    
    // Create first table in group
    {
        Group& g1 = shared.start_write();
        TestTableShared::Ref t1 = g1.get_table<TestTableShared>("test");
        t1->add(1, 2, false, "test");
        shared.end_write();
    }
    
    // Open same db again
    SharedGroup shared2("test_shared.tdb");
    CHECK(shared2.is_valid());
    {
        const Group& g2 = shared2.start_read();
        
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
            Group& g1 = shared.start_write();
            TestTableShared::Ref t1 = g1.get_table<TestTableShared>("test");
            t1->add(2, 3, true, "more test");
            shared.end_write();
        }
    
        // Verify that that the read transaction does not see
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
        const Group& g3 = shared2.start_read();
        TestTableShared::ConstRef t3 = g3.get_table<TestTableShared>("test");
        
        CHECK(t3->size() == 2);
        CHECK_EQUAL(1, t3[0].first);
        CHECK_EQUAL(2, t3[0].second);
        CHECK_EQUAL(false, t3[0].third);
        CHECK_EQUAL("test", (const char*)t3[0].fourth);
        CHECK_EQUAL(2, t3[1].first);
        CHECK_EQUAL(3, t3[1].second);
        CHECK_EQUAL(true, t3[1].third);
        CHECK_EQUAL("more test", (const char*)t3[1].fourth);
        
        shared2.end_read();
    }

#ifdef _DEBUG
    shared.test_ringbuf();
#endif
}

#endif //_MSV_VER