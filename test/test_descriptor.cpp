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

#include <realm/descriptor.hpp>
#include <realm/group.hpp>

#include "test.hpp"

using namespace realm;


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


/*

FIXME: Test: Multiple subdescs, insert subdescriptor, check that all
subdescriptors are still attached and still work. Also check that
subtable accessors are detached.

FIXME: Test: Multiple subdescs, then remove column, then use others,
and theck that they are still attached. Also check that removed
descriptor is detached. Also check that subtable accessors are
detached.

FIXME: Test: Multiple subdescs, rename column, check that all
subdescriptors are still attached and still work. Also check that
subtable accessors are detached.

*/


TEST(Descriptor_Basics)
{
    TableRef table = Table::create();
    DescriptorRef desc = table->get_descriptor();
    CHECK(table->is_attached());
    CHECK(desc->is_attached());
    CHECK_EQUAL(0, desc->get_column_count());
    CHECK_EQUAL(not_found, desc->get_column_index("foo"));

    // The descriptor accessor must be unique
    {
        DescriptorRef desc_2 = table->get_descriptor();
        CHECK_EQUAL(desc, desc_2);
    }

    // A root descriptor must have no parent
    CHECK(!desc->get_parent());

    // The root table of a root descriptor must be the table from
    // which the root descriptor was acquired
    {
        TableRef table_2 = desc->get_root_table();
        CHECK_EQUAL(table, table_2);
    }

    // Check that descriptor keeps table alive
    table.reset();
    CHECK(desc->is_attached());
    table = desc->get_root_table();
    CHECK(desc->is_attached());
    CHECK(table->is_attached());
    {
        DescriptorRef desc_2 = table->get_descriptor();
        CHECK_EQUAL(desc, desc_2);
    }

    // Add column
    desc->add_column(type_Int, "beta");
    CHECK_EQUAL(1, desc->get_column_count());
    CHECK_EQUAL(type_Int, desc->get_column_type(0));
    CHECK_EQUAL("beta", desc->get_column_name(0));
    CHECK_EQUAL(not_found, desc->get_column_index("foo"));
    CHECK_EQUAL(0, desc->get_column_index("beta"));

    // Insert column
    desc->insert_column(0, type_Bool, "alpha");
    CHECK_EQUAL(2, desc->get_column_count());
    CHECK_EQUAL(type_Bool, desc->get_column_type(0));
    CHECK_EQUAL(type_Int, desc->get_column_type(1));
    CHECK_EQUAL("alpha", desc->get_column_name(0));
    CHECK_EQUAL("beta", desc->get_column_name(1));
    CHECK_EQUAL(not_found, desc->get_column_index("foo"));
    CHECK_EQUAL(0, desc->get_column_index("alpha"));
    CHECK_EQUAL(1, desc->get_column_index("beta"));

    // Rename column
    desc->rename_column(0, "alpha_2");
    CHECK_EQUAL(2, desc->get_column_count());
    CHECK_EQUAL(type_Bool, desc->get_column_type(0));
    CHECK_EQUAL(type_Int, desc->get_column_type(1));
    CHECK_EQUAL("alpha_2", desc->get_column_name(0));
    CHECK_EQUAL("beta", desc->get_column_name(1));
    CHECK_EQUAL(not_found, desc->get_column_index("alpha"));
    CHECK_EQUAL(0, desc->get_column_index("alpha_2"));
    CHECK_EQUAL(1, desc->get_column_index("beta"));
    desc->rename_column(1, "beta_2");
    CHECK_EQUAL(2, desc->get_column_count());
    CHECK_EQUAL(type_Bool, desc->get_column_type(0));
    CHECK_EQUAL(type_Int, desc->get_column_type(1));
    CHECK_EQUAL("alpha_2", desc->get_column_name(0));
    CHECK_EQUAL("beta_2", desc->get_column_name(1));
    CHECK_EQUAL(not_found, desc->get_column_index("beta"));
    CHECK_EQUAL(0, desc->get_column_index("alpha_2"));
    CHECK_EQUAL(1, desc->get_column_index("beta_2"));

    // Remove column
    desc->remove_column(0); // alpha_2
    CHECK_EQUAL(1, desc->get_column_count());
    CHECK_EQUAL(type_Int, desc->get_column_type(0));
    CHECK_EQUAL("beta_2", desc->get_column_name(0));
    CHECK_EQUAL(not_found, desc->get_column_index("foo"));
    CHECK_EQUAL(0, desc->get_column_index("beta_2"));
    desc->remove_column(0); // beta_2
    CHECK_EQUAL(0, desc->get_column_count());
    CHECK_EQUAL(not_found, desc->get_column_index("foo"));
}


TEST(Descriptor_MoveColumn)
{
    using df = _impl::DescriptorFriend;

    TableRef table = Table::create();
    DescriptorRef desc = table->get_descriptor();

    desc->add_column(type_Int, "alpha");
    desc->add_column(type_Int, "beta");
    desc->add_column(type_Int, "gamma");
    desc->add_column(type_Int, "delta");

    // Sanity check
    CHECK_EQUAL(4, desc->get_column_count());
    CHECK_EQUAL("alpha", desc->get_column_name(0));
    CHECK_EQUAL("beta", desc->get_column_name(1));
    CHECK_EQUAL("gamma", desc->get_column_name(2));
    CHECK_EQUAL("delta", desc->get_column_name(3));

    // Move up:
    df::move_column(*desc, 1, 3);
    CHECK_EQUAL("alpha", desc->get_column_name(0));
    CHECK_EQUAL("gamma", desc->get_column_name(1));
    CHECK_EQUAL("delta", desc->get_column_name(2));
    CHECK_EQUAL("beta", desc->get_column_name(3));

    // Move down:
    df::move_column(*desc, 2, 0);
    CHECK_EQUAL("alpha", desc->get_column_name(1));
    CHECK_EQUAL("gamma", desc->get_column_name(2));
    CHECK_EQUAL("delta", desc->get_column_name(0));
    CHECK_EQUAL("beta", desc->get_column_name(3));
}


TEST(Descriptor_MoveColumnSparseLinks)
{
    using df = _impl::DescriptorFriend;

    Group group;
    TableRef table = group.add_table("t");
    TableRef t1 = group.add_table("t1");
    TableRef t2 = group.add_table("t2");
    TableRef t3 = group.add_table("t3");
    TableRef t4 = group.add_table("t4");
    TableRef t5 = group.add_table("t5");
    DescriptorRef desc = table->get_descriptor();

    table->add_column(type_Int, "i0");
    table->add_column_link(type_Link, "l1", *t1);
    table->add_column(type_Int, "i2");
    table->add_column_link(type_Link, "l3", *t3);
    table->add_column(type_Int, "i4");
    table->add_column_link(type_Link, "l5", *t5);
    table->add_column(type_Int, "i6");

    auto check_original_order = [this, &desc]() {
        // Sanity check
        CHECK_EQUAL(7, desc->get_column_count());
        CHECK_EQUAL("i0", desc->get_column_name(0));
        CHECK_EQUAL("l1", desc->get_column_name(1));
        CHECK_EQUAL("i2", desc->get_column_name(2));
        CHECK_EQUAL("l3", desc->get_column_name(3));
        CHECK_EQUAL("i4", desc->get_column_name(4));
        CHECK_EQUAL("l5", desc->get_column_name(5));
        CHECK_EQUAL("i6", desc->get_column_name(6));
        CHECK_EQUAL(1, desc->get_column_link_target(1));
        CHECK_EQUAL(3, desc->get_column_link_target(3));
        CHECK_EQUAL(5, desc->get_column_link_target(5));
    };
    check_original_order();
    group.verify();

    df::move_column(*desc, 1, 3); // link to link move
    CHECK_EQUAL("i2", desc->get_column_name(1));
    CHECK_EQUAL("l1", desc->get_column_name(3));
    CHECK_EQUAL(3, desc->get_column_link_target(2));
    CHECK_EQUAL(1, desc->get_column_link_target(3));
    CHECK_EQUAL(5, desc->get_column_link_target(5));
    group.verify();

    df::move_column(*desc, 3, 1); // undo
    check_original_order();
    group.verify();


    df::move_column(*desc, 0, 5); // non link to link
    CHECK_EQUAL("l1", desc->get_column_name(0));
    CHECK_EQUAL("i0", desc->get_column_name(5));
    CHECK_EQUAL(1, desc->get_column_link_target(0));
    CHECK_EQUAL(3, desc->get_column_link_target(2));
    CHECK_EQUAL(5, desc->get_column_link_target(4));
    group.verify();

    df::move_column(*desc, 5, 0); // undo
    check_original_order();
    group.verify();

    df::move_column(*desc, 1, 6); // link to non link
    CHECK_EQUAL("i2", desc->get_column_name(1));
    CHECK_EQUAL("l1", desc->get_column_name(6));
    CHECK_EQUAL(3, desc->get_column_link_target(2));
    CHECK_EQUAL(5, desc->get_column_link_target(4));
    CHECK_EQUAL(1, desc->get_column_link_target(6));
    group.verify();

    df::move_column(*desc, 6, 1); // undo
    check_original_order();
    group.verify();
}


TEST(Descriptor_EmptyAndDuplicateNames)
{
    TableRef table = Table::create();
    DescriptorRef desc = table->get_descriptor();
    desc->add_column(type_Bool, "alpha");   // 0
    desc->add_column(type_Int, "beta");     // 1
    desc->add_column(type_Double, "");      // 2
    desc->add_column(type_String, "alpha"); // 3
    desc->add_column(type_Int, "beta");     // 4
    desc->add_column(type_Float, "");       // 5
    desc->add_column(type_Bool, "gamma");   // 6
    desc->add_column(type_Double, "gamma"); // 7
    desc->add_column(type_String, "");      // 8
    CHECK(table->is_attached());
    CHECK(desc->is_attached());
    CHECK_EQUAL(9, desc->get_column_count());
    CHECK_EQUAL(type_Bool, desc->get_column_type(0));
    CHECK_EQUAL(type_Int, desc->get_column_type(1));
    CHECK_EQUAL(type_Double, desc->get_column_type(2));
    CHECK_EQUAL(type_String, desc->get_column_type(3));
    CHECK_EQUAL(type_Int, desc->get_column_type(4));
    CHECK_EQUAL(type_Float, desc->get_column_type(5));
    CHECK_EQUAL(type_Bool, desc->get_column_type(6));
    CHECK_EQUAL(type_Double, desc->get_column_type(7));
    CHECK_EQUAL(type_String, desc->get_column_type(8));
    CHECK_EQUAL("alpha", desc->get_column_name(0));
    CHECK_EQUAL("beta", desc->get_column_name(1));
    CHECK_EQUAL("", desc->get_column_name(2));
    CHECK_EQUAL("alpha", desc->get_column_name(3));
    CHECK_EQUAL("beta", desc->get_column_name(4));
    CHECK_EQUAL("", desc->get_column_name(5));
    CHECK_EQUAL("gamma", desc->get_column_name(6));
    CHECK_EQUAL("gamma", desc->get_column_name(7));
    CHECK_EQUAL("", desc->get_column_name(8));
    CHECK_EQUAL(not_found, desc->get_column_index("foo"));
    CHECK_EQUAL(0, desc->get_column_index("alpha"));
    CHECK_EQUAL(1, desc->get_column_index("beta"));
    CHECK_EQUAL(6, desc->get_column_index("gamma"));
    CHECK_EQUAL(2, desc->get_column_index(""));
}



TEST(Descriptor_IllegalOps)
{
    // Detached accessor
    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column_link(type_Link, "link", *table);
        DescriptorRef desc = table->get_descriptor();
        group.remove_table("table");
        if (CHECK(!desc->is_attached())) {
            CHECK_LOGIC_ERROR(desc->add_column(type_Int, ""), LogicError::detached_accessor);
            CHECK_LOGIC_ERROR(desc->insert_column(0, type_Int, ""), LogicError::detached_accessor);
            CHECK_LOGIC_ERROR(desc->add_column_link(type_Link, "", *table), LogicError::detached_accessor);
            CHECK_LOGIC_ERROR(desc->insert_column_link(0, type_Link, "", *table), LogicError::detached_accessor);
            CHECK_LOGIC_ERROR(desc->remove_column(0), LogicError::detached_accessor);
            CHECK_LOGIC_ERROR(desc->rename_column(0, "foo"), LogicError::detached_accessor);
            CHECK_LOGIC_ERROR(desc->set_link_type(0, link_Strong), LogicError::detached_accessor);
        }
    }

    // Detached link target
    {
        Group group;
        TableRef origin = group.add_table("origin");
        TableRef target = group.add_table("target");
        group.remove_table("target");
        DescriptorRef desc = origin->get_descriptor();
        CHECK_LOGIC_ERROR(desc->add_column_link(type_Link, "", *target), LogicError::detached_accessor);
        CHECK_LOGIC_ERROR(desc->insert_column_link(0, type_Link, "", *target), LogicError::detached_accessor);
    }

    // Column index out of range
    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column_link(type_Link, "link", *table);
        DescriptorRef desc = table->get_descriptor();
        CHECK_LOGIC_ERROR(desc->insert_column(2, type_Int, ""), LogicError::column_index_out_of_range);
        CHECK_LOGIC_ERROR(desc->insert_column_link(2, type_Link, "", *table), LogicError::column_index_out_of_range);
        CHECK_LOGIC_ERROR(desc->remove_column(1), LogicError::column_index_out_of_range);
        CHECK_LOGIC_ERROR(desc->rename_column(1, "foo"), LogicError::column_index_out_of_range);
        CHECK_LOGIC_ERROR(desc->set_link_type(1, link_Strong), LogicError::column_index_out_of_range);
    }

    // Illegal data type
    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column(type_Int, "int");
        DescriptorRef desc = table->get_descriptor();
        CHECK_LOGIC_ERROR(desc->add_column(type_Link, ""), LogicError::illegal_type);
        CHECK_LOGIC_ERROR(desc->add_column_link(type_Int, "", *table), LogicError::illegal_type);
        CHECK_LOGIC_ERROR(desc->set_link_type(0, link_Strong), LogicError::illegal_type);
    }

    // Wrong kind of table
    {
        // Free-standing link origin
        Table origin;
        Group group;
        TableRef target = group.add_table("target");
        DescriptorRef desc = origin.get_descriptor();
        CHECK_LOGIC_ERROR(desc->add_column_link(type_Link, "link", *target), LogicError::wrong_kind_of_table);
    }
    {
        // Free-standing link target
        Group group;
        TableRef origin = group.add_table("origin");
        Table target;
        DescriptorRef desc = origin->get_descriptor();
        CHECK_LOGIC_ERROR(desc->add_column_link(type_Link, "link", target), LogicError::wrong_kind_of_table);
    }

    // Different groups
    {
        Group group_1, group_2;
        TableRef table_1 = group_1.add_table("table_1");
        TableRef table_2 = group_2.add_table("table_2");
        DescriptorRef desc = table_1->get_descriptor();
        CHECK_LOGIC_ERROR(desc->add_column_link(type_Link, "", *table_2), LogicError::group_mismatch);
    }
}


TEST(Descriptor_Links)
{
    Group group;
    TableRef origin = group.add_table("origin");
    TableRef target = group.add_table("target");
    origin->add_column_link(type_Link, "link", *target);
    CHECK_EQUAL(target, origin->get_link_target(0));
    ConstTableRef const_origin = origin;
    CHECK_EQUAL(target, const_origin->get_link_target(0));
}


TEST(Descriptor_DescriptorEqualityNulls)
{
    Table t1;
    t1.add_column(type_Int, "int");
    t1.add_column(type_String, "str");

    Table t2;
    t2.add_column(type_Int, "int", true);
    t2.add_column(type_String, "str");

    Table t3;
    t3.add_column(type_Int, "int", true);
    t3.add_column(type_String, "str");

    CHECK(*t1.get_descriptor() != *t2.get_descriptor());
    CHECK(*t2.get_descriptor() == *t3.get_descriptor());
}


TEST(Descriptor_TwoStringColumnTypesEquality)
{
    Table t1;
    Table t2;
    Table t3;
    t1.add_column(type_String, "str");
    t2.add_column(type_String, "str");
    t3.add_column(type_String, "str");
    t1.add_empty_row(10);
    t2.add_empty_row(10);
    t3.add_empty_row(10);

    for (int i = 0; i < 10; ++i) {
        t1.set_string(0, i, StringData("a", 1));
        t2.set_string(0, i, StringData("a", 1));
        t3.set_string(0, i, StringData("a", 1));
    }

    CHECK(*t1.get_descriptor() == *t2.get_descriptor()); // (col_type_String == col_type_String)

    t2.optimize();

    CHECK(*t1.get_descriptor() == *t2.get_descriptor()); // (col_type_String == col_type_StringEnum)

    t1.optimize();

    CHECK(*t1.get_descriptor() == *t3.get_descriptor()); // (col_type_StringEnum == col_type_String)

    t3.optimize();

    CHECK(*t1.get_descriptor() == *t3.get_descriptor()); // (col_type_StringEnum == col_type_StringEunm)
}


TEST(Descriptor_LinkEquality)
{
    Group g;
    TableRef t1 = g.add_table("t1");
    TableRef t2 = g.add_table("t2");
    TableRef t3 = g.add_table("t3");
    TableRef t4 = g.add_table("t4");
    t1->add_column(type_String, "str");
    t2->add_column(type_Int, "int");

    t3->add_column_link(type_Link, "link", *t1, link_Strong);
    t4->add_column_link(type_Link, "link", *t2, link_Strong);

    CHECK(*t3->get_descriptor() != *t4->get_descriptor());
}


TEST(Descriptor_LinkListEquality)
{
    Group g;
    TableRef t1 = g.add_table("t1");
    TableRef t2 = g.add_table("t2");
    TableRef t3 = g.add_table("t3");
    TableRef t4 = g.add_table("t4");
    t1->add_column(type_String, "str");
    t2->add_column(type_Int, "int");

    t3->add_column_link(type_LinkList, "links", *t1, link_Strong);
    t4->add_column_link(type_LinkList, "links", *t2, link_Strong);

    CHECK(*t3->get_descriptor() != *t4->get_descriptor());
}
