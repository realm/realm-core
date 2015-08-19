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

    // The descriptor of a root table must be a root rescriptor
    CHECK(desc->is_root());

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
    CHECK_EQUAL(type_Int,  desc->get_column_type(1));
    CHECK_EQUAL("alpha", desc->get_column_name(0));
    CHECK_EQUAL("beta",  desc->get_column_name(1));
    CHECK_EQUAL(not_found, desc->get_column_index("foo"));
    CHECK_EQUAL(0, desc->get_column_index("alpha"));
    CHECK_EQUAL(1, desc->get_column_index("beta"));

    // Rename column
    desc->rename_column(0, "alpha_2");
    CHECK_EQUAL(2, desc->get_column_count());
    CHECK_EQUAL(type_Bool, desc->get_column_type(0));
    CHECK_EQUAL(type_Int,  desc->get_column_type(1));
    CHECK_EQUAL("alpha_2", desc->get_column_name(0));
    CHECK_EQUAL("beta",  desc->get_column_name(1));
    CHECK_EQUAL(not_found, desc->get_column_index("alpha"));
    CHECK_EQUAL(0, desc->get_column_index("alpha_2"));
    CHECK_EQUAL(1, desc->get_column_index("beta"));
    desc->rename_column(1, "beta_2");
    CHECK_EQUAL(2, desc->get_column_count());
    CHECK_EQUAL(type_Bool, desc->get_column_type(0));
    CHECK_EQUAL(type_Int,  desc->get_column_type(1));
    CHECK_EQUAL("alpha_2", desc->get_column_name(0));
    CHECK_EQUAL("beta_2",  desc->get_column_name(1));
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


TEST(Descriptor_EmptyAndDuplicateNames)
{
    TableRef table = Table::create();
    DescriptorRef desc = table->get_descriptor();
    desc->add_column(type_Bool,    "alpha"); // 0
    desc->add_column(type_Int,     "beta");  // 1
    desc->add_column(type_Double,  "");      // 2
    desc->add_column(type_String,  "alpha"); // 3
    desc->add_column(type_Int,     "beta");  // 4
    desc->add_column(type_Float,   "");      // 5
    desc->add_column(type_Bool,    "gamma"); // 6
    desc->add_column(type_Double,  "gamma"); // 7
    desc->add_column(type_String,  "");      // 8
    CHECK(table->is_attached());
    CHECK(desc->is_attached());
    CHECK_EQUAL(9, desc->get_column_count());
    CHECK_EQUAL(type_Bool,   desc->get_column_type(0));
    CHECK_EQUAL(type_Int,    desc->get_column_type(1));
    CHECK_EQUAL(type_Double, desc->get_column_type(2));
    CHECK_EQUAL(type_String, desc->get_column_type(3));
    CHECK_EQUAL(type_Int,    desc->get_column_type(4));
    CHECK_EQUAL(type_Float,  desc->get_column_type(5));
    CHECK_EQUAL(type_Bool,   desc->get_column_type(6));
    CHECK_EQUAL(type_Double, desc->get_column_type(7));
    CHECK_EQUAL(type_String, desc->get_column_type(8));
    CHECK_EQUAL("alpha", desc->get_column_name(0));
    CHECK_EQUAL("beta",  desc->get_column_name(1));
    CHECK_EQUAL("",      desc->get_column_name(2));
    CHECK_EQUAL("alpha", desc->get_column_name(3));
    CHECK_EQUAL("beta",  desc->get_column_name(4));
    CHECK_EQUAL("",      desc->get_column_name(5));
    CHECK_EQUAL("gamma", desc->get_column_name(6));
    CHECK_EQUAL("gamma", desc->get_column_name(7));
    CHECK_EQUAL("",      desc->get_column_name(8));
    CHECK_EQUAL(not_found, desc->get_column_index("foo"));
    CHECK_EQUAL(0,         desc->get_column_index("alpha"));
    CHECK_EQUAL(1,         desc->get_column_index("beta"));
    CHECK_EQUAL(6,         desc->get_column_index("gamma"));
    CHECK_EQUAL(2,         desc->get_column_index(""));
}


TEST(Descriptor_SubtableColumn)
{
    TableRef table = Table::create();
    DescriptorRef desc = table->get_descriptor(), subdesc;
    desc->add_column(type_Int,   "alpha");
    desc->add_column(type_Table, "beta", &subdesc);
    CHECK_EQUAL(2, desc->get_column_count());
    CHECK_EQUAL(type_Int,   desc->get_column_type(0));
    CHECK_EQUAL(type_Table, desc->get_column_type(1));
    CHECK(subdesc);
    CHECK(desc->is_attached());
    CHECK(subdesc->is_attached());
    CHECK(desc->is_root());
    CHECK(!subdesc->is_root());
    {
        DescriptorRef subdesc_2 = desc->get_subdescriptor(1);
        CHECK_EQUAL(subdesc, subdesc_2);
        subdesc_2 = table->get_subdescriptor(1);
        CHECK_EQUAL(subdesc, subdesc_2);
    }
    {
        DescriptorRef desc_2 = subdesc->get_parent();
        CHECK_EQUAL(desc, desc_2);
    }
    {
        TableRef table_2 = desc->get_root_table();
        CHECK_EQUAL(table, table_2);
        table_2 = subdesc->get_root_table();
        CHECK_EQUAL(table, table_2);
    }

    // Check that subdescriptor keeps root descriptor and root table
    // alive
    table.reset();
    CHECK(desc->is_attached());
    CHECK(subdesc->is_attached());
    desc.reset();
    CHECK(subdesc->is_attached());
    desc = subdesc->get_parent();
    CHECK(desc->is_attached());
    CHECK(subdesc->is_attached());
    CHECK(desc->is_root());
    CHECK(!subdesc->is_root());
    table = desc->get_root_table();
    CHECK(table->is_attached());
    CHECK(desc->is_attached());
    CHECK(subdesc->is_attached());
    CHECK(!table->has_shared_type());
    CHECK(desc->is_root());
    CHECK(!subdesc->is_root());
    CHECK(!desc->get_parent());
    {
        DescriptorRef desc_2 = table->get_descriptor();
        CHECK_EQUAL(desc, desc_2);
        desc_2 = subdesc->get_parent();
        CHECK_EQUAL(desc, desc_2);
    }
    {
        DescriptorRef subdesc_2 = table->get_subdescriptor(1);
        CHECK_EQUAL(subdesc, subdesc_2);
        subdesc_2 = desc->get_subdescriptor(1);
        CHECK_EQUAL(subdesc, subdesc_2);
    }

    // Test that columns can be added and removed from subdescriptor
    subdesc->add_column(type_Int,    "foo");
    subdesc->add_column(type_String, "bar");
    subdesc->remove_column(1);
    CHECK(table->is_attached());
    CHECK(desc->is_attached());
    CHECK(subdesc->is_attached());
    CHECK_EQUAL(2, desc->get_column_count());
    CHECK_EQUAL(type_Int,   desc->get_column_type(0));
    CHECK_EQUAL(type_Table, desc->get_column_type(1));
    CHECK_EQUAL("alpha", desc->get_column_name(0));
    CHECK_EQUAL("beta",  desc->get_column_name(1));
    CHECK_EQUAL(not_found, desc->get_column_index("foo"));
    CHECK_EQUAL(0, desc->get_column_index("alpha"));
    CHECK_EQUAL(1, desc->get_column_index("beta"));
    CHECK_EQUAL(1, subdesc->get_column_count());
    CHECK_EQUAL(type_Int, subdesc->get_column_type(0));
    CHECK_EQUAL("foo", subdesc->get_column_name(0));
    CHECK_EQUAL(not_found, subdesc->get_column_index("alpha"));
    CHECK_EQUAL(0, subdesc->get_column_index("foo"));

    // Test rename of subtable column
    desc->rename_column(0, "alpha_2");
    desc->rename_column(1, "beta_2");
    CHECK(table->is_attached());
    CHECK(desc->is_attached());
    CHECK(subdesc->is_attached());
    CHECK_EQUAL(2, desc->get_column_count());
    CHECK_EQUAL(type_Int,   desc->get_column_type(0));
    CHECK_EQUAL(type_Table, desc->get_column_type(1));
    CHECK_EQUAL("alpha_2", desc->get_column_name(0));
    CHECK_EQUAL("beta_2",  desc->get_column_name(1));
    CHECK_EQUAL(not_found, desc->get_column_index("alpha"));
    CHECK_EQUAL(not_found, desc->get_column_index("beta"));
    CHECK_EQUAL(0, desc->get_column_index("alpha_2"));
    CHECK_EQUAL(1, desc->get_column_index("beta_2"));

    // Remove integer column and see that the subtable column still
    // works
    desc->remove_column(0);
    CHECK(table->is_attached());
    CHECK(desc->is_attached());
    CHECK(subdesc->is_attached());
    CHECK_EQUAL(1, desc->get_column_count());
    CHECK_EQUAL(type_Table, desc->get_column_type(0));
    CHECK_EQUAL("beta_2",  desc->get_column_name(0));
    CHECK_EQUAL(not_found, desc->get_column_index("alpha_2"));
    CHECK_EQUAL(0, desc->get_column_index("beta_2"));
    {
        DescriptorRef subdesc_2 = desc->get_subdescriptor(0);
        CHECK_EQUAL(subdesc, subdesc_2);
    }
    subdesc->add_column(type_String, "bar");
    subdesc->add_column(type_Float,  "baz");
    subdesc->remove_column(2);
    CHECK(table->is_attached());
    CHECK(desc->is_attached());
    CHECK(subdesc->is_attached());
    CHECK_EQUAL(1, desc->get_column_count());
    CHECK_EQUAL(type_Table, desc->get_column_type(0));
    CHECK_EQUAL("beta_2",  desc->get_column_name(0));
    CHECK_EQUAL(not_found, desc->get_column_index("foo"));
    CHECK_EQUAL(0, desc->get_column_index("beta_2"));
    CHECK_EQUAL(2, subdesc->get_column_count());
    CHECK_EQUAL(type_Int,    subdesc->get_column_type(0));
    CHECK_EQUAL(type_String, subdesc->get_column_type(1));
    CHECK_EQUAL("foo", subdesc->get_column_name(0));
    CHECK_EQUAL("bar", subdesc->get_column_name(1));
    CHECK_EQUAL(not_found, subdesc->get_column_index("beta_2"));
    CHECK_EQUAL(0, subdesc->get_column_index("foo"));
    CHECK_EQUAL(1, subdesc->get_column_index("bar"));

    // FIXME: Test: Add a new integer column and see that the subtable
    // column still works

    // FIXME: Test: Remove the subtable column and see that the
    // integer column still works
}


TEST(Descriptor_Subtables)
{
    TableRef table = Table::create();
    DescriptorRef desc = table->get_descriptor(), subdesc, subsubdesc;
    desc->add_column(type_Table, "alpha", &subdesc);
    subdesc->add_column(type_Table, "beta", &subsubdesc);
    subdesc->add_column(type_Int, "gamma");

    // Add some subtables
    table->add_empty_row(3);
    TableRef subtab_1, subtab_2, subtab_3;
    subtab_1 = table->get_subtable(0,0);
    subtab_2 = table->get_subtable(0,1);
    subtab_3 = table->get_subtable(0,2);

    // Add second level subtables
    subtab_1->add_empty_row(1);
    subtab_2->add_empty_row(1);
    subtab_3->add_empty_row(1);

    // Check that all first level subtables have same descriptor
    CHECK_EQUAL(subdesc, subtab_1->get_descriptor());
    CHECK_EQUAL(subdesc, subtab_2->get_descriptor());
    CHECK_EQUAL(subdesc, subtab_3->get_descriptor());

    // Check that all second level subtables have same descriptor
    CHECK_EQUAL(subsubdesc, subtab_1->get_subtable(0,0)->get_descriptor());
    CHECK_EQUAL(subsubdesc, subtab_2->get_subtable(0,0)->get_descriptor());
    CHECK_EQUAL(subsubdesc, subtab_3->get_subtable(0,0)->get_descriptor());

    // Clear and reobtain fixed refs
    desc.reset();
    subdesc.reset();
    desc = table->get_descriptor();
    subdesc = desc->get_subdescriptor(0);
    table.reset();
    subtab_1.reset();
    subtab_2.reset();
    subtab_3.reset();
    desc.reset();
    desc = subdesc->get_parent();
    table = desc->get_root_table();
    subtab_1 = table->get_subtable(0,0);
    subtab_2 = table->get_subtable(0,1);
    subtab_3 = table->get_subtable(0,2);

    // Recheck
    CHECK_EQUAL(subdesc, subtab_1->get_descriptor());
    CHECK_EQUAL(subdesc, subtab_2->get_descriptor());
    CHECK_EQUAL(subdesc, subtab_3->get_descriptor());
    CHECK_EQUAL(subsubdesc, subtab_1->get_subtable(0,0)->get_descriptor());
    CHECK_EQUAL(subsubdesc, subtab_2->get_subtable(0,0)->get_descriptor());
    CHECK_EQUAL(subsubdesc, subtab_3->get_subtable(0,0)->get_descriptor());
}


TEST(Descriptor_Subtables2)
{
    TableRef table = Table::create();
    table->add_column(type_Table, "");
    table->add_empty_row(1);
    TableRef subtab = table->get_subtable(0,0);
    DescriptorRef subdesc = subtab->get_descriptor();
    table->remove_column(0);
    CHECK(!subtab->is_attached());
    CHECK(!subdesc->is_attached());
}


TEST(Descriptor_DeeplyNested)
{
    // Build a long branch of subtable columns
    TableRef table = Table::create();
    DescriptorRef desc = table->get_descriptor(), subdesc;
    for (int i = 0; i != 128; ++i) {
        desc->add_column(type_Int,   "foo");
        desc->add_column(type_Table, "bar", &subdesc);
        CHECK(subdesc);
        CHECK(!subdesc->is_root());
        desc = subdesc;
    }

    // Check that parents are correct
    for (int i = 0; i != 128; ++i) {
        desc = desc->get_parent();
        CHECK(desc);
    }
    CHECK(desc->is_root());

    // Add many more columns at each nesting level
    for (int i = 0; i != 128; ++i) {
        desc->insert_column(0, type_Int, "a");
        desc->insert_column(2, type_Int, "b");
        desc->insert_column(4, type_Int, "c");
        desc->add_column(type_Table, "baz", &subdesc);
        for (int i_2 = 0; i_2 != i; ++i_2)
            subdesc->add_column(type_Bool, "dummy");
        desc = desc->get_subdescriptor(3); // bar
    }

    // Check that everything is all right
    desc = table->get_descriptor();
    for (int i = 0; i != 128; ++i) {
        CHECK(desc->is_attached());
        CHECK(desc->is_root() == (i == 0));
        CHECK(bool(desc->get_parent()) == (i != 0));
        CHECK_EQUAL(table, desc->get_root_table());
        CHECK_EQUAL(6, desc->get_column_count());
        CHECK_EQUAL(type_Int,   desc->get_column_type(0));
        CHECK_EQUAL(type_Int,   desc->get_column_type(1));
        CHECK_EQUAL(type_Int,   desc->get_column_type(2));
        CHECK_EQUAL(type_Table, desc->get_column_type(3));
        CHECK_EQUAL(type_Int,   desc->get_column_type(4));
        CHECK_EQUAL(type_Table, desc->get_column_type(5));
        CHECK_EQUAL("a",   desc->get_column_name(0));
        CHECK_EQUAL("foo", desc->get_column_name(1));
        CHECK_EQUAL("b",   desc->get_column_name(2));
        CHECK_EQUAL("bar", desc->get_column_name(3));
        CHECK_EQUAL("c",   desc->get_column_name(4));
        CHECK_EQUAL("baz", desc->get_column_name(5));
        subdesc = desc->get_subdescriptor(5); // baz
        CHECK(subdesc);
        CHECK(subdesc->is_attached());
        CHECK(!subdesc->is_root());
        CHECK_EQUAL(i, subdesc->get_column_count());
        for (int i_2 = 0; i_2 != i; ++i_2) {
            CHECK_EQUAL(type_Bool, subdesc->get_column_type(i_2));
            CHECK_EQUAL("dummy",   subdesc->get_column_name(i_2));
        }
        CHECK_EQUAL(not_found, subdesc->get_column_index("foo"));
        CHECK_EQUAL(i == 0 ? not_found : 0,
                    subdesc->get_column_index("dummy"));
        subdesc = desc->get_subdescriptor(3); // bar
        CHECK_EQUAL(desc, subdesc->get_parent());
        desc = subdesc;
    }
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
            CHECK_LOGIC_ERROR(desc->add_column_link(type_Link, "", *table),
                              LogicError::detached_accessor);
            CHECK_LOGIC_ERROR(desc->insert_column_link(0, type_Link, "", *table),
                              LogicError::detached_accessor);
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
        CHECK_LOGIC_ERROR(desc->add_column_link(type_Link, "", *target),
                          LogicError::detached_accessor);
        CHECK_LOGIC_ERROR(desc->insert_column_link(0, type_Link, "", *target),
                          LogicError::detached_accessor);
    }

    // Column index out of range
    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column_link(type_Link, "link", *table);
        DescriptorRef desc = table->get_descriptor();
        CHECK_LOGIC_ERROR(desc->insert_column(2, type_Int, ""),
                          LogicError::column_index_out_of_range);
        CHECK_LOGIC_ERROR(desc->insert_column_link(2, type_Link, "", *table),
                          LogicError::column_index_out_of_range);
        CHECK_LOGIC_ERROR(desc->remove_column(1), LogicError::column_index_out_of_range);
        CHECK_LOGIC_ERROR(desc->rename_column(1, "foo"), LogicError::column_index_out_of_range);
        CHECK_LOGIC_ERROR(desc->set_link_type(1, link_Strong),
                          LogicError::column_index_out_of_range);
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

    // Wrong kind of descriptor
    {
        // Link origin is a subtable descriptor
        Group group;
        TableRef table = group.add_table("table");
        DescriptorRef subdesc;
        table->add_column(type_Table, "subtable", &subdesc);
        CHECK_LOGIC_ERROR(subdesc->add_column_link(type_Link, "link", *table),
                          LogicError::wrong_kind_of_descriptor);
    }

    // Wrong kind of table
    {
        // Free-standing link origin
        Table origin;
        Group group;
        TableRef target = group.add_table("target");
        DescriptorRef desc = origin.get_descriptor();
        CHECK_LOGIC_ERROR(desc->add_column_link(type_Link, "link", *target),
                          LogicError::wrong_kind_of_table);
    }
    {
        // Free-standing link target
        Group group;
        TableRef origin = group.add_table("origin");
        Table target;
        DescriptorRef desc = origin->get_descriptor();
        CHECK_LOGIC_ERROR(desc->add_column_link(type_Link, "link", target),
                          LogicError::wrong_kind_of_table);
    }
    {
        // Link target is a subtable
        Group group;
        TableRef table = group.add_table("table");
        DescriptorRef desc = table->get_descriptor();
        DescriptorRef subdesc;
        desc->add_column(type_Table, "subtable", &subdesc);
        subdesc->add_column(type_Int, "int");
        table->add_empty_row();
        TableRef subtable = table->get_subtable(0,0);
        CHECK_LOGIC_ERROR(desc->add_column_link(type_Link, "link", *subtable),
                          LogicError::wrong_kind_of_table);
    }

    // Different groups
    {
        Group group_1, group_2;
        TableRef table_1 = group_1.add_table("table_1");
        TableRef table_2 = group_2.add_table("table_2");
        DescriptorRef desc = table_1->get_descriptor();
        CHECK_LOGIC_ERROR(desc->add_column_link(type_Link, "", *table_2),
                          LogicError::group_mismatch);
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


#if REALM_NULL_STRINGS == 1

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

#endif


TEST(Descriptor_SubTableEquality)
{
    DescriptorRef sub;

    Table t1;
    t1.add_column(type_Table, "sub", false, &sub);

    sub->add_column(type_Int, "int");

    Table t2;
    t2.add_column(type_Table, "sub", false, &sub);

    sub->add_column(type_String, "str");

    CHECK(*t1.get_descriptor() != *t2.get_descriptor());

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

    for (int i = 0; i < 10; ++i)
    {
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
