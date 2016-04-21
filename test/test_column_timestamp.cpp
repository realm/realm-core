#include "testsettings.hpp"
#ifdef TEST_COLUMN_TIMESTAMP

#include <realm/column_timestamp.hpp>
#include <realm.hpp>

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


TEST(TimestampColumn_Basic)
{
    ref_type ref = TimestampColumn::create(Allocator::get_default());
    TimestampColumn c(Allocator::get_default(), ref);
    c.add(Timestamp(123,123));
    Timestamp ts = c.get(0);
    CHECK(ts == Timestamp(123, 123));
}

TEST(TimestampColumn_Basic_Nulls)
{
    // Test that default value is null() for nullable column and non-null for non-nullable column
    Table t;
    t.add_column(type_Timestamp, "date", false /*not nullable*/);
    t.add_column(type_Timestamp, "date", true  /*nullable*/);

    t.add_empty_row();
    CHECK(!t.is_null(0, 0));
    CHECK(t.is_null(1, 0));

    CHECK_THROW_ANY(t.set_null(0, 0));
    t.set_null(1, 0);

    CHECK_THROW_ANY(t.set_timestamp(0, 0, Timestamp()));
}

TEST(TimestampColumn_Relocate)
{
    // Fill so much data in a column that it relocates, to check if relocation propagates up correctly
    Table t;
    t.add_column(type_Timestamp, "date", true  /*nullable*/);

    for (unsigned int i = 0; i < 10000; i++) {
        t.add_empty_row();
        t.set_timestamp(0, i, Timestamp(i, i));
    }
}

TEST(TimestampColumn_Compare)
{
    ref_type ref = TimestampColumn::create(Allocator::get_default());
    TimestampColumn c(Allocator::get_default(), ref);

    for (unsigned int i = 0; i < 10000; i++) {
        c.add(Timestamp(i, i));
    }

    CHECK(c.compare(c));

    {
        ref_type ref = TimestampColumn::create(Allocator::get_default());
        TimestampColumn c2(Allocator::get_default(), ref);
        CHECK_NOT(c.compare(c2));
    }
}

TEST(TimestampColumn_Index)
{
    ref_type ref = TimestampColumn::create(Allocator::get_default());
    TimestampColumn c(Allocator::get_default(), ref);
    StringIndex* index = c.create_search_index();
    CHECK(index);

    for (uint32_t i = 0; i < 100; ++i) {
        c.add(Timestamp{i + 10000, i});
    }

    Timestamp last_value{10099, 99};

    CHECK_EQUAL(index->find_first(last_value), 99);

    c.destroy_search_index();
    c.destroy();
}

TEST(TimestampColumn_Is_Nullable)
{
    ref_type ref = TimestampColumn::create(Allocator::get_default());
    TimestampColumn c(Allocator::get_default(), ref);
    CHECK(c.is_nullable());
    c.destroy();
}

TEST(TimestampColumn_Set_Null_With_Index)
{
    ref_type ref = TimestampColumn::create(Allocator::get_default());
    TimestampColumn c(Allocator::get_default(), ref);
    c.add(Timestamp{1, 1});
    CHECK(!c.is_null(0));

    StringIndex* index = c.create_search_index();
    CHECK(index);

    c.set_null(0);
    CHECK(c.is_null(0));

    c.destroy_search_index();
    c.destroy();
}

TEST(TimestampColumn_Insert_Rows_With_Index)
{
    ref_type ref = TimestampColumn::create(Allocator::get_default());
    TimestampColumn c(Allocator::get_default(), ref);

    StringIndex* index = c.create_search_index();
    CHECK(index);

    c.insert_rows(0, 1, 0, true);
    c.set(0, Timestamp{1, 1});
    c.insert_rows(1, 1, 1, true);

    c.destroy_search_index();
    c.destroy();
}

TEST(TimestampColumn_Move_Last_Over)
{
    ref_type ref = TimestampColumn::create(Allocator::get_default());
    TimestampColumn c(Allocator::get_default(), ref);
    StringIndex* index = c.create_search_index();
    CHECK(index);

    c.add(Timestamp{1, 1});
    c.add(Timestamp{2, 2});
    c.add(Timestamp{3, 3});
    c.set_null(2);
    c.move_last_row_over(0, 3, false);
    CHECK(c.is_null(0));

    c.destroy_search_index();
    c.destroy();
}

TEST(TimestampColumn_Clear)
{
    ref_type ref = TimestampColumn::create(Allocator::get_default());
    TimestampColumn c(Allocator::get_default(), ref);
    StringIndex* index = c.create_search_index();
    CHECK(index);

    c.add(Timestamp{1, 1});
    c.add(Timestamp{2, 2});
    c.clear(2, false);
    c.add(Timestamp{3, 3});

    Timestamp last_value{3, 3};
    CHECK_EQUAL(c.get(0), last_value);

    c.destroy_search_index();
    c.destroy();
}

TEST(TimestampColumn_SwapRows)
{
    ref_type ref = TimestampColumn::create(Allocator::get_default());
    TimestampColumn c(Allocator::get_default(), ref);
    StringIndex* index = c.create_search_index();
    CHECK(index);

    Timestamp one {1, 1};
    Timestamp three {3, 3};
    c.add(one);
    c.add(Timestamp{2, 2});
    c.add(three);


    CHECK_EQUAL(c.get(0), one);
    CHECK_EQUAL(c.get(2), three);
    c.swap_rows(0, 2);
    CHECK_EQUAL(c.get(2), one);
    CHECK_EQUAL(c.get(0), three);

    c.destroy_search_index();
    c.destroy();

}

TEST(TimestampColumn_DeleteWithIndex)
{
    ref_type ref = TimestampColumn::create(Allocator::get_default());
    TimestampColumn c(Allocator::get_default(), ref);
    StringIndex* index = c.create_search_index();
    CHECK(index);


    c.add(Timestamp{2, 2});
    c.erase_rows(0, 1, 1, false);
    CHECK_EQUAL(c.size(), 0);

    c.destroy_search_index();
    c.destroy();

}


// Bug found by AFL during development of TimestampColumn
TEST(TimestampColumn_DeleteAfterSetWithIndex)
{
    ref_type ref = TimestampColumn::create(Allocator::get_default());
    TimestampColumn c(Allocator::get_default(), ref);
    StringIndex* index = c.create_search_index();
    CHECK(index);

    c.add(Timestamp{1, 1});
    c.set(0,Timestamp{2, 2});
    c.erase_rows(0, 1, 1, false);
    CHECK_EQUAL(c.size(), 0);

    c.destroy_search_index();
    c.destroy();
}


// Bug found by AFL during development of TimestampColumn
TEST(TimestampColumn_DeleteAfterSetNullWithIndex)
{
    ref_type ref = TimestampColumn::create(Allocator::get_default());
    TimestampColumn c(Allocator::get_default(), ref);
    StringIndex* index = c.create_search_index();
    CHECK(index);

    c.add(Timestamp{0, 0});
    c.set_null(0);
    c.add(Timestamp{1, 1});
    c.add(Timestamp{2, 2});
    c.erase_rows(0, 1, 1, false);
    CHECK_EQUAL(c.size(), 2);

    c.destroy_search_index();
    c.destroy();
}


// Bug found by AFL during development of TimestampColumn
TEST(TimestampColumn_LargeNegativeTimestampSearchIndex)
{
    ref_type ref = TimestampColumn::create(Allocator::get_default());
    TimestampColumn c(Allocator::get_default(), ref);

    c.add(Timestamp{-1934556340879361, 0});
    StringIndex* index = c.create_search_index();
    CHECK(index);
    c.set_null(0);

    c.erase_rows(0, 1, 1, false);
    CHECK_EQUAL(c.size(), 0);

    c.destroy_search_index();
    c.destroy();
}


TEST(TimestampColumn_LargeNegativeTimestampSearchIndexErase)
{
    ref_type ref = TimestampColumn::create(Allocator::get_default());
    TimestampColumn c(Allocator::get_default(), ref);

    c.add(Timestamp{-1934556340879361, 0});
    StringIndex* index = c.create_search_index();
    CHECK(index);
    c.set_null(0);

    c.erase(0, true);
    CHECK_EQUAL(c.size(), 0);
    CHECK(index->is_empty());

    c.destroy_search_index();
    c.destroy();
}

#if __cplusplus == 201402L || defined(REALM_HAVE_AT_LEAST_MSVC_11_2012) // needs c++14 for auto arguments
TEST(TimestampColumn_Operators)
{
    // Note that the Timestamp::operator==, operator>, operator<, operator>=, etc, do not work
    // if one of the Timestamps are null! Please use realm::Greater, realm::Equal, etc instead.

    auto compare = [&](auto& a, auto& b, auto condition) {
        return condition(a, b, a.is_null(), b.is_null()); 
    };

    // Test A. Note that Timestamp() is null and Timestamp(0, 0) is non-null
    // -----------------------------------------------------------------------------------------
    CHECK(compare(Timestamp(), Timestamp(), realm::Equal()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::Equal()));
    CHECK(compare(Timestamp(1, 2), Timestamp(1, 2), realm::Equal()));
    CHECK(compare(Timestamp(-1, 2), Timestamp(-1, 2), realm::Equal()));

    // Test B
    // -----------------------------------------------------------------------------------------
    CHECK(!compare(Timestamp(), Timestamp(0, 0), realm::Equal()));
    CHECK(!compare(Timestamp(0, 0), Timestamp(), realm::Equal()));
    CHECK(!compare(Timestamp(0, 0), Timestamp(0, 1), realm::Equal()));
    CHECK(!compare(Timestamp(0, 1), Timestamp(0, 0), realm::Equal()));
    CHECK(!compare(Timestamp(1, 0), Timestamp(0, 0), realm::Equal()));
    CHECK(!compare(Timestamp(0, 0), Timestamp(1, 0), realm::Equal()));

    // Test C: !compare(..., Equal) == compare(..., NotEqual)
    // -----------------------------------------------------------------------------------------
    CHECK(compare(Timestamp(), Timestamp(0, 0), realm::NotEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(), realm::NotEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 1), realm::NotEqual()));
    CHECK(compare(Timestamp(0, 1), Timestamp(0, 0), realm::NotEqual()));
    CHECK(compare(Timestamp(1, 0), Timestamp(0, 0), realm::NotEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(1, 0), realm::NotEqual()));

    // Test D: compare(..., Equal) == true implies that compare(..., GreaterEqual) == true
    // (but not vice versa). So we copy/pate tests from test B again:
    // -----------------------------------------------------------------------------------------
    CHECK(compare(Timestamp(), Timestamp(), realm::GreaterEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::GreaterEqual()));
    CHECK(compare(Timestamp(1, 2), Timestamp(1, 2), realm::GreaterEqual()));
    CHECK(compare(Timestamp(-1, 2), Timestamp(-1, 2), realm::GreaterEqual()));

    CHECK(compare(Timestamp(), Timestamp(), realm::LessEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::LessEqual()));
    CHECK(compare(Timestamp(1, 2), Timestamp(1, 2), realm::LessEqual()));
    CHECK(compare(Timestamp(-1, 2), Timestamp(-1, 2), realm::LessEqual()));

    // Test E: Sorting order of nulls vs. non-nulls should be the same for Timestamp as for other types
    // -----------------------------------------------------------------------------------------
    // All four data elements are null here (StringData(0, 0) means null)
    CHECK(compare(Timestamp(), Timestamp(), realm::Greater()) ==
          compare(StringData(0, 0), StringData(0, 0), realm::Greater()));

    // Compare null with non-nulls (Timestamp(0, 0) is non-null and StringData("") is non-null
    CHECK(compare(Timestamp(0, 0), Timestamp(), realm::Greater()) ==
          compare(StringData(""), StringData(0, 0), realm::Greater()));

    // All four elements are non-nulls
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::Greater()) ==
        compare(StringData(""), StringData(""), realm::Greater()));

    // Repeat with other operators than Greater
    CHECK(compare(Timestamp(), Timestamp(), realm::Less()) ==
        compare(StringData(0, 0), StringData(0, 0), realm::Less()));
    CHECK(compare(Timestamp(0, 0), Timestamp(), realm::Less()) ==
        compare(StringData(""), StringData(0, 0), realm::Less()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::Less()) ==
        compare(StringData(""), StringData(""), realm::Less()));

    CHECK(compare(Timestamp(), Timestamp(), realm::Equal()) ==
        compare(StringData(0, 0), StringData(0, 0), realm::Equal()));
    CHECK(compare(Timestamp(0, 0), Timestamp(), realm::Equal()) ==
        compare(StringData(""), StringData(0, 0), realm::Equal()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::Equal()) ==
        compare(StringData(""), StringData(""), realm::Equal()));

    CHECK(compare(Timestamp(), Timestamp(), realm::NotEqual()) ==
        compare(StringData(0, 0), StringData(0, 0), realm::NotEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(), realm::NotEqual()) ==
        compare(StringData(""), StringData(0, 0), realm::NotEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::NotEqual()) ==
        compare(StringData(""), StringData(""), realm::NotEqual()));

    CHECK(compare(Timestamp(), Timestamp(), realm::GreaterEqual()) ==
        compare(StringData(0, 0), StringData(0, 0), realm::GreaterEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(), realm::GreaterEqual()) ==
        compare(StringData(""), StringData(0, 0), realm::GreaterEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::GreaterEqual()) ==
        compare(StringData(""), StringData(""), realm::GreaterEqual()));

    CHECK(compare(Timestamp(), Timestamp(), realm::LessEqual()) ==
        compare(StringData(0, 0), StringData(0, 0), realm::LessEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(), realm::LessEqual()) ==
        compare(StringData(""), StringData(0, 0), realm::LessEqual()));
    CHECK(compare(Timestamp(0, 0), Timestamp(0, 0), realm::LessEqual()) ==
        compare(StringData(""), StringData(""), realm::LessEqual()));
}
#endif

#endif // TEST_COLUMN_TIMESTAMP
