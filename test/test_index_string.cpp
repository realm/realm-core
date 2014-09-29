#include "testsettings.hpp"
#ifdef TEST_INDEX_STRING

#include <tightdb/index_string.hpp>

#include "test.hpp"

using namespace tightdb;


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


namespace {

// strings used by tests
const char s1[] = "John";
const char s2[] = "Brian";
const char s3[] = "Samantha";
const char s4[] = "Tom";
const char s5[] = "Johnathan";
const char s6[] = "Johnny";
const char s7[] = "Sam";

} // anonymous namespace


TEST(StringIndex_IsEmpty)
{
    // Create a column with string values
    ref_type ref = AdaptiveStringColumn::create(Allocator::get_default());
    AdaptiveStringColumn col(Allocator::get_default(), ref);

    // Create a new index on column
    const StringIndex& ndx = col.create_index();

    CHECK(ndx.is_empty());

    // Clean up
    col.destroy();
}

TEST(StringIndex_BuildIndex)
{
    // Create a column with string values
    ref_type ref = AdaptiveStringColumn::create(Allocator::get_default());
    AdaptiveStringColumn col(Allocator::get_default(), ref);

    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value
    col.add(s5); // common prefix
    col.add(s6); // common prefix

    // Create a new index on column
    const StringIndex& ndx = col.create_index();

    const size_t r1 = ndx.find_first(s1);
    const size_t r2 = ndx.find_first(s2);
    const size_t r3 = ndx.find_first(s3);
    const size_t r4 = ndx.find_first(s4);
    const size_t r5 = ndx.find_first(s5);
    const size_t r6 = ndx.find_first(s6);

    CHECK_EQUAL(0, r1);
    CHECK_EQUAL(1, r2);
    CHECK_EQUAL(2, r3);
    CHECK_EQUAL(3, r4);
    CHECK_EQUAL(5, r5);
    CHECK_EQUAL(6, r6);

    // Clean up
    col.destroy();
}

TEST(StringIndex_DeleteAll)
{
    // Create a column with string values
    ref_type ref = AdaptiveStringColumn::create(Allocator::get_default());
    AdaptiveStringColumn col(Allocator::get_default(), ref);

    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value
    col.add(s5); // common prefix
    col.add(s6); // common prefix

    // Create a new index on column
    const StringIndex& ndx = col.create_index();

    // Delete all entries
    // (reverse order to avoid ref updates)
    col.erase(6, 6 == col.size()-1);
    col.erase(5, 5 == col.size()-1);
    col.erase(4, 4 == col.size()-1);
    col.erase(3, 3 == col.size()-1);
    col.erase(2, 2 == col.size()-1);
    col.erase(1, 1 == col.size()-1);
    col.erase(0, 0 == col.size()-1);
#ifdef TIGHTDB_DEBUG
    CHECK(ndx.is_empty());
#else
    static_cast<void>(ndx);
#endif

    // Re-insert values
    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value
    col.add(s5); // common prefix
    col.add(s6); // common prefix

    // Delete all entries
    // (in order to force constant ref updating)
    col.erase(0, 0 == col.size()-1);
    col.erase(0, 0 == col.size()-1);
    col.erase(0, 0 == col.size()-1);
    col.erase(0, 0 == col.size()-1);
    col.erase(0, 0 == col.size()-1);
    col.erase(0, 0 == col.size()-1);
    col.erase(0, 0 == col.size()-1);
#ifdef TIGHTDB_DEBUG
    CHECK(ndx.is_empty());
#else
    static_cast<void>(ndx);
#endif

    // Clean up
    col.destroy();
}

TEST(StringIndex_Delete)
{
    // Create a column with random values
    ref_type ref = AdaptiveStringColumn::create(Allocator::get_default());
    AdaptiveStringColumn col(Allocator::get_default(), ref);

    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value

    // Create a new index on column
    const StringIndex& ndx = col.create_index();

    // Delete first item (in index)
    col.erase(1, 1 == col.size()-1);

    CHECK_EQUAL(0, col.find_first(s1));
    CHECK_EQUAL(1, col.find_first(s3));
    CHECK_EQUAL(2, col.find_first(s4));
    CHECK_EQUAL(not_found, ndx.find_first(s2));

    // Delete last item (in index)
    col.erase(2, 2 == col.size()-1);

    CHECK_EQUAL(0, col.find_first(s1));
    CHECK_EQUAL(1, col.find_first(s3));
    CHECK_EQUAL(not_found, col.find_first(s4));
    CHECK_EQUAL(not_found, col.find_first(s2));

    // Delete middle item (in index)
    col.erase(1, 1 == col.size()-1);

    CHECK_EQUAL(0, col.find_first(s1));
    CHECK_EQUAL(not_found, col.find_first(s3));
    CHECK_EQUAL(not_found, col.find_first(s4));
    CHECK_EQUAL(not_found, col.find_first(s2));

    // Delete all items
    col.erase(0, 0 == col.size()-1);
    col.erase(0, 0 == col.size()-1);
#ifdef TIGHTDB_DEBUG
    CHECK(ndx.is_empty());
#endif

    // Clean up
    col.destroy();
}

TEST(StringIndex_ClearEmpty)
{
    // Create a column with string values
    ref_type ref = AdaptiveStringColumn::create(Allocator::get_default());
    AdaptiveStringColumn col(Allocator::get_default(), ref);

    // Create a new index on column
    const StringIndex& ndx = col.create_index();

    // Clear to remove all entries
    col.clear();
#ifdef TIGHTDB_DEBUG
    CHECK(ndx.is_empty());
#else
    static_cast<void>(ndx);
#endif

    // Clean up
    col.destroy();
}

TEST(StringIndex_Clear)
{
    // Create a column with string values
    ref_type ref = AdaptiveStringColumn::create(Allocator::get_default());
    AdaptiveStringColumn col(Allocator::get_default(), ref);

    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value
    col.add(s5); // common prefix
    col.add(s6); // common prefix

    // Create a new index on column
    const StringIndex& ndx = col.create_index();

    // Clear to remove all entries
    col.clear();
#ifdef TIGHTDB_DEBUG
    CHECK(ndx.is_empty());
#else
    static_cast<void>(ndx);
#endif

    // Re-insert values
    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value
    col.add(s5); // common prefix
    col.add(s6); // common prefix

    const size_t r1 = ndx.find_first(s1);
    const size_t r2 = ndx.find_first(s2);
    const size_t r3 = ndx.find_first(s3);
    const size_t r4 = ndx.find_first(s4);
    const size_t r5 = ndx.find_first(s5);
    const size_t r6 = ndx.find_first(s6);

    CHECK_EQUAL(0, r1);
    CHECK_EQUAL(1, r2);
    CHECK_EQUAL(2, r3);
    CHECK_EQUAL(3, r4);
    CHECK_EQUAL(5, r5);
    CHECK_EQUAL(6, r6);

    // Clean up
    col.destroy();
}

TEST(StringIndex_Insert)
{
    // Create a column with random values
    ref_type ref = AdaptiveStringColumn::create(Allocator::get_default());
    AdaptiveStringColumn col(Allocator::get_default(), ref);

    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value

    // Create a new index on column
    col.create_index();

    // Insert item in top of column
    col.insert(0, s5);

    CHECK_EQUAL(0, col.find_first(s5));
    CHECK_EQUAL(1, col.find_first(s1));
    CHECK_EQUAL(2, col.find_first(s2));
    CHECK_EQUAL(3, col.find_first(s3));
    CHECK_EQUAL(4, col.find_first(s4));
    //CHECK_EQUAL(5, ndx.find_first(s1)); // duplicate

    // Append item in end of column
    col.insert(6, s6);

    CHECK_EQUAL(0, col.find_first(s5));
    CHECK_EQUAL(1, col.find_first(s1));
    CHECK_EQUAL(2, col.find_first(s2));
    CHECK_EQUAL(3, col.find_first(s3));
    CHECK_EQUAL(4, col.find_first(s4));
    CHECK_EQUAL(6, col.find_first(s6));

    // Insert item in middle
    col.insert(3, s7);

    CHECK_EQUAL(0, col.find_first(s5));
    CHECK_EQUAL(1, col.find_first(s1));
    CHECK_EQUAL(2, col.find_first(s2));
    CHECK_EQUAL(3, col.find_first(s7));
    CHECK_EQUAL(4, col.find_first(s3));
    CHECK_EQUAL(5, col.find_first(s4));
    CHECK_EQUAL(7, col.find_first(s6));

    // Clean up
    col.destroy();
}

TEST(StringIndex_Set)
{
    // Create a column with random values
    ref_type ref = AdaptiveStringColumn::create(Allocator::get_default());
    AdaptiveStringColumn col(Allocator::get_default(), ref);

    col.add(s1);
    col.add(s2);
    col.add(s3);
    col.add(s4);
    col.add(s1); // duplicate value

    // Create a new index on column
    col.create_index();

    // Set top value
    col.set(0, s5);

    CHECK_EQUAL(0, col.find_first(s5));
    CHECK_EQUAL(1, col.find_first(s2));
    CHECK_EQUAL(2, col.find_first(s3));
    CHECK_EQUAL(3, col.find_first(s4));
    CHECK_EQUAL(4, col.find_first(s1));

    // Set bottom value
    col.set(4, s6);

    CHECK_EQUAL(not_found, col.find_first(s1));
    CHECK_EQUAL(0, col.find_first(s5));
    CHECK_EQUAL(1, col.find_first(s2));
    CHECK_EQUAL(2, col.find_first(s3));
    CHECK_EQUAL(3, col.find_first(s4));
    CHECK_EQUAL(4, col.find_first(s6));

    // Set middle value
    col.set(2, s7);

    CHECK_EQUAL(not_found, col.find_first(s3));
    CHECK_EQUAL(not_found, col.find_first(s1));
    CHECK_EQUAL(0, col.find_first(s5));
    CHECK_EQUAL(1, col.find_first(s2));
    CHECK_EQUAL(2, col.find_first(s7));
    CHECK_EQUAL(3, col.find_first(s4));
    CHECK_EQUAL(4, col.find_first(s6));

    // Clean up
    col.destroy();
}

TEST(StringIndex_Count)
{
    // Create a column with duplcate values
    ref_type ref = AdaptiveStringColumn::create(Allocator::get_default());
    AdaptiveStringColumn col(Allocator::get_default(), ref);

    col.add(s1);
    col.add(s2);
    col.add(s2);
    col.add(s3);
    col.add(s3);
    col.add(s3);
    col.add(s4);
    col.add(s4);
    col.add(s4);
    col.add(s4);

    // Create a new index on column
    col.create_index();

    // Counts
    const size_t c0 = col.count(s5);
    const size_t c1 = col.count(s1);
    const size_t c2 = col.count(s2);
    const size_t c3 = col.count(s3);
    const size_t c4 = col.count(s4);
    CHECK_EQUAL(0, c0);
    CHECK_EQUAL(1, c1);
    CHECK_EQUAL(2, c2);
    CHECK_EQUAL(3, c3);
    CHECK_EQUAL(4, c4);

    // Clean up
    col.destroy();
}

TEST(StringIndex_Distinct)
{
    // Create a column with duplcate values
    ref_type ref = AdaptiveStringColumn::create(Allocator::get_default());
    AdaptiveStringColumn col(Allocator::get_default(), ref);

    col.add(s1);
    col.add(s2);
    col.add(s2);
    col.add(s3);
    col.add(s3);
    col.add(s3);
    col.add(s4);
    col.add(s4);
    col.add(s4);
    col.add(s4);

    // Create a new index on column
    StringIndex& ndx = col.create_index();

    // Get view of unique values
    // (sorted in alphabetical order, each ref to first match)
    ref_type results_ref = Column::create(Allocator::get_default());
    Column results(Allocator::get_default(), results_ref);
    ndx.distinct(results);

    CHECK_EQUAL(4, results.size());
    CHECK_EQUAL(1, results.get(0)); // s2 = Brian
    CHECK_EQUAL(0, results.get(1)); // s1 = John
    CHECK_EQUAL(3, results.get(2)); // s3 = Samantha
    CHECK_EQUAL(6, results.get(3)); // s4 = Tom

    // Clean up
    results.destroy();
    col.destroy();
}

TEST(StringIndex_FindAllNoCopy)
{
    // Create a column with duplcate values
    ref_type ref = AdaptiveStringColumn::create(Allocator::get_default());
    AdaptiveStringColumn col(Allocator::get_default(), ref);

    col.add(s1);
    col.add(s2);
    col.add(s2);
    col.add(s3);
    col.add(s3);
    col.add(s3);
    col.add(s4);
    col.add(s4);
    col.add(s4);
    col.add(s4);

    // Create a new index on column
    StringIndex& ndx = col.create_index();

    size_t ref_2 = not_found;
    FindRes res1 = ndx.find_all("not there", ref_2);
    CHECK_EQUAL(FindRes_not_found, res1);

    FindRes res2 = ndx.find_all(s1, ref_2);
    CHECK_EQUAL(FindRes_single, res2);
    CHECK_EQUAL(0, ref_2);

    FindRes res3 = ndx.find_all(s4, ref_2);
    CHECK_EQUAL(FindRes_column, res3);
    const Column results(Allocator::get_default(), ref_type(ref_2));
    CHECK_EQUAL(4, results.size());
    CHECK_EQUAL(6, results.get(0));
    CHECK_EQUAL(7, results.get(1));
    CHECK_EQUAL(8, results.get(2));
    CHECK_EQUAL(9, results.get(3));

    // Clean up
    col.destroy();
}

ONLY(StringIndex_FindAllNoCopy2)
{
    // Create a column with duplcate values
    ref_type ref = AdaptiveStringColumn::create(Allocator::get_default());
    AdaptiveStringColumn col(Allocator::get_default(), ref);

    StringData bin0 = StringData("\x27\x22\x11\x11\0\0\0\0", 8);
    StringData bin1 = StringData("\x92\x78\0\0\0\0\0\0", 8);

    col.add(bin0);
    col.add(bin0);
    col.add(bin1);
    
    // Create a new index on column
    StringIndex& ndx = col.create_index();
    size_t results = not_found;
    FindRes res = ndx.find_all(bin1, results);
    CHECK_EQUAL(FindRes_column, res);

    // Clean up
    col.destroy();
}

#endif // TEST_INDEX_STRING
