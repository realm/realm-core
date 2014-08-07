#include "testsettings.hpp"
#ifdef TEST_ARRAY_BLOB

#include <cstring>

#include <tightdb/array_blob.hpp>
#include <tightdb/column_string.hpp>

#include "test.hpp"

using namespace std;
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


TEST(ArrayBlob_AddEmpty)
{
    ArrayBlob blob(Allocator::get_default());
    blob.create();

    blob.add("", 0);

    // Cleanup
    blob.destroy();
}


TEST(ArrayBlob_General)
{
    ArrayBlob blob(Allocator::get_default());
    blob.create();

    const char* t1 = "aaa";
    const char* t2 = "bbbbbb";
    const char* t3 = "ccccccccccc";
    const char* t4 = "xxx";
    size_t l1 = strlen(t1) + 1;
    size_t l2 = strlen(t2) + 1;
    size_t l3 = strlen(t3) + 1;

    // Test add
    blob.add(t1, l1);
    blob.add(t2, l2);
    blob.add(t3, l3);

    CHECK_EQUAL(t1, blob.get(0));
    CHECK_EQUAL(t2, blob.get(l1));
    CHECK_EQUAL(t3, blob.get(l1 + l2));

    // Test insert
    blob.insert(0, t3, l3);
    blob.insert(l3, t2, l2);

    CHECK_EQUAL(t3, blob.get(0));
    CHECK_EQUAL(t2, blob.get(l3));
    CHECK_EQUAL(t1, blob.get(l3 + l2));
    CHECK_EQUAL(t2, blob.get(l3 + l2 + l1));
    CHECK_EQUAL(t3, blob.get(l3 + l2 + l1 + l2));

    // Test replace
    blob.replace(l3, l3 + l2, t1, l1); // replace with smaller
    blob.replace(l3 + l1 + l1, l3 + l1 + l1 + l2, t3, l3); // replace with bigger
    blob.replace(l3 + l1, l3 + l1 + l1, t4, l1); // replace with same

    CHECK_EQUAL(t3, blob.get(0));
    CHECK_EQUAL(t1, blob.get(l3));
    CHECK_EQUAL(t4, blob.get(l3 + l1));
    CHECK_EQUAL(t3, blob.get(l3 + l1 + l1));
    CHECK_EQUAL(t3, blob.get(l3 + l1 + l1 + l3));

    // Test delete
    blob.erase(0, l3);                 // top
    blob.erase(l1, l1 + l1);           // middle
    blob.erase(l1 + l3, l1 + l3 + l3); // bottom

    CHECK_EQUAL(t1, blob.get(0));
    CHECK_EQUAL(t3, blob.get(l1));
    CHECK_EQUAL(l1 + l3, blob.size());

    // Delete all
    blob.erase(0, l1 + l3);
    CHECK(blob.is_empty());

    // Cleanup
    blob.destroy();
}


TEST(ArrayBlob_AdaptiveStringLeak)
{
    ref_type col_ref = AdaptiveStringColumn::create(Allocator::get_default());
    AdaptiveStringColumn col(Allocator::get_default(), col_ref);
    for (size_t i = 0; i != 2 * TIGHTDB_MAX_LIST_SIZE; ++i)
        col.insert(0, string(100, 'a'));  // use constant larger than 'medium_string_max_size'

    col.destroy();
}

#endif // TEST_ARRAY_BLOB
