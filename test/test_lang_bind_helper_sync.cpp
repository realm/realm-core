#include <realm/db.hpp>
#include <realm/history.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::sync;
using namespace realm::test_util;


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


TEST(LangBindHelper_SyncCannotBeChanged_1)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // enable sync
        std::unique_ptr<Replication> hist = make_client_replication();
        DBRef sg = DB::create(*hist, path);
        {
            WriteTransaction wt(sg);
            wt.add_table("class_table");
            wt.commit();
        }
    }
    {
        // try to access the database with sync disabled
        std::unique_ptr<Replication> hist(make_in_realm_history());
        CHECK_THROW(DB::create(*hist, path), IncompatibleHistories);
    }
}

TEST(LangBindHelper_SyncCannotBeChanged_2)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // enable sync
        std::unique_ptr<Replication> hist(make_in_realm_history());
        DBRef sg = DB::create(*hist, path);
        {
            WriteTransaction wt(sg);
            wt.add_table("class_table");
            wt.commit();
        }
    }
    {
        // try to access the database with sync enabled
        std::unique_ptr<Replication> hist = make_client_replication();
        CHECK_THROW(DB::create(*hist, path), IncompatibleHistories);
    }
}
