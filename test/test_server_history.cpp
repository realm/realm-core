#include <realm/sync/noinst/server/server_history.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::_impl;
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

namespace {

class HistoryContext : public _impl::ServerHistory::Context {
public:
    HistoryContext() = default;
    std::mt19937_64& server_history_get_random() noexcept override final
    {
        return m_random;
    }

private:
    std::mt19937_64 m_random;
};


TEST(ServerHistory_Verify)
{
    SHARED_GROUP_TEST_PATH(path);
    HistoryContext context;
    ServerHistory::DummyCompactionControl compaction_control;
    ServerHistory history{context, compaction_control};
    DBRef sg = DB::create(history, path);
    {
        ReadTransaction rt{sg};
        rt.get_group().verify();
    }
    {
        WriteTransaction wt{sg};
        wt.get_group().verify();
        wt.commit();
    }
    {
        WriteTransaction wt{sg};
        wt.get_group().verify();
        TableRef table = wt.add_table("class_table");
        table->add_column(type_Int, "alpha");
        table->add_column(type_Int, "beta");
        table->create_object();
        wt.commit();
    }
    {
        ReadTransaction rt{sg};
        rt.get_group().verify();
    }
}

} // unnamed namespace
