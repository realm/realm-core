#include "testsettings.hpp"
#ifdef TEST_REPLICATION

#include <algorithm>

#include <tightdb.hpp>
#include <tightdb/util/features.h>
#include <tightdb/util/unique_ptr.hpp>
#include <tightdb/util/file.hpp>

#include "test.hpp"

#ifdef TIGHTDB_ENABLE_REPLICATION

using namespace std;
using namespace tightdb;
using namespace tightdb::util;


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

class MyTrivialReplication: public TrivialReplication {
public:
    MyTrivialReplication(string path): TrivialReplication(path) {}

    ~MyTrivialReplication() TIGHTDB_NOEXCEPT
    {
        typedef TransactLogs::const_iterator iter;
        iter end = m_transact_logs.end();
        for (iter i = m_transact_logs.begin(); i != end; ++i)
            delete[] i->data();
    }

    void replay_transacts(SharedGroup& target)
    {
        typedef TransactLogs::const_iterator iter;
        iter end = m_transact_logs.end();
        for (iter i = m_transact_logs.begin(); i != end; ++i)
            apply_transact_log(i->data(), i->size(), target);
    }

private:
    void handle_transact_log(const char* data, size_t size) TIGHTDB_OVERRIDE
    {
        UniquePtr<char> log(new char[size]); // Throws
        copy(data, data+size, log.get());
        m_transact_logs.push_back(BinaryData(log.get(), size)); // Throws
        log.release();
    }

    typedef vector<BinaryData> TransactLogs;
    TransactLogs m_transact_logs;
};

TIGHTDB_TABLE_1(MyTable,
                i, Int)

} // anonymous namespace


TEST(Replication_General)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    {
        MyTrivialReplication repl(path_1);
        SharedGroup sg_1(repl);
        {
            WriteTransaction wt(sg_1);
            MyTable::Ref table = wt.get_table<MyTable>("my_table");
            table->add();
            wt.commit();
        }
        {
            WriteTransaction wt(sg_1);
            MyTable::Ref table = wt.get_table<MyTable>("my_table");
            table[0].i = 9;
            wt.commit();
        }
        {
            WriteTransaction wt(sg_1);
            MyTable::Ref table = wt.get_table<MyTable>("my_table");
            table[0].i = 10;
            wt.commit();
        }

        SharedGroup sg_2(path_2);
        repl.replay_transacts(sg_2);

        {
            ReadTransaction rt_1(sg_1);
            ReadTransaction rt_2(sg_2);
            CHECK(rt_1.get_group() == rt_2.get_group());
        }
    }
}


#endif // TIGHTDB_ENABLE_REPLICATION
#endif // TEST_REPLICATION
