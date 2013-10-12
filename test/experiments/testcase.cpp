#include <cstring>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <sys/wait.h>

#include <tightdb/column.hpp>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>
#include <tightdb/column_basic.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/column_string_enum.hpp>
#include <tightdb/column_mixed.hpp>
#include <tightdb/array_binary.hpp>
#include <tightdb/array_string_long.hpp>

#include "../util/thread_wrapper.hpp"
#include "unit_test.hpp"

using namespace tightdb;
using namespace std;


namespace {

class MyTrivialReplication: public TrivialReplication {
public:
    MyTrivialReplication(string database_file): TrivialReplication(database_file) {}

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


TEST(Replication)
{
    string database_1 = "replication-1.tightdb";
    string database_2 = "replication-2.tightdb";
    File::try_remove(database_1);
    File::try_remove(database_2);

    {
        MyTrivialReplication repl(database_1);
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

        SharedGroup sg_2(database_2);
        repl.replay_transacts(sg_2);

        {
            ReadTransaction rt_1(sg_1);
            ReadTransaction rt_2(sg_2);
            CHECK(rt_1.get_group() == rt_2.get_group());
        }
    }

    File::try_remove(database_1);
    File::try_remove(database_2);
}
