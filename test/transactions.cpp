#include <cstdio>
#include <vector>
#include <sstream>
#include <fstream>
#include <iomanip>

#include <unistd.h>

#include <UnitTest++.h>

#include <tightdb/error.hpp>
#include <tightdb/file.hpp>
#include <tightdb/group_shared.hpp>

using namespace std;
using namespace tightdb;

namespace {


template<class T> struct mem_buf {
    mem_buf(std::size_t size): m_ptr(new T[size]) {}
    ~mem_buf() { delete[] m_ptr; }
    T* get()             { return m_ptr; }
    const T* get() const { return m_ptr; }
    T& operator[](std::size_t i)             { return m_ptr[i]; }
    const T& operator[](std::size_t i) const { return m_ptr[i]; }
private:
    T* m_ptr;
};


enum MyEnum { moja, mbili, tatu, nne, tano, sita, saba, nane, tisa, kumi,
              kumi_na_moja, kumi_na_mbili, kumi_na_tatu };

TIGHTDB_TABLE_2(MySubsubtable,
                value,  Int,
                binary, Binary)

TIGHTDB_TABLE_2(MySubtable,
                foo, Int,
                bar, Subtable<MySubsubtable>)

TIGHTDB_TABLE_8(MyTable,
                alpha,   Int,
                beta,    Bool,
                gamma,   Enum<MyEnum>,
                delta,   Date,
                epsilon, String,
                zeta,    Binary,
                eta,     Subtable<MySubtable>,
                theta,   Mixed)


const int num_threads = 23;
const int num_rounds  = 2;


void round(SharedGroup* db, int index)
{
    // Testing all value types
    {
        Group& group = db->begin_write(); // Write transaction #1
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        if (table->is_empty()) {
            table->add();
            table->add(0, false, moja, time_t(), "", BinaryData(0,0), 0, Mixed(int64_t()));
            const char binary_data[] = { 7, 6, 5, 7, 6, 5, 4, 3, 113 };
            table->add(749321, true, kumi_na_tatu, time_t(99992), "click",
                       BinaryData(binary_data, sizeof(binary_data)), 0, Mixed("fido"));
        }
    }
    db->commit();

    // Add more rows
    {
        Group& group = db->begin_write(); // Write transaction #2
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        if (table->size() < 100) for (int i=0; i<10; ++i) table->add();
        ++table[0].alpha;
    }
    db->commit();

    // Testing empty transaction
    {
        Group& group = db->begin_write(); // Write transaction #3
        static_cast<void>(group);
    }
    db->commit();

    // Testing subtables
    {
        Group& group = db->begin_write(); // Write transaction #4
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        MySubtable::Ref subtable = table[0].eta;
        if (subtable->is_empty()) {
            subtable->add(0, 0);
            subtable->add(100, 0);
            subtable->add(0, 0);
        }
        ++table[0].alpha;
    }
    db->commit();

    // Testing subtables within subtables
    {
        Group& group = db->begin_write(); // Write transaction #5
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        ++table[0].alpha;
        MySubtable::Ref subtable = table[0].eta;
        ++subtable[0].foo;
        MySubsubtable::Ref subsubtable = subtable[0].bar;
        for (int i=subsubtable->size(); i<=index; ++i) {
            subsubtable->add();
        }
        ++table[0].alpha;
    }
    db->commit();

    // Testing remove row
    {
        Group& group = db->begin_write(); // Write transaction #6
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        if (3 <= table->size()) {
            if (table[2].alpha == 749321) table->remove(1);
            else table->remove(2);
        }
        MySubtable::Ref subtable = table[0].eta;
        ++subtable[0].foo;
    }
    db->commit();

    // Testing read transaction
    {
        const Group& group = db->begin_read();
        MyTable::ConstRef table = group.get_table<MyTable>("my_table");
        CHECK_EQUAL(749321, table[1].alpha);
        MySubtable::ConstRef subtable = table[0].eta;
        CHECK_EQUAL(100, subtable[1].foo);
    }
    db->end_read();

    {
        Group& group = db->begin_write(); // Write transaction #7
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        MySubtable::Ref subtable = table[0].eta;
        MySubsubtable::Ref subsubtable = subtable[0].bar;
        subsubtable[index].value = index;
        ++table[0].alpha;
        subsubtable[index].value += 2;
        ++subtable[0].foo;
        subsubtable[index].value += 2;
    }
    db->commit();

    // Testing rollback
    {
        Group& group = db->begin_write(); // Write transaction #8
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        MySubtable::Ref subtable = table[0].eta;
        MySubsubtable::Ref subsubtable = subtable[0].bar;
        ++table[0].alpha;
        subsubtable[index].value += 2;
        ++subtable[0].foo;
        subsubtable[index].value += 2;
    }
    db->rollback();

    // Testing large chunks of data
    {
        Group& group = db->begin_write(); // Write transaction #9
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        MySubtable::Ref subtable = table[0].eta;
        MySubsubtable::Ref subsubtable = subtable[0].bar;
        const size_t size = (512 + index%1024) * 1024;
        mem_buf<char> data(size);
        for (size_t i=0; i<size; ++i)
            data[i] = static_cast<unsigned char>((i+index) * 677 % 256);
        subsubtable[index].binary = BinaryData(data.get(), size);
    }
    db->commit();

    {
        Group& group = db->begin_write(); // Write transaction #10
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        MySubtable::Ref subtable = table[0].eta;
        subtable[2].foo = index*677;
    }
    db->commit();

    {
        Group& group = db->begin_write(); // Write transaction #11
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        const size_t size = (512 + (333 + 677*index) % 1024) * 1024;
        mem_buf<char> data(size);
        for (size_t i=0; i<size; ++i)
            data[i] = static_cast<unsigned char>((i+index+73) * 677 % 256);
        table[index%2].zeta = BinaryData(data.get(), size);
    }
    db->commit();

    {
        Group& group = db->begin_write(); // Write transaction #12
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        MySubtable::Ref subtable = table[0].eta;
        MySubsubtable::Ref subsubtable = subtable[0].bar;
        subsubtable[index].value += 1000;
        --table[0].alpha;
        subsubtable[index].value -= 2;
        --subtable[0].foo;
        subsubtable[index].value -= 2;
    }
    db->commit();

    {
        Group& group = db->begin_write(); // Write transaction #13
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        const size_t size = (512 + (333 + 677*index) % 1024) * 327;
        mem_buf<char> data(size);
        for (size_t i=0; i<size; ++i)
            data[i] = static_cast<unsigned char>((i+index+73) * 677 % 256);
        table[(index+1)%2].zeta = BinaryData(data.get(), size);
    }
    db->commit();

    // Testing subtables in mixed column
    {
        Group& group = db->begin_write(); // Write transaction #14
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        MyTable::Ref subtable;
        if (table[1].theta.get_type() == COLUMN_TYPE_TABLE) {
            subtable = table[1].theta.get_subtable<MyTable>();
        }
        else {
            subtable = table[1].theta.set_subtable<MyTable>();
            subtable->add();
            subtable->add();
        }
        const int n = 13 / (1+index);
        for (int i=0; i<n; ++i) {
            const BinaryData bin(0,0);
            const Mixed mix = int64_t(i);
            subtable->add(0, false, moja,  time_t(), "alpha",   bin, 0, mix);
            subtable->add(1, false, mbili, time_t(), "beta",    bin, 0, mix);
            subtable->add(2, false, tatu,  time_t(), "gamma",   bin, 0, mix);
            subtable->add(3, false, nne,   time_t(), "delta",   bin, 0, mix);
            subtable->add(4, false, tano,  time_t(), "epsilon", bin, 0, mix);
            subtable->add(5, false, sita,  time_t(), "zeta",    bin, 0, mix);
            subtable->add(6, false, saba,  time_t(), "eta",     bin, 0, mix);
            subtable->add(7, false, nane,  time_t(), "theta",   bin, 0, mix);
        }
    }
    db->commit();

    // Testing table optimization (unique strings enumeration)
    {
        Group& group = db->begin_write(); // Write transaction #15
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        table->optimize();
        MyTable::Ref subtable = table[1].theta.get_subtable<MyTable>();
        subtable->optimize();
    }
    db->commit();

    // Testing all mixed types
    {
        Group& group = db->begin_write(); // Write transaction #16
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        MyTable::Ref subtable = table[1].theta.get_subtable<MyTable>();
        MyTable::Ref subsubtable;
        if (subtable[0].theta.get_type() == COLUMN_TYPE_TABLE) {
            subsubtable = subtable[0].theta.get_subtable<MyTable>();
        }
        else {
            subsubtable = subtable[0].theta.set_subtable<MyTable>();
        }
        const size_t size = (17 + 233*index) % 523;
        mem_buf<char> data(size);
        for (size_t i=0; i<size; ++i)
            data[i] = static_cast<unsigned char>((i+index+79) * 677 % 256);
        const BinaryData bin(data.get(), size);
        subsubtable->add(0, false, nne,  0, "", bin, 0, Mixed(int64_t(index*13)));
        subsubtable->add(1, false, tano, 0, "", bin, 0, Mixed(index%2==0?false:true));
        subsubtable->add(2, false, sita, 0, "", bin, 0, Mixed(Date(index*13)));
        subsubtable->add(3, false, saba, 0, "", bin, 0, Mixed("click"));
        subsubtable->add(4, false, nane, 0, "", bin, 0, Mixed(bin));
    }
    db->commit();

    // Testing clearing of table with multiple subtables
    {
        Group& group = db->begin_write(); // Write transaction #17
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        MyTable::Ref subtable = table[1].theta.get_subtable<MyTable>();
        MySubtable::Ref subsubtable;
        if (subtable[1].theta.get_type() == COLUMN_TYPE_TABLE) {
            subsubtable = subtable[1].theta.get_subtable<MySubtable>();
        }
        else {
            subsubtable = subtable[1].theta.set_subtable<MySubtable>();
        }
        const int num = 8;
        for (int i=0; i<num; ++i) {
            subsubtable->add(i, 0);
        }
        vector<MySubsubtable::Ref> subsubsubtables;
        for (int i=0; i<num; ++i) {
            subsubsubtables.push_back(subsubtable[i].bar);
        }
        for (int i=0; i<3; ++i) {
            for (int j=0; j<num; j+=2) {
                const BinaryData bin(0,0);
                subsubsubtables[j]->add((i-j)*index-19, bin);
            }
        }
    }
    db->commit();

    {
        Group& group = db->begin_write(); // Write transaction #18
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        MyTable::Ref subtable = table[1].theta.get_subtable<MyTable>();
        MySubtable::Ref subsubtable = subtable[1].theta.get_subtable<MySubtable>();
        subsubtable->clear();
    }
    db->commit();

    // Testing addition of an integer to all values in a column
    {
        Group& group = db->begin_write(); // Write transaction #19
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        MyTable::Ref subtable = table[1].theta.get_subtable<MyTable>();
        MySubsubtable::Ref subsubtable;
        if (subtable[2].theta.get_type() == COLUMN_TYPE_TABLE) {
            subsubtable = subtable[2].theta.get_subtable<MySubsubtable>();
        }
        else {
            subsubtable = subtable[2].theta.set_subtable<MySubsubtable>();
        }
        const int num = 9;
        for (int i=0; i<num; ++i) {
            subsubtable->add(i, BinaryData(0,0));
        }
        subsubtable->column().value += 31;
    }
    db->commit();

    // Testing addition of an index to a column
    {
        Group& group = db->begin_write(); // Write transaction #20
        MyTable::Ref table = group.get_table<MyTable>("my_table");
        MyTable::Ref subtable = table[1].theta.get_subtable<MyTable>();
        MySubsubtable::Ref subsubtable;
        if (subtable[3].theta.get_type() == COLUMN_TYPE_TABLE) {
            subsubtable = subtable[3].theta.get_subtable<MySubsubtable>();
        }
        else {
            subsubtable = subtable[3].theta.set_subtable<MySubsubtable>();
            // FIXME: Reenable this when it works!!!
//            subsubtable->column().value.set_index();
        }
        const int num = 9;
        for (int i=0; i<num; ++i) {
            subsubtable->add(i, BinaryData(0,0));
        }
    }
    db->commit();
}


void thread(int index, const char* database_path)
{
    for (int i=0; i<num_rounds; ++i) {
        SharedGroup db(database_path);
        if (!db.is_valid()) throw_error(ERROR_OTHER);
        round(&db, index);
    }
}


struct ThreadWrapper {
    void run(int index, const char* database_path)
    {
        m_index         = index;
        m_database_path = database_path;
        m_error         = false;
        const int rc = pthread_create(&m_pthread, 0, &ThreadWrapper::run, this);
        if (rc != 0) throw_error(ERROR_OTHER);
    }

    // Returns 'true' on error
    bool join()
    {
        const int rc = pthread_join(m_pthread, 0);
        if (rc != 0) throw_error(ERROR_OTHER);
        return m_error;
    }

private:
    pthread_t m_pthread;
    int m_index;
    const char* m_database_path;
    bool m_error;

    static void* run(void* arg)
    {
        ThreadWrapper &e = *static_cast<ThreadWrapper*>(arg);
        try {
            thread(e.m_index, e.m_database_path);
        }
        catch (...) {
            e.m_error = true;
        }
        return 0;
    }
};

} // anonymous namespace



TEST(Transactions)
{
    const char* database_path = "transactions.tightdb";
    remove(database_path);
    remove((database_path+string(".lock")).c_str());

    // Run N rounds in each thread
    {
        ThreadWrapper threads[num_threads];

        // Start threads
        for (int i=0; i<num_threads; ++i) {
            threads[i].run(i, database_path);
        }

        // Wait for threads to finish
        for (int i=0; i<num_threads; ++i) {
            CHECK(!threads[i].join());
        }
    }

    // Verify database contents
    {
        size_t table1_theta_size = 0;
        for (int i=0; i<num_threads; ++i) table1_theta_size += 13 / (1+i) * 8;
        table1_theta_size *= num_rounds;
        table1_theta_size += 2;

        SharedGroup db(database_path);
        CHECK(db.is_valid());
        {
            const Group& group = db.begin_read();
            MyTable::ConstRef table = group.get_table<MyTable>("my_table");
            CHECK(2 <= table->size());

            CHECK_EQUAL(num_threads*num_rounds*4, table[0].alpha);
            CHECK_EQUAL(false,             table[0].beta);
            CHECK_EQUAL(moja,              table[0].gamma);
            CHECK_EQUAL(time_t(0),         table[0].delta);
            CHECK_EQUAL("",                table[0].epsilon);
            CHECK_EQUAL(3u,                table[0].eta->size());
            CHECK_EQUAL(0,                 table[0].theta);

            CHECK_EQUAL(749321,            table[1].alpha);
            CHECK_EQUAL(true,              table[1].beta);
            CHECK_EQUAL(kumi_na_tatu,      table[1].gamma);
            CHECK_EQUAL(time_t(99992),     table[1].delta);
            CHECK_EQUAL("click",           table[1].epsilon);
            CHECK_EQUAL(0u,                table[1].eta->size());
            CHECK_EQUAL(table1_theta_size, table[1].theta.get_subtable_size());
            CHECK(table[1].theta.is_subtable<MyTable>());

            {
                MySubtable::ConstRef subtable = table[0].eta;
                CHECK_EQUAL(num_threads*num_rounds*2, subtable[0].foo);
                CHECK_EQUAL(size_t(num_threads), subtable[0].bar->size());
                CHECK_EQUAL(100, subtable[1].foo);
                CHECK_EQUAL(0u,  subtable[1].bar->size());
                CHECK_EQUAL(0u,  subtable[2].bar->size());

                MySubsubtable::ConstRef subsubtable = subtable[0].bar;
                for (int i=0; i<num_threads; ++i) {
                    CHECK_EQUAL(1000+i, subsubtable[i].value);
                    const size_t size = (512 + i%1024) * 1024;
                    mem_buf<char> data(size);
                    for (size_t j=0; j<size; ++j)
                        data[j] = static_cast<unsigned char>((j+i) * 677 % 256);
                    CHECK_EQUAL(BinaryData(data.get(), size), subsubtable[i].binary);
                }
            }

            {
                MyTable::ConstRef subtable = table[1].theta.get_subtable<MyTable>();
                for (size_t i=0; i<table1_theta_size; ++i) {
                    CHECK_EQUAL(false,           subtable[i].beta);
                    CHECK_EQUAL(0,               subtable[i].delta);
                    CHECK_EQUAL(BinaryData(0,0), subtable[i].zeta);
                    CHECK_EQUAL(0u,              subtable[i].eta->size());
                    if (4 <= i) {
                        CHECK_EQUAL(COLUMN_TYPE_INT, subtable[i].theta.get_type());
                    }
                }
                CHECK_EQUAL(size_t(num_threads*num_rounds*5),
                            subtable[0].theta.get_subtable_size());
                CHECK(subtable[0].theta.is_subtable<MyTable>());
                CHECK_EQUAL(0u, subtable[1].theta.get_subtable_size());
                CHECK(subtable[1].theta.is_subtable<MySubtable>());
                CHECK_EQUAL(size_t(num_threads*num_rounds*9),
                            subtable[2].theta.get_subtable_size());
                CHECK(subtable[2].theta.is_subtable<MySubsubtable>());
                CHECK_EQUAL(size_t(num_threads*num_rounds*9),
                            subtable[3].theta.get_subtable_size());
                CHECK(subtable[3].theta.is_subtable<MySubsubtable>());

                MyTable::ConstRef subsubtable = subtable[0].theta.get_subtable<MyTable>();
                for (int i=0; i<num_threads*num_rounds; ++i) {
                    CHECK_EQUAL(0,       subsubtable[5*i+0].alpha);
                    CHECK_EQUAL(1,       subsubtable[5*i+1].alpha);
                    CHECK_EQUAL(2,       subsubtable[5*i+2].alpha);
                    CHECK_EQUAL(3,       subsubtable[5*i+3].alpha);
                    CHECK_EQUAL(4,       subsubtable[5*i+4].alpha);
                    CHECK_EQUAL(false,   subsubtable[5*i+0].beta);
                    CHECK_EQUAL(false,   subsubtable[5*i+1].beta);
                    CHECK_EQUAL(false,   subsubtable[5*i+2].beta);
                    CHECK_EQUAL(false,   subsubtable[5*i+3].beta);
                    CHECK_EQUAL(false,   subsubtable[5*i+4].beta);
                    CHECK_EQUAL(nne,     subsubtable[5*i+0].gamma);
                    CHECK_EQUAL(tano,    subsubtable[5*i+1].gamma);
                    CHECK_EQUAL(sita,    subsubtable[5*i+2].gamma);
                    CHECK_EQUAL(saba,    subsubtable[5*i+3].gamma);
                    CHECK_EQUAL(nane,    subsubtable[5*i+4].gamma);
                    CHECK_EQUAL(0,       subsubtable[5*i+0].delta);
                    CHECK_EQUAL(0,       subsubtable[5*i+1].delta);
                    CHECK_EQUAL(0,       subsubtable[5*i+2].delta);
                    CHECK_EQUAL(0,       subsubtable[5*i+3].delta);
                    CHECK_EQUAL(0,       subsubtable[5*i+4].delta);
                    CHECK_EQUAL("",      subsubtable[5*i+0].epsilon);
                    CHECK_EQUAL("",      subsubtable[5*i+1].epsilon);
                    CHECK_EQUAL("",      subsubtable[5*i+2].epsilon);
                    CHECK_EQUAL("",      subsubtable[5*i+3].epsilon);
                    CHECK_EQUAL("",      subsubtable[5*i+4].epsilon);
                    CHECK_EQUAL(0u,      subsubtable[5*i+0].eta->size());
                    CHECK_EQUAL(0u,      subsubtable[5*i+1].eta->size());
                    CHECK_EQUAL(0u,      subsubtable[5*i+2].eta->size());
                    CHECK_EQUAL(0u,      subsubtable[5*i+3].eta->size());
                    CHECK_EQUAL(0u,      subsubtable[5*i+4].eta->size());
                    CHECK_EQUAL("click", subsubtable[5*i+3].theta);
                }
            }
        }
        db.end_read();
    }
}
