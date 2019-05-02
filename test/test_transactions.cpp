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

#include "testsettings.hpp"
#ifdef TEST_TRANSACTIONS

#include <cstdio>
#include <vector>
#include <sstream>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <thread>

#include <realm/history.hpp>
#include <realm/util/file.hpp>
#include <realm/db.hpp>

#include "util/crypt_key.hpp"
#include "util/thread_wrapper.hpp"

#include "test.hpp"
#include "test_table_helper.hpp"

using namespace realm;
using namespace realm::util;
using test_util::unit_test::TestContext;
using realm::test_util::crypt_key;


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

TEST(Transactions_LargeMappingChange)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path);
    int data_size = 12 * 1024 * 1024;
    {
        TransactionRef g = sg->start_write();
        TableRef tr = g->add_table("test");
        auto col = tr->add_column(type_Binary, "binary");
        char* data = new char[data_size];
        for (int i = 0; i < data_size; i += 721) {
            data[i] = i & 0xFF;
        }
        for (int i = 0; i < 20; ++i) {
            auto obj = tr->create_object();
            obj.set(col, BinaryData(data, data_size));
            auto data2 = obj.get<BinaryData>(col);
            for (int k = 0; k < data_size; k += 721) {
                const char* p = data2.data();
                CHECK_EQUAL((p[k] & 0xFF), (k & 0xFF));
            }
        }
        delete[] data;
        g->commit();
    }
    {
        TransactionRef g = sg->start_read();
        ConstTableRef tr = g->get_table("test");
        auto col = tr->get_column_key("binary");
        for (auto it = tr->begin(); it != tr->end(); ++it) {
            auto data = it->get<BinaryData>(col);
            for (int i = 0; i < data_size; i += 721) {
                const char* p = data.data();
                CHECK_EQUAL((p[i] & 0xFF), (i & 0xFF));
            }
        }
    }
}

// This Header declaration must match the file format header declared in alloc_slab.hpp
// (we cannot use the original one, as it is private, and I don't want make new friends)
struct Header {
    uint64_t m_top_ref[2]; // 2 * 8 bytes
    // Info-block 8-bytes
    uint8_t m_mnemonic[4];    // "T-DB"
    uint8_t m_file_format[2]; // See `library_file_format`
    uint8_t m_reserved;
    // bit 0 of m_flags is used to select between the two top refs.
    uint8_t m_flags;
};

TEST(Transactions_LargeUpgrade)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path);
    int data_size = 12 * 1024 * 1024;
    {
        TransactionRef g = sg->start_write();
        TableRef tr = g->add_table("test");
        auto col = tr->add_column(type_Binary, "binary");
        char* data = new char[data_size];
        for (int i = 0; i < data_size; i += 721) {
            data[i] = i & 0xFF;
        }
        for (int i = 0; i < 20; ++i) {
            auto obj = tr->create_object();
            obj.set(col, BinaryData(data, data_size));
            auto data2 = obj.get<BinaryData>(col);
            for (int k = 0; k < data_size; k += 721) {
                const char* p = data2.data();
                CHECK_EQUAL((p[k] & 0xFF), (k & 0xFF));
            }
        }
        delete[] data;
        g->commit();
    }
    sg->close();
    {
        util::File f(path, util::File::mode_Update);
        util::File::Map<Header> headerMap(f, util::File::access_ReadWrite);
        auto* header = headerMap.get_addr();
        // at least one of the versions in the header must be 10.
        CHECK(header->m_file_format[1] == 10 || header->m_file_format[0] == 10);
        header->m_file_format[1] = header->m_file_format[0] = 9; // downgrade (both) to previous version
        headerMap.sync();
    }
    sg = DB::create(path); // triggers idempotent upgrade - but importantly for this test, uses compat mapping
    {
        // compat mapping is in effect for this part of the test
        {
            TransactionRef g = sg->start_read();
            ConstTableRef tr = g->get_table("test");
            auto col = tr->get_column_key("binary");
            for (auto it = tr->begin(); it != tr->end(); ++it) {
                auto data = it->get<BinaryData>(col);
                for (int i = 0; i < data_size; i += 721) {
                    const char* p = data.data();
                    CHECK_EQUAL((p[i] & 0xFF), (i & 0xFF));
                }
            }
        }
        // grow the file further to trigger combined use of compatibility mapping and ordinary mappings
        char* data = new char[data_size];
        for (int i = 0; i < data_size; i += 721) {
            data[i] = i & 0xFF;
        }
        auto g = sg->start_write();
        auto tr = g->get_table("test");
        auto col = tr->get_column_key("binary");
        for (int i = 0; i < 10; ++i) {
            auto obj = tr->create_object();
            obj.set(col, BinaryData(data, data_size));
            auto data2 = obj.get<BinaryData>(col);
            for (int k = 0; k < data_size; k += 721) {
                const char* p = data2.data();
                CHECK_EQUAL((p[k] & 0xFF), (k & 0xFF));
            }
        }
        delete[] data;
        g->commit();
    }
    sg->close();           // file has been upgrade to version 10, so....
    sg = DB::create(path); // when opened again, compatibility mapping is NOT in use:
    {
        TransactionRef g = sg->start_read();
        ConstTableRef tr = g->get_table("test");
        auto col = tr->get_column_key("binary");
        for (auto it = tr->begin(); it != tr->end(); ++it) {
            auto data = it->get<BinaryData>(col);
            for (int i = 0; i < data_size; i += 721) {
                const char* p = data.data();
                CHECK_EQUAL((p[i] & 0xFF), (i & 0xFF));
            }
        }
    }
}

TEST(Transactions_StateChanges)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef db = DB::create(*hist_w);
    TransactionRef writer = db->start_write();
    TableRef tr = writer->add_table("hygge");
    auto col = tr->add_column(type_Int, "hejsa");
    auto lcol = tr->add_column_list(type_Int, "gurgle");
    auto obj = tr->create_object().set_all(45);
    Lst<int64_t> list = obj.get_list<int64_t>(lcol);
    list.add(5);
    list.add(7);
    // verify that we cannot freeze a write transaction
    CHECK_THROW(writer->freeze(), realm::LogicError);
    writer->commit_and_continue_as_read();
    // verify that we cannot modify data in a read transaction
    // FIXME: Checks are not applied at group level yet.
    // CHECK_THROW(writer->add_table("gylle"), realm::LogicError);
    CHECK_THROW(obj.set(col, 100), realm::LogicError);
    // verify that we can freeze a read transaction
    TransactionRef frozen = writer->freeze();
    // verify that we can handover an accessor directly to the frozen transaction.
    auto frozen_obj = frozen->import_copy_of(obj);
    // verify that we can read the correct value(s)
    int64_t val = frozen_obj.get<int64_t>(col);
    CHECK_EQUAL(45, val);
    // FIXME: Why does  this cause a write?
    auto list2 = frozen_obj.get_list<int64_t>(lcol);
    CHECK_EQUAL(list2.get(0), 5);
    CHECK_EQUAL(list2.get(1), 7);
    // verify that we can't change it
    CHECK_THROW(frozen_obj.set<int64_t>(col, 47), realm::LogicError);
    // verify handover of a list
    // FIXME: no change should be needed here
    auto frozen_list = frozen->import_copy_of(list);
    auto frozen_int_list = dynamic_cast<Lst<int64_t>*>(frozen_list.get());
    CHECK(frozen_int_list);
    CHECK_EQUAL(frozen_int_list->get(0), 5);
    CHECK_EQUAL(frozen_int_list->get(1), 7);

    // verify that a fresh read transaction is read only
    TransactionRef reader = db->start_read();
    tr = reader->get_table("hygge");
    CHECK_THROW(tr->create_object(), realm::LogicError);
    // ..but if promoted, becomes writable
    reader->promote_to_write();
    tr->create_object();
    // ..and if rolled back, becomes read-only again
    reader->rollback_and_continue_as_read();
    CHECK_THROW(tr->create_object(), realm::LogicError);
}

namespace {

void writer_thread(TestContext& test_context, int runs, DBRef db, TableKey tk)
{
    try {
        for (int n = 0; n < runs; ++n) {
            auto writer = db->start_write();
            // writer->verify();
            auto table = writer->get_table(tk);
            auto obj = table->get_object(0);
            auto cols = table->get_column_keys();
            int64_t a = obj.get<int64_t>(cols[0]);
            int64_t b = obj.get<int64_t>(cols[1]);
            CHECK_EQUAL(a * a, b);
            obj.set_all(a + 1, (a + 1) * (a + 1));
            writer->commit();
        }
    }
    catch (std::runtime_error& e) {
        std::cout << "gylle: " << e.what() << std::endl;
    }
    catch (realm::LogicError& e) {
        std::cout << "gylle2: " << e.what() << std::endl;
    }
    catch (...) {
        std::cout << "VERY GYLLE" << std::endl;
    }
}

void verifier_thread(TestContext& test_context, int limit, DBRef db, TableKey tk)
{
    bool done = false;
    while (!done) {
        auto reader = db->start_read();
        // reader->verify();
        auto table = reader->get_table(tk);
        auto obj = table->get_object(0);
        auto cols = table->get_column_keys();
        int64_t a = obj.get<int64_t>(cols[0]);
        int64_t b = obj.get<int64_t>(cols[1]);
        CHECK_EQUAL(a * a, b);
        done = (a >= limit);
    }
}

void verifier_thread_advance(TestContext& test_context, int limit, DBRef db, TableKey tk)
{
    auto reader = db->start_read();
    bool done = false;
    while (!done) {
        reader->advance_read();
        auto table = reader->get_table(tk);
        auto obj = table->get_object(0);
        auto cols = table->get_column_keys();
        int64_t a = obj.get<int64_t>(cols[0]);
        int64_t b = obj.get<int64_t>(cols[1]);
        CHECK_EQUAL(a * a, b);
        done = (a >= limit);
    }
}
}

TEST(Transactions_Threaded)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef db = DB::create(*hist_w);
    TableKey tk;
    {
        auto wt = db->start_write();
        auto table = wt->add_table("my_table");
        table->add_column(type_Int, "my_col_1");
        table->add_column(type_Int, "my_col_2");
        table->create_object().set_all(1, 1);
        tk = table->get_key();
        wt->commit();
    }
#if defined(_WIN32) || REALM_ANDROID
    const int num_threads = 2;
#else
    const int num_threads = 20;
#endif
    const int num_iterations = 100;
    std::thread writers[num_threads];
    std::thread verifiers[num_threads];
    for (int i = 0; i < num_threads; ++i) {
        verifiers[i] = std::thread([&] { verifier_thread(test_context, num_threads * num_iterations, db, tk); });
        writers[i] = std::thread([&] { writer_thread(test_context, num_iterations, db, tk); });
    }
    for (int i = 0; i < num_threads; ++i) {
        writers[i].join();
        verifiers[i].join();
    }
}

TEST(Transactions_ThreadedAdvanceRead)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef db = DB::create(*hist_w);
    TableKey tk;
    {
        auto wt = db->start_write();
        auto table = wt->add_table("my_table");
        table->add_column(type_Int, "my_col_1");
        table->add_column(type_Int, "my_col_2");
        table->create_object().set_all(1, 1);
        tk = table->get_key();
        wt->commit();
    }
#if defined(_WIN32) || REALM_ANDROID
    const int num_threads = 2;
#else
    const int num_threads = 20;
#endif
    const int num_iterations = 100;
    std::thread writers[num_threads];
    std::thread verifiers[num_threads];
    for (int i = 0; i < num_threads; ++i) {
        verifiers[i] =
            std::thread([&] { verifier_thread_advance(test_context, num_threads * num_iterations, db, tk); });
        writers[i] = std::thread([&] { writer_thread(test_context, num_iterations, db, tk); });
    }
    for (int i = 0; i < num_threads; ++i) {
        writers[i].join();
        verifiers[i].join();
    }
}

TEST(Transactions_ListOfBinary)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(path);
    {
        auto wt = db->start_write();
        auto table = wt->add_table("my_table");
        table->add_column_list(type_Binary, "list");
        table->create_object();
        wt->commit();
    }
    for (int iter = 0; iter < 1000; iter++) {
        auto wt = db->start_write();
        wt->verify();
        auto table = wt->get_table("my_table");
        auto col1 = table->get_column_key("list");
        Obj obj = table->get_object(0);
        auto list = obj.get_list<Binary>(col1);
        std::string bin(15, 'z');
        list.add(BinaryData(bin.data(), 15));
        if (fastrand(100) < 5) {
            size_t sz = list.size();
            for (size_t i = 0; i < sz - 1; i++) {
                list.remove(0);
            }
        }
        wt->commit();
        auto rt = db->start_read();
        rt->verify();
    }
}



TEST(Transactions_RollbackCreateObject)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    TransactionRef tr = sg_w->start_write();

    auto tk = tr->add_table("t0")->get_key();
    auto col = tr->get_table(tk)->add_column(type_Int, "integers");

    tr->commit_and_continue_as_read();
    tr->promote_to_write();

    tr->get_table(tk)->create_object(ObjKey(0)).set(col, 5);
    auto o = tr->get_table(tk)->get_object(ObjKey(0));
    CHECK_EQUAL(o.get<int64_t>(col), 5);

    tr->rollback_and_continue_as_read();

    CHECK_THROW(o.get<int64_t>(col), InvalidKey);
    tr->verify();

    tr->promote_to_write();

    CHECK_EQUAL(tr->get_table(tk)->size(), 0);
}

TEST(Transactions_ObjectLifetime)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    TransactionRef tr = sg_w->start_write();

    auto table = tr->add_table("t0");
    Obj obj = table->create_object();

    CHECK(obj.is_valid());
    tr->commit();
    CHECK_NOT(obj.is_valid());
}

TEST(Transactions_Continuous_ParallelWrites)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist);
    TransactionRef t = sg->start_write();
    auto _table = t->add_table("t0");
    TableKey table_key = _table->get_key();
    t->commit();

    auto t0 = std::thread([&]() {
        TransactionRef tr = sg->start_read();
        tr->promote_to_write();
        TableRef table = tr->get_table(table_key);
        table->create_object();
        tr->commit();
    });
    auto t1 = std::thread([&]() {
        TransactionRef tr = sg->start_read();
        tr->promote_to_write();
        TableRef table = tr->get_table(table_key);
        table->create_object();
        tr->commit();
    });
    t0.join();
    t1.join();
}

TEST(Transactions_Continuous_SerialWrites)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist);

    TableKey table_key;
    {
        TransactionRef tr = sg->start_write();
        auto table = tr->add_table("t0");
        table_key = table->get_key();
        tr->commit();
    }

    TransactionRef tr1 = sg->start_read();
    TransactionRef tr2 = sg->start_read();
    {
        tr1->promote_to_write();
        TableRef table = tr1->get_table(table_key);
        table->create_object();
        tr1->commit_and_continue_as_read();
    }

    {
        tr2->promote_to_write();
        TableRef table = tr2->get_table(table_key);
        table->create_object();
        tr2->commit_and_continue_as_read();
    }
}


// Check that enumeration is gone after
// rolling back the insertion of a string enum column
TEST(LangBindHelper_RollbackStringEnumInsert)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    auto sg_w = DB::create(*hist_w);
    auto g = sg_w->start_write();
    auto t = g->add_table("t1");
    auto col = t->add_column(type_String, "t1_col0_string");

    auto populate_with_string_enum = [&]() {
        t->create_object().set_all("simple_string");
        t->create_object().set_all("duplicate");
        t->create_object().set_all("duplicate");
        t->enumerate_string_column(col); // upgrade to internal string enum column type
        CHECK(t->is_enumerated(col));
        CHECK_EQUAL(t->get_num_unique_values(col), 2);
    };

    g->commit_and_continue_as_read();
    g->promote_to_write();

    populate_with_string_enum();

    g->rollback_and_continue_as_read();
    g->promote_to_write();
    CHECK(!t->is_enumerated(col));
    populate_with_string_enum();

    t->begin()->set(col, "duplicate");

    g->commit_and_continue_as_read();
    CHECK(t->is_enumerated(col));
}

#if 0
// The following code is a longer running test, so disabled when for ordinary testing

void growth_phase(SharedGroup& sg_w)
{
    std::cout << "Growing..." << std::endl;
    for (int j = 0; j < 100; ++j) {
        //std::cout << "growth phase " << j << std::endl;
        WriteTransaction wt(sg_w);
        Group& g = wt.get_group();
        TableRef t = g.get_table("spoink");
        for (int k = 0; k < 50000; ++k) {
            auto row = t->add_empty_row();
            t->set_string(0, row, "yooodle-de-do-glabtyligok-pluut");
        }
        //std::cout << "     - commit" << std::endl;
        wt.commit();
    }
}

void query_phase(SharedGroup& sg_w)
{
    std::cout << "Querying..." << std::endl;
    for (int j = 0; j < 1; ++j) {
        //std::cout << "growth phase " << j << std::endl;
        ReadTransaction wt(sg_w);
        const Group& g = wt.get_group();
        ConstTableRef t = g.get_table("spoink");
        TableView tv = t->where().equal(0,"gylle").find_all();
    }
}

void partial_read_phase(SharedGroup& sg_w)
{
    std::cout << "Reading..." << std::endl;
    for (int j = 0; j < 100; ++j) {
        //std::cout << "growth phase " << j << std::endl;
        ReadTransaction wt(sg_w);
        const Group& g = wt.get_group();
        ConstTableRef t = g.get_table("spoink");
        int max = t->size();
        for (int z = 0; z < max/100; ++z) {
            t->get_string(0,z);
        }
    }
}

void modification_phase(SharedGroup& sg_w)
{
    std::cout << "Modifying..." << std::endl;
    int row = 0;
    for (int j = 0; j < 100; ++j) {
        //std::cout << "growth phase " << j << std::endl;
        WriteTransaction wt(sg_w);
        Group& g = wt.get_group();
        TableRef t = g.get_table("spoink");
        int max = t->size();
        for (int k = 0; k < 100000; ++k) {
            if (row == max) row = 0;
            std::string s("yooodle-glabtyligok-plut-fnytliandomcrackplaf!");
            s = s + to_string(j);
            t->set_string(0, row, s);
            ++row;
        }
        //std::cout << "     - commit" << std::endl;
        wt.commit();
    }
}

void preparations(SharedGroup& sg_w)
{
    std::cout << "Setup...." << std::endl;
    {
        WriteTransaction wt(sg_w);
        Group& g = wt.get_group();
        TableRef t = g.get_table("spoink");
        if (t.get() == nullptr) {
            t = g.add_table("spoink");
            t->add_column(type_String,"spoink-column");
        }
        wt.commit();
    }
}

// illustration of possible governor function which takes total system load into account
class ExampleGovernor : public util::PageReclaimGovernor {
public:
    std::function<int64_t()> current_target_getter(size_t load) override
    {
        return std::bind(file_control_governor, load);
    }
    void report_target_result(int64_t) override
    {
    }

private:
    static size_t system_memory_governor(size_t load) {
        try {
            auto file = fopen("/proc/meminfo","r");
            if (file == nullptr)
                return 0;
            size_t total, free;
            int r = fscanf(file,"MemTotal: %zu kB MemFree: %zu kB", &total, &free);
            if (r != 2)
                return 0;
            fclose(file);
            size_t target;
            /*
            if (free < total * 0.25)
                target = size_t(load * 0.9);
            else if (free < total * 0.3)
                target =  load;
            else
                target = size_t(load * 2.1);
            if (target > total * 768)
                target = total * 768;
                */
            target = total * 256;
            std::cout << "total: " << total << "   free: " << free
                        << "   load: " << load << "   target: " << target << "    \r";
            return target;
        } catch (...) {
            return 0;
        }
    }

    static size_t file_control_governor(size_t load) {
        try {
            auto file = fopen("governor.txt", "r");
            if (file == nullptr)
                return system_memory_governor(load);
            size_t target;
            int r = fscanf(file, "%zu", &target);
            if (r != 1)
                return system_memory_governor(load);
            fclose(file);
            std::cout << "Encryption: active data = " << load << "    set target: " << target << "   \r";
            return target;
        } catch (...) {
            return system_memory_governor(load);
        }
    }
};

ExampleGovernor example_governor;

ONLY(LangBindHelper_EncryptionGiga)
{
    //realm::util::set_page_reclaim_governor(&example_governor);
    std::string path1 = "dont_try_this_at_home1.realm";
    std::unique_ptr<Replication> hist_w1(make_in_realm_history(path1));

    std::cout << "Opening..." << path1 << std::endl;
    SharedGroup sg_w1(*hist_w1, SharedGroupOptions(crypt_key()));
    preparations(sg_w1);

    std::string path2 = "dont_try_this_at_home2.realm";
    std::unique_ptr<Replication> hist_w2(make_in_realm_history(path2));

    std::cout << "Opening..." << path2 << std::endl;
    SharedGroup sg_w2(*hist_w2, SharedGroupOptions(crypt_key()));
    preparations(sg_w2);
    for (int r = 0; r < 4; ++r) {
        growth_phase(sg_w1);
        growth_phase(sg_w2);
        modification_phase(sg_w1);
        modification_phase(sg_w2);
        partial_read_phase(sg_w1);
        partial_read_phase(sg_w2);
        query_phase(sg_w1);
        query_phase(sg_w2);
        std::cout << "Sleeping.." << std::endl;
        millisleep(10000);
    }
    millisleep(100000);
}
#endif

#endif // TEST_TRANSACTIONS
