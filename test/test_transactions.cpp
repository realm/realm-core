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

#include <realm/history.hpp>
#include <realm/lang_bind_helper.hpp>
#include <realm/util/file.hpp>
#include <realm/group_shared.hpp>

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
    SharedGroup sg(path);
    int data_size = 12 * 1024 * 1024;
    {
        WriteTransaction wt(sg);
        Group& g = wt.get_group();
        TableRef tr = g.add_table("test");
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
        wt.commit();
    }
    {
        ReadTransaction rt(sg);
        const Group& g = rt.get_group();
        ConstTableRef tr = g.get_table("test");
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
#ifdef LEGACY_TESTS

enum MyEnum { moja, mbili, tatu, nne, tano, sita, saba, nane, tisa, kumi, kumi_na_moja, kumi_na_mbili, kumi_na_tatu };

const int num_threads = 23;
const int num_rounds = 2;

const size_t max_blob_size = 32 * 1024; // 32 KiB

const BinaryData EmptyNonNul = BinaryData("", 0);

template <>
void TestTable::set(size_t column_ndx, size_t row_ndx, MyEnum value, bool is_default)
{
    set_int(column_ndx, row_ndx, value, is_default);
}

namespace {

constexpr size_t col_value = 0;
constexpr size_t col_binary = 1;

template <class T>
void subsubtable_add_columns(T t)
{
    t->add_column(type_Int, "value");
    t->add_column(type_Binary, "binary");
}

constexpr size_t col_foo = 0;
constexpr size_t col_bar = 1;

template <class T>
void subtable_add_columns(T t)
{
    t->add_column(type_Int, "foo");
    DescriptorRef subdesc;
    t->add_column(type_Table, "bar", &subdesc);
    subsubtable_add_columns(subdesc);
}


constexpr size_t col_alpha = 0;
constexpr size_t col_beta = 1;
constexpr size_t col_gamma = 2;
constexpr size_t col_delta = 3;
constexpr size_t col_epsilon = 4;
constexpr size_t col_zeta = 5;
constexpr size_t col_eta = 6;
constexpr size_t col_theta = 7;

template <class T>
void my_table_add_columns(T table)
{
    table->add_column(type_Int, "alpha");         // 0
    table->add_column(type_Bool, "beta");         // 1
    table->add_column(type_Int, "gamma");         // 2
    table->add_column(type_OldDateTime, "delta"); // 3
    table->add_column(type_String, "epsilon");    // 4
    table->add_column(type_Binary, "zeta");       // 5
    DescriptorRef subdesc;
    table->add_column(type_Table, "eta", &subdesc); // 6
    table->add_column(type_Mixed, "theta");         // 7

    subtable_add_columns(subdesc);
}

void round(TestContext& test_context, SharedGroup& db, int index)
{
    // Testing all value types
    {
        WriteTransaction wt(db); // Write transaction #1
        TableRef table = wt.get_or_add_table("my_table");
        if (table->is_empty()) {
            my_table_add_columns(table);

            table->add_empty_row();
            add(table, 0, false, moja, 0, "", EmptyNonNul, nullptr, Mixed(int64_t()));
            char binary_data[] = {7, 6, 5, 7, 6, 5, 4, 3, 113};
            add(table, 749321, true, kumi_na_tatu, 99992, "click", BinaryData(binary_data), nullptr, Mixed("fido"));
        }
        wt.commit();
    }

    // Add more rows
    {
        WriteTransaction wt(db); // Write transaction #2
        TableRef table = wt.get_table("my_table");
        if (table->size() < 100) {
            for (int i = 0; i < 10; ++i)
                table->add_empty_row();
        }
        table->add_int(col_alpha, 0, 1);
        wt.commit();
    }

    // Testing empty transaction
    {
        WriteTransaction wt(db); // Write transaction #3
        wt.commit();
    }

    // Testing subtables
    {
        WriteTransaction wt(db); // Write transaction #4
        TableRef table = wt.get_table("my_table");
        TableRef subtable = table->get_subtable(col_eta, 0);
        if (subtable->is_empty()) {
            add(subtable, 0, nullptr);
            add(subtable, 100, nullptr);
            add(subtable, 0, nullptr);
        }
        table->add_int(col_alpha, 0, 1);
        wt.commit();
    }

    // Testing subtables within subtables
    {
        WriteTransaction wt(db); // Write transaction #5
        TableRef table = wt.get_table("my_table");
        table->add_int(col_alpha, 0, 1);
        TableRef subtable = table->get_subtable(col_eta, 0);
        subtable->add_int(col_foo, 0, 1);
        TableRef subsubtable = subtable->get_subtable(col_bar, 0);
        for (int i = int(subsubtable->size()); i <= index; ++i)
            subsubtable->add_empty_row();
        table->add_int(col_alpha, 0, 1);
        wt.commit();
    }

    // Testing remove row
    {
        WriteTransaction wt(db); // Write transaction #6
        TableRef table = wt.get_table("my_table");
        if (3 <= table->size()) {
            if (table->get_int(col_alpha, 2) == 749321) {
                table->remove(1);
            }
            else {
                table->remove(2);
            }
        }
        TableRef subtable = table->get_subtable(col_eta, 0);
        subtable->add_int(col_foo, 0, 1);
        wt.commit();
    }

    // Testing read transaction
    {
        ReadTransaction rt(db);
        ConstTableRef table = rt.get_table("my_table");
        CHECK_EQUAL(749321, table->get_int(col_alpha, 1));
        ConstTableRef subtable = table->get_subtable(col_eta, 0);
        CHECK_EQUAL(100, subtable->get_int(col_foo, 1));
    }

    {
        WriteTransaction wt(db); // Write transaction #7
        TableRef table = wt.get_table("my_table");
        TableRef subtable = table->get_subtable(col_eta, 0);
        TableRef subsubtable = subtable->get_subtable(col_bar, 0);
        subsubtable->set_int(col_value, index, index);
        table->add_int(col_alpha, 0, 1);
        subsubtable->add_int(col_value, index, 2);
        subtable->add_int(col_foo, 0, 1);
        subsubtable->add_int(col_value, index, 2);
        wt.commit();
    }

    // Testing rollback
    {
        WriteTransaction wt(db); // Write transaction #8
        TableRef table = wt.get_table("my_table");
        TableRef subtable = table->get_subtable(col_eta, 0);
        TableRef subsubtable = subtable->get_subtable(col_bar, 0);
        table->add_int(col_alpha, 0, 1);
        subsubtable->add_int(col_value, index, 2);
        subtable->add_int(col_foo, 0, 1);
        subsubtable->add_int(col_value, index, 2);
        // Note: Implicit rollback
    }

    // Testing large chunks of data
    {
        WriteTransaction wt(db); // Write transaction #9
        TableRef table = wt.get_table("my_table");
        TableRef subtable = table->get_subtable(col_eta, 0);
        TableRef subsubtable = subtable->get_subtable(col_bar, 0);
        size_t size = ((512 + index % 1024) * 1024) % max_blob_size;
        std::unique_ptr<char[]> data(new char[size]);
        for (size_t i = 0; i < size; ++i)
            data[i] = static_cast<unsigned char>((i + index) * 677 % 256);
        subsubtable->set_binary(col_binary, index, BinaryData(data.get(), size));
        wt.commit();
    }

    {
        WriteTransaction wt(db); // Write transaction #10
        TableRef table = wt.get_table("my_table");
        TableRef subtable = table->get_subtable(col_eta, 0);
        subtable->set_int(col_foo, 2, index * 677);
        wt.commit();
    }

    {
        WriteTransaction wt(db); // Write transaction #11
        TableRef table = wt.get_table("my_table");
        size_t size = ((512 + (333 + 677 * index) % 1024) * 1024) % max_blob_size;
        std::unique_ptr<char[]> data(new char[size]);
        for (size_t i = 0; i < size; ++i)
            data[i] = static_cast<unsigned char>((i + index + 73) * 677 % 256);
        table->set_binary(col_zeta, index % 2, BinaryData(data.get(), size));
        wt.commit();
    }

    {
        WriteTransaction wt(db); // Write transaction #12
        TableRef table = wt.get_table("my_table");
        TableRef subtable = table->get_subtable(col_eta, 0);
        TableRef subsubtable = subtable->get_subtable(col_bar, 0);
        subsubtable->add_int(col_value, index, 1000);
        table->add_int(col_alpha, 0, -1);
        subsubtable->add_int(col_value, index, -2);
        subtable->add_int(col_foo, 0, -1);
        subsubtable->add_int(col_value, index, -2);
        wt.commit();
    }

    {
        WriteTransaction wt(db); // Write transaction #13
        TableRef table = wt.get_table("my_table");
        size_t size = (512 + (333 + 677 * index) % 1024) * 327;
        std::unique_ptr<char[]> data(new char[size]);
        for (size_t i = 0; i < size; ++i)
            data[i] = static_cast<unsigned char>((i + index + 73) * 677 % 256);
        table->set_binary(col_zeta, (index + 1) % 2, BinaryData(data.get(), size));
        wt.commit();
    }

    // Testing subtables in mixed column
    {
        WriteTransaction wt(db); // Write transaction #14
        TableRef table = wt.get_table("my_table");
        TableRef subtable;
        if (table->get_mixed(col_theta, 1).get_type() == type_Table) {
            subtable = table->get_subtable(col_theta, 1);
        }
        else {
            table->clear_subtable(col_theta, 1);
            subtable = table->get_subtable(col_theta, 1);
            my_table_add_columns(subtable);
            subtable->add_empty_row();
            subtable->add_empty_row();
        }
        int n = 1 + 13 / (1 + index);
        for (int i = 0; i < n; ++i) {
            BinaryData bin("", 0);
            Mixed mix = int64_t(i);
            add(subtable, 0, false, moja, 0, "alpha", bin, nullptr, mix);
            add(subtable, 1, false, mbili, 0, "beta", bin, nullptr, mix);
            add(subtable, 2, false, tatu, 0, "gamma", bin, nullptr, mix);
            add(subtable, 3, false, nne, 0, "delta", bin, nullptr, mix);
            add(subtable, 4, false, tano, 0, "epsilon", bin, nullptr, mix);
            add(subtable, 5, false, sita, 0, "zeta", bin, nullptr, mix);
            add(subtable, 6, false, saba, 0, "eta", bin, nullptr, mix);
            add(subtable, 7, false, nane, 0, "theta", bin, nullptr, mix);
        }
        wt.commit();
    }

    // Testing table optimization (unique strings enumeration)
    {
        WriteTransaction wt(db); // Write transaction #15
        TableRef table = wt.get_table("my_table");
        table->optimize();
        TableRef subtable = table->get_subtable(col_theta, 1);
        subtable->optimize();
        wt.commit();
    }

    // Testing all mixed types
    {
        WriteTransaction wt(db); // Write transaction #16
        TableRef table = wt.get_table("my_table");
        TableRef subtable = table->get_subtable(col_theta, 1);
        TableRef subsubtable;
        if (subtable->get_mixed(col_theta, 0).get_type() == type_Table) {
            subsubtable = subtable->get_subtable(col_theta, 0);
        }
        else {
            subtable->clear_subtable(col_theta, 0);
            subsubtable = subtable->get_subtable(col_theta, 0);
            my_table_add_columns(subsubtable);
        }
        size_t size = (17 + 233 * index) % 523;
        std::unique_ptr<char[]> data(new char[size]);
        for (size_t i = 0; i < size; ++i)
            data[i] = static_cast<unsigned char>((i + index + 79) * 677 % 256);
        BinaryData bin(data.get(), size);
        add(subsubtable, 0, false, nne, 0, "", bin, nullptr, Mixed(int64_t(index * 13)));
        add(subsubtable, 1, false, tano, 0, "", bin, nullptr, Mixed(index % 2 == 0 ? false : true));
        add(subsubtable, 2, false, sita, 0, "", bin, nullptr, Mixed(OldDateTime(index * 13)));
        add(subsubtable, 3, false, saba, 0, "", bin, nullptr, Mixed("click"));
        add(subsubtable, 4, false, nane, 0, "", bin, nullptr, Mixed(bin));
        wt.commit();
    }

    // Testing clearing of table with multiple subtables
    {
        WriteTransaction wt(db); // Write transaction #17
        TableRef table = wt.get_table("my_table");
        TableRef subtable = table->get_subtable(col_theta, 1);
        TableRef subsubtable;
        if (subtable->get_mixed(col_theta, 1).get_type() == type_Table) {
            subsubtable = subtable->get_subtable(col_theta, 1);
        }
        else {
            subtable->clear_subtable(col_theta, 1);
            subsubtable = subtable->get_subtable(col_theta, 1);
            subtable_add_columns(subsubtable);
        }
        int num = 8;
        for (int i = 0; i < num; ++i)
            add(subsubtable, i, nullptr);
        std::vector<TableRef> subsubsubtables;
        for (int i = 0; i < num; ++i)
            subsubsubtables.push_back(subsubtable->get_subtable(col_bar, i));
        for (int i = 0; i < 3; ++i) {
            for (int j = 0; j < num; j += 2) {
                BinaryData bin("", 0);
                add(subsubsubtables[j], (i - j) * index - 19, bin);
            }
        }
        wt.commit();
    }

    {
        WriteTransaction wt(db); // Write transaction #18
        TableRef table = wt.get_table("my_table");
        TableRef subtable = table->get_subtable(col_theta, 1);
        TableRef subsubtable = subtable->get_subtable(col_theta, 1);
        ;
        subsubtable->clear();
        wt.commit();
    }

    // Testing addition of an integer to all values in a column
    {
        WriteTransaction wt(db); // Write transaction #19
        TableRef table = wt.get_table("my_table");
        TableRef subtable = table->get_subtable(col_theta, 1);
        TableRef subsubtable;
        if (subtable->get_mixed(col_theta, 2).get_type() == type_Table) {
            subsubtable = subtable->get_subtable(col_theta, 2);
        }
        else {
            subtable->clear_subtable(col_theta, 2);
            subsubtable = subtable->get_subtable(col_theta, 2);
            subsubtable_add_columns(subsubtable);
        }
        int num = 9;
        for (int i = 0; i < num; ++i)
            add(subsubtable, i, BinaryData("", 0));
        wt.commit();
    }

    // Testing addition of an index to a column
    {
        WriteTransaction wt(db); // Write transaction #20
        TableRef table = wt.get_table("my_table");
        TableRef subtable = table->get_subtable(col_theta, 1);
        TableRef subsubtable;
        if (subtable->get_mixed(col_theta, 3).get_type() == type_Table) {
            subsubtable = subtable->get_subtable(col_theta, 3);
        }
        else {
            subtable->clear_subtable(col_theta, 3);
            subsubtable = subtable->get_subtable(col_theta, 3);
            subsubtable_add_columns(subsubtable);
            // FIXME: Reenable this when it works!!!
            // subsubtable->add_search_index(col_value);
        }
        int num = 9;
        for (int i = 0; i < num; ++i)
            add(subsubtable, i, BinaryData("", 0));
        wt.commit();
    }
}


void thread(TestContext& test_context, int index, std::string path)
{
    for (int i = 0; i < num_rounds; ++i) {
        SharedGroup db(path);
        round(test_context, db, index);
    }
}

} // anonymous namespace


TEST(Transactions_General)
{
    SHARED_GROUP_TEST_PATH(path);

    // Run N rounds in each thread
    {
        test_util::ThreadWrapper threads[num_threads];

        // Start threads
        for (int i = 0; i != num_threads; ++i)
            threads[i].start([this, i, &path] { thread(test_context, i, path); });

        // Wait for threads to finish
        for (int i = 0; i != num_threads; ++i) {
            bool thread_has_thrown = false;
            std::string except_msg;
            if (threads[i].join(except_msg)) {
                std::cerr << "Exception thrown in thread " << i << ": " << except_msg << "\n";
                thread_has_thrown = true;
            }
            CHECK(!thread_has_thrown);
        }
    }

    // Verify database contents
    size_t table1_theta_size = 0;
    for (int i = 0; i != num_threads; ++i)
        table1_theta_size += (1 + 13 / (1 + i)) * 8;
    table1_theta_size *= num_rounds;
    table1_theta_size += 2;

    SharedGroup db(path);
    ReadTransaction rt(db);
    ConstTableRef table = rt.get_table("my_table");
    CHECK(2 <= table->size());

    CHECK_EQUAL(num_threads * num_rounds * 4, table->get_int(col_alpha, 0));
    CHECK_EQUAL(false, table->get_bool(col_beta, 0));
    CHECK_EQUAL(moja, table->get_int(col_gamma, 0));
    CHECK_EQUAL(0, table->get_olddatetime(col_delta, 0));
    CHECK_EQUAL("", table->get_string(col_epsilon, 0));
    CHECK_EQUAL(3u, table->get_subtable(col_eta, 0)->size());
    CHECK_EQUAL(0, table->get_mixed(col_theta, 0));

    CHECK_EQUAL(749321, table->get_int(col_alpha, 1));
    CHECK_EQUAL(true, table->get_bool(col_beta, 1));
    CHECK_EQUAL(kumi_na_tatu, table->get_int(col_gamma, 1));
    CHECK_EQUAL(99992, table->get_olddatetime(col_delta, 1));
    CHECK_EQUAL("click", table->get_string(col_epsilon, 1));
    CHECK_EQUAL(0u, table->get_subtable(col_eta, 1)->size());
    CHECK_EQUAL(table1_theta_size, table->get_subtable_size(col_theta, 1));
    CHECK_EQUAL(table->get_subtable(col_theta, 1)->get_column_name(0), "alpha");

    {
        ConstTableRef subtable = table->get_subtable(col_eta, 0);
        CHECK_EQUAL(num_threads * num_rounds * 2, subtable->get_int(col_foo, 0));
        CHECK_EQUAL(size_t(num_threads), subtable->get_subtable(col_bar, 0)->size());
        CHECK_EQUAL(100, subtable->get_int(col_foo, 1));
        CHECK_EQUAL(0u, subtable->get_subtable(col_bar, 1)->size());
        CHECK_EQUAL(0u, subtable->get_subtable(col_bar, 2)->size());

        ConstTableRef subsubtable = subtable->get_subtable(col_bar, 0);
        for (int i = 0; i != num_threads; ++i) {
            CHECK_EQUAL(1000 + i, subsubtable->get_int(col_value, i));
            size_t size = ((512 + i % 1024) * 1024) % max_blob_size;
            std::unique_ptr<char[]> data(new char[size]);
            for (size_t j = 0; j != size; ++j)
                data[j] = static_cast<unsigned char>((j + i) * 677 % 256);
            CHECK_EQUAL(BinaryData(data.get(), size), subsubtable->get_binary(col_binary, i));
        }
    }

    {
        ConstTableRef subtable = table->get_subtable(col_theta, 1);
        for (size_t i = 0; i < table1_theta_size; ++i) {
            CHECK_EQUAL(false, subtable->get_bool(col_beta, i));
            CHECK_EQUAL(0, subtable->get_olddatetime(col_delta, i));
            CHECK_EQUAL(BinaryData("", 0), subtable->get_binary(col_zeta, i));
            CHECK_EQUAL(0u, subtable->get_subtable(col_eta, i)->size());
            if (4 <= i)
                CHECK_EQUAL(type_Int, subtable->get_mixed(col_theta, i).get_type());
        }
        CHECK_EQUAL(size_t(num_threads * num_rounds * 5), subtable->get_subtable_size(col_theta, 0));
        CHECK_EQUAL(subtable->get_subtable(col_theta, 0)->get_column_name(col_alpha), "alpha");
        CHECK_EQUAL(0u, subtable->get_subtable_size(col_theta, 1));
        CHECK_EQUAL(subtable->get_subtable(col_theta, 1)->get_column_name(col_foo), "foo");
        CHECK_EQUAL(size_t(num_threads * num_rounds * 9), subtable->get_subtable_size(col_theta, 2));
        CHECK_EQUAL(subtable->get_subtable(col_theta, 2)->get_column_name(col_value), "value");
        CHECK_EQUAL(size_t(num_threads * num_rounds * 9), subtable->get_subtable_size(col_theta, 3));
        CHECK_EQUAL(subtable->get_subtable(col_theta, 3)->get_column_name(col_value), "value");

        ConstTableRef subsubtable = subtable->get_subtable(col_theta, 0);
        for (int i = 0; i < num_threads * num_rounds; ++i) {
            CHECK_EQUAL(0, subsubtable->get_int(col_alpha, 5 * i + 0));
            CHECK_EQUAL(1, subsubtable->get_int(col_alpha, 5 * i + 1));
            CHECK_EQUAL(2, subsubtable->get_int(col_alpha, 5 * i + 2));
            CHECK_EQUAL(3, subsubtable->get_int(col_alpha, 5 * i + 3));
            CHECK_EQUAL(4, subsubtable->get_int(col_alpha, 5 * i + 4));
            CHECK_EQUAL(false, subsubtable->get_bool(col_beta, 5 * i + 0));
            CHECK_EQUAL(false, subsubtable->get_bool(col_beta, 5 * i + 1));
            CHECK_EQUAL(false, subsubtable->get_bool(col_beta, 5 * i + 2));
            CHECK_EQUAL(false, subsubtable->get_bool(col_beta, 5 * i + 3));
            CHECK_EQUAL(false, subsubtable->get_bool(col_beta, 5 * i + 4));
            CHECK_EQUAL(nne, subsubtable->get_int(col_gamma, 5 * i + 0));
            CHECK_EQUAL(tano, subsubtable->get_int(col_gamma, 5 * i + 1));
            CHECK_EQUAL(sita, subsubtable->get_int(col_gamma, 5 * i + 2));
            CHECK_EQUAL(saba, subsubtable->get_int(col_gamma, 5 * i + 3));
            CHECK_EQUAL(nane, subsubtable->get_int(col_gamma, 5 * i + 4));
            CHECK_EQUAL(0, subsubtable->get_olddatetime(col_delta, 5 * i + 0));
            CHECK_EQUAL(0, subsubtable->get_olddatetime(col_delta, 5 * i + 1));
            CHECK_EQUAL(0, subsubtable->get_olddatetime(col_delta, 5 * i + 2));
            CHECK_EQUAL(0, subsubtable->get_olddatetime(col_delta, 5 * i + 3));
            CHECK_EQUAL(0, subsubtable->get_olddatetime(col_delta, 5 * i + 4));
            CHECK_EQUAL("", subsubtable->get_string(col_epsilon, 5 * i + 0));
            CHECK_EQUAL("", subsubtable->get_string(col_epsilon, 5 * i + 1));
            CHECK_EQUAL("", subsubtable->get_string(col_epsilon, 5 * i + 2));
            CHECK_EQUAL("", subsubtable->get_string(col_epsilon, 5 * i + 3));
            CHECK_EQUAL("", subsubtable->get_string(col_epsilon, 5 * i + 4));
            CHECK_EQUAL(0u, subsubtable->get_subtable(col_eta, 5 * i + 0)->size());
            CHECK_EQUAL(0u, subsubtable->get_subtable(col_eta, 5 * i + 1)->size());
            CHECK_EQUAL(0u, subsubtable->get_subtable(col_eta, 5 * i + 2)->size());
            CHECK_EQUAL(0u, subsubtable->get_subtable(col_eta, 5 * i + 3)->size());
            CHECK_EQUAL(0u, subsubtable->get_subtable(col_eta, 5 * i + 4)->size());
            CHECK_EQUAL("click", subsubtable->get_mixed(col_theta, 5 * i + 3).get_string());
        }
    }
    // End of read transaction
}
#endif

#ifdef LEGACY_TESTS

TEST(Transactions_RollbackCreateObject)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    SharedGroup sg_w(*hist_w, SharedGroupOptions(crypt_key()));
    WriteTransaction wt(sg_w);
    Group& g = wt.get_group();

    auto tk = g.add_table("t0")->get_key();
    g.get_table(tk)->add_column(type_Int, "integers");

    LangBindHelper::commit_and_continue_as_read(sg_w);
    LangBindHelper::promote_to_write(sg_w);

    g.get_table(tk)->create_object(Key(0)).set(0, 5);
    auto o = g.get_table(tk)->get_object(Key(0));
    CHECK_EQUAL(o.get<int64_t>(0), 5);
    LangBindHelper::rollback_and_continue_as_read(sg_w);


    CHECK_THROW(o.get<int64_t>(0), InvalidKey);
    g.verify();

    LangBindHelper::promote_to_write(sg_w);

    CHECK_EQUAL(g.get_table(tk)->size(), 0);
}


<<<<<<< HEAD
// Rollback a table move operation and check accessors.
// This case checks column accessors when a table is inserted, moved, rolled back.
// In this case it is easy to see (by just looking at the assert message) that the
// accessors have not been updated after rollback because the column count is swapped.
TEST(Transactions_RollbackMoveTableColumns)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    SharedGroup sg_w(*hist_w, SharedGroupOptions(crypt_key()));
    WriteTransaction wt(sg_w);
    Group& g = wt.get_group();

    auto t0k = g.add_table("t0")->get_key();
    g.get_table(t0k)->insert_column_link(0, type_Link, "t0_link0_to_t0", *g.get_table(t0k));

    LangBindHelper::commit_and_continue_as_read(sg_w);
    LangBindHelper::promote_to_write(sg_w);

    g.add_table("t1")->get_key();

    g.add_table(0, "inserted_at_the end");
    LangBindHelper::rollback_and_continue_as_read(sg_w);

    g.verify(); // table.cpp:5249: [realm-core-0.97.0] Assertion failed: col_ndx <= m_cols.size() [2, 0]

    LangBindHelper::promote_to_write(sg_w);

    CHECK_EQUAL(g.get_table(t0k)->get_name(), StringData("t0"));
    CHECK_EQUAL(g.size(), 1);
}

// Rollback a table move operation and check accessors.
// This case reveals that after cancelling a table move operation
// the accessor references in memory are not what they should be
TEST(Transactions_RollbackMoveTableReferences)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    SharedGroup sg_w(*hist_w, SharedGroupOptions(crypt_key()));
    WriteTransaction wt(sg_w);
    Group& g = wt.get_group();

    auto t0k = g.add_table(0, "t0")->get_key();
    g.get_table(t0k)->insert_column(0, type_Int, "t0_int0");

    LangBindHelper::commit_and_continue_as_read(sg_w);
    LangBindHelper::promote_to_write(sg_w);
    g.add_table("t1");
    LangBindHelper::rollback_and_continue_as_read(sg_w);

    g.verify(); // array.cpp:2111: [realm-core-0.97.0] Assertion failed: ref_in_parent == m_ref [112, 4864]

    LangBindHelper::promote_to_write(sg_w);

    CHECK_EQUAL(g.get_table(t0k)->get_name(), StringData("t0"));
    CHECK_EQUAL(g.size(), 1);
}

=======
>>>>>>> v5.0.1
// Check that the spec.enumkeys become detached when
// rolling back the insertion of a string enum column
#ifdef LEGACY_TESTS
TEST(LangBindHelper_RollbackStringEnumInsert)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    std::unique_ptr<Replication> hist_2(make_in_realm_history(path));
    SharedGroup sg_w(*hist_w);
    SharedGroup sg_2(*hist_2);
    Group& g = const_cast<Group&>(sg_w.begin_read());
    Group& g2 = const_cast<Group&>(sg_2.begin_read());
    LangBindHelper::promote_to_write(sg_w);

    auto populate_with_string_enum = [](TableRef t) {
        t->add_column(type_String, "t1_col0_string");
        t->add_empty_row(3);
        t->set_string(0, 0, "simple string");
        t->set_string(0, 1, "duplicate");
        t->set_string(0, 2, "duplicate");
        bool force = true;
        t->optimize(force); // upgrade to internal string enum column type
    };

    g.add_table("t0");
    auto t1k = g.add_table("t1")->get_key();

    LangBindHelper::commit_and_continue_as_read(sg_w);
    LangBindHelper::promote_to_write(sg_w);

    populate_with_string_enum(g.get_table(t1k));

    LangBindHelper::rollback_and_continue_as_read(sg_w);
    LangBindHelper::promote_to_write(sg_w);

    populate_with_string_enum(g.get_table(t1k));

    g.get_table(t1k)->set_string(0, 0, "duplicate");

    LangBindHelper::commit_and_continue_as_read(sg_w);
    LangBindHelper::advance_read(sg_2);

    CHECK_EQUAL(g2.get_table(t1k)->size(), 3);
    CHECK_EQUAL(g2.get_table(t1k)->get_string(0, 2), "duplicate");

    CHECK_EQUAL(g.size(), 2);
    CHECK_EQUAL(g.get_table(t1k)->get_column_count(), 1);
    CHECK_EQUAL(g.get_table(t1k)->size(), 3);
}
#endif

// Check that the table.spec.subspec array becomes detached
// after rolling back the insertion of a subspec type
TEST(LangBindHelper_RollbackLinkInsert)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));

    SharedGroup sg_w(*hist_w);
    Group& g = const_cast<Group&>(sg_w.begin_read());
    LangBindHelper::promote_to_write(sg_w);

    auto t0k = g.add_table("t0")->get_key();
    auto t1k = g.add_table("t1")->get_key();

    LangBindHelper::commit_and_continue_as_read(sg_w);
    LangBindHelper::promote_to_write(sg_w);

    g.get_table(t1k)->add_column_link(type_LinkList, "t1_col0_link", *g.get_table(t0k));
    // or
    // g.get_table(0)->add_column_link(type_Link, "t0_col0_link", *g.get_table(1));

    LangBindHelper::rollback_and_continue_as_read(sg_w);
    LangBindHelper::promote_to_write(sg_w);

    g.add_table("t2");
    g.get_table(t1k)->add_column_link(type_Link, "link", *g.get_table(t0k));
    // or
    // g.get_table(0)->add_column_link(type_Link, "link", *g.get_table(1));

    g.add_table("t3");

    CHECK_EQUAL(g.size(), 4);
    CHECK_EQUAL(g.get_table(t1k)->get_column_count(), 1);
    CHECK_EQUAL(g.get_table(t1k)->get_link_target(0), g.get_table(t0k));
}
#endif


#endif // TEST_TRANSACTIONS
