/*************************************************************************
 *
 * Copyright 2022 Realm Inc.
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

#include <realm.hpp>
#include "test.hpp"

#include <iostream>

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
using unit_test::TestContext;

TEST(Compaction_WhileGrowing)
{
    Random random(random_int<unsigned long>());
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(make_in_realm_history(), path);
    size_t free_space, used_space;

    auto tr = db->start_write();
    auto table1 = tr->add_table("Binaries");
    auto col_bin1 = table1->add_column(type_Binary, "str", true);
    auto table2 = tr->add_table("Integers");
    auto col_bin2 = table2->add_column(type_Binary, "str", true);
    tr->commit_and_continue_as_read();
    char w[5000];
    for (int i = 0; i < 5000; ++i) {
        w[i] = '0' + (i % 64);
    }
    int num = 500;
    tr->promote_to_write();
    for (int j = 0; j < num; ++j) {
        BinaryData sd(w, 200 + j);
        table1->create_object().set(col_bin1, sd);
        table2->create_object().set(col_bin2, sd);
        if (j % 10 == 0) {
            tr->commit_and_continue_as_read();
            tr->promote_to_write();
        }
    }
    tr->commit_and_continue_as_read();

    tr->promote_to_write();
    table1->clear();
    tr->commit_and_continue_as_read();
    db->get_stats(free_space, used_space);
    CHECK(free_space > used_space);

    // During the following, the space kept in "m_under_evacuation" will be used
    // before all elements have been moved, which will terminate that session
    while (free_space > used_space) {
        tr->promote_to_write();
        table1->create_object().set(col_bin1, BinaryData(w, 2500 + random.draw_int_mod(2500)));
        tr->commit_and_continue_as_read();
        db->get_stats(free_space, used_space);
        // std::cout << "Total: " << free_space + used_space << ", "
        //           << "Free: " << free_space << ", "
        //           << "Used: " << used_space << std::endl;
    }
}


TEST(Compaction_Large)
{
    Random random(random_int<unsigned long>());
    SHARED_GROUP_TEST_PATH(path);
    int64_t total;
    {
        DBRef db = DB::create(make_in_realm_history(), path);
        auto tr = db->start_write();
        auto t = tr->add_table("the_table");
        auto c = t->add_column(type_Binary, "str", true);
        char w[1000];
        for (int i = 0; i < 1000; ++i) {
            w[i] = '0' + (i % 10);
        }
        size_t num = 100000;
        for (size_t j = 0; j < num; ++j) {
            BinaryData sd(w, 500 + (j % 500));
            t->create_object().set(c, sd);
        }
        tr->commit_and_continue_as_read();

        tr->promote_to_write();
        int j = 0;
        for (auto o : *t) {
            BinaryData sd(w, j % 500);
            o.set(c, sd);
            ++j;
        }
        tr->commit_and_continue_as_read();

        tr->promote_to_write();
        // This will likely make the table names reside in the upper end of the file
        tr->add_table("another_table");
        tr->commit_and_continue_as_read();

        size_t free_space, used_space;
        do {
            tr->promote_to_write();
            for (j = 0; j < 500; j++) {
                int index = random.draw_int_mod(10000);
                auto obj = t->get_object(index);
                obj.set(c, BinaryData(w, j % 10));
            }
            tr->commit_and_continue_as_read();
            db->get_stats(free_space, used_space);
            // std::cout << "Total: " << free_space + used_space << ", "
            //           << "Free: " << free_space << ", "
            //           << "Used: " << used_space << std::endl;
        } while (free_space > used_space);
    }
    File f(path);
    // std::cout << "Size : " << f.get_size() << std::endl;
    {
        DB::create(make_in_realm_history(), path);
    }
    // std::cout << "Size : " << f.get_size() << std::endl;
    REALM_ASSERT(f.get_size() == total);
}
