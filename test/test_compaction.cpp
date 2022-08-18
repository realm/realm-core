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

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
using unit_test::TestContext;

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
