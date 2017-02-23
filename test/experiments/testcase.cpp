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

#include <cstring>
#include <typeinfo>
#include <limits>
#include <vector>
#include <map>
#include <sstream>
#include <fstream>
#include <iostream>
#include <random>
#include <algorithm>
#include <iterator>

#include <unistd.h>
#include <sys/wait.h>

#include <realm.hpp>
#include <realm/impl/destroy_guard.hpp>
#include <realm/impl/simulated_failure.hpp>
#include <realm/column_string.hpp>
#include <realm/column_string_enum.hpp>
#include <realm/column_mixed.hpp>
#include <realm/array_binary.hpp>
#include <realm/array_string_long.hpp>
#include <realm/lang_bind_helper.hpp>
#include <realm/group_shared.hpp>

#include "../test.hpp"
#include "../util/demangle.hpp"


using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
using namespace realm::_impl;

const int limit = 10000000;

TEST(PerfTest) {
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;

    std::vector<unsigned> shuffle;
    shuffle.reserve(limit);
    for (int j=0; j<limit; ++j) shuffle.push_back(j);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(shuffle.begin(), shuffle.end(), g);
    const char* path = "testing.realm";
    SharedGroup sg(path);
    {
        WriteTransaction wt(sg);
        TableRef t = wt.add_table("my_table");
        t->add_column(type_Int, "i");
        t->add_column(type_Int, "i");
        t->add_column(type_Int, "i");
        t->add_column(type_Int, "i");

        std::cout << "inserting " << limit << " keys..." << std::flush;
        start = std::chrono::high_resolution_clock::now();
        t->add_empty_row(limit);
        end = std::chrono::high_resolution_clock::now();
        std::chrono::nanoseconds ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
        std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;

        std::cout << "setting values (4 random values/key) " << limit << " keys..." << std::flush;
        start = std::chrono::high_resolution_clock::now();
        for (int idx = 0; idx < limit; ++idx) {
            t->set_int(0, idx, rand() % 2000);
            t->set_int(1, idx, rand() % 2000);
            t->set_int(2, idx, rand() % 2000);
            t->set_int(3, idx, rand() % 2000);
        }
        end = std::chrono::high_resolution_clock::now();
        ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
        std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;

        std::cout << "committing " << limit << " keys..." << std::flush;
        start = std::chrono::high_resolution_clock::now();
        wt.commit();
        end = std::chrono::high_resolution_clock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end-start);
        std::cout << "   ...done in " << ms.count() << " millisecs " << std::endl;

    }
    {
        WriteTransaction wt(sg);
        TableRef t = wt.get_table("my_table");

        std::cout << "first access (seq order) " << limit << " keys..." << std::flush;
        int sum = 0;
        start = std::chrono::high_resolution_clock::now();
        for (int idx = 0; idx < limit; ++idx) {
            sum += t->get_int(0, idx);
        }
        end = std::chrono::high_resolution_clock::now();
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
        auto baseline = ns;
        std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;

        std::cout << "2nd access, same field (seq order) " << limit << " keys..." << std::flush;
        sum = 0;
        start = std::chrono::high_resolution_clock::now();
        for (int idx = 0; idx < limit; ++idx) {
            sum += t->get_int(0, idx);
            sum += t->get_int(0, idx);
        }
        end = std::chrono::high_resolution_clock::now();
        ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
        std::cout << "   ...done in " << (ns - baseline).count() << " nsecs/key" << std::endl;

        std::cout << "2nd access, other field (seq order) " << limit << " keys..." << std::flush;
        sum = 0;
        start = std::chrono::high_resolution_clock::now();
        for (int idx = 0; idx < limit; ++idx) {
            sum += t->get_int(0, idx);
            sum += t->get_int(1, idx);
        }
        end = std::chrono::high_resolution_clock::now();
        ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
        std::cout << "   ...done in " << (ns - baseline).count() << " nsecs/key" << std::endl;



        std::cout << "first access (random order) " << limit << " keys..." << std::flush;
        sum = 0;
        start = std::chrono::high_resolution_clock::now();
        for (int j = 0; j < limit; ++j) {
            int idx = shuffle[j];
            sum += t->get_int(0, idx);
        }
        end = std::chrono::high_resolution_clock::now();
        ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
        baseline = ns;
        std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;

        std::cout << "2nd access, same field (random order) " << limit << " keys..." << std::flush;
        sum = 0;
        start = std::chrono::high_resolution_clock::now();
        for (int j = 0; j < limit; ++j) {
            int idx = shuffle[j];
            sum += t->get_int(0, idx);
            sum += t->get_int(0, idx);
        }
        end = std::chrono::high_resolution_clock::now();
        ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
        std::cout << "   ...done in " << (ns - baseline).count() << " nsecs/key" << std::endl;

        std::cout << "2nd access, other field (random order) " << limit << " keys..." << std::flush;
        sum = 0;
        start = std::chrono::high_resolution_clock::now();
        for (int j = 0; j < limit; ++j) {
            int idx = shuffle[j];
            sum += t->get_int(0, idx);
            sum += t->get_int(1, idx);
        }
        end = std::chrono::high_resolution_clock::now();
        ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
        std::cout << "   ...done in " << (ns - baseline).count() << " nsecs/key" << std::endl;



        std::cout << "manual query (4 reads/key) for " << limit << " keys..." << std::flush;
        int count = 0;
        start = std::chrono::high_resolution_clock::now();
        for (int idx = 0; idx < limit; ++idx) {
            if ((t->get_int(0, idx) < 1000) && (t->get_int(1, idx) < 1000)
                && (t->get_int(2, idx) < 1000) && (t->get_int(3, idx) < 1000))
                count++;
        }
        end = std::chrono::high_resolution_clock::now();
        ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
        std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;

        std::cout << "manual query in random order for " << limit << " keys..." << std::flush;
        int count2 = 0;
        start = std::chrono::high_resolution_clock::now();
        for (int j = 0; j < limit; ++j) {
            int idx = shuffle[j];
            if ((t->get_int(0, idx) < 1000) && (t->get_int(1, idx) < 1000)
                && (t->get_int(2, idx) < 1000) && (t->get_int(3, idx) < 1000))
                count2++;
        }
        end = std::chrono::high_resolution_clock::now();
        ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
        std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;
        CHECK_EQUAL(count, count2);

        std::cout << "builtin query (4 reads/key) for " << limit << " keys..." << std::flush;
        int count3 = 0;
        start = std::chrono::high_resolution_clock::now();
        auto q = t->where().less(0, 1000).less(1, 1000).less(2, 1000).less(3, 1000);
        count3 = q.count();
        end = std::chrono::high_resolution_clock::now();
        ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
        std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;
        CHECK_EQUAL(count, count3);

        std::cout << "committing " << limit << " keys..." << std::flush;
        start = std::chrono::high_resolution_clock::now();
        wt.commit();
        end = std::chrono::high_resolution_clock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end-start);
        std::cout << "   ...done in " << ms.count() << " millisecs " << std::endl;
    }
}

#if 0

TEST(PerfTest_UID) {
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;

    std::vector<unsigned> shuffle;
    shuffle.reserve(limit);
    for (int j=0; j<limit; ++j) shuffle.push_back(j);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(shuffle.begin(), shuffle.end(), g);
    const char* path = "testing_UID.realm";
    SharedGroup sg(path);
    {
        WriteTransaction wt(sg);
        TableRef t = wt.add_table("my_uid_table");
        t->add_column(type_Int, "i");
        t->add_column(type_Int, "i");
        t->add_column(type_Int, "i");
        t->add_column(type_Int, "i");
        t->add_column(type_Int, "i");
        t->add_search_index(4);
        std::cout << std::endl << "UID based performance:" << std::endl;
        std::cout << "inserting " << limit << " keys..." << std::flush;
        start = std::chrono::high_resolution_clock::now();
        for (int idx = 0; idx < limit; ++idx) {
            t->add_empty_row();
            t->set_int(4, idx, shuffle[idx]);
        }
        end = std::chrono::high_resolution_clock::now();
        std::chrono::nanoseconds ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
        std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;

        std::cout << "setting values (4 random values/key) UID order " << limit << " keys..." << std::flush;
        start = std::chrono::high_resolution_clock::now();
        for (int j = 0; j < limit; ++j) {
            int idx = t->find_first_int(4, j);
            t->set_int(0, idx, rand() % 2000);
            t->set_int(1, idx, rand() % 2000);
            t->set_int(2, idx, rand() % 2000);
            t->set_int(3, idx, rand() % 2000);
        }
        end = std::chrono::high_resolution_clock::now();
        ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
        std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;

        std::cout << "manual query (4 reads/key) UID order, for " << limit << " keys..." << std::flush;
        int count = 0;
        start = std::chrono::high_resolution_clock::now();
        for (int j = 0; j < limit; ++j) {
            int idx = t->find_first_int(4, j);
            if ((t->get_int(0, idx) < 1000) && (t->get_int(1, idx) < 1000)
                && (t->get_int(2, idx) < 1000) && (t->get_int(3, idx) < 1000))
                count++;
        }
        end = std::chrono::high_resolution_clock::now();
        ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
        std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;

        std::cout << "builtin query (4 reads/key) for " << limit << " keys..." << std::flush;
        int count3 = 0;
        start = std::chrono::high_resolution_clock::now();
        auto q = t->where().less(0, 1000).less(1, 1000).less(2, 1000).less(3, 1000);
        count3 = q.count();
        end = std::chrono::high_resolution_clock::now();
        ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
        std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;
        CHECK_EQUAL(count, count3);


        std::cout << "committing " << limit << " keys..." << std::flush;
        start = std::chrono::high_resolution_clock::now();
        wt.commit();
        end = std::chrono::high_resolution_clock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end-start);
        std::cout << "   ...done in " << ms.count() << " millisecs " << std::endl;
    }
}
#endif
