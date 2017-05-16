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

#include "realm.hpp"
#include "../test.hpp"

#include <iostream>
#include <set>
#include <chrono>

using namespace std::chrono;
using namespace realm;

int _index_performance(unsigned nb_rows)
{
    Table table;
    table.add_column(type_Int, "keys");
    table.add_search_index(0);

    auto t1 = steady_clock::now();

    for (unsigned i = 0; i < nb_rows; i++) {
        table.add_row_with_key(0, i);
    }

    auto t2 = steady_clock::now();

    for (unsigned i = 0; i < nb_rows; i++) {
        if (table.find_first_int(0, i) != i) {
            std::cout << "*** ERROR ***" << std::endl;
            return 0;
        }
    }

    auto t3 = steady_clock::now();

    std::cout << nb_rows << " rows" << std::endl;
    std::cout << "   insertion time: " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows << " ns/key"
              << std::endl;
    std::cout << "   lookup time   : " << duration_cast<nanoseconds>(t3 - t2).count() / nb_rows << " ns/key"
              << std::endl;

    return 0;
}

int index_performance(unsigned nb_rows)
{
    Table table;
    table.add_column(type_Int, "keys");
    table.add_search_index(0);
    std::set<int> numbers;
    std::set<int> numbers_1;

    for (unsigned i = 0; i < nb_rows; i++) {
        while (true) {
            auto res = numbers.insert(rand());
            if (res.second)
                break;
        }
    }
    for (unsigned i = 0; i < nb_rows; i++) {
        while (true) {
            auto x = rand();
            if (numbers.count(x) == 0) {
                auto res = numbers_1.insert(x);
                if (res.second)
                    break;
            }
        }
    }

    auto t1 = steady_clock::now();

    for (auto n : numbers) {
        table.add_row_with_key(0, n);
    }

    auto t2 = steady_clock::now();

    unsigned i = 0;
    for (auto n : numbers) {
        if (table.find_first_int(0, n) != i) {
            std::cout << "*** ERROR ***" << std::endl;
            return 0;
        }
        i++;
    }

    auto t3 = steady_clock::now();

    for (auto n : numbers_1) {
        size_t x = rand() % nb_rows;
        table.move_last_over(x);
        table.add_row_with_key(0, n);
    }

    auto t4 = steady_clock::now();

    std::cout << nb_rows << " rows" << std::endl;
    std::cout << "   total time    : " << duration_cast<microseconds>(t3 - t1).count() << " us" << std::endl;
    std::cout << "   insertion time: " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows << " ns/key"
              << std::endl;
    std::cout << "   lookup time   : " << duration_cast<nanoseconds>(t3 - t2).count() / nb_rows << " ns/key"
              << std::endl;
    std::cout << "   replace time  : " << duration_cast<nanoseconds>(t4 - t3).count() / nb_rows << " ns/key"
              << std::endl;

    return 0;
}

TEST(Cuckoo_performance)
{
    index_performance(100);
    index_performance(1000);
    index_performance(10000);
    index_performance(100000);
}
