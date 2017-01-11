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

#include <cstdlib>
#include <algorithm>
#include <memory>
#include <iostream>

#include <realm.hpp>

#include "../util/timer.hpp"
#include "../util/random.hpp"
#include "../util/benchmark_results.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;


namespace {

inline int_fast64_t read(TableRef table, const std::vector<size_t> order)
{
    int_fast64_t dummy = 0;
    size_t n = order.size();
    for (size_t i = 0; i != n; ++i)
        dummy += table->get_int(0, order[i]);
    return dummy;
}

inline void write(TableRef table, const std::vector<size_t> order)
{
    size_t n = order.size();
    for (size_t i = 0; i != n; ++i)
        table->set_int(0, order[i], 125);
}

inline void insert(TableRef table, const std::vector<size_t> order)
{
    size_t n = order.size();
    for (size_t i = 0; i != n; ++i) {
        table->insert_empty_row(order[i]);
        table->set_int(0, order[i], 127);
    }
}

inline void erase(TableRef table, const std::vector<size_t> order)
{
    size_t n = order.size();
    for (size_t i = 0; i != n; ++i)
        table->remove(order[i]);
}

} // anonymous namepsace


int main()
{
    const size_t target_size = 1100 * 1000L;
    const int num_tables = 50;
    std::cout << "Number of tables: " << num_tables << "\n";
    std::cout << "Elements per table: " << target_size << "\n";

    std::vector<size_t> rising_order;
    std::vector<size_t> falling_order;
    std::vector<size_t> random_order;
    std::vector<size_t> random_insert_order;
    std::vector<size_t> random_erase_order;
    for (size_t i = 0; i != target_size; ++i) {
        rising_order.push_back(i);
        falling_order.push_back(target_size - 1 - i);
        random_order.push_back(i);
        random_insert_order.push_back(rand() % (i + 1));
        random_erase_order.push_back(rand() % (target_size - i));
    }
    Random random;
    random.shuffle(random_order.begin(), random_order.end());

    std::unique_ptr<Group> group;
    TableRef tables_1[num_tables], tables_2[num_tables];

    group.reset(new Group);
    bool require_unique_name = false;
    for (int i = 0; i < num_tables; ++i) {
        tables_1[i] = group->add_table("IntTable", require_unique_name);
        tables_1[i]->add_column(type_Int, "i");
    }
    for (int i = 0; i < num_tables; ++i) {
        tables_2[i] = group->add_table("IntTable", require_unique_name);
        tables_2[i]->add_column(type_Int, "i");
    }

    int_fast64_t dummy = 0;

    int max_lead_text_size = 26;
    BenchmarkResults results(max_lead_text_size);

    Timer timer_total(Timer::type_UserTime);
    Timer timer(Timer::type_UserTime);
    {
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            insert(tables_1[i], rising_order);
        results.submit_single("insert_end_compact", "Insert at end (compact)", timer);
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            dummy += read(tables_1[i], rising_order);
        results.submit_single("read_seq_compact", "Sequential read (compact)", timer);
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            dummy += read(tables_1[i], random_order);
        results.submit_single("read_ran_compact", "Random read (compact)", timer);
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            write(tables_1[i], rising_order);
        results.submit_single("write_seq_compact", "Sequential write (compact)", timer);
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            write(tables_1[i], random_order);
        results.submit_single("write_ran_compact", "Random write (compact)", timer);
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            erase(tables_1[i], falling_order);
        results.submit_single("erase_end_compact", "Erase from end (compact)", timer);
    }
    {
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            insert(tables_2[i], random_insert_order);
        results.submit_single("insert_ran_general", "Random insert (general)", timer);
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            dummy += read(tables_2[0], rising_order);
        results.submit_single("read_seq_general", "Sequential read (general)", timer);
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            dummy += read(tables_2[0], random_order);
        results.submit_single("read_ran_general", "Random read (general)", timer);
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            write(tables_2[i], rising_order);
        results.submit_single("write_seq_general", "Sequential write (general)", timer);
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            write(tables_2[i], random_order);
        results.submit_single("write_ran_general", "Random write (general)", timer);
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            erase(tables_2[i], random_erase_order);
        results.submit_single("erase_ran_general", "Random erase (general)", timer);
    }

    results.submit_single("total_time", "Total time", timer_total);

    std::cout << "dummy = " << dummy << " (to avoid over-optimization)\n";
}
