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

#ifdef REALM_CLUSTER_IF
using OrderVec = std::vector<ObjKey>;
#else
using OrderVec = std::vector<size_t>;
#endif

namespace {

inline int_fast64_t read(TableRef table, const OrderVec& order)
{
    int_fast64_t dummy = 0;
    size_t n = order.size();
#ifdef REALM_CLUSTER_IF
    ColKey col0 = table->ndx2colkey(0);
#endif
    for (size_t i = 0; i != n; ++i)
#ifdef REALM_CLUSTER_IF
        dummy += table->get_object(order[i]).get<Int>(col0);
#else
        dummy += table->get_int(0, order[i]);
#endif
    return dummy;
}

inline void write(TableRef table, const OrderVec& order)
{
    size_t n = order.size();
#ifdef REALM_CLUSTER_IF
    ColKey col0 = table->ndx2colkey(0);
#endif
    for (size_t i = 0; i != n; ++i)
#ifdef REALM_CLUSTER_IF
        table->get_object(order[i]).set(col0, 125);
#else
        table->set_int(0, order[i], 125);
#endif
}

inline void insert(TableRef table, const OrderVec& order)
{
    size_t n = order.size();
#ifdef REALM_CLUSTER_IF
    ColKey col0 = table->ndx2colkey(0);
#endif
    for (size_t i = 0; i != n; ++i) {
#ifdef REALM_CLUSTER_IF
        table->create_object(order[i]).set(col0, 127);
#else
        table->insert_empty_row(order[i]);
        table->set_int(0, order[i], 127);
#endif
    }
}

inline void erase(TableRef table, const OrderVec& order)
{
    size_t n = order.size();
    for (size_t i = 0; i != n; ++i)
#ifdef REALM_CLUSTER_IF
        table->remove_object(order[i]);
#else
        table->remove(order[i]);
#endif
}

} // anonymous namepsace


int main()
{
    const size_t target_size = 1100 * 100L;
    const int num_tables = 20;
    std::cout << "Number of tables: " << num_tables << "\n";
    std::cout << "Elements per table: " << target_size << "\n";

    OrderVec rising_order;
    OrderVec falling_order;
    OrderVec random_order;
    OrderVec random_insert_order;
    OrderVec random_erase_order;
    for (size_t i = 0; i != target_size; ++i) {
        rising_order.emplace_back(i);
        falling_order.emplace_back(target_size - 1 - i);
        random_order.emplace_back(i);
#ifdef REALM_CLUSTER_IF
        random_insert_order.emplace_back(i);
        random_erase_order.emplace_back(i);
#else
        random_insert_order.push_back(rand() % (i + 1));
        random_erase_order.push_back(rand() % (target_size - i));
#endif
    }
    Random random;
    random.shuffle(random_order.begin(), random_order.end());
#ifdef REALM_CLUSTER_IF
    random.shuffle(random_insert_order.begin(), random_insert_order.end());
    random.shuffle(random_erase_order.begin(), random_erase_order.end());
#endif

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
    const char *id, *desc;
    {
        id = "insert_end_compact";
        desc = "Insert at end (compact)";
        for (int i = 0; i != num_tables; ++i) {
            timer.reset();
            insert(tables_1[i], rising_order);
            results.submit(id, timer);
        }
        results.finish(id, desc);

        id = "read_sequential_compact";
        desc = "Sequential read (compact)";
        for (int i = 0; i != num_tables; ++i) {
            timer.reset();
            dummy += read(tables_1[i], rising_order);
            results.submit(id, timer);
        }
        results.finish(id, desc);

        id = "read_random_compact";
        desc = "Random read (compact)";
        for (int i = 0; i != num_tables; ++i) {
            timer.reset();
            dummy += read(tables_1[i], random_order);
            results.submit(id, timer);
        }
        results.finish(id, desc);

        id = "write_sequential_compact";
        desc = "Sequential write (compact)";
        for (int i = 0; i != num_tables; ++i) {
            timer.reset();
            write(tables_1[i], rising_order);
            results.submit(id, timer);
        }
        results.finish(id, desc);

        id = "write_random_compact";
        desc = "Random write (compact)";
        for (int i = 0; i != num_tables; ++i) {
            timer.reset();
            write(tables_1[i], random_order);
            results.submit(id, timer);
        }
        results.finish(id, desc);

        id = "erase_end_compact";
        desc = "Erase from end (compact)";
        for (int i = 0; i != num_tables; ++i) {
            timer.reset();
            erase(tables_1[i], falling_order);
            results.submit(id, timer);
        }
        results.finish(id, desc);
    }

    // Start again using a random order (generalized).
    {
        id = "insert_random_general";
        desc = "Random insert (general)";
        for (int i = 0; i != num_tables; ++i) {
            timer.reset();
            insert(tables_2[i], random_insert_order);
            results.submit(id, timer);
        }
        results.finish(id, desc);
        id = "read_sequential_general";
        desc = "Sequential read (general)";
        for (int i = 0; i != num_tables; ++i) {
            dummy += read(tables_2[0], rising_order);
            results.submit(id, timer);
        }
        results.finish(id, desc);
        id = "read_random_general";
        desc = "Random read (general)";
        for (int i = 0; i != num_tables; ++i) {
            dummy += read(tables_2[0], random_order);
            results.submit(id, timer);
        }
        results.finish(id, desc);
        id = "write_sequential_general";
        desc = "Sequential write (general)";
        for (int i = 0; i != num_tables; ++i) {
            write(tables_2[i], rising_order);
            results.submit(id, timer);
        }
        results.finish(id, desc);
        id = "write_random_general";
        desc = "Random write (general)";
        for (int i = 0; i != num_tables; ++i) {
            write(tables_2[i], random_order);
            results.submit(id, timer);
        }
        results.finish(id, desc);
        id = "erase_random_general";
        desc = "Random erase (general)";
        for (int i = 0; i != num_tables; ++i) {
            erase(tables_2[i], random_erase_order);
            results.submit(id, timer);
        }
        results.finish(id, desc);
    }

    results.submit_single("crud_total_time", "Total time", timer_total);

    std::cout << "dummy = " << dummy << " (to avoid over-optimization)\n";
}
