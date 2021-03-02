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
#include <realm/history.hpp>

#include "../util/timer.hpp"
#include "../util/random.hpp"
#include "../util/unit_test.hpp"
#include "../test.hpp"
#include "../test_table_helper.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
using unit_test::TestContext;

#ifdef REALM_CLUSTER_IF
using OrderVec = std::vector<ObjKey>;
#else
using OrderVec = std::vector<size_t>;
#endif


enum step_type { DIRECT, INDEXED_BEST, INDEXED_WORST, PK };
std::vector<std::string> step_names{"Direct", "Idx_bst", "Idx_wst", "PK"};

int main()
{
    auto run_steps = [&](int num_steps, int step_size, step_type st, const char* step_layout, bool test_rw = false) {
                         TestPathGuard guard("benchmark-insertion.realm");
                         std::string path(guard);
                         auto history = make_in_realm_history(path);
                         DBOptions options;
                         DBRef db = DB::create(*history, options);
                         {
                             WriteTransaction wt(db);
                             auto t = wt.add_table("table");
                             auto col = t->add_column(type_String, "str");
                             if (test_rw)
                                 t->add_column(type_Int, "int");
                             if (st == INDEXED_BEST || st == INDEXED_WORST) {
                                 t->add_search_index(col);
                             }
                             if (st == PK) {
                                 t->set_primary_key_column(col);
                             }
                             wt.commit();
                         }
                         //std::cout << "Building DB for type " << st << " format " << num_steps << " x " << step_size << std::endl;
                         auto total_size = num_steps * step_size;
                         std::vector<std::string> keys;
                         keys.reserve(total_size);
                         for (int j = 0; j < num_steps * step_size; j += step_size) {
                             // generate keys used in this step:
                             auto shuffle_start = keys.size();
                             for (int i = j; i < j + step_size; ++i) {
                                 keys.push_back(std::to_string(i));
                             }
                             if (st == INDEXED_WORST /* || st == PK */) {
                                 std::random_shuffle(keys.begin() + shuffle_start, keys.end());
                             }
                             auto start = std::chrono::steady_clock::now();
                             WriteTransaction wt(db);
                             auto t = wt.get_table("table");
                             auto col = t->get_column_key("str");
                             auto col2 = t->get_column_key("int");

                             for (int i = 0; i < step_size; ++i) {
                                 if (st == DIRECT || st == INDEXED_BEST || st == INDEXED_WORST) {
                                     auto o = t->create_object();
                                     o.set(col, keys[shuffle_start + i]);
                                     if (test_rw)
                                         o.set(col2, i + j);
                                 } else { // PK
                                     auto o = t->create_object_with_primary_key(keys[shuffle_start + i],{});
                                     if (test_rw)
                                         o.set(col2, i + j);
                                 }
                             }
                             if (st == INDEXED_WORST && test_rw) {
                                 // worst case spreads keys all over key space
                                 std::random_shuffle(keys.begin(), keys.end());
                                 // delete random 5% of step_size objects - and their keys
                                 auto limit = step_size / 20;
                                 while (limit--) {
                                     auto& s = keys.back();
                                     auto ok = t->find_first_string(col, s);
                                     t->remove_object(ok);
                                     keys.pop_back();
                                 }
                             }
                             wt.commit();
                             if (test_rw) {
                                 {
                                     int probe_size = 90000; // fixed number of probes - not varied with step size
                                     // cannot be a full step size, has to have room for the worst case deleted objects
                                     // from here use access to the last step_size elements of 'keys' for evaluations
                                     std::vector<Obj> objects;
                                     objects.reserve(probe_size);
                                     start = std::chrono::steady_clock::now();
                                     auto trans = db->start_write();
                                     auto t = trans->get_table("table");
                                     volatile int64_t sum = 0; // prevent optimization
                                     auto start_idx = keys.size();
                                     if (start_idx > probe_size) start_idx -= probe_size;
                                     else start_idx = 0;
                                     for (int i = 0; i < probe_size; ++i) {
                                         auto& s = keys[i + start_idx];
                                         if (st == DIRECT || st == INDEXED_BEST || st == INDEXED_WORST) {
                                             objects.push_back(t->get_object(t->find_first_string(col, s)));
                                         }
                                         else { // PK
                                             objects.push_back(t->get_object_with_primary_key(s));
                                         }
                                     }
                                     auto end = std::chrono::steady_clock::now();
                                     auto diff = end - start;
                                     auto print_diff = std::chrono::duration_cast<std::chrono::milliseconds>(diff);
                                     std::cout << step_names[st] << " " << step_layout << " ";
                                     std::cout << j + step_size << " " << print_diff.count();
                                     //std::cout << "Reading " << step_size << " properties from objects" << std::endl;
                                     start = end;
                                     for (int i = 0; i <  probe_size; ++i) {
                                         sum += objects[i].get<Int>(col2);
                                     }
                                     end = std::chrono::steady_clock::now();
                                     diff = end - start;
                                     print_diff = std::chrono::duration_cast<std::chrono::milliseconds>(diff);
                                     std::cout << " " << print_diff.count();
                                     start = end;
                                     //std::cout << "Changing " << step_size << " properties from objects" << std::endl;
                                     for (int i = 0; i < probe_size; ++i) {
                                         objects[i].set<Int>(col2, i + j + 3);
                                     }
                                     trans->commit();
                                     end = std::chrono::steady_clock::now();
                                     diff = end - start;
                                     print_diff = std::chrono::duration_cast<std::chrono::milliseconds>(diff);
                                     std::cout << " " << print_diff.count()  << std::endl;
                                 }
                                 {
                                     WriteTransaction wt(db);
                                 }
                             }
                             else {
                                 auto end = std::chrono::steady_clock::now();
                                 auto diff = end - start;
                                 auto print_diff = std::chrono::duration_cast<std::chrono::milliseconds>(diff);
                                 std::cout << step_names[st] << " " << step_layout << " ";
                                 std::cout << j << " " << print_diff.count()  << std::endl;
                             }
                         }
                     };
    auto run_type = [&](step_type st, bool test_rw = false) {
                        if (!test_rw) {
                            std::cout << "Insertion run for type " << step_names[st] << std::endl;
                            run_steps(10, 1000000, st, "10x1000000");
                            // run_steps(30, 333333, st);
                            run_steps(100, 100000, st, "100x100000");
                            // run_steps(300, 33333, st);
                            run_steps(1000, 10000, st, "1000x10000");
                            // run_steps(3000, 3333, st);
                            //run_steps(10000, 1000, st, "10000x1000");
                        }
                        else {
                            std::cout << "R/W run for type " << step_names[st] << " with 100000 accesses" << std::endl;
                            std::cout << "DB size - Object Read - Property Read - Property Change" << std::endl;
                            //run_steps(100, 100000, st, "100x100000", true);
                            run_steps(10, 1000000, st, "10x1000000", true);
                        }
                    };
    // insertion tests
    run_type(DIRECT);
    run_type(INDEXED_BEST);
    run_type(INDEXED_WORST);
    run_type(PK);
    // access tests
    // no access test for DIRECT
    run_type(INDEXED_BEST, true);
    run_type(INDEXED_WORST, true);
    run_type(PK, true);
}


