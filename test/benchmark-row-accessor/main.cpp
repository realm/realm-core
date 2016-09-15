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
#include <iostream>

#include <realm.hpp>
#include <memory>

#include "../util/timer.hpp"
#include "../util/random.hpp"
#include "../util/benchmark_results.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;


/// Row Accessor Benchmarks
///
/// To measure the performance of the row accessor only, the table tested on is
/// minimal, one empty row nothing else. Bigger tables might be necessary, but
/// beware of skewed results.

namespace {

enum DetachOrder { AttachOrder, RevAttOrder, RandomOrder };

/// Benchmark the (=) operator on row accessors.
///
/// The (=) operator causes a reattachment a row expression to the table.
/// `heap` signfies that this reattachment will happen many times over.
///
/// Here it is in pseduocode:
///
///     table = add_empty_row(table())
///     rows = replicate(table[0], n)
///     time {
///       repeat 10000 * 10000 times {
///         rows[random(n)] = table[0]
///       }
///     }
///
void heap(Timer& timer, BenchmarkResults& results, int n, const char* ident, const char* lead_text)
{
    Table table;
    table.add_empty_row();
    std::unique_ptr<Row[]> rows(new Row[n]);
    for (int i = 0; i < n; ++i)
        rows[i] = table[0];

    // Generate random numbers before timing because Random is slooow
    // (thread-safe):
    int m = 10000;
    std::unique_ptr<int[]> indexes(new int[m]);
    Random random;
    for (int i = 0; i < m; ++i)
        indexes[i] = random.draw_int_mod(n);
    // indexes is not guaranteed to contain all indexes from 0 to n..

    // Now get to business:
    timer.reset();
    for (int j = 0; j < 10000; ++j) {
        for (int i = 0; i < m; ++i)
            rows[indexes[i]] = table[0];
    }
    results.submit_single(ident, lead_text, timer);
}

/// Benchmark the (=) operator on row accessors, while detaching them in /
/// various orders. `balloon` signifies that the row accessors are first
/// attached (inflating a balloon) and then detached in some order
/// (deflating the balloon).
///
/// Here it is in pseduocode:
///
///     table = add_empty_row(table())
///     detach_indexes = sort(detach_order, range(balloon_size))
///     time {
///       rows = replicate(table[0], ballon_size)
///       for i in range(ballon_size) {
///         rows[detach_indexes[i]].detach()
///       }
///     }
///
void balloon(Timer& timer, BenchmarkResults& results, int balloon_size, DetachOrder detach_order, const char* ident,
             const char* lead_text)
{
    Table table;
    table.add_empty_row();
    std::unique_ptr<Row[]> rows(new Row[balloon_size]);
    std::unique_ptr<int[]> detach_indexes(new int[balloon_size]);
    for (int i = 0; i < balloon_size; ++i)
        detach_indexes[i] = i;
    Random random;
    switch (detach_order) {
        case AttachOrder:
            break;
        case RevAttOrder:
            std::reverse(detach_indexes.get(), detach_indexes.get() + balloon_size);
            break;
        case RandomOrder:
            random.shuffle(detach_indexes.get(), detach_indexes.get() + balloon_size);
            break;
    }
    int n = (100000000L + balloon_size - 1) / balloon_size;
    timer.reset();
    for (int j = 0; j < n; ++j) {
        for (int i = 0; i < balloon_size; ++i)
            rows[i] = table[0];
        for (int i = 0; i < balloon_size; ++i)
            rows[detach_indexes[i]].detach();
    }
    results.submit_single(ident, lead_text, timer);
}

} // anonymous namepsace


int main()
{
    int max_lead_text_size = 22;
    BenchmarkResults results(max_lead_text_size);

    Timer timer_total(Timer::type_UserTime);
    Timer timer(Timer::type_UserTime);

    heap(timer, results, 1, "heap_1", "Heap 1");
    heap(timer, results, 10, "heap_10", "Heap 10");
    heap(timer, results, 100, "heap_100", "Heap 100");
    heap(timer, results, 1000, "heap_1000", "Heap 1000");

    balloon(timer, results, 10, AttachOrder, "balloon_10", "Balloon 10");
    balloon(timer, results, 10, RevAttOrder, "balloon_10_reverse", "Balloon 10 (reverse)");
    balloon(timer, results, 10, RandomOrder, "balloon_10_random", "Balloon 10 (random)");

    balloon(timer, results, 100, AttachOrder, "balloon_100", "Balloon 100");
    balloon(timer, results, 100, RevAttOrder, "balloon_100_reverse", "Balloon 100 (reverse)");
    balloon(timer, results, 100, RandomOrder, "balloon_100_random", "Balloon 100 (random)");

    balloon(timer, results, 1000, AttachOrder, "balloon_1000", "Balloon 1000");
    balloon(timer, results, 1000, RevAttOrder, "balloon_1000_reverse", "Balloon 1000 (reverse)");
    balloon(timer, results, 1000, RandomOrder, "balloon_1000_random", "Balloon 1000 (random)");

    results.submit_single("total_time", "Total time", timer_total);
}
