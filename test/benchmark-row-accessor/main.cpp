#include <cstdlib>
#include <stdexcept>
#include <algorithm>
#include <iostream>

#include <realm.hpp>
#include <memory>

#include "../util/timer.hpp"
#include "../util/random.hpp"
#include "../util/benchmark_results.hpp"

using namespace std;
using namespace realm;
using namespace realm::util;
using namespace realm::test_util;


namespace {

void heap(Timer& timer, BenchmarkResults& results, int n, const char* ident, const char* lead_text)
{
    Table table;
    table.add_empty_row();
    std::unique_ptr<Row[]> rows(new Row[n]);
    for (int i = 0; i < n; ++i)
        rows[i] = table[0];
    int m = 10000;
    std::unique_ptr<int[]> indexes(new int[m]);
    Random random;
    for (int i = 0; i < m; ++i)
        indexes[i] = random.draw_int_mod(n);
    timer.reset();
    for (int j = 0; j < 10000; ++j) {
        for (int i = 0; i < m; ++i)
            rows[indexes[i]] = table[0];
    }
    results.submit_single(ident, lead_text, timer);
}

void balloon(Timer& timer, BenchmarkResults& results, int balloon_size, int detach_order, const char* ident, const char* lead_text)
{
    Table table;
    table.add_empty_row();
    std::unique_ptr<Row[]> rows(new Row[balloon_size]);
    std::unique_ptr<int[]> detach_indexes(new int[balloon_size]);
    for (int i = 0; i < balloon_size; ++i)
        detach_indexes[i] = i;
    Random random;
    switch (detach_order) {
        case 0: // Same as attach order
            break;
        case 1: // Opposite of attach order
            reverse(detach_indexes.get(), detach_indexes.get() + balloon_size);
            break;
        case 2: // Randomized
            random.shuffle(detach_indexes.get(), detach_indexes.get() + balloon_size);
            break;
        default:
            throw runtime_error("Bad order specification");
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

    heap(timer, results,    1, "heap_1",    "Heap 1");
    heap(timer, results,   10, "heap_10",   "Heap 10");
    heap(timer, results,  100, "heap_100",  "Heap 100");
    heap(timer, results, 1000, "heap_1000", "Heap 1000");

    balloon(timer, results, 10, 0, "balloon_10",         "Balloon 10");
    balloon(timer, results, 10, 1, "balloon_10_reverse", "Balloon 10 (reverse)");
    balloon(timer, results, 10, 2, "balloon_10_random",  "Balloon 10 (random)");

    balloon(timer, results, 100, 0, "balloon_100",         "Balloon 100");
    balloon(timer, results, 100, 1, "balloon_100_reverse", "Balloon 100 (reverse)");
    balloon(timer, results, 100, 2, "balloon_100_random",  "Balloon 100 (random)");

    balloon(timer, results, 1000, 0, "balloon_1000",         "Balloon 1000");
    balloon(timer, results, 1000, 1, "balloon_1000_reverse", "Balloon 1000 (reverse)");
    balloon(timer, results, 1000, 2, "balloon_1000_random",  "Balloon 1000 (random)");

    results.submit_single("total_time", "Total time", timer_total);
}
