#include <iostream>
#include <sstream>

#include <realm.hpp>
#include <realm/util/file.hpp>

#include "../util/timer.hpp"
#include "../util/random.hpp"
#include "../util/benchmark_results.hpp"
#include "../util/test_path.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;

namespace {

/**
  This bechmark suite represents a number of common use cases,
  from the perspective of the bindings. It does *not* benchmark
  the type-safe C++ API, but only the things that language bindings
  are likely to use internally.

  This has the following implications:
  - All access is done with a SharedGroup in transactions.
  - The SharedGroup has full durability (is backed by a file).
    (but all benchmarks are also run with MemOnly durability for comparison)
  - Cases have been derived from:
    https://github.com/realm/realm-java/blob/bp-performance-test/realm/src/androidTest/java/io/realm/RealmPerformanceTest.java
*/

const size_t min_repetitions = 10;
const size_t max_repetitions = 1000;
const double min_duration_s = 0.05;
const double min_warmup_time_s = 0.01;

struct Benchmark
{
    virtual const char* name() const = 0;
    virtual void before_all(SharedGroup&) {}
    virtual void after_all(SharedGroup&) {}
    virtual void before_each(SharedGroup&) {}
    virtual void after_each(SharedGroup&) {}
    virtual void operator()(SharedGroup&) = 0;
};

struct AddTable : Benchmark {
    const char* name() const { return "AddTable"; }

    void operator()(SharedGroup& group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table(name());
        t->add_column(type_String, "first");
        t->add_column(type_Int, "second");
        t->add_column(type_DateTime, "third");
        tr.commit();
    }

    void after_each(SharedGroup& group)
    {
        Group& g = group.begin_write();
        g.remove_table(name());
        group.commit();
    }
};

struct BenchmarkWithStringsTable : Benchmark {
    void before_all(SharedGroup& group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table("StringOnly");
        t->add_column(type_String, "chars");
        tr.commit();
    }

    void after_all(SharedGroup& group)
    {
        Group& g = group.begin_write();
        g.remove_table("StringOnly");
        group.commit();
    }
};

struct BenchmarkWithStrings : BenchmarkWithStringsTable {
    void before_all(SharedGroup& group)
    {
        BenchmarkWithStringsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("StringOnly");
        t->add_empty_row(REALM_MAX_BPNODE_SIZE * 4);
        for (size_t i = 0; i < REALM_MAX_BPNODE_SIZE * 4; ++i) {
            std::stringstream ss;
            ss << rand();
            t->set_string(0, i, ss.str());
        }
        tr.commit();
    }
};

struct BenchmarkWithLongStrings : BenchmarkWithStrings {
    void before_all(SharedGroup& group)
    {
        BenchmarkWithStrings::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("StringOnly");
        t->insert_empty_row(0);
        // This should be enough to upgrade the entire array:
        t->set_string(0, 0, "A really long string, longer than 63 bytes at least, I guess......");
        t->set_string(0, REALM_MAX_BPNODE_SIZE, "A really long string, longer than 63 bytes at least, I guess......");
        t->set_string(0, REALM_MAX_BPNODE_SIZE * 2, "A really long string, longer than 63 bytes at least, I guess......");
        t->set_string(0, REALM_MAX_BPNODE_SIZE * 3, "A really long string, longer than 63 bytes at least, I guess......");
        tr.commit();
    }
};

struct BenchmarkWithIntsTable : Benchmark {
    void before_all(SharedGroup& group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table("IntOnly");
        t->add_column(type_Int, "ints");
        tr.commit();
    }

    void after_all(SharedGroup& group)
    {
        Group& g = group.begin_write();
        g.remove_table("IntOnly");
        group.commit();
    }
};

struct BenchmarkWithInts : BenchmarkWithIntsTable {
    void before_all(SharedGroup& group)
    {
        BenchmarkWithIntsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("IntOnly");
        t->add_empty_row(REALM_MAX_BPNODE_SIZE * 4);
        Random r;
        for (size_t i = 0; i < REALM_MAX_BPNODE_SIZE * 4; ++i) {
            t->set_int(0, i, r.draw_int<int64_t>());
        }
        tr.commit();
    }
};

struct BenchmarkQuery : BenchmarkWithStrings {
    const char* name() const { return "Query"; }

    void operator()(SharedGroup& group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        ConstTableView view = table->find_all_string(0, "200");
    }
};

struct BenchmarkSize : BenchmarkWithStrings {
    const char* name() const { return "Size"; }

    void operator()(SharedGroup& group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        volatile size_t dummy = table->size();
        static_cast<void>(dummy);
    }
};

struct BenchmarkSort : BenchmarkWithStrings {
    const char* name() const { return "Sort"; }

    void operator()(SharedGroup& group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        ConstTableView view = table->get_sorted_view(0);
    }
};

struct BenchmarkSortInt : BenchmarkWithInts {
    const char* name() const { return "SortInt"; }

    void operator()(SharedGroup& group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("IntOnly");
        ConstTableView view = table->get_sorted_view(0);
    }
};

struct BenchmarkInsert : BenchmarkWithStringsTable {
    const char* name() const { return "Insert"; }

    void operator()(SharedGroup& group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.get_table("StringOnly");
        for (size_t i = 0; i < 10000; ++i) {
            t->add_empty_row();
            t->set_string(0, i, "a");
        }
        tr.commit();
    }
};

struct BenchmarkGetString : BenchmarkWithStrings {
    const char* name() const { return "GetString"; }

    void operator()(SharedGroup& group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        size_t len = table->size();
        volatile int dummy = 0;
        for (size_t i = 0; i < len; ++i) {
            StringData str = table->get_string(0, i);
            dummy += str[0]; // to avoid over-optimization
        }
    }
};

struct BenchmarkSetString : BenchmarkWithStrings {
    const char* name() const { return "SetString"; }

    void operator()(SharedGroup& group)
    {
        WriteTransaction tr(group);
        TableRef table = tr.get_table("StringOnly");
        size_t len = table->size();
        for (size_t i = 0; i < len; ++i) {
            table->set_string(0, i, "c");
        }
        tr.commit();
    }
};

struct BenchmarkCreateIndex : BenchmarkWithStrings {
    const char* name() const { return "CreateIndex"; }
    void operator()(SharedGroup& group)
    {
        WriteTransaction tr(group);
        TableRef table = tr.get_table("StringOnly");
        table->add_search_index(0);
        tr.commit();
    }
};

struct BenchmarkGetLongString : BenchmarkWithLongStrings {
    const char* name() const { return "GetLongString"; }

    void operator()(SharedGroup& group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        size_t len = table->size();
        volatile int dummy = 0;
        for (size_t i = 0; i < len; ++i) {
            StringData str = table->get_string(0, i);
            dummy += str[0]; // to avoid over-optimization
        }
    }
};

struct BenchmarkSetLongString : BenchmarkWithLongStrings {
    const char* name() const { return "SetLongString"; }

    void operator()(SharedGroup& group)
    {
        WriteTransaction tr(group);
        TableRef table = tr.get_table("StringOnly");
        size_t len = table->size();
        for (size_t i = 0; i < len; ++i) {
            table->set_string(0, i, "c");
        }
        tr.commit();
    }
};

struct BenchmarkQueryNot : Benchmark {
    const char* name() const { return "QueryNot"; }

    void before_all(SharedGroup& group)
    {
        WriteTransaction tr(group);
        TableRef table = tr.add_table(name());
        table->add_column(type_Int, "first");
        table->add_empty_row(1000);
        for (size_t i = 0; i < 1000; ++i) {
            table->set_int(0, i, 1);
        }
        tr.commit();
    }

    void operator()(SharedGroup& group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table(name());
        Query q = table->where();
        q.not_equal(0, 2); // never found, = worst case
        TableView results = q.find_all();
        results.size();
    }

    void after_all(SharedGroup& group)
    {
        Group& g = group.begin_write();
        g.remove_table(name());
        group.commit();
    }
};




const char* durability_level_to_cstr(SharedGroup::DurabilityLevel level)
{
    switch (level) {
        case SharedGroup::durability_Full: return "Full";
        case SharedGroup::durability_MemOnly: return "MemOnly";
#ifndef _WIN32
        case SharedGroup::durability_Async: return "Async";
#endif
    }
}

void run_benchmark_once(Benchmark& benchmark, SharedGroup& sg, Timer& timer)
{
    benchmark.before_each(sg);

    benchmark(sg);

    benchmark.after_each(sg);
}


/// This little piece of likely over-engineering runs the benchmark a number of times,
/// with each durability setting, and reports the results for each run.
template <typename B>
void run_benchmark(BenchmarkResults& results)
{
#ifdef _WIN32
    const size_t num_durabilities = 2;
#else
    const size_t num_durabilities = 2; // FIXME Figure out how to run the async commit daemon.
#endif

    static long test_counter = 0;

    Timer timer(Timer::type_UserTime);
    for (size_t i = 0; i < num_durabilities; ++i) {
        SharedGroup::DurabilityLevel level = static_cast<SharedGroup::DurabilityLevel>(i);
        B benchmark;

        // Generate the benchmark result texts:
        std::stringstream lead_text_ss;
        std::stringstream ident_ss;
        lead_text_ss << benchmark.name() << " (" << durability_level_to_cstr(level) << ")";
        ident_ss << benchmark.name() << "_" << durability_level_to_cstr(level);
        std::string ident = ident_ss.str();

        realm::test_util::unit_test::TestDetails test_details;
        test_details.test_index = test_counter++;
        test_details.suite_name = "BenchmarkCommonTasks";
        test_details.test_name = ident.c_str();
        test_details.file_name = __FILE__;
        test_details.line_number = __LINE__;

        // Open a SharedGroup:
        SHARED_GROUP_TEST_PATH(realm_path);
        std::unique_ptr<SharedGroup> group;
        group.reset(new SharedGroup(realm_path, false, level));

        benchmark.before_all(*group);

        // Warm-up and initial measuring:
        Timer t_baseline(Timer::type_UserTime);
        size_t num_warmup_reps = 1;
        double time_to_execute_warmup_reps = 0;
        while (time_to_execute_warmup_reps < min_warmup_time_s && num_warmup_reps < max_repetitions) {
            num_warmup_reps *= 10;
            Timer t_baseline(Timer::type_UserTime);
            for (size_t i = 0; i < num_warmup_reps; ++i) {
                run_benchmark_once(benchmark, *group, t_baseline);
            }
            time_to_execute_warmup_reps = t_baseline.get_elapsed_time();
        }

        size_t required_reps = size_t(min_duration_s / (time_to_execute_warmup_reps / num_warmup_reps));
        if (required_reps < min_repetitions) {
            required_reps = min_repetitions;
        }
        if (required_reps > max_repetitions) {
            required_reps = max_repetitions;
        }

        for (size_t rep = 0; rep < required_reps; ++rep) {
            Timer t;
            run_benchmark_once(benchmark, *group, t);
            double s = t.get_elapsed_time();
            results.submit(ident.c_str(), s);
        }

        benchmark.after_all(*group);

        results.finish(ident, lead_text_ss.str());
    }
}

} // anonymous namespace

extern "C" int benchmark_common_tasks_main();

int benchmark_common_tasks_main()
{
    std::string results_file_stem = test_util::get_test_path_prefix() + "results";
    BenchmarkResults results(40, results_file_stem.c_str());

    run_benchmark<AddTable>(results);
    run_benchmark<BenchmarkQuery>(results);
    run_benchmark<BenchmarkQueryNot>(results);
    run_benchmark<BenchmarkSize>(results);
    run_benchmark<BenchmarkSort>(results);
    run_benchmark<BenchmarkSortInt>(results);
    run_benchmark<BenchmarkInsert>(results);
    run_benchmark<BenchmarkGetString>(results);
    run_benchmark<BenchmarkSetString>(results);
    run_benchmark<BenchmarkCreateIndex>(results);
    run_benchmark<BenchmarkGetLongString>(results);
    run_benchmark<BenchmarkSetLongString>(results);

    return 0;
}

#if !defined(REALM_IOS)
int main(int, const char**)
{
    return benchmark_common_tasks_main();
}
#endif
