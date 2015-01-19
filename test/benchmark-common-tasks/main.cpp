#include <iostream>
#include <sstream>

#include <tightdb.hpp>
#include <tightdb/util/file.hpp>

#include "../util/timer.hpp"
#include "../util/random.hpp"
#include "../util/benchmark_results.hpp"

using namespace tightdb;
using namespace tightdb::util;
using namespace tightdb::test_util;

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

static const char realm_path[] = "/tmp/benchmark-common-tasks.tightdb";
static const size_t repetitions[] = { 10, 100, 1000 };

struct Benchmark
{
    virtual const char* name() const = 0;
    virtual void setup(SharedGroup&) {}
    virtual void teardown(SharedGroup&) {}
    virtual void operator()(SharedGroup&) = 0;
};

struct AddTable : Benchmark
{
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

    void teardown(SharedGroup& group)
    {
        Group& g = group.begin_write();
        g.remove_table(name());
        group.commit();
    }
};

struct BenchmarkWithStringsTable : Benchmark
{
    void setup(SharedGroup& group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table("StringOnly");
        t->add_column(type_String, "chars");
        tr.commit();
    }

    void teardown(SharedGroup& group)
    {
        Group& g = group.begin_write();
        g.remove_table("StringOnly");
        group.commit();
    }
};

struct BenchmarkWithStrings : BenchmarkWithStringsTable
{
    void setup(SharedGroup& group)
    {
        BenchmarkWithStringsTable::setup(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("StringOnly");
        t->add_empty_row(1000);
        for (size_t i = 0; i < 1000; ++i) {
            std::stringstream ss;
            ss << rand();
            t->set_string(0, i, ss.str());
        }
        tr.commit();
    }
};

struct BenchmarkQuery : BenchmarkWithStrings
{
    const char* name() const { return "Query"; }

    void operator()(SharedGroup& group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        ConstTableView view = table->find_all_string(0, "200");
    }
};

struct BenchmarkSize : BenchmarkWithStrings
{
    const char* name() const { return "Size"; }

    void operator()(SharedGroup& group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        volatile size_t dummy = table->size();
        static_cast<void>(dummy);
    }
};

struct BenchmarkSort : BenchmarkWithStrings
{
    const char* name() const { return "Sort"; }

    void operator()(SharedGroup& group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        ConstTableView view = table->get_sorted_view(0);
    }
};

struct BenchmarkInsert : BenchmarkWithStringsTable
{
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

struct BenchmarkGetString : BenchmarkWithStrings
{
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

struct BenchmarkSetString : BenchmarkWithStrings
{
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




static const char* durability_level_to_cstr(SharedGroup::DurabilityLevel level)
{
    switch (level) {
        case SharedGroup::durability_Full: return "Full";
        case SharedGroup::durability_MemOnly: return "MemOnly";
#ifndef _WIN32
        case SharedGroup::durability_Async: return "Async";
#endif
    }
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
    static const size_t num_repetition_groups = sizeof(repetitions) / sizeof(*repetitions);

    Timer timer(Timer::type_UserTime);
    for (size_t i = 0; i < num_durabilities; ++i) {
        SharedGroup::DurabilityLevel level = static_cast<SharedGroup::DurabilityLevel>(i);
        for (size_t j = 0; j < num_repetition_groups; ++j) {
            size_t rep = repetitions[j];
            B benchmark;

            // Generate the benchmark result texts:
            std::stringstream lead_text_ss;
            std::stringstream ident_ss;
            lead_text_ss << benchmark.name() << " (" << durability_level_to_cstr(level) << ", x" << rep << ")";
            ident_ss << benchmark.name() << "_" << durability_level_to_cstr(level) << "_" << rep;
            std::string lead_text = lead_text_ss.str();
            std::string ident = ident_ss.str();

            // Open a SharedGroup:
            File::try_remove(realm_path);
            UniquePtr<SharedGroup> group;
            group.reset(new SharedGroup(realm_path, false, level));

            // Run the benchmarks with cumulative results.
            timer.reset();
            timer.pause();
            for (size_t r = 0; r < rep; ++r) {
                benchmark.setup(*group);

                timer.unpause();
                benchmark(*group);
                timer.pause();
                
                benchmark.teardown(*group);
            }
            timer.unpause();
            results.submit(timer, ident.c_str(), lead_text.c_str());
        }
    }
}


int main(int, const char**)
{
    BenchmarkResults results(40);

    run_benchmark<AddTable>(results);
    run_benchmark<BenchmarkQuery>(results);
    run_benchmark<BenchmarkSize>(results);
    run_benchmark<BenchmarkSort>(results);
    run_benchmark<BenchmarkInsert>(results);
    run_benchmark<BenchmarkGetString>(results);
    run_benchmark<BenchmarkSetString>(results);

    return 0;
}
