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
static const size_t min_repetitions = 10;
static const size_t max_repetitions = 100;
static const double min_duration_s = 0.05;

struct Benchmark
{
    virtual const char* name() const = 0;
    virtual void setup(SharedGroup&) {}
    virtual void teardown(SharedGroup&) {}
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

    void teardown(SharedGroup& group)
    {
        Group& g = group.begin_write();
        g.remove_table(name());
        group.commit();
    }
};

struct BenchmarkWithStringsTable : Benchmark {
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

struct BenchmarkWithStrings : BenchmarkWithStringsTable {
    void setup(SharedGroup& group)
    {
        BenchmarkWithStringsTable::setup(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("StringOnly");
        t->add_empty_row(999);
        for (size_t i = 0; i < 999; ++i) {
            std::stringstream ss;
            ss << rand();
            t->set_string(0, i, ss.str());
        }
        tr.commit();
    }
};

struct BenchmarkWithLongStrings : BenchmarkWithStrings {
    void setup(SharedGroup& group)
    {
        BenchmarkWithStrings::setup(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("StringOnly");
        t->insert_empty_row(0);
        t->set_string(0, 0, "A really long string, longer than 63 bytes at least, I guess......");
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

    void setup(SharedGroup& group)
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

    void teardown(SharedGroup& group)
    {
        Group& g = group.begin_write();
        g.remove_table(name());
        group.commit();
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

void run_benchmark_once(Benchmark& benchmark, SharedGroup& sg, Timer& timer)
{
    timer.pause();
    benchmark.setup(sg);
    timer.unpause();

    benchmark(sg);

    timer.pause();
    benchmark.teardown(sg);
    timer.unpause();
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

    Timer timer(Timer::type_UserTime);
    for (size_t i = 0; i < num_durabilities; ++i) {
        // Open a SharedGroup:
        File::try_remove(realm_path);
        UniquePtr<SharedGroup> group;
        SharedGroup::DurabilityLevel level = static_cast<SharedGroup::DurabilityLevel>(i);
        group.reset(new SharedGroup(realm_path, false, level));

        B benchmark;

        // Generate the benchmark result texts:
        std::stringstream lead_text_ss;
        std::stringstream ident_ss;
        lead_text_ss << benchmark.name() << " (" << durability_level_to_cstr(level) << ")";
        ident_ss << benchmark.name() << "_" << durability_level_to_cstr(level);
        std::string ident = ident_ss.str();

        // Warm-up and initial measuring:
        Timer t_unused(Timer::type_UserTime);
        run_benchmark_once(benchmark, *group, t_unused);

        size_t rep;
        double total = 0;
        for (rep = 0; rep < max_repetitions && (rep < min_repetitions || total < min_duration_s); ++rep) {
            Timer t;
            run_benchmark_once(benchmark, *group, t);
            double s = t.get_elapsed_time();
            total += s;
            results.submit(ident.c_str(), s);
        }

        results.finish(ident, lead_text_ss.str());
    }
}


int main(int, const char**)
{
    BenchmarkResults results(40);

    run_benchmark<AddTable>(results);
    run_benchmark<BenchmarkQuery>(results);
    run_benchmark<BenchmarkQueryNot>(results);
    run_benchmark<BenchmarkSize>(results);
    run_benchmark<BenchmarkSort>(results);
    run_benchmark<BenchmarkInsert>(results);
    run_benchmark<BenchmarkGetString>(results);
    run_benchmark<BenchmarkSetString>(results);
    run_benchmark<BenchmarkGetLongString>(results);
    run_benchmark<BenchmarkSetLongString>(results);

    return 0;
}
