#ifndef REALM_BENCHMARK_HPP
#define REALM_BENCHMARK_HPP

#include "results.hpp"  // Results

#include <string> // string

namespace realm {
namespace test_util {

class Benchmark {
    std::string lead_text();
    std::string ident();

    double warmup();

public:
    bool asExpected = false;

    virtual const char *name() const = 0;

    virtual void before_all() {}
    virtual void before_each() {}
    virtual void measure() = 0;
    virtual void after_each() {}
    virtual void after_all() {}

    virtual double min_warmup_time()
    {
        return 0.05;
    }

    virtual size_t max_warmup_reps()
    {
        return 100;
    }

    virtual double min_time()
    {
        return 0.1;
    }

    virtual size_t min_reps()
    {
        return 1000;
    }

    virtual size_t max_reps()
    {
        return 10000;
    }

    void run(Results& results);
};

template<class Benchmark>
void benchmark(Results& r) {
    Benchmark bm;
    bm.run(r);
}

} // namespace test_util
} // namespace realm

#endif // REALM_BENCHMARK_HPP
