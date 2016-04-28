#ifndef REALM_BENCHMARK_HPP
#define REALM_BENCHMARK_HPP

#include <realm.hpp>

#include "timer.hpp"    // Timer
#include "results.hpp"  // Results

namespace realm {
namespace test_util {

class Benchmark
{
    virtual const char* name() const = 0;
    virtual void before_all(SharedGroup&) {}
    virtual void after_all(SharedGroup&) {}
    virtual void before_each(SharedGroup&) {}
    virtual void after_each(SharedGroup&) {}
    virtual void operator()(SharedGroup&) = 0;

    std::string lead_text();
    std::string ident();

    void run_once(SharedGroup&, Timer&);

public:
    void run(Results& results);
};

} // namespace test_util
} // namespace realm

#endif // REALM_BENCHMARK_HPP
