#include "benchmark.hpp"

#include "results.hpp"      // Results
#include "timer.hpp"        // Timer

#include <iostream>

using namespace realm;
using namespace realm::test_util;

namespace {

inline void
run_once(Benchmark *bench, SharedGroup& sg, Timer& timer)
{
    timer.pause();
    bench->before_each(sg);
    timer.unpause();

    bench->operator()(sg);

    timer.pause();
    bench->after_each(sg);
    timer.unpause();
}

} // Anonumous namespace

std::string Benchmark::lead_text()
{
    std::stringstream stream;
    stream << this->name() << " (MemOnly, EncryptionOff)";
    return stream.str();
}

std::string Benchmark::ident()
{
    std::stringstream stream;
    stream << this->name() << "_MemOnly_EncryptionOff";
    return stream.str();
}

double Benchmark::warmup(SharedGroup& sg)
{
    double warmup_time = 0.0;
    size_t warmup_reps = 0;
    Timer timer(Timer::type_UserTime);
    timer.pause();
    while(warmup_time < this->min_warmup_time() &&
          warmup_reps < this->max_warmup_reps()) {
        timer.unpause();
        run_once(this, sg, timer);
        timer.pause();
        warmup_time = timer.get_elapsed_time();
        warmup_reps++;
    }

    return warmup_reps == 0 ? 0.0 : (warmup_time / warmup_reps);
}

void Benchmark::run(Results& results)
{
    double warmup_secs_per_rep;
    size_t reps;

    std::string lead_text = this->lead_text();
    std::string ident = this->ident();

    std::string realm_path = "results.realm";
    std::unique_ptr<SharedGroup> sg;
    sg.reset(new SharedGroup(realm_path, false,
                             SharedGroup::durability_MemOnly, nullptr));

    this->before_all(*sg);

    warmup_secs_per_rep = this->warmup(*sg);
    reps = size_t(this->min_time() / warmup_secs_per_rep);
    reps = std::max(reps, this->min_reps());
    reps = std::min(reps, this->max_reps());

//    reps = 10000;
    std::cout << "Repeating: " << reps << std::endl;

    for (size_t i = 0; i < reps; i++) {
        Timer t;
        run_once(this, *sg, t);
        results.submit(ident.c_str(), t.get_elapsed_time());
    }

    this->after_all(*sg);

    results.finish(ident, lead_text);
}
