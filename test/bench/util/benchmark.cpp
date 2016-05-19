#include "benchmark.hpp"

#include "results.hpp"      // Results
#include "timer.hpp"        // Timer

#include <iostream>

using namespace realm;
using namespace realm::test_util;

namespace {

inline void
run_once(Benchmark *bm, Timer& timer)
{
    timer.pause();
    bm->before_each();
    timer.unpause();

    bm->bench();

    timer.pause();
    bm->after_each();
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

double Benchmark::warmup()
{
    double warmup_time = 0.0;
    size_t warmup_reps = 0;
    Timer timer(Timer::type_UserTime);
    timer.pause();
    while(warmup_time < this->min_warmup_time() &&
          warmup_reps < this->max_warmup_reps()) {
        timer.unpause();
        run_once(this, timer);
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

    this->before_all();

    warmup_secs_per_rep = this->warmup();
    reps = size_t(this->min_time() / warmup_secs_per_rep);
    reps = std::max(reps, this->min_reps());
    reps = std::min(reps, this->max_reps());

    std::cout << "Repeating: " << reps << std::endl;

    for (size_t i = 0; i < reps; i++) {
        Timer t;
        run_once(this, t);
        results.submit(ident.c_str(), t.get_elapsed_time());
        if (!this->asExpected) {
            std::cout << "Unexpected!" << std::endl;
        }
    }

    this->after_all();

    results.finish(ident, lead_text);
}

WithSharedGroup::WithSharedGroup()
{
    std::string realm_path = "results.realm";
    this->sg.reset(new SharedGroup(realm_path, false,
        SharedGroup::durability_MemOnly, nullptr));
}
