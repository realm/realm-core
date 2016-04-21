#include <realm.hpp>

#include <random>

#include "benchmark.hpp"
#include "results.hpp"

using namespace realm;
using namespace realm::test_util;

struct OneColumn : Benchmark {
    const char* name() const { return "OneColumn"; }

    void operator()(SharedGroup& group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table(name());
        t->add_column(type_DateTime, "datetime");
        tr.commit();
    }

    void after_each(SharedGroup& group)
    {
        Group& g = group.begin_write();
        g.remove_table(name());
        group.commit();
    }
};

int main() {
    Table t;
}
