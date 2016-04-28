#include <realm.hpp>

#include "benchmark.hpp"
#include "results.hpp"
#include "random.hpp"

using namespace realm;
using namespace realm::test_util;

template<bool nullable = false>
class WithOneColumn : public Benchmark {
protected:
    void before_all(SharedGroup& sg)
    {
        WriteTransaction tr(sg);
        TableRef t = tr.add_table("table");
        t->add_column(type_Timestamp, "timestamp", nullable);
        tr.commit();
    }

    void after_all(SharedGroup& sg)
    {
        // WriteTransaction doesn't have remove_table :-/
        Group& g = sg.begin_write();
        g.remove_table("table");
        sg.commit();
    }
};

template<class WithClass, size_t N>
class AddEmptyRows : public WithClass {

    void operator()(SharedGroup& sg)
    {
        WriteTransaction tr(sg);
        TableRef t = tr.get_table(0);
        t->add_empty_row(N);
        tr.commit();
    }
};

template<class WithClass, size_t N>
class WithEmptyRows : public WithClass {

    void before_all(SharedGroup& sg)
    {
        WithClass::before_all(sg);

        WriteTransaction tr(sg);
        TableRef t = tr.get_table(0);
        t->add_empty_row(N);
        tr.commit();
    }
};

class WithNullColumn_Add1000EmptyRows :
    public AddEmptyRows<WithOneColumn<true>, 1000> {

    const char *name() const {
        return "WithNullColumn_Add1000EmptyRows";
    }
};

template<class WithClass, size_t N>
class AddRandomRows : public WithClass {

    Timestamp ts[N];

    void before_all(SharedGroup& sg)
    {
        Random random;
        int_fast64_t sinceEpoch;

        for (size_t i = 0; i < N; i++) {
            sinceEpoch = random.draw_int<int_fast64_t>();
            ts[i] = Timestamp(sinceEpoch, 0);
        }

        WithClass::before_all(sg);
    }

    void operator()(SharedGroup& sg)
    {
        WriteTransaction tr(sg);
        TableRef t = tr.get_table(0);
        t->add_empty_row(N);

        for (size_t i = 0; i < N; i++) {
            t->set_timestamp(0, i, ts[i]);
        }

        tr.commit();
    }
};

class WithNullColumn_Add1000RandomRows :
    public AddRandomRows<WithOneColumn<true>, 1000> {

    const char *name() const {
        return "WithNullColumn_Add1000RandomRows";
    }
};

template<class Benchmark>
void run(Results& results) {
    Benchmark benchmark;
    benchmark.run(results);
}

int main() {
    Results results(10);
    run<WithNullColumn_Add1000EmptyRows>(results);
    run<WithNullColumn_Add1000RandomRows>(results);
}
