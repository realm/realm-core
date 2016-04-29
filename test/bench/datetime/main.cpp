#include <realm.hpp>

#include "benchmark.hpp"
#include "results.hpp"
#include "random.hpp"

using namespace realm;
using namespace realm::test_util;

class Nullable_Add1000EmptyRows :
    public AddEmptyRows<WithOneColumn<type_DateTime, true>, 1000> {

    const char *name() const {
        return "Nullable_Add1000EmptyRows";
    }
};

class NonNullable_Add1000EmptyRows :
    public AddEmptyRows<WithOneColumn<type_DateTime, false>, 1000> {

    const char *name() const {
        return "NonNullable_Add1000EmptyRows";
    }
};

template<class WithClass, size_t N,
    int_fast64_t min = std::numeric_limits<int_fast64_t>::min(),
    int_fast64_t max = std::numeric_limits<int_fast64_t>::max(),
    unsigned long seed = std::mt19937::default_seed>
class AddRandomRows : public WithClass {

    DateTime dts[N];

    void before_all(SharedGroup& sg)
    {
        Random random(seed);
        int_fast64_t sinceEpoch;

        for (size_t i = 0; i < N; i++) {
            sinceEpoch = random.draw_int<int_fast64_t>(min, max);
            dts[i] = DateTime(sinceEpoch);
        }

        WithClass::before_all(sg);
    }

    void operator()(SharedGroup& sg)
    {
        WriteTransaction tr(sg);
        TableRef t = tr.get_table(0);
        t->add_empty_row(N);

        for (size_t i = 0; i < N; i++) {
            t->set_datetime(0, i, dts[i]);
        }

        tr.commit();
    }
};

template<class WithClass, size_t N,
    int_fast64_t min = std::numeric_limits<int_fast64_t>::min(),
    int_fast64_t max = std::numeric_limits<int_fast64_t>::max(),
    unsigned long seed = std::mt19937::default_seed>
class WithRandomRows : public WithClass {
    void before_all(SharedGroup& sg)
    {
        WithClass::before_all(sg);

        Random random(seed);
        int_fast64_t sinceEpoch;

        WriteTransaction tr(sg);
        TableRef t = tr.get_table(0);

        for (size_t i = 0; i < N; i++) {
            t->add_empty_row();
            sinceEpoch = random.draw_int<int_fast64_t>(min, max);
            t->set_datetime(0, i, sinceEpoch);
        }

        tr.commit();
    }
};

template<class WithClass>
class QueryEqualsZero : public WithClass {
    void operator()(SharedGroup& sg)
    {
        ReadTransaction tr(sg);
        ConstTableRef t = tr.get_table(0);
        (t->where().get_table()->column<DateTime>(0) ==
            DateTime(0)).find_all();
    }
};

/*
template<class WithClass>
class QueryEqualsNonZero : public WithClass {
    void operator()(SharedGroup& sg)
    {
        ReadTransaction tr(sg);
        ConstTableRef t = tr.get_table(0);
        (t->where().get_table()->column<DateTime>(0) ==
            DateTime(1293840000)).find_all();
        // 2011-01-01T00:00:00Z
    }
};
*/

class Nullable_Add1000RandomRows :
    public AddRandomRows<WithOneColumn<type_DateTime, true>, 1000> {

    const char *name() const {
        return "Nullable_Add1000RandomRows";
    }
};

class NonNullable_Add1000RandomRows :
    public AddRandomRows<WithOneColumn<type_DateTime, false>, 1000> {

    const char *name() const {
        return "NonNullable_Add1000RandomRows";
    }
};

class EqualsZero :
    public
        QueryEqualsZero<
            WithRandomRows<
                WithOneColumn<
                    type_DateTime, true>,
                10000,
                946684800,  // 2000-01-01T00:00:00Z
                1893455999, // 2029-12-31T23:59:59Z
                1337        // A deterministic seed
                >
            > {

    const char *name() const {
        return "EqualsZero";
    }
};

int main() {
    Results results(10);
    bench<Nullable_Add1000EmptyRows>(results);
    bench<NonNullable_Add1000EmptyRows>(results);
    bench<Nullable_Add1000RandomRows>(results);
    bench<NonNullable_Add1000RandomRows>(results);
    bench<EqualsZero>(results);
}
