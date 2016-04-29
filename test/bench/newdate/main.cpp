#include <realm.hpp>

#include "benchmark.hpp"
#include "results.hpp"
#include "random.hpp"

using namespace realm;
using namespace realm::test_util;

#define DEF_N 100000

template<class WithClass, size_t N,
    int_fast64_t min = std::numeric_limits<int_fast64_t>::min(),
    int_fast64_t max = std::numeric_limits<int_fast64_t>::max(),
    unsigned long seed = std::mt19937::default_seed>
class WithRandomTRows : public WithClass {
    void before_all(SharedGroup& sg)
    {
        WithClass::before_all(sg);

        Random random(seed);
        int_fast64_t sinceEpoch;

        WriteTransaction tr(sg);
        TableRef t = tr.get_table(0);

        size_t i;
        for (i = 0; i < N; i++) {
            t->add_empty_row();
            sinceEpoch = random.draw_int<int_fast64_t>(min, max);
            t->set_timestamp(0, i, Timestamp(sinceEpoch, 0));
        }

        t->add_empty_row();
        t->set_timestamp(0, i, Timestamp(0, 0));

        tr.commit();
    }
};


template<class WithClass, size_t N,
    int_fast64_t min = std::numeric_limits<int_fast64_t>::min(),
    int_fast64_t max = std::numeric_limits<int_fast64_t>::max(),
    unsigned long seed = std::mt19937::default_seed>
class WithRandomDTRows : public WithClass {
    void before_all(SharedGroup& sg)
    {
        WithClass::before_all(sg);

        Random random(seed);
        int_fast64_t sinceEpoch;

        WriteTransaction tr(sg);
        TableRef t = tr.get_table(0);

        size_t i;
        for (i = 0; i < N; i++) {
            t->add_empty_row();
            sinceEpoch = random.draw_int<int_fast64_t>(min, max);
            t->set_olddatetime(0, i, OldDateTime(sinceEpoch));
        }

        t->add_empty_row();
        t->set_olddatetime(0, i, OldDateTime(0));

        tr.commit();
    }
};

template<class WithClass>
class QueryEqualsZeroT : public WithClass {
    void operator()(SharedGroup& sg)
    {
        ReadTransaction tr(sg);
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<Timestamp>(0) ==
            Timestamp(0, 0)).count();
        this->expected = count == 1;
    }
};

template<class WithClass>
class QueryEqualsZeroDT : public WithClass {
    void operator()(SharedGroup& sg)
    {
        ReadTransaction tr(sg);
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<OldDateTime>(0) ==
            OldDateTime(0)).count();
        this->expected = count == 1;
    }
};

template<class WithClass>
class QueryGreaterThanZeroT : public WithClass {
    void operator()(SharedGroup& sg)
    {
        ReadTransaction tr(sg);
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<Timestamp>(0) >
            Timestamp(0, 0)).count();
        this->expected = count == DEF_N;
    }
};

template<class WithClass>
class QueryGreaterThanZeroDT : public WithClass {
    void operator()(SharedGroup& sg)
    {
        ReadTransaction tr(sg);
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<OldDateTime>(0) >
            OldDateTime(0)).count();
        this->expected = count == DEF_N;
    }
};

class EqualsZeroT :
    public
        QueryEqualsZeroT<
            WithRandomTRows<
                WithOneColumn<
                    type_Timestamp, true>,
                DEF_N,
                946684800,  // 2000-01-01T00:00:00Z
                1893455999, // 2029-12-31T23:59:59Z
                1337        // A deterministic seed
                >
            > {

    const char *name() const {
        return "EqualsZero_Timestamp";
    }
};

class EqualsZeroDT :
    public
        QueryEqualsZeroDT<
            WithRandomDTRows<
                WithOneColumn<
                    type_OldDateTime, true>,
                DEF_N,
                946684800,  // 2000-01-01T00:00:00Z
                1893455999, // 2029-12-31T23:59:59Z
                1337        // A deterministic seed
                >
            > {

    const char *name() const {
        return "EqualsZero_OldDateTime";
    }
};

class GreaterThanZeroT :
    public
        QueryGreaterThanZeroT<
            WithRandomTRows<
                WithOneColumn<
                    type_Timestamp, true>,
                DEF_N,
                946684800,  // 2000-01-01T00:00:00Z
                1893455999, // 2029-12-31T23:59:59Z
                1337        // A deterministic seed
                >
            > {

    const char *name() const {
        return "GreaterThanZero_Timestamp";
    }
};

class GreaterThanZeroDT :
    public
        QueryGreaterThanZeroDT<
            WithRandomDTRows<
                WithOneColumn<
                    type_OldDateTime, true>,
                DEF_N,
                946684800,  // 2000-01-01T00:00:00Z
                1893455999, // 2029-12-31T23:59:59Z
                1337        // A deterministic seed
                >
            > {

    const char *name() const {
        return "GreaterThanZero_OldDateTime";
    }
};

int main() {
    Results results(10);
    bench<EqualsZeroT>(results);
    bench<EqualsZeroDT>(results);
    bench<GreaterThanZeroT>(results);
    bench<GreaterThanZeroDT>(results);
}
