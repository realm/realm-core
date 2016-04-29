#include <realm.hpp>

#include "benchmark.hpp"
#include "results.hpp"
#include "random.hpp"

using namespace realm;
using namespace realm::test_util;

#define DEF_N 10000

#define base WithRandomTs<WithClass, N, int_fast64_t, min, max, seed>
template<class WithClass, size_t N,
    int_fast64_t min = std::numeric_limits<int_fast64_t>::min(),
    int_fast64_t max = std::numeric_limits<int_fast64_t>::max(),
    unsigned long seed = std::mt19937::default_seed>
class WithRandomTimedates : public base {
    void before_all(SharedGroup& sg)
    {
        base::before_all(sg);

        WriteTransaction tr(sg);
        TableRef t = tr.get_table(0);

        size_t i;
        for (i = 0; i < N; i++) {
            t->add_empty_row();
            t->set_timestamp(0, i, Timestamp(this->values[i], 0));
        }

        t->add_empty_row();
        t->set_timestamp(0, i, Timestamp(0, 0));

        tr.commit();
    }
};
#undef base

#define base WithRandomTs<WithClass, N, int_fast64_t, min, max, seed>
template<class WithClass, size_t N,
    int_fast64_t min = std::numeric_limits<int_fast64_t>::min(),
    int_fast64_t max = std::numeric_limits<int_fast64_t>::max(),
    unsigned long seed = std::mt19937::default_seed>
class WithRandomOldDateTimes : public base {
    void before_all(SharedGroup& sg)
    {
        base::before_all(sg);

        WriteTransaction tr(sg);
        TableRef t = tr.get_table(0);

        size_t i;
        for (i = 0; i < N; i++) {
            t->add_empty_row();
            t->set_olddatetime(0, i, OldDateTime(this->values[i]));
        }

        t->add_empty_row();
        t->set_olddatetime(0, i, OldDateTime(0));

        tr.commit();
    }
};
#undef base

#define base WithRandomTs<WithClass, N, int_fast64_t, min, max, seed>
template<class WithClass, size_t N,
    int_fast64_t min = std::numeric_limits<int_fast64_t>::min(),
    int_fast64_t max = std::numeric_limits<int_fast64_t>::max(),
    unsigned long seed = std::mt19937::default_seed>
class WithRandomIntegers : public base {
    void before_all(SharedGroup& sg)
    {
        base::before_all(sg);

        WriteTransaction tr(sg);
        TableRef t = tr.get_table(0);

        size_t i;
        for (i = 0; i < N; i++) {
            t->add_empty_row();
            t->set_int(0, i, this->values[i]);
        }

        t->add_empty_row();
        t->set_int(0, i, 0);

        tr.commit();
    }
};
#undef base

template<class WithClass>
class QueryEqualsZeroTimestamp : public WithClass {
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
class QueryEqualsZeroOldDateTime : public WithClass {
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
class QueryEqualsZeroInteger : public WithClass {
    void operator()(SharedGroup& sg)
    {
        ReadTransaction tr(sg);
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<Int>(0) == 0).count();
        this->expected = count == 1;
    }
};

template<class WithClass>
class QueryGreaterThanZeroTimestamp : public WithClass {
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
class QueryGreaterThanZeroOldDateTime : public WithClass {
    void operator()(SharedGroup& sg)
    {
        ReadTransaction tr(sg);
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<OldDateTime>(0) >
            OldDateTime(0)).count();
        this->expected = count == DEF_N;
    }
};

template<class WithClass>
class QueryGreaterThanZeroInteger : public WithClass {
    void operator()(SharedGroup& sg)
    {
        ReadTransaction tr(sg);
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<Int>(0) > 0).count();
        this->expected = count == DEF_N;
    }
};

class EqualsZeroTimestamp :
    public
        QueryEqualsZeroTimestamp<
            WithRandomTimedates<
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

class EqualsZeroOldDateTime :
    public
        QueryEqualsZeroOldDateTime<
            WithRandomOldDateTimes<
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

class EqualsZeroInteger :
    public
        QueryEqualsZeroInteger<
            WithRandomIntegers<
                WithOneColumn<
                    type_Int, true>,
                DEF_N,
                946684800,  // 2000-01-01T00:00:00Z
                1893455999, // 2029-12-31T23:59:59Z
                1337        // A deterministic seed
                >
            > {

    const char *name() const {
        return "EqualsZero_Integer";
    }
};

class GreaterThanZeroTimestamp :
    public
        QueryGreaterThanZeroTimestamp<
            WithRandomTimedates<
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

class GreaterThanZeroOldDateTime :
    public
        QueryGreaterThanZeroOldDateTime<
            WithRandomOldDateTimes<
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

class GreaterThanZeroInteger :
    public
        QueryGreaterThanZeroInteger<
            WithRandomIntegers<
                WithOneColumn<
                    type_Int, true>,
                DEF_N,
                946684800,  // 2000-01-01T00:00:00Z
                1893455999, // 2029-12-31T23:59:59Z
                1337        // A deterministic seed
                >
            > {

    const char *name() const {
        return "GreaterThanZero_Integer";
    }
};

int main() {
    Results results(10);
    bench<EqualsZeroTimestamp>(results);
    bench<EqualsZeroOldDateTime>(results);
    bench<EqualsZeroInteger>(results);
    bench<GreaterThanZeroTimestamp>(results);
    bench<GreaterThanZeroOldDateTime>(results);
    bench<GreaterThanZeroInteger>(results);
}
