#include <realm.hpp>

#include "benchmark.hpp"
#include "results.hpp"
#include "random.hpp"

using namespace realm;
using namespace realm::test_util;

#define DEF_N 10000


#define base WithRandomTs<WithClass, N, int_fast64_t, \
                946684800, \
                1893455999, \
                1337>
                // 2029-12-31T23:59:59Z
                // 2000-01-01T00:00:00Z
                // A deterministic seed
template<class WithClass, size_t N>
class WithRandomUnixTimes : public base {
  // Intentionally left blank; template paremeters do all the work.
};
#undef base

#define basebase WithOneColumn<type_Timestamp, true>
#define base WithRandomUnixTimes<basebase, N>
template<size_t N>
class WithRandomTimestamps : public base {
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
#undef basebase

#define basebase WithOneColumn<type_OldDateTime, true>
#define base WithRandomUnixTimes<basebase, N>
template<size_t N>
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
#undef basebase

#define basebase WithOneColumn<type_Int, true>
#define base WithRandomUnixTimes<basebase, N>
template<size_t N>
class WithRandomInts : public base {
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
#undef basebase

template<class WithClass>
class QueryEqualsZeroTimestamp : public WithClass {
    void operator()(SharedGroup& sg)
    {
        ReadTransaction tr(sg);
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<Timestamp>(0) ==
            Timestamp(0, 0)).count();
        this->asExpected = count == 1;
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
        this->asExpected = count == 1;
    }
};

template<class WithClass>
class QueryEqualsZeroInt : public WithClass {
    void operator()(SharedGroup& sg)
    {
        ReadTransaction tr(sg);
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<Int>(0) == 0).count();
        this->asExpected = count == 1;
    }
};

template<class WithClass, size_t expected>
class QueryGreaterThanZeroTimestamp : public WithClass {
    void operator()(SharedGroup& sg)
    {
        ReadTransaction tr(sg);
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<Timestamp>(0) >
            Timestamp(0, 0)).count();
        this->asExpected = count == expected;
    }
};

template<class WithClass, size_t expected>
class QueryGreaterThanZeroOldDateTime : public WithClass {
    void operator()(SharedGroup& sg)
    {
        ReadTransaction tr(sg);
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<OldDateTime>(0) >
            OldDateTime(0)).count();
        this->asExpected = count == expected;
    }
};

template<class WithClass, size_t expected>
class QueryGreaterThanZeroInt : public WithClass {
    void operator()(SharedGroup& sg)
    {
        ReadTransaction tr(sg);
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<Int>(0) > 0).count();
        this->asExpected = count == expected;
    }
};

class EqualsZeroTimestamp :
    public
        QueryEqualsZeroTimestamp<
            WithRandomTimestamps<DEF_N>
            > {

    const char *name() const {
        return "EqualsZero_Timestamp";
    }
};

class EqualsZeroOldDateTime :
    public
        QueryEqualsZeroOldDateTime<
            WithRandomOldDateTimes<DEF_N>
            > {

    const char *name() const {
        return "EqualsZero_OldDateTime";
    }
};

class EqualsZeroInt :
    public
        QueryEqualsZeroInt<
            WithRandomInts<DEF_N>
            > {

    const char *name() const {
        return "EqualsZero_Integer";
    }
};

class GreaterThanZeroTimestamp :
    public
        QueryGreaterThanZeroTimestamp<
            WithRandomTimestamps<DEF_N>,
            DEF_N> {

    const char *name() const {
        return "GreaterThanZero_Timestamp";
    }
};

class GreaterThanZeroOldDateTime :
    public
        QueryGreaterThanZeroOldDateTime<
            WithRandomOldDateTimes<DEF_N>,
            DEF_N> {

    const char *name() const {
        return "GreaterThanZero_OldDateTime";
    }
};

class GreaterThanZeroInt :
    public
        QueryGreaterThanZeroInt<
            WithRandomInts<DEF_N>,
            DEF_N> {

    const char *name() const {
        return "GreaterThanZero_Integer";
    }
};

int main()
{
    Results results(10);
    bench<EqualsZeroTimestamp>(results);
    bench<EqualsZeroOldDateTime>(results);
    bench<EqualsZeroInt>(results);
    bench<GreaterThanZeroTimestamp>(results);
    bench<GreaterThanZeroOldDateTime>(results);
    bench<GreaterThanZeroInt>(results);
}
