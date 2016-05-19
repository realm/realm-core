#include <realm.hpp>

#include "benchmark.hpp"
#include "realmbench.hpp"
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
    void before_all()
    {
        base::before_all();

        WriteTransaction tr(*(this->sg));
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
    void before_all()
    {
        base::before_all();

        WriteTransaction tr(*(this->sg));
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
    void before_all()
    {
        base::before_all();

        WriteTransaction tr(*(this->sg));
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
    void bench()
    {
        ReadTransaction tr(*(this->sg));
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<Timestamp>(0) ==
            Timestamp(0, 0)).count();
        this->asExpected = count == 1;
    }
};

template<class WithClass>
class QueryEqualsZeroOldDateTime : public WithClass {
    void bench()
    {
        ReadTransaction tr(*(this->sg));
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<OldDateTime>(0) ==
            OldDateTime(0)).count();
        this->asExpected = count == 1;
    }
};

template<class WithClass>
class QueryEqualsZeroInt : public WithClass {
    void bench()
    {
        ReadTransaction tr(*(this->sg));
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<Int>(0) == 0).count();
        this->asExpected = count == 1;
    }
};

template<class WithClass, size_t expected>
class QueryGreaterThanZeroTimestamp : public WithClass {
    void bench()
    {
        ReadTransaction tr(*(this->sg));
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<Timestamp>(0) >
            Timestamp(0, 0)).count();
        this->asExpected = count == expected;
    }
};

template<class WithClass, size_t expected>
class QueryGreaterThanZeroOldDateTime : public WithClass {
    void bench()
    {
        ReadTransaction tr(*(this->sg));
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<OldDateTime>(0) >
            OldDateTime(0)).count();
        this->asExpected = count == expected;
    }
};

template<class WithClass, size_t expected>
class QueryGreaterThanZeroInt : public WithClass {
    void bench()
    {
        ReadTransaction tr(*(this->sg));
        ConstTableRef t = tr.get_table(0);
        size_t count = (t->where().get_table()->column<Int>(0) > 0).count();
        this->asExpected = count == expected;
    }
};

template<size_t N>
class EqualsZeroTimestamp :
    public
        QueryEqualsZeroTimestamp<
            WithRandomTimestamps<N>
            > {

    const char *name() const {
        return "EqualsZero_Timestamp";
    }
};

template<size_t N>
class EqualsZeroOldDateTime :
    public
        QueryEqualsZeroOldDateTime<
            WithRandomOldDateTimes<N>
            > {

    const char *name() const {
        return "EqualsZero_OldDateTime";
    }
};

template<size_t N>
class EqualsZeroInt :
    public
        QueryEqualsZeroInt<
            WithRandomInts<N>
            > {

    const char *name() const {
        return "EqualsZero_Integer";
    }
};

template<size_t N>
class GreaterThanZeroTimestamp :
    public
        QueryGreaterThanZeroTimestamp<
            WithRandomTimestamps<N>,
            N> {

    const char *name() const {
        return "GreaterThanZero_Timestamp";
    }
};

template<size_t N>
class GreaterThanZeroOldDateTime :
    public
        QueryGreaterThanZeroOldDateTime<
            WithRandomOldDateTimes<N>,
            N> {

    const char *name() const {
        return "GreaterThanZero_OldDateTime";
    }
};

template<size_t N>
class GreaterThanZeroInt :
    public
        QueryGreaterThanZeroInt<
            WithRandomInts<N>,
            N> {

    const char *name() const {
        return "GreaterThanZero_Integer";
    }
};

template<DataType data_type, size_t N>
class SizeEmptyRows :
    public
        Size<
            WithEmptyRows<
                WithOneColumn<data_type, true>,
                N
            >,
            N
        > {
    const char *name() const {
        return typeid(this).name();
    }
};

template<size_t N>
class SizeRandomInts :
    public
        Size<WithRandomInts<N>, N + 1> {
    const char *name() const {
        return typeid(this).name();
    }
};

template<size_t N>
class SizeRandomTimestamps :
    public
        Size<WithRandomTimestamps<N>, N + 1> {
    const char *name() const {
        return typeid(this).name();
    }
};

template<size_t N>
class SizeRandomOldDateTimes :
    public
        Size<WithRandomOldDateTimes<N>, N + 1> {
    const char *name() const {
        return typeid(this).name();
    }
};

int main()
{
    Results results(10);

    bench< SizeEmptyRows<type_Int, DEF_N> >(results);
    bench< SizeEmptyRows<type_Timestamp, DEF_N> >(results);
    bench< SizeEmptyRows<type_OldDateTime, DEF_N> >(results);

    bench< SizeRandomInts<DEF_N> >(results);
    bench< SizeRandomTimestamps<DEF_N> >(results);
    bench< SizeRandomOldDateTimes<DEF_N> >(results);

    bench< EqualsZeroTimestamp<DEF_N> >(results);
    bench< EqualsZeroOldDateTime<DEF_N> >(results);
    bench< EqualsZeroInt<DEF_N> >(results);

    bench< GreaterThanZeroTimestamp<DEF_N> >(results);
    bench< GreaterThanZeroOldDateTime<DEF_N> >(results);
    bench< GreaterThanZeroInt<DEF_N> >(results);
}
