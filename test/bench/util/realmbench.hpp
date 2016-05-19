#ifndef REALM_REALMBENCH_HPP
#define REALM_REALMBENCH_HPP

#include <realm.hpp>
#include "benchmark.hpp"

#include "random.hpp"   // Random

namespace realm {
namespace test_util {

class WithSharedGroup : public Benchmark {
public:
    std::unique_ptr<SharedGroup> sg;

    WithSharedGroup();
};

template<
    DataType data_type, bool nullable = false,
    class WithClass = WithSharedGroup>
class WithOneColumn : public WithClass {
public:
    void before_all()
    {
        WriteTransaction tr(*(this->sg));
        TableRef t = tr.add_table("table");
        t->add_column(data_type, "first", nullable);
        tr.commit();
    }

    void after_all()
    {
        // WriteTransaction doesn't have remove_table :-/
        Group& g = this->sg->begin_write();
        g.remove_table("table");
        this->sg->commit();
    }
};

template<class WithClass, size_t N>
class AddEmptyRows : public WithClass {
public:
    void bench()
    {
        WriteTransaction tr(*(this->sg));
        TableRef t = tr.get_table(0);
        t->add_empty_row(N);
        tr.commit();
    }
};

template<class WithClass, size_t N>
class WithEmptyRows : public WithClass {
public:
    void before_all()
    {
        WithClass::before_all();

        WriteTransaction tr(*(this->sg));
        TableRef t = tr.get_table(0);
        t->add_empty_row(N);
        tr.commit();
    }
};

template<class WithClass, size_t N,
    typename T,
    T min = std::numeric_limits<T>::min(),
    T max = std::numeric_limits<T>::max(),
    unsigned long seed = std::mt19937::default_seed>
class WithRandomTs : public WithClass {
public:
    T values[N];

    void before_all()
    {
        WithClass::before_all();

        Random random(seed);
        size_t i;
        for (i = 0; i < N; i++) {
            this->values[i] = random.draw_int<T>(min, max);
        }
    }
};

template<class WithClass, size_t expected>
class Size : public WithClass {
    void bench()
    {
        ReadTransaction tr(*(this->sg));
        ConstTableRef t = tr.get_table(0);
        this->asExpected = t->size() == expected;
    }
};

} // namespace test_util
} // namespace realm

#endif // REALM_REALMBENCH_HPP
