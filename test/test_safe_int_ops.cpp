#include <limits>
#include <vector>
#include <set>
#include <iostream>
#include <typeinfo>

#include <stdint.h>

#include <tightdb/util/type_list.hpp>
#include <tightdb/util/safe_int_ops.hpp>

#include "util/demangle.hpp"
#include "util/super_int.hpp"

#include "test.hpp"

using namespace std;
using namespace tightdb::util;
using namespace tightdb::test_util;
using unit_test::TestResults;


// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment varible `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.



// FIXME: Test T -> tightdb::test_util::super_int -> T using min/max
// values for each fundamental standard type, and also using 0 and -1
// for signed types.

// FIXME: Test tightdb::util::from_twos_compl(). For each type pair
// (S,U), and for each super_int `i` in special set, if i.get_as<S>(s)
// && two's compl of `s` can be stored in U without loss of
// information, then CHECK_EQUAL(s, from_twos_compl(U(s))). Two's
// compl of `s` can be stored in U without loss of information if, and
// only if make_unsigned<S>::type(s < 0 ?  -1-s : s) <
// (U(1)<<(lim_u::digits-1)).

// FIXME: Test tightdb::util::int_shift_left_with_overflow_detect().

// FIXME: Test tightdb::util::int_cast_with_overflow_detect().


TEST(SafeIntOps_AddWithOverflowDetect)
{
    int lval = 255;
    CHECK(!int_add_with_overflow_detect(lval, char(10)));
}


namespace {

template<class T_1, class T_2>
void test_two_args(TestResults& test_results, const set<super_int>& values)
{
//    if (!(SameType<T_1, bool>::value && SameType<T_2, char>::value))
//        return;

//    cerr << get_type_name<T_1>() << ", " << get_type_name<T_2>() << "\n";
    vector<T_1> values_1;
    vector<T_2> values_2;
    {
        typedef set<super_int>::const_iterator iter;
        iter end = values.end();
        for (iter i = values.begin(); i != end; ++i) {
            T_1 v_1;
            if (i->get_as<T_1>(v_1))
                values_1.push_back(v_1);
            T_2 v_2;
            if (i->get_as<T_2>(v_2))
                values_2.push_back(v_2);
        }
    }

    typedef typename vector<T_1>::const_iterator iter_1;
    typedef typename vector<T_2>::const_iterator iter_2;
    iter_1 end_1 = values_1.end();
    iter_2 end_2 = values_2.end();
    for (iter_1 i_1 = values_1.begin(); i_1 != end_1; ++i_1) {
        for (iter_2 i_2 = values_2.begin(); i_2 != end_2; ++i_2) {
//            cout << "--> " << promote(*i_1) << " vs " << promote(*i_2) << "\n";
            // Comparisons
            {
                T_1 v_1 = *i_1;
                T_2 v_2 = *i_2;
                super_int s_1(v_1), s_2(v_2);
                bool eq_1 = s_1 == s_2;
                bool eq_2 = int_equal_to(v_1, v_2);
                CHECK_EQUAL(eq_1, eq_2);
                bool ne_1 = s_1 != s_2;
                bool ne_2 = int_not_equal_to(v_1, v_2);
                CHECK_EQUAL(ne_1, ne_2);
                bool lt_1 = s_1 < s_2;
                bool lt_2 = int_less_than(v_1, v_2);
                CHECK_EQUAL(lt_1, lt_2);
                bool gt_1 = s_1 > s_2;
                bool gt_2 = int_greater_than(v_1, v_2);
                CHECK_EQUAL(gt_1, gt_2);
                bool le_1 = s_1 <= s_2;
                bool le_2 = int_less_than_or_equal(v_1, v_2);
                CHECK_EQUAL(le_1, le_2);
                bool ge_1 = s_1 >= s_2;
                bool ge_2 = int_greater_than_or_equal(v_1, v_2);
                CHECK_EQUAL(ge_1, ge_2);
            }
            // Addition
            {
                T_1 v_1 = *i_1;
                T_2 v_2 = *i_2;
                super_int s_1(v_1), s_2(v_2);
                bool add_overflow_1 = s_1.add_with_overflow_detect(s_2) ||
                    s_1.cast_has_overflow<T_1>();
                bool add_overflow_2 = int_add_with_overflow_detect(v_1, v_2);
                CHECK_EQUAL(add_overflow_1, add_overflow_2);
                if (!add_overflow_1 && !add_overflow_2)
                    CHECK_EQUAL(s_1, super_int(v_1));
            }
            // Subtraction
            {
                T_1 v_1 = *i_1;
                T_2 v_2 = *i_2;
                super_int s_1(v_1), s_2(v_2);
                bool sub_overflow_1 = s_1.subtract_with_overflow_detect(s_2) ||
                    s_1.cast_has_overflow<T_1>();
                bool sub_overflow_2 = int_subtract_with_overflow_detect(v_1, v_2);
                CHECK_EQUAL(sub_overflow_1, sub_overflow_2);
                if (!sub_overflow_1 && !sub_overflow_2)
                    CHECK_EQUAL(s_1, super_int(v_1));
            }
/*
            // Multiplication
            {
                T_1 v_1 = *i_1;
                T_2 v_2 = *i_2;
                if (v_1 >= 0 && v_2 > 0) {
                    super_int s_1(v_1), s_2(v_2);
                    bool mul_overflow_1 = s_1.multiply_with_overflow_detect(s_2) ||
                        s_1.cast_has_overflow<T_1>();
                    bool mul_overflow_2 = int_multiply_with_overflow_detect(v_1, v_2);
                    CHECK_EQUAL(mul_overflow_1, mul_overflow_2);
                    if (!mul_overflow_1 && !mul_overflow_2)
                        CHECK_EQUAL(s_1, super_int(v_1));
                }
            }
*/
        }
    }
}


typedef void                                           types_00;
typedef TypeAppend<types_00, bool>::type               types_01;
typedef TypeAppend<types_01, char>::type               types_02;
typedef TypeAppend<types_02, signed char>::type        types_03;
typedef TypeAppend<types_03, unsigned char>::type      types_04;
typedef TypeAppend<types_04, wchar_t>::type            types_05;
typedef TypeAppend<types_05, short>::type              types_06;
typedef TypeAppend<types_06, unsigned short>::type     types_07;
typedef TypeAppend<types_07, int>::type                types_08;
typedef TypeAppend<types_08, unsigned>::type           types_09;
typedef TypeAppend<types_09, long>::type               types_10;
typedef TypeAppend<types_10, unsigned long>::type      types_11;
typedef TypeAppend<types_11, long long>::type          types_12;
typedef TypeAppend<types_12, unsigned long long>::type types_13;
typedef types_13 types;


template<class T, int> struct add_min_max {
    static void exec(set<super_int>* values)
    {
        typedef numeric_limits<T> lim;
        values->insert(super_int(lim::min()));
        values->insert(super_int(lim::max()));
    }
};

template<class T_1, int> struct test_two_args_1 {
    template<class T_2, int> struct test_two_args_2 {
        static void exec(TestResults* test_results, const set<super_int>* values)
        {
            test_two_args<T_1, T_2>(*test_results, *values);
        }
    };
    static void exec(TestResults* test_results, const set<super_int>* values)
    {
        ForEachType<types, test_two_args_2>::exec(test_results, values);
    }
};

} // anonymous namespace



TEST(SafeIntOps_General)
{
    // Generate a set of interesting values in these steps
    set<super_int> values;

    // Add 0 to the set (worst case 1)
    values.insert(super_int(0));

    // Add min and max for all integer types to set (worst case 27)
    ForEachType<types, add_min_max>::exec(&values);

    // Add x-1 and x+1 to the set for all x in set (worst case 81)
    {
        super_int min_val(numeric_limits<intmax_t>::min());
        super_int max_val(numeric_limits<uintmax_t>::max());
        set<super_int> values_2 = values;
        typedef set<super_int>::const_iterator iter;
        iter end = values_2.end();
        for (iter i = values_2.begin(); i != end; ++i) {
            if (*i > min_val)
                values.insert(*i - super_int(1));
            if (*i < max_val)
                values.insert(*i + super_int(1));
        }
    }

    // Add x+y and x-y to the set for all x and y in set (worst case
    // 13203)
    {
        super_int min_val(numeric_limits<intmax_t>::min());
        super_int max_val(numeric_limits<uintmax_t>::max());
        set<super_int> values_2 = values;
        typedef set<super_int>::const_iterator iter;
        iter end = values_2.end();
        for (iter i_1 = values_2.begin(); i_1 != end; ++i_1) {
            for (iter i_2 = values_2.begin(); i_2 != end; ++i_2) {
                super_int v_1 = *i_1;
                if (!v_1.add_with_overflow_detect(*i_2)) {
                    if (v_1 >= min_val && v_1 <= max_val)
                        values.insert(v_1);
                }
                super_int v_2 = *i_1;
                if (!v_2.subtract_with_overflow_detect(*i_2)) {
                    if (v_2 >= min_val && v_2 <= max_val)
                        values.insert(v_2);
                }
            }
        }
    }

/*
    {
        typedef set<super_int>::const_iterator iter;
        iter end = values.end();
        for (iter i = values.begin(); i != end; ++i)
            cout << *i << "\n";
    }
*/

    ForEachType<types, test_two_args_1>::exec(&test_results, &values);
}
