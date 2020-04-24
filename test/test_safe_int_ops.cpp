/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include "testsettings.hpp"

#include <limits>
#include <vector>
#include <set>
#include <iostream>
#include <typeinfo>

#include <cstdint>

#include <realm/util/type_list.hpp>
#include <realm/util/safe_int_ops.hpp>

#include "util/demangle.hpp"
#include "util/super_int.hpp"

#include "test.hpp"

using namespace realm::util;
using namespace realm::test_util;
using unit_test::TestContext;


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


// FIXME: Test T -> realm::test_util::super_int -> T using min/max
// values for each fundamental standard type, and also using 0 and -1
// for signed types.


TEST(SafeIntOps_AddWithOverflowDetect)
{
    { // signed and signed
        int lval = 255;
        signed char rval = 10;
        CHECK(!int_add_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, 255 + 10);

        rval = 1;
        lval = std::numeric_limits<int>::max();
        CHECK(int_add_with_overflow_detect(lval, rval));    // does overflow
        CHECK_EQUAL(lval, std::numeric_limits<int>::max()); // unchanged

        rval = 1;
        lval = std::numeric_limits<int>::max() - 1;
        CHECK(!int_add_with_overflow_detect(lval, rval));   // does not overflow
        CHECK_EQUAL(lval, std::numeric_limits<int>::max()); // changed

        rval = 0;
        lval = std::numeric_limits<int>::max();
        CHECK(!int_add_with_overflow_detect(lval, rval));   // does not overflow
        CHECK_EQUAL(lval, std::numeric_limits<int>::max()); // unchanged

        rval = -1;
        lval = std::numeric_limits<int>::min();
        CHECK(int_add_with_overflow_detect(lval, rval));    // does overflow
        CHECK_EQUAL(lval, std::numeric_limits<int>::min()); // unchanged
    }
    { // signed and unsigned
        signed char lval = std::numeric_limits<signed char>::max();
        size_t rval = 0;
        CHECK(!int_add_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<signed char>::max());

        lval = std::numeric_limits<signed char>::max();
        rval = 1;
        CHECK(int_add_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<signed char>::max());

        lval = 0;
        rval = std::numeric_limits<signed char>::max();
        CHECK(!int_add_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<signed char>::max());

        lval = -1;
        rval = std::numeric_limits<signed char>::max() + 1;
        CHECK(!int_add_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<signed char>::max());

        lval = -1;
        rval = std::numeric_limits<signed char>::max() + 2;
        CHECK(int_add_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, -1);
    }
    { // unsigned and signed
        size_t lval = std::numeric_limits<size_t>::max();
        signed char rval = 0;
        CHECK(!int_add_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::max());

        lval = std::numeric_limits<size_t>::max();
        rval = 1;
        CHECK(int_add_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::max());

        lval = std::numeric_limits<size_t>::max();
        rval = -1;
        CHECK(!int_add_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::max() - 1);

        lval = std::numeric_limits<size_t>::min();
        rval = 0;
        CHECK(!int_add_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::min());

        lval = std::numeric_limits<size_t>::min();
        rval = -1;
        CHECK(int_add_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::min());

        // lval::bits < rval::bits
        unsigned char lval2 = std::numeric_limits<unsigned char>::max();
        int64_t rval2 = 1;
        CHECK(int_add_with_overflow_detect(lval2, rval2));
        CHECK_EQUAL(lval2, std::numeric_limits<unsigned char>::max());

        lval2 = std::numeric_limits<unsigned char>::max() - 1;
        rval2 = 1;
        CHECK(!int_add_with_overflow_detect(lval2, rval2));
        CHECK_EQUAL(lval2, std::numeric_limits<unsigned char>::max());

        lval2 = 0;
        rval2 = std::numeric_limits<unsigned char>::max() + 1;
        CHECK(int_add_with_overflow_detect(lval2, rval2));
        CHECK_EQUAL(lval2, 0);
    }
    { // unsigned and unsigned
        size_t lval = std::numeric_limits<size_t>::max();
        size_t rval = 0;
        CHECK(!int_add_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::max());

        lval = std::numeric_limits<size_t>::max();
        rval = 1;
        CHECK(int_add_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::max());

        lval = 0;
        rval = std::numeric_limits<size_t>::max();
        CHECK(!int_add_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::max());

        lval = 1;
        rval = std::numeric_limits<size_t>::max();
        CHECK(int_add_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, 1);

        lval = std::numeric_limits<size_t>::max();
        rval = std::numeric_limits<size_t>::max();
        CHECK(int_add_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::max());
    }
}


TEST(SafeIntOps_SubtractWithOverflowDetect)
{
    { // signed and signed
        int lval = std::numeric_limits<int>::max() - 1;
        signed char rval = -10;
        CHECK(int_subtract_with_overflow_detect(lval, rval));   // does overflow
        CHECK_EQUAL(lval, std::numeric_limits<int>::max() - 1); // unchanged

        rval = -1;
        lval = std::numeric_limits<int>::max();
        CHECK(int_subtract_with_overflow_detect(lval, rval)); // does overflow
        CHECK_EQUAL(lval, std::numeric_limits<int>::max());   // unchanged

        rval = 0;
        lval = std::numeric_limits<int>::max();
        CHECK(!int_subtract_with_overflow_detect(lval, rval)); // does not overflow
        CHECK_EQUAL(lval, std::numeric_limits<int>::max());    // unchanged

        rval = 0;
        lval = std::numeric_limits<int>::min();
        CHECK(!int_subtract_with_overflow_detect(lval, rval)); // does not overflow
        CHECK_EQUAL(lval, std::numeric_limits<int>::min());    // unchanged

        rval = 1;
        lval = std::numeric_limits<int>::min();
        CHECK(int_subtract_with_overflow_detect(lval, rval)); // does overflow
        CHECK_EQUAL(lval, std::numeric_limits<int>::min());   // unchanged
    }
    { // signed and unsigned
        signed char lval = std::numeric_limits<signed char>::min();
        size_t rval = 0;
        CHECK(!int_subtract_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<signed char>::min());

        lval = std::numeric_limits<signed char>::min();
        rval = 1;
        CHECK(int_subtract_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<signed char>::min());

        lval = std::numeric_limits<signed char>::min() + 1;
        rval = 1;
        CHECK(!int_subtract_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<signed char>::min());

        lval = std::numeric_limits<signed char>::min() + 1;
        rval = 2;
        CHECK(int_subtract_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<signed char>::min() + 1);

        lval = 0;
        rval = -1 * std::numeric_limits<signed char>::min();
        CHECK(!int_subtract_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<signed char>::min());

        lval = -1;
        rval = -1 * std::numeric_limits<signed char>::min();
        CHECK(int_subtract_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, -1);
    }
    { // unsigned and signed
        size_t lval = std::numeric_limits<size_t>::min();
        signed char rval = 0;
        CHECK(!int_subtract_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::min());

        lval = std::numeric_limits<size_t>::min();
        rval = 1;
        CHECK(int_subtract_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::min());

        lval = std::numeric_limits<size_t>::max();
        rval = 1;
        CHECK(!int_subtract_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::max() - 1);

        lval = std::numeric_limits<size_t>::max();
        rval = 0;
        CHECK(!int_subtract_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::max());

        lval = std::numeric_limits<size_t>::max();
        rval = -1;
        CHECK(int_subtract_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::max());

        // lval::bits < rval::bits
        unsigned char lval2 = 0;
        int64_t rval2 = 1;
        CHECK(int_subtract_with_overflow_detect(lval2, rval2));
        CHECK_EQUAL(lval2, 0);

        lval2 = std::numeric_limits<unsigned char>::max();
        rval2 = std::numeric_limits<unsigned char>::max();
        CHECK(!int_subtract_with_overflow_detect(lval2, rval2));
        CHECK_EQUAL(lval2, 0);

        lval2 = std::numeric_limits<unsigned char>::max();
        rval2 = std::numeric_limits<unsigned char>::max() + 1;
        CHECK(int_subtract_with_overflow_detect(lval2, rval2));
        CHECK_EQUAL(lval2, std::numeric_limits<unsigned char>::max());
    }
    { // unsigned and unsigned
        size_t lval = std::numeric_limits<size_t>::min();
        size_t rval = 0;
        CHECK(!int_subtract_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::min());

        lval = std::numeric_limits<size_t>::min();
        rval = 1;
        CHECK(int_subtract_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::min());

        lval = 0;
        rval = std::numeric_limits<size_t>::max();
        CHECK(int_subtract_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, 0);

        lval = std::numeric_limits<size_t>::max() - 1;
        rval = std::numeric_limits<size_t>::max();
        CHECK(int_subtract_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, std::numeric_limits<size_t>::max() - 1);

        lval = std::numeric_limits<size_t>::max();
        rval = std::numeric_limits<size_t>::max();
        CHECK(!int_subtract_with_overflow_detect(lval, rval));
        CHECK_EQUAL(lval, 0);
    }
}


TEST(SafeIntOps_Comparisons)
{
    int lval = 0;
    unsigned char rval = 0;
    CHECK(int_equal_to(lval, rval));
    CHECK(!int_not_equal_to(lval, rval));
    CHECK(!int_less_than(lval, rval));
    CHECK(int_less_than_or_equal(lval, rval));
    CHECK(!int_greater_than(lval, rval));
    CHECK(int_greater_than_or_equal(lval, rval));

    lval = std::numeric_limits<int>::max();
    rval = std::numeric_limits<unsigned char>::max();
    CHECK(!int_equal_to(lval, rval));
    CHECK(int_not_equal_to(lval, rval));
    CHECK(!int_less_than(lval, rval));
    CHECK(!int_less_than_or_equal(lval, rval));
    CHECK(int_greater_than(lval, rval));
    CHECK(int_greater_than_or_equal(lval, rval));
}


TEST(SafeIntOps_MultiplyOverflow)
{
    int lval = 256;
    signed char rval = 2;
    CHECK(!int_multiply_with_overflow_detect(lval, rval));
    CHECK_EQUAL(lval, 512);

    lval = std::numeric_limits<int>::max();
    rval = 2;
    CHECK(int_multiply_with_overflow_detect(lval, rval));
    CHECK_EQUAL(lval, std::numeric_limits<int>::max());

    signed char lval2 = 2;
    int rval2 = 63;
    CHECK(!int_multiply_with_overflow_detect(lval2, rval2));
    CHECK_EQUAL(lval2, 126);

    lval2 = 2;
    rval2 = 64; // numeric_limits<signed char>::max() is 127
    CHECK(int_multiply_with_overflow_detect(lval2, rval2));
    CHECK_EQUAL(lval2, 2);
}


TEST(SafeIntOps_IntCast)
{
    int64_t signed_int = std::numeric_limits<signed char>::max() + 1;
    signed char signed_char = 0;
    CHECK(int_cast_with_overflow_detect(signed_int, signed_char));
    CHECK_EQUAL(signed_char, 0);

    signed_int = std::numeric_limits<signed char>::max();
    signed_char = 0;
    CHECK(!int_cast_with_overflow_detect(signed_int, signed_char));
    CHECK_EQUAL(signed_char, std::numeric_limits<signed char>::max());

    signed_int = std::numeric_limits<signed char>::min();
    signed_char = 0;
    CHECK(!int_cast_with_overflow_detect(signed_int, signed_char));
    CHECK_EQUAL(signed_int, signed_char);

    signed_int = std::numeric_limits<signed char>::min() - 1;
    signed_char = 0;
    CHECK(int_cast_with_overflow_detect(signed_int, signed_char));
    CHECK_EQUAL(signed_char, 0);

    signed_char = std::numeric_limits<signed char>::max();
    signed_int = 0;
    CHECK(!int_cast_with_overflow_detect(signed_char, signed_int));
    CHECK_EQUAL(signed_int, signed_char);

    signed_char = std::numeric_limits<signed char>::min();
    signed_int = 0;
    CHECK(!int_cast_with_overflow_detect(signed_char, signed_int));
    CHECK_EQUAL(signed_int, signed_char);
}


TEST(SafeIntOps_ShiftLeft)
{
    size_t unsigned_int = 1;
    CHECK(!int_shift_left_with_overflow_detect(unsigned_int, 0));
    CHECK_EQUAL(unsigned_int, 1);

    unsigned_int = 0;
    CHECK(!int_shift_left_with_overflow_detect(unsigned_int, 1));
    CHECK_EQUAL(unsigned_int, 0);

    unsigned_int = 1;
    CHECK(!int_shift_left_with_overflow_detect(unsigned_int, 1));
    CHECK_EQUAL(unsigned_int, 2);

    unsigned_int = 1;
    CHECK(!int_shift_left_with_overflow_detect(unsigned_int, std::numeric_limits<size_t>::digits - 1));
    CHECK_EQUAL(unsigned_int, size_t(1) << (std::numeric_limits<size_t>::digits - 1));

    // Shifting by 64 (or greater) is not defined behaviour.
    // With clang, the following does not overflow and gives unsigned_int == 1
    //    unsigned_int = 1;
    //    CHECK(int_shift_left_with_overflow_detect(unsigned_int, std::numeric_limits<size_t>::digits));
    //    CHECK_EQUAL(unsigned_int, 1);

    unsigned_int = 2;
    CHECK(int_shift_left_with_overflow_detect(unsigned_int, std::numeric_limits<size_t>::digits - 1));
    CHECK_EQUAL(unsigned_int, 2);

    unsigned_int = std::numeric_limits<size_t>::max();
    CHECK(int_shift_left_with_overflow_detect(unsigned_int, 1));
    CHECK_EQUAL(unsigned_int, std::numeric_limits<size_t>::max());

    int signed_int = 1;
    CHECK(!int_shift_left_with_overflow_detect(signed_int, 0));
    CHECK_EQUAL(signed_int, 1);

    signed_int = 0;
    CHECK(!int_shift_left_with_overflow_detect(signed_int, 1));
    CHECK_EQUAL(signed_int, 0);

    signed_int = 1;
    CHECK(!int_shift_left_with_overflow_detect(signed_int, 1));
    CHECK_EQUAL(signed_int, 2);

    signed_int = 1;
    CHECK(!int_shift_left_with_overflow_detect(signed_int, std::numeric_limits<int>::digits - 1));
    CHECK_EQUAL(signed_int, int(1) << (std::numeric_limits<int>::digits - 1));

    signed_int = 2;
    CHECK(int_shift_left_with_overflow_detect(signed_int, std::numeric_limits<int>::digits - 1));
    CHECK_EQUAL(signed_int, 2);

    signed_int = std::numeric_limits<int>::max();
    CHECK(int_shift_left_with_overflow_detect(signed_int, 1));
    CHECK_EQUAL(signed_int, std::numeric_limits<int>::max());
}


namespace {

template <class T_1, class T_2>
void test_two_args(TestContext& test_context, const std::set<super_int>& values)
{
    //    if (!(std::is_same<T_1, bool>::value && std::is_same<T_2, char>::value))
    //        return;

    //    std::cerr << get_type_name<T_1>() << ", " << get_type_name<T_2>() << "\n";
    std::vector<T_1> values_1;
    std::vector<T_2> values_2;
    {
        typedef std::set<super_int>::const_iterator iter;
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

    typedef typename std::vector<T_1>::const_iterator iter_1;
    typedef typename std::vector<T_2>::const_iterator iter_2;
    iter_1 end_1 = values_1.end();
    iter_2 end_2 = values_2.end();
    for (iter_1 i_1 = values_1.begin(); i_1 != end_1; ++i_1) {
        for (iter_2 i_2 = values_2.begin(); i_2 != end_2; ++i_2) {
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
                bool add_overflow_1 = s_1.add_with_overflow_detect(s_2) || s_1.cast_has_overflow<T_1>();
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
                bool sub_overflow_1 = s_1.subtract_with_overflow_detect(s_2) || s_1.cast_has_overflow<T_1>();
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


typedef void types_01;
typedef TypeAppend<types_01, char>::type types_02;
typedef TypeAppend<types_02, signed char>::type types_03;
typedef TypeAppend<types_03, unsigned char>::type types_04;
typedef TypeAppend<types_04, wchar_t>::type types_05;
typedef TypeAppend<types_05, short>::type types_06;
typedef TypeAppend<types_06, unsigned short>::type types_07;
typedef TypeAppend<types_07, int>::type types_08;
typedef TypeAppend<types_08, unsigned>::type types_09;
typedef TypeAppend<types_09, long>::type types_10;
typedef TypeAppend<types_10, unsigned long>::type types_11;
typedef TypeAppend<types_11, long long>::type types_12;
typedef TypeAppend<types_12, unsigned long long>::type types_13;
typedef types_13 types;


template <class T, int>
struct add_min_max {
    static void exec(std::set<super_int>* values)
    {
        typedef std::numeric_limits<T> lim;
        values->insert(super_int(lim::min()));
        values->insert(super_int(lim::max()));
    }
};

template <class T_1, int>
struct test_two_args_1 {
    template <class T_2, int>
    struct test_two_args_2 {
        static void exec(TestContext* test_context, const std::set<super_int>* values)
        {
            test_two_args<T_1, T_2>(*test_context, *values);
        }
    };
    static void exec(TestContext* test_context, const std::set<super_int>* values)
    {
        ForEachType<types, test_two_args_2>::exec(test_context, values);
    }
};

} // anonymous namespace


TEST_IF(SafeIntOps_General, TEST_DURATION >= 1)
{
    // Generate a set of interesting values in three steps
    std::set<super_int> values;

    // Add 0 to the set (worst case 1)
    values.insert(super_int(0));

    // Add min and max for all integer types to set (worst case 27)
    ForEachType<types, add_min_max>::exec(&values);

    // Add x-1 and x+1 to the set for all x in set (worst case 81)
    {
        super_int min_val(std::numeric_limits<intmax_t>::min());
        super_int max_val(std::numeric_limits<uintmax_t>::max());
        std::set<super_int> values_2 = values;
        typedef std::set<super_int>::const_iterator iter;
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
        super_int min_val(std::numeric_limits<intmax_t>::min());
        super_int max_val(std::numeric_limits<uintmax_t>::max());
        std::set<super_int> values_2 = values;
        typedef std::set<super_int>::const_iterator iter;
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
        typedef std::set<super_int>::const_iterator iter;
        iter end = values.end();
        for (iter i = values.begin(); i != end; ++i)
            std::cout << *i << "\n";
    }
    */

    ForEachType<types, test_two_args_1>::exec(&test_context, &values);
}
