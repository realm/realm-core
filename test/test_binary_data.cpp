#include "testsettings.hpp"
#ifdef TEST_BINARY_DATA

#include <string>

#include <realm/binary_data.hpp>

#include "test.hpp"

using namespace std;
using namespace realm;


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


TEST(BinaryData_Equal)
{
    // Test operator==() and operator!=()
    BinaryData bd_00_1("", 0);
    BinaryData bd_00_2("", 0);
    BinaryData bd_00_3("", 0);
    BinaryData bd_01_1("x", 1);
    BinaryData bd_01_2("x", 1);
    BinaryData bd_01_3("y", 1);
    BinaryData bd_02_1("xy", 2);
    BinaryData bd_02_2("xy", 2);
    BinaryData bd_02_3("yz", 2);
    BinaryData bd_26_1("abcdefghijklmnopqrstuvwxyz", 26);
    BinaryData bd_26_2("abcdefghijklmnopqrstuvwxyz", 26);
    BinaryData bd_26_3("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 26);

    CHECK(bd_00_1 == bd_00_1 && !(bd_00_1 != bd_00_1));
    CHECK(bd_00_1 == bd_00_2 && !(bd_00_1 != bd_00_2));
    CHECK(bd_00_1 == bd_00_3 && !(bd_00_1 != bd_00_3));
    CHECK(bd_00_1 != bd_01_1 && !(bd_00_1 == bd_01_1));
    CHECK(bd_00_1 != bd_01_3 && !(bd_00_1 == bd_01_3));
    CHECK(bd_00_1 != bd_02_1 && !(bd_00_1 == bd_02_1));
    CHECK(bd_00_1 != bd_02_3 && !(bd_00_1 == bd_02_3));
    CHECK(bd_00_1 != bd_26_1 && !(bd_00_1 == bd_26_1));
    CHECK(bd_00_1 != bd_26_3 && !(bd_00_1 == bd_26_3));

    CHECK(bd_00_3 == bd_00_1 && !(bd_00_3 != bd_00_1));
    CHECK(bd_00_3 == bd_00_3 && !(bd_00_3 != bd_00_3));
    CHECK(bd_00_3 != bd_01_1 && !(bd_00_3 == bd_01_1));
    CHECK(bd_00_3 != bd_01_3 && !(bd_00_3 == bd_01_3));
    CHECK(bd_00_3 != bd_02_1 && !(bd_00_3 == bd_02_1));
    CHECK(bd_00_3 != bd_02_3 && !(bd_00_3 == bd_02_3));
    CHECK(bd_00_3 != bd_26_1 && !(bd_00_3 == bd_26_1));
    CHECK(bd_00_3 != bd_26_3 && !(bd_00_3 == bd_26_3));


    CHECK(bd_01_1 != bd_00_1 && !(bd_01_1 == bd_00_1));
    CHECK(bd_01_1 != bd_00_3 && !(bd_01_1 == bd_00_3));
    CHECK(bd_01_1 == bd_01_1 && !(bd_01_1 != bd_01_1));
    CHECK(bd_01_1 == bd_01_2 && !(bd_01_1 != bd_01_2));
    CHECK(bd_01_1 != bd_01_3 && !(bd_01_1 == bd_01_3));
    CHECK(bd_01_1 != bd_02_1 && !(bd_01_1 == bd_02_1));
    CHECK(bd_01_1 != bd_02_3 && !(bd_01_1 == bd_02_3));
    CHECK(bd_01_1 != bd_26_1 && !(bd_01_1 == bd_26_1));
    CHECK(bd_01_1 != bd_26_3 && !(bd_01_1 == bd_26_3));

    CHECK(bd_01_3 != bd_00_1 && !(bd_01_3 == bd_00_1));
    CHECK(bd_01_3 != bd_00_3 && !(bd_01_3 == bd_00_3));
    CHECK(bd_01_3 != bd_01_1 && !(bd_01_3 == bd_01_1));
    CHECK(bd_01_3 == bd_01_3 && !(bd_01_3 != bd_01_3));
    CHECK(bd_01_3 != bd_02_1 && !(bd_01_3 == bd_02_1));
    CHECK(bd_01_3 != bd_02_3 && !(bd_01_3 == bd_02_3));
    CHECK(bd_01_3 != bd_26_1 && !(bd_01_3 == bd_26_1));
    CHECK(bd_01_3 != bd_26_3 && !(bd_01_3 == bd_26_3));


    CHECK(bd_02_1 != bd_00_1 && !(bd_02_1 == bd_00_1));
    CHECK(bd_02_1 != bd_00_3 && !(bd_02_1 == bd_00_3));
    CHECK(bd_02_1 != bd_01_1 && !(bd_02_1 == bd_01_1));
    CHECK(bd_02_1 != bd_01_3 && !(bd_02_1 == bd_01_3));
    CHECK(bd_02_1 == bd_02_1 && !(bd_02_1 != bd_02_1));
    CHECK(bd_02_1 == bd_02_2 && !(bd_02_1 != bd_02_2));
    CHECK(bd_02_1 != bd_02_3 && !(bd_02_1 == bd_02_3));
    CHECK(bd_02_1 != bd_26_1 && !(bd_02_1 == bd_26_1));
    CHECK(bd_02_1 != bd_26_3 && !(bd_02_1 == bd_26_3));

    CHECK(bd_02_3 != bd_00_1 && !(bd_02_3 == bd_00_1));
    CHECK(bd_02_3 != bd_00_3 && !(bd_02_3 == bd_00_3));
    CHECK(bd_02_3 != bd_01_1 && !(bd_02_3 == bd_01_1));
    CHECK(bd_02_3 != bd_01_3 && !(bd_02_3 == bd_01_3));
    CHECK(bd_02_3 != bd_02_1 && !(bd_02_3 == bd_02_1));
    CHECK(bd_02_3 == bd_02_3 && !(bd_02_3 != bd_02_3));
    CHECK(bd_02_3 != bd_26_1 && !(bd_02_3 == bd_26_1));
    CHECK(bd_02_3 != bd_26_3 && !(bd_02_3 == bd_26_3));


    CHECK(bd_26_1 != bd_00_1 && !(bd_26_1 == bd_00_1));
    CHECK(bd_26_1 != bd_00_3 && !(bd_26_1 == bd_00_3));
    CHECK(bd_26_1 != bd_01_1 && !(bd_26_1 == bd_01_1));
    CHECK(bd_26_1 != bd_01_3 && !(bd_26_1 == bd_01_3));
    CHECK(bd_26_1 != bd_02_1 && !(bd_26_1 == bd_02_1));
    CHECK(bd_26_1 != bd_02_3 && !(bd_26_1 == bd_02_3));
    CHECK(bd_26_1 == bd_26_1 && !(bd_26_1 != bd_26_1));
    CHECK(bd_26_1 == bd_26_2 && !(bd_26_1 != bd_26_2));
    CHECK(bd_26_1 != bd_26_3 && !(bd_26_1 == bd_26_3));

    CHECK(bd_26_3 != bd_00_1 && !(bd_26_3 == bd_00_1));
    CHECK(bd_26_3 != bd_00_3 && !(bd_26_3 == bd_00_3));
    CHECK(bd_26_3 != bd_01_1 && !(bd_26_3 == bd_01_1));
    CHECK(bd_26_3 != bd_01_3 && !(bd_26_3 == bd_01_3));
    CHECK(bd_26_3 != bd_02_1 && !(bd_26_3 == bd_02_1));
    CHECK(bd_26_3 != bd_02_3 && !(bd_26_3 == bd_02_3));
    CHECK(bd_26_3 != bd_26_1 && !(bd_26_3 == bd_26_1));
    CHECK(bd_26_3 == bd_26_3 && !(bd_26_3 != bd_26_3));
}


TEST(BinaryData_LexicographicCompare)
{
    // Test lexicographic ordering (<, >, <=, >=)
    char c_11 = 11;
    char c_22 = 22;
    string s_8_11(8, c_11);
    string s_8_22(8, c_22);
    string s_9_11(9, c_11);
    string s_9_22(9, c_22);
    BinaryData bd_0;
    BinaryData bd_8_11(s_8_11.data(), s_8_11.size());
    BinaryData bd_8_22(s_8_22.data(), s_8_22.size());
    BinaryData bd_9_11(s_9_11.data(), s_9_11.size());
    BinaryData bd_9_22(s_9_22.data(), s_9_22.size());

    CHECK((bd_0    >= bd_0)    && !(bd_0    <  bd_0));
    CHECK((bd_0    <= bd_0)    && !(bd_0    >  bd_0));
    CHECK((bd_0    <  bd_8_11) && !(bd_0    >= bd_8_11));
    CHECK((bd_0    <= bd_8_11) && !(bd_0    >  bd_8_11));
    CHECK((bd_0    <  bd_8_22) && !(bd_0    >= bd_8_22));
    CHECK((bd_0    <= bd_8_22) && !(bd_0    >  bd_8_22));
    CHECK((bd_0    <  bd_9_11) && !(bd_0    >= bd_9_11));
    CHECK((bd_0    <= bd_9_11) && !(bd_0    >  bd_9_11));
    CHECK((bd_0    <  bd_9_22) && !(bd_0    >= bd_9_22));
    CHECK((bd_0    <= bd_9_22) && !(bd_0    >  bd_9_22));

    CHECK((bd_8_11 >= bd_0)    && !(bd_8_11 <  bd_0));
    CHECK((bd_8_11 >  bd_0)    && !(bd_8_11 <= bd_0));
    CHECK((bd_8_11 >= bd_8_11) && !(bd_8_11 <  bd_8_11));
    CHECK((bd_8_11 <= bd_8_11) && !(bd_8_11 >  bd_8_11));
    CHECK((bd_8_11 <  bd_8_22) && !(bd_8_11 >= bd_8_22));
    CHECK((bd_8_11 <= bd_8_22) && !(bd_8_11 >  bd_8_22));
    CHECK((bd_8_11 <  bd_9_11) && !(bd_8_11 >= bd_9_11));
    CHECK((bd_8_11 <= bd_9_11) && !(bd_8_11 >  bd_9_11));
    CHECK((bd_8_11 <  bd_9_22) && !(bd_8_11 >= bd_9_22));
    CHECK((bd_8_11 <= bd_9_22) && !(bd_8_11 >  bd_9_22));

    CHECK((bd_8_22 >= bd_0)    && !(bd_8_22 <  bd_0));
    CHECK((bd_8_22 >  bd_0)    && !(bd_8_22 <= bd_0));
    CHECK((bd_8_22 >= bd_8_11) && !(bd_8_22 <  bd_8_11));
    CHECK((bd_8_22 >  bd_8_11) && !(bd_8_22 <= bd_8_11));
    CHECK((bd_8_22 >= bd_8_22) && !(bd_8_22 <  bd_8_22));
    CHECK((bd_8_22 <= bd_8_22) && !(bd_8_22 >  bd_8_22));
    CHECK((bd_8_22 >= bd_9_11) && !(bd_8_22 <  bd_9_11));
    CHECK((bd_8_22 >  bd_9_11) && !(bd_8_22 <= bd_9_11));
    CHECK((bd_8_22 <  bd_9_22) && !(bd_8_22 >= bd_9_22));
    CHECK((bd_8_22 <= bd_9_22) && !(bd_8_22 >  bd_9_22));

    CHECK((bd_9_11 >= bd_0)    && !(bd_9_11 <  bd_0));
    CHECK((bd_9_11 >  bd_0)    && !(bd_9_11 <= bd_0));
    CHECK((bd_9_11 >= bd_8_11) && !(bd_9_11 <  bd_8_11));
    CHECK((bd_9_11 >  bd_8_11) && !(bd_9_11 <= bd_8_11));
    CHECK((bd_9_11 <  bd_8_22) && !(bd_9_11 >= bd_8_22));
    CHECK((bd_9_11 <= bd_8_22) && !(bd_9_11 >  bd_8_22));
    CHECK((bd_9_11 >= bd_9_11) && !(bd_9_11 <  bd_9_11));
    CHECK((bd_9_11 <= bd_9_11) && !(bd_9_11 >  bd_9_11));
    CHECK((bd_9_11 <  bd_9_22) && !(bd_9_11 >= bd_9_22));
    CHECK((bd_9_11 <= bd_9_22) && !(bd_9_11 >  bd_9_22));

    CHECK((bd_9_22 >= bd_0)    && !(bd_9_22 <  bd_0));
    CHECK((bd_9_22 >  bd_0)    && !(bd_9_22 <= bd_0));
    CHECK((bd_9_22 >= bd_8_11) && !(bd_9_22 <  bd_8_11));
    CHECK((bd_9_22 >  bd_8_11) && !(bd_9_22 <= bd_8_11));
    CHECK((bd_9_22 >= bd_8_22) && !(bd_9_22 <  bd_8_22));
    CHECK((bd_9_22 >  bd_8_22) && !(bd_9_22 <= bd_8_22));
    CHECK((bd_9_22 >= bd_9_11) && !(bd_9_22 <  bd_9_11));
    CHECK((bd_9_22 >  bd_9_11) && !(bd_9_22 <= bd_9_11));
    CHECK((bd_9_22 >= bd_9_22) && !(bd_9_22 <  bd_9_22));
    CHECK((bd_9_22 <= bd_9_22) && !(bd_9_22 >  bd_9_22));
}


TEST(BinaryData_Subblobs)
{
    BinaryData bd_0;
    CHECK(bd_0.begins_with(bd_0));
    CHECK(bd_0.begins_with(BinaryData()));
    CHECK(bd_0.ends_with(bd_0));
    CHECK(bd_0.ends_with(BinaryData()));
    CHECK(bd_0.contains(bd_0));
    CHECK(bd_0.contains(BinaryData()));
    CHECK(!bd_0.begins_with(BinaryData("x", 1)));
    CHECK(!bd_0.ends_with(BinaryData("x", 1)));
    CHECK(!bd_0.contains(BinaryData("x", 1)));

    BinaryData sd("Minkowski", 9);
    CHECK(sd.begins_with(bd_0));
    CHECK(sd.begins_with(BinaryData()));
    CHECK(sd.begins_with(BinaryData("Min", 3)));
    CHECK(sd.ends_with(bd_0));
    CHECK(sd.ends_with(BinaryData()));
    CHECK(sd.ends_with(BinaryData("ski", 3)));
    CHECK(sd.contains(bd_0));
    CHECK(sd.contains(BinaryData()));
    CHECK(sd.contains(BinaryData("Min", 3)));
    CHECK(sd.contains(BinaryData("kow", 3)));
    CHECK(sd.contains(BinaryData("ski", 3)));
    CHECK(!sd.begins_with(BinaryData("ski", 3)));
    CHECK(!sd.ends_with(BinaryData("Min", 3)));
    CHECK(!sd.contains(BinaryData("wok", 3)));
}

#endif
