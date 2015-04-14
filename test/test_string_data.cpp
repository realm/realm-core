#include "testsettings.hpp"
#ifdef TEST_STRING_DATA

#include <cstring>
#include <string>
#include <sstream>

#include <realm.hpp>
#include <realm/string_data.hpp>

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


TEST(StringData_Null)
{
    // A default constructed reference must be a null reference.
    {
        StringData sd;
        CHECK(!sd);
        CHECK(sd.is_null());
    }
    // When constructed from the empty string literal, it must not be
    // a null reference.
    {
        StringData sd("");
        CHECK(sd);
        CHECK(!sd.is_null());
    }
}


TEST(StringData_Equal)
{
    // Test operator==() and operator!=()
    StringData sd_00_1("");
    StringData sd_00_2("");
    StringData sd_00_3("");
    StringData sd_01_1("x");
    StringData sd_01_2("x");
    StringData sd_01_3("y");
    StringData sd_02_1("xy");
    StringData sd_02_2("xy");
    StringData sd_02_3("yz");
    StringData sd_26_1("abcdefghijklmnopqrstuvwxyz");
    StringData sd_26_2("abcdefghijklmnopqrstuvwxyz");
    StringData sd_26_3("ABCDEFGHIJKLMNOPQRSTUVWXYZ");

    CHECK(sd_00_1 == sd_00_1 && !(sd_00_1 != sd_00_1));
    CHECK(sd_00_1 == sd_00_2 && !(sd_00_1 != sd_00_2));
    CHECK(sd_00_1 == sd_00_3 && !(sd_00_1 != sd_00_3));
    CHECK(sd_00_1 != sd_01_1 && !(sd_00_1 == sd_01_1));
    CHECK(sd_00_1 != sd_01_3 && !(sd_00_1 == sd_01_3));
    CHECK(sd_00_1 != sd_02_1 && !(sd_00_1 == sd_02_1));
    CHECK(sd_00_1 != sd_02_3 && !(sd_00_1 == sd_02_3));
    CHECK(sd_00_1 != sd_26_1 && !(sd_00_1 == sd_26_1));
    CHECK(sd_00_1 != sd_26_3 && !(sd_00_1 == sd_26_3));

    CHECK(sd_00_3 == sd_00_1 && !(sd_00_3 != sd_00_1));
    CHECK(sd_00_3 == sd_00_3 && !(sd_00_3 != sd_00_3));
    CHECK(sd_00_3 != sd_01_1 && !(sd_00_3 == sd_01_1));
    CHECK(sd_00_3 != sd_01_3 && !(sd_00_3 == sd_01_3));
    CHECK(sd_00_3 != sd_02_1 && !(sd_00_3 == sd_02_1));
    CHECK(sd_00_3 != sd_02_3 && !(sd_00_3 == sd_02_3));
    CHECK(sd_00_3 != sd_26_1 && !(sd_00_3 == sd_26_1));
    CHECK(sd_00_3 != sd_26_3 && !(sd_00_3 == sd_26_3));


    CHECK(sd_01_1 != sd_00_1 && !(sd_01_1 == sd_00_1));
    CHECK(sd_01_1 != sd_00_3 && !(sd_01_1 == sd_00_3));
    CHECK(sd_01_1 == sd_01_1 && !(sd_01_1 != sd_01_1));
    CHECK(sd_01_1 == sd_01_2 && !(sd_01_1 != sd_01_2));
    CHECK(sd_01_1 != sd_01_3 && !(sd_01_1 == sd_01_3));
    CHECK(sd_01_1 != sd_02_1 && !(sd_01_1 == sd_02_1));
    CHECK(sd_01_1 != sd_02_3 && !(sd_01_1 == sd_02_3));
    CHECK(sd_01_1 != sd_26_1 && !(sd_01_1 == sd_26_1));
    CHECK(sd_01_1 != sd_26_3 && !(sd_01_1 == sd_26_3));

    CHECK(sd_01_3 != sd_00_1 && !(sd_01_3 == sd_00_1));
    CHECK(sd_01_3 != sd_00_3 && !(sd_01_3 == sd_00_3));
    CHECK(sd_01_3 != sd_01_1 && !(sd_01_3 == sd_01_1));
    CHECK(sd_01_3 == sd_01_3 && !(sd_01_3 != sd_01_3));
    CHECK(sd_01_3 != sd_02_1 && !(sd_01_3 == sd_02_1));
    CHECK(sd_01_3 != sd_02_3 && !(sd_01_3 == sd_02_3));
    CHECK(sd_01_3 != sd_26_1 && !(sd_01_3 == sd_26_1));
    CHECK(sd_01_3 != sd_26_3 && !(sd_01_3 == sd_26_3));


    CHECK(sd_02_1 != sd_00_1 && !(sd_02_1 == sd_00_1));
    CHECK(sd_02_1 != sd_00_3 && !(sd_02_1 == sd_00_3));
    CHECK(sd_02_1 != sd_01_1 && !(sd_02_1 == sd_01_1));
    CHECK(sd_02_1 != sd_01_3 && !(sd_02_1 == sd_01_3));
    CHECK(sd_02_1 == sd_02_1 && !(sd_02_1 != sd_02_1));
    CHECK(sd_02_1 == sd_02_2 && !(sd_02_1 != sd_02_2));
    CHECK(sd_02_1 != sd_02_3 && !(sd_02_1 == sd_02_3));
    CHECK(sd_02_1 != sd_26_1 && !(sd_02_1 == sd_26_1));
    CHECK(sd_02_1 != sd_26_3 && !(sd_02_1 == sd_26_3));

    CHECK(sd_02_3 != sd_00_1 && !(sd_02_3 == sd_00_1));
    CHECK(sd_02_3 != sd_00_3 && !(sd_02_3 == sd_00_3));
    CHECK(sd_02_3 != sd_01_1 && !(sd_02_3 == sd_01_1));
    CHECK(sd_02_3 != sd_01_3 && !(sd_02_3 == sd_01_3));
    CHECK(sd_02_3 != sd_02_1 && !(sd_02_3 == sd_02_1));
    CHECK(sd_02_3 == sd_02_3 && !(sd_02_3 != sd_02_3));
    CHECK(sd_02_3 != sd_26_1 && !(sd_02_3 == sd_26_1));
    CHECK(sd_02_3 != sd_26_3 && !(sd_02_3 == sd_26_3));


    CHECK(sd_26_1 != sd_00_1 && !(sd_26_1 == sd_00_1));
    CHECK(sd_26_1 != sd_00_3 && !(sd_26_1 == sd_00_3));
    CHECK(sd_26_1 != sd_01_1 && !(sd_26_1 == sd_01_1));
    CHECK(sd_26_1 != sd_01_3 && !(sd_26_1 == sd_01_3));
    CHECK(sd_26_1 != sd_02_1 && !(sd_26_1 == sd_02_1));
    CHECK(sd_26_1 != sd_02_3 && !(sd_26_1 == sd_02_3));
    CHECK(sd_26_1 == sd_26_1 && !(sd_26_1 != sd_26_1));
    CHECK(sd_26_1 == sd_26_2 && !(sd_26_1 != sd_26_2));
    CHECK(sd_26_1 != sd_26_3 && !(sd_26_1 == sd_26_3));

    CHECK(sd_26_3 != sd_00_1 && !(sd_26_3 == sd_00_1));
    CHECK(sd_26_3 != sd_00_3 && !(sd_26_3 == sd_00_3));
    CHECK(sd_26_3 != sd_01_1 && !(sd_26_3 == sd_01_1));
    CHECK(sd_26_3 != sd_01_3 && !(sd_26_3 == sd_01_3));
    CHECK(sd_26_3 != sd_02_1 && !(sd_26_3 == sd_02_1));
    CHECK(sd_26_3 != sd_02_3 && !(sd_26_3 == sd_02_3));
    CHECK(sd_26_3 != sd_26_1 && !(sd_26_3 == sd_26_1));
    CHECK(sd_26_3 == sd_26_3 && !(sd_26_3 != sd_26_3));
}


TEST(StringData_LexicographicCompare)
{
    // Test lexicographic ordering (<, >, <=, >=)
    char c_11 = 11;
    char c_22 = 22;
    string s_8_11(8, c_11);
    string s_8_22(8, c_22);
    string s_9_11(9, c_11);
    string s_9_22(9, c_22);
    StringData sd_0("");
    StringData sd_8_11(s_8_11);
    StringData sd_8_22(s_8_22);
    StringData sd_9_11(s_9_11);
    StringData sd_9_22(s_9_22);

    CHECK((sd_0    >= sd_0)    && !(sd_0    <  sd_0));
    CHECK((sd_0    <= sd_0)    && !(sd_0    >  sd_0));
    CHECK((sd_0    <  sd_8_11) && !(sd_0    >= sd_8_11));
    CHECK((sd_0    <= sd_8_11) && !(sd_0    >  sd_8_11));
    CHECK((sd_0    <  sd_8_22) && !(sd_0    >= sd_8_22));
    CHECK((sd_0    <= sd_8_22) && !(sd_0    >  sd_8_22));
    CHECK((sd_0    <  sd_9_11) && !(sd_0    >= sd_9_11));
    CHECK((sd_0    <= sd_9_11) && !(sd_0    >  sd_9_11));
    CHECK((sd_0    <  sd_9_22) && !(sd_0    >= sd_9_22));
    CHECK((sd_0    <= sd_9_22) && !(sd_0    >  sd_9_22));

    CHECK((sd_8_11 >= sd_0)    && !(sd_8_11 <  sd_0));
    CHECK((sd_8_11 >  sd_0)    && !(sd_8_11 <= sd_0));
    CHECK((sd_8_11 >= sd_8_11) && !(sd_8_11 <  sd_8_11));
    CHECK((sd_8_11 <= sd_8_11) && !(sd_8_11 >  sd_8_11));
    CHECK((sd_8_11 <  sd_8_22) && !(sd_8_11 >= sd_8_22));
    CHECK((sd_8_11 <= sd_8_22) && !(sd_8_11 >  sd_8_22));
    CHECK((sd_8_11 <  sd_9_11) && !(sd_8_11 >= sd_9_11));
    CHECK((sd_8_11 <= sd_9_11) && !(sd_8_11 >  sd_9_11));
    CHECK((sd_8_11 <  sd_9_22) && !(sd_8_11 >= sd_9_22));
    CHECK((sd_8_11 <= sd_9_22) && !(sd_8_11 >  sd_9_22));

    CHECK((sd_8_22 >= sd_0)    && !(sd_8_22 <  sd_0));
    CHECK((sd_8_22 >  sd_0)    && !(sd_8_22 <= sd_0));
    CHECK((sd_8_22 >= sd_8_11) && !(sd_8_22 <  sd_8_11));
    CHECK((sd_8_22 >  sd_8_11) && !(sd_8_22 <= sd_8_11));
    CHECK((sd_8_22 >= sd_8_22) && !(sd_8_22 <  sd_8_22));
    CHECK((sd_8_22 <= sd_8_22) && !(sd_8_22 >  sd_8_22));
    CHECK((sd_8_22 >= sd_9_11) && !(sd_8_22 <  sd_9_11));
    CHECK((sd_8_22 >  sd_9_11) && !(sd_8_22 <= sd_9_11));
    CHECK((sd_8_22 <  sd_9_22) && !(sd_8_22 >= sd_9_22));
    CHECK((sd_8_22 <= sd_9_22) && !(sd_8_22 >  sd_9_22));

    CHECK((sd_9_11 >= sd_0)    && !(sd_9_11 <  sd_0));
    CHECK((sd_9_11 >  sd_0)    && !(sd_9_11 <= sd_0));
    CHECK((sd_9_11 >= sd_8_11) && !(sd_9_11 <  sd_8_11));
    CHECK((sd_9_11 >  sd_8_11) && !(sd_9_11 <= sd_8_11));
    CHECK((sd_9_11 <  sd_8_22) && !(sd_9_11 >= sd_8_22));
    CHECK((sd_9_11 <= sd_8_22) && !(sd_9_11 >  sd_8_22));
    CHECK((sd_9_11 >= sd_9_11) && !(sd_9_11 <  sd_9_11));
    CHECK((sd_9_11 <= sd_9_11) && !(sd_9_11 >  sd_9_11));
    CHECK((sd_9_11 <  sd_9_22) && !(sd_9_11 >= sd_9_22));
    CHECK((sd_9_11 <= sd_9_22) && !(sd_9_11 >  sd_9_22));

    CHECK((sd_9_22 >= sd_0)    && !(sd_9_22 <  sd_0));
    CHECK((sd_9_22 >  sd_0)    && !(sd_9_22 <= sd_0));
    CHECK((sd_9_22 >= sd_8_11) && !(sd_9_22 <  sd_8_11));
    CHECK((sd_9_22 >  sd_8_11) && !(sd_9_22 <= sd_8_11));
    CHECK((sd_9_22 >= sd_8_22) && !(sd_9_22 <  sd_8_22));
    CHECK((sd_9_22 >  sd_8_22) && !(sd_9_22 <= sd_8_22));
    CHECK((sd_9_22 >= sd_9_11) && !(sd_9_22 <  sd_9_11));
    CHECK((sd_9_22 >  sd_9_11) && !(sd_9_22 <= sd_9_11));
    CHECK((sd_9_22 >= sd_9_22) && !(sd_9_22 <  sd_9_22));
    CHECK((sd_9_22 <= sd_9_22) && !(sd_9_22 >  sd_9_22));
}


TEST(StringData_Substrings)
{
    // Reasoning behind behaviour is that if you append strings A + B then B is a suffix of a, and hence A
    // "ends with" B, and B "begins with" A. This is true even though appending a null or empty string keeps the 
    // original unchanged

    StringData sd_0("");
    StringData ns = realm::null();
    StringData data("x");

    // null.
    CHECK(ns.begins_with(ns));
    CHECK(ns.begins_with(sd_0));
    CHECK(ns.begins_with(""));
    CHECK(!ns.begins_with("x"));

    CHECK(ns.ends_with(ns));
    CHECK(ns.ends_with(sd_0));
    CHECK(ns.ends_with(""));
    CHECK(!ns.ends_with("x"));

    CHECK(sd_0.begins_with(ns));
    CHECK(sd_0.ends_with(ns));

    CHECK(data.begins_with(ns));
    CHECK(data.ends_with(ns));

    CHECK(data.contains(ns));
    CHECK(!ns.contains(data));

    CHECK(sd_0.contains(ns));
    CHECK(!sd_0.contains(data));

    CHECK(ns.contains(ns));
    CHECK(!ns.contains(data));

    CHECK(ns.contains(sd_0));
    CHECK(sd_0.contains(ns));

    // non-nulls
    CHECK(sd_0.begins_with(sd_0));
    CHECK(sd_0.begins_with(""));
    CHECK(sd_0.ends_with(sd_0));
    CHECK(sd_0.ends_with(""));
    CHECK(sd_0.contains(sd_0));
    CHECK(sd_0.contains(""));
    CHECK(!sd_0.begins_with("x"));
    CHECK(!sd_0.ends_with("x"));
    CHECK(!sd_0.contains("x"));
    CHECK_EQUAL("", sd_0.prefix(0));
    CHECK_EQUAL("", sd_0.suffix(0));
    CHECK_EQUAL("", sd_0.substr(0));
    CHECK_EQUAL("", sd_0.substr(0,0));

    StringData sd("Minkowski");
    CHECK(sd.begins_with(sd_0));
    CHECK(sd.begins_with(""));
    CHECK(sd.begins_with("Min"));
    CHECK(sd.ends_with(sd_0));
    CHECK(sd.ends_with(""));
    CHECK(sd.ends_with("ski"));
    CHECK(sd.contains(sd_0));
    CHECK(sd.contains(""));
    CHECK(sd.contains("Min"));
    CHECK(sd.contains("kow"));
    CHECK(sd.contains("ski"));
    CHECK(!sd.begins_with("ski"));
    CHECK(!sd.ends_with("Min"));
    CHECK(!sd.contains("wok"));
    CHECK_EQUAL("Min",    sd.prefix(3));
    CHECK_EQUAL("ski",    sd.suffix(3));
    CHECK_EQUAL("kowski", sd.substr(3));
    CHECK_EQUAL("kow",    sd.substr(3,3));
}


TEST(StringData_STL_String)
{
    const char* pre = "hilbert";
    const char* suf_1 = "banachA";
    const char* suf_2 = "banachB";
    string s_1;
    s_1 += pre;
    s_1 += char(); // Null
    s_1 += suf_1;
    CHECK_EQUAL(strlen(pre) + 1 + strlen(suf_1), s_1.size());
    string s_2;
    s_2 += pre;
    s_2 += char(); // Null
    s_2 += suf_2;
    CHECK_EQUAL(strlen(pre) + 1 + strlen(suf_2), s_2.size());
    CHECK(s_1 != s_2);
    StringData sd_1(s_1);
    CHECK_EQUAL(s_1, sd_1);
    // Check assignment too
    StringData sd_2;
    sd_2 = s_2;
    CHECK_EQUAL(s_2, sd_2);
    CHECK(sd_1 != sd_2);
    string t_1(sd_1);
    CHECK_EQUAL(sd_1, t_1);
    string t_2;
    t_2 = sd_2;
    CHECK_EQUAL(sd_2, t_2);
    CHECK(sd_1 != sd_2);
    CHECK_EQUAL(s_1, t_1);
    CHECK_EQUAL(s_2, t_2);
}


TEST(StringData_STL_Stream)
{
    const char* pre = "hilbert";
    const char* suf = "banach";
    string s;
    s += pre;
    s += char(); // Null
    s += suf;
    StringData sd(s);
    ostringstream out;
    out << sd;
    CHECK_EQUAL(s, out.str());
}

#endif
