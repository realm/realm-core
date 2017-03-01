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
#ifdef TEST_STRING_DATA

#include <cstring>
#include <string>
#include <sstream>

#include <realm.hpp>
#include <realm/string_data.hpp>
#include <realm/unicode.hpp>

#include "test.hpp"

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
    // Null must be strictly less-than the empty string.
    {
        StringData a = realm::null{};
        StringData b{""};
        CHECK(a < b);
        CHECK(b > a);
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
    std::string s_8_11(8, c_11);
    std::string s_8_22(8, c_22);
    std::string s_9_11(9, c_11);
    std::string s_9_22(9, c_22);
    StringData sd_0("");
    StringData sd_8_11(s_8_11);
    StringData sd_8_22(s_8_22);
    StringData sd_9_11(s_9_11);
    StringData sd_9_22(s_9_22);

    CHECK((sd_0 >= sd_0) && !(sd_0 < sd_0));
    CHECK((sd_0 <= sd_0) && !(sd_0 > sd_0));
    CHECK((sd_0 < sd_8_11) && !(sd_0 >= sd_8_11));
    CHECK((sd_0 <= sd_8_11) && !(sd_0 > sd_8_11));
    CHECK((sd_0 < sd_8_22) && !(sd_0 >= sd_8_22));
    CHECK((sd_0 <= sd_8_22) && !(sd_0 > sd_8_22));
    CHECK((sd_0 < sd_9_11) && !(sd_0 >= sd_9_11));
    CHECK((sd_0 <= sd_9_11) && !(sd_0 > sd_9_11));
    CHECK((sd_0 < sd_9_22) && !(sd_0 >= sd_9_22));
    CHECK((sd_0 <= sd_9_22) && !(sd_0 > sd_9_22));

    CHECK((sd_8_11 >= sd_0) && !(sd_8_11 < sd_0));
    CHECK((sd_8_11 > sd_0) && !(sd_8_11 <= sd_0));
    CHECK((sd_8_11 >= sd_8_11) && !(sd_8_11 < sd_8_11));
    CHECK((sd_8_11 <= sd_8_11) && !(sd_8_11 > sd_8_11));
    CHECK((sd_8_11 < sd_8_22) && !(sd_8_11 >= sd_8_22));
    CHECK((sd_8_11 <= sd_8_22) && !(sd_8_11 > sd_8_22));
    CHECK((sd_8_11 < sd_9_11) && !(sd_8_11 >= sd_9_11));
    CHECK((sd_8_11 <= sd_9_11) && !(sd_8_11 > sd_9_11));
    CHECK((sd_8_11 < sd_9_22) && !(sd_8_11 >= sd_9_22));
    CHECK((sd_8_11 <= sd_9_22) && !(sd_8_11 > sd_9_22));

    CHECK((sd_8_22 >= sd_0) && !(sd_8_22 < sd_0));
    CHECK((sd_8_22 > sd_0) && !(sd_8_22 <= sd_0));
    CHECK((sd_8_22 >= sd_8_11) && !(sd_8_22 < sd_8_11));
    CHECK((sd_8_22 > sd_8_11) && !(sd_8_22 <= sd_8_11));
    CHECK((sd_8_22 >= sd_8_22) && !(sd_8_22 < sd_8_22));
    CHECK((sd_8_22 <= sd_8_22) && !(sd_8_22 > sd_8_22));
    CHECK((sd_8_22 >= sd_9_11) && !(sd_8_22 < sd_9_11));
    CHECK((sd_8_22 > sd_9_11) && !(sd_8_22 <= sd_9_11));
    CHECK((sd_8_22 < sd_9_22) && !(sd_8_22 >= sd_9_22));
    CHECK((sd_8_22 <= sd_9_22) && !(sd_8_22 > sd_9_22));

    CHECK((sd_9_11 >= sd_0) && !(sd_9_11 < sd_0));
    CHECK((sd_9_11 > sd_0) && !(sd_9_11 <= sd_0));
    CHECK((sd_9_11 >= sd_8_11) && !(sd_9_11 < sd_8_11));
    CHECK((sd_9_11 > sd_8_11) && !(sd_9_11 <= sd_8_11));
    CHECK((sd_9_11 < sd_8_22) && !(sd_9_11 >= sd_8_22));
    CHECK((sd_9_11 <= sd_8_22) && !(sd_9_11 > sd_8_22));
    CHECK((sd_9_11 >= sd_9_11) && !(sd_9_11 < sd_9_11));
    CHECK((sd_9_11 <= sd_9_11) && !(sd_9_11 > sd_9_11));
    CHECK((sd_9_11 < sd_9_22) && !(sd_9_11 >= sd_9_22));
    CHECK((sd_9_11 <= sd_9_22) && !(sd_9_11 > sd_9_22));

    CHECK((sd_9_22 >= sd_0) && !(sd_9_22 < sd_0));
    CHECK((sd_9_22 > sd_0) && !(sd_9_22 <= sd_0));
    CHECK((sd_9_22 >= sd_8_11) && !(sd_9_22 < sd_8_11));
    CHECK((sd_9_22 > sd_8_11) && !(sd_9_22 <= sd_8_11));
    CHECK((sd_9_22 >= sd_8_22) && !(sd_9_22 < sd_8_22));
    CHECK((sd_9_22 > sd_8_22) && !(sd_9_22 <= sd_8_22));
    CHECK((sd_9_22 >= sd_9_11) && !(sd_9_22 < sd_9_11));
    CHECK((sd_9_22 > sd_9_11) && !(sd_9_22 <= sd_9_11));
    CHECK((sd_9_22 >= sd_9_22) && !(sd_9_22 < sd_9_22));
    CHECK((sd_9_22 <= sd_9_22) && !(sd_9_22 > sd_9_22));
}

TEST(StringData_Like)
{
    StringData null = realm::null();
    StringData empty("");
    StringData f("f");
    StringData foo("foo");
    StringData bar("bar");
    StringData foobar("foobar");
    StringData foofoo("foofoo");
    StringData foobarfoo("foobarfoo");
    StringData star_in_string("*bar");
    StringData unicode("\xc3\xa6\xc3\xb8\xc3\xa5\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9etest"); // utf-8 "æøå日本語test"

    CHECK(null.like(null));
    CHECK(!null.like(""));
    CHECK(!null.like("*"));
    CHECK(!null.like("?"));
    CHECK(!empty.like(null));

    CHECK(empty.like(""));
    CHECK(empty.like("*"));

    CHECK(!f.like(""));
    CHECK(f.like("*"));
    CHECK(foo.like("foo*"));
    CHECK(foo.like("*foo"));
    CHECK(foobar.like("foo*"));
    CHECK(foofoo.like("foo*foo"));
    CHECK(foobarfoo.like("foo*foo"));
    CHECK(!foobarfoo.like("foo*bar"));
    CHECK(star_in_string.like("*ar"));

    CHECK(unicode.like("*test"));
    CHECK(unicode.like("\xc3\xa6\xc3\xb8\xc3\xa5*"));              // "æøå*"
    CHECK(unicode.like("\xc3\xa6\xc3\xb8\xc3\xa5*test"));          // "æøå*test"
    CHECK(unicode.like("*\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e*")); // "*日本語*"

    CHECK(f.like("?"));
    CHECK(foo.like("?oo"));
    CHECK(foo.like("f?o"));
    CHECK(foo.like("fo?"));
    CHECK(!empty.like("?"));
    CHECK(!foo.like("foo?"));
    CHECK(!foo.like("?foo"));

    CHECK(unicode.like("?\xc3\xb8\xc3\xa5\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9etest")); // "?øå日本語test"
    CHECK(unicode.like("\xc3\xa6?\xc3\xa5\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9etest")); // "æ?å日本語test"));
    CHECK(unicode.like("\xc3\xa6\xc3\xb8\xc3\xa5?\xe6\x9c\xac\xe8\xaa\x9etest"));     // "æøå?本語test"));
    CHECK(unicode.like("\xc3\xa6?\xc3\xa5?\xe6\x9c\xac?test"));                       // "æ?å?本?t?s?"));

    CHECK(foo.like("?oo*"));
    CHECK(foo.like("*?o?"));
    CHECK(foobar.like("???*"));
    CHECK(foofoo.like("?oo*?oo"));
    CHECK(foobarfoo.like("?oo*?oo"));
    CHECK(!foobarfoo.like("*f*x*"));
}

TEST(StringData_Like_CaseInsensitive)
{
    StringData null = realm::null();
    StringData empty("");
    StringData f("f");
    StringData foo("FoO");
    StringData bar("bAr");
    StringData foobar("FOOBAR");
    StringData foofoo("FOOfoo");
    StringData foobarfoo("FoObArFoO");
    StringData star_in_string("*bar");
    StringData unicode("\xc3\xa6\xc3\xb8\xc3\xa5\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9etest"); // utf-8 "æøå日本語test"

    CHECK(string_like_ins(null, null));
    CHECK(!string_like_ins(null, ""));
    CHECK(!string_like_ins(null, "*"));
    CHECK(!string_like_ins(null, "?"));
    CHECK(!string_like_ins("", null));

    CHECK(string_like_ins(empty, ""));
    CHECK(string_like_ins(empty, "*"));

    CHECK(!string_like_ins(f, ""));
    CHECK(string_like_ins(f, "*"));
    CHECK(string_like_ins(foo, "foo*"));
    CHECK(string_like_ins(foo, "*foo"));
    CHECK(string_like_ins(foobar, "foo*"));
    CHECK(string_like_ins(foofoo, "foo*foo"));
    CHECK(string_like_ins(foobarfoo, "foo*foo"));
    CHECK(!string_like_ins(foobarfoo, "foo*bar"));
    CHECK(string_like_ins(star_in_string, "*ar"));

    CHECK(string_like_ins(unicode, "*test"));
    CHECK(string_like_ins(unicode, "\xc3\xa6\xc3\xb8\xc3\xa5*"));              // "æøå*"
    CHECK(string_like_ins(unicode, "\xc3\xa6\xc3\xb8\xc3\xa5*test"));          // "æøå*test"
    CHECK(string_like_ins(unicode, "*\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e*")); // "*日本語*"

    CHECK(string_like_ins(f, "?"));
    CHECK(string_like_ins(foo, "?oo"));
    CHECK(string_like_ins(foo, "f?o"));
    CHECK(string_like_ins(foo, "fo?"));
    CHECK(!string_like_ins(empty, "?"));
    CHECK(!string_like_ins(foo, "foo?"));
    CHECK(!string_like_ins(foo, "?foo"));

    CHECK(string_like_ins(unicode, "?\xc3\xb8\xc3\xa5\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9etest")); // "?øå日本語test"
    CHECK(string_like_ins(unicode, "\xc3\xa6?\xc3\xa5\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9etest")); // "æ?å日本語test"
    CHECK(string_like_ins(unicode, "\xc3\xa6\xc3\xb8\xc3\xa5?\xe6\x9c\xac\xe8\xaa\x9etest")); // "æøå?本語test"
    CHECK(string_like_ins(unicode, "\xc3\xa6?\xc3\xa5?\xe6\x9c\xac?test"));                   // "æ?å?本?t?s?"

    CHECK(string_like_ins(foo, "?oo*"));
    CHECK(string_like_ins(foo, "*?o?"));
    CHECK(string_like_ins(foobar, "???*"));
    CHECK(string_like_ins(foofoo, "?oo*?oo"));
    CHECK(string_like_ins(foobarfoo, "?oo*?oo"));
}

TEST(StringData_Substrings)
{
    // Reasoning behind behaviour is that if you append strings A + B then B is a suffix of a, and hence A
    // "ends with" B, and B "begins with" A. This is true even though appending a null or empty string keeps the
    // original unchanged

    StringData empty("");
    StringData null = realm::null();
    StringData data("x");

    // null.
    CHECK(null.begins_with(null));
    CHECK(!null.begins_with(empty));
    CHECK(!null.begins_with("x"));

    CHECK(null.ends_with(null));
    CHECK(!null.ends_with(empty));
    CHECK(!null.ends_with(""));
    CHECK(!null.ends_with("x"));

    CHECK(empty.begins_with(null));
    CHECK(empty.ends_with(null));

    CHECK(data.begins_with(null));
    CHECK(data.ends_with(null));

    CHECK(data.contains(null));
    CHECK(!null.contains(data));

    CHECK(empty.contains(null));
    CHECK(!empty.contains(data));

    CHECK(null.contains(null));
    CHECK(!null.contains(data));

    CHECK(!null.contains(empty));
    CHECK(empty.contains(null));

    // non-nulls
    CHECK(empty.begins_with(empty));
    CHECK(empty.begins_with(""));
    CHECK(empty.ends_with(empty));
    CHECK(empty.ends_with(""));
    CHECK(empty.contains(empty));
    CHECK(empty.contains(""));
    CHECK(!empty.begins_with("x"));
    CHECK(!empty.ends_with("x"));
    CHECK(!empty.contains("x"));
    CHECK_EQUAL("", empty.prefix(0));
    CHECK_EQUAL("", empty.suffix(0));
    CHECK_EQUAL("", empty.substr(0));
    CHECK_EQUAL("", empty.substr(0, 0));

    StringData sd("Minkowski");
    CHECK(sd.begins_with(empty));
    CHECK(sd.begins_with(""));
    CHECK(sd.begins_with("Min"));
    CHECK(sd.ends_with(empty));
    CHECK(sd.ends_with(""));
    CHECK(sd.ends_with("ski"));
    CHECK(sd.contains(empty));
    CHECK(sd.contains(""));
    CHECK(sd.contains("Min"));
    CHECK(sd.contains("kow"));
    CHECK(sd.contains("ski"));
    CHECK(!sd.begins_with("ski"));
    CHECK(!sd.ends_with("Min"));
    CHECK(!sd.contains("wok"));
    CHECK_EQUAL("Min", sd.prefix(3));
    CHECK_EQUAL("ski", sd.suffix(3));
    CHECK_EQUAL("kowski", sd.substr(3));
    CHECK_EQUAL("kow", sd.substr(3, 3));
}


TEST(StringData_STL_String)
{
    const char* pre = "hilbert";
    const char* suf_1 = "banachA";
    const char* suf_2 = "banachB";
    std::string s_1;
    s_1 += pre;
    s_1 += char(); // Null
    s_1 += suf_1;
    CHECK_EQUAL(strlen(pre) + 1 + strlen(suf_1), s_1.size());
    std::string s_2;
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
    std::string t_1(sd_1);
    CHECK_EQUAL(sd_1, t_1);
    std::string t_2;
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
    std::string s;
    s += pre;
    s += char(); // Null
    s += suf;
    StringData sd(s);
    std::ostringstream out;
    out << sd;
    CHECK_EQUAL(s, out.str());
}

#endif
