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
#ifdef TEST_UTIL_TO_STRING

#include <realm/util/to_string.hpp>

#include <realm/column_type.hpp>
#include <realm/data_type.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;

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

TEST(ToString_Basic)
{
    CHECK_EQUAL(to_string(false), "false");
    CHECK_EQUAL(to_string(true), "true");
    CHECK_EQUAL(to_string(static_cast<signed char>(-1)), "-1");
    CHECK_EQUAL(to_string(static_cast<unsigned char>(255)), "255");
    CHECK_EQUAL(to_string(-1), "-1");
    CHECK_EQUAL(to_string(0xFFFF0000U), "4294901760");
    CHECK_EQUAL(to_string(-1L), "-1");
    CHECK_EQUAL(to_string(0xFFFF0000UL), "4294901760");
    CHECK_EQUAL(to_string(-1LL), "-1");
    CHECK_EQUAL(to_string(0xFFFF0000ULL), "4294901760");
    CHECK_EQUAL(to_string("Foo"), "\"Foo\"");

    {
        std::ostringstream ostr;
        Printable::print_all(ostr, {0, true, "Hello"}, false);
        std::string s = ostr.str();
        CHECK_EQUAL(s, " [0, true, Hello]");
    }

    {
        std::ostringstream ostr;
        Printable::print_all(ostr, {0, true, "Hello"}, true);
        std::string s = ostr.str();
        CHECK_EQUAL(s, " [0, true, \"Hello\"]");
    }

    {
        std::ostringstream ostr;
        Printable::print_all(ostr, {}, false);
        std::string s = ostr.str();
        CHECK_EQUAL(s, "");
    }
}

TEST(ToString_DataTypes)
{
#define CHECK_ENUM(type) CHECK_EQUAL(to_string(type), "\"" #type "\"")
    CHECK_ENUM(type_Int);
    CHECK_ENUM(type_Bool);
    CHECK_ENUM(type_String);
    CHECK_ENUM(type_Binary);
    CHECK_ENUM(type_Mixed);
    CHECK_ENUM(type_Timestamp);
    CHECK_ENUM(type_Float);
    CHECK_ENUM(type_Double);
    CHECK_ENUM(type_Decimal);
    CHECK_ENUM(type_Link);
    CHECK_ENUM(type_LinkList);
    CHECK_ENUM(type_ObjectId);
    CHECK_ENUM(type_TypedLink);
    CHECK_ENUM(type_UUID);
    CHECK_ENUM(type_OldTable);
    CHECK_ENUM(type_OldDateTime);
    CHECK_ENUM(type_TypeOfValue);

    CHECK_ENUM(col_type_Int);
    CHECK_ENUM(col_type_Bool);
    CHECK_ENUM(col_type_String);
    CHECK_ENUM(col_type_Binary);
    CHECK_ENUM(col_type_Mixed);
    CHECK_ENUM(col_type_Timestamp);
    CHECK_ENUM(col_type_Float);
    CHECK_ENUM(col_type_Double);
    CHECK_ENUM(col_type_Decimal);
    CHECK_ENUM(col_type_Link);
    CHECK_ENUM(col_type_LinkList);
    CHECK_ENUM(col_type_BackLink);
    CHECK_ENUM(col_type_ObjectId);
    CHECK_ENUM(col_type_TypedLink);
    CHECK_ENUM(col_type_UUID);
    CHECK_ENUM(col_type_OldStringEnum);
    CHECK_ENUM(col_type_OldTable);
    CHECK_ENUM(col_type_OldDateTime);
}

struct StreamableType {
};
std::ostream& operator<<(std::ostream& os, StreamableType)
{
    return os << "Custom streamable type";
}

TEST(ToString_OStreamFallback)
{
    CHECK_EQUAL(to_string(StreamableType()), "Custom streamable type");
}

struct ImplicitlyConvertibleToPrintable {
    operator util::Printable() const noexcept
    {
        return "printable";
    }
};
struct ExplicitlyConvertibleToPrintable {
    explicit operator util::Printable() const noexcept
    {
        return "printable";
    }
};
struct ExplicitlyConvertibleToPrintableAndStreamable {
    explicit operator util::Printable() const noexcept
    {
        return "conversion to printable";
    }
};
std::ostream& operator<<(std::ostream&, const ExplicitlyConvertibleToPrintableAndStreamable&)
{
    throw std::runtime_error("called stream operator instead of conversion");
}

TEST(ToString_ConversionsToPrintable)
{
    CHECK_EQUAL(util::format("%1", ImplicitlyConvertibleToPrintable()), "printable");
    CHECK_EQUAL(util::format("%1", ExplicitlyConvertibleToPrintable()), "printable");
    CHECK_EQUAL(util::format("%1", ExplicitlyConvertibleToPrintableAndStreamable()), "conversion to printable");
}

#endif
