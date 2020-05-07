#include <realm/util/enum.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;

namespace {

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


enum class Color { orange, purple, brown };

struct ColorSpec {
    static EnumAssoc map[];
};
using ColorEnum = Enum<Color, ColorSpec>;

EnumAssoc ColorSpec::map[] = {
    {int(Color::orange), "orange"}, {int(Color::purple), "purple"}, {int(Color::brown), "brown"}, {0, 0}};

TEST(Util_Enum_Basics)
{
    // Write a color
    ColorEnum color = Color::purple;
    std::ostringstream out;
    out << color;

    // Read a color
    ColorEnum color_2 = Color::orange;
    std::istringstream in;
    in.str(out.str());
    if (CHECK(in >> color_2))
        CHECK_EQUAL(color, color_2);
}

} // unnamed namespace
