#include "testsettings.hpp"
#ifdef TEST_UTIL_STRINGBUFFER

#include <realm/util/string_buffer.hpp>

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

TEST(Utils_StringBuffer)
{
    // str() on empty sb
    {
        StringBuffer sb;

        std::string s = sb.str();
        CHECK_EQUAL(s.size(), 0);
    }

    // str() on sb with data
    {
        StringBuffer sb;
        sb.append("foo");

        std::string s = sb.str();
        CHECK_EQUAL(s.size(), 3);
        CHECK_EQUAL(s.size(), sb.size());
        CHECK_EQUAL(s, "foo");
    }

    // data() on empty sb
    {
        StringBuffer sb;

        CHECK(sb.data() == nullptr);
    }

    // data() on sb with data
    {
        StringBuffer sb;
        sb.append("foo");

        CHECK(sb.data() != nullptr);
    }

    // c_str() on empty sb
    {
        StringBuffer sb;

        CHECK(sb.c_str() != nullptr);
        CHECK(sb.c_str() == sb.c_str());
        CHECK_EQUAL(strlen(sb.c_str()), 0);
    }

    // c_str() on sb with data
    {
        StringBuffer sb;
        sb.append("foo");

        CHECK(sb.c_str() != nullptr);
        CHECK(sb.c_str() == sb.c_str());
        CHECK_EQUAL(strlen(sb.c_str()), 3);
    }

    // append_c_str()
    {
        StringBuffer sb;
        sb.append_c_str("foo");

        CHECK(sb.size() == 3);
        CHECK(sb.str().size() == 3);
        CHECK(sb.str() == "foo");
    }

    // clear()
    {
        StringBuffer sb;

        sb.clear();
        CHECK(sb.size() == 0);

        sb.append_c_str("foo");

        CHECK(sb.size() == 3);

        sb.clear();

        CHECK(sb.size() == 0);
        CHECK(sb.str().size() == 0);
        CHECK(sb.str() == "");
    }

    // resize()
    {
        // size reduction
        {
            StringBuffer sb;
            sb.append_c_str("foo");
            sb.resize(1);

            CHECK(sb.size() == 1);
            CHECK(sb.str() == "f");
        }

        // size increase
        {
            StringBuffer sb;
            sb.append_c_str("foo");
            sb.resize(10);

            CHECK(sb.size() == 10);
            CHECK(sb.str().size() == 10);
        }
    }

    // overflow detection
    {
        StringBuffer sb;
        sb.append("foo");
        CHECK_THROW(sb.append("foo", static_cast<size_t>(-1)), BufferSizeOverflow);
        CHECK_THROW(sb.reserve(static_cast<size_t>(-1)), BufferSizeOverflow);
    }
}

#endif
