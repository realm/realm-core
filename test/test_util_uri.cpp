#include <realm/util/uri.hpp>

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

TEST(Util_Uri_Basics)
{
    // normal uri
    {
        const std::string input = "http://www.realm.io/foo?bar#zob";
        auto u = Uri(input);

        CHECK_EQUAL(u.get_scheme(), "http:");
        CHECK_EQUAL(u.get_auth(), "//www.realm.io");
        CHECK_EQUAL(u.get_path(), "/foo");
        CHECK_EQUAL(u.get_query(), "?bar");
        CHECK_EQUAL(u.get_frag(), "#zob");
        CHECK_EQUAL(u.recompose(), input);

        {
            std::string userinfo, host, port;
            auto result = u.get_auth(userinfo, host, port);

            CHECK(result);
            CHECK(userinfo.empty());
            CHECK(port.empty());
            CHECK_EQUAL(host, "www.realm.io");
        }
    }

    // complex authority
    {
        const std::string input = "http://myuser:mypass@www.realm.io:12345/foo?bar#zob";
        auto u = Uri(input);

        CHECK_EQUAL(u.get_scheme(), "http:");
        CHECK_EQUAL(u.get_auth(), "//myuser:mypass@www.realm.io:12345");
        CHECK_EQUAL(u.get_path(), "/foo");
        CHECK_EQUAL(u.get_query(), "?bar");
        CHECK_EQUAL(u.get_frag(), "#zob");
        CHECK_EQUAL(u.recompose(), input);

        {
            std::string userinfo, host, port;
            auto result = u.get_auth(userinfo, host, port);

            CHECK(result);
            CHECK_EQUAL(userinfo, "myuser:mypass");
            CHECK_EQUAL(host, "www.realm.io");
            CHECK_EQUAL(port, "12345");
        }
    }

    // empty authority
    {
        const std::string input = "mailto:foo@example.com";
        auto u = Uri(input);

        CHECK(u.get_auth().empty());
        CHECK_EQUAL(u.get_scheme(), "mailto:");
        CHECK_EQUAL(u.get_path(), "foo@example.com");
    }

    // empty path
    {
        const std::string input = "foo://example.com?bar";
        auto u = Uri(input);

        CHECK(u.get_path().empty());
        CHECK_EQUAL(u.get_scheme(), "foo:");
        CHECK_EQUAL(u.get_auth(), "//example.com");
        CHECK_EQUAL(u.get_query(), "?bar");
    }

    // empty setters
    {
        const std::string input = "http://www.realm.io/foo?bar#zob";
        auto u = Uri(input);

        u.set_scheme("");
        u.set_auth("");
        u.set_path("");
        u.set_query("");
        u.set_frag("");

        CHECK(u.get_scheme().empty());
        CHECK(u.get_auth().empty());
        CHECK(u.get_path().empty());
        CHECK(u.get_query().empty());
        CHECK(u.get_frag().empty());

        {
            std::string userinfo, host, port;
            auto result = u.get_auth(userinfo, host, port);

            CHECK(!result);
            CHECK(userinfo.empty() && host.empty() && port.empty());
        }
    }

    // set_scheme
    {
        auto u = Uri();

        CHECK_THROW(u.set_scheme("foo"), std::invalid_argument);
        CHECK_THROW(u.set_scheme("foo::"), std::invalid_argument);

        // FIXME: These tests are failing
        // CHECK_THROW(u.set_scheme("foo :"), std::invalid_argument);
        // CHECK_THROW(u.set_scheme("4foo:"), std::invalid_argument);
    }

    // set_auth
    {
        auto u = Uri();

        u.set_auth("//foo:foo%3A@myhost.com:123");
        u.set_auth("//foo%20bar");
        u.set_auth("//a.b.c");

        CHECK_THROW(u.set_auth("f"), std::invalid_argument);
        CHECK_THROW(u.set_auth("foo"), std::invalid_argument);
        CHECK_THROW(u.set_auth("///"), std::invalid_argument);
        CHECK_THROW(u.set_auth("//#"), std::invalid_argument);
        CHECK_THROW(u.set_auth("//?"), std::invalid_argument);
        CHECK_THROW(u.set_auth("//??"), std::invalid_argument);
        CHECK_THROW(u.set_auth("//?\?/"), std::invalid_argument);

        // FIXME: These tests are failing
        // CHECK_THROW(u.set_auth("// "), std::invalid_argument);
        // CHECK_THROW(u.set_auth("//..."), std::invalid_argument);
        // CHECK_THROW(u.set_auth("// should fail"), std::invalid_argument);
        // CHECK_THROW(u.set_auth("//123456789"), std::invalid_argument);
    }

    // set_path
    {
        auto u = Uri();

        u.set_path("/foo");
        u.set_path("//foo");
        u.set_path("foo@example.com");
        u.set_path("foo@example.com/bar");
        u.set_path("foo%20example.com/bar");

        CHECK_THROW(u.set_path("/foo#bar"), std::invalid_argument);

        // FIXME: These tests are failing
        // CHECK_THROW(u.set_path("/foo bar"), std::invalid_argument);
    }

    // set_query
    {
        auto u = Uri();

        u.set_query("?foo");
        u.set_query("?foo/bar");
        u.set_query("?foo/bar?zob");
        u.set_query("?");

        CHECK_THROW(u.set_query("/foo"), std::invalid_argument);
        CHECK_THROW(u.set_query("?foo#bar"), std::invalid_argument);
    }

    // set_frag
    {
        auto u = Uri();
        u.set_frag("#");
        u.set_frag("#foo");

        CHECK_THROW(u.set_frag("?#"), std::invalid_argument);
    }

    // canonicalize
    {
        auto u = Uri();

        u.set_scheme(":");
        u.set_auth("//");
        u.set_query("?");
        u.set_frag("#");

        u.canonicalize();

        CHECK(u.get_scheme().empty());
        CHECK(u.get_auth().empty());
        CHECK(u.get_path().empty());
        CHECK(u.get_query().empty());
        CHECK(u.get_frag().empty());
    }


    // path canonicalization
    {
        auto u = Uri();

        u.set_scheme("foo:");
        u.canonicalize();

        CHECK_EQUAL(u.get_path(), "/");
    }
}

TEST(Util_UriPercentEncoding_1)
{
    std::vector<std::string> unescaped_strings{
        "", {'A', '\0'}, "/", "abc", "def", {'\xff', '\x7f', '\x80'}, "/sync/calendar"};

    std::vector<std::string> escaped_strings{"", "A%00", "%2F", "abc", "def", "%FF%7F%80", "%2Fsync%2Fcalendar"};

    REALM_ASSERT(unescaped_strings.size() == escaped_strings.size());
    for (size_t i = 0; i < unescaped_strings.size(); ++i) {
        const std::string& unescaped = unescaped_strings[i];
        const std::string& escaped = escaped_strings[i];
        CHECK_EQUAL(uri_percent_encode(unescaped), escaped);
        CHECK_EQUAL(uri_percent_decode(escaped), unescaped);
    }
}

TEST(Util_UriPercentEncoding_2)
{
    for (int ch = 0; ch < 256; ++ch) {
        const std::string str{static_cast<char>(ch)};
        std::string escaped = uri_percent_encode(str);
        std::string unescaped = uri_percent_decode(escaped);
        CHECK_EQUAL(str, unescaped);
    }
}

TEST(Util_UriPercentEncoding_3)
{
    std::vector<std::string> invalid_escaped_strings{"/", "%", "%q", "%Aq", ">", {'\xdd'}, "%%%%", "%AG"};

    for (size_t i = 0; i < invalid_escaped_strings.size(); ++i)
        CHECK_THROW(uri_percent_decode(invalid_escaped_strings[i]), std::runtime_error);
}

} // unnamed namespace
