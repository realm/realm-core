#include "testsettings.hpp"
#ifdef TEST_BASIC_UTILS

#include <realm/alloc_slab.hpp>
#include <realm/util/file.hpp>
#include <realm/util/shared_ptr.hpp>
#include <realm/util/uri.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;

namespace {
    struct Foo {
        void func() {}
        void modify() { c = 123; }
        char c;
    };
}

TEST(Utils_SharedPtr)
{
    const SharedPtr<Foo> foo1 = new Foo();
    Foo* foo2 = foo1.get();
    static_cast<void>(foo2);

    const SharedPtr<Foo> foo3 = new Foo();
    foo3->modify();

    SharedPtr<Foo> foo4 = new Foo();
    foo4->modify();

    SharedPtr<const int> a = new int(1);
    const int* b = a.get();
    CHECK_EQUAL(1, *b);

    const SharedPtr<const int> c = new int(2);
    const int* const d = c.get();
    CHECK_EQUAL(2, *d);

    const SharedPtr<int> e = new int(3);
    const int* f = e.get();
    static_cast<void>(f);
    CHECK_EQUAL(3, *e);
    *e = 123;

    SharedPtr<int> g = new int(4);
    int* h = g.get();
    static_cast<void>(h);
    CHECK_EQUAL(4, *g);
    *g = 123;
}

TEST(Utils_Uri)
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

#endif
