#include "testsettings.hpp"

#include <realm/util/optional.hpp>

#include "test.hpp"

using namespace realm::util;

TEST(Optional_DefaultConstructor)
{
    Optional<int> x{};
    CHECK(!bool(x));
}

TEST(Optional_NoneConstructor)
{
    Optional<int> x{realm::none};
    CHECK(!bool(x));
}

TEST(Optional_ValueConstructor)
{
    Optional<std::string> a { "foo" };
    CHECK(bool(a));
}

TEST(Optional_MoveConstructor)
{
    Optional<std::string> a { "foo" };
    Optional<std::string> b { std::move(a) };
    CHECK(bool(a));
    CHECK(bool(b));
    CHECK_EQUAL(*a, "");
    CHECK_EQUAL(*b, "foo");
}

TEST(Optional_CopyConstructor)
{
    Optional<std::string> a { "foo" };
    Optional<std::string> b { a };
    CHECK(bool(a));
    CHECK(bool(b));
    CHECK_EQUAL(*a, "foo");
    CHECK_EQUAL(*b, "foo");
}

TEST(Optional_MoveValueConstructor)
{
    std::string a = "foo";
    Optional<std::string> b { std::move(a) };
    CHECK(bool(b));
    CHECK_EQUAL(*b, "foo");
    CHECK_EQUAL(a, "");
}

struct SetBooleanOnDestroy {
    bool& m_b;
    explicit SetBooleanOnDestroy(bool& b) : m_b(b) {}
    ~SetBooleanOnDestroy() { m_b = true; }
};

TEST(Optional_Destructor)
{
    bool b = false;
    {
        Optional<SetBooleanOnDestroy> x { SetBooleanOnDestroy(b) };
    }
    CHECK(b);
}

TEST(Optional_DestroyOnAssignNone)
{
    bool b = false;
    {
        Optional<SetBooleanOnDestroy> x { SetBooleanOnDestroy(b) };
        x = realm::none;
        CHECK(b);
    }
    CHECK(b);
}

TEST(Optional_References)
{
    int n = 0;
    Optional<int&> x { n };
    fmap(x, [&](int& y) {
        y = 123;
    });
    CHECK(x);
    CHECK_EQUAL(x.value(), 123);
    x = realm::none;
    CHECK(!x);
}

TEST(Optional_PolymorphicReferences)
{
    struct Foo {
        virtual ~Foo() {}
    };
    struct Bar: Foo {
        virtual ~Bar() {}
    };

    Bar bar;
    Optional<Bar&> bar_ref { bar };
    Optional<Foo&> foo_ref { bar_ref };
    CHECK(foo_ref);
    CHECK_EQUAL(&foo_ref.value(), &bar);
 }

namespace {

int make_rvalue()
{
    return 1;
}

}

TEST(Optional_RvalueReferences)
{
    // Should compile:
    const int foo = 1;
    Optional<const int&> x{foo};
    static_cast<void>(x);

    
    static_cast<void>(make_rvalue);
    // Should not compile (would generate references to temporaries):
    // Optional<const int&> y{1};
    // Optional<const int&> z = 1;
    // Optional<const int&> w = make_rvalue();
}

namespace {

/// See:
/// http://www.boost.org/doc/libs/1_57_0/libs/optional/doc/html/boost_optional/dependencies_and_portability/optional_reference_binding.html

const int global_i = 0;

struct TestingReferenceBinding {
    TestingReferenceBinding(const int& ii)
    {
        REALM_ASSERT(&ii == &global_i);
    }

    void operator=(const int& ii)
    {
        REALM_ASSERT(&ii == &global_i);
    }

    void operator=(int&&)
    {
        REALM_ASSERT(false);
    }
};
}

TEST(Optional_ReferenceBinding)
{
    const int& iref = global_i;
    CHECK_EQUAL(&iref, &global_i);
    TestingReferenceBinding ttt = global_i;
    ttt = global_i;
    TestingReferenceBinding ttt2 = iref;
    ttt2 = iref;
}

TEST(Optional_VoidIsEquivalentToBool)
{
    auto a = some<void>();
    CHECK_EQUAL(sizeof(a), sizeof(bool));
    CHECK(a);
    Optional<void> b = none;
    CHECK_EQUAL(sizeof(b), sizeof(bool));
    CHECK(!b);
}

TEST(Optional_fmap)
{
    Optional<int> a { 123 };
    bool a_called = false;
    auto ar = fmap(a, [&](int) {
        a_called = true;
    });
    CHECK(a_called);
    CHECK(ar);

    Optional<int> b { 123 };
    auto bs = fmap(b, [](int foo) {
        std::stringstream ss;
        ss << foo;
        return ss.str();
    });
    CHECK(bs);
    CHECK_EQUAL(*bs, "123");

    Optional<int> c;
    Optional<int> cx = fmap(c, [](int) { return 0; });
    CHECK(!cx);
}

TEST(Optional_StreamingMap)
{
    Optional<int> a { 123 };

    auto result = a
        >> [](int b) { return b + 200; }
        >> [](int c) {
            std::ostringstream os;
            os << c;
            return os.str();
        };
    CHECK(result);
    CHECK_EQUAL(result.value(), "323");

    Optional<int> b { 500 };
    Optional<int> result2 = b
        >> [](int x) { return x > 300 ? some<int>(x + 300) : none; };
    CHECK(result2);
}
