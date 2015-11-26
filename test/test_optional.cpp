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

TEST(Optional_CopyAssignment)
{
    Optional<std::string> a { "foo" };
    Optional<std::string> b;
    b = a;
    CHECK(bool(a));
    CHECK(bool(b));
    CHECK_EQUAL(*a, "foo");
    CHECK_EQUAL(*b, "foo");

    Optional<std::string> c { "foo" };
    Optional<std::string> d { "bar" };
    d = c;
    CHECK(bool(c));
    CHECK(bool(d));
    CHECK_EQUAL(*c, "foo");
    CHECK_EQUAL(*d, "foo");

    Optional<std::string> e;
    Optional<std::string> f { "foo" };
    f = e;
    CHECK(!bool(e));
    CHECK(!bool(f));
}

TEST(Optional_MoveAssignment)
{
    Optional<std::string> a { "foo" };
    Optional<std::string> b;
    b = std::move(a);
    CHECK(bool(a));
    CHECK(bool(b));
    CHECK_EQUAL(*b, "foo");

    Optional<std::string> c { "foo" };
    Optional<std::string> d { "bar" };
    d = std::move(c);
    CHECK(bool(c));
    CHECK(bool(d));
    CHECK_EQUAL(*d, "foo");

    Optional<std::string> e;
    Optional<std::string> f { "foo" };
    f = std::move(e);
    CHECK(!bool(e));
    CHECK(!bool(f));
}

TEST(Optional_ValueAssignment)
{
    Optional<std::string> o;
    o = std::string { "foo" };
    CHECK(bool(o));
    CHECK_EQUAL(*o, "foo");

    o = std::string { "bar" };
    CHECK(bool(o));
    CHECK_EQUAL(*o, "bar");
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
    if (x) {
        x.value() = 123;
    }
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
        static_cast<void>(ii);
        REALM_ASSERT(&ii == &global_i);
    }

    void operator=(const int& ii)
    {
        static_cast<void>(ii);
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

TEST(Optional_ValueDoesntGenerateWarning)
{
    // Shouldn't generate any warnings:
    const Optional<int> i { 1 };
    CHECK(*i);
    int one = 1;
    const Optional<int&> ii { one };
    CHECK(*ii);
}

TEST(Optional_ConstExpr) {
    // Should compile:
    constexpr Optional<int> a;
    constexpr Optional<int> b { none };
    constexpr Optional<int> c { 1 };
    CHECK_EQUAL(bool(c), true);
    constexpr int d = *c;
    CHECK_EQUAL(1, d);
    constexpr bool e { Optional<int>{ 1 } };
    CHECK_EQUAL(true, e);
    constexpr bool f { Optional<int>{ none } };
    CHECK_EQUAL(false, f);
    constexpr int g = b.value_or(1234);
    CHECK_EQUAL(1234, g);
}

// FIXME: Visual Studio 2015's constexpr support isn't sufficient to allow Optional<T&> to compile.
#ifndef _WIN32
TEST(Optional_ReferenceConstExpr) {
    // Should compile:
    constexpr Optional<const int&> a;
    constexpr Optional<const int&> b { none };
    constexpr Optional<const int&> c { global_i };
    CHECK_EQUAL(bool(c), true);
    constexpr int d = *c;
    CHECK_EQUAL(0, d);
    constexpr bool e { Optional<const int&>{ global_i } };
    CHECK_EQUAL(true, e);
    constexpr bool f { Optional<const int&>{ none } };
    CHECK_EQUAL(false, f);
}
#endif

// Disabled for compliance with std::optional
// TEST(Optional_VoidIsEquivalentToBool)
// {
//     auto a = some<void>();
//     CHECK_EQUAL(sizeof(a), sizeof(bool));
//     CHECK(a);
//     Optional<void> b = none;
//     CHECK_EQUAL(sizeof(b), sizeof(bool));
//     CHECK(!b);
// }
