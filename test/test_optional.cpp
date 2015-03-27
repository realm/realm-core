#include "testsettings.hpp"

#include <realm/util/optional.hpp>

#include "test.hpp"

using namespace realm::util;

TEST(Optional_DefaultConstructor) {
  Optional<int> x{};
  CHECK(!bool(x));
}

TEST(Optional_NoneConstructor) {
  Optional<int> x{realm::none};
  CHECK(!bool(x));
}

TEST(Optional_ValueConstructor) {
  Optional<std::string> a { "foo" };
  CHECK(bool(a));
}

TEST(Optional_MoveConstructor) {
  Optional<std::string> a { "foo" };
  Optional<std::string> b { std::move(a) };
  CHECK(bool(a));
  CHECK(bool(b));
  CHECK_EQUAL(*a, "");
  CHECK_EQUAL(*b, "foo");
}

TEST(Optional_CopyConstructor) {
  Optional<std::string> a { "foo" };
  Optional<std::string> b { a };
  CHECK(bool(a));
  CHECK(bool(b));
  CHECK_EQUAL(*a, "foo");
  CHECK_EQUAL(*b, "foo");
}

TEST(Optional_MoveValueConstructor) {
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

TEST(Optional_Destructor) {
  bool b = false;
  {
    Optional<SetBooleanOnDestroy> x { SetBooleanOnDestroy(b) };
  }
  CHECK(b);
}

TEST(Optional_DestroyOnAssignNone) {
  bool b = false;
  {
    Optional<SetBooleanOnDestroy> x { SetBooleanOnDestroy(b) };
    x = realm::none;
    CHECK(b);
  }
  CHECK(b);
}

// TEST(Optional_References) {
//   int n;
//   Optional<int&> x { n };
// }

TEST(Optional_fmap) {
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
