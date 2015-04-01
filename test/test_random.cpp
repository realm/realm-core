#include "testsettings.hpp"

#include <realm/util/random.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;

TEST(random_between_int64)
{
  int_fast64_t lbound = -32;
  int_fast64_t ubound = 48;
  Random r;
  for (size_t i = 0; i < 10000; ++i) {
    auto x = r.draw_int(lbound, ubound);
    CHECK(x >= lbound);
    CHECK(x <= ubound);
  }
}

// TEST(random_between_float)
// {
//   float lbound = -2.34f;
//   float ubound = 40.99999f;
//   for (size_t i = 0; i < 10000; ++i) {
//     auto x = realm::util::random_float(lbound, ubound);
//     CHECK(x >= lbound);
//     CHECK(x <= ubound);
//   }
// }
