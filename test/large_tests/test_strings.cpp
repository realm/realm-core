#include <tightdb/column.hpp>

#include "../util/number_names.hpp"

#include "../testsettings.hpp"
#include "../test.hpp"

#include "verified_string.hpp"

#if TEST_DURATION > 0

using namespace std;
using namespace tightdb;
using namespace tightdb::test_util;

namespace {

string randstring(Random& random)
{
    // If there are in the order of TIGHTDB_MAX_LIST_SIZE different strings, then we'll get a good
    // distribution btw. arrays with no matches and arrays with multiple matches, when
    // testing Find/FindAll
    int64_t t = random.draw_int_mod(100) * 100;
    size_t len = random.draw_int_mod(10) * 100 + 1;
    string s;
    while (s.length() < len)
        s += number_name(t);

    s = s.substr(0, len);
    return s;
}

} // anonymous namespace


TEST(Strings_Monkey2)
{
    const uint64_t ITER = 16 * 5000 * TEST_DURATION * TEST_DURATION * TEST_DURATION;
    const uint64_t seed = 123;

    VerifiedString a;
    Array res;

    Random random(seed);
    int trend = 5;

    for (size_t iter = 0; iter < ITER; iter++) {

//        if (random.chance(1, 10))
//            cout << "Input bitwidth around ~"<<current_bitwidth<<", , a.Size()="<<a.size()<<"\n";

        if (random.draw_int_mod(ITER / 100) == 0) {
            trend = random.draw_int_mod(10);

            a.find_first(randstring(random));
            a.find_all(res, randstring(random));
        }

        if (random.draw_int_mod(10) > trend && a.size() < ITER / 100) {
            if (random.draw_bool()) {
                // Insert
                size_t pos = random.draw_int_max(a.size());
                a.insert(pos, randstring(random));
            }
            else {
                // Add
                a.add(randstring(random));
            }
        }
        else if (a.size() > 0) {
            // Delete
            size_t i = random.draw_int_mod(a.size());
            a.erase(i);
        }
    }

    // Cleanup
    a.destroy();
    res.destroy();
}

#endif // TEST_DURATION > 0
