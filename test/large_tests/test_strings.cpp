#include <tightdb/column.hpp>

#include "../util/number_names.hpp"
#include "../util/verified_string.hpp"

#include "../testsettings.hpp"
#include "../test.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::test_util;


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


namespace {

string randstring(Random& random)
{
    // If there are in the order of REALM_MAX_BPNODE_SIZE different strings, then we'll get a good
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


TEST_IF(Strings_Monkey2, TEST_DURATION >= 1)
{
    uint64_t ITER = 16 * 5000 * TEST_DURATION * TEST_DURATION * TEST_DURATION;
    int seed = 123;

    VerifiedString a;
    ref_type res_ref = Column::create(Allocator::get_default());
    Column res(Allocator::get_default(), res_ref);

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
    res.destroy();
}
