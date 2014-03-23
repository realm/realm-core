#include <cstdlib>

#include "../util/unit_test.hpp"

int main()
{
    tightdb::test_util::unit_test::SimpleReporter reporter;
    bool success = run(&reporter);
    return success ? EXIT_SUCCESS : EXIT_FAILURE;
}
