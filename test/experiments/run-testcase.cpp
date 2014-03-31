#include <cstdlib>

#include "../util/unit_test.hpp"

using namespace tightdb::test_util::unit_test;

int main()
{
    SimpleReporter reporter;
    bool success = get_default_test_list().run(&reporter);
    return success ? EXIT_SUCCESS : EXIT_FAILURE;
}
