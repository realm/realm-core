#include <realm/group_shared.hpp>
#include "../fuzz_group.hpp"

// AFL not yet supported by Windows
#ifndef _MSC_VER

int main(int argc, const char* argv[])
{
    return run_fuzzy(argc, argv);
}

#endif
