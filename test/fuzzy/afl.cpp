#include <realm/group_shared.hpp>

// AFL not yet supported by Windows
#ifndef _MSC_VER

struct EndOfFile {};

int run_fuzzy(int argc, const char* argv[]);

int main(int argc, const char* argv[])
{
    return run_fuzzy(argc, argv);
}

#endif