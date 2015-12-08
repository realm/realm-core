#include <realm/group_shared.hpp>
#include <realm/link_view.hpp>
#include <realm/commit_log.hpp>
#include "../test.hpp"

#include <stdio.h>
#include <fstream>

using namespace realm;
using namespace realm::util;
using namespace std;

// AFL not yet supported by Windows
#ifndef _MSC_VER

struct EndOfFile {};

int run_fuzzy(int argc, const char* argv[]);

int main(int argc, const char* argv[])
{
    return run_fuzzy(argc, argv);
}

#endif