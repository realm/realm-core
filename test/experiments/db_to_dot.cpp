#include <iostream>

#include <tightdb/group.hpp>

using namespace std;
using namespace realm;

int main(int argc, const char* const argv[])
{
    if (argc != 2) {
        cerr << "Wrong number of command line arguments\n"
            "Synopsis: " << argv[0] << "  DATABASE-FILE" << endl;
        return 1;
    }
    Group g(argv[1], GROUP_READONLY);
    if (!g.is_valid()) {
        cerr << "Failed to open TightDB database '" << argv[1] << "'" << endl;
        return 1;
    }
    g.to_dot(cout);
    return 0;
}
