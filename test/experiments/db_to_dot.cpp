#include <iostream>

#include <realm/group.hpp>

using namespace realm;

int main(int argc, const char* const argv[])
{
    if (argc != 2) {
        std::cerr << "Wrong number of command line arguments\n"
            "Synopsis: " << argv[0] << "  DATABASE-FILE" << std::endl;
        return 1;
    }
    Group g(argv[1], GROUP_READONLY);
    if (!g.is_valid()) {
        std::cerr << "Failed to open Realm database '" << argv[1] << "'" << std::endl;
        return 1;
    }
    g.to_dot(cout);
    return 0;
}
