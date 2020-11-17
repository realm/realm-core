#include <getopt.h>
#include <string>
#include <iostream>
#include <sstream>

#include "util.hpp"

void usage(char* prog)
{
    std::cerr << "Synopsis: " << prog << " PATH\n";
}

int main(int argc, char* argv[])
{
    if (argc != 2) {
        usage(argv[0]);
        std::exit(EXIT_FAILURE);
    }

    const std::string path = argv[1];

    realm::inspector::inspect_client_realm(path);

    return 0;
}
