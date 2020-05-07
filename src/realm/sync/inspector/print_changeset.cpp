#include <cstring>
#include <cstdlib>
#include <string>
#include <iostream>

#include "util.hpp"

int main(int argc, char* argv[])
{
    std::string changeset_path;
    bool hex = false;

    // Process command line
    {
        const char* prog = argv[0];
        --argc;
        ++argv;
        bool error = false;
        bool help = false;
        int argc_2 = 0;
        int i = 0;
        char* arg = nullptr;
        auto get_string_value = [&](std::string& var) {
            if (i < argc) {
                var = argv[i++];
                return true;
            }
            return false;
        };
        while (i < argc) {
            arg = argv[i++];
            if (arg[0] != '-') {
                argv[argc_2++] = arg;
                continue;
            }
            if (std::strcmp(arg, "-h") == 0 || std::strcmp(arg, "--help") == 0) {
                help = true;
                continue;
            }
            else if (std::strcmp(arg, "-H") == 0 || std::strcmp(arg, "--hex") == 0) {
                hex = true;
                continue;
            }
            std::cerr << "ERROR: Unknown option: " << arg << "\n";
            error = true;
        }
        argc = argc_2;

        i = 0;
        if (!get_string_value(changeset_path)) {
            error = true;
        }
        else if (i < argc) {
            error = true;
        }

        if (help) {
            std::cerr << "Synopsis: " << prog
                      << " <changeset file>\n"
                         "\n"
                         "Where <changeset file> is the file system path of a file containing a\n"
                         "changeset, possibly in hex format.\n"
                         "\n"
                         "Options:\n"
                         "  -h, --help           Display command-line synopsis followed by the list of\n"
                         "                       available options.\n"
                         "  -H, --hex            Interpret file contents as hex encoded.\n";
            return EXIT_SUCCESS;
        }

        if (error) {
            std::cerr << "ERROR: Bad command line.\n"
                         "Try `"
                      << prog << " --help`\n";
            return EXIT_FAILURE;
        }
    }

    realm::inspector::print_changeset(changeset_path, hex);
}
