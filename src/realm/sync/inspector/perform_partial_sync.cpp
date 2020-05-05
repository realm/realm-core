#include <getopt.h>
#include <string>
#include <iostream>
#include <sstream>

#include "util.hpp"

using namespace realm;

void usage(char* prog)
{
    std::cerr <<
        "Synopsis: " << prog << " -options\n"
        "\n"
        "Options:\n"
        "  -h, --help                            Display usage\n"
        "  -l, --log_level                       Log level\n"
        "  -u, --user_identity                   User identity\n"
        "  -a, --is_admin                        Is admin\n"
        "  -p, --partial_realm_path              The path of the partial Realm\n"
        "  -r, --reference_realm_path            The path of the reference Realm\n"
        "\n";
}

const char* optstring = "hl:u:a:p:r:";

struct option longopts[] = {
    { "help", no_argument, nullptr, 'h' },
    { "log_level", required_argument, nullptr, 'l' },
    { "user_identity", required_argument, nullptr, 'u' },
    { "is_admin", required_argument, nullptr, 'a' },
    { "partial_realm_path", required_argument, nullptr, 'p' },
    { "reference_realm_path", required_argument, nullptr, 'r' },
};

inspector::PartialSyncConfiguration parse_arguments(int argc, char* argv[])
{
    inspector::PartialSyncConfiguration config;

    int ch;
    while ( (ch = getopt_long(argc, argv, optstring, longopts, nullptr)) != -1 ) {
        switch (ch) {
            case 'h':
                usage(argv[0]);
                std::exit(EXIT_SUCCESS);
            case 'l':
                {
                    std::string log_level_str {optarg};
                    std::istringstream in {log_level_str};
                    in.imbue(std::locale::classic());
                    in.unsetf(std::ios_base::skipws);
                    in >> config.log_level;
                    if (!in || !in.eof()) {
                        usage(argv[0]);
                        std::exit(EXIT_FAILURE);
                    }
                }
                break;
            case 'u':
                config.user_identity = std::string(optarg);
                break;
            case 'a':
                config.is_admin = (std::string(optarg) == "true") ? true : false;
                break;
            case 'p':
                config.partial_realm_path = std::string(optarg);
                break;
            case 'r':
                config.reference_realm_path = std::string(optarg);
                break;
            default:
                usage(argv[0]);
                std::exit(EXIT_FAILURE);
        }
    }

    if (optind != argc ||
        config.partial_realm_path.empty() ||
        config.reference_realm_path.empty()
       ) {
        usage(argv[0]);
        std::exit(EXIT_FAILURE);
    }

    return config;
}

int main(int argc, char* argv[])
{
    inspector::PartialSyncConfiguration config = parse_arguments(argc, argv);

    std::cout << "config.log_level = " << config.log_level << "\n";
    std::cout << "config.user_identity = " << config.user_identity << "\n";
    std::cout << "config.is_admin = " << (config.is_admin ? "true" : "false") << "\n";
    std::cout << "config.partial_realm_path = " << config.partial_realm_path << "\n";
    std::cout << "config.reference_realm_path = " << config.reference_realm_path << "\n";

    inspector::perform_partial_sync(config);

    return 0;
}
