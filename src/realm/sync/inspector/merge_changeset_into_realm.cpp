#include <getopt.h>
#include <string>
#include <iostream>
#include <sstream>

#include "util.hpp"

using namespace realm;

void usage(char* prog)
{
    std::cerr << "Synopsis: " << prog
              << " -options REALM_PATH\n"
                 "\n"
                 "Options:\n"
                 "  -h, --help                            Display usage\n"
                 "  -a, --client_file_ident               Client file ident of changeset\n"
                 "  -b, --origin_timestamp                Timestamp of changeset\n"
                 "  -c, --last_integrated_server_version  Last integrated server version\n"
                 "  -d, --client_version                  Client version\n"
                 "  -e, --changeset_path                  The path of the file containing the hex changeset\n"
                 "\n";
}

const char* optstring = "ha:b:c:d:e:";

struct option longopts[] = {
    {"help", no_argument, nullptr, 'h'},
    {"client_file_ident", required_argument, nullptr, 'a'},
    {"origin_timestamp", required_argument, nullptr, 'b'},
    {"last_integrated_server_version", required_argument, nullptr, 'c'},
    {"client_version", required_argument, nullptr, 'd'},
    {"changeset_path", required_argument, nullptr, 'e'},
};

inspector::MergeConfiguration parse_arguments(int argc, char* argv[])
{
    inspector::MergeConfiguration config;

    int ch;
    while ((ch = getopt_long(argc, argv, optstring, longopts, nullptr)) != -1) {
        switch (ch) {
            case 'h':
                usage(argv[0]);
                std::exit(EXIT_SUCCESS);
            case 'a': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                uint_fast64_t v = 0;
                in >> v;
                if (in && in.eof()) {
                    config.client_file_ident = v;
                }
                else {
                    std::cerr << "Error: Invalid client file ident\n\n";
                    usage(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'b': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                uint_fast64_t v = 0;
                in >> v;
                if (in && in.eof()) {
                    config.origin_timestamp = v;
                }
                else {
                    std::cerr << "Error: Invalid origin timestamp\n\n";
                    usage(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'c': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                uint_fast64_t v = 0;
                in >> v;
                if (in && in.eof()) {
                    config.last_integrated_server_version = v;
                }
                else {
                    std::cerr << "Error: Invalid last integrated server version\n\n";
                    usage(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'd': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                uint_fast64_t v = 0;
                in >> v;
                if (in && in.eof()) {
                    config.client_version = v;
                }
                else {
                    std::cerr << "Error: Invalid client version\n\n";
                    usage(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'e':
                config.changeset_path = std::string(optarg);
                break;
            default:
                usage(argv[0]);
                std::exit(EXIT_FAILURE);
        }
    }

    if (optind >= argc) {
        usage(argv[0]);
        std::exit(EXIT_FAILURE);
    }

    config.realm_path = std::string(argv[optind]);

    return config;
}

int main(int argc, char* argv[])
{
    inspector::MergeConfiguration config = parse_arguments(argc, argv);

    std::cout << "config.client_file_ident = " << config.client_file_ident << "\n";
    std::cout << "config.origin_timestamp = " << config.origin_timestamp << "\n";
    std::cout << "config.last_integrated_server_version = " << config.last_integrated_server_version << "\n";
    std::cout << "config.client_version = " << config.client_version << "\n";
    std::cout << "config.changeset_path = " << config.changeset_path << "\n";
    std::cout << "config.realm_path = " << config.realm_path << "\n";

    inspector::merge_changeset_into_server_realm(config);

    return 0;
}
