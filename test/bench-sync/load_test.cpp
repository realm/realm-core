#include <getopt.h>
#include <fstream>

#include "load_tester.hpp"

using namespace realm;
using namespace sync;

static void print_usage(std::ostream& os, const char* prog)
{
    os << "Usage: " << prog << " <server-url> <local-root> <options>\n";
    os << "Options:\n";
    os << "    --verbose          Print debug information.\n";
    os << "    --token=<file>  User token file. (Required)\n";
    os << "    --machine-id=N     Id of the machine the client is started on, starting with 1.(Required)\n";
    os << "    --client-id=N    Numeric Client id, starting with 0.(Required)\n";
    os << "    --sleep-between=N    Sleep time in between transactions. Milliseconds.(Optional)\n";
    os << "    --num-transactions=N    The number of transactions after which the client stops sending.(Optional)\n";
    os << "    --listen=N    The client will just listen for changes\n";
    os << "    --statsd-host=N    Statsd server hostname\n";
    os << "    --statsd-port=N    Statsd server port\n";
}

int main(int argc, char** argv)
{
    std::string root_dir;
    std::string server_url;

    int verbose = 0;
    int listen = 0;
    unsigned int sleep_in_between = 0; // Sleep in between transactions
    unsigned int num_operations = 0;   // Number of operations in a transaction
    int num_transactions = -1;
    unsigned int client_id = 0;
    std::string machine_id = "1";
    util::Optional<std::string> token_path;
    util::Optional<std::string> signature_path;
    std::string statsd_hostname{"localhost"};
    unsigned int statsd_port = 8125;
    // Process command line
    const struct option long_options[] = {{"verbose", no_argument, &verbose, 1},
                                          {"token", required_argument, 0, 't'},
                                          {"machine-id", required_argument, 0, 'm'},
                                          {"client-id", required_argument, 0, 'c'},
                                          {"sleep-between", optional_argument, 0, 's'},
                                          {"num-transactions", optional_argument, 0, 'n'},
                                          {"num-operations", optional_argument, 0, 'o'},
                                          {"listen", no_argument, &listen, 1},
                                          {"help", no_argument, 0, 'h'},
                                          {"statsd-host", optional_argument, 0, 'x'},
                                          {"statsd-port", optional_argument, 0, 'y'}};
    while (true) {
        int option_index = 0;
        int c = getopt_long(argc, argv, "l:t:m:c:s:n:o:h:x:y", long_options, &option_index);
        if (c == -1)
            break; // no more options

        switch (c) {
            case 0: /* flag set */
                break;
            case 't': {
                token_path = std::string(optarg);
                break;
            }
            case 'm': {
                machine_id = std::string(optarg);
                break;
            }
            case 'c': {
                std::stringstream ss;
                ss << optarg;
                if (!(ss >> client_id)) {
                    std::cerr << "Invalid Client id.\n";
                    print_usage(std::cerr, argv[0]);
                    return 1;
                }
                break;
            }
            case 's': {
                std::stringstream ss;
                ss << optarg;
                if (!(ss >> sleep_in_between)) {
                    std::cerr << "Invalid sleep time.\n";
                    print_usage(std::cerr, argv[0]);
                    return 1;
                }
                break;
            }
            case 'x': {
                statsd_hostname = std::string(optarg);
                break;
            }
            case 'y': {
                std::stringstream ss;
                ss << optarg;
                if (!(ss >> statsd_port)) {
                    std::cerr << "Invalid statsd port.\n";
                    print_usage(std::cerr, argv[0]);
                    return 1;
                }
                break;
            }
            case 'n': {
                std::stringstream ss;
                ss << optarg;
                if (!(ss >> num_transactions) || num_transactions < 0) {
                    std::cerr << "Invalid number of transactions. Should be > 0\n";
                    print_usage(std::cerr, argv[0]);
                    return 1;
                }
                break;
            }
            case 'o': {
                std::stringstream ss;
                ss << optarg;
                if (!(ss >> num_operations)) {
                    std::cerr << "Invalid number of operations. Should be > 0\n";
                    print_usage(std::cerr, argv[0]);
                    return 1;
                }
                break;
            }
            case 'h': {
                print_usage(std::cout, argv[0]);
                return 1;
            }
            case '?': {
                // getopt already printed an error
                print_usage(std::cerr, argv[0]);
                return 1;
            }
        }
    }

    if (optind != argc - 2) {
        print_usage(std::cerr, argv[0]);
        return 1;
    }

    server_url = argv[optind];
    root_dir = argv[optind + 1];

    if (!token_path) {
        std::cerr << "Please provide a user token file. :-)\n";
        print_usage(std::cerr, argv[0]);
        return 1;
    }
    if (!util::File::exists(*token_path)) {
        std::cerr << "User token file not found (\"" << *token_path << "\")\n";
        return 1;
    }

    std::ifstream identity_file;
    identity_file.open(*token_path);
    std::string syncUserToken;
    identity_file >> syncUserToken;
    std::string realm_path = root_dir + "/load.realm";

    LoadTester ld{syncUserToken,  realm_path,       server_url,  machine_id,      client_id,  sleep_in_between,
                  num_operations, num_transactions, listen == 1, statsd_hostname, statsd_port};
    ld.run();
    return 0;
}
