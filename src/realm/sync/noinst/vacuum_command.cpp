#include <algorithm>
#include <stdexcept>
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>

#include <realm/util/logger.hpp>
#include <realm/util/load_file.hpp>
#include <realm/sync/noinst/vacuum.hpp>
#include <realm/sync/noinst/server_history.hpp>
#include <realm/sync/noinst/command_line_util.hpp>

using namespace realm;
using namespace realm::sync;
using namespace realm::_impl;

namespace {

void usage(const char* program_name)
{
    std::cerr << "Usage: " << program_name
              << " [OPTIONS] [FILES]\n"
                 "\n"
                 "  Vacuum attempts to reduce the size of Realm files without modifying observable\n"
                 "  state. If the file is a server-side Realm, its transaction log may also be\n"
                 "  compacted.\n"
                 "\n"
                 "  NOTE: Vacuuming a Realm file that is currently opened by another process (such as\n"
                 "        the Realm Object Server) is currently not supported. Attempts to vacuum a\n"
                 "        file that is opened by another process will be ignored, and a warning will\n"
                 "        be emitted.\n"
                 "\n"
                 "Arguments:\n"
                 "\n"
                 "  -n, --dry-run                 Do not perform any modifying actions, but report\n"
                 "                                potential reductions in file size.\n"
                 "  --no-log-compaction           Do not run log compaction.\n"
                 "  --no-file-compaction          Do not run file compaction.\n"
                 "  --no-file-upgrade             Do not do attempt to upgrade any files to the\n"
                 "                                current format.\n"
                 "  --no-prompt                   Do not prompt for confirmation before modifying\n"
                 "                                files.\n"
                 "  -E, --encryption-key          Specify the path to a file containing an encryption\n"
                 "                                key, which will be used to open the Realm file(s).\n"
                 "  --history-type                None, InRealm, SyncServer or SyncClient.\n"
                 "  --bump-realm-version          Bump Realm snapshot version.\n"
                 "  --server-history-ttl          The 'time to live' in seconds since last activity for\n"
                 "                                entries in the client files registry of a server-side\n"
                 "                                file. This affects the potential for history compaction\n"
                 "                                to make a difference. The default is 'infinite'.\n"
                 "  --ignore-clients              If specified, the determination of how far in-place\n"
                 "                                history compaction can proceed will be based entirely\n"
                 "                                on the history itself, and the 'last access' timestamps\n"
                 "                                of client file entries will be completely ignored. This\n"
                 "                                should only be done in emergency situations. Expect it\n"
                 "                                to cause expiration of client files even when they have\n"
                 "                                seen acitivity within the specified time to live\n"
                 "                                (`--server-history-ttl`).\n"
                 "  -l, --log-level               Set log level. Valid values are 'all', 'trace',\n"
                 "                                'debug', 'detail', 'info', 'warn', 'error', 'fatal',\n"
                 "                                or 'off'. (default 'info')\n"
                 "  -h, --help                    Display command-line synopsis followed by the\n"
                 "                                available options.\n"
                 "\n";
}

struct Configuration {
    util::Logger::Level log_level = util::Logger::Level::info;
    bool dry_run = false;
    bool prompt = true;
    Vacuum::Options options;
    std::vector<std::string> files;
};

Configuration parse_options(int argc, char* argv[])
{
    static ::option long_options[] = {{"dry-run", no_argument, nullptr, 'n'},
                                      {"no-log-compaction", no_argument, nullptr, 0},
                                      {"no-file-compaction", no_argument, nullptr, 0},
                                      {"no-file-upgrade", no_argument, nullptr, 0},
                                      {"no-prompt", no_argument, nullptr, 0},
                                      {"encryption-key", required_argument, nullptr, 'E'},
                                      {"history-type", required_argument, nullptr, 0},
                                      {"bump-realm-version", no_argument, nullptr, 0},
                                      {"server-history-ttl", required_argument, nullptr, 0},
                                      {"ignore-clients", no_argument, nullptr, 0},
                                      {"log-level", required_argument, nullptr, 'l'},
                                      {"help", no_argument, nullptr, 'h'},
                                      {nullptr, 0, nullptr, 0}};

    // Initial '-' means filenames can be intermixed with options.
    static const char opt_desc[] = "-nhE:l:";
    int opt_index = 0;
    int opt;

    Configuration result;

    util::Optional<std::string> encryption_key_path;

    while ((opt = getopt_long_only(argc, argv, opt_desc, long_options, &opt_index)) != -1) {
        switch (opt) {
            case 0: {
                switch (opt_index) {
                    case 0: {
                        result.dry_run = true;
                        break;
                    }
                    case 1: {
                        result.options.no_log_compaction = true;
                        break;
                    }
                    case 2: {
                        result.options.no_file_compaction = true;
                        break;
                    }
                    case 3: {
                        result.options.no_file_upgrade = true;
                        break;
                    }
                    case 4: {
                        result.prompt = false;
                        break;
                    }
                    case 5: {
                        encryption_key_path = std::string{optarg};
                        break;
                    }
                    case 6: {
                        std::string type = std::string{optarg};
                        if (type == "None")
                            result.options.history_type = Replication::hist_None;
                        else if (type == "InRealm")
                            result.options.history_type = Replication::hist_InRealm;
                        else if (type == "SyncServer")
                            result.options.history_type = Replication::hist_SyncServer;
                        else if (type == "SyncClient")
                            result.options.history_type = Replication::hist_SyncClient;
                        else {
                            usage(argv[0]);
                            std::exit(EXIT_SUCCESS);
                        }
                        break;
                    }
                    case 7: {
                        result.options.bump_realm_version = true;
                        break;
                    }
                    case 8: {
                        std::istringstream in(optarg);
                        in.unsetf(std::ios_base::skipws);
                        int v = 0;
                        in >> v;
                        if (in && in.eof() && v >= 0) {
                            result.options.server_history_ttl = std::chrono::seconds{v};
                        }
                        else {
                            std::cerr << "Error: Invalid number of seconds "
                                         "`"
                                      << optarg << "'.\n\n";
                            usage(argv[0]);
                            std::exit(EXIT_FAILURE);
                        }
                        break;
                    }
                    case 9: {
                        result.options.ignore_clients = true;
                        break;
                    }
                    case 10:
                        if (!_impl::parse_log_level(optarg, result.log_level)) {
                            std::cerr << "Error: Invalid log level value `" << optarg << "'.\n\n";
                            usage(argv[0]);
                            std::exit(EXIT_FAILURE);
                        }
                        break;
                    case 11: {
                        usage(argv[0]);
                        std::exit(EXIT_SUCCESS);
                        break;
                    }
                    default:
                        REALM_TERMINATE("Missing option handling");
                }
                break;
            }
            case 'h': {
                usage(argv[0]);
                std::exit(EXIT_SUCCESS);
                break;
            }
            case 'n': {
                result.dry_run = true;
                break;
            }
            case 'E': {
                encryption_key_path = std::string{optarg};
                break;
            }
            case 'l':
                if (!_impl::parse_log_level(optarg, result.log_level)) {
                    std::cerr << "Error: Invalid log level value `" << optarg << "'.\n\n";
                    usage(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
                break;
            case 1: {
                result.files.push_back(std::string{optarg});
                break;
            }
            case '?': {
                usage(argv[0]);
                std::exit(EXIT_FAILURE);
                break;
            }
            default:
                REALM_TERMINATE("Unhandled option");
        }
    }

    ++::optind; // Skip program name

    while (::optind < argc) {
        result.files.push_back(std::string{argv[::optind]});
        ++::optind;
    }

    if (result.files.empty()) {
        throw std::invalid_argument{"No files given."};
    }

    if (encryption_key_path) {
        std::string encryption_key = util::load_file_and_chomp(*encryption_key_path);
        if (encryption_key.size() != 64)
            throw std::runtime_error("Encyption key has bad size");
        std::array<char, 64> encryption_key_2;
        std::copy(encryption_key.begin(), encryption_key.end(), encryption_key_2.data());
        result.options.encryption_key = encryption_key_2;
    }

    if (result.prompt) {
        std::cerr << "WARNING: Prompting before compaction has not been implemented yet. Pass --no-prompt to "
                     "suppress this warning.\n";
    }

    if (result.options.dry_run) {
        result.options.no_file_upgrade = true;
    }

    return result;
}

static const char* program_name = "realm-vacuum";

template <class F>
auto catch_errors(F func, bool print_usage_and_exit = false)
{
    try {
        return func();
    }
    catch (const std::exception& ex) {
        std::cerr << "ERROR: " << ex.what() << std::endl;
    }
    catch (...) {
        std::cerr << "An unknown error occurred." << std::endl;
    }
    if (print_usage_and_exit) {
        usage(program_name);
        std::exit(EXIT_FAILURE);
    }
}

} // unnamed namespace

int main(int argc, char* argv[])
{
    // Force GNU getopt to behave in a POSIX-y way. This is required so that
    // positional argument detection is handled properly, and the same on all
    // platforms.
#ifdef _WIN32
    _putenv_s("POSIXLY_CORRECT", "1");
#else
    setenv("POSIXLY_CORRECT", "1", 0);
#endif

    program_name = argv[0];
    Configuration config;
    catch_errors(
        [&] {
            config = parse_options(argc, argv);
        },
        true);

    size_t errors_seen = 0;

    util::StderrLogger logger;
    logger.set_level_threshold(config.log_level);
    Vacuum vacuum{logger, config.options};
    for (auto& file : config.files) {
        try {
            Vacuum::Results results;
            if (config.dry_run) {
                results = vacuum.dry_run(file);
            }
            else {
                results = vacuum.vacuum(file);
            }
            if (results.ignored) {
                std::cout << "Ignored file: " << file << "\n";
            }
            else {
                std::cout << "File:   " << file << "\n";
                std::cout << "Type:   " << results.type_description << "\n";
                std::cout << "Before: " << results.before_size << " bytes\n";
                std::cout << "After:  " << results.after_size << " bytes\n";
                double change_pct = -(1.0 - double(results.after_size) / double(results.before_size)) * 100.0;
                std::cout << "Change: " << std::setprecision(2) << change_pct << "%";
                if (config.dry_run) {
                    std::cout << " (dry run; no modifications made)";
                }
                std::cout << "\n\n";
            }
        }
        catch (const std::exception& ex) {
            std::cerr << "ERROR (" << file << "): " << ex.what() << std::endl;
            ++errors_seen;
        }
    }
    return (errors_seen == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
}
