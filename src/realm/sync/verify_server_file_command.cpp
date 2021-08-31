#include <cstdint>
#include <cstring>
#include <string>
#include <locale>
#include <sstream>
#include <iostream>

#include <realm/util/load_file.hpp>
#include <realm/sync/noinst/server_history.hpp>
#include <realm/db.hpp>
#include <realm/version.hpp>

using namespace realm;


int main(int argc, char* argv[])
{
    std::string path;
    std::string encryption_key;

    // Process command line
    {
        const char* prog = argv[0];
        --argc;
        ++argv;
        bool error = false;
        bool help = false;
        bool version = false;
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
            else if (std::strcmp(arg, "-e") == 0 || std::strcmp(arg, "--encryption-key") == 0) {
                if (get_string_value(encryption_key))
                    continue;
            }
            else if (std::strcmp(arg, "-v") == 0 || std::strcmp(arg, "--version") == 0) {
                version = true;
                continue;
            }
            std::cerr << "ERROR: Bad or missing value for option: " << arg << "\n";
            error = true;
        }
        argc = argc_2;

        i = 0;
        if (!get_string_value(path)) {
            error = true;
        }
        else if (i < argc) {
            error = true;
        }

        if (help) {
            std::cerr << "Synopsis: " << prog
                      << "  PATH\n"
                         "\n"
                         "Options:\n"
                         "  -h, --help           Display command-line synopsis followed by the list of\n"
                         "                       available options.\n"
                         "  -e, --encryption-key  The file-system path of a file containing a 64-byte\n"
                         "                       encryption key to be used for accessing the specified\n"
                         "                       Realm file.\n"
                         "  -v, --version        Show the version of the Realm Sync release that this\n"
                         "                       command belongs to.\n";
            return EXIT_SUCCESS;
        }

        if (version) {
            const char* build_mode;
#if REALM_DEBUG
            build_mode = "Debug";
#else
            build_mode = "Release";
#endif
            std::cerr << "RealmSync/" REALM_VERSION_STRING " (build_mode=" << build_mode << ")\n";
            return EXIT_SUCCESS;
        }

        if (error) {
            std::cerr << "ERROR: Bad command line.\n"
                         "Try `"
                      << prog << " --help`\n";
            return EXIT_FAILURE;
        }
    }

    DBOptions options;
    std::string encryption_key_2;
    if (!encryption_key.empty()) {
        encryption_key_2 = util::load_file(encryption_key);
        options.encryption_key = encryption_key_2.data();
    }
    class HistoryContext : public _impl::ServerHistory::Context {
    public:
        std::mt19937_64& server_history_get_random() noexcept override final
        {
            return m_random;
        }

    private:
        std::mt19937_64 m_random;
    };
    HistoryContext history_context;
    _impl::ServerHistory::DummyCompactionControl compaction_control;
    _impl::ServerHistory hist{path, history_context, compaction_control}; // Throws
    auto sg = DB::create(hist, options);                                  // Throws
    ReadTransaction rt{sg};                                               // Throws
    rt.get_group().verify();                                              // Throws
}
