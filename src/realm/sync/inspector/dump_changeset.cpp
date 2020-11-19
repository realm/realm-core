#include <cstring>
#include <type_traits>
#include <random>
#include <string>
#include <locale>
#include <sstream>
#include <iostream>

#include <realm/util/hex_dump.hpp>
#include <realm/group_shared.hpp>
#include <realm/noinst/server_history.hpp>

using namespace realm;


namespace {

class HistoryContext : public _impl::ServerHistory::Context {
public:
    bool owner_is_sync_server() const noexcept override
    {
        return false;
    }
    std::mt19937_64& server_history_get_random() noexcept override
    {
        return m_random;
    }

private:
    std::mt19937_64 m_random;
};

} // unnamed namespace


int main(int argc, char* argv[])
{
    std::string realm_path;
    sync::version_type sync_version = 0;

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
        auto get_parsed_value_with_check = [&](auto& var, auto check_val) {
            std::string str_val;
            if (get_string_value(str_val)) {
                std::istringstream in(str_val);
                in.imbue(std::locale::classic());
                in.unsetf(std::ios_base::skipws);
                using value_type = typename std::remove_reference<decltype(var)>::type;
                value_type val = value_type{};
                in >> val;
                if (in && in.eof() && check_val(val)) {
                    var = val;
                    return true;
                }
            }
            return false;
        };
        auto get_parsed_value = [&](auto& var) {
            return get_parsed_value_with_check(var, [](auto) {
                return true;
            });
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
            std::cerr << "ERROR: Bad or missing value for option: " << arg << "\n";
            error = true;
        }
        argc = argc_2;

        i = 0;
        if (!get_string_value(realm_path)) {
            error = true;
        }
        else if (!get_parsed_value(sync_version)) {
            error = true;
        }
        else if (i < argc) {
            error = true;
        }

        if (help) {
            std::cerr << "Synopsis: " << prog
                      << "  PATH  VERSION\n"
                         "\n"
                         "Options:\n"
                         "  -h, --help           Display command-line synopsis followed by the list of\n"
                         "                       available options.\n";
            return EXIT_SUCCESS;
        }

        if (error) {
            std::cerr << "ERROR: Bad command line.\n"
                         "Try `"
                      << prog << " --help`\n";
            return EXIT_FAILURE;
        }
    }

    HistoryContext context;
    _impl::ServerHistory::DummyCompactionControl compaction_control;
    _impl::ServerHistory history{realm_path, context, compaction_control};
    SharedGroup sg{history};

    auto history_contents = history.get_history_contents();
    const auto& sync_history = history_contents.sync_history;
    std::size_t history_entry_index = std::size_t(sync_version - history_contents.history_base_version - 1);
    bool good_sync_version =
        (sync_version > history_contents.history_base_version && history_entry_index < sync_history.size());
    if (!good_sync_version) {
        std::cerr << "Version is out of range\n";
        return EXIT_FAILURE;
    }
    const std::string& changeset = sync_history[history_entry_index].changeset;
    std::cout << util::hex_dump(changeset.data(), changeset.size()) << "\n";
}
