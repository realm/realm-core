#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <locale>
#include <sstream>
#include <type_traits>
#include <iostream>

#include <realm/util/load_file.hpp>
#include <realm/sync/noinst/server_history.hpp>
#include <realm/db.hpp>
#include <realm/version.hpp>

using namespace realm;

using SteadyClock = std::conditional<std::chrono::high_resolution_clock::is_steady,
                                     std::chrono::high_resolution_clock, std::chrono::steady_clock>::type;
using SteadyTimePoint = SteadyClock::time_point;

SteadyTimePoint steady_clock_now() noexcept
{
    return SteadyClock::now();
}
using milliseconds_type = std::int_fast64_t;

milliseconds_type steady_duration(SteadyTimePoint start_time, SteadyTimePoint end_time = steady_clock_now()) noexcept
{
    auto duration = end_time - start_time;
    auto millis_duration = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    return milliseconds_type(millis_duration);
}

struct IndexChange {
    std::string table_name;
    std::string column_name;
    bool add;
};

void change_indices(WriteTransaction& wt, const std::vector<IndexChange>& changes, bool dry_run)
{
    SteadyTimePoint total_start = steady_clock_now();
    std::vector<milliseconds_type> timings;
    timings.reserve(changes.size());
    Group& g = wt.get_group();
    for (size_t i = 0; i < changes.size(); ++i) {
        SteadyTimePoint inner_start = steady_clock_now();
        std::cout << (changes[i].add ? "adding" : "removing") << " index on " << changes[i].table_name << "."
                  << changes[i].column_name << std::endl;

        auto table_ndx = g.find_table(changes[i].table_name);
        if (!table_ndx) {
            std::cout << "No table called: " << changes[i].table_name << std::endl;
            return;
        }
        TableRef table = g.get_table(table_ndx);
        auto col_ndx = table->get_column_key(changes[i].column_name);
        if (!col_ndx) {
            std::cout << "No column called: " << changes[i].column_name << " on table: " << changes[i].table_name
                      << std::endl;
            return;
        }
        bool has_index = table->has_search_index(col_ndx);
        if (has_index == changes[i].add) {
            std::cout << "\t nothing to do, column "
                      << (has_index ? "already has an index" : "does not have an index") << std::endl;
        }
        if (dry_run) {
            continue;
        }

        if (changes[i].add) {
            table->add_search_index(col_ndx);
        }
        else {
            table->remove_search_index(col_ndx);
        }

        SteadyTimePoint inner_end = steady_clock_now();
        milliseconds_type inner_time = steady_duration(inner_start, inner_end);
        std::cout << (changes[i].add ? "addition" : "removal") << " took " << inner_time << " milliseconds"
                  << std::endl;
        timings.push_back(inner_time);
    }
    std::cout << "total time: " << steady_duration(total_start, steady_clock_now()) << " ms" << std::endl;
    if (timings.size() > 0) {
        milliseconds_type total_time = 0;
        for (size_t i = 0; i < timings.size(); ++i) {
            total_time += timings[i];
        }
        total_time = total_time / milliseconds_type(timings.size());
        std::cout << "average operation time: " << total_time << " milliseconds" << std::endl;
    }
    if (dry_run) {
        std::cout << "not committing, this is a dry run" << std::endl;
    }
    else {
        wt.commit();
    }
}

int main(int argc, char* argv[])
{
    std::string path;
    std::string encryption_key;

    std::vector<IndexChange> changes;
    bool is_dry_run = false;

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
            else if (std::strcmp(arg, "-d") == 0 || std::strcmp(arg, "--dry-run") == 0) {
                is_dry_run = true;
                continue;
            }
            else if (std::strcmp(arg, "-e") == 0 || std::strcmp(arg, "--encryption-key") == 0) {
                if (get_string_value(encryption_key))
                    continue;
            }
            else if (std::strcmp(arg, "-r") == 0 || std::strcmp(arg, "--remove") == 0) {
                std::string dest;
                if (get_string_value(dest)) {
                    size_t ndx = dest.find_first_of(".");
                    if (ndx != std::string::npos) {
                        IndexChange c;
                        c.add = false;
                        c.column_name = dest.substr(ndx + 1);
                        c.table_name = dest.substr(0, ndx);
                        changes.push_back(c);
                        continue;
                    }
                }
            }
            else if (std::strcmp(arg, "-a") == 0 || std::strcmp(arg, "--add") == 0) {
                std::string dest;
                if (get_string_value(dest)) {
                    size_t ndx = dest.find_first_of(".");
                    if (ndx != std::string::npos) {
                        IndexChange c;
                        c.add = true;
                        c.column_name = dest.substr(ndx + 1);
                        c.table_name = dest.substr(0, ndx);
                        changes.push_back(c);
                        continue;
                    }
                }
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
                      << "  PATH [-a table.column] [-r table.column] [-d]"
                         "\n"
                         "Options:\n"
                         "  -h, --help           Display command-line synopsis followed by the list of\n"
                         "                       available options.\n"
                         "  -e, --encryption-key  The file-system path of a file containing a 64-byte\n"
                         "                       encryption key to be used for accessing the specified\n"
                         "                       Realm file.\n"
                         "  -a, --add            Add an index to the specified table.column\n"
                         "  -r, --remove         Remove an index on the specified table.column\n"
                         "  -d, --dry-run        No changes will be applied, checks that all table.column\n"
                         "                       args exist\n"
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
        bool owner_is_sync_server() const noexcept override final
        {
            return false;
        }
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
    WriteTransaction wt{sg};                                              // Throws
    change_indices(wt, changes, is_dry_run);
}
