
#ifndef REALM_NOINST_COMMAND_LINE_UTIL_HPP
#define REALM_NOINST_COMMAND_LINE_UTIL_HPP

#ifdef _WIN32
#include <win32/getopt.h>
#else
#include <getopt.h>
#endif

#include <realm/util/logger.hpp>

namespace realm {
namespace _impl {


inline bool parse_log_level(const char* string, util::Logger::Level& log_level)
{
    std::istringstream in(string);
    in.imbue(std::locale::classic());
    in.unsetf(std::ios_base::skipws);
    util::Logger::Level v = util::Logger::Level();
    in >> v;
    if (in && in.eof()) {
        log_level = v;
        return true;
    }
    return false;
}

template <typename Configuration>
void parse_config_file_path(int argc, char* argv[], Configuration& configuration)
{
    static struct option long_options[] = {{"configuration", required_argument, nullptr, 'c'},
                                           {nullptr, 0, nullptr, 0}};

    static const char* opt_desc = "c:";

    // Prevent getopt from printing error messages to stdout
    int old_opterr = opterr;
    opterr = 0;

    int opt_index = 0;
    char opt;

    while ((opt = getopt_long(argc, argv, opt_desc, long_options, &opt_index)) != -1) {
        if (opt == 'c') {
            configuration.config_file_path = std::string(optarg);
            break;
        }
    }

    opterr = old_opterr;
    optind = 0;
}


} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_COMMAND_LINE_UTIL_HPP
