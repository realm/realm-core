#include "realm/util/cli_args.hpp"
#include <string>
#include <errno.h>
#include <algorithm>

namespace realm::util {

CliArgumentParser::ParseResult CliArgumentParser::parse(int argc, const char** argv)
{
    ParseResult result;
    result.program_name = argv[0];
    for (int i = 1; i < argc; ++i) {
        std::string_view cur_arg(argv[i]);

        auto it = std::find_if(m_flags.begin(), m_flags.end(), [&](const auto& flag) {
            if (cur_arg.size() < 2 || cur_arg.front() != '-') {
                return false;
            }
            auto flag_name = flag->name();
            if (cur_arg[1] == '-' && cur_arg.size() >= flag_name.size() + 2 &&
                cur_arg.substr(2, flag_name.size()) == flag_name) {
                return true;
            }
            if (cur_arg[1] == flag->short_name() && cur_arg.size() == 2) {
                return true;
            }
            return false;
        });
        if (it == m_flags.end()) {
            result.unmatched_arguments.push_back(cur_arg);
            continue;
        }

        CliFlag* arg_holder = *it;
        if (!arg_holder->expects_value()) {
            arg_holder->assign(std::string_view{});
            continue;
        }

        if (auto eq_pos = cur_arg.find_first_of('='); eq_pos != std::string_view::npos && eq_pos < cur_arg.size()) {
            arg_holder->assign(cur_arg.substr(eq_pos + 1));
        }
        else {
            ++i;
            if (i == argc) {
                throw CliParseException("not enough arguments to parse argument with value");
            }
            arg_holder->assign(std::string_view(argv[i]));
        }
    }

    return result;
}

void CliArgumentParser::add_argument(CliFlag* flag)
{
    m_flags.push_back(flag);
}

template <>
std::string CliArgument::as<std::string>() const
{
    return std::string{m_value};
}

template <>
int64_t CliArgument::as<int64_t>() const
{
    int64_t val = std::strtol(m_value.data(), nullptr, 10);
    if (errno == ERANGE) {
        throw CliParseException("parsing integer argument produced an integer out-of-range");
    }
    if (val == 0 && m_value.size() != 0) {
        throw CliParseException("integer argument parsed to zero, but argument was more than 1 character");
    }
    return val;
}

} // namespace realm::util
