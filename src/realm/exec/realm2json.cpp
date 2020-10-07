#include <realm.hpp>
#include <iostream>
#include <realm/history.hpp>

const char* legend = "Simple tool to output the JSON representation of a Realm:\n"
                     "  realm2json [--link-depth=N] [--xjson] <.realm file>\n"
                     "\n"
                     "Options:\n"
                     " --link-depth: How deep to traverse linking objects (use -1 for infinite). See test_json.cpp "
                     "for more details. Defaults to 0.\n"
                     " --xjson: Output should be formatted as MongoDB XJSON \n"
                     "\n";

template <typename FormatStr>
void abort_if(bool cond, FormatStr fmt)
{
    if (!cond) {
        return;
    }

    fputs(fmt, stderr);
    std::exit(1);
}

template <typename FormatStr, typename... Args>
void abort_if(bool cond, FormatStr fmt, Args... args)
{
    if (!cond) {
        return;
    }

    fprintf(stderr, fmt, args...);
    std::exit(1);
}

int main(int argc, char const* argv[])
{
    std::map<std::string, std::string> renames;
    size_t link_depth = 0;
    realm::JSONOutputMode output_mode = realm::output_mode_json;

    abort_if(argc <= 1, legend);

    // Parse from 1'st argument until before source args
    for (int a = 1; a < argc - 1; ++a) {
        abort_if(strlen(argv[a]) == 0 || argv[a][strlen(argv[a]) - 1] == '=' || argv[a + 1][0] == '=',
                 "Please remove space characters before and after '=' signs in command line flags");

        if (strncmp(argv[a], "--link-depth=", 13) == 0)
            link_depth = atoi(&argv[a][13]);
        else if (strncmp(argv[a], "--xjson", 7) == 0) {
            output_mode = realm::output_mode_xjson;
        }
        else {
            abort_if(true, "Received unknown option '%s' - please see description below\n\n%s", argv[a], legend);
        }
    }

    std::string path = argv[argc - 1];

    try {
        // First we try to open in read_only mode. In this way we can also open
        // realms with a client history
        realm::Group g(path);
        g.to_json(std::cout, link_depth, &renames, output_mode);
    }
    catch (const realm::FileFormatUpgradeRequired& e) {
        // In realm history
        // Last chance - this one must succeed
        auto hist = realm::make_in_realm_history(path);
        realm::DBOptions options;
        options.allow_file_format_upgrade = true;

        auto db = realm::DB::create(*hist, options);

        std::cerr << "File upgraded to latest version: " << path << std::endl;

        auto tr = db->start_read();
        tr->to_json(std::cout, link_depth, &renames, output_mode);
    }

    return 0;
}
