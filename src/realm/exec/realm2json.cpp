#include <realm.hpp>
#include <iostream>
#include <realm/history.hpp>

const char* legend = "Simple tool to output the JSON representation of a Realm:\n"
                     "  realm2json [-link-depth=N] [-output-mode=N] <.realm file>\n"
                     "\n"
                     "Options:\n"
                     " -link-depth: How deep to traverse linking objects (use -1 for infinite). See test_json.cpp "
                     "for more details. Defaults to 0.\n"
                     " -output-mode:Can specify MongoDB XJSON output format by passing in 1. Defaults to 0. \n"
                     "\n";

void abort2(bool b, const char* fmt, ...)
{
    if (b) {
        va_list argv;
        va_start(argv, fmt);
        fprintf(stderr, "realm2json: ");
        vfprintf(stderr, fmt, argv);
        va_end(argv);
        fprintf(stderr, "\n");
        exit(1);
    }
}

int main(int argc, char const* argv[])
{
    std::map<std::string, std::string> renames;
    size_t link_depth = 0;
    realm::JSONOutputMode output_mode = realm::output_mode_json;

    // Parse from 1'st argument until before source args
    for (int a = 1; a < argc - 1; ++a) {
        abort2(strlen(argv[a]) == 0 || argv[a][strlen(argv[a]) - 1] == '=' || argv[a + 1][0] == '=',
               "Please remove space characters before and after '=' signs in command line flags");

        if (strncmp(argv[a], "-link-depth=", 12) == 0)
            link_depth = atoi(&argv[a][12]);
        else if (strncmp(argv[a], "-output-mode=", 13) == 0) {
            auto output_mode_int = atoi(&argv[a][13]);
            if (output_mode_int == 1)
                output_mode = realm::output_mode_xjson;
        }
        else {
            abort2(true, "Received unknown option '%s' - please see description below\n\n%s", argv[a], legend);
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
