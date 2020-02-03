#include <realm.hpp>
#include <iostream>
#include <realm/history.hpp>

int main(int argc, char const* argv[])
{
    if (argc > 1) {
        std::string path = argv[1];
        std::map<std::string, std::string> renames;
        size_t link_depth = 0;
        if (argc > 2) {
            link_depth = strtol(argv[2], nullptr, 0);
        }
        try {
            // First we try to open in read_only mode. In this way we can also open
            // realms with a client history
            realm::Group g(path);
            g.to_json(std::cout, link_depth, &renames);
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
            tr->to_json(std::cout, link_depth, &renames);
        }
    }
    return 0;
}
