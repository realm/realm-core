#include <realm/sync/changeset.hpp>
#include <realm/sync/changeset_parser.hpp>

#include <iostream>
#include <vector>
#include <string>
#include <algorithm>
#include <fstream>
#include <iomanip>

void usage(const char* prog)
{
    std::cout << "Usage: " << prog << " [OPTIONS] <files>\n";
    std::cout << "\n"
              << "    -x    Interpret contents of input file as hex words (\"3F 00 04 ...\") (default)\n"
                 "    -h    Display this help screen.\n";
    std::cout << "\n";
}

int main(int argc, char const* argv[])
{
    // Process command line
    bool hex_mode = true;
    static_cast<void>(hex_mode);

    std::vector<std::string> file_names;
    {
        const char* prog = argv[0];
        --argc;
        ++argv;

        std::vector<std::string> args;
        for (size_t i = 0; i < size_t(argc); ++i) {
            args.push_back(std::string(argv[i]));
        }

        auto contains = [](const auto& container, auto thing) -> bool {
            return std::find(begin(container), end(container), thing) != end(container);
        };

        if (contains(args, "-h") || contains(args, "--help")) {
            usage(prog);
            return 0;
        }

        for (auto& arg : args) {
            if (arg != "-h" && arg != "--help" && arg != "-x") {
                file_names.push_back(arg);
            }
        }
    }

    if (file_names.empty()) {
        std::cerr << "No input files given.\n";
        return 1;
    }

    bool errors = false;

    for (auto& file_name : file_names) {
        std::cout << "File: " << file_name << "\n";

        std::vector<char> parsed_bytes;
        std::ifstream file{file_name};
        if (!file.good()) {
            std::cerr << "Error opening file.\n";
            errors = true;
            continue;
        }

        while (!file.eof()) {
            unsigned int byte;
            if (!(file >> std::hex >> byte)) {
                break;
            }
            if (byte < std::numeric_limits<unsigned char>::min() ||
                byte > std::numeric_limits<unsigned char>::max()) {
                std::cerr << "Invalid byte.\n";
                errors = true;
                continue;
            }
            parsed_bytes.push_back(char(byte));
        }

        std::cout << parsed_bytes.size() << " bytes read.\n";

        auto stream = realm::_impl::SimpleNoCopyInputStream{parsed_bytes.data(), parsed_bytes.size()};

        realm::sync::Changeset parsed;
        try {
            realm::sync::parse_changeset(stream, parsed);
        }
        catch (std::exception& ex) {
            std::cerr << "Parser error: " << ex.what();
            errors = true;
            continue;
        }

        std::cout << "Parsed changeset:\n";
        parsed.print(std::cout);

        std::cout << "\n";
    }

    return errors ? 1 : 0;
}