#include <cstring>
#include <cstdlib>
#include <fstream>
#include <string>
#include <iostream>

#include <realm/util/cli_args.hpp>
#include <realm/util/compression.hpp>
#include <realm/util/base64.hpp>
#include <realm/sync/changeset.hpp>
#include <realm/sync/changeset_parser.hpp>

using namespace realm;
#if REALM_DEBUG
namespace {

sync::Changeset changeset_binary_to_sync_changeset(const std::string& changeset_binary)
{
    util::SimpleInputStream input_stream{changeset_binary};
    sync::Changeset changeset;
    sync::parse_changeset(input_stream, changeset);

    return changeset;
}

std::string changeset_hex_to_binary(const std::string& changeset_hex)
{
    std::vector<char> changeset_vec;

    std::istringstream in{changeset_hex};
    int n;
    in >> std::hex >> n;
    while (in) {
        REALM_ASSERT(n >= 0 && n <= 255);
        changeset_vec.push_back(n);
        in >> std::hex >> n;
    }

    return std::string{changeset_vec.data(), changeset_vec.size()};
}

std::string changeset_compressed_to_binary(const std::string& changeset_compressed)
{
    // The size of the decompressed size data must come first
    char* p;
    const char* start = changeset_compressed.data();
    size_t decompressed_size = size_t(strtol(start, &p, 10));
    REALM_ASSERT(*p == ' ');
    p++;

    // Decode from BASE64
    const size_t encoded_size = changeset_compressed.size() - (p - start);
    std::string decode_buffer;
    decode_buffer.resize(util::base64_decoded_size(encoded_size));
    StringData window(p, encoded_size);
    util::Optional<size_t> decoded_size = util::base64_decode(window, decode_buffer);
    if (!decoded_size || *decoded_size > encoded_size) {
        throw std::runtime_error("Invalid base64 value");
    }

    // Decompress
    std::string decompressed;
    decompressed.resize(decompressed_size);
    std::error_code ec = util::compression::decompress({decode_buffer.data(), *decoded_size}, decompressed);

    if (ec) {
        throw std::runtime_error(util::format("compression::decompress: %1", ec.message()));
    }

    return decompressed;
}

void parse_and_print_changeset(const std::string& changeset_binary)
{
    sync::Changeset changeset = changeset_binary_to_sync_changeset(changeset_binary);
    changeset.print(std::cout);
}

void print_changeset_in_file(std::istream& input_file, bool hex, bool compressed)
{
    std::stringstream input_file_ss;
    input_file_ss << input_file.rdbuf();
    const auto& file_contents = input_file_ss.str();
    std::string changeset_binary;
    if (hex) {
        changeset_binary = changeset_hex_to_binary(file_contents);
    }
    else if (compressed) {
        changeset_binary = changeset_compressed_to_binary(file_contents);
    }
    else {
        changeset_binary = file_contents;
    }
    parse_and_print_changeset(changeset_binary);
}

void print_changesets_in_log_file(std::istream& input_file)
{
    int log_line_num = 1;
    try {
        for (std::string line; std::getline(input_file, line); ++log_line_num) {
            const std::string_view changeset_prefix("Changeset: ");
            const std::string_view compressed_changeset_prefix("Changeset(comp): ");
            std::string changeset_contents;
            if (auto pos = line.find(changeset_prefix); pos != std::string::npos) {
                changeset_contents = changeset_hex_to_binary(line.substr(pos + changeset_prefix.size()));
            }
            else if (auto pos = line.find(compressed_changeset_prefix); pos != std::string::npos) {
                changeset_contents =
                    changeset_compressed_to_binary(line.substr(pos + compressed_changeset_prefix.size()));
            }
            else {
                std::cout << line << std::endl;
                continue;
            }
            parse_and_print_changeset(changeset_contents);
        }
    }
    catch (const Exception& e) {
        throw RuntimeError(e.code(), util::format("Exception at line number %1: %2", log_line_num, e.to_status()));
    }
    catch (const std::exception& e) {
        throw RuntimeError(ErrorCodes::RuntimeError,
                           util::format("Exception at line number %1: %2", log_line_num, e.what()));
    }
}

void print_help(std::string_view prog_name)
{
    std::cerr << "Synopsis: " << prog_name
              << " [changeset file]\n"
                 "\n"
                 "Where <changeset file> is the file system path of a file containing a\n"
                 "changeset encoded in hex/base64 compressed format or sync client trace-level log output.\n"
                 "If no changeset file is given, input shall be read from stdin.\n"
                 "\n"
                 "Options:\n"
                 "  -h, --help             Display command-line synopsis followed by the list of\n"
                 "                         available options.\n"
                 "  -H, --hex              Interpret file contents as hex encoded.\n"
                 "  -C, --compressed       Interpret file contents as Base64 encoded and compressed.\n"
                 "  -l, -input-is-logfile  Read input from stdin as a trace-level log file\n";
}

} // namespace
#endif

int main(int argc, const char** argv)
{
#if !REALM_DEBUG
    static_cast<void>(argc);
    static_cast<void>(argv);
    util::format(std::cerr, "changeset printing is disabled in Release mode, build in Debug mode to use this tool\n");
    return EXIT_FAILURE;
#else
    util::CliArgumentParser arg_parser;
    util::CliFlag hex(arg_parser, "hex", 'H');
    util::CliFlag compressed(arg_parser, "compressed", 'C');
    util::CliFlag help(arg_parser, "help", 'h');
    util::CliFlag as_logs(arg_parser, "input-is-logfile", 'l');

    std::fstream changeset_input_file;
    std::istream* changeset_input = &std::cin;
    try {
        auto arg_result = arg_parser.parse(argc, argv);
        if (arg_result.unmatched_arguments.size() > 1) {
            throw std::runtime_error(
                util::format("Expected one input argument, got %1", arg_result.unmatched_arguments.size()));
        }
        else if (arg_result.unmatched_arguments.size() == 1) {
            std::string file_path(arg_result.unmatched_arguments.front());
            if (file_path.empty() || file_path.front() == '-') {
                throw std::runtime_error(util::format("Expected path to file, got \"%1\"", file_path));
            }
            changeset_input_file.open(file_path);
            changeset_input = &changeset_input_file;
        }
    }
    catch (const std::runtime_error& e) {
        util::format(std::cerr, "Error parsing arguments: %1\n", e.what());
        return EXIT_FAILURE;
    }

    if (help) {
        print_help(argv[0]);
        return EXIT_SUCCESS;
    }

    try {
        if (as_logs) {
            print_changesets_in_log_file(*changeset_input);
        }
        else {
            print_changeset_in_file(*changeset_input, static_cast<bool>(hex), static_cast<bool>(compressed));
        }
    }
    catch (const std::exception& e) {
        util::format(std::cerr, "Error parsing/printing changesets: %1", e.what());
        return EXIT_FAILURE;
    }
#endif
}
