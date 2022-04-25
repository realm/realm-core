#include <cstring>
#include <cstdlib>
#include <string>
#include <iostream>

#include <realm/util/compression.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/load_file.hpp>
#include <realm/sync/changeset.hpp>
#include <realm/sync/changeset_parser.hpp>

using namespace realm;

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
    size_t buffer_size = util::base64_decoded_size(encoded_size);
    std::string decode_buffer;
    decode_buffer.resize(buffer_size);
    StringData window(p, encoded_size);
    util::Optional<size_t> decoded_size = util::base64_decode(window, decode_buffer.data(), buffer_size);
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

void print_changeset(const std::string& path, bool hex, bool compressed)
{
    std::string file_contents = util::load_file(path);
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
    sync::Changeset changeset = changeset_binary_to_sync_changeset(changeset_binary);
#if REALM_DEBUG
    changeset.print(std::cout);
#else
    std::cout << "changeset printing is disabled in Release mode, build in Debug mode to use this tool" << std::endl;
#endif
}

} // namespace

int main(int argc, char* argv[])
{
    std::string changeset_path;
    bool hex = false;
    bool compressed = false;

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
            else if (std::strcmp(arg, "-H") == 0 || std::strcmp(arg, "--hex") == 0) {
                hex = true;
                continue;
            }
            else if (std::strcmp(arg, "-C") == 0 || std::strcmp(arg, "--compressed") == 0) {
                compressed = true;
                continue;
            }
            std::cerr << "ERROR: Unknown option: " << arg << "\n";
            error = true;
        }
        argc = argc_2;

        i = 0;
        if (!get_string_value(changeset_path)) {
            error = true;
        }
        else if (i < argc) {
            error = true;
        }

        if (help) {
            std::cerr << "Synopsis: " << prog
                      << " <changeset file>\n"
                         "\n"
                         "Where <changeset file> is the file system path of a file containing a\n"
                         "changeset, possibly in hex format.\n"
                         "\n"
                         "Options:\n"
                         "  -h, --help           Display command-line synopsis followed by the list of\n"
                         "                       available options.\n"
                         "  -H, --hex            Interpret file contents as hex encoded.\n"
                         "  -C, --compressed     Interpret file contents as Base64 encoded and compressed.\n";
            return EXIT_SUCCESS;
        }

        if (error) {
            std::cerr << "ERROR: Bad command line.\n"
                         "Try `"
                      << prog << " --help`\n";
            return EXIT_FAILURE;
        }
    }

    print_changeset(changeset_path, hex, compressed);
}
