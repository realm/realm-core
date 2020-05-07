#include <cstring>
#include <cstdlib>
#include <array>
#include <string>
#include <iostream>
#include <sstream>
#include <vector>


#include "encryption_transformer.hpp"

#include <realm/noinst/command_line_util.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/file.hpp>
#include <realm/util/load_file.hpp>

using namespace realm;

void usage(char* prog)
{
    std::cerr
        << "Synopsis: " << prog
        << " [-i INPUT_KEY_FILE][-o OUTPUT_KEY_FILE][-l LIST_FILE_PATH][-f FILE][-j JOBS][-v][-h]\n"
           "Transform Realm file encryption state.\n"
           "Both the input and output keys are optional.\n"
           "When a key is omitted, it means no encryption is used in that direction.\n"
           "\n"
           "Options:\n"
           "  -h, --help                   Display usage\n"
           "  -i, --input_key_file         The path to a file containing the 64 byte encryption key to be used for "
           "reading\n"
           "  -o, --output_key_file        The path to a file containing the 64 byte encryption key to be used for "
           "writing\n"
           "  -l, --list_file              The path to a file containing a list of realm files to operate on\n"
           "  -f, --file                   The path to a single Realm file to operate on\n"
           "  -n, --input_key_env          The name of the environment variable containing the Base64-encoding of\n"
           "                               the 64 byte encryption key to be used for reading\n"
           "  -t, --output_key_env         The name of the environment variable containing the Base64 encoding of\n"
           "                               the 64 byte encryption key to be used for writing\n"
           "  -t, --jobs                   Number of parallel jobs\n"
           "  -v, --verbose                Turn on verbose output. WARNING: The keys will be visible on the "
           "console!\n"
           "\n";
}

const char* optstring = "hi:o:l:f:vn:t:j:";

struct option longopts[] = {{"help", no_argument, nullptr, 'h'},
                            {"input_key_file", optional_argument, nullptr, 'i'},
                            {"output_key_file", optional_argument, nullptr, 'o'},
                            {"list_file", optional_argument, nullptr, 'l'},
                            {"file", optional_argument, nullptr, 'f'},
                            {"verbose", no_argument, nullptr, 'v'},
                            {"input_key_env", optional_argument, nullptr, 'n'},
                            {"output_key_env", optional_argument, nullptr, 't'},
                            {"jobs", optional_argument, nullptr, 'j'}};

struct EncryptionCLIArgs {
    std::string input_key_file;
    std::string output_key_file;
    std::string list_file;
    std::string file;
    std::string input_key_env_name;
    std::string output_key_env_name;
    bool verbose = false;
    util::Optional<size_t> jobs;
};

EncryptionCLIArgs parse_arguments(int argc, char* argv[])
{
    EncryptionCLIArgs config{};

    int ch;
    while ((ch = getopt_long(argc, argv, optstring, longopts, nullptr)) != -1) {
        switch (ch) {
            case 'h':
                usage(argv[0]);
                std::exit(EXIT_SUCCESS);
            case 'i':
                config.input_key_file = std::string(optarg);
                break;
            case 'o':
                config.output_key_file = std::string(optarg);
                break;
            case 'l':
                config.list_file = std::string(optarg);
                break;
            case 'f':
                config.file = std::string(optarg);
                break;
            case 'n':
                config.input_key_env_name = std::string(optarg);
                break;
            case 't':
                config.output_key_env_name = std::string(optarg);
                break;
            case 'j':
                config.jobs = std::stoi(optarg);
                break;
            case 'v':
                config.verbose = true;
                break;
            default:
                usage(argv[0]);
                std::exit(EXIT_FAILURE);
        }
    }

    if (optind != argc) {
        usage(argv[0]);
        std::exit(EXIT_FAILURE);
    }

    return config;
}

std::string base64_decode(const std::string& encoded)
{
    std::size_t buffer_size = util::base64_decoded_size(encoded.size());
    std::string decoded;
    decoded.resize(buffer_size);
    util::Optional<std::size_t> decoded_size =
        util::base64_decode(StringData{encoded}, &decoded.front(), decoded.size());
    if (!decoded_size)
        throw std::runtime_error("Invalid Base64 input");
    decoded.resize(*decoded_size); // Throws
    return decoded;
}

std::array<char, 64> read_key_from_file(const std::string& path)
{
    std::string contents = util::load_file_and_chomp(path);
    if (contents.size() != 64) {
        throw std::runtime_error("Key must be 64 bytes in file " + path);
    }
    std::array<char, 64> key = std::array<char, 64>();
    std::memcpy(key.data(), contents.data(), 64);
    return key;
}

std::array<char, 64> read_key_from_env_var(const std::string& var_name)
{
    std::array<char, 64> key = std::array<char, 64>();
    if (const char* env_p = std::getenv(var_name.c_str())) {
        std::string encoded = env_p; // Copy
        std::string decoded = base64_decode(encoded);
        if (decoded.size() != 64) {
            throw std::runtime_error("Key must be 64 bytes in envvar `" + var_name + "`");
        }
        std::memcpy(key.data(), decoded.data(), 64);
    }
    else {
        throw std::runtime_error("Could not find the variable '" + var_name + "' in your environment");
    }
    return key;
}

int main(int argc, char* argv[])
{
    EncryptionCLIArgs cli_config = parse_arguments(argc, argv);
    encryption_transformer::Configuration config;
    config.verbose = cli_config.verbose;
    config.jobs = cli_config.jobs;
    if (cli_config.jobs && *cli_config.jobs <= 0) {
        std::cerr << "Config error: jobs cannot be less than 1\n\n";
        usage(argv[0]);
        std::exit(EXIT_FAILURE);
    }
    if (!cli_config.input_key_file.empty()) {
        config.input_key = read_key_from_file(cli_config.input_key_file);
    }
    if (!cli_config.output_key_file.empty()) {
        config.output_key = read_key_from_file(cli_config.output_key_file);
    }
    if (!cli_config.input_key_env_name.empty()) {
        if (config.input_key) {
            std::cerr << "Config error: multiple input keys specified\n\n";
            usage(argv[0]);
            std::exit(EXIT_FAILURE);
        }
        config.input_key = read_key_from_env_var(cli_config.input_key_env_name);
    }
    if (!cli_config.output_key_env_name.empty()) {
        if (config.output_key) {
            std::cerr << "Config error: multiple output keys specified\n\n";
            usage(argv[0]);
            std::exit(EXIT_FAILURE);
        }
        config.output_key = read_key_from_env_var(cli_config.output_key_env_name);
    }
    if (!cli_config.file.empty()) {
        config.target_path = cli_config.file;
        config.type = encryption_transformer::Configuration::TransformType::File;
    }
    if (!cli_config.list_file.empty()) {
        if (!config.target_path.empty()) {
            std::cerr << "Config error: multiple target files (-l -f) specified\n\n";
            usage(argv[0]);
            std::exit(EXIT_FAILURE);
        }
        config.target_path = cli_config.list_file;
        config.type = encryption_transformer::Configuration::TransformType::FileContaingPaths;
    }

    if (config.verbose) {
        std::cout << "config.target_path = " << config.target_path << "\n";
        std::cout << "config.type = "
                  << (config.type == encryption_transformer::Configuration::TransformType::File
                          ? "Single File"
                          : "File Containing Paths")
                  << "\n";
        std::cout << "input key: " << (config.input_key ? std::string(config.input_key->data(), 64) : "none")
                  << std::endl;
        std::cout << "output key: " << (config.output_key ? std::string(config.output_key->data(), 64) : "none")
                  << std::endl;
    }

    size_t result = encryption_transformer::encrypt_transform(config);
    std::cout << "transformed " << result << " files successfully" << std::endl;

    return EXIT_SUCCESS;
}
