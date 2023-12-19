#include "realm/db.hpp"
#include "realm/transaction.hpp"
#include "realm/sync/history.hpp"
#include "realm/sync/instruction_applier.hpp"
#include "realm/sync/impl/clamped_hex_dump.hpp"
#include "realm/sync/noinst/client_history_impl.hpp"
#include "realm/sync/protocol.hpp"
#include "realm/sync/transform.hpp"
#include "realm/sync/changeset_parser.hpp"
#include "realm/util/compression.hpp"
#include "realm/util/cli_args.hpp"
#include "realm/util/from_chars.hpp"
#include "realm/util/load_file.hpp"
#include "realm/util/safe_int_ops.hpp"
#include <realm/util/base64.hpp>

#include <algorithm>
#include <iostream>
#include <string>
#include <charconv>
#include <type_traits>

using namespace realm;
using namespace realm::util;

enum class HeaderType { Upload, Download, DownloadChangeset, Ident, None };

struct MessageParseException : public std::runtime_error {
    using std::runtime_error::runtime_error;

    template <typename... Args>
    MessageParseException(const char* fmt, Args&&... args)
        : std::runtime_error(format(fmt, args...))
    {
    }
};

using ValueMap = std::map<std::string, uint64_t>;

class DownloadChangeset {
public:
    ValueMap values;
    Buffer<char> changeset_buffer;
    std::string_view remaining;
    void parse_download_message(std::string_view sv);
};

ValueMap parse_args(std::string_view sv)
{
    ValueMap values;
    if (sv[0] != '(') {
        throw MessageParseException("'(' not found");
    }
    const char* p = sv.begin();
    while (*p != ')') {
        p++;
        while (*p == ' ')
            p++;
        auto equal_pos = std::string_view(p, sv.end() - p).find('=');
        if (equal_pos == realm::npos)
            break;
        std::string kw = std::string(p, equal_pos);
        p += (equal_pos + 1);
        uint64_t i;
        auto parse_res = std::from_chars(p, sv.end(), i, 10);
        if (parse_res.ec != std::errc()) {
            if (strncmp(p, "true,", 5) == 0) {
                i = 1;
                p += 4;
            }
            if (strncmp(p, "false,", 6) == 0) {
                i = 0;
                p += 5;
            }
        }
        else {
            p = parse_res.ptr;
        }
        values.emplace(kw, i);
    }
    return values;
}

Buffer<char> changeset_hex_to_binary(const std::string changeset_hex, size_t changeset_size)
{
    Buffer<char> changeset_vec;

    changeset_vec.set_size(changeset_size);
    std::istringstream in{changeset_hex};
    int n;
    in >> std::hex >> n;
    int i = 0;
    while (in) {
        REALM_ASSERT(n >= 0 && n <= 255);
        changeset_vec[i++] = static_cast<char>(n);
        in >> std::hex >> n;
    }

    return changeset_vec;
}

Buffer<char> changeset_compressed_to_binary(const char* start, const char* end)
{
    // The size of the decompressed size data must come first
    char* p;
    size_t decompressed_size = size_t(strtol(start, &p, 10));
    REALM_ASSERT(*p == ' ');
    p++;

    // Decode from BASE64
    const size_t encoded_size = end - p;
    size_t buffer_size = util::base64_decoded_size(encoded_size);
    Buffer<char> decode_buffer;
    decode_buffer.set_size(buffer_size);
    StringData window(p, encoded_size);
    util::Optional<size_t> decoded_size = util::base64_decode(window, decode_buffer.data(), buffer_size);
    if (!decoded_size || *decoded_size > encoded_size) {
        throw MessageParseException("Invalid base64 value");
    }

    // Decompress
    Buffer<char> uncompressed_body_buffer(decompressed_size);
    std::error_code ec =
        util::compression::decompress(Span(decode_buffer).first(*decoded_size), uncompressed_body_buffer);

    if (ec) {
        throw MessageParseException("compression::inflate: %1", ec.message());
    }

    return uncompressed_body_buffer;
}

void DownloadChangeset::parse_download_message(std::string_view sv)
{
    auto start = sv.begin();
    auto end = strchr(start, ')') + 1;

    values = parse_args(std::string_view(start, end - start));
    auto pos = end;
    std::string kw = "Changeset";
    start = strstr(pos, kw.c_str()) + kw.size();
    if (*start == ':') {
        // Not compressed
        start++;
        end = strchr(start, '\n') + 1;
        changeset_buffer =
            std::move(changeset_hex_to_binary(std::string(start, end - start), values["changeset_size"]));
    }
    else {
        // Compressed
        start += 7;
        // end = strstr(start, "Connection"); // A bit of a HACK
        end = strchr(start, '\n');
        changeset_buffer = std::move(changeset_compressed_to_binary(start, end));
    }
    size_t changeset_size = changeset_buffer.size();
    size_t expected_size = values["changeset_size"];
    if (changeset_size != expected_size) {
        throw MessageParseException("changeset length is %1 but buffer size is %2", expected_size, changeset_size);
    }

    remaining = sv.substr(end - sv.begin());
}

std::pair<HeaderType, size_t> find_interesting_header(std::string_view sv)
{
    for (auto p = sv.begin(); p < sv.end(); p++) {
        p = strchr(p, ':');
        if (!p)
            return {HeaderType::None, 0};
        p++;
        if (*p == ' ' && (p[1] == 'S' || p[1] == 'R')) {
            p++;
            if (strncmp(p, "Received: DOWNLOAD CHANGESET", 28) == 0) {
                return {HeaderType::DownloadChangeset, p - sv.begin() + 28};
            }
            if (strncmp(p, "Received: DOWNLOAD", 18) == 0) {
                return {HeaderType::Download, p - sv.begin() + 18};
            }
            if (strncmp(p, "Received: IDENT", 15) == 0) {
                return {HeaderType::Ident, p - sv.begin() + 15};
            }
            if (strncmp(p, "Sending: UPLOAD", 15) == 0) {
                return {HeaderType::Upload, p - sv.begin() + 15};
            }
        }
    }
    return {HeaderType::None, 0};
}

void print_usage(std::string_view program_name)
{
    std::cout << "Synopsis: " << program_name << " -r <PATH-TO-REALM> -i <PATH-TO-MESSAGES> [OPTIONS]"
              << "\n"
                 "Options:\n"
                 "  -h, --help           Display command-line synopsis followed by the list of\n"
                 "                       available options.\n"
                 "  -e, --encryption-key  The file-system path of a file containing a 64-byte\n"
                 "                       encryption key to be used for accessing the specified\n"
                 "                       Realm file.\n"
                 "  -r, --realm          The file-system path to the realm to be created and/or have\n"
                 "                       state applied to.\n"
                 "  -i, --input          The file-system path a file containing UPLOAD, DOWNLOAD,\n"
                 "                       and IDENT messages to apply to the realm state\n"
                 "  -f, --flx-sync       Flexible sync session\n"
                 "  --verbose            Print all messages including trace messages to stderr\n"
                 "  -v, --version        Show the version of the Realm Sync release that this\n"
                 "                       command belongs to."
              << std::endl;
}

int main(int argc, const char** argv)
{
    CliArgumentParser arg_parser;
    CliFlag help_arg(arg_parser, "help", 'h');
    CliArgument realm_arg(arg_parser, "realm", 'r');
    CliArgument encryption_key_arg(arg_parser, "encryption-key", 'e');
    CliArgument input_arg(arg_parser, "input", 'i');
    CliFlag verbose_arg(arg_parser, "verbose");
    CliFlag flx_sync_arg(arg_parser, "flx-sync", 'f');
    auto arg_results = arg_parser.parse(argc, argv);

    std::unique_ptr<Logger> logger = std::make_unique<StderrLogger>(); // Throws
    if (verbose_arg) {
        logger->set_level_threshold(Logger::Level::all);
    }
    else {
        logger->set_level_threshold(Logger::Level::error);
    }

    if (help_arg) {
        print_usage(arg_results.program_name);
        return EXIT_SUCCESS;
    }

    if (!realm_arg) {
        logger->error("missing path to realm to apply changesets to");
        print_usage(arg_results.program_name);
        return EXIT_FAILURE;
    }
    if (!input_arg) {
        logger->error("missing path to messages to apply to realm");
        print_usage(arg_results.program_name);
        return EXIT_FAILURE;
    }
    auto realm_path = realm_arg.as<std::string>();

    std::string encryption_key;
    if (encryption_key_arg) {
        encryption_key = load_file(encryption_key_arg.as<std::string>());
    }

    realm::DBOptions db_opts(encryption_key.empty() ? nullptr : encryption_key.c_str());
    realm::sync::ClientReplication repl{};
    auto local_db = realm::DB::create(repl, realm_path, db_opts);
    auto& history = repl.get_history();

    auto input_contents = load_file(input_arg.as<std::string>());
    auto input_view = std::string_view{input_contents};
    std::vector<DownloadChangeset> downloaded_changesets;
    std::vector<realm::sync::RemoteChangeset> changesets;
    realm::sync::SyncProgress progress;
    uint64_t downloadable_bytes = 0;

    while (!input_view.empty()) {
        auto [type, idx] = find_interesting_header(input_view);
        if (type == HeaderType::None) {
            break;
        }
        input_view = input_view.substr(idx);
        if (input_view[0] != '(') {
            logger->error("*** Error parsing input message file: '(' not found");
            return EXIT_FAILURE;
        }
        switch (type) {
            case HeaderType::DownloadChangeset: {
                downloaded_changesets.emplace_back();
                DownloadChangeset& downloaded_changeset = downloaded_changesets.back();
                try {
                    downloaded_changeset.parse_download_message(input_view);
                }
                catch (const MessageParseException& e) {
                    logger->error("*** Error parsing input message file: %1", e.what());
                    return EXIT_FAILURE;
                }

                changesets.emplace_back();
                auto& cur_changeset = changesets.back();

                util::SimpleInputStream input_stream{Span(downloaded_changeset.changeset_buffer)};
                sync::Changeset changeset;
                sync::parse_changeset(input_stream, changeset);
                // changeset.print();

                realm::BinaryData changeset_data{downloaded_changeset.changeset_buffer.data(),
                                                 downloaded_changeset.changeset_buffer.size()};
                cur_changeset.data = changeset_data;
                cur_changeset.origin_file_ident = downloaded_changeset.values["origin_file_ident"];
                cur_changeset.original_changeset_size = downloaded_changeset.values["original_changeset_size"];
                cur_changeset.origin_timestamp = downloaded_changeset.values["origin_timestamp"];
                cur_changeset.remote_version = downloaded_changeset.values["server_version"];

                input_view = downloaded_changeset.remaining;
                break;
            }
            case HeaderType::Download: {
                auto start = input_view.begin();
                auto end = strchr(start, ')') + 1;
                auto download_header = parse_args(std::string_view(start, end - start));

                progress.download.server_version = sync::version_type(download_header["download_server_version"]);
                progress.download.last_integrated_client_version =
                    sync::version_type(download_header["download_client_version"]);
                progress.upload.client_version = sync::version_type(download_header["upload_client_version"]);
                progress.upload.last_integrated_server_version =
                    sync::version_type(download_header["upload_server_version"]);
                progress.latest_server_version.version = sync::version_type(download_header["latest_server_version"]);
                progress.latest_server_version.salt = sync::salt_type(download_header["latest_server_version_salt"]);
                downloadable_bytes = download_header["downloadable_bytes"];
                realm::sync::DownloadBatchState batch_state;
                if (flx_sync_arg) {
                    bool last_in_batch = bool(download_header["last_in_batch"]);
                    batch_state =
                        last_in_batch ? sync::DownloadBatchState::LastInBatch : sync::DownloadBatchState::MoreToCome;
                }
                else {
                    batch_state = sync::DownloadBatchState::SteadyState;
                }

                auto num_changesets = changesets.size();
                if (num_changesets != download_header["num_changesets"]) {
                    throw MessageParseException("Number of collected changesets is %1 but we should have found %2",
                                                downloaded_changesets.size(), download_header["num_changesets"]);
                }

                if (num_changesets) {
                    realm::sync::VersionInfo version_info;
                    auto transact = bool(flx_sync_arg) ? local_db->start_write() : local_db->start_read();
                    history.integrate_server_changesets(progress, &downloadable_bytes, changesets, version_info,
                                                        batch_state, *logger, transact);
                    downloaded_changesets.clear();
                    changesets.clear();
                }
                break;
            }
            case HeaderType::Upload: {
                auto start = input_view.begin();
                auto end = strchr(start, ')') + 1;
                auto upload_header = parse_args(std::string_view(start, end - start));
                if (upload_header["num_changesets"] == 1) {
                    auto changeset_header = strstr(end, "Fetching changeset for upload ");
                    input_view = input_view.substr(changeset_header - input_view.begin() + 30);

                    DownloadChangeset upload_changeset;
                    upload_changeset.parse_download_message(input_view);

                    history.set_local_origin_timestamp_source([&]() {
                        return upload_changeset.values["origin_timestamp"];
                    });

                    util::SimpleInputStream input_stream{upload_changeset.changeset_buffer};
                    sync::Changeset changeset;
                    sync::parse_changeset(input_stream, changeset);
                    // changeset.print();

                    auto transaction = local_db->start_write();
                    realm::sync::InstructionApplier applier(*transaction);
                    applier.apply(changeset, logger.get());
                    auto generated_version = transaction->commit();
                    logger->debug("integrated local changesets as version %1", generated_version);
                    history.set_local_origin_timestamp_source(realm::sync::generate_changeset_timestamp);
                }
                break;
            }
            case HeaderType::Ident: {
                auto start = input_view.begin();
                auto end = strchr(start, ')') + 1;
                auto ident_header = parse_args(std::string_view(start, end - start));
                sync::SaltedFileIdent file_ident;
                file_ident.ident = ident_header["client_file_ident"];
                file_ident.salt = ident_header["client_file_ident_salt"];
                history.set_client_file_ident(file_ident, true);
                input_view = input_view.substr(end - input_view.begin());
                break;
            }
            default:
                break;
        }
    }

    return EXIT_SUCCESS;
}
