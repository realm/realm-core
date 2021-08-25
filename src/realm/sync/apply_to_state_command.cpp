#include "realm/db.hpp"
#include "realm/sync/history.hpp"
#include "realm/sync/instruction_applier.hpp"
#include "realm/sync/impl/clamped_hex_dump.hpp"
#include "realm/sync/noinst/client_history_impl.hpp"
#include "realm/sync/protocol.hpp"
#include "realm/sync/transform.hpp"
#include "realm/sync/changeset_parser.hpp"
#include "realm/sync/noinst/compression.hpp"
#include "realm/util/cli_args.hpp"
#include "realm/util/from_chars.hpp"
#include "realm/util/load_file.hpp"
#include "realm/util/safe_int_ops.hpp"
#include "realm/util/string_view.hpp"

#include <external/mpark/variant.hpp>

#include <algorithm>
#include <iostream>
#include <string>
#include <type_traits>


using namespace realm::util;

template <typename T>
using ParseResult = std::pair<T, StringView>;

struct ServerIdentMessage {
    realm::sync::session_ident_type session_ident;
    realm::sync::SaltedFileIdent file_ident;

    static ParseResult<ServerIdentMessage> parse(StringView sv);
};

struct DownloadMessage {
    realm::sync::session_ident_type session_ident;
    realm::sync::SyncProgress progress;
    realm::sync::SaltedVersion latest_server_version;
    uint64_t downloadable_bytes;

    Buffer<char> uncompressed_body_buffer;
    std::vector<realm::sync::Transformer::RemoteChangeset> changesets;

    static ParseResult<DownloadMessage> parse(StringView sv, Logger& logger);
};

struct UploadMessage {
    realm::sync::session_ident_type session_ident;
    realm::sync::UploadCursor upload_progress;
    realm::sync::version_type locked_server_version;

    Buffer<char> uncompressed_body_buffer;
    std::vector<realm::sync::Changeset> changesets;

    static ParseResult<UploadMessage> parse(StringView sv, Logger& logger);
};

using Message = mpark::variant<ServerIdentMessage, DownloadMessage, UploadMessage>;

struct MessageParseException : public std::runtime_error {
    using std::runtime_error::runtime_error;

    template <typename... Args>
    MessageParseException(const char* fmt, Args&&... args)
        : std::runtime_error(format(fmt, args...))
    {
    }
};

// These functions will parse the space/new-line delimited headers found at the beginning of
// messages and changesets.
StringView parse_header_element(StringView sv, char)
{
    return sv;
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<std::remove_reference_t<T>>>>
StringView parse_header_value(StringView sv, T&& cur_arg)
{
    auto parse_res = realm::util::from_chars(sv.begin(), sv.end(), cur_arg, 10);
    if (parse_res.ec != std::errc{}) {
        throw MessageParseException("error parsing integer in header line: %1",
                                    std::make_error_code(parse_res.ec).message());
    }

    return sv.substr(parse_res.ptr - sv.begin());
}

StringView parse_header_value(StringView sv, StringView& cur_arg)
{
    auto delim_at = std::find(sv.begin(), sv.end(), ' ');
    if (delim_at == sv.end()) {
        throw MessageParseException("reached end of header line prematurely");
    }

    auto sub_str_len = std::distance(sv.begin(), delim_at);
    cur_arg = StringView(sv.begin(), sub_str_len);

    return sv.substr(sub_str_len);
}

template <typename T, typename... Args>
StringView parse_header_element(StringView sv, char end_delim, T&& cur_arg, Args&&... next_args)
{
    if (sv.empty()) {
        throw MessageParseException("cannot parse an empty header line");
    }
    sv = parse_header_value(sv, std::forward<T&&>(cur_arg));

    if (sv.front() == ' ') {
        return parse_header_element(sv.substr(1), end_delim, next_args...);
    }
    if (sv.front() == end_delim) {
        return sv.substr(1);
    }
    throw MessageParseException("found invalid character in header line");
}

template <typename... Args>
StringView parse_header_line(StringView sv, char end_delim, Args&&... args)
{
    return parse_header_element(sv, end_delim, args...);
}

struct MessageBody {
    StringView body_view;
    StringView remaining;
    Buffer<char> uncompressed_body_buffer;
    static MessageBody parse(StringView sv, std::size_t compressed_body_size, std::size_t uncompressed_body_size,
                             bool is_body_compressed);
};

MessageBody MessageBody::parse(StringView sv, std::size_t compressed_body_size, std::size_t uncompressed_body_size,
                               bool is_body_compressed)
{
    MessageBody ret;
    if (is_body_compressed) {
        if (sv.size() < compressed_body_size) {
            throw MessageParseException("compressed message body is bigger (%1) than available bytes (%2)",
                                        compressed_body_size, sv.size());
        }

        ret.uncompressed_body_buffer.set_size(uncompressed_body_size);
        auto ec = realm::_impl::compression::decompress(sv.data(), compressed_body_size,
                                                        ret.uncompressed_body_buffer.data(), uncompressed_body_size);
        if (ec) {
            throw MessageParseException("error decompressing message body: %1", ec.message());
        }

        ret.remaining = sv.substr(compressed_body_size);
        ret.body_view = StringView{ret.uncompressed_body_buffer.data(), uncompressed_body_size};
    }
    else {
        if (sv.size() < uncompressed_body_size) {
            throw MessageParseException("message body is bigger (%1) than available bytes (%2)",
                                        uncompressed_body_size, sv.size());
        }
        ret.body_view = sv.substr(0, uncompressed_body_size);
        ret.remaining = sv.substr(uncompressed_body_size);
    }

    return ret;
}

ParseResult<Message> parse_message(StringView sv, Logger& logger)
{
    StringView message_type;
    sv = parse_header_element(sv, ' ', message_type);

    if (message_type == "download") {
        return DownloadMessage::parse(sv, logger);
    }
    else if (message_type == "upload") {
        return UploadMessage::parse(sv, logger);
    }
    else if (message_type == "ident") {
        return ServerIdentMessage::parse(sv);
    }
    throw MessageParseException("could not find valid message in input");
}

ParseResult<ServerIdentMessage> ServerIdentMessage::parse(StringView sv)
{
    ServerIdentMessage ret;
    sv = parse_header_line(sv, '\n', ret.session_ident, ret.file_ident.ident, ret.file_ident.salt);

    return std::make_pair(std::move(ret), sv);
}

ParseResult<DownloadMessage> DownloadMessage::parse(StringView sv, Logger& logger)
{
    DownloadMessage ret;
    int is_body_compressed;
    std::size_t uncompressed_body_size, compressed_body_size;

    sv = parse_header_line(sv, '\n', ret.session_ident, ret.progress.download.server_version,
                           ret.progress.download.last_integrated_client_version, ret.latest_server_version.version,
                           ret.latest_server_version.salt, ret.progress.upload.client_version,
                           ret.progress.upload.last_integrated_server_version, ret.downloadable_bytes,
                           is_body_compressed, uncompressed_body_size, compressed_body_size);

    auto message_body = MessageBody::parse(sv, compressed_body_size, uncompressed_body_size, is_body_compressed);
    ret.uncompressed_body_buffer = std::move(message_body.uncompressed_body_buffer);
    sv = message_body.remaining;
    auto body_view = message_body.body_view;

    logger.trace("decoding download message. "
                 "{download: {server: %1, client: %2} upload: {server: %3, client: %4}, latest: %5}",
                 ret.progress.download.server_version, ret.progress.download.last_integrated_client_version,
                 ret.progress.upload.last_integrated_server_version, ret.progress.upload.client_version,
                 ret.latest_server_version.version);

    while (!body_view.empty()) {
        realm::sync::Transformer::RemoteChangeset cur_changeset;
        std::size_t changeset_size;
        body_view =
            parse_header_line(body_view, ' ', cur_changeset.remote_version,
                              cur_changeset.last_integrated_local_version, cur_changeset.origin_timestamp,
                              cur_changeset.origin_file_ident, cur_changeset.original_changeset_size, changeset_size);
        if (changeset_size > body_view.size()) {
            throw MessageParseException("changeset length is %1 but buffer size is %2", changeset_size,
                                        body_view.size());
        }

        realm::sync::Changeset parsed_changeset;
        realm::_impl::SimpleNoCopyInputStream changeset_stream(body_view.data(), changeset_size);
        realm::sync::parse_changeset(changeset_stream, parsed_changeset);
        logger.trace("found download changeset: serverVersion: %1, clientVersion: %2, origin: %3 %4",
                     cur_changeset.remote_version, cur_changeset.last_integrated_local_version,
                     cur_changeset.origin_file_ident, parsed_changeset);
        realm::BinaryData changeset_data{body_view.data(), changeset_size};
        cur_changeset.data = changeset_data;
        ret.changesets.push_back(cur_changeset);
        body_view = body_view.substr(changeset_size);
    }

    return std::make_pair(std::move(ret), sv);
}

ParseResult<UploadMessage> UploadMessage::parse(StringView sv, Logger& logger)
{
    UploadMessage ret;
    int is_body_compressed;
    std::size_t uncompressed_body_size, compressed_body_size;

    sv = parse_header_line(sv, '\n', ret.session_ident, is_body_compressed, uncompressed_body_size,
                           compressed_body_size, ret.upload_progress.client_version,
                           ret.upload_progress.last_integrated_server_version, ret.locked_server_version);

    auto message_body = MessageBody::parse(sv, compressed_body_size, uncompressed_body_size, is_body_compressed);
    ret.uncompressed_body_buffer = std::move(message_body.uncompressed_body_buffer);
    sv = message_body.remaining;
    auto body_view = message_body.body_view;

    while (!body_view.empty()) {
        realm::sync::Changeset cur_changeset;
        std::size_t changeset_size;
        body_view =
            parse_header_line(body_view, ' ', cur_changeset.version, cur_changeset.last_integrated_remote_version,
                              cur_changeset.origin_timestamp, cur_changeset.origin_file_ident, changeset_size);
        if (changeset_size > body_view.size()) {
            throw MessageParseException("changeset length in upload message is %1 but bufer size is %2",
                                        changeset_size, body_view.size());
        }

        logger.trace("found upload changeset: %1 %2 %3 %4 %5", cur_changeset.last_integrated_remote_version,
                     cur_changeset.version, cur_changeset.origin_timestamp, cur_changeset.origin_file_ident,
                     changeset_size);
        realm::_impl::SimpleNoCopyInputStream changeset_stream(body_view.data(), changeset_size);
        try {
            realm::sync::parse_changeset(changeset_stream, cur_changeset);
        }
        catch (...) {
            logger.error("error decoding changeset after instructions %1", cur_changeset);
            throw;
        }
        logger.trace("Decoded changeset: %1", cur_changeset);
        ret.changesets.push_back(std::move(cur_changeset));
        body_view = body_view.substr(changeset_size);
    }

    return std::make_pair(std::move(ret), sv);
}

void print_usage(StringView program_name)
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
    auto arg_results = arg_parser.parse(argc, argv);

    std::unique_ptr<RootLogger> logger = std::make_unique<StderrLogger>(); // Throws
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
    realm::_impl::ClientHistoryImpl history{realm_path};
    auto local_db = realm::DB::create(history, db_opts);

    auto input_contents = load_file(input_arg.as<std::string>());
    auto input_view = StringView{input_contents};
    while (!input_view.empty()) {
        ParseResult<Message> message;
        try {
            message = parse_message(input_view, *logger);
        }
        catch (const MessageParseException& e) {
            logger->error("Error parsing input message file: %1", e.what());
            return EXIT_FAILURE;
        }

        input_view = message.second;
        bool download_integration_failed = false;
        mpark::visit(realm::util::overload{
                         [&](const DownloadMessage& download_message) {
                             realm::sync::VersionInfo version_info;
                             realm::sync::ClientReplication::IntegrationError integration_error;
                             if (!history.integrate_server_changesets(
                                     download_message.progress, &download_message.downloadable_bytes,
                                     download_message.changesets.data(), download_message.changesets.size(),
                                     version_info, integration_error, *logger, nullptr)) {
                                 logger->error("Error applying download message to realm");
                                 download_integration_failed = true;
                             }
                         },
                         [&](const UploadMessage& upload_message) {
                             for (const auto& changeset : upload_message.changesets) {
                                 history.set_local_origin_timestamp_source([&]() {
                                     return changeset.origin_timestamp;
                                 });
                                 auto transaction = local_db->start_write();
                                 realm::sync::InstructionApplier applier(*transaction);
                                 applier.apply(changeset, logger.get());
                                 auto generated_version = transaction->commit();
                                 logger->debug("integrated local changesets as version %1", generated_version);
                                 history.set_local_origin_timestamp_source(realm::sync::generate_changeset_timestamp);
                             }
                         },
                         [&](const ServerIdentMessage& ident_message) {
                             history.set_client_file_ident(ident_message.file_ident, true);
                         }},
                     message.first);
        if (download_integration_failed) {
            return EXIT_FAILURE;
        }
    }

    return EXIT_SUCCESS;
}
