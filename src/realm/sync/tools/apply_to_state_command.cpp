#include "realm/db.hpp"
#include "realm/transaction.hpp"
#include "realm/sync/history.hpp"
#include "realm/sync/instruction_applier.hpp"
#include "realm/sync/impl/clamped_hex_dump.hpp"
#include "realm/sync/noinst/client_history_impl.hpp"
#include "realm/sync/noinst/protocol_codec.hpp"
#include "realm/sync/protocol.hpp"
#include "realm/sync/transform.hpp"
#include "realm/sync/changeset_parser.hpp"
#include "realm/util/cli_args.hpp"
#include "realm/util/compression.hpp"
#include "realm/util/load_file.hpp"
#include "realm/util/safe_int_ops.hpp"

#include <external/mpark/variant.hpp>

#include <algorithm>
#include <iostream>
#include <string>
#include <string_view>
#include <type_traits>

namespace {
using namespace realm;
using namespace realm::util;
using namespace realm::_impl;

struct ServerIdentMessage {
    realm::sync::session_ident_type session_ident;
    realm::sync::SaltedFileIdent file_ident;

    static ServerIdentMessage parse(HeaderLineParser& msg);
};

struct DownloadMessage {
    realm::sync::session_ident_type session_ident;
    realm::sync::SyncProgress progress;
    realm::sync::SaltedVersion latest_server_version;
    uint64_t downloadable_bytes;
    realm::sync::DownloadBatchState batch_state;
    int64_t query_version;

    Buffer<char> uncompressed_body_buffer;
    std::vector<realm::sync::RemoteChangeset> changesets;

    static DownloadMessage parse(HeaderLineParser& msg, Logger& logger, bool is_flx_sync);
};

struct UploadMessage {
    realm::sync::session_ident_type session_ident;
    realm::sync::UploadCursor upload_progress;
    realm::sync::version_type locked_server_version;

    Buffer<char> uncompressed_body_buffer;
    std::vector<realm::sync::Changeset> changesets;

    static UploadMessage parse(HeaderLineParser& msg, Logger& logger);
};

using Message = mpark::variant<ServerIdentMessage, DownloadMessage, UploadMessage>;

Message parse_message(HeaderLineParser& msg, Logger& logger, bool is_flx_sync)
{
    auto message_type = msg.read_next<std::string_view>();
    if (message_type == "download") {
        return DownloadMessage::parse(msg, logger, is_flx_sync);
    }
    else if (message_type == "upload") {
        return UploadMessage::parse(msg, logger);
    }
    else if (message_type == "ident") {
        return ServerIdentMessage::parse(msg);
    }
    throw ProtocolCodecException("could not find valid message in input");
}

ServerIdentMessage ServerIdentMessage::parse(HeaderLineParser& msg)
{
    ServerIdentMessage ret;
    ret.session_ident = msg.read_next<sync::session_ident_type>();
    ret.file_ident.ident = msg.read_next<sync::file_ident_type>();
    ret.file_ident.salt = msg.read_next<sync::salt_type>('\n');

    return ret;
}

DownloadMessage DownloadMessage::parse(HeaderLineParser& msg, Logger& logger, bool is_flx_sync)
{
    DownloadMessage ret;

    ret.session_ident = msg.read_next<sync::session_ident_type>();
    ret.progress.download.server_version = msg.read_next<sync::version_type>();
    ret.progress.download.last_integrated_client_version = msg.read_next<sync::version_type>();
    ret.progress.latest_server_version.version = msg.read_next<sync::version_type>();
    ret.progress.latest_server_version.salt = msg.read_next<sync::salt_type>();
    ret.progress.upload.client_version = msg.read_next<sync::version_type>();
    ret.progress.upload.last_integrated_server_version = msg.read_next<sync::version_type>();
    if (is_flx_sync) {
        ret.query_version = msg.read_next<int64_t>();
        ret.batch_state = static_cast<sync::DownloadBatchState>(msg.read_next<int>());
        switch (ret.batch_state) {
            case realm::sync::DownloadBatchState::SteadyState:
            case realm::sync::DownloadBatchState::MoreToCome:
            case realm::sync::DownloadBatchState::LastInBatch:
                break;
            default:
                throw ProtocolCodecException(
                    util::format("invalid batch state in download msg: %1", static_cast<int>(ret.batch_state)));
        }
    }
    else {
        ret.query_version = 0;
        ret.batch_state = sync::DownloadBatchState::SteadyState;
    }
    ret.downloadable_bytes = msg.read_next<int64_t>();
    auto is_body_compressed = msg.read_next<bool>();
    auto uncompressed_body_size = msg.read_next<size_t>();
    auto compressed_body_size = msg.read_next<size_t>('\n');

    logger.trace(util::LogCategory::changeset,
                 "decoding download message. "
                 "{download: {server: %1, client: %2} upload: {server: %3, client: %4}, latest: %5}",
                 ret.progress.download.server_version, ret.progress.download.last_integrated_client_version,
                 ret.progress.upload.last_integrated_server_version, ret.progress.upload.client_version,
                 ret.latest_server_version.version);

    std::string_view body_str;
    if (is_body_compressed) {
        ret.uncompressed_body_buffer.set_size(uncompressed_body_size);
        auto compressed_body = msg.read_sized_data<BinaryData>(compressed_body_size);
        std::error_code ec = util::compression::decompress(compressed_body, ret.uncompressed_body_buffer);

        if (ec) {
            throw ProtocolCodecException("error decompressing download message");
        }

        body_str = std::string_view(ret.uncompressed_body_buffer.data(), uncompressed_body_size);
    }
    else {
        body_str = msg.read_sized_data<std::string_view>(uncompressed_body_size);
    }

    HeaderLineParser body(body_str);
    while (!body.at_end()) {
        realm::sync::RemoteChangeset cur_changeset;
        cur_changeset.remote_version = body.read_next<sync::version_type>();
        cur_changeset.last_integrated_local_version = body.read_next<sync::version_type>();
        cur_changeset.origin_timestamp = body.read_next<sync::timestamp_type>();
        cur_changeset.origin_file_ident = body.read_next<sync::file_ident_type>();
        cur_changeset.original_changeset_size = body.read_next<size_t>();
        auto changeset_size = body.read_next<size_t>();

        realm::sync::Changeset parsed_changeset;
        auto changeset_data = body.read_sized_data<BinaryData>(changeset_size);
        auto changeset_stream = realm::util::SimpleInputStream(changeset_data);
        realm::sync::parse_changeset(changeset_stream, parsed_changeset);
        logger.trace(util::LogCategory::changeset,
                     "found download changeset: serverVersion: %1, clientVersion: %2, origin: %3 %4",
                     cur_changeset.remote_version, cur_changeset.last_integrated_local_version,
                     cur_changeset.origin_file_ident, parsed_changeset);
        cur_changeset.data = changeset_data;
        ret.changesets.push_back(cur_changeset);
    }

    return ret;
}

UploadMessage UploadMessage::parse(HeaderLineParser& msg, Logger& logger)
{
    UploadMessage ret;

    ret.session_ident = msg.read_next<sync::session_ident_type>();
    auto is_body_compressed = msg.read_next<bool>();
    auto uncompressed_body_size = msg.read_next<size_t>();
    auto compressed_body_size = msg.read_next<size_t>();
    ret.upload_progress.client_version = msg.read_next<sync::version_type>();
    ret.upload_progress.last_integrated_server_version = msg.read_next<sync::version_type>();
    ret.locked_server_version = msg.read_next<sync::version_type>('\n');

    // if is_body_compressed == true, we must decompress the received body.
    std::string_view body_str;
    if (is_body_compressed) {
        ret.uncompressed_body_buffer.set_size(uncompressed_body_size);
        auto compressed_body = msg.read_sized_data<BinaryData>(compressed_body_size);
        std::error_code ec = util::compression::decompress(compressed_body, ret.uncompressed_body_buffer);

        if (ec) {
            throw ProtocolCodecException("error decompressing upload message");
        }

        body_str = std::string_view(ret.uncompressed_body_buffer.data(), uncompressed_body_size);
    }
    else {
        body_str = msg.read_sized_data<std::string_view>(uncompressed_body_size);
    }

    HeaderLineParser body(body_str);
    while (!body.at_end()) {
        realm::sync::Changeset cur_changeset;
        cur_changeset.version = body.read_next<sync::version_type>();
        cur_changeset.last_integrated_remote_version = body.read_next<sync::version_type>();
        cur_changeset.origin_timestamp = body.read_next<sync::timestamp_type>();
        cur_changeset.origin_file_ident = body.read_next<sync::file_ident_type>();
        auto changeset_size = body.read_next<size_t>();

        auto changeset_buffer = body.read_sized_data<BinaryData>(changeset_size);

        logger.trace(util::LogCategory::changeset, "found upload changeset: %1 %2 %3 %4 %5",
                     cur_changeset.last_integrated_remote_version, cur_changeset.version,
                     cur_changeset.origin_timestamp, cur_changeset.origin_file_ident, changeset_size);
        realm::util::SimpleInputStream changeset_stream(changeset_buffer);
        try {
            realm::sync::parse_changeset(changeset_stream, cur_changeset);
        }
        catch (...) {
            logger.error("error decoding changeset after instructions %1", cur_changeset);
            throw;
        }
        logger.trace(util::LogCategory::changeset, "Decoded changeset: %1", cur_changeset);
        ret.changesets.push_back(std::move(cur_changeset));
    }

    return ret;
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

} // namespace

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

    std::unique_ptr<Logger> logger =
        std::make_unique<StderrLogger>(verbose_arg ? Logger::Level::all : Logger::Level::error);

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
    HeaderLineParser msg(input_contents);
    while (!msg.at_end()) {
        Message message;
        try {
            message = parse_message(msg, *logger, bool(flx_sync_arg));
        }
        catch (const ProtocolCodecException& e) {
            logger->error("Error parsing input message file: %1", e.what());
            return EXIT_FAILURE;
        }

        mpark::visit(realm::util::overload{
                         [&](const DownloadMessage& download_message) {
                             realm::sync::VersionInfo version_info;
                             TransactionRef tr;
                             switch (download_message.batch_state) {
                                 case realm::sync::DownloadBatchState::SteadyState:
                                     tr = local_db->start_read();
                                     break;
                                 case realm::sync::DownloadBatchState::LastInBatch:
                                     [[fallthrough]];
                                 case realm::sync::DownloadBatchState::MoreToCome:
                                     tr = local_db->start_write();
                                     break;
                             }
                             history.integrate_server_changesets(download_message.progress,
                                                                 download_message.downloadable_bytes,
                                                                 download_message.changesets, version_info,
                                                                 download_message.batch_state, *logger, tr);
                         },
                         [&](const UploadMessage& upload_message) {
                             for (const auto& changeset : upload_message.changesets) {
                                 history.set_local_origin_timestamp_source([&]() {
                                     return changeset.origin_timestamp;
                                 });
                                 auto transaction = local_db->start_write();
                                 realm::sync::InstructionApplier applier(*transaction);
                                 applier.apply(changeset);
                                 auto generated_version = transaction->commit();
                                 logger->debug("integrated local changesets as version %1", generated_version);
                                 history.set_local_origin_timestamp_source(realm::sync::generate_changeset_timestamp);
                             }
                         },
                         [&](const ServerIdentMessage& ident_message) {
                             history.set_client_file_ident(ident_message.file_ident, true);
                         }},
                     message);
    }

    return EXIT_SUCCESS;
}
