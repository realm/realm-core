#ifndef REALM_NOINST_PROTOCOL_CODEC_HPP
#define REALM_NOINST_PROTOCOL_CODEC_HPP

#include <cstdint>
#include <algorithm>
#include <memory>
#include <vector>
#include <string>

#include <realm/util/buffer_stream.hpp>
#include <realm/util/compression.hpp>
#include <realm/util/from_chars.hpp>
#include <realm/util/logger.hpp>
#include <realm/util/memory_stream.hpp>
#include <realm/util/optional.hpp>
#include <realm/binary_data.hpp>
#include <realm/chunked_binary.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/impl/clamped_hex_dump.hpp>
#include <realm/sync/noinst/integer_codec.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/transform.hpp>

#include <external/json/json.hpp>

namespace realm::_impl {
struct ProtocolCodecException : public std::runtime_error {
    using std::runtime_error::runtime_error;
};
class HeaderLineParser {
public:
    explicit HeaderLineParser(std::string_view line)
        : m_sv(line)
    {
    }

    template <typename T>
    T read_next(char expected_terminator = ' ')
    {
        const auto [tok, rest] = peek_token_impl<T>();
        if (rest.empty()) {
            throw ProtocolCodecException("header line ended prematurely without terminator");
        }
        if (rest.front() != expected_terminator) {
            throw ProtocolCodecException(util::format(
                "expected to find delimeter '%1' in header line, but found '%2'", expected_terminator, rest.front()));
        }
        m_sv = rest.substr(1);
        return tok;
    }

    template <typename T>
    T read_sized_data(size_t size)
    {
        auto ret = m_sv;
        advance(size);
        return T(ret.data(), size);
    }

    size_t bytes_remaining() const noexcept
    {
        return m_sv.size();
    }

    std::string_view remaining() const noexcept
    {
        return m_sv;
    }

    bool at_end() const noexcept
    {
        return m_sv.empty();
    }

    void advance(size_t size)
    {
        if (size > m_sv.size()) {
            throw ProtocolCodecException(
                util::format("cannot advance header by %1 characters, only %2 characters left", size, m_sv.size()));
        }
        m_sv.remove_prefix(size);
    }

private:
    template <typename T>
    std::pair<T, std::string_view> peek_token_impl() const
    {
        // We currently only support numeric, string, and boolean values in header lines.
        static_assert(std::is_integral_v<T> || std::is_floating_point_v<T> ||
                      is_any_v<T, std::string_view, std::string>);
        if (at_end()) {
            throw ProtocolCodecException("reached end of header line prematurely");
        }
        if constexpr (is_any_v<T, std::string_view, std::string>) {
            // Currently all string fields in wire protocol header lines appear at the beginning of the line and
            // should be delimited by a space.
            auto delim_at = m_sv.find(' ');
            if (delim_at == std::string_view::npos) {
                throw ProtocolCodecException("reached end of header line prematurely");
            }

            return {m_sv.substr(0, delim_at), m_sv.substr(delim_at)};
        }
        else if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
            T cur_arg = {};
            auto parse_res = util::from_chars(m_sv.data(), m_sv.data() + m_sv.size(), cur_arg, 10);
            if (parse_res.ec != std::errc{}) {
                throw ProtocolCodecException(util::format("error parsing integer in header line: %1",
                                                          std::make_error_code(parse_res.ec).message()));
            }

            return {cur_arg, m_sv.substr(parse_res.ptr - m_sv.data())};
        }
        else if constexpr (std::is_same_v<T, bool>) {
            int cur_arg;
            auto parse_res = util::from_chars(m_sv.data(), m_sv.data() + m_sv.size(), cur_arg, 10);
            if (parse_res.ec != std::errc{}) {
                throw ProtocolCodecException(util::format("error parsing boolean in header line: %1",
                                                          std::make_error_code(parse_res.ec).message()));
            }

            return {(cur_arg != 0), m_sv.substr(parse_res.ptr - m_sv.data())};
        }
        else if constexpr (std::is_floating_point_v<T>) {
            // Currently all double are in the middle of the string delimited by a space.
            auto delim_at = m_sv.find(' ');
            if (delim_at == std::string_view::npos)
                throw ProtocolCodecException("reached end of header line prematurely for double value parsing");

            // FIXME use std::from_chars one day when it's availiable in every std lib
            T val = {};
            try {
                std::string str(m_sv.substr(0, delim_at));
                if constexpr (std::is_same_v<T, float>)
                    val = std::stof(str);
                else if constexpr (std::is_same_v<T, double>)
                    val = std::stod(str);
                else if constexpr (std::is_same_v<T, long double>)
                    val = std::stold(str);
            }
            catch (const std::exception& err) {
                throw ProtocolCodecException(
                    util::format("error parsing floating-point number in header line: %1", err.what()));
            }

            return {val, m_sv.substr(delim_at)};
        }
    }

    std::string_view m_sv;
};

class ClientProtocol {
public:
    // clang-format off
    using file_ident_type    = sync::file_ident_type;
    using version_type       = sync::version_type;
    using salt_type          = sync::salt_type;
    using timestamp_type     = sync::timestamp_type;
    using session_ident_type = sync::session_ident_type;
    using request_ident_type = sync::request_ident_type;
    using milliseconds_type  = sync::milliseconds_type;
    using SaltedFileIdent    = sync::SaltedFileIdent;
    using SaltedVersion      = sync::SaltedVersion;
    using DownloadCursor     = sync::DownloadCursor;
    using UploadCursor       = sync::UploadCursor;
    using SyncProgress       = sync::SyncProgress;
    // clang-format on

    using OutputBuffer = util::ResettableExpandableBufferOutputStream;
    using RemoteChangeset = sync::RemoteChangeset;
    using ReceivedChangesets = std::vector<RemoteChangeset>;

    /// Messages sent by the client.

    void make_pbs_bind_message(int protocol_version, OutputBuffer&, session_ident_type session_ident,
                               const std::string& server_path, const std::string& signed_user_token,
                               bool need_client_file_ident, bool is_subserver);

    void make_flx_bind_message(int protocol_version, OutputBuffer& out, session_ident_type session_ident,
                               const nlohmann::json& json_data, const std::string& signed_user_token,
                               bool need_client_file_ident, bool is_subserver);

    void make_pbs_ident_message(OutputBuffer&, session_ident_type session_ident, SaltedFileIdent client_file_ident,
                                const SyncProgress& progress);

    void make_flx_ident_message(OutputBuffer&, session_ident_type session_ident, SaltedFileIdent client_file_ident,
                                const SyncProgress& progress, int64_t query_version, std::string_view query_body);

    void make_query_change_message(OutputBuffer&, session_ident_type, int64_t version, std::string_view query_body);

    void make_json_error_message(OutputBuffer&, session_ident_type, int error_code, std::string_view error_body);

    void make_test_command_message(OutputBuffer&, session_ident_type session, request_ident_type request_ident,
                                   std::string_view body);

    class UploadMessageBuilder {
    public:
        UploadMessageBuilder(OutputBuffer& body_buffer, std::vector<char>& compression_buffer,
                             util::compression::CompressMemoryArena& compress_memory_arena);

        void add_changeset(version_type client_version, version_type server_version, timestamp_type origin_timestamp,
                           file_ident_type origin_file_ident, ChunkedBinaryData changeset);

        void make_upload_message(int protocol_version, OutputBuffer&, session_ident_type session_ident,
                                 version_type progress_client_version, version_type progress_server_version,
                                 version_type locked_server_version);

    private:
        std::size_t m_num_changesets = 0;
        OutputBuffer& m_body_buffer;
        std::vector<char>& m_compression_buffer;
        util::compression::CompressMemoryArena& m_compress_memory_arena;
    };

    UploadMessageBuilder make_upload_message_builder();

    void make_unbind_message(OutputBuffer&, session_ident_type session_ident);

    void make_mark_message(OutputBuffer&, session_ident_type session_ident, request_ident_type request_ident);

    void make_ping(OutputBuffer&, milliseconds_type timestamp, milliseconds_type rtt);

    std::string compressed_hex_dump(BinaryData blob);

    // Messages received by the client.

    // parse_message_received takes a (WebSocket) message and parses it.
    // The result of the parsing is handled by an object of type Connection.
    // Typically, Connection would be the Connection class from client.cpp
    template <class Connection>
    void parse_message_received(Connection& connection, std::string_view msg_data)
    {
        util::Logger& logger = connection.logger;
        auto report_error = [&](const auto fmt, auto&&... args) {
            auto msg = util::format(fmt, std::forward<decltype(args)>(args)...);
            connection.handle_protocol_error(Status{ErrorCodes::SyncProtocolInvariantFailed, std::move(msg)});
        };

        HeaderLineParser msg(msg_data);
        std::string_view message_type;
        try {
            message_type = msg.read_next<std::string_view>();
        }
        catch (const ProtocolCodecException& e) {
            return report_error("Could not find message type in message: %1", e.what());
        }

        try {
            if (message_type == "download") {
                parse_download_message(connection, msg);
            }
            else if (message_type == "pong") {
                auto timestamp = msg.read_next<milliseconds_type>('\n');
                connection.receive_pong(timestamp);
            }
            else if (message_type == "unbound") {
                auto session_ident = msg.read_next<session_ident_type>('\n');
                connection.receive_unbound_message(session_ident); // Throws
            }
            else if (message_type == "error") {
                auto error_code = msg.read_next<int>();
                auto message_size = msg.read_next<size_t>();
                auto is_fatal = sync::IsFatal{!msg.read_next<bool>()};
                auto session_ident = msg.read_next<session_ident_type>('\n');
                auto message = msg.read_sized_data<StringData>(message_size);

                connection.receive_error_message(sync::ProtocolErrorInfo{error_code, message, is_fatal},
                                                 session_ident); // Throws
            }
            else if (message_type == "log_message") { // introduced in protocol version 10
                parse_log_message(connection, msg);
            }
            else if (message_type == "json_error") { // introduced in protocol 4
                sync::ProtocolErrorInfo info{};
                info.raw_error_code = msg.read_next<int>();
                auto message_size = msg.read_next<size_t>();
                auto session_ident = msg.read_next<session_ident_type>('\n');
                auto json_raw = msg.read_sized_data<std::string_view>(message_size);
                try {
                    auto json = nlohmann::json::parse(json_raw);
                    logger.trace(util::LogCategory::session, "Error message encoded as json: %1", json_raw);
                    info.client_reset_recovery_is_disabled = json["isRecoveryModeDisabled"];
                    info.is_fatal = sync::IsFatal{!json["tryAgain"]};
                    info.message = json["message"];
                    info.log_url = std::make_optional<std::string>(json["logURL"]);
                    info.should_client_reset = std::make_optional<bool>(json["shouldClientReset"]);
                    info.server_requests_action = string_to_action(json["action"]); // Throws

                    if (auto backoff_interval = json.find("backoffIntervalSec"); backoff_interval != json.end()) {
                        info.resumption_delay_interval.emplace();
                        info.resumption_delay_interval->resumption_delay_interval =
                            std::chrono::seconds{backoff_interval->get<int>()};
                        info.resumption_delay_interval->max_resumption_delay_interval =
                            std::chrono::seconds{json.at("backoffMaxDelaySec").get<int>()};
                        info.resumption_delay_interval->resumption_delay_backoff_multiplier =
                            json.at("backoffMultiplier").get<int>();
                    }

                    if (info.raw_error_code == static_cast<int>(sync::ProtocolError::migrate_to_flx)) {
                        auto query_string = json.find("partitionQuery");
                        if (query_string == json.end() || !query_string->is_string() ||
                            query_string->get<std::string_view>().empty()) {
                            return report_error(
                                "Missing/invalid partition query string in migrate to flexible sync error response");
                        }

                        info.migration_query_string.emplace(query_string->get<std::string_view>());
                    }

                    if (info.raw_error_code == static_cast<int>(sync::ProtocolError::schema_version_changed)) {
                        auto schema_version = json.find("previousSchemaVersion");
                        if (schema_version == json.end() || !schema_version->is_number_unsigned()) {
                            return report_error(
                                "Missing/invalid previous schema version in schema migration error response");
                        }

                        info.previous_schema_version.emplace(schema_version->get<uint64_t>());
                    }

                    if (auto rejected_updates = json.find("rejectedUpdates"); rejected_updates != json.end()) {
                        if (!rejected_updates->is_array()) {
                            return report_error(
                                "Compensating writes error list is not stored in an array as expected");
                        }

                        for (const auto& rejected_update : *rejected_updates) {
                            if (!rejected_update.is_object()) {
                                return report_error(
                                    "Compensating write error information is not stored in an object as expected");
                            }

                            sync::CompensatingWriteErrorInfo cwei;
                            cwei.reason = rejected_update["reason"];
                            cwei.object_name = rejected_update["table"];
                            std::string_view pk = rejected_update["pk"].get<std::string_view>();
                            cwei.primary_key = sync::parse_base64_encoded_primary_key(pk);
                            info.compensating_writes.push_back(std::move(cwei));
                        }

                        // Not provided when 'write_not_allowed' (230) error is received from the server.
                        if (auto server_version = json.find("compensatingWriteServerVersion");
                            server_version != json.end()) {
                            info.compensating_write_server_version =
                                std::make_optional<version_type>(server_version->get<int64_t>());
                        }
                        info.compensating_write_rejected_client_version =
                            json.at("rejectedClientVersion").get<int64_t>();
                    }
                }
                catch (const nlohmann::json::exception& e) {
                    // If any of the above json fields are not present, this is a fatal error
                    // however, additional optional fields may be added in the future.
                    return report_error("Failed to parse 'json_error' with error_code %1: '%2'", info.raw_error_code,
                                        e.what());
                }
                connection.receive_error_message(info, session_ident); // Throws
            }
            else if (message_type == "query_error") {
                auto error_code = msg.read_next<int>();
                auto message_size = msg.read_next<size_t>();
                auto session_ident = msg.read_next<session_ident_type>();
                auto query_version = msg.read_next<int64_t>('\n');

                auto message = msg.read_sized_data<std::string_view>(message_size);

                connection.receive_query_error_message(error_code, message, query_version, session_ident); // throws
            }
            else if (message_type == "mark") {
                auto session_ident = msg.read_next<session_ident_type>();
                auto request_ident = msg.read_next<request_ident_type>('\n');

                connection.receive_mark_message(session_ident, request_ident); // Throws
            }
            else if (message_type == "ident") {
                session_ident_type session_ident = msg.read_next<session_ident_type>();
                SaltedFileIdent client_file_ident;
                client_file_ident.ident = msg.read_next<file_ident_type>();
                client_file_ident.salt = msg.read_next<salt_type>('\n');

                connection.receive_ident_message(session_ident, client_file_ident); // Throws
            }
            else if (message_type == "test_command") {
                session_ident_type session_ident = msg.read_next<session_ident_type>();
                request_ident_type request_ident = msg.read_next<request_ident_type>();
                auto body_size = msg.read_next<size_t>('\n');
                auto body = msg.read_sized_data<std::string_view>(body_size);

                connection.receive_test_command_response(session_ident, request_ident, body);
            }
            else {
                return report_error("Unknown input message type '%1'", msg_data);
            }
        }
        catch (const ProtocolCodecException& e) {
            return report_error("Bad syntax in %1 message: %2", message_type, e.what());
        }
        if (!msg.at_end()) {
            return report_error("wire protocol message had leftover data after being parsed");
        }
    }

    struct DownloadMessage {
        SyncProgress progress;
        std::optional<int64_t> query_version; // FLX sync only
        sync::DownloadBatchState batch_state = sync::DownloadBatchState::SteadyState;
        sync::DownloadableProgress downloadable;
        ReceivedChangesets changesets;
    };

private:
    template <typename Connection>
    void parse_download_message(Connection& connection, HeaderLineParser& msg)
    {
        bool is_flx = connection.is_flx_sync_connection();

        util::Logger& logger = connection.logger;
        auto report_error = [&](ErrorCodes::Error code, const auto fmt, auto&&... args) {
            auto msg = util::format(fmt, std::forward<decltype(args)>(args)...);
            connection.handle_protocol_error(Status{code, std::move(msg)});
        };

        auto msg_with_header = msg.remaining();
        auto session_ident = msg.read_next<session_ident_type>();

        DownloadMessage message;
        auto&& progress = message.progress;
        progress.download.server_version = msg.read_next<version_type>();
        progress.download.last_integrated_client_version = msg.read_next<version_type>();
        progress.latest_server_version.version = msg.read_next<version_type>();
        progress.latest_server_version.salt = msg.read_next<salt_type>();
        progress.upload.client_version = msg.read_next<version_type>();
        progress.upload.last_integrated_server_version = msg.read_next<version_type>();

        if (is_flx) {
            message.query_version = msg.read_next<int64_t>();
            if (message.query_version < 0)
                return report_error(ErrorCodes::SyncProtocolInvariantFailed, "Bad query version: %1",
                                    message.query_version);
            int batch_state = msg.read_next<int>();
            if (batch_state != static_cast<int>(sync::DownloadBatchState::MoreToCome) &&
                batch_state != static_cast<int>(sync::DownloadBatchState::LastInBatch) &&
                batch_state != static_cast<int>(sync::DownloadBatchState::SteadyState)) {
                return report_error(ErrorCodes::SyncProtocolInvariantFailed, "Bad batch state: %1", batch_state);
            }
            message.batch_state = static_cast<sync::DownloadBatchState>(batch_state);

            double progress_estimate = msg.read_next<double>();
            if (progress_estimate < 0 || progress_estimate > 1)
                return report_error(ErrorCodes::SyncProtocolInvariantFailed, "Bad progress value: %1",
                                    progress_estimate);
            message.downloadable = progress_estimate;
        }
        else
            message.downloadable = uint64_t(msg.read_next<int64_t>());

        auto is_body_compressed = msg.read_next<bool>();
        auto uncompressed_body_size = msg.read_next<size_t>();
        auto compressed_body_size = msg.read_next<size_t>('\n');

        if (uncompressed_body_size > s_max_body_size) {
            auto header = msg_with_header.substr(0, msg_with_header.size() - msg.remaining().size());
            return report_error(ErrorCodes::LimitExceeded, "Limits exceeded in input message '%1'", header);
        }

        std::unique_ptr<char[]> uncompressed_body_buffer;
        // if is_body_compressed == true, we must decompress the received body.
        if (is_body_compressed) {
            uncompressed_body_buffer = std::make_unique<char[]>(uncompressed_body_size);
            std::error_code ec =
                util::compression::decompress({msg.remaining().data(), compressed_body_size},
                                              {uncompressed_body_buffer.get(), uncompressed_body_size});

            if (ec) {
                return report_error(ErrorCodes::RuntimeError, "compression::inflate: %1", ec.message());
            }

            msg = HeaderLineParser(std::string_view(uncompressed_body_buffer.get(), uncompressed_body_size));
        }

        logger.debug(util::LogCategory::changeset,
                     "Download message compression: session_ident=%1, is_body_compressed=%2, "
                     "compressed_body_size=%3, uncompressed_body_size=%4",
                     session_ident, is_body_compressed, compressed_body_size, uncompressed_body_size);

        // Loop through the body and find the changesets.
        while (!msg.at_end()) {
            RemoteChangeset cur_changeset;
            cur_changeset.remote_version = msg.read_next<version_type>();
            cur_changeset.last_integrated_local_version = msg.read_next<version_type>();
            cur_changeset.origin_timestamp = msg.read_next<timestamp_type>();
            cur_changeset.origin_file_ident = msg.read_next<file_ident_type>();
            cur_changeset.original_changeset_size = msg.read_next<size_t>();
            auto changeset_size = msg.read_next<size_t>();

            if (changeset_size > msg.bytes_remaining()) {
                return report_error(ErrorCodes::SyncProtocolInvariantFailed, "Bad changeset size %1 > %2",
                                    changeset_size, msg.bytes_remaining());
            }
            if (cur_changeset.remote_version == 0) {
                return report_error(ErrorCodes::SyncProtocolInvariantFailed,
                                    "Server version in downloaded changeset cannot be zero");
            }
            auto changeset_data = msg.read_sized_data<BinaryData>(changeset_size);
            logger.debug(util::LogCategory::changeset,
                         "Received: DOWNLOAD CHANGESET(session_ident=%1, server_version=%2, "
                         "client_version=%3, origin_timestamp=%4, origin_file_ident=%5, "
                         "original_changeset_size=%6, changeset_size=%7)",
                         session_ident, cur_changeset.remote_version, cur_changeset.last_integrated_local_version,
                         cur_changeset.origin_timestamp, cur_changeset.origin_file_ident,
                         cur_changeset.original_changeset_size, changeset_size); // Throws
            if (logger.would_log(util::LogCategory::changeset, util::Logger::Level::trace)) {
                if (changeset_data.size() < 1056) {
                    logger.trace(util::LogCategory::changeset, "Changeset: %1",
                                 clamped_hex_dump(changeset_data)); // Throws
                }
                else {
                    logger.trace(util::LogCategory::changeset, "Changeset(comp): %1 %2", changeset_data.size(),
                                 compressed_hex_dump(changeset_data)); // Throws
                }
#if REALM_DEBUG
                ChunkedBinaryInputStream in{changeset_data};
                sync::Changeset log;
                sync::parse_changeset(in, log);
                std::stringstream ss;
                log.print(ss);
                logger.trace(util::LogCategory::changeset, "Changeset (parsed):\n%1", ss.str());
#endif
            }

            cur_changeset.data = changeset_data;
            message.changesets.push_back(std::move(cur_changeset)); // Throws
        }

        connection.receive_download_message(session_ident, message); // Throws
    }

    static sync::ProtocolErrorInfo::Action string_to_action(const std::string& action_string)
    {
        using action = sync::ProtocolErrorInfo::Action;
        static const std::unordered_map<std::string, action> mapping{
            {"ProtocolViolation", action::ProtocolViolation},
            {"ApplicationBug", action::ApplicationBug},
            {"Warning", action::Warning},
            {"Transient", action::Transient},
            {"DeleteRealm", action::DeleteRealm},
            {"ClientReset", action::ClientReset},
            {"ClientResetNoRecovery", action::ClientResetNoRecovery},
            {"MigrateToFLX", action::MigrateToFLX},
            {"RevertToPBS", action::RevertToPBS},
            {"RefreshUser", action::RefreshUser},
            {"RefreshLocation", action::RefreshLocation},
            {"LogOutUser", action::LogOutUser},
            {"MigrateSchema", action::MigrateSchema},
        };

        if (auto action_it = mapping.find(action_string); action_it != mapping.end()) {
            return action_it->second;
        }
        return action::ApplicationBug;
    }

    template <typename Connection>
    void parse_log_message(Connection& connection, HeaderLineParser& msg)
    {
        auto report_error = [&](const auto fmt, auto&&... args) {
            auto msg = util::format(fmt, std::forward<decltype(args)>(args)...);
            connection.handle_protocol_error(Status{ErrorCodes::SyncProtocolInvariantFailed, std::move(msg)});
        };

        auto session_ident = msg.read_next<session_ident_type>();
        auto message_length = msg.read_next<size_t>('\n');
        auto message_body_str = msg.read_sized_data<std::string_view>(message_length);
        nlohmann::json message_body;
        try {
            message_body = nlohmann::json::parse(message_body_str);
        }
        catch (const nlohmann::json::exception& e) {
            return report_error("Malformed json in log_message message: \"%1\": %2", message_body_str, e.what());
        }
        static const std::unordered_map<std::string_view, util::Logger::Level> name_to_level = {
            {"fatal", util::Logger::Level::fatal},   {"error", util::Logger::Level::error},
            {"warn", util::Logger::Level::warn},     {"info", util::Logger::Level::info},
            {"detail", util::Logger::Level::detail}, {"debug", util::Logger::Level::debug},
            {"trace", util::Logger::Level::trace},
        };

        // See if the log_message contains the appservices_request_id
        if (auto it = message_body.find("co_id"); it != message_body.end() && it->is_string()) {
            connection.receive_appservices_request_id(it->get<std::string_view>());
        }

        std::string_view log_level;
        bool has_level = false;
        if (auto it = message_body.find("level"); it != message_body.end() && it->is_string()) {
            log_level = it->get<std::string_view>();
            has_level = !log_level.empty();
        }

        std::string_view msg_text;
        if (auto it = message_body.find("msg"); it != message_body.end() && it->is_string()) {
            msg_text = it->get<std::string_view>();
        }

        // If there is no message text, then we're done
        if (msg_text.empty()) {
            return;
        }

        // If a log level wasn't provided, default to debug
        util::Logger::Level parsed_level = util::Logger::Level::debug;
        if (has_level) {
            if (auto it = name_to_level.find(log_level); it != name_to_level.end()) {
                parsed_level = it->second;
            }
            else {
                return report_error("Unknown log level found in log_message: \"%1\"", log_level);
            }
        }
        connection.receive_server_log_message(session_ident, parsed_level, msg_text);
    }

    static constexpr std::size_t s_max_body_size = std::numeric_limits<std::size_t>::max();

    // Permanent buffer to use for building messages.
    OutputBuffer m_output_buffer;

    // Permanent buffers to use for internal purposes such as compression.
    std::vector<char> m_buffer;

    util::compression::CompressMemoryArena m_compress_memory_arena;
};


class ServerProtocol {
public:
    // clang-format off
    using file_ident_type    = sync::file_ident_type;
    using version_type       = sync::version_type;
    using salt_type          = sync::salt_type;
    using timestamp_type     = sync::timestamp_type;
    using session_ident_type = sync::session_ident_type;
    using request_ident_type = sync::request_ident_type;
    using SaltedFileIdent    = sync::SaltedFileIdent;
    using SaltedVersion      = sync::SaltedVersion;
    using milliseconds_type  = sync::milliseconds_type;
    using UploadCursor       = sync::UploadCursor;
    // clang-format on

    using OutputBuffer = util::ResettableExpandableBufferOutputStream;

    // Messages sent by the server to the client

    void make_ident_message(int protocol_version, OutputBuffer&, session_ident_type session_ident,
                            file_ident_type client_file_ident, salt_type client_file_ident_salt);

    void make_alloc_message(OutputBuffer&, session_ident_type session_ident, file_ident_type file_ident);

    void make_unbound_message(OutputBuffer&, session_ident_type session_ident);


    struct ChangesetInfo {
        version_type server_version;
        version_type client_version;
        sync::HistoryEntry entry;
        std::size_t original_size;
    };

    void make_download_message(int protocol_version, OutputBuffer&, session_ident_type session_ident,
                               version_type download_server_version, version_type download_client_version,
                               version_type latest_server_version, salt_type latest_server_version_salt,
                               version_type upload_client_version, version_type upload_server_version,
                               std::uint_fast64_t downloadable_bytes, std::size_t num_changesets, const char* body,
                               std::size_t uncompressed_body_size, std::size_t compressed_body_size,
                               bool body_is_compressed, util::Logger&);

    void make_mark_message(OutputBuffer&, session_ident_type session_ident, request_ident_type request_ident);

    void make_error_message(int protocol_version, OutputBuffer&, sync::ProtocolError error_code, const char* message,
                            std::size_t message_size, bool try_again, session_ident_type session_ident);

    void make_pong(OutputBuffer&, milliseconds_type timestamp);

    void make_log_message(OutputBuffer& out, util::Logger::Level level, std::string message,
                          session_ident_type sess_id = 0, std::optional<std::string> co_id = std::nullopt);

    // Messages received by the server.

    // parse_ping_received takes a (WebSocket) ping and parses it.
    // The result of the parsing is handled by an object of type Connection.
    // Typically, Connection would be the Connection class from server.cpp
    template <typename Connection>
    void parse_ping_received(Connection& connection, std::string_view msg_data)
    {
        try {
            HeaderLineParser msg(msg_data);
            auto timestamp = msg.read_next<milliseconds_type>();
            auto rtt = msg.read_next<milliseconds_type>('\n');

            connection.receive_ping(timestamp, rtt);
        }
        catch (const ProtocolCodecException& e) {
            connection.handle_protocol_error(Status{ErrorCodes::SyncProtocolInvariantFailed,
                                                    util::format("Bad syntax in PING message: %1", e.what())});
        }
    }

    // UploadChangeset is used to store received changesets in
    // the UPLOAD message.
    struct UploadChangeset {
        UploadCursor upload_cursor;
        timestamp_type origin_timestamp;
        file_ident_type origin_file_ident; // Zero when originating from connected client file
        BinaryData changeset;
    };

    // parse_message_received takes a (WebSocket) message and parses it.
    // The result of the parsing is handled by an object of type Connection.
    // Typically, Connection would be the Connection class from server.cpp
    template <class Connection>
    void parse_message_received(Connection& connection, std::string_view msg_data)
    {
        auto& logger = connection.logger;

        auto report_error = [&](ErrorCodes::Error err, const auto fmt, auto&&... args) {
            auto msg = util::format(fmt, std::forward<decltype(args)>(args)...);
            connection.handle_protocol_error(Status{err, std::move(msg)});
        };

        HeaderLineParser msg(msg_data);
        std::string_view message_type;
        try {
            message_type = msg.read_next<std::string_view>();
        }
        catch (const ProtocolCodecException& e) {
            return report_error(ErrorCodes::SyncProtocolInvariantFailed, "Could not find message type in message: %1",
                                e.what());
        }

        try {
            if (message_type == "upload") {
                auto msg_with_header = msg.remaining();
                auto session_ident = msg.read_next<session_ident_type>();
                auto is_body_compressed = msg.read_next<bool>();
                auto uncompressed_body_size = msg.read_next<size_t>();
                auto compressed_body_size = msg.read_next<size_t>();
                auto progress_client_version = msg.read_next<version_type>();
                auto progress_server_version = msg.read_next<version_type>();
                auto locked_server_version = msg.read_next<version_type>('\n');

                std::size_t body_size = (is_body_compressed ? compressed_body_size : uncompressed_body_size);
                if (body_size > s_max_body_size) {
                    auto header = msg_with_header.substr(0, msg_with_header.size() - msg.bytes_remaining());

                    return report_error(ErrorCodes::LimitExceeded,
                                        "Body size of upload message is too large. Raw header: %1", header);
                }


                std::unique_ptr<char[]> uncompressed_body_buffer;
                // if is_body_compressed == true, we must decompress the received body.
                if (is_body_compressed) {
                    uncompressed_body_buffer = std::make_unique<char[]>(uncompressed_body_size);
                    auto compressed_body = msg.read_sized_data<BinaryData>(compressed_body_size);

                    std::error_code ec = util::compression::decompress(
                        compressed_body, {uncompressed_body_buffer.get(), uncompressed_body_size});

                    if (ec) {
                        return report_error(ErrorCodes::RuntimeError, "compression::inflate: %1", ec.message());
                    }

                    msg = HeaderLineParser(std::string_view(uncompressed_body_buffer.get(), uncompressed_body_size));
                }

                logger.debug(util::LogCategory::changeset,
                             "Upload message compression: is_body_compressed = %1, "
                             "compressed_body_size=%2, uncompressed_body_size=%3, "
                             "progress_client_version=%4, progress_server_version=%5, "
                             "locked_server_version=%6",
                             is_body_compressed, compressed_body_size, uncompressed_body_size,
                             progress_client_version, progress_server_version, locked_server_version); // Throws


                std::vector<UploadChangeset> upload_changesets;

                // Loop through the body and find the changesets.
                while (!msg.at_end()) {
                    UploadChangeset upload_changeset;
                    size_t changeset_size;
                    try {
                        upload_changeset.upload_cursor.client_version = msg.read_next<version_type>();
                        upload_changeset.upload_cursor.last_integrated_server_version = msg.read_next<version_type>();
                        upload_changeset.origin_timestamp = msg.read_next<timestamp_type>();
                        upload_changeset.origin_file_ident = msg.read_next<file_ident_type>();
                        changeset_size = msg.read_next<size_t>();
                    }
                    catch (const ProtocolCodecException& e) {
                        return report_error(ErrorCodes::SyncProtocolInvariantFailed,
                                            "Bad changeset header syntax: %1", e.what());
                    }

                    if (changeset_size > msg.bytes_remaining()) {
                        return report_error(ErrorCodes::SyncProtocolInvariantFailed, "Bad changeset size");
                    }

                    upload_changeset.changeset = msg.read_sized_data<BinaryData>(changeset_size);

                    if (logger.would_log(util::Logger::Level::trace)) {
                        logger.trace(util::LogCategory::changeset,
                                     "Received: UPLOAD CHANGESET(client_version=%1, server_version=%2, "
                                     "origin_timestamp=%3, origin_file_ident=%4, changeset_size=%5)",
                                     upload_changeset.upload_cursor.client_version,
                                     upload_changeset.upload_cursor.last_integrated_server_version,
                                     upload_changeset.origin_timestamp, upload_changeset.origin_file_ident,
                                     changeset_size); // Throws
                        logger.trace(util::LogCategory::changeset, "Changeset: %1",
                                     clamped_hex_dump(upload_changeset.changeset)); // Throws
                    }
                    upload_changesets.push_back(std::move(upload_changeset)); // Throws
                }

                connection.receive_upload_message(session_ident, progress_client_version, progress_server_version,
                                                  locked_server_version,
                                                  upload_changesets); // Throws
            }
            else if (message_type == "mark") {
                auto session_ident = msg.read_next<session_ident_type>();
                auto request_ident = msg.read_next<request_ident_type>('\n');

                connection.receive_mark_message(session_ident, request_ident); // Throws
            }
            else if (message_type == "ping") {
                auto timestamp = msg.read_next<milliseconds_type>();
                auto rtt = msg.read_next<milliseconds_type>('\n');

                connection.receive_ping(timestamp, rtt);
            }
            else if (message_type == "bind") {
                auto session_ident = msg.read_next<session_ident_type>();
                auto path_size = msg.read_next<size_t>();
                auto signed_user_token_size = msg.read_next<size_t>();
                auto need_client_file_ident = msg.read_next<bool>();
                auto is_subserver = msg.read_next<bool>('\n');

                if (path_size == 0) {
                    return report_error(ErrorCodes::SyncProtocolInvariantFailed, "Path size in BIND message is zero");
                }
                if (path_size > s_max_path_size) {
                    return report_error(ErrorCodes::SyncProtocolInvariantFailed,
                                        "Path size in BIND message is too large");
                }
                if (signed_user_token_size > s_max_signed_user_token_size) {
                    return report_error(ErrorCodes::SyncProtocolInvariantFailed,
                                        "Signed user token size in BIND message is too large");
                }

                auto path = msg.read_sized_data<std::string>(path_size);
                auto signed_user_token = msg.read_sized_data<std::string>(signed_user_token_size);

                connection.receive_bind_message(session_ident, std::move(path), std::move(signed_user_token),
                                                need_client_file_ident, is_subserver); // Throws
            }
            else if (message_type == "ident") {
                auto session_ident = msg.read_next<session_ident_type>();
                auto client_file_ident = msg.read_next<file_ident_type>();
                auto client_file_ident_salt = msg.read_next<salt_type>();
                auto scan_server_version = msg.read_next<version_type>();
                auto scan_client_version = msg.read_next<version_type>();
                auto latest_server_version = msg.read_next<version_type>();
                auto latest_server_version_salt = msg.read_next<salt_type>('\n');

                connection.receive_ident_message(session_ident, client_file_ident, client_file_ident_salt,
                                                 scan_server_version, scan_client_version, latest_server_version,
                                                 latest_server_version_salt); // Throws
            }
            else if (message_type == "unbind") {
                auto session_ident = msg.read_next<session_ident_type>('\n');

                connection.receive_unbind_message(session_ident); // Throws
            }
            else if (message_type == "json_error") {
                auto error_code = msg.read_next<int>();
                auto message_size = msg.read_next<size_t>();
                auto session_ident = msg.read_next<session_ident_type>('\n');
                auto json_raw = msg.read_sized_data<std::string_view>(message_size);

                connection.receive_error_message(session_ident, error_code, json_raw);
            }
            else {
                return report_error(ErrorCodes::SyncProtocolInvariantFailed, "unknown message type %1", message_type);
            }
        }
        catch (const ProtocolCodecException& e) {
            return report_error(ErrorCodes::SyncProtocolInvariantFailed, "bad syntax in %1 message: %2", message_type,
                                e.what());
        }
    }

    void insert_single_changeset_download_message(OutputBuffer&, const ChangesetInfo&, util::Logger&);

private:
    // clang-format off
    static constexpr std::size_t s_max_head_size              =  256;
    static constexpr std::size_t s_max_signed_user_token_size = 2048;
    static constexpr std::size_t s_max_client_info_size       = 1024;
    static constexpr std::size_t s_max_path_size              = 1024;
    static constexpr std::size_t s_max_changeset_size         = std::numeric_limits<std::size_t>::max(); // FIXME: What is a reasonable value here?
    static constexpr std::size_t s_max_body_size              = std::numeric_limits<std::size_t>::max();
    // clang-format on
};

// make_authorization_header() makes the value of the Authorization header used in the
// sync Websocket handshake.
std::string make_authorization_header(const std::string& signed_user_token);

// parse_authorization_header() parses the value of the Authorization header and returns
// the signed_user_token. None is returned in case of syntax error.
util::Optional<StringData> parse_authorization_header(const std::string& authorization_header);

} // namespace realm::_impl

#endif // REALM_NOINST_PROTOCOL_CODEC_HPP
