#ifndef REALM_NOINST_PROTOCOL_CODEC_HPP
#define REALM_NOINST_PROTOCOL_CODEC_HPP

#include <cstdint>
#include <algorithm>
#include <memory>
#include <vector>
#include <string>

#include <realm/util/optional.hpp>
#include <realm/util/memory_stream.hpp>
#include <realm/util/buffer_stream.hpp>
#include <realm/util/from_chars.hpp>
#include <realm/util/logger.hpp>
#include <realm/binary_data.hpp>
#include <realm/chunked_binary.hpp>
#include <realm/sync/impl/clamped_hex_dump.hpp>
#include <realm/sync/noinst/compression.hpp>
#include <realm/sync/noinst/integer_codec.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/transform.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/history.hpp>

namespace realm::_impl {
struct ProtocolCodecException : public std::runtime_error {
    using std::runtime_error::runtime_error;
};
class HeaderLineParser {
public:
    HeaderLineParser() = default;
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
        static_assert(std::is_integral_v<T> || is_any_v<T, std::string_view, std::string>);
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
    using RemoteChangeset = sync::Transformer::RemoteChangeset;
    using ReceivedChangesets = std::vector<RemoteChangeset>;

    // FIXME: No need to explicitly assign numbers to these
    enum class Error {
        // clang-format off
        unknown_message             = 101, // Unknown type of input message
        bad_syntax                  = 102, // Bad syntax in input message head
        limits_exceeded             = 103, // Limits exceeded in input message
        bad_changeset_header_syntax = 108, // Bad syntax in changeset header (DOWNLOAD)
        bad_changeset_size          = 109, // Bad changeset size in changeset header (DOWNLOAD)
        bad_server_version          = 111, // Bad server version in changeset header (DOWNLOAD)
        bad_error_code              = 114, ///< Bad error code (ERROR)
        bad_decompression           = 115, // Error in decompression (DOWNLOAD)
        // clang-format on
    };


    /// Messages sent by the client.

    void make_bind_message(int protocol_version, OutputBuffer&, session_ident_type session_ident,
                           const std::string& server_path, const std::string& signed_user_token,
                           bool need_client_file_ident, bool is_subserver);

    void make_pbs_ident_message(OutputBuffer&, session_ident_type session_ident, SaltedFileIdent client_file_ident,
                                const SyncProgress& progress);

    void make_flx_ident_message(OutputBuffer&, session_ident_type session_ident, SaltedFileIdent client_file_ident,
                                const SyncProgress& progress, int64_t query_version, std::string_view query_body);

    void make_query_change_message(OutputBuffer&, session_ident_type, int64_t version, std::string_view query_body);

    class UploadMessageBuilder {
    public:
        util::Logger& logger;

        UploadMessageBuilder(util::Logger& logger, OutputBuffer& body_buffer, std::vector<char>& compression_buffer,
                             _impl::compression::CompressMemoryArena& compress_memory_arena);

        void add_changeset(version_type client_version, version_type server_version, timestamp_type origin_timestamp,
                           file_ident_type origin_file_ident, ChunkedBinaryData changeset);

        void make_upload_message(int protocol_version, OutputBuffer&, session_ident_type session_ident,
                                 version_type progress_client_version, version_type progress_server_version,
                                 version_type locked_server_version);

    private:
        std::size_t m_num_changesets = 0;
        OutputBuffer& m_body_buffer;
        std::vector<char>& m_compression_buffer;
        _impl::compression::CompressMemoryArena& m_compress_memory_arena;
    };

    UploadMessageBuilder make_upload_message_builder(util::Logger& logger);

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
        auto report_error = [&](Error err, const auto fmt, auto&&... args) {
            logger.error(fmt, std::forward<decltype(args)>(args)...);
            connection.handle_protocol_error(err);
        };

        HeaderLineParser msg(msg_data);
        std::string_view message_type;
        try {
            message_type = msg.read_next<std::string_view>();
        }
        catch (const ProtocolCodecException& e) {
            return report_error(Error::bad_syntax, "Could not find message type in message: %1", e.what());
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
                auto try_again = msg.read_next<bool>();
                auto session_ident = msg.read_next<session_ident_type>('\n');

                bool unknown_error = !sync::get_protocol_error_message(error_code);
                if (unknown_error) {
                    return report_error(Error::bad_error_code, "Bad error code");
                }

                auto message = msg.read_sized_data<StringData>(message_size);

                connection.receive_error_message(error_code, message, try_again, session_ident); // Throws
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
            else {
                return report_error(Error::unknown_message, "Unknown input message type '%1'", msg_data);
            }
        }
        catch (const ProtocolCodecException& e) {
            return report_error(Error::bad_syntax, "Bad syntax in %1 message: %2", message_type, e.what());
        }
        if (!msg.at_end()) {
            return report_error(Error::bad_syntax, "wire protocol message had leftover data after being parsed");
        }
    }

private:
    template <typename Connection>
    void parse_download_message(Connection& connection, HeaderLineParser& msg)
    {
        util::Logger& logger = connection.logger;
        auto report_error = [&](Error err, const auto fmt, auto&&... args) {
            logger.error(fmt, std::forward<decltype(args)>(args)...);
            connection.handle_protocol_error(err);
        };

        auto msg_with_header = msg.remaining();
        auto session_ident = msg.read_next<session_ident_type>();
        SyncProgress progress;
        progress.download.server_version = msg.read_next<version_type>();
        progress.download.last_integrated_client_version = msg.read_next<version_type>();
        progress.latest_server_version.version = msg.read_next<version_type>();
        progress.latest_server_version.salt = msg.read_next<salt_type>();
        progress.upload.client_version = msg.read_next<version_type>();
        progress.upload.last_integrated_server_version = msg.read_next<version_type>();
        auto query_version = connection.is_flx_sync_connection() ? msg.read_next<int64_t>() : 0;

        // If this is a PBS connection, then every download message is its own complete batch.
        auto last_in_batch = connection.is_flx_sync_connection() ? msg.read_next<bool>() : true;
        auto downloadable_bytes = msg.read_next<int64_t>();
        auto is_body_compressed = msg.read_next<bool>();
        auto uncompressed_body_size = msg.read_next<size_t>();
        auto compressed_body_size = msg.read_next<size_t>('\n');

        if (uncompressed_body_size > s_max_body_size) {
            auto header = msg_with_header.substr(0, msg_with_header.size() - msg.remaining().size());
            return report_error(Error::limits_exceeded, "Limits exceeded in input message '%1'", header);
        }

        std::unique_ptr<char[]> uncompressed_body_buffer;
        // if is_body_compressed == true, we must decompress the received body.
        if (is_body_compressed) {
            uncompressed_body_buffer = std::make_unique<char[]>(uncompressed_body_size);
            std::error_code ec = _impl::compression::decompress(
                msg.remaining().data(), compressed_body_size, uncompressed_body_buffer.get(), uncompressed_body_size);

            if (ec) {
                return report_error(Error::bad_decompression, "compression::inflate: %1", ec.message());
            }

            msg = HeaderLineParser(std::string_view(uncompressed_body_buffer.get(), uncompressed_body_size));
        }

        logger.trace("Download message compression: is_body_compressed = %1, "
                     "compressed_body_size=%2, uncompressed_body_size=%3",
                     is_body_compressed, compressed_body_size, uncompressed_body_size);

        ReceivedChangesets received_changesets;

        // Loop through the body and find the changesets.
        while (!msg.at_end()) {
            realm::sync::Transformer::RemoteChangeset cur_changeset;
            cur_changeset.remote_version = msg.read_next<version_type>();
            cur_changeset.last_integrated_local_version = msg.read_next<version_type>();
            cur_changeset.origin_timestamp = msg.read_next<timestamp_type>();
            cur_changeset.origin_file_ident = msg.read_next<file_ident_type>();
            cur_changeset.original_changeset_size = msg.read_next<size_t>();
            auto changeset_size = msg.read_next<size_t>();

            if (changeset_size > msg.bytes_remaining()) {
                return report_error(Error::bad_changeset_size, "Bad changeset size %1 > %2", changeset_size,
                                    msg.bytes_remaining());
            }
            if (cur_changeset.remote_version == 0) {
                return report_error(Error::bad_server_version,
                                    "Server version in downloaded changeset cannot be zero");
            }
            auto changeset_data = msg.read_sized_data<BinaryData>(changeset_size);

            if (logger.would_log(util::Logger::Level::trace)) {
                logger.trace("Received: DOWNLOAD CHANGESET(server_version=%1, "
                             "client_version=%2, origin_timestamp=%3, origin_file_ident=%4, "
                             "original_changeset_size=%5, changeset_size=%6)",
                             cur_changeset.remote_version, cur_changeset.last_integrated_local_version,
                             cur_changeset.origin_timestamp, cur_changeset.origin_file_ident,
                             cur_changeset.original_changeset_size, changeset_size); // Throws;
                if (changeset_data.size() < 1056) {
                    logger.trace("Changeset: %1",
                                 clamped_hex_dump(changeset_data)); // Throws
                }
                else {
                    logger.trace("Changeset(comp): %1 %2", changeset_data.size(),
                                 compressed_hex_dump(changeset_data)); // Throws
                }
#if REALM_DEBUG
                ChunkedBinaryInputStream in{changeset_data};
                sync::Changeset log;
                sync::parse_changeset(in, log);
                std::stringstream ss;
                log.print(ss);
                logger.trace("Changeset (parsed):\n%1", ss.str());
#endif
            }

            cur_changeset.data = changeset_data;
            received_changesets.push_back(std::move(cur_changeset)); // Throws
        }

        auto batch_state =
            last_in_batch ? sync::DownloadBatchState::LastInBatch : sync::DownloadBatchState::MoreToCome;
        connection.receive_download_message(session_ident, progress, downloadable_bytes, query_version, batch_state,
                                            received_changesets); // Throws
    }

    static constexpr std::size_t s_max_body_size = std::numeric_limits<std::size_t>::max();

    // Permanent buffer to use for building messages.
    OutputBuffer m_output_buffer;

    // Permanent buffers to use for internal purposes such as compression.
    std::vector<char> m_buffer;

    _impl::compression::CompressMemoryArena m_compress_memory_arena;
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

    // FIXME: No need to explicitly assign numbers to these
    enum class Error {
        // clang-format off
        unknown_message             = 101, // Unknown type of input message
        bad_syntax                  = 102, // Bad syntax in input message head
        limits_exceeded             = 103, // Limits exceeded in input message
        bad_decompression           = 104, // Error in decompression (UPLOAD)
        bad_changeset_header_syntax = 105, // Bad syntax in changeset header (UPLOAD)
        bad_changeset_size          = 106, // Changeset size doesn't fit in message (UPLOAD)
        // clang-format on
    };

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
            connection.logger.error("Bad syntax in ping message: %1", e.what());
            connection.handle_protocol_error(Error::bad_syntax);
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
        util::Logger& logger = connection.logger;

        auto report_error = [&](Error err, const auto fmt, auto&&... args) {
            logger.error(fmt, std::forward<decltype(args)>(args)...);
            connection.handle_protocol_error(err);
        };

        HeaderLineParser msg(msg_data);
        std::string_view message_type;
        try {
            message_type = msg.read_next<std::string_view>();
        }
        catch (const ProtocolCodecException& e) {
            return report_error(Error::bad_syntax, "Could not find message type in message: %1", e.what());
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
                    return report_error(Error::limits_exceeded,
                                        "Body size of upload message is too large. Raw header: %1", header);
                }


                std::unique_ptr<char[]> uncompressed_body_buffer;
                // if is_body_compressed == true, we must decompress the received body.
                if (is_body_compressed) {
                    uncompressed_body_buffer = std::make_unique<char[]>(uncompressed_body_size);
                    auto compressed_body = msg.read_sized_data<BinaryData>(compressed_body_size);

                    std::error_code ec =
                        _impl::compression::decompress(compressed_body.data(), compressed_body.size(),
                                                       uncompressed_body_buffer.get(), uncompressed_body_size);

                    if (ec) {
                        return report_error(Error::bad_decompression, "compression::inflate: %1", ec.message());
                    }

                    msg = HeaderLineParser(std::string_view(uncompressed_body_buffer.get(), uncompressed_body_size));
                }

                logger.debug("Upload message compression: is_body_compressed = %1, "
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
                        return report_error(Error::bad_changeset_header_syntax, "Bad changeset header syntax: %1",
                                            e.what());
                    }

                    if (changeset_size > msg.bytes_remaining()) {
                        return report_error(Error::bad_changeset_size, "Bad changeset size");
                    }

                    upload_changeset.changeset = msg.read_sized_data<BinaryData>(changeset_size);

                    if (logger.would_log(util::Logger::Level::trace)) {
                        logger.trace("Received: UPLOAD CHANGESET(client_version=%1, server_version=%2, "
                                     "origin_timestamp=%3, origin_file_ident=%4, changeset_size=%5)",
                                     upload_changeset.upload_cursor.client_version,
                                     upload_changeset.upload_cursor.last_integrated_server_version,
                                     upload_changeset.origin_timestamp, upload_changeset.origin_file_ident,
                                     changeset_size); // Throws
                        logger.trace("Changeset: %1",
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
                    return report_error(Error::bad_syntax, "Path size in BIND message is zero");
                }
                if (path_size > s_max_path_size) {
                    return report_error(Error::limits_exceeded, "Path size in BIND message is too large");
                }
                if (signed_user_token_size > s_max_signed_user_token_size) {
                    return report_error(Error::limits_exceeded,
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
            else {
                return report_error(Error::unknown_message, "unknown message type %1", message_type);
            }
        }
        catch (const ProtocolCodecException& e) {
            return report_error(Error::bad_syntax, "bad syntax in %1 message: %2", message_type, e.what());
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
