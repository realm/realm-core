#ifndef REALM_NOINST_PROTOCOL_CODEC_HPP
#define REALM_NOINST_PROTOCOL_CODEC_HPP

#include <cstdint>
#include <algorithm>
#include <memory>
#include <vector>
#include <string>

#include <realm/util/optional.hpp>
#include <realm/util/string_view.hpp>
#include <realm/util/memory_stream.hpp>
#include <realm/util/buffer_stream.hpp>
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


namespace realm {
namespace _impl {

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

    void make_refresh_message(OutputBuffer&, session_ident_type session_ident, const std::string& signed_user_token);

    void make_ident_message(OutputBuffer&, session_ident_type session_ident, SaltedFileIdent client_file_ident,
                            const SyncProgress& progress);

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

    void make_alloc_message(OutputBuffer&, session_ident_type session_ident);

    void make_ping(OutputBuffer&, milliseconds_type timestamp, milliseconds_type rtt);

    std::string compressed_hex_dump(BinaryData blob);

    // Messages received by the client.

    // parse_pong_received takes a (WebSocket) pong and parses it.
    // The result of the parsing is handled by an object of type Connection.
    // Typically, Connection would be the Connection class from client.cpp
    template <typename Connection>
    void parse_pong_received(Connection& connection, const char* data, std::size_t size)
    {
        util::Logger& logger = connection.logger;

        util::MemoryInputStream in;
        in.set_buffer(data, data + size);
        in.unsetf(std::ios_base::skipws);

        milliseconds_type timestamp;

        char newline = 0;
        in >> timestamp >> newline;
        std::size_t expected_size = std::size_t(in.tellg());
        bool good_syntax = (in && newline == '\n' && expected_size == size);
        if (!good_syntax)
            goto bad_syntax;

        connection.receive_pong(timestamp);
        return;

    bad_syntax:
        logger.error("Bad syntax in input message '%1'", StringData(data, size));
        connection.handle_protocol_error(Error::bad_syntax); // Throws
        return;
    }

    // parse_message_received takes a (WebSocket) message and parses it.
    // The result of the parsing is handled by an object of type Connection.
    // Typically, Connection would be the Connection class from client.cpp
    template <class Connection>
    void parse_message_received(Connection& connection, const char* data, std::size_t size)
    {
        util::Logger& logger = connection.logger;

        util::MemoryInputStream in;
        in.set_buffer(data, data + size);
        in.unsetf(std::ios_base::skipws);
        std::size_t header_size = 0;
        std::string message_type;
        in >> message_type;

        if (message_type == "download") {
            session_ident_type session_ident;
            version_type download_server_version, download_client_version;
            version_type latest_server_version;
            salt_type latest_server_version_salt;
            version_type upload_client_version, upload_server_version;
            std::int_fast64_t downloadable_bytes;
            int is_body_compressed;
            std::size_t uncompressed_body_size, compressed_body_size;
            char sp_1, sp_2, sp_3, sp_4, sp_5, sp_6, sp_7, sp_8, sp_9, sp_10, sp_11, newline;
            in >> sp_1 >> session_ident >> sp_2 >> download_server_version >> sp_3 >> download_client_version >>
                sp_4 >> latest_server_version >> sp_5 >> latest_server_version_salt >> sp_6 >>
                upload_client_version >> sp_7 >> upload_server_version >> sp_8 >> downloadable_bytes >> sp_9 >>
                is_body_compressed >> sp_10 >> uncompressed_body_size >> sp_11 >> compressed_body_size >>
                newline; // Throws
            header_size = std::size_t(in.tellg());
            std::size_t body_size = (is_body_compressed ? compressed_body_size : uncompressed_body_size);
            std::size_t expected_size = header_size + body_size;
            bool good_syntax = (in && sp_1 == ' ' && sp_2 == ' ' && sp_3 == ' ' && sp_4 == ' ' && sp_5 == ' ' &&
                                sp_6 == ' ' && sp_7 == ' ' && sp_8 == ' ' && sp_9 == ' ' && sp_10 == ' ' &&
                                sp_11 == ' ' && newline == '\n' && expected_size == size);
            if (!good_syntax)
                goto bad_syntax;
            if (uncompressed_body_size > s_max_body_size)
                goto limits_exceeded;

            BinaryData body(data + header_size, body_size);
            BinaryData uncompressed_body;

            std::unique_ptr<char[]> uncompressed_body_buffer;
            // if is_body_compressed == true, we must decompress the received body.
            if (is_body_compressed) {
                uncompressed_body_buffer.reset(new char[uncompressed_body_size]);
                std::error_code ec = _impl::compression::decompress(
                    body.data(), compressed_body_size, uncompressed_body_buffer.get(), uncompressed_body_size);

                if (ec) {
                    logger.error("compression::inflate: %1", ec.message());
                    connection.handle_protocol_error(Error::bad_decompression);
                    return;
                }

                uncompressed_body = BinaryData(uncompressed_body_buffer.get(), uncompressed_body_size);
            }
            else {
                uncompressed_body = body;
            }

            logger.trace("Download message compression: is_body_compressed = %1, "
                         "compressed_body_size=%2, uncompressed_body_size=%3",
                         is_body_compressed, compressed_body_size, uncompressed_body_size);

            util::MemoryInputStream in;
            in.unsetf(std::ios_base::skipws);
            in.set_buffer(uncompressed_body.data(), uncompressed_body.data() + uncompressed_body_size);

            ReceivedChangesets received_changesets;

            // Loop through the body and find the changesets.
            std::size_t position = 0;
            while (position < uncompressed_body_size) {
                version_type server_version;
                version_type client_version;
                timestamp_type origin_timestamp;
                file_ident_type origin_file_ident;
                std::size_t original_changeset_size, changeset_size;
                char sp_1, sp_2, sp_3, sp_4, sp_5, sp_6;

                in >> server_version >> sp_1 >> client_version >> sp_2 >> origin_timestamp >> sp_3 >>
                    origin_file_ident >> sp_4 >> original_changeset_size >> sp_5 >> changeset_size >> sp_6;

                bool good_syntax =
                    in && sp_1 == ' ' && sp_2 == ' ' && sp_3 == ' ' && sp_4 == ' ' && sp_5 == ' ' && sp_6 == ' ';

                if (!good_syntax) {
                    logger.error("Bad changeset header syntax");
                    connection.handle_protocol_error(Error::bad_changeset_header_syntax);
                    return;
                }

                // Update position to the end of the change set
                position = std::size_t(in.tellg()) + changeset_size;

                if (position > uncompressed_body_size) {
                    logger.error("Bad changeset size %1 > %2", position, uncompressed_body_size);
                    connection.handle_protocol_error(Error::bad_changeset_size);
                    return;
                }

                if (server_version == 0) {
                    // The received changeset can never have version 0.
                    logger.error("Bad server version=0");
                    connection.handle_protocol_error(Error::bad_server_version);
                    return;
                }

                BinaryData changeset_data(uncompressed_body.data() + std::size_t(in.tellg()), changeset_size);
                in.seekg(position);

                if (logger.would_log(util::Logger::Level::trace)) {
                    logger.trace("Received: DOWNLOAD CHANGESET(server_version=%1, "
                                 "client_version=%2, origin_timestamp=%3, origin_file_ident=%4, "
                                 "original_changeset_size=%5, changeset_size=%6)",
                                 server_version, client_version, origin_timestamp, origin_file_ident,
                                 original_changeset_size,
                                 changeset_size); // Throws
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

                RemoteChangeset changeset_2(server_version, client_version, changeset_data, origin_timestamp,
                                            origin_file_ident);
                changeset_2.original_changeset_size = original_changeset_size;
                received_changesets.push_back(changeset_2); // Throws
            }

            SyncProgress progress;
            progress.latest_server_version = SaltedVersion{latest_server_version, latest_server_version_salt};
            progress.download = DownloadCursor{download_server_version, download_client_version};
            progress.upload = UploadCursor{upload_client_version, upload_server_version};
            connection.receive_download_message(session_ident, progress, downloadable_bytes,
                                                received_changesets); // Throws
            return;
        }
        if (message_type == "pong") {
            milliseconds_type timestamp;
            char newline = 0;
            char sp1;
            in >> sp1 >> timestamp >> newline;
            header_size = std::size_t(in.tellg());
            std::size_t expected_size = header_size;
            bool good_syntax = (in && sp1 == ' ' && newline == '\n' && expected_size == size);
            if (!good_syntax) {
                goto bad_syntax;
            }

            connection.receive_pong(timestamp);
            return;
        }
        if (message_type == "unbound") {
            session_ident_type session_ident;
            char sp_1, newline;
            in >> sp_1 >> session_ident >> newline; // Throws
            header_size = std::size_t(in.tellg());
            std::size_t expected_size = header_size;
            bool good_syntax = (in && sp_1 == ' ' && newline == '\n' && expected_size == size);
            if (!good_syntax)
                goto bad_syntax;

            connection.receive_unbound_message(session_ident); // Throws
            return;
        }
        if (message_type == "error") {
            int error_code;
            std::size_t message_size;
            bool try_again;
            session_ident_type session_ident;
            char sp_1, sp_2, sp_3, sp_4, newline;
            in >> sp_1 >> error_code >> sp_2 >> message_size >> sp_3 >> try_again >> sp_4 >> session_ident >>
                newline; // Throws
            header_size = std::size_t(in.tellg());
            std::size_t expected_size = header_size + message_size;
            bool good_syntax = (in && sp_1 == ' ' && sp_2 == ' ' && sp_3 == ' ' && sp_4 == ' ' && newline == '\n' &&
                                expected_size == size);
            if (!good_syntax)
                goto bad_syntax;

            bool unknown_error = !sync::get_protocol_error_message(error_code);
            if (unknown_error) {
                logger.error("Bad error code"); // Throws
                connection.handle_protocol_error(Error::bad_error_code);
                return;
            }

            StringData message{data + header_size, message_size};
            connection.receive_error_message(error_code, message, try_again, session_ident); // Throws
            return;
        }
        if (message_type == "mark") {
            session_ident_type session_ident;
            request_ident_type request_ident;
            char sp_1, sp_2, newline;
            in >> sp_1 >> session_ident >> sp_2 >> request_ident >> newline; // Throws
            header_size = std::size_t(in.tellg());
            std::size_t expected_size = header_size;
            bool good_syntax = (in && sp_1 == ' ' && sp_2 == ' ' && newline == '\n' && expected_size == size);
            if (!good_syntax)
                goto bad_syntax;

            connection.receive_mark_message(session_ident, request_ident); // Throws
            return;
        }
        if (message_type == "alloc") {
            session_ident_type session_ident;
            file_ident_type file_ident;
            char sp_1, sp_2, newline;
            in >> sp_1 >> session_ident >> sp_2 >> file_ident >> newline; // Throws
            header_size = std::size_t(in.tellg());
            std::size_t expected_size = header_size;
            bool good_syntax = (in && sp_1 == ' ' && sp_2 == ' ' && newline == '\n' && expected_size == size);
            if (!good_syntax)
                goto bad_syntax;

            connection.receive_alloc_message(session_ident, file_ident); // Throws
            return;
        }
        if (message_type == "ident") {
            session_ident_type session_ident;
            file_ident_type client_file_ident;
            salt_type client_file_ident_salt;
            char sp_1, sp_2, sp_3, newline;
            in >> sp_1 >> session_ident >> sp_2 >> client_file_ident >> sp_3 >> client_file_ident_salt >>
                newline; // Throws
            header_size = std::size_t(in.tellg());
            std::size_t expected_size = header_size;
            bool good_syntax =
                (in && sp_1 == ' ' && sp_2 == ' ' && sp_3 == ' ' && newline == '\n' && expected_size == size);
            if (!good_syntax)
                goto bad_syntax;

            SaltedFileIdent client_file_ident_2{client_file_ident, client_file_ident_salt};
            connection.receive_ident_message(session_ident, client_file_ident_2); // Throws
            return;
        }

        logger.error("Unknown input message type '%1'", StringData(data, size));
        connection.handle_protocol_error(Error::unknown_message);
        return;
    bad_syntax:
        logger.error("Bad syntax in input message '%1'", StringData(data, size));
        connection.handle_protocol_error(Error::bad_syntax);
        return;
    limits_exceeded:
        logger.error("Limits exceeded in input message '%1'", StringData(data, header_size));
        connection.handle_protocol_error(Error::limits_exceeded);
        return;
    }

private:
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

    void make_client_version_message(OutputBuffer&, session_ident_type session_ident, version_type client_version);

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
    void parse_ping_received(Connection& connection, const char* data, std::size_t size)
    {
        util::Logger& logger = connection.logger;

        util::MemoryInputStream in;
        in.set_buffer(data, data + size);
        in.unsetf(std::ios_base::skipws);

        milliseconds_type timestamp, rtt;

        char sp_1 = 0, newline = 0;
        in >> timestamp >> sp_1 >> rtt >> newline;
        std::size_t expected_size = std::size_t(in.tellg());
        bool good_syntax = (in && sp_1 == ' ' && newline == '\n' && expected_size == size);
        if (!good_syntax)
            goto bad_syntax;

        connection.receive_ping(timestamp, rtt);
        return;

    bad_syntax:
        logger.error("Bad syntax in PING message '%1'", StringData(data, size));
        connection.handle_protocol_error(Error::bad_syntax);
        return;
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
    void parse_message_received(Connection& connection, const char* data, std::size_t size)
    {
        util::Logger& logger = connection.logger;

        util::MemoryInputStream in;
        in.set_buffer(data, data + size);
        in.unsetf(std::ios_base::skipws);
        std::size_t header_size = 0;
        std::string message_type;
        in >> message_type;

        int protocol_version = connection.get_client_protocol_version();
        static_cast<void>(protocol_version); // No divergent protocol behavior (yet).

        if (message_type == "upload") {
            session_ident_type session_ident;
            int is_body_compressed;
            std::size_t uncompressed_body_size, compressed_body_size;
            version_type progress_client_version, progress_server_version;
            version_type locked_server_version;
            char sp_1, sp_2, sp_3, sp_4, sp_5, sp_6, sp_7, newline;
            in >> sp_1 >> session_ident >> sp_2 >> is_body_compressed >> sp_3 >> uncompressed_body_size >> sp_4 >>
                compressed_body_size;
            in >> sp_5 >> progress_client_version >> sp_6 >> progress_server_version >> sp_7 >> locked_server_version;
            in >> newline;
            header_size = std::size_t(in.tellg());
            std::size_t body_size = (is_body_compressed ? compressed_body_size : uncompressed_body_size);
            std::size_t expected_size = header_size + body_size;
            bool good_syntax = (in && sp_1 == ' ' && sp_2 == ' ' && sp_3 == ' ' && sp_4 == ' ' && sp_5 == ' ' &&
                                sp_6 == ' ' && sp_7 == ' ' && newline == '\n' && expected_size == size);
            if (!good_syntax)
                goto bad_syntax;
            if (uncompressed_body_size > s_max_body_size)
                goto limits_exceeded;

            BinaryData body(data + header_size, body_size);
            BinaryData uncompressed_body;
            std::unique_ptr<char[]> uncompressed_body_buffer;
            // if is_body_compressed == true, we must decompress the received body.
            if (is_body_compressed) {
                uncompressed_body_buffer.reset(new char[uncompressed_body_size]);
                std::error_code ec = _impl::compression::decompress(
                    body.data(), compressed_body_size, uncompressed_body_buffer.get(), uncompressed_body_size);

                if (ec) {
                    logger.error("compression::inflate: %1", ec.message());
                    connection.handle_protocol_error(Error::bad_decompression);
                    return;
                }

                uncompressed_body = BinaryData(uncompressed_body_buffer.get(), uncompressed_body_size);
            }
            else {
                uncompressed_body = body;
            }

            logger.debug("Upload message compression: is_body_compressed = %1, "
                         "compressed_body_size=%2, uncompressed_body_size=%3, "
                         "progress_client_version=%4, progress_server_version=%5, "
                         "locked_server_version=%6",
                         is_body_compressed, compressed_body_size, uncompressed_body_size, progress_client_version,
                         progress_server_version, locked_server_version); // Throws

            util::MemoryInputStream in;
            in.unsetf(std::ios_base::skipws);
            in.set_buffer(uncompressed_body.data(), uncompressed_body.data() + uncompressed_body_size);

            std::vector<UploadChangeset> upload_changesets;

            // Loop through the body and find the changesets.
            std::size_t position = 0;
            while (position < uncompressed_body_size) {
                version_type client_version;
                version_type server_version;
                timestamp_type origin_timestamp;
                file_ident_type origin_file_ident;
                std::size_t changeset_size;
                char sp_1, sp_2, sp_3, sp_4, sp_5;

                in >> client_version >> sp_1 >> server_version >> sp_2 >> origin_timestamp >> sp_3 >>
                    origin_file_ident >> sp_4 >> changeset_size >> sp_5;

                bool good_syntax = in && sp_1 == ' ' && sp_2 == ' ' && sp_3 == ' ' && sp_4 == ' ' && sp_5 == ' ';

                if (!good_syntax) {
                    logger.error("Bad changeset header syntax");
                    connection.handle_protocol_error(Error::bad_changeset_header_syntax);
                    return;
                }

                // Update position to the end of the change set
                position = std::size_t(in.tellg()) + changeset_size;

                if (position > uncompressed_body_size) {
                    logger.error("Bad changeset size");
                    connection.handle_protocol_error(Error::bad_changeset_size);
                    return;
                }

                BinaryData changeset_data(uncompressed_body.data() + std::size_t(in.tellg()), changeset_size);
                in.seekg(position);

                if (logger.would_log(util::Logger::Level::trace)) {
                    logger.trace("Received: UPLOAD CHANGESET(client_version=%1, server_version=%2, "
                                 "origin_timestamp=%3, origin_file_ident=%4, changeset_size=%5)",
                                 client_version, server_version, origin_timestamp, origin_file_ident,
                                 changeset_size); // Throws
                    logger.trace("Changeset: %1",
                                 clamped_hex_dump(changeset_data)); // Throws
                }

                UploadChangeset upload_changeset{UploadCursor{client_version, server_version}, origin_timestamp,
                                                 origin_file_ident, changeset_data};

                upload_changesets.push_back(upload_changeset); // Throws
            }

            connection.receive_upload_message(session_ident, progress_client_version, progress_server_version,
                                              locked_server_version,
                                              upload_changesets); // Throws
            return;
        }
        if (message_type == "mark") {
            session_ident_type session_ident;
            request_ident_type request_ident;
            char sp_1, sp_2, newline;
            in >> sp_1 >> session_ident >> sp_2 >> request_ident >> newline;
            header_size = std::size_t(in.tellg());
            std::size_t expected_size = header_size;
            bool good_syntax = (in && sp_1 == ' ' && sp_2 == ' ' && newline == '\n' && expected_size == size);
            if (!good_syntax)
                goto bad_syntax;

            connection.receive_mark_message(session_ident, request_ident); // Throws
            return;
        }
        if (message_type == "ping") {
            milliseconds_type timestamp, rtt;

            char sp_1 = 0, sp_2 = 0, newline = 0;
            in >> sp_1 >> timestamp >> sp_2 >> rtt >> newline;
            std::size_t expected_size = std::size_t(in.tellg());

            bool good_syntax = (in && sp_1 == ' ' && sp_2 == ' ' && newline == '\n' && expected_size == size);
            if (!good_syntax)
                goto bad_syntax;

            connection.receive_ping(timestamp, rtt);
            return;
        }
        if (message_type == "bind") {
            session_ident_type session_ident;
            std::size_t path_size;
            std::size_t signed_user_token_size;
            bool need_client_file_ident;
            bool is_subserver;
            char sp_1, sp_2, sp_3, sp_4, sp_5, newline;
            in >> sp_1 >> session_ident >> sp_2 >> path_size >> sp_3 >> signed_user_token_size >> sp_4 >>
                need_client_file_ident >> sp_5 >> is_subserver >> newline;
            header_size = std::size_t(in.tellg());
            std::size_t expected_size = header_size + path_size + signed_user_token_size;
            bool good_syntax = (in && sp_1 == ' ' && sp_2 == ' ' && sp_3 == ' ' && sp_4 == ' ' && sp_5 == ' ' &&
                                newline == '\n' && expected_size == size && path_size != 0);
            if (!good_syntax)
                goto bad_syntax;
            if (path_size > s_max_path_size)
                goto limits_exceeded;
            if (signed_user_token_size > s_max_signed_user_token_size)
                goto limits_exceeded;

            std::string path{data + header_size, path_size}; // Throws
            std::string signed_user_token(data + header_size + path_size,
                                          signed_user_token_size); // Throws

            connection.receive_bind_message(session_ident, std::move(path), std::move(signed_user_token),
                                            need_client_file_ident, is_subserver); // Throws
            return;
        }
        if (message_type == "refresh") {
            session_ident_type session_ident;
            std::size_t signed_user_token_size;
            char sp_1, sp_2, newline;
            in >> sp_1 >> session_ident >> sp_2 >> signed_user_token_size >> newline;
            header_size = std::size_t(in.tellg());
            std::size_t expected_size = header_size + signed_user_token_size;
            bool good_syntax = (in && sp_1 == ' ' && sp_2 == ' ' && newline == '\n' && expected_size == size);
            if (!good_syntax)
                goto bad_syntax;
            if (signed_user_token_size > s_max_signed_user_token_size)
                goto limits_exceeded;

            std::string signed_user_token{data + header_size, signed_user_token_size};

            connection.receive_refresh_message(session_ident, std::move(signed_user_token)); // Throws
            return;
        }
        if (message_type == "ident") {
            session_ident_type session_ident;
            file_ident_type client_file_ident;
            salt_type client_file_ident_salt;
            version_type scan_server_version, scan_client_version, latest_server_version;
            salt_type latest_server_version_salt;
            char sp_1, sp_2, sp_3, sp_4, sp_5, sp_6, sp_7, sp_8, newline;
            in >> sp_1 >> session_ident >> sp_2 >> client_file_ident >> sp_3 >> client_file_ident_salt >> sp_4 >>
                scan_server_version >> sp_5 >> scan_client_version >> sp_6 >> latest_server_version >> sp_7 >>
                latest_server_version_salt >> newline;
            sp_8 = ' ';
            header_size = std::size_t(in.tellg());
            std::size_t expected_size = header_size;
            bool good_syntax =
                (in && sp_1 == ' ' && sp_2 == ' ' && sp_3 == ' ' && sp_4 == ' ' && sp_5 == ' ' && sp_6 == ' ' &&
                 sp_7 == ' ' && sp_8 == ' ' && newline == '\n' && expected_size == size);
            if (!good_syntax)
                goto bad_syntax;

            connection.receive_ident_message(session_ident, client_file_ident, client_file_ident_salt,
                                             scan_server_version, scan_client_version, latest_server_version,
                                             latest_server_version_salt); // Throws
            return;
        }
        if (message_type == "alloc") {
            session_ident_type session_ident;
            char sp_1, newline;
            in >> sp_1 >> session_ident >> newline;
            header_size = std::size_t(in.tellg());
            std::size_t expected_size = header_size;
            bool good_syntax = (in && sp_1 == ' ' && newline == '\n' && expected_size == size);
            if (!good_syntax)
                goto bad_syntax;

            connection.receive_alloc_message(session_ident); // Throws
            return;
        }
        if (message_type == "unbind") {
            session_ident_type session_ident;
            char sp_1, newline;
            in >> sp_1 >> session_ident >> newline;
            header_size = std::size_t(in.tellg());
            std::size_t expected_size = header_size;
            bool good_syntax = (in && sp_1 == ' ' && newline == '\n' && expected_size == size);
            if (!good_syntax)
                goto bad_syntax;

            connection.receive_unbind_message(session_ident); // Throws
            return;
        }

        // unknown message
        if (size < 256)
            logger.error("Unknown input message type '%1'", StringData(data, size)); // Throws
        else
            logger.error("Unknown input message type '%1'.......", StringData(data, 256)); // Throws

        connection.handle_protocol_error(Error::unknown_message);
        return;

    bad_syntax:
        logger.error("Bad syntax in input message '%1'", StringData(data, size));
        connection.handle_protocol_error(Error::bad_syntax); // Throws
        return;
    limits_exceeded:
        logger.error("Limits exceeded in input message '%1'", StringData(data, header_size));
        connection.handle_protocol_error(Error::limits_exceeded); // Throws
        return;
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

} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_PROTOCOL_CODEC_HPP
