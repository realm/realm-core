#include <realm/util/assert.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/from_chars.hpp>
#include <realm/sync/noinst/protocol_codec.hpp>

namespace realm::_impl {

using OutputBuffer = util::ResettableExpandableBufferOutputStream;

// Client protocol

void ClientProtocol::make_bind_message(int protocol_version, OutputBuffer& out, session_ident_type session_ident,
                                       const std::string& server_path, const std::string& signed_user_token,
                                       bool need_client_file_ident, bool is_subserver)
{
    static_cast<void>(protocol_version);
    out << "bind " << session_ident << " " << server_path.size() << " " << signed_user_token.size() << " "
        << int(need_client_file_ident); // Throws
    out << " " << int(is_subserver);    // Throws
    out << "\n";                        // Throws
    REALM_ASSERT(!out.fail());

    out.write(server_path.data(), server_path.size());             // Throws
    out.write(signed_user_token.data(), signed_user_token.size()); // Throws
}

void ClientProtocol::make_pbs_ident_message(OutputBuffer& out, session_ident_type session_ident,
                                            SaltedFileIdent client_file_ident, const SyncProgress& progress)
{
    out << "ident " << session_ident << " " << client_file_ident.ident << " " << client_file_ident.salt << " "
        << progress.download.server_version << " " << progress.download.last_integrated_client_version << " "
        << progress.latest_server_version.version << " " << progress.latest_server_version.salt << "\n"; // Throws
    REALM_ASSERT(!out.fail());
}

void ClientProtocol::make_flx_ident_message(OutputBuffer& out, session_ident_type session_ident,
                                            SaltedFileIdent client_file_ident, const SyncProgress& progress,
                                            int64_t query_version, std::string_view query_body)
{
    out << "ident " << session_ident << " " << client_file_ident.ident << " " << client_file_ident.salt << " "
        << progress.download.server_version << " " << progress.download.last_integrated_client_version << " "
        << progress.latest_server_version.version << " " << progress.latest_server_version.salt << " "
        << query_version << " " << query_body.size() << "\n"
        << query_body; // Throws
    REALM_ASSERT(!out.fail());
}

void ClientProtocol::make_query_change_message(OutputBuffer& out, session_ident_type session, int64_t version,
                                               std::string_view query_body)
{
    out << "query " << session << " " << version << " " << query_body.size() << "\n" << query_body; // throws
    REALM_ASSERT(!out.fail());
}

ClientProtocol::UploadMessageBuilder::UploadMessageBuilder(
    util::Logger& logger, OutputBuffer& body_buffer, std::vector<char>& compression_buffer,
    util::compression::CompressMemoryArena& compress_memory_arena)
    : logger{logger}
    , m_body_buffer{body_buffer}
    , m_compression_buffer{compression_buffer}
    , m_compress_memory_arena{compress_memory_arena}
{
    m_body_buffer.reset();
}

void ClientProtocol::UploadMessageBuilder::add_changeset(version_type client_version, version_type server_version,
                                                         timestamp_type origin_timestamp,
                                                         file_ident_type origin_file_ident,
                                                         ChunkedBinaryData changeset)
{
    m_body_buffer << client_version << " " << server_version << " " << origin_timestamp << " " << origin_file_ident
                  << " " << changeset.size() << " "; // Throws
    changeset.write_to(m_body_buffer);               // Throws
    REALM_ASSERT(!m_body_buffer.fail());

    ++m_num_changesets;
}

void ClientProtocol::UploadMessageBuilder::make_upload_message(int protocol_version, OutputBuffer& out,
                                                               session_ident_type session_ident,
                                                               version_type progress_client_version,
                                                               version_type progress_server_version,
                                                               version_type locked_server_version)
{
    static_cast<void>(protocol_version);
    BinaryData body = {m_body_buffer.data(), std::size_t(m_body_buffer.size())};

    constexpr std::size_t g_max_uncompressed = 1024;

    bool is_body_compressed = false;
    if (body.size() > g_max_uncompressed) {
        util::compression::allocate_and_compress(m_compress_memory_arena, body,
                                                 m_compression_buffer); // Throws
        is_body_compressed = m_compression_buffer.size() < body.size();
    }

    // The compressed body is only sent if it is smaller than the uncompressed body.
    std::size_t compressed_body_size = is_body_compressed ? m_compression_buffer.size() : 0;

    // The header of the upload message.
    out << "upload " << session_ident << " " << int(is_body_compressed) << " " << body.size() << " "
        << compressed_body_size;
    out << " " << progress_client_version << " " << progress_server_version << " " << locked_server_version; // Throws
    out << "\n";                                                                                             // Throws

    if (is_body_compressed)
        out.write(m_compression_buffer.data(), compressed_body_size); // Throws
    else
        out.write(body.data(), body.size()); // Throws

    REALM_ASSERT(!out.fail());
}

ClientProtocol::UploadMessageBuilder ClientProtocol::make_upload_message_builder(util::Logger& logger)
{
    return UploadMessageBuilder{logger, m_output_buffer, m_buffer, m_compress_memory_arena};
}

void ClientProtocol::make_unbind_message(OutputBuffer& out, session_ident_type session_ident)
{
    out << "unbind " << session_ident << "\n"; // Throws
    REALM_ASSERT(!out.fail());
}

void ClientProtocol::make_mark_message(OutputBuffer& out, session_ident_type session_ident,
                                       request_ident_type request_ident)
{
    out << "mark " << session_ident << " " << request_ident << "\n"; // Throws
    REALM_ASSERT(!out.fail());
}


void ClientProtocol::make_ping(OutputBuffer& out, milliseconds_type timestamp, milliseconds_type rtt)
{
    out << "ping " << timestamp << " " << rtt << "\n"; // Throws
}


std::string ClientProtocol::compressed_hex_dump(BinaryData blob)
{
    std::vector<char> buf;
    util::compression::allocate_and_compress(m_compress_memory_arena, blob, buf); // Throws

    std::string encode_buffer;
    auto encoded_size = util::base64_encoded_size(buf.size());
    encode_buffer.resize(encoded_size);
    util::base64_encode(buf.data(), buf.size(), encode_buffer.data(), encode_buffer.size());

    return encode_buffer;
}

// Server protocol

void ServerProtocol::make_ident_message(int protocol_version, OutputBuffer& out, session_ident_type session_ident,
                                        file_ident_type client_file_ident, salt_type client_file_ident_salt)
{
    static_cast<void>(protocol_version);
    out << "ident " << session_ident << " " << client_file_ident << " " << client_file_ident_salt << "\n"; // Throws
}

void ServerProtocol::make_alloc_message(OutputBuffer& out, session_ident_type session_ident,
                                        file_ident_type file_ident)
{
    out << "alloc " << session_ident << " " << file_ident << "\n"; // Throws
}


/// insert_single_changeset_download_message() inserts a single changeset and
/// the associated meta data into the output buffer.
///
/// It is the functions responsibility to make sure that the buffer has
/// capacity to hold the inserted data.
///
/// The message format for the single changeset is <server_version>
/// <client_version> <timestamp> <client_file_ident> <changeset size>
/// <changeset>
void ServerProtocol::insert_single_changeset_download_message(OutputBuffer& out, const ChangesetInfo& changeset_info,
                                                              util::Logger& logger)
{
    const sync::HistoryEntry& entry = changeset_info.entry;

    out << changeset_info.server_version << " " << changeset_info.client_version << " " << entry.origin_timestamp
        << " " << entry.origin_file_ident << " " << changeset_info.original_size << " " << entry.changeset.size()
        << " ";
    entry.changeset.write_to(out);

    if (logger.would_log(util::Logger::Level::trace)) {
        logger.trace("DOWNLOAD: insert single changeset (server_version=%1, "
                     "client_version=%2, timestamp=%3, client_file_ident=%4, "
                     "original_changeset_size=%5, changeset_size=%6, changeset='%7').",
                     changeset_info.server_version, changeset_info.client_version, entry.origin_timestamp,
                     entry.origin_file_ident, changeset_info.original_size, entry.changeset.size(),
                     _impl::clamped_hex_dump(entry.changeset.get_first_chunk())); // Throws
    }
}


void ServerProtocol::make_download_message(int protocol_version, OutputBuffer& out, session_ident_type session_ident,
                                           version_type download_server_version, version_type download_client_version,
                                           version_type latest_server_version, salt_type latest_server_version_salt,
                                           version_type upload_client_version, version_type upload_server_version,
                                           std::uint_fast64_t downloadable_bytes, std::size_t num_changesets,
                                           const char* body, std::size_t uncompressed_body_size,
                                           std::size_t compressed_body_size, bool body_is_compressed,
                                           util::Logger& logger)
{
    static_cast<void>(protocol_version);
    // The header of the download message.
    out << "download " << session_ident << " " << download_server_version << " " << download_client_version << " "
        << latest_server_version << " " << latest_server_version_salt << " " << upload_client_version << " "
        << upload_server_version << " " << downloadable_bytes << " " << int(body_is_compressed) << " "
        << uncompressed_body_size << " " << compressed_body_size << "\n"; // Throws

    std::size_t body_size = (body_is_compressed ? compressed_body_size : uncompressed_body_size);
    out.write(body, body_size);

    logger.detail("Sending: DOWNLOAD(download_server_version=%1, download_client_version=%2, "
                  "latest_server_version=%3, latest_server_version_salt=%4, "
                  "upload_client_version=%5, upload_server_version=%6, "
                  "num_changesets=%7, is_body_compressed=%8, body_size=%9, "
                  "compressed_body_size=%10)",
                  download_server_version, download_client_version, latest_server_version, latest_server_version_salt,
                  upload_client_version, upload_server_version, num_changesets, body_is_compressed,
                  uncompressed_body_size, compressed_body_size); // Throws
}


void ServerProtocol::make_unbound_message(OutputBuffer& out, session_ident_type session_ident)
{
    out << "unbound " << session_ident << "\n"; // Throws
}


void ServerProtocol::make_mark_message(OutputBuffer& out, session_ident_type session_ident,
                                       request_ident_type request_ident)
{
    out << "mark " << session_ident << " " << request_ident << "\n"; // Throws
    REALM_ASSERT(!out.fail());
}


void ServerProtocol::make_error_message(int protocol_version, OutputBuffer& out, sync::ProtocolError error_code,
                                        const char* message, std::size_t message_size, bool try_again,
                                        session_ident_type session_ident)
{
    static_cast<void>(protocol_version);
    sync::ProtocolError error_code_2 = error_code;
    out << "error " << int(error_code_2) << " " << message_size << " " << int(try_again) << " " << session_ident
        << "\n";                      // Throws
    out.write(message, message_size); // Throws
}


void ServerProtocol::make_pong(OutputBuffer& out, milliseconds_type timestamp)
{
    out << "pong " << timestamp << "\n"; // Throws
}


std::string make_authorization_header(const std::string& signed_user_token)
{
    return "Bearer " + signed_user_token;
}


util::Optional<StringData> parse_authorization_header(const std::string& authorization_header)
{
    StringData prefix = "Bearer ";
    // Token contains at least three characters. Stricter checks are possible, but do
    // not belong here.
    if (authorization_header.size() < prefix.size() + 4)
        return util::none;

    if (authorization_header.compare(0, prefix.size(), prefix) != 0)
        return util::none;

    std::size_t token_size = authorization_header.size() - prefix.size();
    return StringData{authorization_header.data() + prefix.size(), token_size};
}

} // namespace realm::_impl
