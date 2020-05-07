
#ifndef REALM_NOINST_CLIENT_STATE_DOWNLOAD_HPP
#define REALM_NOINST_CLIENT_STATE_DOWNLOAD_HPP

#include <realm/binary_data.hpp>
#include <realm/util/logger.hpp>
#include <realm/util/optional.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/util/aes_cryptor.hpp>

namespace realm {
namespace _impl {

// A ClientStateDownload object is used per client session to keep track of
// state Realm download.
class ClientStateDownload {
public:
    util::Logger& logger;

    ClientStateDownload(util::Logger& logger, const std::string& realm_path, const std::string& metadata_dir,
                        bool recover_local_changes, util::Optional<std::array<char, 64>> encryption_key);

    // When the client has received the salted file ident from the server, it
    // should deliver the ident to the ClientStateDownload object. The ident
    // will be inserted in the Realm after download.
    void set_salted_file_ident(sync::SaltedFileIdent salted_file_ident);

    // When the client has obtained a client reset clent version from the
    // CLIENT_VERSION meesage, it should deliver it to the ClientStateDownload
    // object. The client version will be used for the client reset computation.
    void set_client_reset_client_version(sync::version_type client_version);

    // receive_state receives the values received from a STATE message. The
    // return value is true if the values were compatible with prior values,
    // false otherwise.
    bool receive_state(sync::version_type server_version, sync::salt_type server_version_salt,
                       uint_fast64_t begin_offset, uint_fast64_t end_offset, uint_fast64_t max_offset,
                       BinaryData chunk);

    sync::version_type get_server_version();
    sync::salt_type get_server_version_salt();
    uint_fast64_t get_end_offset();

    bool is_complete();

    bool is_client_reset();
    realm::VersionID get_client_reset_old_version();
    realm::VersionID get_client_reset_new_version();

private:
    const std::string m_realm_path;
    const std::string m_versioned_metadata_dir;
    const std::string m_meta_realm_path;
    const std::string m_partially_downloaded_realm_path;
    util::Optional<std::array<char, 64>> m_encryption_key;
#if REALM_ENABLE_ENCRYPTION
    std::unique_ptr<AESCryptor> m_aes_cryptor;
#endif

    bool m_complete = false;
    sync::SaltedFileIdent m_salted_file_ident = {0, 0};
    sync::SaltedVersion m_server_version = {0, 0};

    // m_client_reset_client_version is used in client reset.  It is the latest
    // client version that the server has integrated before the client reset.
    // This number is obtained from the server with a CLIENT_VERSION_REQUEST.
    sync::version_type m_client_reset_client_version = 0;

    // Recover local changes in client reset.
    bool m_recover_local_changes = true;

    uint_fast64_t m_end_offset = 0;
    uint_fast64_t m_max_offset = 0;
    uint_fast64_t m_file_size = 0;

    bool m_is_client_reset = false;
    realm::VersionID m_client_reset_old_version;
    realm::VersionID m_client_reset_new_version;

    void initialize();
    void initialize_from_new();
    bool initialize_from_existing();

    bool finalize();
    bool finalize_async_open();
    bool finalize_client_reset();

    void reset();
};

// Implementation

inline sync::version_type ClientStateDownload::get_server_version()
{
    return m_server_version.version;
}

inline sync::salt_type ClientStateDownload::get_server_version_salt()
{
    return m_server_version.salt;
}

inline uint_fast64_t ClientStateDownload::get_end_offset()
{
    return m_end_offset;
}

inline bool ClientStateDownload::is_complete()
{
    return m_complete;
}

inline bool ClientStateDownload::is_client_reset()
{
    return m_is_client_reset;
}

inline realm::VersionID ClientStateDownload::get_client_reset_old_version()
{
    return m_client_reset_old_version;
}

inline realm::VersionID ClientStateDownload::get_client_reset_new_version()
{
    return m_client_reset_new_version;
}

} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_CLIENT_STATE_DOWNLOAD_HPP
