
#ifndef REALM_NOINST_CLIENT_STATE_DOWNLOAD_HPP
#define REALM_NOINST_CLIENT_STATE_DOWNLOAD_HPP

#include <realm/binary_data.hpp>
#include <realm/util/logger.hpp>
#include <realm/util/optional.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/util/aes_cryptor.hpp>

namespace realm {
namespace _impl {

// A ClientResetOperation object is used per client session to keep track of
// state Realm download.
class ClientResetOperation {
public:
    util::Logger& logger;

    ClientResetOperation(util::Logger& logger, const std::string& realm_path, const std::string& metadata_dir,
                         util::Optional<std::array<char, 64>> encryption_key);

    // When the client has received the salted file ident from the server, it
    // should deliver the ident to the ClientStateDownload object. The ident
    // will be inserted in the Realm after download.
    void set_salted_file_ident(sync::SaltedFileIdent salted_file_ident);

    // receive_state receives the values received from a STATE message. The
    // return value is true if the values were compatible with prior values,
    // false otherwise.
    bool receive_state(sync::version_type server_version, sync::salt_type server_version_salt,
                       uint_fast64_t begin_offset, uint_fast64_t end_offset, uint_fast64_t max_offset,
                       BinaryData chunk);

    sync::version_type get_server_version();
    sync::salt_type get_server_version_salt();

    realm::VersionID get_client_reset_old_version();
    realm::VersionID get_client_reset_new_version();

private:
    const std::string m_realm_path;
    util::Optional<std::array<char, 64>> m_encryption_key;
#if REALM_ENABLE_ENCRYPTION
    std::unique_ptr<util::AESCryptor> m_aes_cryptor;
#endif

    sync::SaltedFileIdent m_salted_file_ident = {0, 0};
    sync::SaltedVersion m_server_version = {0, 0};

    realm::VersionID m_client_reset_old_version;
    realm::VersionID m_client_reset_new_version;

    bool finalize();
};

// Implementation

inline sync::version_type ClientResetOperation::get_server_version()
{
    return m_server_version.version;
}

inline sync::salt_type ClientResetOperation::get_server_version_salt()
{
    return m_server_version.salt;
}

inline realm::VersionID ClientResetOperation::get_client_reset_old_version()
{
    return m_client_reset_old_version;
}

inline realm::VersionID ClientResetOperation::get_client_reset_new_version()
{
    return m_client_reset_new_version;
}

} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_CLIENT_STATE_DOWNLOAD_HPP
