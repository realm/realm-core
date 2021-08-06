
#ifndef REALM_NOINST_CLIENT_RESET_OPERATION_HPP
#define REALM_NOINST_CLIENT_RESET_OPERATION_HPP

#include <realm/binary_data.hpp>
#include <realm/util/logger.hpp>
#include <realm/util/optional.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/util/aes_cryptor.hpp>

namespace realm::_impl {

// A ClientResetOperation object is used per client session to keep track of
// state Realm download.
class ClientResetOperation {
public:
    util::Logger& logger;

    ClientResetOperation(util::Logger& logger, const std::string& realm_path, const std::string& metadata_dir,
                         util::Optional<std::array<char, 64>> encryption_key);

    // When the client has received the salted file ident from the server, it
    // should deliver the ident to the ClientResetOperation object. The ident
    // will be inserted in the Realm after download.
    bool finalize(sync::SaltedFileIdent salted_file_ident);

    sync::version_type get_server_version() const noexcept;
    sync::salt_type get_server_version_salt() const noexcept;

    realm::VersionID get_client_reset_old_version() const noexcept;
    realm::VersionID get_client_reset_new_version() const noexcept;

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
};

// Implementation

inline sync::version_type ClientResetOperation::get_server_version() const noexcept
{
    return m_server_version.version;
}

inline sync::salt_type ClientResetOperation::get_server_version_salt() const noexcept
{
    return m_server_version.salt;
}

inline realm::VersionID ClientResetOperation::get_client_reset_old_version() const noexcept
{
    return m_client_reset_old_version;
}

inline realm::VersionID ClientResetOperation::get_client_reset_new_version() const noexcept
{
    return m_client_reset_new_version;
}

} // namespace realm::_impl

#endif // REALM_NOINST_CLIENT_RESET_OPERATION_HPP
