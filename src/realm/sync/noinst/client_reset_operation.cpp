#include <realm/db.hpp>
#include <realm/sync/encrypt/fingerprint.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/noinst/common_dir.hpp>
#include <realm/sync/noinst/compression.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/noinst/client_reset_operation.hpp>

namespace realm::_impl {

ClientResetOperation::ClientResetOperation(util::Logger& logger, const std::string& realm_path,
                                           const std::string& metadata_dir,
                                           util::Optional<std::array<char, 64>> encryption_key)
    : logger{logger}
    , m_realm_path{realm_path}
    , m_encryption_key{encryption_key}
{
    logger.debug("Create ClientStateDownload, realm_path = %1, metadata_dir = %2", realm_path, metadata_dir);
#ifdef REALM_ENABLE_ENCRYPTION
    if (m_encryption_key)
        m_aes_cryptor =
            std::make_unique<util::AESCryptor>(reinterpret_cast<unsigned char*>(m_encryption_key->data()));
#else
    REALM_ASSERT(!encryption_key);
#endif
    if (!util::File::is_dir(metadata_dir)) {
        const std::string msg = metadata_dir + " must be an existing directory";
        throw std::runtime_error(msg);
    }
}

bool ClientResetOperation::finalize(sync::SaltedFileIdent salted_file_ident)
{
    m_salted_file_ident = salted_file_ident;
    bool local_realm_exists = util::File::exists(m_realm_path);
    logger.debug("finalize_client_reset, realm_path = %1, local_realm_exists = %2", m_realm_path, local_realm_exists);

    // only do the reset if the file exists
    // if there is no existing file, there is nothing to reset
    if (local_realm_exists) {
        client_reset::LocalVersionIDs local_version_ids;
        try {
            local_version_ids = client_reset::perform_client_reset_diff(
                m_realm_path, m_encryption_key, m_salted_file_ident, m_server_version, logger);
        }
        catch (util::File::AccessError& e) {
            logger.error("In finalize_client_reset, the client reset failed, "
                         "realm path = %1, msg = %2",
                         m_realm_path, e.what());
            return false;
        }

        m_client_reset_old_version = local_version_ids.old_version;
        m_client_reset_new_version = local_version_ids.new_version;
        return true;
    }

    return false;
}

} // namespace realm::_impl
