#include <realm/db.hpp>
#include <realm/sync/encrypt/fingerprint.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/noinst/common_dir.hpp>
#include <realm/sync/noinst/compression.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/noinst/client_reset_operation.hpp>

using namespace realm;
using namespace _impl;
using namespace _impl::client_reset;

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
        m_aes_cryptor.reset(new util::AESCryptor(reinterpret_cast<unsigned char*>(m_encryption_key->data())));
#else
    REALM_ASSERT(!encryption_key);
#endif
    if (!util::File::is_dir(metadata_dir)) {
        const std::string msg = metadata_dir + " must be an existing directory";
        throw std::runtime_error(msg);
    }
}

void ClientResetOperation::set_salted_file_ident(sync::SaltedFileIdent salted_file_ident)
{
    m_salted_file_ident = salted_file_ident;
}

bool ClientResetOperation::receive_state(sync::version_type, sync::salt_type, uint_fast64_t, uint_fast64_t,
                                         uint_fast64_t, BinaryData)
{
    REALM_ASSERT(m_salted_file_ident.ident != 0);
    return finalize();
}

bool ClientResetOperation::finalize()
{
    bool local_realm_exists = util::File::exists(m_realm_path);

    REALM_ASSERT(local_realm_exists);

    logger.debug("finalize_client_reset, realm_path = %1", m_realm_path);
    REALM_ASSERT(util::File::exists(m_realm_path));

    LocalVersionIDs local_version_ids;
    try {
        local_version_ids =
            perform_client_reset_diff(m_realm_path, m_encryption_key, m_salted_file_ident, m_server_version, logger);
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
