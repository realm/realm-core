#include <realm/db.hpp>
#include <realm/sync/encrypt/fingerprint.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/noinst/compression.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/noinst/client_reset_operation.hpp>

namespace realm::_impl {

ClientResetOperation::ClientResetOperation(util::Logger& logger, DB& db, const std::string& metadata_dir)
    : logger{logger}
    , m_db(db)
    , m_metadata_dir(metadata_dir)
{
    logger.debug("Create ClientStateDownload, realm_path = %1, metadata_dir = %2", m_db.get_path(), metadata_dir);
}

bool ClientResetOperation::finalize(sync::SaltedFileIdent salted_file_ident)
{
    m_salted_file_ident = salted_file_ident;
    bool local_realm_exists = m_db.get_version_of_latest_snapshot() != 0;
    logger.debug("finalize_client_reset, realm_path = %1, local_realm_exists = %2", m_db.get_path(),
                 local_realm_exists);

    // only do the reset if the file exists
    // if there is no existing file, there is nothing to reset
    if (local_realm_exists) {
        client_reset::LocalVersionIDs local_version_ids;
        try {
            local_version_ids =
                client_reset::perform_client_reset_diff(m_db, m_salted_file_ident, m_server_version, logger);
        }
        catch (util::File::AccessError& e) {
            logger.error("In finalize_client_reset, the client reset failed, "
                         "realm path = %1, msg = %2",
                         m_db.get_path(), e.what());
            return false;
        }

        m_client_reset_old_version = local_version_ids.old_version;
        m_client_reset_new_version = local_version_ids.new_version;
        return true;
    }

    return false;
}

} // namespace realm::_impl
