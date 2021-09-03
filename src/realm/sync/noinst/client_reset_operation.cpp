#include <realm/db.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/noinst/client_reset_operation.hpp>

namespace realm::_impl {

struct ResetPathPair {
    // takes a fresh or local path and distinguishes the two
    ResetPathPair(std::string path)
    {
        const std::string fresh_suffix = ".fresh";
        const size_t suffix_len = fresh_suffix.size();
        if (path.size() > suffix_len && path.substr(path.size() - suffix_len, suffix_len) == fresh_suffix) {
            fresh_path = path;
            local_path = path.substr(0, path.size() - suffix_len);
        }
        else {
            fresh_path = path + fresh_suffix;
            local_path = path;
        }
    }
    std::string local_path;
    std::string fresh_path;
};

ClientResetOperation::ClientResetOperation(
    util::Logger& logger, DB& db, DBRef db_fresh, bool seamless_loss,
    std::function<void(TransactionRef local, TransactionRef remote)> notify_before,
    std::function<void(TransactionRef local)> notify_after)
    : logger{logger}
    , m_db{db}
    , m_db_fresh(std::move(db_fresh))
    , m_seamless_loss(seamless_loss)
    , m_notify_before(notify_before)
    , m_notify_after(notify_after)
{
    logger.debug("Create ClientStateDownload, realm_path = %1, seamless_loss = %2", m_db.get_path(), seamless_loss);
}

std::string ClientResetOperation::get_fresh_path_for(const std::string& realm_path)
{
    ResetPathPair paths(realm_path);
    REALM_ASSERT_EX(paths.fresh_path != realm_path, realm_path);
    return paths.fresh_path;
}

bool ClientResetOperation::finalize(sync::SaltedFileIdent salted_file_ident)
{
    if (m_seamless_loss) {
        ResetPathPair paths(m_db.get_path());
        REALM_ASSERT_EX(m_db.get_path() == paths.local_path, m_db.get_path(), paths.local_path, paths.fresh_path);
        REALM_ASSERT(m_db_fresh);
    }

    m_salted_file_ident = salted_file_ident;
    // only do the reset if the file exists
    // if there is no existing file, there is nothing to reset
    bool local_realm_exists = m_db.get_version_of_latest_snapshot() != 0;
    if (local_realm_exists) {
        logger.debug("ClientResetOperation::finalize, realm_path = %1, local_realm_exists = %2", m_db.get_path(),
                     local_realm_exists);

        client_reset::LocalVersionIDs local_version_ids;
        try {
            local_version_ids = client_reset::perform_client_reset_diff(m_db, m_db_fresh, m_notify_before,
                                                                        m_notify_after, m_salted_file_ident, logger);
        }
        catch (util::File::AccessError& e) {
            logger.error("In ClientResetOperation::finalize, the client reset failed due to a FileAccessError, "
                         "realm path = %1, msg = %2",
                         m_db.get_path(), e.what());
            return false;
        }
        catch (client_reset::ClientResetFailed& e) {
            logger.error("In ClientResetOperation::finalize, the client reset failed, "
                         "realm path = %1, msg = %2",
                         m_db.get_path(), e.what());
            return false;
        }

        m_client_reset_old_version = local_version_ids.old_version;
        m_client_reset_new_version = local_version_ids.new_version;

        if (m_db_fresh) {
            std::string path_to_clean = m_db_fresh->get_path();
            try {
                // In order to obtain the lock and delete the realm, we first have to close
                // the Realm. This requires that we are the only remaining ref holder, and
                // this is expected. Releasing the last ref should release the hold on the
                // lock file and allow us to clean up.
                long use_count = m_db_fresh.use_count();
                REALM_ASSERT_DEBUG_EX(m_db_fresh.use_count() == 1, m_db_fresh.use_count(), path_to_clean);
                m_db_fresh.reset();
                // clean up the fresh Realm
                // we don't mind leaving the fresh lock file around because trying to delete it
                // here could cause a race if there are multiple resets ongoing
                bool did_lock = DB::call_with_lock(path_to_clean, [&](const std::string& path) {
                    constexpr bool delete_lockfile = false;
                    DB::delete_files(path, nullptr, delete_lockfile);
                });
                if (!did_lock) {
                    logger.warn("In ClientResetOperation::finalize, the fresh copy '%1' could not be cleaned up. "
                                "There were %2 refs remaining.",
                                path_to_clean, use_count);
                }
            }
            catch (const std::exception& err) {
                logger.warn("In ClientResetOperation::finalize, the fresh copy '%1' could not be cleaned up due to "
                            "an exception: '%2'",
                            path_to_clean, err.what());
                // ignored, this is just a best effort
            }
        }

        return true;
    }
    return false;
}

} // namespace realm::_impl
