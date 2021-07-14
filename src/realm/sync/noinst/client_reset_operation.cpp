#include <realm/db.hpp>
#include <realm/sync/encrypt/fingerprint.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/noinst/compression.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/noinst/client_reset_operation.hpp>

namespace realm::_impl {

const char* table_name_to_check = "_client_reset_status_complete";
bool fresh_copy_is_downloaded(std::string path, util::Optional<std::array<char, 64>> encryption_key)
{
    if (!util::File::exists(path)) {
        return false;
    }

    DBOptions shared_group_options(encryption_key ? encryption_key->data() : nullptr);
    ClientHistoryImpl history_local{path};
    DBRef sg_local = DB::create(history_local, shared_group_options);

    auto group_local = sg_local->start_read();
    return bool(group_local->find_table(table_name_to_check));
}

void mark_fresh_copy_as_downloaded(std::string path, util::Optional<std::array<char, 64>> encryption_key)
{
    DBOptions shared_group_options(encryption_key ? encryption_key->data() : nullptr);
    ClientHistoryImpl history_local{path};
    DBRef sg_local = DB::create(history_local, shared_group_options);

    auto group_local = sg_local->start_write();
    group_local->get_or_add_table(table_name_to_check);
    group_local->commit();
}

struct ResetPathPair {
    // takes a fresh or local path and distinguishes the two
    ResetPathPair(std::string path)
    {
        // FIXME: this needs to go into a separate temp/metadata directory
        // because users could potentially choose the same name for a different Realm.
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

ClientResetOperation::ClientResetOperation(util::Logger& logger, const std::string& realm_path, bool seamless_loss,
                                           util::Optional<std::array<char, 64>> encryption_key)
    : logger{logger}
    , m_realm_path{realm_path}
    , m_seamless_loss(seamless_loss)
    , m_encryption_key{encryption_key}
{
    logger.debug("Create ClientStateDownload, realm_path = %1, seamless_loss = %2", realm_path, seamless_loss);
#ifdef REALM_ENABLE_ENCRYPTION
    if (m_encryption_key)
        m_aes_cryptor =
            std::make_unique<util::AESCryptor>(reinterpret_cast<unsigned char*>(m_encryption_key->data()));
#else
    REALM_ASSERT(!encryption_key);
#endif
}

void ClientResetOperation::download_complete()
{
    ResetPathPair paths(m_realm_path);
    // it should only be possible to reach download completion on the fresh copy of the Realm
    // because the local Realm has already recieved the reset error at some point to get here
    REALM_ASSERT_RELEASE_EX(paths.fresh_path == m_realm_path, paths.fresh_path, paths.local_path, m_realm_path);
    mark_fresh_copy_as_downloaded(m_realm_path, m_encryption_key);
}

std::string ClientResetOperation::open_session_for_path(std::string realm_path, bool seamless_loss,
                                                        util::Optional<std::array<char, 64>> encryption_key)
{
    if (seamless_loss) {
        ResetPathPair paths(realm_path);
        bool local_realm_exists = util::File::exists(paths.local_path);
        if (!local_realm_exists) {
            return paths.local_path; // nothing to reset
        }
        // if sync was interrupted during the download of the fresh copy, continue that session until it is finished
        // otherwise if the completely downloaded fresh copy is available, continue the reset on the original Realm.
        return fresh_copy_is_downloaded(paths.fresh_path, encryption_key) ? paths.local_path : paths.fresh_path;
    }
    return realm_path;
}

bool ClientResetOperation::is_downloading_fresh_copy()
{
    ResetPathPair paths(m_realm_path);
    return m_realm_path == paths.fresh_path;
}

bool ClientResetOperation::has_fresh_copy()
{
    ResetPathPair paths(m_realm_path);
    return fresh_copy_is_downloaded(paths.fresh_path, m_encryption_key);
}

bool ClientResetOperation::finalize(sync::SaltedFileIdent salted_file_ident)
{
    util::Optional<std::string> fresh_path;
    if (m_seamless_loss) {
        if (is_downloading_fresh_copy() || !has_fresh_copy()) {
            return false;
        }
        ResetPathPair paths(m_realm_path);
        REALM_ASSERT_EX(m_realm_path == paths.local_path, m_realm_path, paths.local_path, paths.fresh_path);
        fresh_path = paths.fresh_path;
    }

    m_salted_file_ident = salted_file_ident;
    // only do the reset if the file exists
    // if there is no existing file, there is nothing to reset
    bool local_realm_exists = util::File::exists(m_realm_path);
    if (local_realm_exists) {
        m_salted_file_ident = salted_file_ident;
        logger.debug("finalize_client_reset:discard, realm_path = %1, local_realm_exists = %2", m_realm_path,
                     local_realm_exists);

        client_reset::LocalVersionIDs local_version_ids;
        try {
            local_version_ids = client_reset::perform_client_reset_diff(m_realm_path, fresh_path, m_encryption_key,
                                                                        m_salted_file_ident, logger);
        }
        catch (util::File::AccessError& e) {
            logger.error("In finalize_client_reset, the client reset failed, "
                         "realm path = %1, msg = %2",
                         m_realm_path, e.what());
            return false;
        }

        m_client_reset_old_version = local_version_ids.old_version;
        m_client_reset_new_version = local_version_ids.new_version;

        // FIXME: clean up the fresh Realm
        return true;
    }
    return false;
}

} // namespace realm::_impl
