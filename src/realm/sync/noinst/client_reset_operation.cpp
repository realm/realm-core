/*************************************************************************
 *
 * Copyright 2021 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/transaction.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/noinst/client_reset_operation.hpp>
#include <realm/util/scope_exit.hpp>

namespace realm::_impl {

ClientResetOperation::ClientResetOperation(util::Logger& logger, DBRef db, DBRef db_fresh, ClientResyncMode mode,
                                           CallbackBeforeType notify_before, CallbackAfterType notify_after,
                                           bool recovery_is_allowed)
    : m_logger{logger}
    , m_db{db}
    , m_db_fresh(std::move(db_fresh))
    , m_mode(mode)
    , m_notify_before(std::move(notify_before))
    , m_notify_after(std::move(notify_after))
    , m_recovery_is_allowed(recovery_is_allowed)
{
    REALM_ASSERT(m_db);
    REALM_ASSERT_RELEASE(m_mode != ClientResyncMode::Manual);
    logger.debug("Create ClientResetOperation, realm_path = %1, mode = %2, recovery_allowed = %3", m_db->get_path(),
                 m_mode, m_recovery_is_allowed);
}

std::string ClientResetOperation::get_fresh_path_for(const std::string& path)
{
    const std::string fresh_suffix = ".fresh";
    const size_t suffix_len = fresh_suffix.size();
    REALM_ASSERT(path.length());
    REALM_ASSERT_DEBUG_EX(
        path.size() < suffix_len || path.substr(path.size() - suffix_len, suffix_len) != fresh_suffix, path);
    return path + fresh_suffix;
}

bool ClientResetOperation::finalize(sync::SaltedFileIdent salted_file_ident, sync::SubscriptionStore* sub_store,
                                    util::UniqueFunction<void(int64_t)> on_flx_version_complete)
{
    m_salted_file_ident = salted_file_ident;
    // only do the reset if there is data to reset
    // if there is nothing in this Realm, then there is nothing to reset and
    // sync should be able to continue as normal
    bool local_realm_exists = m_db->get_version_of_latest_snapshot() != 0;
    if (local_realm_exists) {
        REALM_ASSERT_EX(m_db_fresh, m_db->get_path(), m_mode);
        m_logger.debug("ClientResetOperation::finalize, realm_path = %1, local_realm_exists = %2, mode = %3",
                       m_db->get_path(), local_realm_exists, m_mode);

        client_reset::LocalVersionIDs local_version_ids;
        auto always_try_clean_up = util::make_scope_exit([&]() noexcept {
            clean_up_state();
        });

        std::string local_path = m_db->get_path();
        if (m_notify_before) {
            m_notify_before(local_path);
        }

        // If m_notify_after is set, pin the previous state to keep it around.
        TransactionRef previous_state;
        if (m_notify_after) {
            previous_state = m_db->start_frozen();
        }
        bool did_recover_out = false;
        local_version_ids = client_reset::perform_client_reset_diff(
            m_db, m_db_fresh, m_salted_file_ident, m_logger, m_mode, m_recovery_is_allowed, &did_recover_out,
            sub_store, std::move(on_flx_version_complete)); // throws

        if (m_notify_after) {
            m_notify_after(local_path, previous_state->get_version_of_current_transaction(), did_recover_out);
        }

        m_client_reset_old_version = local_version_ids.old_version;
        m_client_reset_new_version = local_version_ids.new_version;

        return true;
    }
    return false;
}

void ClientResetOperation::clean_up_state() noexcept
{
    if (m_db_fresh) {
        std::string path_to_clean = m_db_fresh->get_path();
        try {
            // In order to obtain the lock and delete the realm, we first have to close
            // the Realm. This requires that we are the only remaining ref holder, and
            // this is expected. Releasing the last ref should release the hold on the
            // lock file and allow us to clean up.
            long use_count = m_db_fresh.use_count();
            REALM_ASSERT_DEBUG_EX(use_count == 1, use_count, path_to_clean);
            m_db_fresh.reset();
            // clean up the fresh Realm
            // we don't mind leaving the fresh lock file around because trying to delete it
            // here could cause a race if there are multiple resets ongoing
            bool did_lock = DB::call_with_lock(path_to_clean, [&](const std::string& path) {
                constexpr bool delete_lockfile = false;
                DB::delete_files(path, nullptr, delete_lockfile);
            });
            if (!did_lock) {
                m_logger.warn("In ClientResetOperation::finalize, the fresh copy '%1' could not be cleaned up. "
                              "There were %2 refs remaining.",
                              path_to_clean, use_count);
            }
        }
        catch (const std::exception& err) {
            m_logger.warn("In ClientResetOperation::finalize, the fresh copy '%1' could not be cleaned up due to "
                          "an exception: '%2'",
                          path_to_clean, err.what());
            // ignored, this is just a best effort
        }
    }
}

} // namespace realm::_impl
