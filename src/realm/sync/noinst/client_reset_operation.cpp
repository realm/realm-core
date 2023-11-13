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

#include <realm/sync/noinst/client_reset_operation.hpp>

#include <realm/sync/history.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/transaction.hpp>
#include <realm/util/scope_exit.hpp>

namespace realm::_impl::client_reset {

namespace {

constexpr static std::string_view c_fresh_suffix(".fresh");

} // namespace

std::string get_fresh_path_for(const std::string& path)
{
    const size_t suffix_len = c_fresh_suffix.size();
    REALM_ASSERT(path.length());
    REALM_ASSERT_DEBUG_EX(
        path.size() < suffix_len || path.substr(path.size() - suffix_len, suffix_len) != c_fresh_suffix, path);
    return path + c_fresh_suffix.data();
}

bool is_fresh_path(const std::string& path)
{
    const size_t suffix_len = c_fresh_suffix.size();
    REALM_ASSERT(path.length());
    if (path.size() < suffix_len) {
        return false;
    }
    return path.substr(path.size() - suffix_len, suffix_len) == c_fresh_suffix;
}

bool perform_client_reset(util::Logger& logger, DB& db, DB& fresh_db, ClientResyncMode mode,
                          CallbackBeforeType notify_before, CallbackAfterType notify_after,
                          sync::SaltedFileIdent new_file_ident, sync::SubscriptionStore* sub_store,
                          util::FunctionRef<void(int64_t)> on_flx_version, bool recovery_is_allowed)
{
    REALM_ASSERT(mode != ClientResyncMode::Manual);
    logger.debug("Possibly beginning client reset operation: realm_path = %1, mode = %2, recovery_allowed = %3",
                 db.get_path(), mode, recovery_is_allowed);

    auto always_try_clean_up = util::make_scope_exit([&]() noexcept {
        std::string path_to_clean = fresh_db.get_path();
        try {
            fresh_db.close();
            constexpr bool delete_lockfile = true;
            DB::delete_files(path_to_clean, nullptr, delete_lockfile);
        }
        catch (const std::exception& err) {
            logger.warn("In ClientResetOperation::finalize, the fresh copy '%1' could not be cleaned up due to "
                        "an exception: '%2'",
                        path_to_clean, err.what());
            // ignored, this is just a best effort
        }
    });

    // only do the reset if there is data to reset
    // if there is nothing in this Realm, then there is nothing to reset and
    // sync should be able to continue as normal
    auto latest_version = db.get_version_id_of_latest_snapshot();
    bool local_realm_exists = latest_version.version > 1;
    if (!local_realm_exists) {
        logger.debug("Local Realm file has never been written to, so skipping client reset.");
        return false;
    }

    VersionID frozen_before_state_version = notify_before ? notify_before() : latest_version;

    // If m_notify_after is set, pin the previous state to keep it around.
    TransactionRef previous_state;
    if (notify_after) {
        previous_state = db.start_frozen(frozen_before_state_version);
    }
    bool did_recover_out = false;
    client_reset::perform_client_reset_diff(db, fresh_db, new_file_ident, logger, mode, recovery_is_allowed,
                                            &did_recover_out, sub_store,
                                            on_flx_version); // throws

    if (notify_after) {
        notify_after(previous_state->get_version_of_current_transaction(), did_recover_out);
    }

    return true;
}

} // namespace realm::_impl::client_reset
