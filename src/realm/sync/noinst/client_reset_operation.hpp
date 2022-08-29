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

#ifndef REALM_NOINST_CLIENT_RESET_OPERATION_HPP
#define REALM_NOINST_CLIENT_RESET_OPERATION_HPP

#include <realm/db.hpp>
#include <realm/util/functional.hpp>
#include <realm/util/logger.hpp>
#include <realm/sync/protocol.hpp>

namespace realm::sync {
class SubscriptionStore;
}

namespace realm::_impl {

// A ClientResetOperation object is used per client session to keep track of
// state Realm download.
class ClientResetOperation {
public:
    using CallbackBeforeType = util::UniqueFunction<void(std::string)>;
    using CallbackAfterType = util::UniqueFunction<void(std::string, VersionID, bool)>;

    ClientResetOperation(util::Logger& logger, DBRef db, DBRef db_fresh, ClientResyncMode mode,
                         CallbackBeforeType notify_before, CallbackAfterType notify_after, bool recovery_is_allowed);

    // When the client has received the salted file ident from the server, it
    // should deliver the ident to the ClientResetOperation object. The ident
    // will be inserted in the Realm after download.
    bool finalize(sync::SaltedFileIdent salted_file_ident, sync::SubscriptionStore*,
                  util::UniqueFunction<void(int64_t)>); // throws

    static std::string get_fresh_path_for(const std::string& realm_path);

    realm::VersionID get_client_reset_old_version() const noexcept;
    realm::VersionID get_client_reset_new_version() const noexcept;

private:
    void clean_up_state() noexcept;

    util::Logger& m_logger;
    DBRef m_db;
    DBRef m_db_fresh;
    ClientResyncMode m_mode;
    sync::SaltedFileIdent m_salted_file_ident = {0, 0};
    realm::VersionID m_client_reset_old_version;
    realm::VersionID m_client_reset_new_version;
    CallbackBeforeType m_notify_before;
    CallbackAfterType m_notify_after;
    bool m_recovery_is_allowed;
};

// Implementation

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
