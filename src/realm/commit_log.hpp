/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2014] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/
#ifndef REALM_COMMIT_LOG_HPP
#define REALM_COMMIT_LOG_HPP

#ifdef REALM_ENABLE_REPLICATION

#include <exception>

#include <realm/binary_data.hpp>
#include <realm/replication.hpp>
#include <realm/lang_bind_helper.hpp>

namespace realm {

class ClientHistory: public Replication, public History {
public:
    using version_type = History::version_type;

    /// Implements History::get_changesets().
    ///
    /// The memory referenced by a particular returned changeset will remain
    /// accessible to the caller at least until the corresponding version is
    /// declared stale by a call to set_last_version_seen_locally() *on any
    /// commitlog instance participating in the session*, OR until a new call to
    /// get_changesets(), or commit_write_transact() is made *on the same
    /// commitlog instance*.
    virtual void get_changesets(version_type begin_version, version_type end_version,
                                BinaryData* buffer) const REALM_NOEXCEPT override = 0;

    virtual ~ClientHistory() REALM_NOEXCEPT {}
};


// FIXME: Why is this exception class exposed?
class LogFileError: public std::runtime_error {
public:
    LogFileError(std::string file_name):
        std::runtime_error(file_name)
    {
    }
};

// Create a writelog collector and associate it with a filepath. You'll need one
// writelog collector for each shared group. Commits from writelog collectors
// for a specific filepath may later be obtained through other writelog
// collectors associated with said filepath.  The caller assumes ownership of
// the writelog collector and must destroy it, but only AFTER destruction of the
// shared group using it.
std::unique_ptr<ClientHistory> make_client_history(const std::string& path,
                                                   const char* encryption_key = 0);

} // namespace realm


#endif // REALM_ENABLE_REPLICATION
#endif // REALM_COMMIT_LOG_HPP
