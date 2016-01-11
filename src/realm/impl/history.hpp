/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] Realm Inc
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

#ifndef REALM_IMPL_HISTORY_HPP
#define REALM_IMPL_HISTORY_HPP

#include <stdint.h>

#include <realm/binary_data.hpp>
#include <realm/alloc.hpp>

namespace realm {
namespace _impl {


/// Read-only access to history of changesets as needed to enable continuous
/// transactions.
class ContinTransactHistory {
public:
    using version_type = uint_fast64_t;

    /// May be called during, or at the beginning of a transaction to gain
    /// access to the history of changesets preceeding the snapshot that is
    /// bound to that transaction.
    virtual void refresh_accessor_tree(ref_type hist_ref) = 0;

    /// Get all changesets between the specified versions. References to those
    /// changesets will be made availble in successive entries of `buffer`. The
    /// number of retreived changesets is exactly `end_version -
    /// begin_version`. If this number is greater than zero, the changeset made
    /// avaialable in `buffer[0]` is the one that brought the database from
    /// `begin_version` to `begin_version + 1`.
    ///
    /// It is an error to specify a version (for \a begin_version or \a
    /// end_version) that is outside the range [V,W] where V is the version that
    /// immediately precedes the first changeset available in the history as the
    /// history appears in the **latest** available snapshot, and W is the
    /// versionm that immediately succeeds the last changeset available in the
    /// history as the history appears in the snapshot bound to the **current**
    /// transaction. This restriction is necessary to allow for different kinds
    /// of implementations of the history (separate standalone history or
    /// history as part of versioned Realm state).
    ///
    /// The calee retains ownership of the memory referenced by those entries,
    /// i.e., the memory referenced by `buffer[i].changeset` is **not** handed
    /// over to the caller.
    ///
    /// This function may be called only during a transaction (prior to
    /// initiation of commit operation), and only after a successfull invocation
    /// of refresh_accessor_tree(). In that case, the caller may assume that the
    /// memory references stay valid for the remainder of the transaction (up
    /// until initiation of the commit operation).
    virtual void get_changesets(version_type begin_version, version_type end_version,
                                BinaryData* buffer) const noexcept = 0;

    /// Get the list of uncommited changes accumulated so far in the current
    /// write transaction.
    ///
    /// The callee retains ownership of the referenced memory. The ownership is
    /// not handed over the the caller.
    ///
    /// This function may be called only during a write transaction (prior to
    /// initiation of commit operation). In that case, the caller may assume that the
    /// returned memory reference stays valid for the remainder of the transaction (up
    /// until initiation of the commit operation).
    virtual BinaryData get_uncommitted_changes() noexcept = 0;

    virtual ~ContinTransactHistory() noexcept {}
};


} // namespace _impl
} // namespace realm

#endif // REALM_IMPL_HISTORY_HPP
