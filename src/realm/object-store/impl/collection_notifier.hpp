////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REALM_BACKGROUND_COLLECTION_HPP
#define REALM_BACKGROUND_COLLECTION_HPP

#include <realm/object-store/object_changeset.hpp>
#include <realm/object-store/impl/collection_change_builder.hpp>
#include <realm/object-store/util/checked_mutex.hpp>

#include <realm/util/assert.hpp>
#include <realm/version_id.hpp>
#include <realm/keys.hpp>
#include <realm/table_ref.hpp>

#include <array>
#include <atomic>
#include <exception>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace realm {
class Realm;
class Transaction;

namespace _impl {
class RealmCoordinator;

struct ListChangeInfo {
    TableKey table_key;
    int64_t row_key;
    int64_t col_key;
    CollectionChangeBuilder* changes;
};

// FIXME: this should be in core
using TableKeyType = decltype(TableKey::value);
using ObjKeyType = decltype(ObjKey::value);

using KeyPathArray = std::vector<std::vector<std::pair<TableKey, ColKey>>>;

struct TransactionChangeInfo {
    std::vector<ListChangeInfo> lists;
    std::unordered_map<TableKeyType, ObjectChangeSet> tables;
    bool track_all;
    bool schema_changed;
};

struct Callback {
    // The actual callback to invoke
    CollectionChangeCallback fn;
    // The pending changes accumulated on the worker thread. This field is
    // guarded by m_callback_mutex and is written to on the worker thread,
    // then read from on the target thread.
    CollectionChangeBuilder accumulated_changes;
    // The changeset which will actually be passed to `fn`. This field is
    // not guarded by a lock and can only be accessed on the notifier's
    // target thread.
    CollectionChangeBuilder changes_to_deliver;
    // The filter that this `Callback` is restricted to. Elements not part
    // of the `key_path_array` should not invoke a notification.
    KeyPathArray key_path_array;
    // A unique-per-notifier identifier used to unregister the callback.
    uint64_t token;
    // We normally want to skip calling the callback if there's no changes,
    // but only if we've sent the initial notification (to support the
    // async query use-case). Not guarded by a mutex and is only readable
    // on the target thread.
    bool initial_delivered;
    // Set within a write transaction on the target thread if this callback
    // should not be called with changes for that write. requires m_callback_mutex.
    bool skip_next;
};

/**
 * The `DeepChangeChecker` serves two purposes:
 * - Given an initial `Table` and an optional `KeyPathArray` it find all tables related to that initial table.
 *   A `RelatedTable` is a `Table` that can be reached via a link from another `Table`.
 * - The `DeepChangeChecker` also offers a way to check if a specific `ObjKey` was changed.
 */
class DeepChangeChecker {
public:
    struct OutgoingLink {
        int64_t col_key;
        bool is_list;
    };

    /**
     * `RelatedTable` is used to describe a the connections of a `Table` to other tables.
     * Tables count as related if they can be reached via a link.
     */
    struct RelatedTable {
        // The key of the table for which this struct holds all outgoing links.
        TableKey table_key;
        // All outgoing links to the table specified by `table_key`.
        std::vector<OutgoingLink> links;
    };

    DeepChangeChecker(TransactionChangeInfo const& info, Table const& root_table,
                      std::vector<RelatedTable> const& related_tables, std::vector<KeyPathArray> key_path_arrays);

    /**
     * Check if the object identified by `obj_key` was changed.
     *
     * @param obj_key The `ObjKey::value` for the object that is supposed to be checked.
     *
     * @return True if the object was changed, false otherwise.
     */
    bool operator()(int64_t obj_key);

    static void find_all_related_tables(std::vector<RelatedTable>& out, Table const& table,
                                        std::vector<TableKey> tables_in_filters);

    /**
     * Search for related tables within the specified `table`.
     * Related tables are all tables that can be reached via links from the `table`.
     * A table is always related to itself.
     *
     * Example schema:
     * {
     *   {"root_table",
     *       {
     *           {"link", PropertyType::Object | PropertyType::Nullable, "linked_table"},
     *       }
     *   },
     *   {"linked_table",
     *       {
     *           {"value", PropertyType::Int}
     *       }
     *   },
     * }
     *
     * Asking for related tables for `root_table` based on this schema will result in a `std::vector<RelatedTable>`
     * with two entries, one for `root_table` and one for `linked_table`. The function would be called once for
     * each table involved until there are no further links.
     *
     * Likewise a search for related tables starting with `linked_table` would only return this table.
     *
     * Filter:
     * Using a `key_path_array` that only consists of the table key for `"root_table"` would result
     * in `out` just having this one entry.
     *
     * @param out Return value containing all tables that can be reached from the given `table` including
     *            some additional information about those tables (see `OutgoingLink` in `RelatedTable`).
     * @param table The table that the related tables will be searched for.
     * @param key_path_arrays A collection of all `KeyPathArray`s passed to the `Callback`s for this
     * `CollectionNotifier`.
     * @param all_callback_have_filters The beheviour when filtering tables depends on all of them having a filter or
     * just some. In the latter case the related tables will be a combination of all tables for the non-filtered way
     * plus the explicitely filtered tables.
     */
    static void find_filtered_related_tables(std::vector<RelatedTable>& out, Table const& table,
                                             std::vector<KeyPathArray> key_path_arrays,
                                             bool all_callback_have_filters);

private:
    TransactionChangeInfo const& m_info;
    Table const& m_root_table;
    // The `ObjectChangeSet` for `root_table` if it is contained in `m_info`.
    ObjectChangeSet const* const m_root_object_changes;
    std::unordered_map<TableKeyType, std::unordered_set<ObjKeyType>> m_not_modified;
    std::vector<RelatedTable> const& m_related_tables;
    // The `m_key_path_array` contains all columns filtered for. We need this when checking for
    // changes in `operator()` to make sure only columns actually filtered for send notifications.
    std::vector<KeyPathArray> m_key_path_arrays;
    struct Path {
        int64_t obj_key;
        int64_t col_key;
        bool depth_exceeded;
    };
    std::array<Path, 4> m_current_path;

    /**
     * Checks if a specific object, identified by it's `ObjKeyType` in a given `Table` was changed.
     *
     * @param table The `Table` that contains the `ObjKeyType` that will be checked.
     * @param obj_key The `ObjKeyType` identifying the object to be checked for changes.
     * @param depth Determines how deep the search will be continued if the change could not be found
     *              on the first level.
     * @param filtered_columns TBD
     *
     * @return True if the object was changed, false otherwise.
     */
    bool check_row(Table const& table, ObjKeyType obj_key, std::vector<ColKey> filtered_columns, size_t depth = 0);

    /**
     * Check the `table` within `m_related_tables` for changes in it's outgoing links.
     *
     * @param table_key The `TableKey` for the `table` in question.
     * @param table The table to check for changed links.
     * @param obj_key The key for the object to look for.
     * @param depth The maximum depth that should be considered for this search.
     *
     * @return True if the specified `table` does have linked objects that have been changed.
     *         False if the `table` is not contained in `m_related_tables` or the `table` does not have any
     *         outgoing links at all or the `table` does not have linked objects with changes.
     */
    bool check_outgoing_links(TableKey table_key, Table const& table, int64_t obj_key,
                              std::vector<ColKey> filtered_columns, size_t depth = 0);
};

// A base class for a notifier that keeps a collection up to date and/or
// generates detailed change notifications on a background thread. This manages
// most of the lifetime-management issues related to sharing an object between
// the worker thread and the collection on the target thread, along with the
// thread-safe callback collection.
class CollectionNotifier {
public:
    CollectionNotifier(std::shared_ptr<Realm>);
    virtual ~CollectionNotifier();

    // ------------------------------------------------------------------------
    // Public API for the collections using this to get notifications:

    // Stop receiving notifications from this background worker
    // This must be called in the destructor of the collection
    void unregister() noexcept;

    /**
     * Add a callback to be called each time the collection changes.
     * This can only be called from the target collection's thread.
     *
     * @param callback The `CollectionChangeCallback` that will be executed when a change happens.
     * @param key_path_array An array of all key paths that should be filtered for. If a changed
     *                       table/column combination is not part of the `key_path_array`, no
     *                       notification will be sent.
     *
     * @return A token which can be passed to `remove_callback()`.
     */
    uint64_t add_callback(CollectionChangeCallback callback, KeyPathArray key_path_array) REQUIRES(!m_callback_mutex);
    // Remove a previously added token. The token is no longer valid after
    // calling this function and must not be used again. This function can be
    // called from any thread.
    void remove_callback(uint64_t token) REQUIRES(!m_callback_mutex);

    void suppress_next_notification(uint64_t token) REQUIRES(!m_callback_mutex);

    // ------------------------------------------------------------------------
    // API for RealmCoordinator to manage running things and calling callbacks

    bool is_for_realm(Realm&) const noexcept;
    Realm* get_realm() const noexcept
    {
        return m_realm.get();
    }

    // Get the Transaction version which this collection can attach to (if it's
    // in handover mode), or can deliver to (if it's been handed over to the BG worker alredad)
    // precondition: RealmCoordinator::m_notifier_mutex is locked
    VersionID version() const noexcept
    {
        return m_sg_version;
    }

    // Release references to all core types
    // This is called on the worker thread to ensure that non-thread-safe things
    // can be destroyed on the correct thread, even if the last reference to the
    // CollectionNotifier is released on a different thread
    virtual void release_data() noexcept;

    // Prepare to deliver the new collection and call callbacks.
    // Returns whether or not it has anything to deliver.
    // precondition: RealmCoordinator::m_notifier_mutex is locked
    bool package_for_delivery() REQUIRES(!m_callback_mutex);

    // Pass the given error to all registered callbacks, then remove them
    // precondition: RealmCoordinator::m_notifier_mutex is unlocked
    void deliver_error(std::exception_ptr) REQUIRES(!m_callback_mutex);

    // Call each of the given callbacks with the changesets prepared by package_for_delivery()
    // precondition: RealmCoordinator::m_notifier_mutex is unlocked
    void before_advance() REQUIRES(!m_callback_mutex);
    void after_advance() REQUIRES(!m_callback_mutex);

    bool is_alive() const noexcept;

    // precondition: RealmCoordinator::m_notifier_mutex is locked *or* is called on worker thread
    bool has_run() const noexcept
    {
        return m_has_run;
    }

    // Attach the handed-over query to `sg`. Must not be already attached to a Transaction.
    // precondition: RealmCoordinator::m_notifier_mutex is locked
    void attach_to(std::shared_ptr<Transaction> sg);

    // Set `info` as the new ChangeInfo that will be populated by the next
    // transaction advance, and register all required information in it
    // precondition: RealmCoordinator::m_notifier_mutex is locked
    void add_required_change_info(TransactionChangeInfo& info);

    // precondition: RealmCoordinator::m_notifier_mutex is unlocked
    virtual void run() = 0;

    // precondition: RealmCoordinator::m_notifier_mutex is locked
    void prepare_handover() REQUIRES(!m_callback_mutex);

    template <typename T>
    class Handle;

    bool have_callbacks() const noexcept
    {
        return m_have_callbacks;
    }

protected:
    void add_changes(CollectionChangeBuilder change) REQUIRES(!m_callback_mutex);
    void set_table(ConstTableRef table);
    std::unique_lock<std::mutex> lock_target();
    Transaction& source_shared_group();
    // signal that the underlying source object of the collection has been deleted
    // but only report this to the notifiers the first time this is reported
    void report_collection_root_is_deleted();

    bool any_related_table_was_modified(TransactionChangeInfo const&) const noexcept;
    std::function<bool(ObjectChangeSet::ObjectKeyType)> get_modification_checker(TransactionChangeInfo const&,
                                                                                 ConstTableRef);
    std::vector<KeyPathArray> get_key_path_arrays();
    std::vector<ColKey> get_filtered_col_keys(bool root_table_only);
    bool all_callbacks_have_filters();
    // The actual change, calculated in run() and delivered in prepare_handover()
    CollectionChangeBuilder m_change;

    std::vector<DeepChangeChecker::RelatedTable> m_related_tables;

    // Due to the keypath filtered notifications we need to update the related tables every time the callbacks do see
    // a change since the list of related tables is filtered by the key paths used for the notifcations.
    bool m_did_modify_callbacks = true;

private:
    virtual void do_attach_to(Transaction&) {}
    virtual void do_prepare_handover(Transaction&) {}
    virtual bool do_add_required_change_info(TransactionChangeInfo&) = 0;
    virtual bool prepare_to_deliver()
    {
        return true;
    }

    mutable std::mutex m_realm_mutex;
    std::shared_ptr<Realm> m_realm;

    VersionID m_sg_version;
    std::shared_ptr<Transaction> m_sg;

    bool m_has_run = false;
    bool m_error = false;
    bool m_has_delivered_root_deletion_event = false;

    // Currently registered callbacks and a mutex which must always be held
    // while doing anything with them or m_callback_index
    util::CheckedMutex m_callback_mutex;

    // All `Callback`s added to this `ColellectionNotifier` via `add_callback()`.
    std::vector<Callback> m_callbacks;

    // Cached value for if m_callbacks is empty, needed to avoid deadlocks in
    // run() due to lock-order inversion between m_callback_mutex and m_target_mutex
    // It's okay if this value is stale as at worst it'll result in us doing
    // some extra work.
    std::atomic<bool> m_have_callbacks = {false};

    // Iteration variable for looping over callbacks. remove_callback() will
    // sometimes update this to ensure that removing a callback while iterating
    // over the callbacks will not skip an unrelated callback.
    size_t m_callback_index = -1;
    // The number of callbacks which were present when the notifier was packaged
    // for delivery which are still present.
    // Updated by packaged_for_delivery and remove_callback(), and used in
    // for_each_callback() to avoid calling callbacks registered during delivery.
    size_t m_callback_count = -1;

    uint64_t m_next_token = 0;

    // Iterate over m_callbacks and call the given function on each one. This
    // does fancy locking things to allow fn to drop the lock before invoking
    // the callback (which must be done to avoid deadlocks).
    template <typename Fn>
    void for_each_callback(Fn&& fn) REQUIRES(!m_callback_mutex);

    std::vector<Callback>::iterator find_callback(uint64_t token);
};

// A smart pointer to a CollectionNotifier that unregisters the notifier when
// the pointer is destroyed. Movable. Copying will produce a null Handle.
template <typename T>
class CollectionNotifier::Handle : public std::shared_ptr<T> {
public:
    using std::shared_ptr<T>::shared_ptr;

    Handle() = default;
    ~Handle()
    {
        reset();
    }

    // Copying a Handle produces a null Handle.
    Handle(const Handle&)
        : Handle()
    {
    }
    Handle& operator=(const Handle& other)
    {
        if (this != &other) {
            reset();
        }
        return *this;
    }

    Handle(Handle&&) = default;
    Handle& operator=(Handle&& other)
    {
        reset();
        std::shared_ptr<T>::operator=(std::move(other));
        return *this;
    }

    template <typename U>
    Handle& operator=(std::shared_ptr<U>&& other)
    {
        reset();
        std::shared_ptr<T>::operator=(std::move(other));
        return *this;
    }

    void reset()
    {
        if (*this) {
            this->get()->unregister();
            std::shared_ptr<T>::reset();
        }
    }
};

// A package of CollectionNotifiers for a single Realm instance which is passed
// around to the various places which need to actually trigger the notifications
class NotifierPackage {
public:
    NotifierPackage() = default;
    NotifierPackage(std::exception_ptr error, std::vector<std::shared_ptr<CollectionNotifier>> notifiers,
                    RealmCoordinator* coordinator);

    explicit operator bool() const noexcept
    {
        return !m_notifiers.empty();
    }

    // Get the version which this package can deliver into, or VersionID{} if
    // it has not yet been packaged
    util::Optional<VersionID> version() const noexcept
    {
        return m_version;
    }

    // If a version is given, block until notifications are ready for that
    // version, and then regardless of whether or not a version was given filter
    // the notifiers to just the ones which have anything to deliver.
    // No-op if called multiple times
    void package_and_wait(util::Optional<VersionID::version_type> target_version);

    // Send the before-change notifications
    void before_advance();
    // Deliver the payload associated with the contained notifiers and/or the error
    void deliver(Transaction& sg);
    // Send the after-change notifications
    void after_advance();

private:
    util::Optional<VersionID> m_version;
    std::vector<std::shared_ptr<CollectionNotifier>> m_notifiers;

    RealmCoordinator* m_coordinator = nullptr;
    std::exception_ptr m_error;
};

} // namespace _impl
} // namespace realm

#endif /* REALM_BACKGROUND_COLLECTION_HPP */
