/*************************************************************************
 *
 * Copyright 2021 Realm, Inc.
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

#pragma once

#include "realm/db.hpp"
#include "realm/list.hpp"
#include "realm/obj.hpp"
#include "realm/query.hpp"
#include "realm/timestamp.hpp"
#include "realm/util/future.hpp"
#include "realm/util/functional.hpp"
#include "realm/util/optional.hpp"

#include <list>
#include <string_view>

namespace realm::sync {

class MutableSubscriptionSet;
class SubscriptionSet;
class SubscriptionStore;

// A Subscription represents a single query that may be OR'd with other queries on the same object class to be
// send to the server in a QUERY or IDENT message.
class Subscription {
public:
    // Returns the unique ID for this subscription.
    ObjectId id() const;

    // Returns the timestamp of when this subscription was originally created.
    Timestamp created_at() const;

    // Returns the timestamp of the last time this subscription was updated by calling update_query.
    Timestamp updated_at() const;

    // Returns the name of the subscription that was set when it was created.
    std::string_view name() const;

    // Returns the name of the object class of the query for this subscription.
    std::string_view object_class_name() const;

    // Returns a stringified version of the query associated with this subscription.
    std::string_view query_string() const;

private:
    friend class SubscriptionSet;
    friend class MutableSubscriptionSet;

    Subscription(const SubscriptionStore* parent, Obj obj);
    Subscription(std::string name, std::string object_class_name, std::string query_str);

    ObjectId m_id;
    Timestamp m_created_at;
    Timestamp m_updated_at;
    std::string m_name;
    std::string m_object_class_name;
    std::string m_query_string;
};

// SubscriptionSets contain a set of unique queries by either name or Query object that will be constructed into a
// single QUERY or IDENT message to be sent to the server.
class SubscriptionSet {
public:
    /*
     * State diagram:
     *
     *                    ┌───────────┬─────────►Error─────────┐
     *                    │           │                        │
     *                    │           │                        ▼
     *   Uncommitted──►Pending──►Bootstrapping──►Complete───►Superceded
     *                    │                                    ▲
     *                    │                                    │
     *                    └────────────────────────────────────┘
     *
     */
    enum class State : int64_t {
        // This subscription set has not been persisted and has not been sent to the server. This state is only valid
        // for MutableSubscriptionSets
        Uncommitted = 0,
        // The subscription set has been persisted locally but has not been acknowledged by the server yet.
        Pending,
        // The server is currently sending the initial state that represents this subscription set to the client.
        Bootstrapping,
        // This subscription set is the active subscription set that is currently being synchronized with the server.
        Complete,
        // An error occurred while processing this subscription set on the server. Check error_str() for details.
        Error,
        // The server responded to a later subscription set to this one and this one has been trimmed from the
        // local storage of subscription sets.
        Superceded,
    };

    // Used in tests.
    inline friend std::ostream& operator<<(std::ostream& o, State state)
    {
        switch (state) {
            case State::Uncommitted:
                o << "Uncommitted";
                break;
            case State::Pending:
                o << "Pending";
                break;
            case State::Bootstrapping:
                o << "Bootstrapping";
                break;
            case State::Complete:
                o << "Complete";
                break;
            case State::Error:
                o << "Error";
                break;
            case State::Superceded:
                o << "Superceded";
                break;
        }
        return o;
    }

    using iterator = std::vector<Subscription>::iterator;
    using const_iterator = std::vector<Subscription>::const_iterator;

    // This will make a copy of this subscription set with the next available version number and return it as
    // a mutable SubscriptionSet to be updated. The new SubscriptionSet's state will be Uncommitted. This
    // subscription set will be unchanged.
    MutableSubscriptionSet make_mutable_copy() const;

    // Returns a future that will resolve either with an error status if this subscription set encounters an
    // error, or resolves when the subscription set reaches at least that state. It's possible for a subscription
    // set to skip a state (i.e. go from Pending to Complete or Pending to Superceded), and the future value
    // will the the state it actually reached.
    util::Future<State> get_state_change_notification(State notify_when) const;

    // The query version number used in the sync wire protocol to identify this subscription set to the server.
    int64_t version() const;

    // The current state of this subscription set
    State state() const;

    // The error string for this subscription set if any.
    StringData error_str() const;

    // Returns the number of subscriptions in the set.
    size_t size() const;

    // A const_iterator interface for finding/working with individual subscriptions.
    const_iterator begin() const;
    const_iterator end() const;

    Subscription at(size_t index) const;

    // Returns a const_iterator to the query matching either the name or Query object, or end() if no such
    // subscription exists.
    const_iterator find(StringData name) const;
    const_iterator find(const Query& query) const;

    // Returns this query set as extended JSON in a form suitable for transmitting to the server.
    std::string to_ext_json() const;

    // Reloads the state of this SubscriptionSet so that it reflects the latest state from synchronizing with the
    // server. This will invalidate all iterators.
    void refresh();

protected:
    friend class SubscriptionStore;
    struct SupercededTag {
    };

    explicit SubscriptionSet(const SubscriptionStore* mgr, int64_t version, SupercededTag);
    explicit SubscriptionSet(const SubscriptionStore* mgr, TransactionRef tr, Obj obj);

    void load_from_database(TransactionRef tr, Obj obj);

    const SubscriptionStore* m_mgr;

    DB::version_type m_cur_version = 0;
    int64_t m_version = 0;
    State m_state = State::Uncommitted;
    std::string m_error_str;
    DB::version_type m_snapshot_version;
    std::vector<Subscription> m_subs;
};

class MutableSubscriptionSet : public SubscriptionSet {
public:
    // Erases all subscriptions in the subscription set.
    void clear();

    iterator begin();
    iterator end();

    // Inserts a new subscription into the set if one does not exist already - returns an iterator to the
    // subscription and a bool that is true if a new subscription was actually created. The SubscriptionSet
    // must be in the Uncommitted state to call this - otherwise this will throw.
    //
    // The Query portion of the subscription is mutable, however the name portion is immutable after the
    // subscription is inserted.
    //
    // If insert is called twice for the same name, the Query portion and updated_at timestamp for that named
    // subscription will be updated to match the new Query.
    std::pair<iterator, bool> insert_or_assign(std::string_view name, const Query& query);

    // Inserts a new subscription into the set if one does not exist already - returns an iterator to the
    // subscription and a bool that is true if a new subscription was actually created. The SubscriptionSet
    // must be in the Uncommitted state to call this - otherwise this will throw.
    //
    // If insert is called twice for the same query, then the updated_at timestamp for that subscription will
    // be updated.
    //
    // The inserted subscription will have an empty name - to update this Subscription's query, the caller
    // will have
    std::pair<iterator, bool> insert_or_assign(const Query& query);

    // Erases a subscription pointed to by an iterator. Returns the "next" iterator in the set - to provide
    // STL compatibility. The SubscriptionSet must be in the Uncommitted state to call this - otherwise
    // this will throw.
    iterator erase(const_iterator it);

    // Updates the state of the transaction and optionally updates its error information.
    //
    // You may only set an error_str when the State is State::Error.
    //
    // If set to State::Complete, this will erase all subscription sets with a version less than this one's.
    //
    // This should be called internally within the sync client.
    void update_state(State state, util::Optional<std::string_view> error_str = util::none);

    // This commits any changes to the subscription set and returns an this subscription set as an immutable view
    // from after the commit.
    //
    // This must be called as an r-value, like this:
    //     auto sub_set = std::move(mut_sub_set).commit();
    SubscriptionSet commit() &&;

protected:
    friend class SubscriptionStore;

    MutableSubscriptionSet(const SubscriptionStore* mgr, TransactionRef tr, Obj obj);

    void insert_sub(const Subscription& sub);

private:
    // To refresh a MutableSubscriptionSet, you should call commit() and call refresh() on its return value.
    void refresh() = delete;

    std::pair<iterator, bool> insert_or_assign_impl(iterator it, std::string name, std::string object_class_name,
                                                    std::string query_str);

    void insert_sub_impl(ObjectId id, Timestamp created_at, Timestamp updated_at, StringData name,
                         StringData object_class_name, StringData query_str);

    void process_notifications();

    TransactionRef m_tr;
    Obj m_obj;
    State m_old_state;
};

// A SubscriptionStore manages the FLX metadata tables and the lifecycles of SubscriptionSets and Subscriptions.
class SubscriptionStore {
public:
    explicit SubscriptionStore(DBRef db, util::UniqueFunction<void(int64_t)> on_new_subscription_set);

    // Get the latest subscription created by calling update_latest(). Once bootstrapping is complete,
    // this and get_active() will return the same thing. If no SubscriptionSet has been set, then
    // this returns an empty SubscriptionSet that you can clone() in order to mutate.
    SubscriptionSet get_latest() const;

    // Gets the subscription set that has been acknowledged by the server as having finished bootstrapping.
    // If no subscriptions have reached the complete stage, this returns an empty subscription with version
    // zero.
    SubscriptionSet get_active() const;

    // Returns the version number of the current active and latest subscription sets. This function guarantees
    // that the versions will be read from the same underlying transaction and will thus be consistent.
    std::pair<int64_t, int64_t> get_active_and_latest_versions() const;

    // To be used internally by the sync client. This returns a mutable view of a subscription set by its
    // version ID. If there is no SubscriptionSet with that version ID, this throws KeyNotFound.
    MutableSubscriptionSet get_mutable_by_version(int64_t version_id);

    // To be used internally by the sync client. This returns a read-only view of a subscription set by its
    // version ID. If there is no SubscriptionSet with that version ID, this throws KeyNotFound.
    SubscriptionSet get_by_version(int64_t version_id) const;

    struct PendingSubscription {
        int64_t query_version;
        DB::version_type snapshot_version;
    };

    util::Optional<PendingSubscription> get_next_pending_version(int64_t last_query_version,
                                                                 DB::version_type after_client_version) const;

private:
    DBRef m_db;

protected:
    struct SubscriptionKeys {
        TableKey table;
        ColKey id;
        ColKey created_at;
        ColKey updated_at;
        ColKey name;
        ColKey object_class_name;
        ColKey query_str;
    };

    struct SubscriptionSetKeys {
        TableKey table;
        ColKey snapshot_version;
        ColKey state;
        ColKey error_str;
        ColKey subscriptions;
    };

    struct NotificationRequest {
        NotificationRequest(int64_t version, util::Promise<SubscriptionSet::State> promise,
                            SubscriptionSet::State notify_when)
            : version(version)
            , promise(std::move(promise))
            , notify_when(notify_when)
        {
        }

        int64_t version;
        util::Promise<SubscriptionSet::State> promise;
        SubscriptionSet::State notify_when;
    };

    void supercede_prior_to(TransactionRef tr, int64_t version_id) const;

    SubscriptionSet get_by_version_impl(int64_t flx_version, util::Optional<DB::VersionID> version) const;
    MutableSubscriptionSet make_mutable_copy(const SubscriptionSet& set) const;

    friend class MutableSubscriptionSet;
    friend class Subscription;
    friend class SubscriptionSet;

    util::UniqueFunction<void(int64_t)> m_on_new_subscription_set;
    std::unique_ptr<SubscriptionSetKeys> m_sub_set_keys;
    std::unique_ptr<SubscriptionKeys> m_sub_keys;

    mutable std::mutex m_pending_notifications_mutex;
    mutable std::condition_variable m_pending_notifications_cv;
    mutable int64_t m_outstanding_requests = 0;
    mutable int64_t m_min_outstanding_version = 0;
    mutable std::list<NotificationRequest> m_pending_notifications;
};

} // namespace realm::sync
