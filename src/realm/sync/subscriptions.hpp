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
#include "realm/util/optional.hpp"

#include <string_view>

namespace realm::sync {

class SubscriptionSet;
class SubscriptionStore;

// A Subscription represents a single query that may be OR'd with other queries on the same object class to be
// send to the server in a QUERY or IDENT message.
class Subscription {
public:
    // Returns the timestamp of when this subscription was originally created.
    Timestamp created_at() const;

    // Returns the timestamp of the last time this subscription was updated by calling update_query.
    Timestamp updated_at() const;

    // Returns the name of the subscription that was set when it was created. If no name was set, this
    // returns a default-constructed std::string_view.
    StringData name() const;

    // Returns the name of the object class of the query for this subscription.
    StringData object_class_name() const;

    // Returns a stringified version of the query associated with this subscription.
    StringData query_string() const;

    // Replaces the query/object class of this query with another one.
    //
    // If another Subscription exists in the SubscriptionSet containing this subscription with the same
    // query, then this will throw a std::runtime_error.
    void update_query(const Query& query_obj);

protected:
    friend class SubscriptionSet;

    Subscription() = default;
    Subscription(const SubscriptionSet* parent, Obj obj);

private:
    const SubscriptionStore* store() const;

    const SubscriptionSet* m_parent = nullptr;
    Obj m_obj;
};

// SubscriptionSets contain a set of unique queries by either name or Query object that will be constructed into a
// single QUERY or IDENT message to be sent to the server.
class SubscriptionSet {
public:
    enum class State : int64_t {
        Uncommitted = 0,
        Pending,
        Bootstrapping,
        Complete,
        Error,
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
        }
        return o;
    }

    class iterator {
    public:
        using difference_type = size_t;
        using value_type = Subscription;
        using pointer = value_type*;
        using reference = value_type&;
        using iterator_category = std::input_iterator_tag;

        bool operator!=(const iterator& other) const
        {
            return (m_parent != other.m_parent) || (m_sub_it != other.m_sub_it);
        }

        bool operator==(const iterator& other) const
        {
            return (m_parent == other.m_parent && m_sub_it == other.m_sub_it);
        }

        reference operator*() const noexcept
        {
            return m_cur_sub;
        }

        pointer operator->() const noexcept
        {
            return &m_cur_sub;
        }

        // used in tests.
        inline friend std::ostream& operator<<(std::ostream& o, const iterator& it)
        {
            o << "SubscriptionSet::iterator(" << std::hex << it.m_parent << ", " << std::dec << it.m_sub_it.index()
              << ")";
            return o;
        }

        iterator& operator++();
        iterator operator++(int);

    protected:
        friend class SubscriptionSet;

        iterator(const SubscriptionSet* parent, LnkLst::iterator it);

    private:
        const SubscriptionSet* m_parent;
        LnkLst::iterator m_sub_it;
        mutable Subscription m_cur_sub;
    };

    using const_iterator = const iterator;

    // This will make a copy of this subscription set with the next available version number and return it as
    // a mutable SubscriptionSet to be updated. The new SubscriptionSet's state will be Uncommitted. This
    // subscription set will be unchanged.
    SubscriptionSet make_mutable_copy() const;

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

    // Returns a const_iterator to the query matching either the name or Query object, or end() if no such
    // subscription exists.
    const_iterator find(StringData name) const;
    const_iterator find(const Query& query) const;

    // A mutable iterator interface for working with individual subscriptions in when the set is in the
    // Uncommitted state.
    iterator begin();
    iterator end();

    // Erases a subscription pointed to by an iterator. Returns the "next" iterator in the set - to provide
    // STL compatibility. The SubscriptionSet must be in the Uncommitted state to call this - otherwise
    // this will throw.
    iterator erase(iterator it);

    // Erases all subscriptions in the subscription set.
    void clear();

    // Inserts a new subscription into the set if one does not exist already - returns an iterator to the
    // subscription and a bool that is true if a new subscription was actually created. The SubscriptionSet
    // must be in the Uncommitted state to call this - otherwise this will throw.
    //
    // The Query portion of the subscription is mutable, however the name portion is immutable after the
    // subscription is inserted.
    //
    // If insert is called twice for the same query, this is a no-op.
    std::pair<iterator, bool> insert(const Query& query, util::Optional<std::string> name = util::none);

    // Updates the state of the transaction and optionally updates its error information.
    //
    // You may only set an error_str when the State is State::Error.
    //
    // If set to State::Complete, this will erase all subscription sets with a version less than this one's.
    //
    // This should be called internally within the sync client.
    void update_state(State state, util::Optional<std::string> error_str = util::none);

    // If this is a mutable subscription set that has not had its changes committed, this commits them and
    // continues the set's lifetime in a read-only transaction. Otherwise, this will throw.
    void commit();

protected:
    friend class SubscriptionStore;
    friend class Subscription;

    void insert_sub_impl(Timestamp created_at, Timestamp updated_at, StringData name, StringData object_class_name,
                         StringData query_str);

    explicit SubscriptionSet(const SubscriptionStore* mgr, TransactionRef tr, Obj obj);

    Subscription subscription_from_iterator(LnkLst::iterator it) const;

    const SubscriptionStore* m_mgr;
    TransactionRef m_tr;
    Obj m_obj;
    LnkLst m_sub_list;
};

// A SubscriptionStore manages the FLX metadata tables and the lifecycles of SubscriptionSets and Subscriptions.
class SubscriptionStore {
public:
    explicit SubscriptionStore(DBRef db);

    // Get the latest subscription created by calling update_latest(). Once bootstrapping is complete,
    // this and get_active() will return the same thing. If no SubscriptionSet has been set, then
    // this returns an empty SubscriptionSet that you can clone() in order to mutate.
    const SubscriptionSet get_latest() const;

    // Gets the subscription set that has been acknowledged by the server as having finished bootstrapping.
    const SubscriptionSet get_active() const;

    // To be used internally by the sync client. This returns a mutable view of a subscription set by its
    // version ID. If there is no SubscriptionSet with that version ID, this throws std::out_of_range.
    SubscriptionSet get_mutable_by_version(int64_t version_id);

private:
    DBRef m_db;

protected:
    struct SubscriptionKeys {
        TableKey table;
        ColKey created_at;
        ColKey updated_at;
        ColKey name;
        ColKey object_class_name;
        ColKey query_str;
    };

    struct SubscriptionSetKeys {
        TableKey table;
        ColKey state;
        ColKey error_str;
        ColKey subscriptions;
    };

    void supercede_prior_to(TransactionRef tr, int64_t version_id) const;

    friend class Subscription;
    friend class SubscriptionSet;

    std::unique_ptr<SubscriptionSetKeys> m_sub_set_keys;
    std::unique_ptr<SubscriptionKeys> m_sub_keys;
};

} // namespace realm::sync
