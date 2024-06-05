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

#include "realm/sync/subscriptions.hpp"

#include "external/json/json.hpp"

#include "realm/data_type.hpp"
#include "realm/keys.hpp"
#include "realm/list.hpp"
#include "realm/sort_descriptor.hpp"
#include "realm/sync/noinst/sync_metadata_schema.hpp"
#include "realm/table.hpp"
#include "realm/table_view.hpp"
#include "realm/transaction.hpp"
#include "realm/util/flat_map.hpp"

#include <algorithm>
#include <initializer_list>
#include <stdexcept>

namespace realm::sync {
namespace {
// Schema version history:
//   v2: Initial public beta.

constexpr static int c_flx_schema_version = 2;
constexpr static std::string_view c_flx_subscription_sets_table("flx_subscription_sets");
constexpr static std::string_view c_flx_subscriptions_table("flx_subscriptions");

constexpr static std::string_view c_flx_sub_sets_state_field("state");
constexpr static std::string_view c_flx_sub_sets_version_field("version");
constexpr static std::string_view c_flx_sub_sets_error_str_field("error");
constexpr static std::string_view c_flx_sub_sets_subscriptions_field("subscriptions");
constexpr static std::string_view c_flx_sub_sets_snapshot_version_field("snapshot_version");

constexpr static std::string_view c_flx_sub_id_field("id");
constexpr static std::string_view c_flx_sub_created_at_field("created_at");
constexpr static std::string_view c_flx_sub_updated_at_field("updated_at");
constexpr static std::string_view c_flx_sub_name_field("name");
constexpr static std::string_view c_flx_sub_object_class_field("object_class");
constexpr static std::string_view c_flx_sub_query_str_field("query");

using OptionalString = util::Optional<std::string>;

enum class SubscriptionStateForStorage : int64_t {
    // The subscription set has been persisted locally but has not been acknowledged by the server yet.
    Pending = 1,
    // The server is currently sending the initial state that represents this subscription set to the client.
    Bootstrapping = 2,
    // This subscription set is the active subscription set that is currently being synchronized with the server.
    Complete = 3,
    // An error occurred while processing this subscription set on the server. Check error_str() for details.
    Error = 4,
    // The last bootstrap message containing the initial state for this subscription set has been received. The
    // client is awaiting a mark message to mark this subscription as fully caught up to history.
    AwaitingMark = 6,
};

SubscriptionSet::State state_from_storage(int64_t value)
{
    switch (static_cast<SubscriptionStateForStorage>(value)) {
        case SubscriptionStateForStorage::Pending:
            return SubscriptionSet::State::Pending;
        case SubscriptionStateForStorage::Bootstrapping:
            return SubscriptionSet::State::Bootstrapping;
        case SubscriptionStateForStorage::AwaitingMark:
            return SubscriptionSet::State::AwaitingMark;
        case SubscriptionStateForStorage::Complete:
            return SubscriptionSet::State::Complete;
        case SubscriptionStateForStorage::Error:
            return SubscriptionSet::State::Error;
        default:
            throw RuntimeError(ErrorCodes::InvalidArgument,
                               util::format("Invalid state for SubscriptionSet stored on disk: %1", value));
    }
}

int64_t state_to_storage(SubscriptionSet::State state)
{
    switch (state) {
        case SubscriptionSet::State::Pending:
            return static_cast<int64_t>(SubscriptionStateForStorage::Pending);
        case SubscriptionSet::State::Bootstrapping:
            return static_cast<int64_t>(SubscriptionStateForStorage::Bootstrapping);
        case SubscriptionSet::State::AwaitingMark:
            return static_cast<int64_t>(SubscriptionStateForStorage::AwaitingMark);
        case SubscriptionSet::State::Complete:
            return static_cast<int64_t>(SubscriptionStateForStorage::Complete);
        case SubscriptionSet::State::Error:
            return static_cast<int64_t>(SubscriptionStateForStorage::Error);
        default:
            REALM_UNREACHABLE();
    }
}

size_t state_to_order(SubscriptionSet::State needle)
{
    using State = SubscriptionSet::State;
    switch (needle) {
        case State::Uncommitted:
            return 0;
        case State::Pending:
            return 1;
        case State::Bootstrapping:
            return 2;
        case State::AwaitingMark:
            return 3;
        case State::Complete:
            return 4;
        case State::Error:
            return 5;
        case State::Superseded:
            return 6;
    }
    REALM_UNREACHABLE();
}

template <typename T, typename Predicate>
void splice_if(std::list<T>& src, std::list<T>& dst, Predicate pred)
{
    for (auto it = src.begin(); it != src.end();) {
        if (pred(*it)) {
            dst.splice(dst.end(), src, it++);
        }
        else {
            ++it;
        }
    }
}

} // namespace

Subscription::Subscription(const SubscriptionStore* parent, Obj obj)
    : id(obj.get<ObjectId>(parent->m_sub_id))
    , created_at(obj.get<Timestamp>(parent->m_sub_created_at))
    , updated_at(obj.get<Timestamp>(parent->m_sub_updated_at))
    , name(obj.is_null(parent->m_sub_name) ? OptionalString(util::none)
                                           : OptionalString{obj.get<String>(parent->m_sub_name)})
    , object_class_name(obj.get<String>(parent->m_sub_object_class_name))
    , query_string(obj.get<String>(parent->m_sub_query_str))
{
}

Subscription::Subscription(util::Optional<std::string> name, std::string object_class_name, std::string query_str)
    : id(ObjectId::gen())
    , created_at(std::chrono::system_clock::now())
    , updated_at(created_at)
    , name(std::move(name))
    , object_class_name(std::move(object_class_name))
    , query_string(std::move(query_str))
{
}


SubscriptionSet::SubscriptionSet(std::weak_ptr<SubscriptionStore> mgr, const Transaction& tr, const Obj& obj,
                                 MakingMutableCopy making_mutable_copy)
    : m_mgr(mgr)
    , m_cur_version(tr.get_version())
    , m_version(obj.get_primary_key().get_int())
    , m_obj_key(obj.get_key())
{
    REALM_ASSERT(obj.is_valid());
    if (!making_mutable_copy) {
        load_from_database(obj);
    }
}

SubscriptionSet::SubscriptionSet(std::weak_ptr<SubscriptionStore> mgr, int64_t version, SupersededTag)
    : m_mgr(mgr)
    , m_version(version)
    , m_state(State::Superseded)
{
}

void SubscriptionSet::load_from_database(const Obj& obj)
{
    auto mgr = get_flx_subscription_store(); // Throws

    m_state = state_from_storage(obj.get<int64_t>(mgr->m_sub_set_state));
    m_error_str = obj.get<String>(mgr->m_sub_set_error_str);
    m_snapshot_version = static_cast<DB::version_type>(obj.get<int64_t>(mgr->m_sub_set_snapshot_version));
    auto sub_list = obj.get_linklist(mgr->m_sub_set_subscriptions);
    m_subs.clear();
    for (size_t idx = 0; idx < sub_list.size(); ++idx) {
        m_subs.push_back(Subscription(mgr.get(), sub_list.get_object(idx)));
    }
}

std::shared_ptr<SubscriptionStore> SubscriptionSet::get_flx_subscription_store() const
{
    if (auto mgr = m_mgr.lock()) {
        return mgr;
    }
    throw RuntimeError(ErrorCodes::BrokenInvariant, "Active SubscriptionSet without a SubscriptionStore");
}

int64_t SubscriptionSet::version() const
{
    return m_version;
}

DB::version_type SubscriptionSet::snapshot_version() const
{
    return m_snapshot_version;
}

SubscriptionSet::State SubscriptionSet::state() const
{
    return m_state;
}

StringData SubscriptionSet::error_str() const
{
    if (m_error_str.empty()) {
        return StringData{};
    }
    return m_error_str;
}

size_t SubscriptionSet::size() const
{
    return m_subs.size();
}

const Subscription& SubscriptionSet::at(size_t index) const
{
    return m_subs.at(index);
}

SubscriptionSet::const_iterator SubscriptionSet::begin() const
{
    return m_subs.begin();
}

SubscriptionSet::const_iterator SubscriptionSet::end() const
{
    return m_subs.end();
}

const Subscription* SubscriptionSet::find(StringData name) const
{
    for (auto&& sub : *this) {
        if (sub.name == name)
            return &sub;
    }
    return nullptr;
}

const Subscription* SubscriptionSet::find(const Query& query) const
{
    const auto query_desc = query.get_description();
    const auto table_name = Group::table_name_to_class_name(query.get_table()->get_name());
    for (auto&& sub : *this) {
        if (sub.object_class_name == table_name && sub.query_string == query_desc)
            return &sub;
    }
    return nullptr;
}

MutableSubscriptionSet::MutableSubscriptionSet(std::weak_ptr<SubscriptionStore> mgr, TransactionRef tr, Obj obj)
    : SubscriptionSet(mgr, *tr, obj, MakingMutableCopy{true})
    , m_tr(std::move(tr))
    , m_obj(std::move(obj))
{
}

void MutableSubscriptionSet::check_is_mutable() const
{
    if (m_tr->get_transact_stage() != DB::transact_Writing) {
        throw WrongTransactionState("Not a write transaction");
    }
}

// This uses the 'swap and pop' idiom to run in constant time.
// The iterator returned is:
//  1. end(), if the last subscription is removed
//  2. same iterator it is passed (but pointing to the last subscription in set), otherwise
MutableSubscriptionSet::iterator MutableSubscriptionSet::erase(const_iterator it)
{
    check_is_mutable();
    REALM_ASSERT(it != end());
    if (it == std::prev(m_subs.end())) {
        m_subs.pop_back();
        return end();
    }
    auto back = std::prev(m_subs.end());
    // const_iterator to iterator in constant time (See https://stackoverflow.com/a/10669041)
    auto iterator = m_subs.erase(it, it);
    std::swap(*iterator, *back);
    m_subs.pop_back();
    return iterator;
}

bool MutableSubscriptionSet::erase(StringData name)
{
    check_is_mutable();
    auto ptr = find(name);
    if (!ptr)
        return false;
    auto it = m_subs.begin() + (ptr - &m_subs.front());
    erase(it);
    return true;
}

bool MutableSubscriptionSet::erase(const Query& query)
{
    check_is_mutable();
    auto ptr = find(query);
    if (!ptr)
        return false;
    auto it = m_subs.begin() + (ptr - &m_subs.front());
    erase(it);
    return true;
}

bool MutableSubscriptionSet::erase_by_class_name(StringData object_class_name)
{
    // TODO: Use std::erase_if when switching to C++20.
    auto it = std::remove_if(m_subs.begin(), m_subs.end(), [&object_class_name](const Subscription& sub) {
        return sub.object_class_name == object_class_name;
    });
    auto erased = end() - it;
    m_subs.erase(it, end());
    return erased > 0;
}

bool MutableSubscriptionSet::erase_by_id(ObjectId id)
{
    auto it = std::find_if(m_subs.begin(), m_subs.end(), [&id](const Subscription& sub) -> bool {
        return sub.id == id;
    });
    if (it == end()) {
        return false;
    }
    erase(it);
    return true;
}

void MutableSubscriptionSet::clear()
{
    check_is_mutable();
    m_subs.clear();
}

void MutableSubscriptionSet::insert_sub(const Subscription& sub)
{
    check_is_mutable();
    m_subs.push_back(sub);
}

std::pair<SubscriptionSet::iterator, bool>
MutableSubscriptionSet::insert_or_assign_impl(iterator it, util::Optional<std::string> name,
                                              std::string object_class_name, std::string query_str)
{
    check_is_mutable();
    if (it != end()) {
        auto& sub = m_subs[it - begin()];
        sub.object_class_name = std::move(object_class_name);
        sub.query_string = std::move(query_str);
        sub.updated_at = Timestamp{std::chrono::system_clock::now()};

        return {it, false};
    }
    it = m_subs.insert(m_subs.end(),
                       Subscription(std::move(name), std::move(object_class_name), std::move(query_str)));

    return {it, true};
}

std::pair<SubscriptionSet::iterator, bool> MutableSubscriptionSet::insert_or_assign(std::string_view name,
                                                                                    const Query& query)
{
    auto table_name = Group::table_name_to_class_name(query.get_table()->get_name());
    auto query_str = query.get_description();
    auto it = std::find_if(begin(), end(), [&](const Subscription& sub) {
        return sub.name == name;
    });

    return insert_or_assign_impl(it, std::string{name}, std::move(table_name), std::move(query_str));
}

std::pair<SubscriptionSet::iterator, bool> MutableSubscriptionSet::insert_or_assign(const Query& query)
{
    auto table_name = Group::table_name_to_class_name(query.get_table()->get_name());
    auto query_str = query.get_description();
    auto it = std::find_if(begin(), end(), [&](const Subscription& sub) {
        return (!sub.name && sub.object_class_name == table_name && sub.query_string == query_str);
    });

    return insert_or_assign_impl(it, util::none, std::move(table_name), std::move(query_str));
}

void MutableSubscriptionSet::import(SubscriptionSet&& src_subs)
{
    check_is_mutable();
    SubscriptionSet::import(std::move(src_subs));
}

void SubscriptionSet::import(SubscriptionSet&& src_subs)
{
    m_subs = std::move(src_subs.m_subs);
}

void MutableSubscriptionSet::set_state(State new_state)
{
    REALM_ASSERT(m_state == State::Uncommitted);
    m_state = new_state;
}

MutableSubscriptionSet SubscriptionSet::make_mutable_copy() const
{
    auto mgr = get_flx_subscription_store(); // Throws
    return mgr->make_mutable_copy(*this);
}

void SubscriptionSet::refresh()
{
    auto mgr = get_flx_subscription_store(); // Throws
    if (mgr->would_refresh(m_cur_version)) {
        *this = mgr->get_refreshed(m_obj_key, version());
    }
}

util::Future<SubscriptionSet::State> SubscriptionSet::get_state_change_notification(State notify_when) const
{
    auto mgr = get_flx_subscription_store(); // Throws

    util::CheckedLockGuard lk(mgr->m_pending_notifications_mutex);
    // If we've already been superseded by another version getting completed, then we should skip registering
    // a notification because it may never fire.
    if (mgr->m_min_outstanding_version > version()) {
        return util::Future<State>::make_ready(State::Superseded);
    }

    State cur_state = state();
    std::string err_str = error_str();

    // If there have been writes to the database since this SubscriptionSet was created, we need to fetch
    // the updated version from the DB to know the true current state and maybe return a ready future.
    if (m_cur_version < mgr->m_db->get_version_of_latest_snapshot()) {
        auto refreshed_self = mgr->get_refreshed(m_obj_key, version());
        cur_state = refreshed_self.state();
        err_str = refreshed_self.error_str();
    }
    // If we've already reached the desired state, or if the subscription is in an error state,
    // we can return a ready future immediately.
    if (cur_state == State::Error) {
        return util::Future<State>::make_ready(Status{ErrorCodes::SubscriptionFailed, err_str});
    }
    else if (state_to_order(cur_state) >= state_to_order(notify_when)) {
        return util::Future<State>::make_ready(cur_state);
    }

    // Otherwise, make a promise/future pair and add it to the list of pending notifications.
    auto [promise, future] = util::make_promise_future<State>();
    mgr->m_pending_notifications.emplace_back(version(), std::move(promise), notify_when);
    return std::move(future);
}

void SubscriptionSet::get_state_change_notification(
    State notify_when, util::UniqueFunction<void(util::Optional<State>, util::Optional<Status>)> cb) const
{
    get_state_change_notification(notify_when).get_async([cb = std::move(cb)](StatusWith<State> result) {
        if (result.is_ok()) {
            cb(result.get_value(), {});
        }
        else {
            cb({}, result.get_status());
        }
    });
}

void SubscriptionStore::process_notifications(State new_state, int64_t version, std::string_view error_str)
{
    std::list<SubscriptionStore::NotificationRequest> to_finish;
    {
        util::CheckedLockGuard lk(m_pending_notifications_mutex);
        splice_if(m_pending_notifications, to_finish, [&](auto& req) {
            return (req.version == version &&
                    (new_state == State::Error || state_to_order(new_state) >= state_to_order(req.notify_when))) ||
                   (new_state == State::Complete && req.version < version);
        });

        if (new_state == State::Complete) {
            m_min_outstanding_version = version;
        }
    }

    for (auto& req : to_finish) {
        if (new_state == State::Error && req.version == version) {
            req.promise.set_error({ErrorCodes::SubscriptionFailed, error_str});
        }
        else if (req.version < version) {
            req.promise.emplace_value(State::Superseded);
        }
        else {
            req.promise.emplace_value(new_state);
        }
    }
}

SubscriptionSet MutableSubscriptionSet::commit()
{
    if (m_tr->get_transact_stage() != DB::transact_Writing) {
        throw LogicError(ErrorCodes::WrongTransactionState, "SubscriptionSet has already been committed");
    }
    auto mgr = get_flx_subscription_store(); // Throws

    if (m_state == State::Uncommitted) {
        m_state = State::Pending;
    }
    m_obj.set(mgr->m_sub_set_snapshot_version, static_cast<int64_t>(m_tr->get_version()));

    auto obj_sub_list = m_obj.get_linklist(mgr->m_sub_set_subscriptions);
    obj_sub_list.clear();
    for (const auto& sub : m_subs) {
        auto new_sub = obj_sub_list.create_and_insert_linked_object(obj_sub_list.size());
        new_sub.set(mgr->m_sub_id, sub.id);
        new_sub.set(mgr->m_sub_created_at, sub.created_at);
        new_sub.set(mgr->m_sub_updated_at, sub.updated_at);
        if (sub.name) {
            new_sub.set(mgr->m_sub_name, StringData(*sub.name));
        }
        new_sub.set(mgr->m_sub_object_class_name, StringData(sub.object_class_name));
        new_sub.set(mgr->m_sub_query_str, StringData(sub.query_string));
    }
    m_obj.set(mgr->m_sub_set_state, state_to_storage(m_state));
    if (!m_error_str.empty()) {
        m_obj.set(mgr->m_sub_set_error_str, StringData(m_error_str));
    }

    const auto flx_version = version();
    m_tr->commit_and_continue_as_read();

    mgr->process_notifications(m_state, flx_version, std::string_view(error_str()));

    return mgr->get_refreshed(m_obj.get_key(), flx_version, m_tr->get_version_of_current_transaction());
}

std::string SubscriptionSet::to_ext_json() const
{
    if (m_subs.empty()) {
        return "{}";
    }

    util::FlatMap<std::string, std::vector<std::string>> table_to_query;
    for (const auto& sub : *this) {
        std::string table_name(sub.object_class_name);
        auto& queries_for_table = table_to_query.at(table_name);
        auto query_it = std::find(queries_for_table.begin(), queries_for_table.end(), sub.query_string);
        if (query_it != queries_for_table.end()) {
            continue;
        }
        queries_for_table.emplace_back(sub.query_string);
    }

    if (table_to_query.empty()) {
        return "{}";
    }

    // TODO this is pulling in a giant compile-time dependency. We should have a better way of escaping the
    // query strings into a json object.
    nlohmann::json output_json;
    for (auto& table : table_to_query) {
        // We want to make sure that the queries appear in some kind of canonical order so that if there are
        // two subscription sets with the same subscriptions in different orders, the server doesn't have to
        // waste a bunch of time re-running the queries for that table.
        std::stable_sort(table.second.begin(), table.second.end());

        bool is_first = true;
        std::ostringstream obuf;
        for (const auto& query_str : table.second) {
            if (!is_first) {
                obuf << " OR ";
            }
            is_first = false;
            obuf << "(" << query_str << ")";
        }
        output_json[table.first] = obuf.str();
    }

    return output_json.dump();
}

SubscriptionStoreRef SubscriptionStore::create(DBRef db)
{
    return std::make_shared<SubscriptionStore>(Private(), std::move(db));
}

SubscriptionStore::SubscriptionStore(Private, DBRef db)
    : m_db(std::move(db))
{
    std::vector<SyncMetadataTable> internal_tables{
        {&m_sub_set_table,
         c_flx_subscription_sets_table,
         {&m_sub_set_version_num, c_flx_sub_sets_version_field, type_Int},
         {
             {&m_sub_set_state, c_flx_sub_sets_state_field, type_Int},
             {&m_sub_set_snapshot_version, c_flx_sub_sets_snapshot_version_field, type_Int},
             {&m_sub_set_error_str, c_flx_sub_sets_error_str_field, type_String, true},
             {&m_sub_set_subscriptions, c_flx_sub_sets_subscriptions_field, c_flx_subscriptions_table, true},
         }},
        {&m_sub_table,
         c_flx_subscriptions_table,
         SyncMetadataTable::IsEmbeddedTag{},
         {
             {&m_sub_id, c_flx_sub_id_field, type_ObjectId},
             {&m_sub_created_at, c_flx_sub_created_at_field, type_Timestamp},
             {&m_sub_updated_at, c_flx_sub_updated_at_field, type_Timestamp},
             {&m_sub_name, c_flx_sub_name_field, type_String, true},
             {&m_sub_object_class_name, c_flx_sub_object_class_field, type_String},
             {&m_sub_query_str, c_flx_sub_query_str_field, type_String},
         }},
    };

    auto tr = m_db->start_read();
    // Start with a reader so it doesn't try to write until we are ready
    SyncMetadataSchemaVersionsReader schema_versions_reader(tr);

    if (auto schema_version =
            schema_versions_reader.get_version_for(tr, internal_schema_groups::c_flx_subscription_store)) {
        if (*schema_version != c_flx_schema_version) {
            throw RuntimeError(ErrorCodes::UnsupportedFileFormatVersion,
                               "Invalid schema version for flexible sync metadata");
        }
        load_sync_metadata_schema(tr, &internal_tables);
    }
    else {
        tr->promote_to_write();
        // Ensure the schema versions table is initialized (may add its own commit)
        SyncMetadataSchemaVersions schema_versions(tr);
        // Create the metadata schema and set the version (in the same commit)
        schema_versions.set_version_for(tr, internal_schema_groups::c_flx_subscription_store, c_flx_schema_version);
        create_sync_metadata_schema(tr, &internal_tables);
        tr->commit_and_continue_as_read();
    }
    REALM_ASSERT(m_sub_set_table);

    // Make sure the subscription set table is properly initialized
    initialize_subscriptions_table(std::move(tr));
}

void SubscriptionStore::initialize_subscriptions_table(TransactionRef&& tr)
{
    if (auto sub_sets = tr->get_table(m_sub_set_table); sub_sets->is_empty()) {
        tr->promote_to_write();
        clear(*tr);
        tr->commit();
    }
}

void SubscriptionStore::clear(Transaction& wt)
{
    auto sub_sets = wt.get_table(m_sub_set_table);
    sub_sets->clear();
    // There should always be at least one subscription set so that the user can always wait
    // for synchronizationon on the result of get_latest().
    auto zero_sub = sub_sets->create_object_with_primary_key(Mixed{int64_t(0)});
    zero_sub.set(m_sub_set_state, static_cast<int64_t>(SubscriptionSet::State::Pending));
    zero_sub.set(m_sub_set_snapshot_version, wt.get_version());
}

SubscriptionSet SubscriptionStore::get_latest()
{
    auto tr = m_db->start_frozen();
    auto sub_sets = tr->get_table(m_sub_set_table);
    // There should always be at least one SubscriptionSet - the zeroth subscription set for schema instructions.
    REALM_ASSERT(!sub_sets->is_empty());

    auto latest_id = sub_sets->max(sub_sets->get_primary_key_column())->get_int();
    auto latest_obj = sub_sets->get_object_with_primary_key(Mixed{latest_id});

    return SubscriptionSet(weak_from_this(), *tr, latest_obj);
}

SubscriptionSet SubscriptionStore::get_active()
{
    auto tr = m_db->start_frozen();
    return SubscriptionSet(weak_from_this(), *tr, get_active(*tr));
}

Obj SubscriptionStore::get_active(const Transaction& tr)
{
    auto sub_sets = tr.get_table(m_sub_set_table);
    // There should always be at least one SubscriptionSet - the zeroth subscription set for schema instructions.
    REALM_ASSERT(!sub_sets->is_empty());

    DescriptorOrdering descriptor_ordering;
    descriptor_ordering.append_sort(SortDescriptor{{{sub_sets->get_primary_key_column()}}, {false}});
    descriptor_ordering.append_limit(LimitDescriptor{1});
    auto res = sub_sets->where()
                   .equal(m_sub_set_state, state_to_storage(SubscriptionSet::State::Complete))
                   .Or()
                   .equal(m_sub_set_state, state_to_storage(SubscriptionSet::State::AwaitingMark))
                   .find_all(descriptor_ordering);

    // If there is no active subscription yet, return the zeroth subscription.
    if (res.is_empty()) {
        return sub_sets->get_object_with_primary_key(0);
    }
    return res.get_object(0);
}

SubscriptionStore::VersionInfo SubscriptionStore::get_version_info() const
{
    auto tr = m_db->start_read();
    auto sub_sets = tr->get_table(m_sub_set_table);
    // There should always be at least one SubscriptionSet - the zeroth subscription set for schema instructions.
    REALM_ASSERT(!sub_sets->is_empty());

    VersionInfo ret;
    ret.latest = sub_sets->max(sub_sets->get_primary_key_column())->get_int();
    DescriptorOrdering descriptor_ordering;
    descriptor_ordering.append_sort(SortDescriptor{{{sub_sets->get_primary_key_column()}}, {false}});
    descriptor_ordering.append_limit(LimitDescriptor{1});

    auto res = sub_sets->where()
                   .equal(m_sub_set_state, state_to_storage(SubscriptionSet::State::Complete))
                   .Or()
                   .equal(m_sub_set_state, state_to_storage(SubscriptionSet::State::AwaitingMark))
                   .find_all(descriptor_ordering);
    ret.active = res.is_empty() ? SubscriptionSet::EmptyVersion : res.get_object(0).get_primary_key().get_int();

    res = sub_sets->where()
              .equal(m_sub_set_state, state_to_storage(SubscriptionSet::State::AwaitingMark))
              .find_all(descriptor_ordering);
    ret.pending_mark = res.is_empty() ? SubscriptionSet::EmptyVersion : res.get_object(0).get_primary_key().get_int();

    return ret;
}

util::Optional<SubscriptionStore::PendingSubscription>
SubscriptionStore::get_next_pending_version(int64_t last_query_version) const
{
    auto tr = m_db->start_read();
    auto sub_sets = tr->get_table(m_sub_set_table);
    // There should always be at least one SubscriptionSet - the zeroth subscription set for schema instructions.
    REALM_ASSERT(!sub_sets->is_empty());

    DescriptorOrdering descriptor_ordering;
    descriptor_ordering.append_sort(SortDescriptor{{{sub_sets->get_primary_key_column()}}, {true}});
    auto res = sub_sets->where()
                   .greater(sub_sets->get_primary_key_column(), last_query_version)
                   .group()
                   .equal(m_sub_set_state, state_to_storage(SubscriptionSet::State::Pending))
                   .Or()
                   .equal(m_sub_set_state, state_to_storage(SubscriptionSet::State::Bootstrapping))
                   .end_group()
                   .find_all(descriptor_ordering);

    if (res.is_empty()) {
        return util::none;
    }

    auto obj = res.get_object(0);
    auto query_version = obj.get_primary_key().get_int();
    auto snapshot_version = obj.get<int64_t>(m_sub_set_snapshot_version);
    return PendingSubscription{query_version, static_cast<DB::version_type>(snapshot_version)};
}

std::vector<SubscriptionSet> SubscriptionStore::get_pending_subscriptions()
{
    std::vector<SubscriptionSet> subscriptions_to_recover;
    auto active_sub = get_active();
    auto cur_query_version = active_sub.version();
    // get a copy of the pending subscription sets since the active version
    while (auto next_pending = get_next_pending_version(cur_query_version)) {
        cur_query_version = next_pending->query_version;
        subscriptions_to_recover.push_back(get_by_version(cur_query_version));
    }
    return subscriptions_to_recover;
}

void SubscriptionStore::notify_all_state_change_notifications(Status status)
{
    util::CheckedUniqueLock lk(m_pending_notifications_mutex);
    auto to_finish = std::move(m_pending_notifications);
    lk.unlock();

    // Just complete/cancel the pending notifications - this function does not alter the
    // state of any pending subscriptions
    for (auto& req : to_finish) {
        req.promise.set_error(status);
    }
}

void SubscriptionStore::reset(Transaction& wt)
{
    // Clear out and initialize the subscription store
    clear(wt);

    util::CheckedUniqueLock lk(m_pending_notifications_mutex);
    auto to_finish = std::move(m_pending_notifications);
    m_min_outstanding_version = 0;
    lk.unlock();

    for (auto& req : to_finish) {
        req.promise.emplace_value(SubscriptionSet::State::Superseded);
    }
}

void SubscriptionStore::update_state(int64_t version, State new_state, std::optional<std::string_view> error_str)
{
    REALM_ASSERT(error_str.has_value() == (new_state == State::Error));
    REALM_ASSERT(new_state != State::Pending);
    REALM_ASSERT(new_state != State::Superseded);

    auto tr = m_db->start_write();
    auto sub_sets = tr->get_table(m_sub_set_table);
    auto obj = sub_sets->get_object_with_primary_key(version);
    if (!obj) {
        // This can happen either due to a bug in the sync client or due to the
        // server sending us an error message for an invalid query version. We
        // assume it is the latter here.
        throw RuntimeError(ErrorCodes::SyncProtocolInvariantFailed,
                           util::format("Invalid state update for nonexistent query version %1", version));
    }

    auto old_state = state_from_storage(obj.get<int64_t>(m_sub_set_state));
    switch (new_state) {
        case State::Error:
            if (old_state == State::Complete) {
                throw RuntimeError(ErrorCodes::SyncProtocolInvariantFailed,
                                   util::format("Received error '%1' for already-completed query version %2. This "
                                                "may be due to a queryable field being removed in the server-side "
                                                "configuration making the previous subscription set no longer valid.",
                                                *error_str, version));
            }
            break;

        case State::Bootstrapping:
        case State::AwaitingMark:
            REALM_ASSERT(old_state != State::Complete);
            REALM_ASSERT(old_state != State::Error);
            break;

        case State::Complete:
            supercede_prior_to(tr, version);
            break;

        case State::Uncommitted:
        case State::Superseded:
        case State::Pending:
            REALM_TERMINATE("Illegal new state for subscription set");
    }

    obj.set(m_sub_set_state, state_to_storage(new_state));
    obj.set(m_sub_set_error_str, error_str ? StringData(*error_str) : StringData());

    tr->commit();

    process_notifications(new_state, version, error_str.value_or(std::string_view{}));
}

SubscriptionSet SubscriptionStore::get_by_version(int64_t version_id)
{
    auto tr = m_db->start_frozen();
    auto sub_sets = tr->get_table(m_sub_set_table);
    if (auto obj = sub_sets->get_object_with_primary_key(version_id)) {
        return SubscriptionSet(weak_from_this(), *tr, obj);
    }

    util::CheckedLockGuard lk(m_pending_notifications_mutex);
    if (version_id < m_min_outstanding_version) {
        return SubscriptionSet(weak_from_this(), version_id, SubscriptionSet::SupersededTag{});
    }
    throw KeyNotFound(util::format("Subscription set with version %1 not found", version_id));
}

SubscriptionSet SubscriptionStore::get_refreshed(ObjKey key, int64_t version, std::optional<DB::VersionID> db_version)
{
    auto tr = m_db->start_frozen(db_version.value_or(VersionID{}));
    auto sub_sets = tr->get_table(m_sub_set_table);
    if (auto obj = sub_sets->try_get_object(key)) {
        return SubscriptionSet(weak_from_this(), *tr, obj);
    }
    return SubscriptionSet(weak_from_this(), version, SubscriptionSet::SupersededTag{});
}

SubscriptionStore::TableSet SubscriptionStore::get_tables_for_latest(const Transaction& tr) const
{
    auto sub_sets = tr.get_table(m_sub_set_table);
    // There should always be at least one SubscriptionSet - the zeroth subscription set for schema instructions.
    REALM_ASSERT(!sub_sets->is_empty());

    auto latest_id = sub_sets->max(sub_sets->get_primary_key_column())->get_int();
    auto latest_obj = sub_sets->get_object_with_primary_key(Mixed{latest_id});

    TableSet ret;
    auto subs = latest_obj.get_linklist(m_sub_set_subscriptions);
    for (size_t idx = 0; idx < subs.size(); ++idx) {
        auto sub_obj = subs.get_object(idx);
        ret.emplace(sub_obj.get<StringData>(m_sub_object_class_name));
    }

    return ret;
}

void SubscriptionStore::supercede_prior_to(TransactionRef tr, int64_t version_id) const
{
    auto sub_sets = tr->get_table(m_sub_set_table);
    Query remove_query(sub_sets);
    remove_query.less(sub_sets->get_primary_key_column(), version_id);
    remove_query.remove();
}

MutableSubscriptionSet SubscriptionStore::make_mutable_copy(const SubscriptionSet& set)
{
    auto new_tr = m_db->start_write();

    auto sub_sets = new_tr->get_table(m_sub_set_table);
    auto new_pk = sub_sets->max(sub_sets->get_primary_key_column())->get_int() + 1;

    MutableSubscriptionSet new_set_obj(weak_from_this(), std::move(new_tr),
                                       sub_sets->create_object_with_primary_key(Mixed{new_pk}));
    for (const auto& sub : set) {
        new_set_obj.insert_sub(sub);
    }

    return new_set_obj;
}

bool SubscriptionStore::would_refresh(DB::version_type version) const noexcept
{
    return version < m_db->get_version_of_latest_snapshot();
}

int64_t SubscriptionStore::set_active_as_latest(Transaction& wt)
{
    auto sub_sets = wt.get_table(m_sub_set_table);
    auto active = get_active(wt);
    // Delete all newer subscription sets, if any
    sub_sets->where().greater(sub_sets->get_primary_key_column(), active.get_primary_key().get_int()).remove();
    // Mark the active set as complete even if it was previously WaitingForMark
    // as we've completed rebootstrapping before calling this.
    active.set(m_sub_set_state, state_to_storage(State::Complete));
    auto version = active.get_primary_key().get_int();

    std::list<NotificationRequest> to_finish;
    {
        util::CheckedLockGuard lock(m_pending_notifications_mutex);
        splice_if(m_pending_notifications, to_finish, [&](auto& req) {
            if (req.version == version && state_to_order(req.notify_when) <= state_to_order(State::Complete))
                return true;
            return req.version != version;
        });
    }

    for (auto& req : to_finish) {
        req.promise.emplace_value(req.version == version ? State::Complete : State::Superseded);
    }

    return version;
}

int64_t SubscriptionStore::mark_active_as_complete(Transaction& wt)
{
    auto active = get_active(wt);
    active.set(m_sub_set_state, state_to_storage(State::Complete));
    auto version = active.get_primary_key().get_int();

    std::list<NotificationRequest> to_finish;
    {
        util::CheckedLockGuard lock(m_pending_notifications_mutex);
        splice_if(m_pending_notifications, to_finish, [&](auto& req) {
            return req.version == version && state_to_order(req.notify_when) <= state_to_order(State::Complete);
        });
    }

    for (auto& req : to_finish) {
        req.promise.emplace_value(State::Complete);
    }

    return version;
}

} // namespace realm::sync
