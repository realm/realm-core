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
#include "realm/transaction.hpp"
#include "realm/keys.hpp"
#include "realm/list.hpp"
#include "realm/sort_descriptor.hpp"
#include "realm/sync/noinst/sync_metadata_schema.hpp"
#include "realm/table.hpp"
#include "realm/table_view.hpp"
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
            throw std::runtime_error(util::format("Invalid state for SubscriptionSet stored on disk: %1", value));
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
        case State::PendingFlush:
            return 1;
        case State::Pending:
            return 2;
        case State::Bootstrapping:
            return 3;
        case State::AwaitingMark:
            return 4;
        case State::Complete:
            return 5;
        case State::Error:
            return 6;
        case State::Superseded:
            return 7;
    }
    REALM_UNREACHABLE();
}

constexpr auto s_empty_snapshot_version = std::numeric_limits<DB::version_type>::max();

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


SubscriptionSet::SubscriptionSet(std::weak_ptr<SubscriptionStore> mgr, int64_t version, State state,
                                 std::string error_str, DB::version_type snapshot_version,
                                 std::vector<Subscription> subs)
    : m_mgr(mgr)
    , m_version(version)
    , m_state(state)
    , m_error_str(std::move(error_str))
    , m_snapshot_version(snapshot_version)
    , m_subs(std::move(subs))
{
}

SubscriptionSet::SubscriptionSet(std::weak_ptr<SubscriptionStore> mgr, int64_t version, SupersededTag)
    : m_mgr(mgr)
    , m_version(version)
    , m_state(State::Superseded)
{
}

std::shared_ptr<SubscriptionStore> SubscriptionSet::get_flx_subscription_store() const
{
    if (auto mgr = m_mgr.lock()) {
        return mgr;
    }
    throw std::logic_error("Active SubscriptionSet without a SubscriptionStore");
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

MutableSubscriptionSet::MutableSubscriptionSet(std::weak_ptr<SubscriptionStore> mgr, int64_t version, State state,
                                               std::string error_str, DB::version_type snapshot_version,
                                               std::vector<Subscription> subs)
    : SubscriptionSet(mgr, version, state, std::move(error_str), snapshot_version, std::move(subs))
    , m_committed(util::make_bind<OutstandingMutableGuard>(mgr.lock()))
    , m_old_state(state)
{
}

void MutableSubscriptionSet::check_is_mutable()
{
    if (m_committed->finished) {
        throw WrongTransactionState("MutableSubscriptionSet has already been committed");
    }
}

MutableSubscriptionSet::iterator MutableSubscriptionSet::erase(const_iterator it)
{
    check_is_mutable();
    REALM_ASSERT(it != end());
    return m_subs.erase(it);
}

bool MutableSubscriptionSet::erase(StringData name)
{
    check_is_mutable();
    auto ptr = find(name);
    if (!ptr)
        return false;
    m_subs.erase(m_subs.begin() + (ptr - &m_subs.front()));
    return true;
}

bool MutableSubscriptionSet::erase(const Query& query)
{
    check_is_mutable();
    auto ptr = find(query);
    if (!ptr)
        return false;
    m_subs.erase(m_subs.begin() + (ptr - &m_subs.front()));
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

void MutableSubscriptionSet::import(const SubscriptionSet& src_subs)
{
    clear();
    for (const Subscription& sub : src_subs) {
        insert_sub(sub);
    }
}

void MutableSubscriptionSet::update_state(State new_state, util::Optional<std::string_view> error_str)
{
    check_is_mutable();
    auto old_state = state();
    if (error_str && new_state != State::Error) {
        throw std::logic_error("Cannot supply an error message for a subscription set when state is not Error");
    }
    switch (new_state) {
        case State::Uncommitted:
            throw std::logic_error("cannot set subscription set state to uncommitted");

        case State::Error:
            if (old_state != State::Bootstrapping && old_state != State::Pending && old_state != State::Uncommitted) {
                throw std::logic_error(
                    "subscription set must be in Bootstrapping or Pending to update state to error");
            }
            if (!error_str) {
                throw std::logic_error("Must supply an error message when setting a subscription to the error state");
            }

            m_state = new_state;
            m_error_str = std::string{*error_str};
            break;
        case State::Bootstrapping:
            [[fallthrough]];
        case State::AwaitingMark:
            [[fallthrough]];
        case State::Complete:
            m_state = new_state;
            break;
        case State::Superseded:
            throw std::logic_error("Cannot set a subscription to the superseded state");
            break;
        case State::PendingFlush:
            throw std::logic_error("Cannot set subscription set to the pending flush state");
            break;
        case State::Pending:
            throw std::logic_error("Cannot set subscription set to the pending state");
            break;
    }
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
        *this = mgr->get_by_version(version());
    }
}

util::Future<SubscriptionSet::State> SubscriptionSet::get_state_change_notification(State notify_when) const
{
    auto mgr = get_flx_subscription_store(); // Throws

    std::unique_lock<std::mutex> lk(mgr->m_pending_notifications_mutex);
    // If we've already been superceded by another version getting completed, then we should skip registering
    // a notification because it may never fire.
    if (mgr->m_min_outstanding_version > version()) {
        return util::Future<State>::make_ready(State::Superseded);
    }

    // Begin by blocking process_notifications from starting to fill futures. No matter the outcome, we'll
    // unblock process_notifications() at the end of this function via the guard we construct below.
    mgr->m_outstanding_requests++;
    auto guard = util::make_scope_exit([&]() noexcept {
        if (!lk.owns_lock()) {
            lk.lock();
        }
        --mgr->m_outstanding_requests;
        mgr->m_pending_notifications_cv.notify_one();
    });
    lk.unlock();

    State cur_state = state();
    std::string err_str = error_str();

    // If there have been writes to the database since this SubscriptionSet was created, we need to fetch
    // the updated version from the DB to know the true current state and maybe return a ready future.
    if (m_cur_version < mgr->m_db->get_version_of_latest_snapshot()) {
        try {
            auto refreshed_self = mgr->get_by_version(version());
            cur_state = refreshed_self.state();
            err_str = refreshed_self.error_str();
        }
        catch (const KeyNotFound&) {
            // We may not have been committed yet if this is actually a MutableSubscriptionSet, so just
            // ignore this for now.
        }
    }
    // If we've already reached the desired state, or if the subscription is in an error state,
    // we can return a ready future immediately.
    if (cur_state == State::Error) {
        mgr->m_logger->trace("Returning ready error future for state change notification version %1", version());
        return util::Future<State>::make_ready(Status{ErrorCodes::SubscriptionFailed, std::move(err_str)});
    }
    else if (state_to_order(cur_state) >= state_to_order(notify_when)) {
        mgr->m_logger->trace("Returning ready error future for state change notification version %1, "
                             "requested_state: %2 reached_state: %3",
                             version(), notify_when, cur_state);
        return util::Future<State>::make_ready(cur_state);
    }

    // Otherwise put in a new request to be filled in by process_notifications().
    lk.lock();

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


SubscriptionSet MutableSubscriptionSet::commit()
{
    check_is_mutable();
    if (m_state == State::Uncommitted) {
        m_state = State::PendingFlush;
    }
    auto mgr = get_flx_subscription_store(); // Throws
    m_committed->finished = true;
    mgr->cache_mutable_subscription_set(*this);

    return mgr->get_by_version(version());
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

OutstandingMutableGuard::~OutstandingMutableGuard()
{
    if (finished) {
        return;
    }

    store->release_outstanding_mutable_sub_set();
}

namespace {
class SubscriptionStoreInit : public SubscriptionStore {
public:
    explicit SubscriptionStoreInit(DBRef db, const std::shared_ptr<util::Logger>& parent_logger,
                                   util::UniqueFunction<void(int64_t)> on_new_subscription_set)
        : SubscriptionStore(std::move(db), parent_logger, std::move(on_new_subscription_set))
    {
    }
};
} // namespace

SubscriptionStoreRef SubscriptionStore::create(DBRef db, const std::shared_ptr<util::Logger>& parent_logger,
                                               util::UniqueFunction<void(int64_t)> on_new_subscription_set)
{
    return std::make_shared<SubscriptionStoreInit>(std::move(db), parent_logger, std::move(on_new_subscription_set));
}

SubscriptionStore::SubscriptionStore(DBRef db, const std::shared_ptr<util::Logger>& parent_logger,
                                     util::UniqueFunction<void(int64_t)> on_new_subscription_set)
    : m_db(std::move(db))
    , m_logger(std::make_shared<util::PrefixLogger>(
          util::format("SubscriptionStore DB %1 ", StringData(m_db->get_path()).hash() & 0xffff), parent_logger))
    , m_on_new_subscription_set(std::move(on_new_subscription_set))
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
    SyncMetadataSchemaVersions schema_versions(tr);

    if (auto schema_version = schema_versions.get_version_for(tr, internal_schema_groups::c_flx_subscription_store);
        !schema_version) {
        tr->promote_to_write();
        schema_versions.set_version_for(tr, internal_schema_groups::c_flx_subscription_store, c_flx_schema_version);
        create_sync_metadata_schema(tr, &internal_tables);
        tr->commit_and_continue_as_read();
    }
    else {
        if (*schema_version != c_flx_schema_version) {
            throw std::runtime_error("Invalid schema version for flexible sync metadata");
        }
        load_sync_metadata_schema(tr, &internal_tables);
    }

    // Make sure the subscription set table is properly initialized
    initialize_subscriptions_table(std::move(tr), false);
}

void SubscriptionStore::initialize_subscriptions_table(TransactionRef&& tr, bool clear_table)
{
    auto sub_sets = tr->get_table(m_sub_set_table);
    if (!sub_sets) {
        return;
    }
    if (clear_table || sub_sets->is_empty()) {
        tr->promote_to_write();
        // If erase_table is true, clear out the sub_sets table
        if (clear_table) {
            sub_sets->clear();
        }
        // There should always be at least one subscription set so that the user can always wait
        // for synchronizationon on the result of get_latest().
        auto zero_sub = sub_sets->create_object_with_primary_key(Mixed{int64_t(0)});
        zero_sub.set(m_sub_set_state, static_cast<int64_t>(SubscriptionSet::State::Pending));
        zero_sub.set(m_sub_set_snapshot_version, tr->get_version());
        tr->commit_and_continue_as_read();
    }

    m_next_new_subscription_set_version = sub_sets->max(sub_sets->get_primary_key_column())->get_int() + 1;

    m_latest_version = sub_sets->max(sub_sets->get_primary_key_column())->get_int();

    DescriptorOrdering descriptor_ordering;
    descriptor_ordering.append_sort(SortDescriptor{{{sub_sets->get_primary_key_column()}}, {false}});
    descriptor_ordering.append_limit(LimitDescriptor{1});
    auto res = sub_sets->where()
                   .equal(m_sub_set_state, state_to_storage(SubscriptionSet::State::Complete))
                   .Or()
                   .equal(m_sub_set_state, state_to_storage(SubscriptionSet::State::AwaitingMark))
                   .find_all(descriptor_ordering);
    m_active_version = res.is_empty() ? SubscriptionSet::EmptyVersion : res.get_object(0).get_primary_key().get_int();

    res = sub_sets->where()
              .equal(m_sub_set_state, state_to_storage(SubscriptionSet::State::AwaitingMark))
              .find_all(descriptor_ordering);
    m_awaiting_mark_version =
        res.is_empty() ? SubscriptionSet::EmptyVersion : res.get_object(0).get_primary_key().get_int();
    m_logger->trace("Initialized subscription store. Latest version: %1, Active version: %2, Awaiting Mark version: "
                    "%3 Next version number: %4",
                    m_latest_version, m_active_version, m_awaiting_mark_version, m_next_new_subscription_set_version);
}


SubscriptionSet SubscriptionStore::get_latest()
{
    std::unique_lock<std::mutex> lk(m_cached_sub_sets_mutex);
    return get_by_version_impl(lk, m_latest_version, std::nullopt);
}


SubscriptionSet SubscriptionStore::get_active()
{
    std::unique_lock<std::mutex> lk(m_cached_sub_sets_mutex);
    auto version = m_active_version;
    // If there is no active subscription set then return the zero'th subscription set instead.
    if (version == SubscriptionSet::EmptyVersion) {
        version = 0;
    }
    return get_by_version_impl(lk, version, std::nullopt);
}


SubscriptionStore::VersionInfo SubscriptionStore::get_version_info() const
{
    std::unique_lock<std::mutex> lk(m_cached_sub_sets_mutex);
    m_logger->trace("Getting version info: latest version: %1, active version: %2, awaiting mark version: %3",
                    m_latest_version, m_active_version, m_awaiting_mark_version);
    return VersionInfo{m_latest_version, m_active_version, m_awaiting_mark_version};
}

util::Optional<SubscriptionStore::PendingSubscription>
SubscriptionStore::get_next_pending_version(int64_t last_query_version, DB::version_type after_client_version)
{
    if (auto from_db = get_next_pending_version_from_db(last_query_version, after_client_version)) {
        return from_db;
    }

    std::lock_guard<std::mutex> lk(m_cached_sub_sets_mutex);
    for (auto&& [version, sub_set] : m_unpersisted_subscription_sets) {
        if (version <= last_query_version) {
            continue;
        }
        if (sub_set.snapshot_version() != s_empty_snapshot_version &&
            sub_set.snapshot_version() >= after_client_version) {
            continue;
        }
        if (sub_set.state() != SubscriptionSet::State::Pending &&
            sub_set.state() != SubscriptionSet::State::PendingFlush &&
            sub_set.state() != SubscriptionSet::State::Bootstrapping) {
            continue;
        }

        return PendingSubscription{version, sub_set.snapshot_version()};
    }

    return util::none;
}

util::Optional<SubscriptionStore::PendingSubscription>
SubscriptionStore::get_next_pending_version_from_db(int64_t last_query_version, DB::version_type after_client_version)
{
    auto tr = m_db->start_frozen();
    auto sub_sets = tr->get_table(m_sub_set_table);
    // There should always be at least one SubscriptionSet - the zero'th subscription set for schema instructions.
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
                   .greater_equal(m_sub_set_snapshot_version, static_cast<int64_t>(after_client_version))
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
    while (auto next_pending = get_next_pending_version(cur_query_version, 0)) {
        cur_query_version = next_pending->query_version;
        subscriptions_to_recover.push_back(get_by_version(cur_query_version));
    }
    return subscriptions_to_recover;
}

void SubscriptionStore::notify_all_state_change_notifications(Status status)
{
    std::unique_lock<std::mutex> lk(m_pending_notifications_mutex);
    m_pending_notifications_cv.wait(lk, [&] {
        return m_outstanding_requests == 0;
    });

    auto to_finish = std::move(m_pending_notifications);
    lk.unlock();

    // Just complete/cancel the pending notifications - this function does not alter the
    // state of any pending subscriptions
    for (auto& req : to_finish) {
        req.promise.set_error(status);
    }
}

void SubscriptionStore::terminate()
{
    // Clear out and initialize the subscription store
    initialize_subscriptions_table(m_db->start_read(), true);

    std::unique_lock<std::mutex> lk(m_pending_notifications_mutex);
    m_pending_notifications_cv.wait(lk, [&] {
        return m_outstanding_requests == 0;
    });
    auto to_finish = std::move(m_pending_notifications);
    m_min_outstanding_version = 0;

    lk.unlock();

    for (auto& req : to_finish) {
        req.promise.emplace_value(SubscriptionSet::State::Superseded);
    }
}

MutableSubscriptionSet SubscriptionStore::get_mutable_by_version(int64_t version_id)
{
    std::unique_lock<std::mutex> lk(m_cached_sub_sets_mutex);
    m_outstanding_sub_set_committed_cv.wait(lk, [&] {
        return m_mutable_subcription_outstanding == false;
    });
    m_mutable_subcription_outstanding = true;
    auto mut_guard = util::make_scope_exit([&]() noexcept {
        if (!lk.owns_lock()) {
            lk.lock();
        }
        m_mutable_subcription_outstanding = false;
    });
    if (version_id < m_min_outstanding_version) {
        throw KeyNotFound(util::format("SubscriptionSet version %1 has been superseded", version_id));
    }

    auto persisted_sub = get_by_version_impl(lk, version_id, std::nullopt);

    auto ret = MutableSubscriptionSet(weak_from_this(), persisted_sub.version(), persisted_sub.state(),
                                      persisted_sub.error_str(), persisted_sub.snapshot_version(),
                                      std::move(persisted_sub.m_subs));
    mut_guard.release();
    return ret;
}


SubscriptionSet SubscriptionStore::get_by_version_impl(std::unique_lock<std::mutex>&, int64_t version_id,
                                                       std::optional<TransactionRef> opt_tr)
{
    if (version_id < m_min_outstanding_version) {
        return SubscriptionSet(weak_from_this(), version_id, SubscriptionSet::SupersededTag{});
    }
    if (auto it = m_unpersisted_subscription_sets.find(version_id); it != m_unpersisted_subscription_sets.end()) {
        auto& sub_set = it->second;
        std::vector<Subscription> subs(sub_set.begin(), sub_set.end());
        m_logger->trace("Found cached subscription set version %1, state: %2, snapshot_version: %3, nsubs: %4",
                        sub_set.version(), sub_set.state(), sub_set.snapshot_version(), subs.size());
        return SubscriptionSet(weak_from_this(), sub_set.version(), sub_set.state(), sub_set.error_str(),
                               sub_set.snapshot_version(), std::move(subs));
    }

    if (!opt_tr) {
        opt_tr = m_db->start_frozen();
    }
    const auto& tr = opt_tr->get();
    auto sub_sets = tr->get_table(m_sub_set_table);
    auto obj = sub_sets->get_object_with_primary_key(Mixed{version_id});

    std::vector<Subscription> subs;
    auto sub_list = obj.get_linklist(m_sub_set_subscriptions);
    for (size_t idx = 0; idx < sub_list.size(); ++idx) {
        subs.push_back(Subscription(this, sub_list.get_object(idx)));
        m_logger->trace("Found subscription %1: \"%2\"", idx, subs.back().query_string);
    }
    auto sub_set =
        SubscriptionSet(weak_from_this(), version_id, state_from_storage(obj.get<int64_t>(m_sub_set_state)),
                        obj.get<String>(m_sub_set_error_str),
                        static_cast<DB::version_type>(obj.get<int64_t>(m_sub_set_snapshot_version)), std::move(subs));
    m_logger->trace("Got subscription set form storage version %1, state: %2, snapshot_version: %3",
                    sub_set.version(), sub_set.state(), sub_set.snapshot_version());
    return sub_set;
}


SubscriptionSet SubscriptionStore::get_by_version(int64_t version_id)
{
    std::unique_lock<std::mutex> lk(m_cached_sub_sets_mutex);
    return get_by_version_impl(lk, version_id, std::nullopt);
}

SubscriptionStore::TableSet SubscriptionStore::get_tables_for_latest(const Transaction& tr) const
{
    TableSet ret;
    {
        std::lock_guard<std::mutex> lk(m_cached_sub_sets_mutex);
        if (auto it = m_unpersisted_subscription_sets.find(m_latest_version);
            it != m_unpersisted_subscription_sets.end()) {
            for (const auto& sub : it->second) {
                ret.emplace(sub.object_class_name);
            }

            return ret;
        }
    }

    auto sub_sets = tr.get_table(m_sub_set_table);
    // There should always be at least one SubscriptionSet - the zero'th subscription set for schema instructions.
    REALM_ASSERT(!sub_sets->is_empty());

    auto latest_obj = sub_sets->get_object_with_primary_key(Mixed{m_latest_version});

    auto subs = latest_obj.get_linklist(m_sub_set_subscriptions);
    for (size_t idx = 0; idx < subs.size(); ++idx) {
        auto sub_obj = subs.get_object(idx);
        ret.emplace(sub_obj.get<StringData>(m_sub_object_class_name));
    }

    return ret;
}

void SubscriptionStore::supercede_prior_to(Transaction& tr, int64_t version_id) const
{
    auto sub_sets = tr.get_table(m_sub_set_table);
    Query remove_query(sub_sets);
    remove_query.less(sub_sets->get_primary_key_column(), version_id);
    remove_query.remove();
}

bool SubscriptionStore::flush_changes()
{
    auto tr = m_db->start_write();
    return flush_changes_impl(*tr, CommitMode::Commit);
}

bool SubscriptionStore::flush_changes(Transaction& tr)
{
    return flush_changes_impl(tr, CommitMode::NoCommit);
}

bool SubscriptionStore::flush_changes_impl(Transaction& tr, CommitMode commit_mode)
{
    std::unique_lock<std::mutex> lk(m_cached_sub_sets_mutex);
    if (m_unpersisted_subscription_sets.empty()) {
        return false;
    }

    if (tr.get_transact_stage() != DB::transact_Writing) {
        tr.promote_to_write();
        commit_mode = CommitMode::ContinueAsRead;
    }

    for (auto&& [version, sub_set] : m_unpersisted_subscription_sets) {
        auto table = tr.get_table(m_sub_set_table);
        bool did_create = false;
        auto obj = table->create_object_with_primary_key(Mixed{sub_set.version()}, &did_create);

        auto state = sub_set.state();
        auto snapshot_version = sub_set.snapshot_version();
        if (state == SubscriptionSet::State::PendingFlush) {
            state = SubscriptionSet::State::Pending;
        }
        if (did_create) {
            snapshot_version = tr.get_version();
            obj.set(m_sub_set_snapshot_version, static_cast<int64_t>(tr.get_version()));

            auto obj_sub_list = obj.get_linklist(m_sub_set_subscriptions);
            obj_sub_list.clear();
            for (const auto& sub : sub_set) {
                auto new_sub =
                    obj_sub_list.create_and_insert_linked_object(obj_sub_list.is_empty() ? 0 : obj_sub_list.size());
                new_sub.set(m_sub_id, sub.id);
                new_sub.set(m_sub_created_at, sub.created_at);
                new_sub.set(m_sub_updated_at, sub.updated_at);
                if (sub.name) {
                    new_sub.set(m_sub_name, StringData(*sub.name));
                }
                new_sub.set(m_sub_object_class_name, StringData(sub.object_class_name));
                new_sub.set(m_sub_query_str, StringData(sub.query_string));
            }
        }

        obj.set(m_sub_set_state, state_to_storage(state));
        if (!sub_set.error_str()) {
            obj.set(m_sub_set_error_str, sub_set.error_str());
        }

        if (m_logger->would_log(util::Logger::Level::trace)) {
            m_logger->trace("Flushing subscription set version: %1, old_state: %2, new_state: %3, snapshot_version: "
                            "%4, json: \"%5\"",
                            version, sub_set.state(), state, snapshot_version, obj.to_string());
        }
        sub_set.m_state = state;
        sub_set.m_snapshot_version = snapshot_version;
    }
    supercede_prior_to(tr, m_min_outstanding_version);

    switch(commit_mode) {
        case CommitMode::Commit:
            tr.commit();
            break;
        case CommitMode::ContinueAsRead:
            tr.commit_and_continue_as_read();
            break;
        case CommitMode::NoCommit:
            break;
    }

    auto to_notify = std::move(m_unpersisted_subscription_sets);
    lk.unlock();

    for (auto&& [version, sub_set] : to_notify) {
        process_notifications_for(sub_set);
    }

    return true;
}

void SubscriptionStore::process_notifications_for(const SubscriptionSet& mut_sub_set)
{
    auto new_state = mut_sub_set.state();
    auto my_version = mut_sub_set.version();

    std::list<SubscriptionStore::NotificationRequest> to_finish;
    std::unique_lock<std::mutex> lk(m_pending_notifications_mutex);
    m_pending_notifications_cv.wait(lk, [&] {
        return m_outstanding_requests == 0;
    });

    for (auto it = m_pending_notifications.begin(); it != m_pending_notifications.end();) {
        if ((it->version == my_version && (new_state == SubscriptionSet::State::Error ||
                                           state_to_order(new_state) >= state_to_order(it->notify_when))) ||
            (new_state == SubscriptionSet::State::Complete && it->version < my_version)) {
            m_logger->trace("Found notification to finish for version %1. target_state: %2 reached_state: %3",
                            my_version, new_state, it->notify_when);
            to_finish.splice(to_finish.end(), m_pending_notifications, it++);
        }
        else {
            ++it;
        }
    }

    if (new_state == SubscriptionSet::State::Complete) {
        m_min_outstanding_version = my_version;
    }

    lk.unlock();

    for (auto& req : to_finish) {
        if (new_state == SubscriptionSet::State::Error && req.version == my_version) {
            req.promise.set_error({ErrorCodes::SubscriptionFailed, std::string_view(mut_sub_set.error_str())});
        }
        else if (req.version < my_version) {
            req.promise.emplace_value(SubscriptionSet::State::Superseded);
        }
        else {
            req.promise.emplace_value(new_state);
        }
    }
}

void SubscriptionStore::cache_mutable_subscription_set(const MutableSubscriptionSet& mut_sub_set)
{
    std::unique_lock<std::mutex> lk(m_cached_sub_sets_mutex);
    REALM_ASSERT(m_mutable_subcription_outstanding);
    if (mut_sub_set.state() == SubscriptionSet::State::Complete ||
        mut_sub_set.state() == SubscriptionSet::State::AwaitingMark) {
        REALM_ASSERT(mut_sub_set.version() >= m_min_outstanding_version);
        REALM_ASSERT(mut_sub_set.version() >= m_active_version);
        m_min_outstanding_version = mut_sub_set.version();
        m_active_version = mut_sub_set.version();
        m_awaiting_mark_version = mut_sub_set.state() == SubscriptionSet::State::Complete
                                      ? SubscriptionSet::EmptyVersion
                                      : mut_sub_set.version();
    }

    m_latest_version = std::max(m_latest_version, mut_sub_set.version());

    if (auto it = m_unpersisted_subscription_sets.find(mut_sub_set.version());
        it != m_unpersisted_subscription_sets.end()) {
        it->second = mut_sub_set;
    }
    else {
        bool inserted;
        std::tie(it, inserted) = m_unpersisted_subscription_sets.insert({mut_sub_set.version(), mut_sub_set});
        REALM_ASSERT(inserted);
    }

    m_logger->trace("Caching subscription set for version %1, state: %2", mut_sub_set.version(), mut_sub_set.state());

    m_mutable_subcription_outstanding = false;
    lk.unlock();
    m_outstanding_sub_set_committed_cv.notify_one();
    process_notifications_for(mut_sub_set);

    if (mut_sub_set.state() == SubscriptionSet::State::PendingFlush) {
        m_on_new_subscription_set(mut_sub_set.version());
    }
}

void SubscriptionStore::release_outstanding_mutable_sub_set()
{
    std::lock_guard<std::mutex> lk(m_cached_sub_sets_mutex);
    REALM_ASSERT(m_mutable_subcription_outstanding);
    m_mutable_subcription_outstanding = false;
    m_outstanding_sub_set_committed_cv.notify_one();
}

void SubscriptionStore::supercede_all_except(MutableSubscriptionSet& mut_sub)
{
    auto version_to_keep = mut_sub.version();
    auto tr = m_db->start_write();
    supercede_prior_to(*tr, version_to_keep);

    std::list<SubscriptionStore::NotificationRequest> to_finish;
    std::unique_lock<std::mutex> lk(m_pending_notifications_mutex);
    m_pending_notifications_cv.wait(lk, [&] {
        return m_outstanding_requests == 0;
    });
    for (auto it = m_pending_notifications.begin(); it != m_pending_notifications.end();) {
        if (it->version != version_to_keep) {
            to_finish.splice(to_finish.end(), m_pending_notifications, it++);
        }
        else {
            ++it;
        }
    }

    REALM_ASSERT_EX(version_to_keep >= m_min_outstanding_version, version_to_keep, m_min_outstanding_version);
    m_min_outstanding_version = version_to_keep;

    lk.unlock();

    for (auto& req : to_finish) {
        req.promise.emplace_value(SubscriptionSet::State::Superseded);
    }
}

MutableSubscriptionSet SubscriptionStore::make_mutable_copy(const SubscriptionSet& set)
{
    std::unique_lock<std::mutex> lk(m_cached_sub_sets_mutex);
    m_outstanding_sub_set_committed_cv.wait(lk, [&] {
        return m_mutable_subcription_outstanding == false;
    });
    m_mutable_subcription_outstanding = true;
    auto mut_guard = util::make_scope_exit([&]() noexcept {
        m_mutable_subcription_outstanding = false;
    });

    auto my_version = m_next_new_subscription_set_version++;
    auto ret = MutableSubscriptionSet(weak_from_this(), my_version, SubscriptionSet::State::Uncommitted, {},
                                      s_empty_snapshot_version, set.m_subs);
    mut_guard.release();
    return ret;
}

bool SubscriptionStore::would_refresh(DB::version_type version) const noexcept
{
    std::lock_guard<std::mutex> lk(m_cached_sub_sets_mutex);
    return version < m_db->get_version_of_latest_snapshot() || !m_unpersisted_subscription_sets.empty();
}

} // namespace realm::sync
