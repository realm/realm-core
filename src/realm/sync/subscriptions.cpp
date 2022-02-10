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

#include "realm/group.hpp"
#include "realm/keys.hpp"
#include "realm/list.hpp"
#include "realm/sort_descriptor.hpp"
#include "realm/table.hpp"
#include "realm/table_view.hpp"
#include "realm/util/flat_map.hpp"

namespace realm::sync {
namespace {
// Schema version history:
//   v2: Initial public beta.

constexpr static int c_flx_schema_version = 2;
constexpr static std::string_view c_flx_metadata_table("flx_metadata");
constexpr static std::string_view c_flx_subscription_sets_table("flx_subscription_sets");
constexpr static std::string_view c_flx_subscriptions_table("flx_subscriptions");

constexpr static std::string_view c_flx_meta_schema_version_field("schema_version");
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

} // namespace

Subscription::Subscription(const SubscriptionStore* parent, Obj obj)
    : m_id(obj.get<ObjectId>(parent->m_sub_keys->id))
    , m_created_at(obj.get<Timestamp>(parent->m_sub_keys->created_at))
    , m_updated_at(obj.get<Timestamp>(parent->m_sub_keys->updated_at))
    , m_name(obj.is_null(parent->m_sub_keys->name) ? OptionalString(util::none)
                                                   : OptionalString{obj.get<String>(parent->m_sub_keys->name)})
    , m_object_class_name(obj.get<String>(parent->m_sub_keys->object_class_name))
    , m_query_string(obj.get<String>(parent->m_sub_keys->query_str))
{
}

Subscription::Subscription(util::Optional<std::string> name, std::string object_class_name, std::string query_str)
    : m_id(ObjectId::gen())
    , m_created_at(std::chrono::system_clock::now())
    , m_updated_at(m_created_at)
    , m_name(std::move(name))
    , m_object_class_name(std::move(object_class_name))
    , m_query_string(std::move(query_str))
{
}

ObjectId Subscription::id() const
{
    return m_id;
}

Timestamp Subscription::created_at() const
{
    return m_created_at;
}

Timestamp Subscription::updated_at() const
{
    return m_updated_at;
}

bool Subscription::has_name() const
{
    return static_cast<bool>(m_name);
}

std::string_view Subscription::name() const
{
    if (!m_name) {
        return std::string_view{};
    }
    return m_name.value();
}

std::string_view Subscription::object_class_name() const
{
    return m_object_class_name;
}

std::string_view Subscription::query_string() const
{
    return m_query_string;
}

SubscriptionSet::SubscriptionSet(std::weak_ptr<const SubscriptionStore> mgr, TransactionRef tr, Obj obj)
    : m_mgr(mgr)
{
    if (obj.is_valid()) {
        load_from_database(std::move(tr), std::move(obj));
    }
}

SubscriptionSet::SubscriptionSet(std::weak_ptr<const SubscriptionStore> mgr, int64_t version, SupersededTag)
    : m_mgr(mgr)
    , m_version(version)
    , m_state(State::Superseded)
{
}

void SubscriptionSet::load_from_database(TransactionRef tr, Obj obj)
{
    auto mgr = get_flx_subscription_store(); // Throws

    m_cur_version = tr->get_version();
    m_version = obj.get_primary_key().get_int();
    m_state = static_cast<State>(obj.get<int64_t>(mgr->m_sub_set_keys->state));
    m_error_str = obj.get<String>(mgr->m_sub_set_keys->error_str);
    m_snapshot_version = static_cast<DB::version_type>(obj.get<int64_t>(mgr->m_sub_set_keys->snapshot_version));
    auto sub_list = obj.get_linklist(mgr->m_sub_set_keys->subscriptions);
    m_subs.clear();
    for (size_t idx = 0; idx < sub_list.size(); ++idx) {
        m_subs.push_back(Subscription(mgr.get(), sub_list.get_object(idx)));
    }
}

std::shared_ptr<const SubscriptionStore> SubscriptionSet::get_flx_subscription_store() const
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

Subscription SubscriptionSet::at(size_t index) const
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

SubscriptionSet::const_iterator SubscriptionSet::find(StringData name) const
{
    return std::find_if(begin(), end(), [&](const Subscription& sub) {
        return sub.name() == name;
    });
}

SubscriptionSet::const_iterator SubscriptionSet::find(const Query& query) const
{
    const auto query_desc = query.get_description();
    const auto table_name = Group::table_name_to_class_name(query.get_table()->get_name());
    return std::find_if(begin(), end(), [&](const Subscription& sub) {
        return sub.object_class_name() == table_name && sub.query_string() == query_desc;
    });
}

MutableSubscriptionSet::MutableSubscriptionSet(std::weak_ptr<const SubscriptionStore> mgr, TransactionRef tr, Obj obj)
    : SubscriptionSet(mgr, tr, obj)
    , m_tr(std::move(tr))
    , m_obj(std::move(obj))
    , m_old_state(state())
{
}

MutableSubscriptionSet::iterator MutableSubscriptionSet::begin()
{
    return m_subs.begin();
}

MutableSubscriptionSet::iterator MutableSubscriptionSet::end()
{
    return m_subs.end();
}

MutableSubscriptionSet::iterator MutableSubscriptionSet::erase(const_iterator it)
{
    return m_subs.erase(it);
}

void MutableSubscriptionSet::clear()
{
    m_subs.clear();
}

void MutableSubscriptionSet::insert_sub(const Subscription& sub)
{
    m_subs.push_back(sub);
}

std::pair<SubscriptionSet::iterator, bool>
MutableSubscriptionSet::insert_or_assign_impl(iterator it, util::Optional<std::string> name,
                                              std::string object_class_name, std::string query_str)
{
    if (it != end()) {
        it->m_object_class_name = std::move(object_class_name);
        it->m_query_string = std::move(query_str);
        it->m_updated_at = Timestamp{std::chrono::system_clock::now()};

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
        return (sub.has_name() && sub.name() == name);
    });

    return insert_or_assign_impl(it, std::string{name}, std::move(table_name), std::move(query_str));
}

std::pair<SubscriptionSet::iterator, bool> MutableSubscriptionSet::insert_or_assign(const Query& query)
{
    auto table_name = Group::table_name_to_class_name(query.get_table()->get_name());
    auto query_str = query.get_description();
    auto it = std::find_if(begin(), end(), [&](const Subscription& sub) {
        return (sub.name().empty() && sub.object_class_name() == table_name && sub.query_string() == query_str);
    });

    return insert_or_assign_impl(it, util::none, std::move(table_name), std::move(query_str));
}

void MutableSubscriptionSet::update_state(State new_state, util::Optional<std::string_view> error_str)
{
    auto old_state = state();
    switch (new_state) {
        case State::Uncommitted:
            throw std::logic_error("cannot set subscription set state to uncommitted");

        case State::Error:
            if (old_state != State::Bootstrapping && old_state != State::Pending) {
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
            if (error_str) {
                throw std::logic_error(
                    "Cannot supply an error message for a subscription set when state is not Error");
            }
            m_state = new_state;
            break;
        case State::Complete: {
            if (error_str) {
                throw std::logic_error(
                    "Cannot supply an error message for a subscription set when state is not Error");
            }
            auto mgr = get_flx_subscription_store(); // Throws
            m_state = new_state;
            mgr->supercede_prior_to(m_tr, version());
            break;
        }
        case State::Superseded:
            throw std::logic_error("Cannot set a subscription to the superseded state");
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
    auto refreshed_self = mgr->get_by_version(version());
    m_state = refreshed_self.m_state;
    m_error_str = refreshed_self.m_error_str;
    m_cur_version = refreshed_self.m_cur_version;
    *this = mgr->get_by_version(version());
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
    StringData err_str = error_str();

    // If there have been writes to the database since this SubscriptionSet was created, we need to fetch
    // the updated version from the DB to know the true current state and maybe return a ready future.
    if (m_cur_version < mgr->m_db->get_version_of_latest_snapshot()) {
        auto refreshed_self = mgr->get_by_version(version());
        cur_state = refreshed_self.state();
        err_str = refreshed_self.error_str();
    }
    // If we've already reached the desired state, or if the subscription is in an error state,
    // we can return a ready future immediately.
    if (cur_state == State::Error) {
        return util::Future<State>::make_ready(Status{ErrorCodes::RuntimeError, err_str});
    }
    else if (cur_state >= notify_when) {
        return util::Future<State>::make_ready(cur_state);
    }

    // Otherwise put in a new request to be filled in by process_notifications().
    lk.lock();

    // Otherwise, make a promise/future pair and add it to the list of pending notifications.
    auto [promise, future] = util::make_promise_future<State>();
    mgr->m_pending_notifications.emplace_back(version(), std::move(promise), notify_when);
    return std::move(future);
}

void MutableSubscriptionSet::process_notifications()
{
    auto mgr = get_flx_subscription_store(); // Throws
    auto new_state = state();
    auto my_version = version();

    std::list<SubscriptionStore::NotificationRequest> to_finish;
    std::unique_lock<std::mutex> lk(mgr->m_pending_notifications_mutex);
    mgr->m_pending_notifications_cv.wait(lk, [&] {
        return mgr->m_outstanding_requests == 0;
    });
    for (auto it = mgr->m_pending_notifications.begin(); it != mgr->m_pending_notifications.end();) {
        if ((it->version == my_version && (new_state == State::Error || new_state >= it->notify_when)) ||
            (new_state == State::Complete && it->version < my_version)) {
            to_finish.splice(to_finish.end(), mgr->m_pending_notifications, it++);
        }
        else {
            ++it;
        }
    }

    if (new_state == State::Complete) {
        mgr->m_min_outstanding_version = my_version;
    }

    lk.unlock();

    for (auto& req : to_finish) {
        if (new_state == State::Error && req.version == my_version) {
            req.promise.set_error({ErrorCodes::RuntimeError, error_str()});
        }
        else if (req.version < my_version) {
            req.promise.emplace_value(State::Superseded);
        }
        else {
            req.promise.emplace_value(new_state);
        }
    }
}

SubscriptionSet MutableSubscriptionSet::commit() &&
{
    if (m_tr->get_transact_stage() != DB::transact_Writing) {
        throw std::logic_error("SubscriptionSet is not in a commitable state");
    }
    auto mgr = get_flx_subscription_store(); // Throws

    if (m_old_state == State::Uncommitted) {
        if (m_state == State::Uncommitted) {
            m_state = State::Pending;
        }
        m_obj.set(mgr->m_sub_set_keys->snapshot_version, static_cast<int64_t>(m_tr->get_version()));

        auto obj_sub_list = m_obj.get_linklist(mgr->m_sub_set_keys->subscriptions);
        obj_sub_list.clear();
        for (const auto& sub : m_subs) {
            auto new_sub =
                obj_sub_list.create_and_insert_linked_object(obj_sub_list.is_empty() ? 0 : obj_sub_list.size());
            new_sub.set(mgr->m_sub_keys->id, sub.id());
            new_sub.set(mgr->m_sub_keys->created_at, sub.created_at());
            new_sub.set(mgr->m_sub_keys->updated_at, sub.updated_at());
            if (sub.m_name) {
                new_sub.set(mgr->m_sub_keys->name, StringData(sub.name()));
            }
            new_sub.set(mgr->m_sub_keys->object_class_name, StringData(sub.object_class_name()));
            new_sub.set(mgr->m_sub_keys->query_str, StringData(sub.query_string()));
        }
    }
    m_obj.set(mgr->m_sub_set_keys->state, static_cast<int64_t>(m_state));
    if (!m_error_str.empty()) {
        m_obj.set(mgr->m_sub_set_keys->error_str, StringData(m_error_str));
    }

    const auto flx_version = version();
    m_tr->commit_and_continue_as_read();

    process_notifications();

    if (state() == State::Pending) {
        mgr->m_on_new_subscription_set(flx_version);
    }

    return mgr->get_by_version_impl(flx_version, m_tr->get_version_of_current_transaction());
}

std::string SubscriptionSet::to_ext_json() const
{
    if (m_subs.empty()) {
        return "{}";
    }

    util::FlatMap<std::string, std::vector<std::string>> table_to_query;
    for (const auto& sub : *this) {
        std::string table_name(sub.object_class_name());
        auto& queries_for_table = table_to_query.at(table_name);
        auto query_it = std::find(queries_for_table.begin(), queries_for_table.end(), sub.query_string());
        if (query_it != queries_for_table.end()) {
            continue;
        }
        queries_for_table.emplace_back(sub.query_string());
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

namespace {
class SubscriptionStoreInit : public SubscriptionStore {
public:
    explicit SubscriptionStoreInit(DBRef db, util::UniqueFunction<void(int64_t)> on_new_subscription_set)
        : SubscriptionStore(std::move(db), std::move(on_new_subscription_set))
    {
    }
};
} // namespace

SubscriptionStoreRef SubscriptionStore::create(DBRef db, util::UniqueFunction<void(int64_t)> on_new_subscription_set)
{
    return std::make_shared<SubscriptionStoreInit>(std::move(db), std::move(on_new_subscription_set));
}

SubscriptionStore::SubscriptionStore(DBRef db, util::UniqueFunction<void(int64_t)> on_new_subscription_set)
    : m_db(std::move(db))
    , m_on_new_subscription_set(std::move(on_new_subscription_set))
    , m_sub_set_keys(std::make_unique<SubscriptionSetKeys>())
    , m_sub_keys(std::make_unique<SubscriptionKeys>())
{
    auto tr = m_db->start_read();

    auto schema_metadata_key = tr->find_table(c_flx_metadata_table);
    auto create_schema_if_needed = [&] {
        tr->promote_to_write();

        if (tr->find_table(c_flx_metadata_table)) {
            return false;
        }

        auto schema_metadata = tr->add_table(c_flx_metadata_table);
        auto version_col = schema_metadata->add_column(type_Int, c_flx_meta_schema_version_field);
        schema_metadata->create_object().set(version_col, int64_t(c_flx_schema_version));

        auto sub_sets_table =
            tr->add_table_with_primary_key(c_flx_subscription_sets_table, type_Int, c_flx_sub_sets_version_field);
        auto subs_table = tr->add_embedded_table(c_flx_subscriptions_table);
        m_sub_keys->table = subs_table->get_key();
        m_sub_keys->id = subs_table->add_column(type_ObjectId, c_flx_sub_id_field);
        m_sub_keys->created_at = subs_table->add_column(type_Timestamp, c_flx_sub_created_at_field);
        m_sub_keys->updated_at = subs_table->add_column(type_Timestamp, c_flx_sub_updated_at_field);
        m_sub_keys->name = subs_table->add_column(type_String, c_flx_sub_name_field, true);
        m_sub_keys->object_class_name = subs_table->add_column(type_String, c_flx_sub_object_class_field);
        m_sub_keys->query_str = subs_table->add_column(type_String, c_flx_sub_query_str_field);

        m_sub_set_keys->table = sub_sets_table->get_key();
        m_sub_set_keys->state = sub_sets_table->add_column(type_Int, c_flx_sub_sets_state_field);
        m_sub_set_keys->snapshot_version =
            sub_sets_table->add_column(type_Int, c_flx_sub_sets_snapshot_version_field);
        m_sub_set_keys->error_str = sub_sets_table->add_column(type_String, c_flx_sub_sets_error_str_field, true);
        m_sub_set_keys->subscriptions =
            sub_sets_table->add_column_list(*subs_table, c_flx_sub_sets_subscriptions_field);
        tr->commit_and_continue_as_read();
        return true;
    };

    if (schema_metadata_key || !create_schema_if_needed()) {
        auto lookup_and_validate_column = [&](TableRef& table, StringData col_name, DataType col_type) -> ColKey {
            auto ret = table->get_column_key(col_name);
            if (!ret) {
                throw std::runtime_error(util::format("Flexible Sync metadata missing %1 column in %2 table",
                                                      col_name, table->get_name()));
            }
            auto found_col_type = table->get_column_type(ret);
            if (found_col_type != col_type) {
                throw std::runtime_error(util::format(
                    "column %1 in Flexible Sync metadata table %2 is the wrong type", col_name, table->get_name()));
            }
            return ret;
        };

        auto schema_metadata = tr->get_table(schema_metadata_key);
        auto version_obj = schema_metadata->get_object(0);
        auto version = version_obj.get<int64_t>(
            lookup_and_validate_column(schema_metadata, c_flx_meta_schema_version_field, type_Int));
        if (version != c_flx_schema_version) {
            throw std::runtime_error("Invalid schema version for flexible sync metadata");
        }

        m_sub_set_keys->table = tr->find_table(c_flx_subscription_sets_table);
        auto sub_sets = tr->get_table(m_sub_set_keys->table);
        m_sub_set_keys->state = lookup_and_validate_column(sub_sets, c_flx_sub_sets_state_field, type_Int);
        m_sub_set_keys->error_str = lookup_and_validate_column(sub_sets, c_flx_sub_sets_error_str_field, type_String);
        m_sub_set_keys->snapshot_version =
            lookup_and_validate_column(sub_sets, c_flx_sub_sets_snapshot_version_field, type_Int);
        m_sub_set_keys->subscriptions =
            lookup_and_validate_column(sub_sets, c_flx_sub_sets_subscriptions_field, type_LinkList);
        if (!m_sub_set_keys->subscriptions) {
            throw std::runtime_error("Flexible Sync metadata missing subscriptions table");
        }

        auto subs = sub_sets->get_opposite_table(m_sub_set_keys->subscriptions);
        if (!subs->is_embedded()) {
            throw std::runtime_error("Flexible Sync subscriptions table should be an embedded object");
        }
        m_sub_keys->table = subs->get_key();
        m_sub_keys->id = lookup_and_validate_column(subs, c_flx_sub_id_field, type_ObjectId);
        m_sub_keys->created_at = lookup_and_validate_column(subs, c_flx_sub_created_at_field, type_Timestamp);
        m_sub_keys->updated_at = lookup_and_validate_column(subs, c_flx_sub_updated_at_field, type_Timestamp);
        m_sub_keys->query_str = lookup_and_validate_column(subs, c_flx_sub_query_str_field, type_String);
        m_sub_keys->object_class_name = lookup_and_validate_column(subs, c_flx_sub_object_class_field, type_String);
        m_sub_keys->name = lookup_and_validate_column(subs, c_flx_sub_name_field, type_String);
    }

    // There should always be at least one subscription set so that the user can always wait for synchronizationon
    // on the result of get_latest().
    if (auto sub_sets = tr->get_table(m_sub_set_keys->table); sub_sets->is_empty()) {
        tr->promote_to_write();
        auto zero_sub = sub_sets->create_object_with_primary_key(Mixed{int64_t(0)});
        zero_sub.set(m_sub_set_keys->state, static_cast<int64_t>(SubscriptionSet::State::Pending));
        zero_sub.set(m_sub_set_keys->snapshot_version, tr->get_version());
        tr->commit();
    }
}

SubscriptionSet SubscriptionStore::get_latest() const
{
    auto tr = m_db->start_frozen();
    auto sub_sets = tr->get_table(m_sub_set_keys->table);
    if (sub_sets->is_empty()) {
        return SubscriptionSet(weak_from_this(), std::move(tr), Obj{});
    }
    auto latest_id = sub_sets->maximum_int(sub_sets->get_primary_key_column());
    auto latest_obj = sub_sets->get_object_with_primary_key(Mixed{latest_id});

    return SubscriptionSet(weak_from_this(), std::move(tr), std::move(latest_obj));
}

SubscriptionSet SubscriptionStore::get_active() const
{
    auto tr = m_db->start_frozen();
    auto sub_sets = tr->get_table(m_sub_set_keys->table);
    if (sub_sets->is_empty()) {
        return SubscriptionSet(weak_from_this(), std::move(tr), Obj{});
    }

    DescriptorOrdering descriptor_ordering;
    descriptor_ordering.append_sort(SortDescriptor{{{sub_sets->get_primary_key_column()}}, {false}});
    descriptor_ordering.append_limit(LimitDescriptor{1});
    auto res = sub_sets->where()
                   .equal(m_sub_set_keys->state, static_cast<int64_t>(SubscriptionSet::State::Complete))
                   .find_all(descriptor_ordering);

    if (res.is_empty()) {
        return SubscriptionSet(weak_from_this(), std::move(tr), Obj{});
    }
    return SubscriptionSet(weak_from_this(), std::move(tr), res.get_object(0));
}

std::pair<int64_t, int64_t> SubscriptionStore::get_active_and_latest_versions() const
{
    auto tr = m_db->start_read();
    auto sub_sets = tr->get_table(m_sub_set_keys->table);
    if (sub_sets->is_empty()) {
        return {0, 0};
    }

    auto latest_id = sub_sets->maximum_int(sub_sets->get_primary_key_column());
    DescriptorOrdering descriptor_ordering;
    descriptor_ordering.append_sort(SortDescriptor{{{sub_sets->get_primary_key_column()}}, {false}});
    descriptor_ordering.append_limit(LimitDescriptor{1});
    auto res = sub_sets->where()
                   .equal(m_sub_set_keys->state, static_cast<int64_t>(SubscriptionSet::State::Complete))
                   .find_all(descriptor_ordering);

    if (res.is_empty()) {
        return {0, latest_id};
    }

    auto active_id = res.get_object(0).get_primary_key();
    return {active_id.get_int(), latest_id};
}

util::Optional<SubscriptionStore::PendingSubscription>
SubscriptionStore::get_next_pending_version(int64_t last_query_version, DB::version_type after_client_version) const
{
    auto tr = m_db->start_read();
    auto sub_sets = tr->get_table(m_sub_set_keys->table);
    if (sub_sets->is_empty()) {
        return util::none;
    }

    DescriptorOrdering descriptor_ordering;
    descriptor_ordering.append_sort(SortDescriptor{{{sub_sets->get_primary_key_column()}}, {true}});
    auto res = sub_sets->where()
                   .greater(sub_sets->get_primary_key_column(), last_query_version)
                   .equal(m_sub_set_keys->state, static_cast<int64_t>(SubscriptionSet::State::Pending))
                   .greater_equal(m_sub_set_keys->snapshot_version, static_cast<int64_t>(after_client_version))
                   .find_all(descriptor_ordering);

    if (res.is_empty()) {
        return util::none;
    }

    auto obj = res.get_object(0);
    auto query_version = obj.get_primary_key().get_int();
    auto snapshot_version = obj.get<int64_t>(m_sub_set_keys->snapshot_version);
    return PendingSubscription{query_version, static_cast<DB::version_type>(snapshot_version)};
}

MutableSubscriptionSet SubscriptionStore::get_mutable_by_version(int64_t version_id)
{
    auto tr = m_db->start_write();
    auto sub_sets = tr->get_table(m_sub_set_keys->table);
    return MutableSubscriptionSet(weak_from_this(), std::move(tr),
                                  sub_sets->get_object_with_primary_key(Mixed{version_id}));
}

SubscriptionSet SubscriptionStore::get_by_version(int64_t version_id) const
{
    return get_by_version_impl(version_id, util::none);
}

SubscriptionSet SubscriptionStore::get_by_version_impl(int64_t version_id,
                                                       util::Optional<DB::VersionID> db_version) const
{
    auto tr = m_db->start_frozen(db_version.value_or(VersionID{}));
    auto sub_sets = tr->get_table(m_sub_set_keys->table);
    try {
        return SubscriptionSet(weak_from_this(), std::move(tr),
                               sub_sets->get_object_with_primary_key(Mixed{version_id}));
    }
    catch (const KeyNotFound&) {
        std::lock_guard<std::mutex> lk(m_pending_notifications_mutex);
        if (version_id < m_min_outstanding_version) {
            return SubscriptionSet(weak_from_this(), version_id, SubscriptionSet::SupersededTag{});
        }
        throw;
    }
}

void SubscriptionStore::supercede_prior_to(TransactionRef tr, int64_t version_id) const
{
    auto sub_sets = tr->get_table(m_sub_set_keys->table);
    Query remove_query(sub_sets);
    remove_query.less(sub_sets->get_primary_key_column(), version_id);
    remove_query.remove();
}

MutableSubscriptionSet SubscriptionStore::make_mutable_copy(const SubscriptionSet& set) const
{
    auto new_tr = m_db->start_write();

    auto sub_sets = new_tr->get_table(m_sub_set_keys->table);
    auto new_pk = sub_sets->maximum_int(sub_sets->get_primary_key_column()) + 1;

    MutableSubscriptionSet new_set_obj(weak_from_this(), std::move(new_tr),
                                       sub_sets->create_object_with_primary_key(Mixed{new_pk}));
    for (const auto& sub : set) {
        new_set_obj.insert_sub(sub);
    }

    return new_set_obj;
}

} // namespace realm::sync
