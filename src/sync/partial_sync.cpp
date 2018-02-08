////////////////////////////////////////////////////////////////////////////
//
// Copyright 2017 Realm Inc.
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

#include "sync/partial_sync.hpp"

#include "impl/collection_notifier.hpp"
#include "impl/notification_wrapper.hpp"
#include "impl/object_accessor_impl.hpp"
#include "impl/realm_coordinator.hpp"
#include "object_schema.hpp"
#include "results.hpp"
#include "shared_realm.hpp"
#include "sync/impl/work_queue.hpp"
#include "sync/subscription_state.hpp"
#include "sync/sync_config.hpp"

#include <realm/util/scope_exit.hpp>

namespace {
constexpr const char* result_sets_type_name = "__ResultSets";
}

namespace realm {

namespace _impl {

void initialize_schema(Group& group)
{
    std::string result_sets_table_name = ObjectStore::table_name_for_object_type(result_sets_type_name);
    if (group.has_table(result_sets_table_name))
        return;

    TableRef table = sync::create_table(group, result_sets_table_name);
    size_t indexable_column_idx = table->add_column(type_String, "name"); // Custom property
    table->add_search_index(indexable_column_idx);
    table->add_column(type_String, "query");
    table->add_column(type_String, "matches_property");
    table->add_column(type_Int, "status");
    table->add_column(type_String, "error_message");
    table->add_column(type_Int, "query_parse_counter");
}

} // namespace _impl

namespace partial_sync {

namespace {

void update_schema(Group& group, Property matches_property)
{
    Schema current_schema;
    std::string table_name = ObjectStore::table_name_for_object_type(result_sets_type_name);
    if (group.has_table(table_name))
        current_schema = {ObjectSchema{group, result_sets_type_name}};

    Schema desired_schema({
        ObjectSchema(result_sets_type_name, {
            {"name", PropertyType::String, Property::IsPrimary{false}, Property::IsIndexed{true}},
            {"matches_property", PropertyType::String},
            {"query", PropertyType::String},
            {"status", PropertyType::Int},
            {"error_message", PropertyType::String},
            {"query_parse_counter", PropertyType::Int},
            std::move(matches_property)
        })
    });
    auto required_changes = current_schema.compare(desired_schema);
    if (!required_changes.empty())
        ObjectStore::apply_additive_changes(group, required_changes, true);
}

bool validate_existing_subscription(std::shared_ptr<Realm> realm,
                                    std::string const& name,
                                    std::string const& query,
                                    std::string const& matches_property,
                                    ObjectSchema const& result_sets_schema)
{
    TableRef table = ObjectStore::table_for_object_type(realm->read_group(), result_sets_type_name);
    size_t name_idx = result_sets_schema.property_for_name("name")->table_column;
    auto existing_row_ndx = table->find_first_string(name_idx, name);
    if (existing_row_ndx == npos)
        return false;

    Object existing_object(realm, result_sets_schema, table->get(existing_row_ndx));

    CppContext context;
    std::string existing_query = any_cast<std::string>(existing_object.get_property_value<util::Any>(context, "query"));
    if (existing_query != query)
        throw std::runtime_error("An existing subscription exists with the same name, but a different query.");

    std::string existing_matches_property = any_cast<std::string>(existing_object.get_property_value<util::Any>(context, "matches_property"));
    if (existing_matches_property != matches_property)
        throw std::runtime_error("An existing subscription exists with the same name, but a different result type.");

    return true;
}

void async_register_query(Realm& realm, std::string object_type, std::string query, std::string name,
                          std::function<void(std::exception_ptr)> callback)
{
    const auto& config = realm.config();
    auto& work_queue = _impl::RealmCoordinator::get_coordinator(config)->partial_sync_work_queue();
    work_queue.enqueue([object_type=std::move(object_type), query=std::move(query), name=std::move(name),
                        callback=std::move(callback), config] {
        auto realm = Realm::get_shared_realm(config);
        realm->begin_transaction();
        auto cleanup = util::make_scope_exit([&]() noexcept {
            if (realm->is_in_transaction())
                realm->cancel_transaction();
        });

        auto matches_property = std::string(object_type) + "_matches";

        update_schema(realm->read_group(), Property(matches_property, PropertyType::Object|PropertyType::Array, object_type));
        ObjectSchema result_sets_schema(realm->read_group(), result_sets_type_name);

        try {
            if (!validate_existing_subscription(realm, name, query,
                                                matches_property, result_sets_schema)) {
                CppContext context;
                auto object = Object::create<util::Any>(context, realm, result_sets_schema,
                                                        AnyDict{
                                                            {"matches_property", matches_property},
                                                            {"name", name},
                                                            {"query", query},
                                                            {"status", int64_t(0)},
                                                            {"error_message", std::string()},
                                                            {"query_parse_counter", int64_t(0)},
                                                        }, false);
            }
        } catch (...) {
            callback(std::current_exception());
            return;
        }

        callback(nullptr);
        realm->commit_transaction();
        realm->close();
    });
}

std::string default_name_for_query(const std::string& query, const std::string& object_type)
{
    return util::format("[%1] %2", object_type, query);
}

} // unnamed namespace

void register_query(std::shared_ptr<Realm> realm, const std::string &object_class, const std::string &query,
                    std::function<void (Results, std::exception_ptr)> callback)
{
    auto sync_config = realm->config().sync_config;
    if (!sync_config || !sync_config->is_partial)
        throw std::logic_error("A partial sync query can only be registered in a partially synced Realm");

    if (realm->schema().find(object_class) == realm->schema().end())
        throw std::logic_error("A partial sync query can only be registered for a type that exists in the Realm's schema");

    auto matches_property = object_class + "_matches";

    // The object schema must outlive `object` below.
    std::unique_ptr<ObjectSchema> result_sets_schema;
    Object raw_object;
    {
        realm->begin_transaction();
        auto cleanup = util::make_scope_exit([&]() noexcept {
            if (realm->is_in_transaction())
                realm->cancel_transaction();
        });

        update_schema(realm->read_group(),
                      Property(matches_property, PropertyType::Object|PropertyType::Array, object_class));

        result_sets_schema = std::make_unique<ObjectSchema>(realm->read_group(), result_sets_type_name);

        CppContext context;
        raw_object = Object::create<util::Any>(context, realm, *result_sets_schema,
                                               AnyDict{
                                                   {"name", query},
                                                   {"matches_property", matches_property},
                                                   {"query", query},
                                                   {"status", int64_t(0)},
                                                   {"error_message", std::string()},
                                                   {"query_parse_counter", int64_t(0)},
                                               }, false);

        realm->commit_transaction();
    }

    auto object = std::make_shared<_impl::NotificationWrapper<Object>>(std::move(raw_object));

    // Observe the new object and notify listener when the results are complete (status != 0).
    auto notification_callback = [object, matches_property,
                                  result_sets_schema=std::move(result_sets_schema),
                                  callback=std::move(callback)](CollectionChangeSet, std::exception_ptr error) mutable {
        if (error) {
            callback(Results(), error);
            object.reset();
            return;
        }

        CppContext context;
        auto status = any_cast<int64_t>(object->get_property_value<util::Any>(context, "status"));
        if (status == 0) {
            // Still computing...
            return;
        } else if (status == 1) {
            // Finished successfully.
            auto list = any_cast<List>(object->get_property_value<util::Any>(context, matches_property));
            callback(list.as_results(), nullptr);
        } else {
            // Finished with error.
            auto message = any_cast<std::string>(object->get_property_value<util::Any>(context, "error_message"));
            callback(Results(), std::make_exception_ptr(std::runtime_error(std::move(message))));
        }
        object.reset();
    };
    object->add_notification_callback(std::move(notification_callback));
}

struct Subscription::Notifier : public _impl::CollectionNotifier {
    Notifier(std::shared_ptr<Realm> realm)
    : _impl::CollectionNotifier(std::move(realm))
    , m_coordinator(_impl::RealmCoordinator::get_coordinator(get_realm()->config()).get())
    {
    }

    void release_data() noexcept override { }
    void run() override
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_has_results_to_deliver)
            m_changes.modify(0);
    }

    void deliver(SharedGroup&) override
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_error = m_pending_error;
        m_pending_error = nullptr;

        m_subscription_completed = true;
        m_has_results_to_deliver = false;
    }

    void finished_subscribing(std::exception_ptr error)
    {
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_pending_error = error;
            m_has_results_to_deliver = true;
        }

        // Trigger processing of change notifications.
        m_coordinator->wake_up_notifier_worker();
    }

    std::exception_ptr error() const
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_error;
    }

    bool subscription_completed() const
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_subscription_completed;
    }

private:
    void do_attach_to(SharedGroup&) override { }
    void do_detach_from(SharedGroup&) override { }

    void do_prepare_handover(SharedGroup&) override
    {
        add_changes(std::move(m_changes));
    }

    bool do_add_required_change_info(_impl::TransactionChangeInfo&) override { return false; }
    bool prepare_to_deliver() override { return m_has_results_to_deliver; }

    _impl::RealmCoordinator *m_coordinator;

    mutable std::mutex m_mutex;
    _impl::CollectionChangeBuilder m_changes;
    std::exception_ptr m_pending_error = nullptr;
    std::exception_ptr m_error = nullptr;
    bool m_has_results_to_deliver = false;
    bool m_subscription_completed = false;
};

Subscription subscribe(Results const& results, util::Optional<std::string> user_provided_name)
{
    auto realm = results.get_realm();

    auto sync_config = realm->config().sync_config;
    if (!sync_config || !sync_config->is_partial)
        throw std::logic_error("A partial sync query can only be registered in a partially synced Realm");

    auto query = results.get_query().get_description(); // Throws if the query cannot be serialized.
    std::string name = user_provided_name ? std::move(*user_provided_name) : default_name_for_query(query,results.get_object_type());

    Subscription subscription(name, results.get_object_type(), realm);
    std::weak_ptr<Subscription::Notifier> weak_notifier = subscription.m_notifier;
    async_register_query(*realm, results.get_object_type(), std::move(query), std::move(name),
                         [weak_notifier=std::move(weak_notifier)](std::exception_ptr error) {
        if (auto notifier = weak_notifier.lock()) {
            notifier->finished_subscribing(error);
        }
    });
    return subscription;
}

Subscription::Subscription(std::string name, std::string object_type, std::shared_ptr<Realm> realm)
: m_object_schema(realm->read_group(), result_sets_type_name)
{
    // FIXME: Why can't I do this in the initializer list?
    m_notifier = std::make_shared<Notifier>(realm);
    _impl::RealmCoordinator::register_notifier(m_notifier);

    auto matches_property = std::string(object_type) + "_matches";

    TableRef table = ObjectStore::table_for_object_type(realm->read_group(), result_sets_type_name);
    Query query = table->where();
    query.equal(m_object_schema.property_for_name("name")->table_column, name);
    query.equal(m_object_schema.property_for_name("matches_property")->table_column, matches_property);
    m_result_sets = Results(realm, std::move(query));
}

Subscription::~Subscription() = default;
Subscription::Subscription(Subscription&&) = default;
Subscription& Subscription::operator=(Subscription&&) = default;

SubscriptionNotificationToken Subscription::add_notification_callback(std::function<void ()> callback)
{
    auto result_sets_token = m_result_sets.add_notification_callback([callback] (CollectionChangeSet, std::exception_ptr) {
        callback();
    });
    NotificationToken registration_token(m_notifier, m_notifier->add_callback([callback] (CollectionChangeSet, std::exception_ptr) {
        callback();
    }));
    return SubscriptionNotificationToken{std::move(registration_token), std::move(result_sets_token)};
}

util::Optional<Object> Subscription::result_set_object() const
{
    if (m_notifier->subscription_completed()) {
        if (auto row = m_result_sets.first())
            return Object(m_result_sets.get_realm(), m_object_schema, *row);
    }

    return util::none;
}

SubscriptionState Subscription::state() const
{
    if (!m_notifier->subscription_completed())
        return SubscriptionState::Creating;

    if (m_notifier->error())
        return SubscriptionState::Error;

    if (auto object = result_set_object()) {
        CppContext context;
        auto value = any_cast<int64_t>(object->get_property_value<util::Any>(context, "status"));
        return (SubscriptionState)value;
    }

    // We may not have an object even if the subscription has completed if the completion callback fired
    // but the result sets callback is yet to fire.
    return SubscriptionState::Creating;
}

std::exception_ptr Subscription::error() const
{
    if (auto error = m_notifier->error())
        return error;

    if (auto object = result_set_object()) {
        CppContext context;
        auto message = any_cast<std::string>(object->get_property_value<util::Any>(context, "error_message"));
        if (message.size())
            return make_exception_ptr(std::runtime_error(message));
    }

    return nullptr;
}

Results Subscription::results() const
{
    auto object = result_set_object();
    REALM_ASSERT_RELEASE(object);

    CppContext context;
    auto matches_property = any_cast<std::string>(object->get_property_value<util::Any>(context, "matches_property"));
    auto list = any_cast<List>(object->get_property_value<util::Any>(context, matches_property));
    return list.as_results();
}

} // namespace partial_sync
} // namespace realm
