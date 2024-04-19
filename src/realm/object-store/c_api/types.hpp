#ifndef REALM_OBJECT_STORE_C_API_TYPES_HPP
#define REALM_OBJECT_STORE_C_API_TYPES_HPP

#include <realm.h>

#include <realm/util/to_string.hpp>

#include <realm/object-store/c_api/conversion.hpp>
#include <realm/object-store/c_api/error.hpp>
#include <realm/object-store/object.hpp>
#include <realm/object-store/object_accessor.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/shared_realm.hpp>
#include <realm/object-store/thread_safe_reference.hpp>
#include <realm/object-store/util/scheduler.hpp>

#if REALM_ENABLE_SYNC

#if REALM_APP_SERVICES
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/app_user.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#endif // REALM_APP_SERVICES

#include <realm/object-store/sync/impl/sync_client.hpp>
#include <realm/sync/binding_callback_thread_observer.hpp>
#include <realm/sync/socket_provider.hpp>
#include <realm/sync/subscriptions.hpp>
#endif // REALM_ENABLE_SYNC

#include <memory>
#include <stdexcept>
#include <string>

namespace realm::c_api {
class NotClonable : public RuntimeError {
public:
    NotClonable()
        : RuntimeError(ErrorCodes::NotCloneable, "Not clonable")
    {
    }
};

class CallbackFailed : public RuntimeError {
public:
    // SDK-provided opaque error value when error == RLM_ERR_CALLBACK with a callout to
    // realm_register_user_code_callback_error()
    void* user_code_error{nullptr};

    CallbackFailed()
        : RuntimeError(ErrorCodes::CallbackFailed, "User-provided callback failed")
    {
    }

    explicit CallbackFailed(void* error)
        : CallbackFailed()
    {
        user_code_error = error;
    }
};

struct WrapC {
    static constexpr uint64_t s_cookie_value = 0xdeadbeefdeadbeef;
    uint64_t cookie;
    WrapC()
        : cookie(s_cookie_value)
    {
    }
    virtual ~WrapC()
    {
        cookie = 0;
    }

    virtual WrapC* clone() const
    {
        throw NotClonable();
    }

    virtual bool is_frozen() const
    {
        return false;
    }

    virtual bool equals(const WrapC& other) const noexcept
    {
        return this == &other;
    }

    virtual realm_thread_safe_reference_t* get_thread_safe_reference() const
    {
        throw LogicError{ErrorCodes::IllegalOperation,
                         "Thread safe references cannot be created for this object type"};
    }
};

struct FreeUserdata {
    realm_free_userdata_func_t m_func;
    FreeUserdata(realm_free_userdata_func_t func = nullptr)
        : m_func(func)
    {
    }
    void operator()(void* ptr)
    {
        if (m_func) {
            (m_func)(ptr);
        }
    }
};

using UserdataPtr = std::unique_ptr<void, FreeUserdata>;
using SharedUserdata = std::shared_ptr<void>;

} // namespace realm::c_api

struct realm_async_error : realm::c_api::WrapC {
    realm::c_api::ErrorStorage error_storage;

    explicit realm_async_error(const realm::c_api::ErrorStorage& storage)
        : error_storage(storage)
    {
    }

    explicit realm_async_error(std::exception_ptr ep)
        : error_storage(std::move(ep))
    {
    }

    realm_async_error* clone() const override
    {
        return new realm_async_error(*this);
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_async_error_t*>(&other)) {
            return error_storage == ptr->error_storage;
        }
        return false;
    }
};

struct realm_thread_safe_reference : realm::c_api::WrapC {
    realm_thread_safe_reference(const realm_thread_safe_reference&) = delete;

protected:
    realm_thread_safe_reference() {}
};

struct realm_config : realm::c_api::WrapC, realm::RealmConfig {
    using RealmConfig::RealmConfig;
    std::map<void*, realm_free_userdata_func_t> free_functions;
    realm_config(const realm_config&) = delete;
    realm_config& operator=(const realm_config&) = delete;
    ~realm_config()
    {
        for (auto& f : free_functions) {
            f.second(f.first);
        }
    }
};

// LCOV_EXCL_START
struct realm_scheduler : realm::c_api::WrapC, std::shared_ptr<realm::util::Scheduler> {
    explicit realm_scheduler(std::shared_ptr<realm::util::Scheduler> ptr)
        : std::shared_ptr<realm::util::Scheduler>(std::move(ptr))
    {
    }

    realm_scheduler* clone() const
    {
        return new realm_scheduler{*this};
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_scheduler_t*>(&other)) {
            if (get() == ptr->get()) {
                return true;
            }
            if (get()->is_same_as(ptr->get())) {
                return true;
            }
        }
        return false;
    }
};
// LCOV_EXCL_STOP

struct realm_schema : realm::c_api::WrapC {
    std::unique_ptr<realm::Schema> owned;
    const realm::Schema* ptr = nullptr;

    realm_schema(std::unique_ptr<realm::Schema> o, const realm::Schema* ptr = nullptr)
        : owned(std::move(o))
        , ptr(ptr ? ptr : owned.get())
    {
    }

    explicit realm_schema(const realm::Schema* ptr)
        : ptr(ptr)
    {
    }

    realm_schema_t* clone() const override
    {
        auto o = std::make_unique<realm::Schema>(*ptr);
        return new realm_schema_t{std::move(o)};
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto other_ptr = dynamic_cast<const realm_schema_t*>(&other)) {
            return *ptr == *other_ptr->ptr;
        }
        return false;
    }
};

struct shared_realm : realm::c_api::WrapC, realm::SharedRealm {
    shared_realm(realm::SharedRealm rlm)
        : realm::SharedRealm{std::move(rlm)}
    {
    }

    shared_realm* clone() const override
    {
        return new shared_realm{*this};
    }

    bool is_frozen() const override
    {
        return get()->is_frozen();
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const shared_realm*>(&other)) {
            return get() == ptr->get();
        }
        return false;
    }

    struct thread_safe_reference : realm_thread_safe_reference, realm::ThreadSafeReference {
        thread_safe_reference(const realm::SharedRealm& rlm)
            : realm::ThreadSafeReference(rlm)
        {
        }

        thread_safe_reference(realm::ThreadSafeReference&& other)
            : realm::ThreadSafeReference(std::move(other))
        {
            REALM_ASSERT(this->is<realm::SharedRealm>());
        }
    };

    realm_thread_safe_reference_t* get_thread_safe_reference() const final
    {
        return new thread_safe_reference{*this};
    }
};

struct realm_object : realm::c_api::WrapC, realm::Object {
    explicit realm_object(realm::Object obj)
        : realm::Object(std::move(obj))
    {
    }

    realm_object* clone() const override
    {
        return new realm_object{*this};
    }

    bool is_frozen() const override
    {
        return realm::Object::is_frozen();
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_object_t*>(&other)) {
            auto a = get_obj();
            auto b = ptr->get_obj();
            return a.get_table() == b.get_table() && a.get_key() == b.get_key();
        }
        return false;
    }

    struct thread_safe_reference : realm_thread_safe_reference, realm::ThreadSafeReference {
        thread_safe_reference(const realm::Object& obj)
            : realm::ThreadSafeReference(obj)
        {
        }
    };

    realm_thread_safe_reference_t* get_thread_safe_reference() const final
    {
        return new thread_safe_reference{*this};
    }
};

struct realm_list : realm::c_api::WrapC, realm::List {
    explicit realm_list(List list)
        : List(std::move(list))
    {
    }

    realm_list* clone() const override
    {
        return new realm_list{*this};
    }

    bool is_frozen() const override
    {
        return List::is_frozen();
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_list_t*>(&other)) {
            return get_realm() == ptr->get_realm() && get_parent_table_key() == ptr->get_parent_table_key() &&
                   get_parent_column_key() == ptr->get_parent_column_key() &&
                   get_parent_object_key() == ptr->get_parent_object_key();
        }
        return false;
    }

    struct thread_safe_reference : realm_thread_safe_reference, realm::ThreadSafeReference {
        thread_safe_reference(const List& list)
            : realm::ThreadSafeReference(list)
        {
        }
    };

    realm_thread_safe_reference_t* get_thread_safe_reference() const final
    {
        return new thread_safe_reference{*this};
    }
};

struct realm_set : realm::c_api::WrapC, realm::object_store::Set {
    explicit realm_set(Set set)
        : Set(std::move(set))
    {
    }

    realm_set* clone() const override
    {
        return new realm_set{*this};
    }

    bool is_frozen() const override
    {
        return Set::is_frozen();
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_set_t*>(&other)) {
            return get_realm() == ptr->get_realm() && get_parent_table_key() == ptr->get_parent_table_key() &&
                   get_parent_column_key() == ptr->get_parent_column_key() &&
                   get_parent_object_key() == ptr->get_parent_object_key();
        }
        return false;
    }

    struct thread_safe_reference : realm_thread_safe_reference, realm::ThreadSafeReference {
        thread_safe_reference(const Set& set)
            : realm::ThreadSafeReference(set)
        {
        }
    };

    realm_thread_safe_reference_t* get_thread_safe_reference() const final
    {
        return new thread_safe_reference{*this};
    }
};

struct realm_dictionary : realm::c_api::WrapC, realm::object_store::Dictionary {
    explicit realm_dictionary(Dictionary set)
        : Dictionary(std::move(set))
    {
    }

    realm_dictionary* clone() const override
    {
        return new realm_dictionary{*this};
    }

    bool is_frozen() const override
    {
        return Dictionary::is_frozen();
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_dictionary_t*>(&other)) {
            return get_realm() == ptr->get_realm() && get_parent_table_key() == ptr->get_parent_table_key() &&
                   get_parent_column_key() == ptr->get_parent_column_key() &&
                   get_parent_object_key() == ptr->get_parent_object_key();
        }
        return false;
    }

    struct thread_safe_reference : realm_thread_safe_reference, realm::ThreadSafeReference {
        thread_safe_reference(const Dictionary& set)
            : realm::ThreadSafeReference(set)
        {
        }
    };

    realm_thread_safe_reference_t* get_thread_safe_reference() const final
    {
        return new thread_safe_reference{*this};
    }
};

struct realm_key_path_array : realm::c_api::WrapC, realm::KeyPathArray {
    explicit realm_key_path_array(realm::KeyPathArray kpa)
        : realm::KeyPathArray(std::move(kpa))
    {
    }
};

struct realm_object_changes : realm::c_api::WrapC, realm::CollectionChangeSet {
    explicit realm_object_changes(realm::CollectionChangeSet changes)
        : realm::CollectionChangeSet(std::move(changes))
    {
    }

    realm_object_changes* clone() const override
    {
        return new realm_object_changes{static_cast<const realm::CollectionChangeSet&>(*this)};
    }
};

struct realm_collection_changes : realm::c_api::WrapC, realm::CollectionChangeSet {
    explicit realm_collection_changes(realm::CollectionChangeSet changes)
        : realm::CollectionChangeSet(std::move(changes))
    {
    }

    realm_collection_changes* clone() const override
    {
        return new realm_collection_changes{static_cast<const realm::CollectionChangeSet&>(*this)};
    }
};

struct realm_dictionary_changes : realm::c_api::WrapC, realm::DictionaryChangeSet {
    explicit realm_dictionary_changes(realm::DictionaryChangeSet changes)
        : realm::DictionaryChangeSet(std::move(changes))
    {
    }

    realm_dictionary_changes* clone() const override
    {
        return new realm_dictionary_changes{static_cast<const realm::DictionaryChangeSet&>(*this)};
    }
};

struct realm_notification_token : realm::c_api::WrapC, realm::NotificationToken {
    explicit realm_notification_token(realm::NotificationToken token)
        : realm::NotificationToken(std::move(token))
    {
    }
};

struct realm_callback_token : realm::c_api::WrapC {
protected:
    realm_callback_token(realm_t* realm, uint64_t token)
        : m_realm(realm)
        , m_token(token)
    {
    }
    realm_t* m_realm;
    uint64_t m_token;
};

struct realm_callback_token_realm : realm_callback_token {
    realm_callback_token_realm(realm_t* realm, uint64_t token)
        : realm_callback_token(realm, token)
    {
    }
    ~realm_callback_token_realm() override;
};

struct realm_callback_token_schema : realm_callback_token {
    realm_callback_token_schema(realm_t* realm, uint64_t token)
        : realm_callback_token(realm, token)
    {
    }
    ~realm_callback_token_schema() override;
};

struct realm_refresh_callback_token : realm_callback_token {
    realm_refresh_callback_token(realm_t* realm, uint64_t token)
        : realm_callback_token(realm, token)
    {
    }
    ~realm_refresh_callback_token() override;
};

struct realm_query : realm::c_api::WrapC {
    realm::Query query;
    std::weak_ptr<realm::Realm> weak_realm;

    explicit realm_query(realm::Query query, realm::util::bind_ptr<realm::DescriptorOrdering> ordering,
                         std::weak_ptr<realm::Realm> realm)
        : query(std::move(query))
        , weak_realm(realm)
        , m_ordering(std::move(ordering))
    {
    }

    realm_query* clone() const override
    {
        return new realm_query{*this};
    }

    realm::Query& get_query()
    {
        return query;
    }

    const realm::DescriptorOrdering& get_ordering() const
    {
        static const realm::DescriptorOrdering null_ordering;
        return m_ordering ? *m_ordering : null_ordering;
    }

    const char* get_description()
    {
        m_description = query.get_description();
        if (m_ordering)
            m_description += " " + m_ordering->get_description(query.get_table());
        return m_description.c_str();
    }

private:
    realm::util::bind_ptr<realm::DescriptorOrdering> m_ordering;
    std::string m_description;

    realm_query(const realm_query&) = default;
};

struct realm_results : realm::c_api::WrapC, realm::Results {
    explicit realm_results(realm::Results results)
        : realm::Results(std::move(results))
    {
    }

    realm_results* clone() const override
    {
        return new realm_results{static_cast<const realm::Results&>(*this)};
    }

    bool is_frozen() const override
    {
        return realm::Results::is_frozen();
    }

    struct thread_safe_reference : realm_thread_safe_reference_t, realm::ThreadSafeReference {
        thread_safe_reference(const realm::Results& results)
            : realm::ThreadSafeReference(results)
        {
        }
    };

    realm_thread_safe_reference_t* get_thread_safe_reference() const final
    {
        return new thread_safe_reference{*this};
    }
};

#if REALM_ENABLE_SYNC

struct realm_async_open_task_progress_notification_token : realm::c_api::WrapC {
    realm_async_open_task_progress_notification_token(std::shared_ptr<realm::AsyncOpenTask> task, uint64_t token)
        : task(task)
        , token(token)
    {
    }
    ~realm_async_open_task_progress_notification_token();
    std::shared_ptr<realm::AsyncOpenTask> task;
    uint64_t token;
};

struct realm_sync_session_connection_state_notification_token : realm::c_api::WrapC {
    realm_sync_session_connection_state_notification_token(std::shared_ptr<realm::SyncSession> session,
                                                           uint64_t token)
        : session(session)
        , token(token)
    {
    }
    ~realm_sync_session_connection_state_notification_token();
    std::shared_ptr<realm::SyncSession> session;
    uint64_t token;
};

struct realm_http_transport : realm::c_api::WrapC, std::shared_ptr<realm::app::GenericNetworkTransport> {
    realm_http_transport(std::shared_ptr<realm::app::GenericNetworkTransport> transport)
        : std::shared_ptr<realm::app::GenericNetworkTransport>(std::move(transport))
    {
    }

    realm_http_transport* clone() const override
    {
        return new realm_http_transport{*this};
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_http_transport*>(&other)) {
            return get() == ptr->get();
        }
        return false;
    }
};

struct realm_sync_client_config : realm::c_api::WrapC, realm::SyncClientConfig {
    using SyncClientConfig::SyncClientConfig;
};

struct realm_sync_config : realm::c_api::WrapC, realm::SyncConfig {
    using SyncConfig::SyncConfig;
    realm_sync_config(const SyncConfig& c)
        : SyncConfig(c)
    {
    }
};

#if REALM_APP_SERVICES

struct realm_app_config : realm::c_api::WrapC, realm::app::AppConfig {
    using AppConfig::AppConfig;
};

struct realm_app : realm::c_api::WrapC, realm::app::SharedApp {
    realm_app(realm::app::SharedApp app)
        : realm::app::SharedApp{std::move(app)}
    {
    }

    realm_app* clone() const override
    {
        return new realm_app{*this};
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_app*>(&other)) {
            return get() == ptr->get();
        }
        return false;
    }
};

struct realm_app_user_subscription_token : realm::c_api::WrapC {
    using Token = realm::Subscribable<realm::app::User>::Token;
    realm_app_user_subscription_token(std::shared_ptr<realm::app::User> user, Token&& token)
        : user(user)
        , token(std::move(token))
    {
    }
    ~realm_app_user_subscription_token();
    std::shared_ptr<realm::app::User> user;
    Token token;
};

struct realm_app_credentials : realm::c_api::WrapC, realm::app::AppCredentials {
    realm_app_credentials(realm::app::AppCredentials credentials)
        : realm::app::AppCredentials{std::move(credentials)}
    {
    }
};

struct realm_mongodb_collection : realm::c_api::WrapC, realm::app::MongoCollection {
    realm_mongodb_collection(realm::app::MongoCollection collection)
        : realm::app::MongoCollection(std::move(collection))
    {
    }
};

#endif // REALM_APP_SERVICES

struct realm_user : realm::c_api::WrapC, std::shared_ptr<realm::SyncUser> {
    realm_user(std::shared_ptr<realm::SyncUser> user)
        : std::shared_ptr<realm::SyncUser>{std::move(user)}
    {
    }

    realm_user* clone() const override
    {
        return new realm_user{*this};
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_user*>(&other)) {
            return get() == ptr->get();
        }
        return false;
    }
};

struct realm_sync_session : realm::c_api::WrapC, std::shared_ptr<realm::SyncSession> {
    realm_sync_session(std::shared_ptr<realm::SyncSession> session)
        : std::shared_ptr<realm::SyncSession>{std::move(session)}
    {
    }

    realm_sync_session* clone() const override
    {
        return new realm_sync_session{*this};
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_sync_session*>(&other)) {
            return get() == ptr->get();
        }
        return false;
    }
};

struct realm_sync_manager : realm::c_api::WrapC, std::shared_ptr<realm::SyncManager> {
    realm_sync_manager(std::shared_ptr<realm::SyncManager> manager)
        : std::shared_ptr<realm::SyncManager>{std::move(manager)}
    {
    }

    realm_sync_manager* clone() const override
    {
        return new realm_sync_manager{*this};
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_sync_manager*>(&other)) {
            return get() == ptr->get();
        }
        return false;
    }
};

struct realm_flx_sync_subscription : realm::c_api::WrapC, realm::sync::Subscription {
    realm_flx_sync_subscription(realm::sync::Subscription&& subscription)
        : realm::sync::Subscription(std::move(subscription))
    {
    }

    realm_flx_sync_subscription(const realm::sync::Subscription& subscription)
        : realm::sync::Subscription(subscription)
    {
    }

    realm_flx_sync_subscription* clone() const override
    {
        return new realm_flx_sync_subscription{*this};
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_flx_sync_subscription*>(&other)) {
            return *ptr == *this;
        }
        return false;
    }
};

struct realm_flx_sync_subscription_set : realm::c_api::WrapC, realm::sync::SubscriptionSet {
    realm_flx_sync_subscription_set(realm::sync::SubscriptionSet&& subscription_set)
        : realm::sync::SubscriptionSet(std::move(subscription_set))
    {
    }
};

struct realm_flx_sync_mutable_subscription_set : realm::c_api::WrapC, realm::sync::MutableSubscriptionSet {
    realm_flx_sync_mutable_subscription_set(realm::sync::MutableSubscriptionSet&& subscription_set)
        : realm::sync::MutableSubscriptionSet(std::move(subscription_set))
    {
    }
};

struct realm_async_open_task : realm::c_api::WrapC, std::shared_ptr<realm::AsyncOpenTask> {
    realm_async_open_task(std::shared_ptr<realm::AsyncOpenTask> task)
        : std::shared_ptr<realm::AsyncOpenTask>{std::move(task)}
    {
    }

    realm_async_open_task* clone() const override
    {
        return new realm_async_open_task{*this};
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_async_open_task*>(&other)) {
            return get() == ptr->get();
        }
        return false;
    }
};

struct realm_sync_socket : realm::c_api::WrapC, std::shared_ptr<realm::sync::SyncSocketProvider> {
    explicit realm_sync_socket(std::shared_ptr<realm::sync::SyncSocketProvider> ptr)
        : std::shared_ptr<realm::sync::SyncSocketProvider>(std::move(ptr))
    {
    }

    realm_sync_socket* clone() const override
    {
        return new realm_sync_socket{*this};
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_sync_socket*>(&other)) {
            return get() == ptr->get();
        }
        return false;
    }
};

struct realm_websocket_observer : realm::c_api::WrapC, std::shared_ptr<realm::sync::WebSocketObserver> {
    explicit realm_websocket_observer(std::shared_ptr<realm::sync::WebSocketObserver> ptr)
        : std::shared_ptr<realm::sync::WebSocketObserver>(std::move(ptr))
    {
    }

    realm_websocket_observer* clone() const override
    {
        return new realm_websocket_observer{*this};
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_websocket_observer*>(&other)) {
            return get() == ptr->get();
        }
        return false;
    }
};

struct realm_sync_socket_callback : realm::c_api::WrapC,
                                    std::shared_ptr<realm::sync::SyncSocketProvider::FunctionHandler> {
    explicit realm_sync_socket_callback(std::shared_ptr<realm::sync::SyncSocketProvider::FunctionHandler> ptr)
        : std::shared_ptr<realm::sync::SyncSocketProvider::FunctionHandler>(std::move(ptr))
    {
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_sync_socket_callback*>(&other)) {
            return get() == ptr->get();
        }
        return false;
    }

    void operator()(realm_sync_socket_callback_result_e result, const char* reason)
    {
        if (!get()) {
            return;
        }

        auto complete_status = result == RLM_ERR_SYNC_SOCKET_SUCCESS
                                   ? realm::Status::OK()
                                   : realm::Status{static_cast<realm::ErrorCodes::Error>(result), reason};
        (*get())(complete_status);
    }
};

struct CBindingThreadObserver final : public realm::BindingCallbackThreadObserver {
public:
    CBindingThreadObserver(realm_on_object_store_thread_callback_t on_thread_create,
                           realm_on_object_store_thread_callback_t on_thread_destroy,
                           realm_on_object_store_error_callback_t on_error, realm_userdata_t userdata,
                           realm_free_userdata_func_t free_userdata)
        : m_create_callback_func{on_thread_create}
        , m_destroy_callback_func{on_thread_destroy}
        , m_error_callback_func{on_error}
        , m_user_data{userdata, [&free_userdata] {
                          if (free_userdata)
                              return free_userdata;
                          return CBindingThreadObserver::m_default_free_userdata;
                      }()}
    {
    }

    void did_create_thread() override
    {
        if (m_create_callback_func)
            m_create_callback_func(m_user_data.get());
    }

    void will_destroy_thread() override
    {
        if (m_destroy_callback_func)
            m_destroy_callback_func(m_user_data.get());
    }

    bool handle_error(std::exception const& e) override
    {
        if (!m_error_callback_func)
            return false;

        return m_error_callback_func(m_user_data.get(), e.what());
    }

    bool has_handle_error() override
    {
        return bool(m_error_callback_func);
    }

    /// {@
    /// For testing: Return the values in this CBindingThreadObserver for comparing if two objects
    /// have the same callback functions and userdata ptr values.
    realm_on_object_store_thread_callback_t test_get_create_callback_func() const noexcept
    {
        return m_create_callback_func;
    }
    realm_on_object_store_thread_callback_t test_get_destroy_callback_func() const noexcept
    {
        return m_destroy_callback_func;
    }
    realm_on_object_store_error_callback_t test_get_error_callback_func() const noexcept
    {
        return m_error_callback_func;
    }
    realm_userdata_t test_get_userdata_ptr() const noexcept
    {
        return m_user_data.get();
    }
    /// @}

private:
    CBindingThreadObserver() = default;

    static constexpr realm_free_userdata_func_t m_default_free_userdata = [](realm_userdata_t) {};

    realm_on_object_store_thread_callback_t m_create_callback_func = nullptr;
    realm_on_object_store_thread_callback_t m_destroy_callback_func = nullptr;
    realm_on_object_store_error_callback_t m_error_callback_func = nullptr;
    realm::c_api::UserdataPtr m_user_data;
};

#endif // REALM_ENABLE_SYNC

#endif // REALM_OBJECT_STORE_C_API_TYPES_HPP
