#ifndef REALM_OBJECT_STORE_C_API_TYPES_HPP
#define REALM_OBJECT_STORE_C_API_TYPES_HPP

#include <realm.h>
#include <realm/object-store/c_api/conversion.hpp>

#include <realm/util/to_string.hpp>

#include <realm/object-store/shared_realm.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object.hpp>
#include <realm/object-store/object_accessor.hpp>
#include <realm/object-store/util/scheduler.hpp>
#include <realm/object-store/thread_safe_reference.hpp>

#if REALM_ENABLE_SYNC
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/impl/sync_client.hpp>
#endif

#include <stdexcept>
#include <string>

namespace realm::c_api {

struct NotClonableException : std::exception {
    const char* what() const noexcept
    {
        return "Not clonable";
    }
};

struct ImmutableException : std::exception {
    const char* what() const noexcept
    {
        return "Immutable object";
    }
};


struct UnexpectedPrimaryKeyException : std::logic_error {
    using std::logic_error::logic_error;
};

struct DuplicatePrimaryKeyException : std::logic_error {
    using std::logic_error::logic_error;
};

struct InvalidPropertyKeyException : std::logic_error {
    using std::logic_error::logic_error;
};
struct CallbackFailed : std::runtime_error {
    CallbackFailed()
        : std::runtime_error("User-provided callback failed")
    {
    }
};

//// FIXME: BEGIN EXCEPTIONS THAT SHOULD BE MOVED INTO OBJECT STORE

struct WrongPrimaryKeyTypeException : std::logic_error {
    WrongPrimaryKeyTypeException(const std::string& object_type)
        : std::logic_error(util::format("Wrong primary key type for '%1'", object_type))
        , object_type(object_type)
    {
    }
    const std::string object_type;
};

struct NotNullableException : std::logic_error {
    NotNullableException(const std::string& object_type, const std::string& property_name)
        : std::logic_error(util::format("Property '%2' of class '%1' cannot be NULL", object_type, property_name))
        , object_type(object_type)
        , property_name(property_name)
    {
    }
    const std::string object_type;
    const std::string property_name;
};

struct PropertyTypeMismatch : std::logic_error {
    PropertyTypeMismatch(const std::string& object_type, const std::string& property_name)
        : std::logic_error(util::format("Type mismatch for property '%2' of class '%1'", object_type, property_name))
        , object_type(object_type)
        , property_name(property_name)
    {
    }
    const std::string object_type;
    const std::string property_name;
};

//// FIXME: END EXCEPTIONS THAT SHOULD BE MOVED INTO OBJECT STORE

struct WrapC {
    virtual ~WrapC() {}

    virtual WrapC* clone() const
    {
        throw NotClonableException();
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
        throw std::logic_error{"Thread safe references cannot be created for this object type"};
    }
};

} // namespace realm::c_api

struct realm_async_error : realm::c_api::WrapC {
    std::exception_ptr ep;

    explicit realm_async_error(std::exception_ptr ep)
        : ep(std::move(ep))
    {
    }

    realm_async_error* clone() const override
    {
        return new realm_async_error{ep};
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const realm_async_error_t*>(&other)) {
            return ep == ptr->ep;
        }
        return false;
    }
};

struct realm_thread_safe_reference : realm::c_api::WrapC {
    realm_thread_safe_reference(const realm_thread_safe_reference&) = delete;

protected:
    realm_thread_safe_reference() {}
};

struct realm_config : realm::c_api::WrapC, realm::Realm::Config {
    using Config::Config;
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
            auto a = obj();
            auto b = ptr->obj();
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

struct realm_notification_token : realm::c_api::WrapC, realm::NotificationToken {
    explicit realm_notification_token(realm::NotificationToken token)
        : realm::NotificationToken(std::move(token))
    {
    }
};

struct realm_query : realm::c_api::WrapC {
    realm::Query query;
    realm::DescriptorOrdering ordering;
    std::weak_ptr<realm::Realm> weak_realm;

    explicit realm_query(realm::Query query, realm::DescriptorOrdering ordering, std::weak_ptr<realm::Realm> realm)
        : query(std::move(query))
        , ordering(std::move(ordering))
        , weak_realm(realm)
    {
    }

    realm_query* clone() const override
    {
        return new realm_query{*this};
    }

private:
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

struct realm_app_config : realm::c_api::WrapC, realm::app::App::Config {
    using Config::Config;
};

struct realm_sync_client_config : realm::c_api::WrapC, realm::SyncClientConfig {
    using SyncClientConfig::SyncClientConfig;
};

struct realm_sync_config : realm::c_api::WrapC, realm::SyncConfig {
    using SyncConfig::SyncConfig;
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

struct realm_app_credentials : realm::c_api::WrapC, realm::app::AppCredentials {
    realm_app_credentials(realm::app::AppCredentials credentials)
        : realm::app::AppCredentials{std::move(credentials)}
    {
    }
};

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
#endif // REALM_ENABLE_SYNC

#endif // REALM_OBJECT_STORE_C_API_TYPES_HPP
