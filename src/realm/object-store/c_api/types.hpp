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

#endif // REALM_OBJECT_STORE_C_API_TYPES_HPP
