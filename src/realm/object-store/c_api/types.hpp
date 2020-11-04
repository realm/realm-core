#ifndef REALM_OBJECT_STORE_C_API_TYPES_HPP
#define REALM_OBJECT_STORE_C_API_TYPES_HPP

#include <realm/realm.h>
#include <realm/object-store/c_api/conversion.hpp>

#include <realm/util/to_string.hpp>

#include <realm/object-store/shared_realm.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object.hpp>
#include <realm/object-store/object_accessor.hpp>
#include <realm/parser/parser.hpp>
#include <realm/parser/query_builder.hpp>
#include <realm/object-store/util/scheduler.hpp>
#include <realm/object-store/thread_safe_reference.hpp>

#include <stdexcept>
#include <string>

// Note: This is OK-ish because types.hpp is not a public header.
using namespace realm;

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

struct InvalidQueryException : std::runtime_error {
    using std::runtime_error::runtime_error;
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

struct realm_async_error : WrapC {
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

struct realm_thread_safe_reference : WrapC {
    realm_thread_safe_reference(const realm_thread_safe_reference&) = delete;

protected:
    realm_thread_safe_reference() {}
};

struct realm_config : WrapC, Realm::Config {
    using Realm::Config::Config;
};

struct realm_scheduler : WrapC, std::shared_ptr<util::Scheduler> {
    explicit realm_scheduler(std::shared_ptr<util::Scheduler> ptr)
        : std::shared_ptr<util::Scheduler>(std::move(ptr))
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

struct realm_schema : WrapC {
    std::unique_ptr<Schema> owned;
    const Schema* ptr = nullptr;

    realm_schema(std::unique_ptr<Schema> o, const Schema* ptr = nullptr)
        : owned(std::move(o))
        , ptr(ptr ? ptr : owned.get())
    {
    }

    explicit realm_schema(const Schema* ptr)
        : ptr(ptr)
    {
    }

    realm_schema_t* clone() const override
    {
        auto o = std::make_unique<Schema>(*ptr);
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

struct shared_realm : WrapC, SharedRealm {
    shared_realm(SharedRealm rlm)
        : SharedRealm{std::move(rlm)}
    {
    }

    shared_realm* clone() const override
    {
        return new shared_realm{*this};
    }

    bool equals(const WrapC& other) const noexcept final
    {
        if (auto ptr = dynamic_cast<const shared_realm*>(&other)) {
            return get() == ptr->get();
        }
        return false;
    }

    struct thread_safe_reference : realm_thread_safe_reference, ThreadSafeReference {
        thread_safe_reference(const std::shared_ptr<Realm>& rlm)
            : ThreadSafeReference(rlm)
        {
        }
    };

    realm_thread_safe_reference_t* get_thread_safe_reference() const final
    {
        return new thread_safe_reference{*this};
    }
};

struct realm_object : WrapC, Object {
    explicit realm_object(Object obj)
        : Object(std::move(obj))
    {
    }

    realm_object* clone() const override
    {
        return new realm_object{*this};
    }

    bool is_frozen() const override
    {
        return Object::is_frozen();
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

    struct thread_safe_reference : realm_thread_safe_reference, ThreadSafeReference {
        thread_safe_reference(const Object& obj)
            : ThreadSafeReference(obj)
        {
        }
    };

    realm_thread_safe_reference_t* get_thread_safe_reference() const final
    {
        return new thread_safe_reference{*this};
    }
};

struct realm_list : WrapC, List {
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

    struct thread_safe_reference : realm_thread_safe_reference, ThreadSafeReference {
        thread_safe_reference(const List& list)
            : ThreadSafeReference(list)
        {
        }
    };

    realm_thread_safe_reference_t* get_thread_safe_reference() const final
    {
        return new thread_safe_reference{*this};
    }
};

struct realm_object_changes : WrapC, CollectionChangeSet {
    explicit realm_object_changes(CollectionChangeSet changes)
        : CollectionChangeSet(std::move(changes))
    {
    }

    realm_object_changes* clone() const override
    {
        return new realm_object_changes{static_cast<const CollectionChangeSet&>(*this)};
    }
};

struct realm_collection_changes : WrapC, CollectionChangeSet {
    explicit realm_collection_changes(CollectionChangeSet changes)
        : CollectionChangeSet(std::move(changes))
    {
    }

    realm_collection_changes* clone() const override
    {
        return new realm_collection_changes{static_cast<const CollectionChangeSet&>(*this)};
    }
};

struct realm_notification_token : WrapC, NotificationToken {
    explicit realm_notification_token(NotificationToken token)
        : NotificationToken(std::move(token))
    {
    }
};

struct realm_query : WrapC {
    Query query;
    DescriptorOrdering ordering;
    std::weak_ptr<Realm> weak_realm;

    explicit realm_query(Query query, DescriptorOrdering ordering, std::weak_ptr<Realm> realm)
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

struct realm_results : WrapC, Results {
    explicit realm_results(Results results)
        : Results(std::move(results))
    {
    }

    realm_results* clone() const override
    {
        return new realm_results{static_cast<const Results&>(*this)};
    }

    bool is_frozen() const override
    {
        return Results::is_frozen();
    }

    struct thread_safe_reference : realm_thread_safe_reference_t, ThreadSafeReference {
        thread_safe_reference(const Results& results)
            : ThreadSafeReference(results)
        {
        }
    };

    realm_thread_safe_reference_t* get_thread_safe_reference() const final
    {
        return new thread_safe_reference{*this};
    }
};


#endif // REALM_OBJECT_STORE_C_API_TYPES_HPP