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

struct realm_parsed_query : WrapC, parser::ParserResult {
    explicit realm_parsed_query(parser::ParserResult result)
        : parser::ParserResult(std::move(result))
    {
    }

    realm_parsed_query* clone() const override
    {
        return new realm_parsed_query{*this};
    }
};

struct realm_query : WrapC {
    std::unique_ptr<Query> ptr;
    std::weak_ptr<Realm> weak_realm;

    explicit realm_query(Query query, std::weak_ptr<Realm> realm)
        : ptr(std::make_unique<Query>(std::move(query)))
        , weak_realm(realm)
    {
    }

    realm_query* clone() const override
    {
        return new realm_query{*ptr, weak_realm};
    }
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
};

struct realm_descriptor_ordering : WrapC, DescriptorOrdering {
    realm_descriptor_ordering() = default;

    explicit realm_descriptor_ordering(DescriptorOrdering o)
        : DescriptorOrdering(std::move(o))
    {
    }

    realm_descriptor_ordering* clone() const override
    {
        return new realm_descriptor_ordering{static_cast<const DescriptorOrdering&>(*this)};
    }
};


#endif // REALM_OBJECT_STORE_C_API_TYPES_HPP