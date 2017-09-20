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

#include "partial_sync.hpp"

#include "impl/notification_wrapper.hpp"
#include "impl/object_accessor_impl.hpp"
#include "object_schema.hpp"
#include "results.hpp"
#include "shared_realm.hpp"
#include "thread_safe_reference.hpp"

#include <realm/util/scope_exit.hpp>

namespace realm {
namespace partial_sync {

namespace {

constexpr const char* result_sets_type_name = "__ResultSets";

std::string matches_property_name_for_object(const std::string& name)
{
    return name + "_matches";
}

Schema add_result_sets_to_schema(const Schema& source_schema, Property matches_property)
{
    std::vector<ObjectSchema> schema;
    schema.reserve(source_schema.size());
    std::copy(source_schema.begin(), source_schema.end(), std::back_inserter(schema));

    ObjectSchema result_sets_schema(result_sets_type_name, {
        {"matches_property", PropertyType::String},
        {"query", PropertyType::String},
        {"status", PropertyType::Int},
        {"error_message", PropertyType::String},
        std::move(matches_property),
    });
    schema.push_back(std::move(result_sets_schema));
    return schema;
}

} // unnamed namespace

void register_query(std::shared_ptr<Realm> realm, const std::string &object_class, const std::string &query,
                    std::function<void (Results, std::exception_ptr)> callback)
{
    auto matches_property = matches_property_name_for_object(object_class);

    auto schema = add_result_sets_to_schema(realm->schema(),
                                            {matches_property, PropertyType::Object|PropertyType::Array, object_class});

    Object raw_object;
    {
        realm->begin_transaction();
        auto cleanup = util::make_scope_exit([&]() noexcept {
            if (realm->is_in_transaction())
                realm->cancel_transaction();
        });

        // FIXME: Is it ok for us to modify the schema of the Realm like this?
        // It will invalidate any references the binding may have stored to object schemas.
        realm->update_schema(schema, ObjectStore::NotVersioned, nullptr, nullptr, true);

        CppContext context;
        raw_object = Object::create<util::Any>(context, realm, *realm->schema().find(result_sets_type_name),
                                               AnyDict{
                                                   {"matches_property", matches_property},
                                                   {"query", query},
                                                   {"status", int64_t(0)},
                                                   {"error_message", std::string()},
                                               }, false);

        realm->commit_transaction();
    }

    auto object = std::make_shared<_impl::NotificationWrapper<Object>>(std::move(raw_object));

    // Observe the new object and notify listener when the results are complete (status != 0).
    auto notification_callback = [object, matches_property,
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

} // namespace partial_sync
} // namespace realm
