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

#ifndef REALM_OS_PARTIAL_SYNC_HPP
#define REALM_OS_PARTIAL_SYNC_HPP

#include "results.hpp"

#include <realm/util/optional.hpp>

#include <functional>
#include <memory>
#include <string>

namespace realm {

class Group;
class Object;
class Realm;

namespace partial_sync {
enum class SubscriptionState : int8_t;

struct SubscriptionNotificationToken {
    NotificationToken subscription_token;
    NotificationToken error_token;
};

class Subscription {
public:
    ~Subscription();
    Subscription(Subscription&&);
    Subscription& operator=(Subscription&&);

    SubscriptionState status() const;
    util::Optional<std::string> error_message() const;

    Results results() const;

    SubscriptionNotificationToken add_notification_callback(std::function<void()> callback);

private:
    Subscription(std::string name, std::string object_type, std::shared_ptr<Realm>);

    util::Optional<Object> result_set_object() const;

    void error_occurred(std::exception_ptr);

    std::unique_ptr<ObjectSchema> m_object_schema;

    mutable Results m_result_sets;

    struct ErrorNotifier;
    _impl::CollectionNotifier::Handle<ErrorNotifier> m_error_notifier;

    friend Subscription subscribe(Results const&, util::Optional<std::string>);
};

Subscription subscribe(Results const& results, util::Optional<std::string> name);

void reset_for_testing();

// Deprecated
void register_query(std::shared_ptr<Realm>, const std::string &object_class, const std::string &query,
					std::function<void (Results, std::exception_ptr)>);

} // namespace partial_sync

namespace _impl {

void initialize_schema(Group&);

} // namespace _impl
} // namespace realm

#endif // REALM_OS_PARTIAL_SYNC_HPP
