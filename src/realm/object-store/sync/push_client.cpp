////////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or utilied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/object-store/sync/push_client.hpp>

#include <realm/object-store/sync/app_utils.hpp>
#include <realm/object-store/sync/auth_request_client.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/object-store/util/bson/bson.hpp>

namespace realm::app {

PushClient::~PushClient() = default;

namespace {
util::UniqueFunction<void(const Response&)>
wrap_completion(util::UniqueFunction<void(util::Optional<AppError>)>&& completion)
{
    return [completion = std::move(completion)](const Response& response) {
        completion(AppUtils::check_for_errors(response));
    };
}
} // anonymous namespace

void PushClient::register_device(const std::string& registration_token, const std::shared_ptr<SyncUser>& sync_user,
                                 util::UniqueFunction<void(util::Optional<AppError>)>&& completion)
{
    auto push_route = util::format("/app/%1/push/providers/%2/registration", m_app_id, m_service_name);
    std::string route = m_auth_request_client->url_for_path(push_route);

    bson::BsonDocument args{{"registrationToken", registration_token}};
    m_auth_request_client->do_authenticated_request(
        {HttpMethod::put, std::move(route), m_timeout_ms, {}, bson::Bson(args).to_string(), false}, sync_user,
        wrap_completion(std::move(completion)));
}

void PushClient::deregister_device(const std::shared_ptr<SyncUser>& sync_user,
                                   util::UniqueFunction<void(util::Optional<AppError>)>&& completion)
{
    auto push_route = util::format("/app/%1/push/providers/%2/registration", m_app_id, m_service_name);

    m_auth_request_client->do_authenticated_request(
        {HttpMethod::del, m_auth_request_client->url_for_path(push_route), m_timeout_ms, {}, "", false}, sync_user,
        wrap_completion(std::move(completion)));
}

} // namespace realm::app
