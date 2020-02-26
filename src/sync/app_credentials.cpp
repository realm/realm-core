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

#include "app_credentials.hpp"
#include "../external/json/json.hpp"

namespace realm {
namespace app {

std::string const kAppProviderKey = "provider";

IdentityProvider const IdentityProviderAnonymous = "anon-user";
IdentityProvider const IdentityProviderFacebook  = "oauth2-facebook";
IdentityProvider const IdentityProviderApple     = "oauth2-apple";
IdentityProvider const IdentityProviderUsernamePassword     = "local-userpass";

std::string AppCredentials::serialize_as_json() const
{
    return m_payload_factory();
}

IdentityProvider provider_type_from_enum(AuthProvider provider)
{
    switch (provider)
    {
        case AuthProvider::ANONYMOUS:
            return IdentityProviderAnonymous;
        case AuthProvider::APPLE:
            return IdentityProviderApple;
        case AuthProvider::FACEBOOK:
            return IdentityProviderFacebook;
        case AuthProvider::USERNAME_PASSWORD:
            return IdentityProviderUsernamePassword;
    }
    throw std::runtime_error("Error: unknown provider type in provider_type_from_enum");
}

AuthProvider AppCredentials::provider() const
{
    return m_provider;
}

std::shared_ptr<AppCredentials> AppCredentials::anonymous()
{
    auto credentials = std::make_shared<AppCredentials>();
    credentials->m_payload_factory = [=] {
        auto raw = nlohmann::json({
            {kAppProviderKey, IdentityProviderAnonymous}
        }).dump();
        return raw;
    };
    credentials->m_provider = AuthProvider::ANONYMOUS;
    return credentials;
}

std::shared_ptr<AppCredentials> AppCredentials::apple(const AppCredentialsToken id_token)
{
    auto credentials = std::make_shared<AppCredentials>();
    credentials->m_payload_factory = [=] {
        auto raw = nlohmann::json({
            {kAppProviderKey, IdentityProviderApple},
            {"id_token", id_token}
        }).dump();
        return raw;
    };
    credentials->m_provider = AuthProvider::APPLE;
    return credentials;
}

std::shared_ptr<AppCredentials> AppCredentials::facebook(const AppCredentialsToken access_token)
{
    auto credentials = std::make_shared<AppCredentials>();
    credentials->m_payload_factory = [=] {
        auto raw = nlohmann::json({
            {kAppProviderKey, IdentityProviderFacebook},
            {"access_token", access_token}
        }).dump();
        return raw;
    };
    credentials->m_provider = AuthProvider::FACEBOOK;
    return credentials;
}

std::shared_ptr<AppCredentials> AppCredentials::username_password(const std::string username,
                                                                  const std::string password)
{
    auto credentials = std::make_shared<AppCredentials>();
    credentials->m_payload_factory = [=] {
        auto raw = nlohmann::json({
            {kAppProviderKey, IdentityProviderUsernamePassword},
            {"username", username},
            {"password", password}
        }).dump();
        return raw;
    };
    credentials->m_provider = AuthProvider::USERNAME_PASSWORD;
    return credentials;
}

} // namespace app
} // namespace realm
