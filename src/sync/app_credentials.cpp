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

#include "sync/app_credentials.hpp"
#include "util/bson/bson.hpp"

#include <json.hpp>
#include <sstream>

namespace realm {
namespace app {

std::string const kAppProviderKey = "provider";

IdentityProvider const IdentityProviderAnonymous = "anon-user";
IdentityProvider const IdentityProviderGoogle = "oauth2-google";
IdentityProvider const IdentityProviderFacebook = "oauth2-facebook";
IdentityProvider const IdentityProviderApple = "oauth2-apple";
IdentityProvider const IdentityProviderUsernamePassword = "local-userpass";
IdentityProvider const IdentityProviderCustom = "custom-token";
IdentityProvider const IdentityProviderFunction = "custom-function";
IdentityProvider const IdentityProviderUserAPIKey = "api-key";
IdentityProvider const IdentityProviderServerAPIKey = "api-key";

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
        case AuthProvider::GOOGLE:
            return IdentityProviderGoogle;
        case AuthProvider::CUSTOM:
            return IdentityProviderCustom;
        case AuthProvider::USERNAME_PASSWORD:
            return IdentityProviderUsernamePassword;
        case AuthProvider::FUNCTION:
            return IdentityProviderFunction;
        case AuthProvider::USER_API_KEY:
            return IdentityProviderUserAPIKey;
        case AuthProvider::SERVER_API_KEY:
            return IdentityProviderServerAPIKey;
    }
    throw std::runtime_error("unknown provider type in provider_type_from_enum");
}

AppCredentials::AppCredentials(AuthProvider provider, std::function<std::string()> factory)
: m_provider(provider)
, m_payload_factory(factory)
{
}

AuthProvider AppCredentials::provider() const
{
    return m_provider;
}

std::string AppCredentials::provider_as_string() const
{
    return provider_type_from_enum(m_provider);
}

std::string AppCredentials::serialize_as_json() const
{
    return m_payload_factory();
}

AppCredentials AppCredentials::anonymous()
{
    return AppCredentials(AuthProvider::ANONYMOUS,
                          [=] {
                              return nlohmann::json({
                                  {kAppProviderKey, IdentityProviderAnonymous}
                              }).dump();
                          });
}

AppCredentials AppCredentials::apple(AppCredentialsToken id_token)
{
    return AppCredentials(AuthProvider::APPLE,
                          [=] {
                              return nlohmann::json({
                                  {kAppProviderKey, IdentityProviderApple},
                                  {"id_token", id_token}
                              }).dump();
                          });
}

AppCredentials AppCredentials::facebook(AppCredentialsToken access_token)
{
    return AppCredentials(AuthProvider::FACEBOOK,
                          [=] {
                              return nlohmann::json({
                                  {kAppProviderKey, IdentityProviderFacebook},
                                  {"accessToken", access_token}
                              }).dump();
                          });
}

AppCredentials AppCredentials::google(AppCredentialsToken auth_token)
{
    return AppCredentials(AuthProvider::GOOGLE,
                          [=] {
                              return nlohmann::json({
                                  {kAppProviderKey, IdentityProviderGoogle},
                                  {"authCode", auth_token}
                              }).dump();
                          });
}

AppCredentials AppCredentials::custom(AppCredentialsToken token)
{
    return AppCredentials(AuthProvider::CUSTOM,
                          [=] {
                              return nlohmann::json({
                                  {kAppProviderKey, IdentityProviderCustom},
                                  {"token", token}
                              }).dump();
                          });
}

AppCredentials AppCredentials::username_password(std::string username,
                                                 std::string password)
{
    return AppCredentials(AuthProvider::USERNAME_PASSWORD,
                          [=] {
                              return nlohmann::json({
                                  {kAppProviderKey, IdentityProviderUsernamePassword},
                                  {"username", username},
                                  {"password", password}
                              }).dump();
                          });
}

AppCredentials AppCredentials::function(const bson::BsonDocument& payload)
{
    return AppCredentials(AuthProvider::FUNCTION,
                          [=] {
                              std::stringstream output;
                              output << bson::Bson(payload);
                              return output.str();
                          });
}

AppCredentials AppCredentials::function(const std::string& serialized_payload)
{
    return AppCredentials(AuthProvider::FUNCTION,
        [=] {
            return serialized_payload;
        });
}


AppCredentials AppCredentials::user_api_key(std::string api_key)
{
    return AppCredentials(AuthProvider::USER_API_KEY,
                          [=] {
                              return nlohmann::json({
                                  {kAppProviderKey, IdentityProviderUserAPIKey},
                                  {"key", api_key}
                              }).dump();
                          });
}

AppCredentials AppCredentials::server_api_key(std::string api_key)
{
    return AppCredentials(AuthProvider::SERVER_API_KEY,
                          [=] {
                              return nlohmann::json({
                                  {kAppProviderKey, IdentityProviderServerAPIKey},
                                  {"key", api_key}
                              }).dump();
                          });
}

} // namespace app
} // namespace realm
