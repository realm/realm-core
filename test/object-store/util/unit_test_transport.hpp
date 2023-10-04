////////////////////////////////////////////////////////////////////////////
//
// Copyright 2023 Realm Inc.
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

#include <realm/object-store/sync/generic_network_transport.hpp>

#include <external/json/json.hpp>

class UnitTestTransport : public realm::app::GenericNetworkTransport {
public:
    UnitTestTransport(const std::string& provider_type, uint64_t request_timeout);

    explicit UnitTestTransport(const std::string& provider_type = "anon-user")
        : UnitTestTransport(provider_type, 60000)
    {
    }
    explicit UnitTestTransport(uint64_t request_timeout)
        : UnitTestTransport("anon-user", request_timeout)
    {
    }

    static std::string access_token;

    static const std::string api_key;
    static const std::string api_key_id;
    static const std::string api_key_name;
    static const std::string auth_route;
    static const std::string identity_0_id;
    static const std::string identity_1_id;

    void set_provider_type(const std::string& provider_type)
    {
        m_provider_type = provider_type;
    }

    void set_profile(nlohmann::json profile)
    {
        m_user_profile = std::move(profile);
    }

    void set_expected_options(nlohmann::json options)
    {
        m_options = std::move(options);
    }

    void send_request_to_server(const realm::app::Request& request,
                                realm::util::UniqueFunction<void(const realm::app::Response&)>&& completion) override;

private:
    std::string m_provider_type;
    uint64_t m_request_timeout = 60000;
    nlohmann::json m_user_profile = nlohmann::json::object();
    nlohmann::json m_options;

    void handle_profile(const realm::app::Request& request,
                        realm::util::UniqueFunction<void(const realm::app::Response&)>&& completion);
    void handle_login(const realm::app::Request& request,
                      realm::util::UniqueFunction<void(const realm::app::Response&)>&& completion);
    void handle_location(const realm::app::Request& request,
                         realm::util::UniqueFunction<void(const realm::app::Response&)>&& completion);
    void handle_create_api_key(const realm::app::Request& request,
                               realm::util::UniqueFunction<void(const realm::app::Response&)>&& completion);
    void handle_fetch_api_key(const realm::app::Request& request,
                              realm::util::UniqueFunction<void(const realm::app::Response&)>&& completion);
    void handle_fetch_api_keys(const realm::app::Request& request,
                               realm::util::UniqueFunction<void(const realm::app::Response&)>&& completion);
    void handle_token_refresh(const realm::app::Request& request,
                              realm::util::UniqueFunction<void(const realm::app::Response&)>&& completion);
};
