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

#ifndef REALM_APP_HPP
#define REALM_APP_HPP

#include "sync_user.hpp"
#include "app_credentials.hpp"
#include "generic_network_transport.hpp"

namespace realm {
namespace app {

#pragma mark RealmApp

/**
 The `RealmApp` has the fundamental set of methods for communicating with a MongoDB
 Realm application backend.

 This class provides access to login and authentication.

 Using `serviceClient`, you can retrieve services, including the `RemoteMongoClient` for reading
 and writing on the database. To create a `RemoteMongoClient`, pass `remoteMongoClientFactory`
 into `serviceClient(fromFactory:withName)`.

 You can also use it to execute [Functions](https://docs.mongodb.com/stitch/functions/).

 Finally, its `RealmPush` object can register the current user for push notifications.

 - SeeAlso:
 `RemoteMongoClient`,
 `RLMPush`,
 [Functions](https://docs.mongodb.com/stitch/functions/)
 */
class App {
public:
    struct Config {
        realm::util::Optional<std::shared_ptr<GenericNetworkTransport>> transport;
        realm::util::Optional<std::string> base_url;
        realm::util::Optional<std::string> local_app_name;
        realm::util::Optional<std::string> local_app_version;
        realm::util::Optional<uint64_t> default_request_timeout_ms;
    };

    static std::shared_ptr<App> app(const std::string app_id,
                                    const realm::util::Optional<App::Config> config);

    /**
    Log in a user and asynchronously retrieve a user object.

    If the log in completes successfully, the completion block will be called, and a
    `SyncUser` representing the logged-in user will be passed to it. This user object
    can be used to open `Realm`s and retrieve `SyncSession`s. Otherwise, the
    completion block will be called with an error.

    - parameter credentials: A `SyncCredentials` object representing the user to log in.
    - parameter completion: A callback block to be invoked once the log in completes.
    */
    void login_with_credentials(const std::shared_ptr<AppCredentials> credentials,
                                std::function<void(std::shared_ptr<SyncUser>, std::unique_ptr<error::AppError>)> completion_block);


    App(const std::string app_id,
        const realm::util::Optional<App::Config> config) :
    m_app_id(app_id) {
        std::string base_url = default_base_url;
        if (config) {
            if (config.value().base_url) {
                base_url = config.value().base_url.value();
            }

            if (config.value().default_request_timeout_ms) {
                m_request_timeout_ms = config.value().default_request_timeout_ms.value();
            } else {
                m_request_timeout_ms = default_timeout_ms;
            }

            if (config.value().transport) {
                // TODO: Install custom transport
            }
        } else {
            m_request_timeout_ms = default_timeout_ms;
        }

        m_base_route = base_url + base_path;
        m_app_route = m_base_route + app_path + "/" + app_id;
        m_auth_route = m_app_route + auth_path;
    };

private:
    static const std::string default_base_url;
    static const std::string base_path;
    static const std::string app_path;
    static const std::string auth_path;
    static const uint64_t default_timeout_ms;

    /// the app ID of this application
    std::string m_app_id;

    std::string m_base_route;
    std::string m_app_route;
    std::string m_auth_route;

    uint64_t m_request_timeout_ms;
};

} // namespace app
} // namespace realm

#endif /* REALM_APP_HPP */
