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

#include "app_credentials.hpp"
#include "generic_network_transport.hpp"
#include "sync_user.hpp"

namespace realm {
namespace app {

/**
 The `App` has the fundamental set of methods for communicating with a MongoDB Realm application backend.

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
        std::string app_id;
        GenericNetworkTransport::NetworkTransportFactory transport_generator;
        realm::util::Optional<std::string> base_url;
        realm::util::Optional<std::string> local_app_name;
        realm::util::Optional<std::string> local_app_version;
        realm::util::Optional<uint64_t> default_request_timeout_ms;
    };

    App(const Config& config);
    App() = default;
    App(const App&) = default;
    App(App&&) = default;
    App& operator=(App const&) = default;
    App& operator=(App&&) = default;

    // Get the last used user.
    std::shared_ptr<SyncUser> current_user() const;

    // Get all users.
    std::vector<std::shared_ptr<SyncUser>> all_users() const;
    /**
     * A struct representing a user API key as returned by the App server.
    */
    struct UserAPIKey {
        // The ID of the key.
        ObjectId id;

        // The actual key. Will only be included in
        // the response when an API key is first created.
        Optional<std::string> key;

        // The name of the key.
        std::string name;

        // Whether or not the key is disabled.
        bool disabled;
    };

    /**
     * A client for the user API key authentication provider which
     * can be used to create and modify user API keys. This
     * client should only be used by an authenticated user.
    */
    class UserAPIKeyProviderClient {
    public:
        /**
         * Creates a user API key that can be used to authenticate as the current user.
         *
         * - parameters:
         *     - name: The name of the API key to be created.
         *     - completion_block: A callback to be invoked once the call is complete.
        */
        void create_api_key(const std::string& name,
                            std::function<void(Optional<UserAPIKey>, Optional<AppError>)> completion_block);

        /**
         * Fetches a user API key associated with the current user.
         *
         * - parameters:
         *     - id: The id of the API key to fetch.
         *     - completion_block: A callback to be invoked once the call is complete.
         */
        void fetch_api_key(const realm::ObjectId& id,
                           std::function<void(Optional<UserAPIKey>, Optional<AppError>)> completion_block);

        /**
         * Fetches the user API keys associated with the current user.
         *
         * - parameters:
         *     - completion_block: A callback to be invoked once the call is complete.
         */
        void fetch_api_keys(std::function<void(std::vector<UserAPIKey>, Optional<AppError>)> completion_block);

        /**
         * Deletes a user API key associated with the current user.
         *
         * - parameters:
         *     - id: The id of the API key to delete.
         *     - completion_block: A callback to be invoked once the call is complete.
         */
        void delete_api_key(const UserAPIKey& api_key,
                            std::function<void(Optional<AppError>)> completion_block);

        /**
         * Enables a user API key associated with the current user.
         *
         * - parameters:
         *     - id: The id of the API key to enable.
         *     - completion_block: A callback to be invoked once the call is complete.
         */
        void enable_api_key(const UserAPIKey& api_key,
                            std::function<void(Optional<AppError>)> completion_block);

        /**
         * Disables a user API key associated with the current user.
         *
         * - parameters:
         *     - id: The id of the API key to disable.
         *     - completion_block: A callback to be invoked once the call is complete.
         */
        void disable_api_key(const UserAPIKey& api_key,
                             std::function<void(Optional<AppError>)> completion_block);
    private:
        friend class App;
        UserAPIKeyProviderClient(App* app) : parent(app) {}
        App* parent;
    };

    /**
     * A client for the username/password authentication provider which
     * can be used to obtain a credential for logging in,
     * and to perform requests specifically related to the username/password provider.
    */
    class UsernamePasswordProviderClient {
    public:
        /**
         * Registers a new email identity with the username/password provider,
         * and sends a confirmation email to the provided address.
         *
         * - parameters:
         *     - email: The email address of the user to register.
         *     - password: The password that the user created for the new username/password identity.
         *     - completion_block: A callback to be invoked once the call is complete.
         */
        void register_email(const std::string& email,
                            const std::string& password,
                            std::function<void(Optional<AppError>)> completion_block);

        /**
         * Confirms an email identity with the username/password provider.
         *
         * - parameters:
         *     - token: The confirmation token that was emailed to the user.
         *     - token_id: The confirmation token id that was emailed to the user.
         *     - completion_block: A callback to be invoked once the call is complete.
         */
        void confirm_user(const std::string& token,
                          const std::string& token_id,
                          std::function<void(Optional<AppError>)> completion_block);

        /**
         * Re-sends a confirmation email to a user that has registered but
         * not yet confirmed their email address.
         *
         * - parameters:
         *     - email: The email address of the user to re-send a confirmation for.
         *     - completion_block: A callback to be invoked once the call is complete.
         */
        void resend_confirmation_email(const std::string& email,
                                       std::function<void(Optional<AppError>)> completion_block);

        /**
         * Sends a password reset email to the given email address.
         *
         * - parameters:
         *     - email: The email address of the user to send a password reset email for.
         *     - completion_block: A callback to be invoked once the call is complete.
         */
        void send_reset_password_email(const std::string& email,
                                       std::function<void(Optional<AppError>)> completion_block);

        /**
         * Resets the password of an email identity using the
         * password reset token emailed to a user.
         *
         * - parameters:
         *     - password: The desired new password.
         *     - token: The password reset token that was emailed to the user.
         *     - token_id: The password reset token id that was emailed to the user.
         *     - completion_block: A callback to be invoked once the call is complete.
         */
        void reset_password(const std::string& password,
                            const std::string& token,
                            const std::string& token_id,
                            std::function<void(Optional<AppError>)> completion_block);

        /**
         * Resets the password of an email identity using the
         * password reset function set up in the application.
         *
         * TODO: Add an overloaded version of this method that takes
         * TODO: raw, non-serialized args.
         *
         * - parameters:
         *     - email: The email address of the user.
         *     - password: The desired new password.
         *     - args: A pre-serialized list of arguments. Must be a JSON array.
         *     - completion_block: A callback to be invoked once the call is complete.
        */
        void call_reset_password_function(const std::string& email,
                                          const std::string& password,
                                          const std::string& args,
                                          std::function<void(Optional<AppError>)> completion_block);
    private:
        friend class App;
        UsernamePasswordProviderClient(App* app) : parent(app) {}
        App* parent;
    };

    /**
    Log in a user and asynchronously retrieve a user object.

    If the log in completes successfully, the completion block will be called, and a
    `SyncUser` representing the logged-in user will be passed to it. This user object
    can be used to open `Realm`s and retrieve `SyncSession`s. Otherwise, the
    completion block will be called with an error.

    - parameter credentials: A `SyncCredentials` object representing the user to log in.
    - parameter completion: A callback block to be invoked once the log in completes.
    */
    void log_in_with_credentials(const AppCredentials& credentials,
                                 std::function<void(std::shared_ptr<SyncUser>, Optional<AppError>)> completion_block) const;

    /**
     Logout the current user.
     */
    void log_out(std::function<void(Optional<AppError>)>) const;

    // Get a provider client for the given class type.
    template <class T>
    T provider_client() {
        return T(this);
    }

private:
    Config m_config;
    std::string m_base_route;
    std::string m_app_route;
    std::string m_auth_route;
    uint64_t m_request_timeout_ms;
};

template<>
App::UsernamePasswordProviderClient App::provider_client <App::UsernamePasswordProviderClient> ();
template<>
App::UserAPIKeyProviderClient App::provider_client <App::UserAPIKeyProviderClient>();

} // namespace app
} // namespace realm

#endif /* REALM_APP_HPP */
