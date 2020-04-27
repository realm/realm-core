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

#include "sync/auth_request_client.hpp"
#include "sync/app_service_client.hpp"
#include "sync/app_credentials.hpp"
#include "sync/generic_network_transport.hpp"
#include "sync/sync_user.hpp"

namespace realm {
namespace app {

class RemoteMongoClient;
class AppServiceClient;

/// The `App` has the fundamental set of methods for communicating with a MongoDB Realm application backend.
///
/// This class provides access to login and authentication.
///
/// Using `remote_mongo_client`, you can retrieve `RemoteMongoClient` for reading
/// and writing on the database.
///
/// You can also use it to execute [Functions](https://docs.mongodb.com/stitch/functions/).
class App : public AuthRequestClient {
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
    App(App&&) noexcept = default;
    App& operator=(App const&) = default;
    App& operator=(App&&) = default;

    /// Get the last used user.
    std::shared_ptr<SyncUser> current_user() const;

    /// Get all users.
    std::vector<std::shared_ptr<SyncUser>> all_users() const;

    /// A struct representing a user API key as returned by the App server.
    struct UserAPIKey {
        // The ID of the key.
        ObjectId id;

        /// The actual key. Will only be included in
        /// the response when an API key is first created.
        Optional<std::string> key;

        /// The name of the key.
        std::string name;

        /// Whether or not the key is disabled.
        bool disabled;
    };

    /// A client for the user API key authentication provider which
    /// can be used to create and modify user API keys. This
    /// client should only be used by an authenticated user.
    class UserAPIKeyProviderClient {
    public:
        /// Creates a user API key that can be used to authenticate as the current user.
        /// @param name The name of the API key to be created.
        /// @param completion_block A callback to be invoked once the call is complete.
        void create_api_key(const std::string& name, std::shared_ptr<SyncUser> user,
                            std::function<void(UserAPIKey, Optional<AppError>)> completion_block);

        /// Fetches a user API key associated with the current user.
        /// @param id The id of the API key to fetch.
        /// @param completion_block A callback to be invoked once the call is complete.
        void fetch_api_key(const realm::ObjectId& id, std::shared_ptr<SyncUser> user,
                           std::function<void(UserAPIKey, Optional<AppError>)> completion_block);

        /// Fetches the user API keys associated with the current user.
        /// @param completion_block A callback to be invoked once the call is complete.
        void fetch_api_keys(std::shared_ptr<SyncUser> user,
                            std::function<void(std::vector<UserAPIKey>, Optional<AppError>)> completion_block);

        /// Deletes a user API key associated with the current user.
        /// @param id The id of the API key to delete.
        /// @param user The user to perform this operation.
        /// @param completion_block A callback to be invoked once the call is complete.
        void delete_api_key(const realm::ObjectId& id, std::shared_ptr<SyncUser> user,
                            std::function<void(Optional<AppError>)> completion_block);

        /// Enables a user API key associated with the current user.
        /// @param id The id of the API key to enable.
        /// @param user The user to perform this operation.
        /// @param completion_block A callback to be invoked once the call is complete.
        void enable_api_key(const realm::ObjectId& id, std::shared_ptr<SyncUser> user,
                            std::function<void(Optional<AppError>)> completion_block);

        /// Disables a user API key associated with the current user.
        /// @param id The id of the API key to disable.
        /// @param user The user to perform this operation.
        /// @param completion_block A callback to be invoked once the call is complete.
        void disable_api_key(const realm::ObjectId& id, std::shared_ptr<SyncUser> user,
                             std::function<void(Optional<AppError>)> completion_block);
    private:
        friend class App;
        UserAPIKeyProviderClient(const AuthRequestClient& auth_request_client)
        : m_auth_request_client(auth_request_client)
        {
        }

        std::string url_for_path(const std::string& path) const;
        const AuthRequestClient& m_auth_request_client;
    };

    /// A client for the username/password authentication provider which
    /// can be used to obtain a credential for logging in,
    /// and to perform requests specifically related to the username/password provider.
    ///
    class UsernamePasswordProviderClient {
    public:

        /// Registers a new email identity with the username/password provider,
        /// and sends a confirmation email to the provided address.
        /// @param email The email address of the user to register.
        /// @param password The password that the user created for the new username/password identity.
        /// @param completion_block A callback to be invoked once the call is complete.
        void register_email(const std::string& email,
                            const std::string& password,
                            std::function<void(Optional<AppError>)> completion_block);

        /// Confirms an email identity with the username/password provider.
        /// @param token The confirmation token that was emailed to the user.
        /// @param token_id The confirmation token id that was emailed to the user.
        /// @param completion_block A callback to be invoked once the call is complete.
        void confirm_user(const std::string& token,
                          const std::string& token_id,
                          std::function<void(Optional<AppError>)> completion_block);

        /// Re-sends a confirmation email to a user that has registered but
        /// not yet confirmed their email address.
        /// @param email The email address of the user to re-send a confirmation for.
        /// @param completion_block A callback to be invoked once the call is complete.
        void resend_confirmation_email(const std::string& email,
                                       std::function<void(Optional<AppError>)> completion_block);

        /// Sends a password reset email to the given email address.
        /// @param email The email address of the user to send a password reset email for.
        /// @param completion_block A callback to be invoked once the call is complete.
        void send_reset_password_email(const std::string& email,
                                       std::function<void(Optional<AppError>)> completion_block);

        /// Resets the password of an email identity using the
        /// password reset token emailed to a user.
        /// @param password The desired new password.
        /// @param token The password reset token that was emailed to the user.
        /// @param token_id The password reset token id that was emailed to the user.
        /// @param completion_block A callback to be invoked once the call is complete.
        void reset_password(const std::string& password,
                            const std::string& token,
                            const std::string& token_id,
                            std::function<void(Optional<AppError>)> completion_block);

        /// Resets the password of an email identity using the
        /// password reset function set up in the application.
        ///
        /// TODO: Add an overloaded version of this method that takes
        /// TODO: raw, non-serialized args.
        /// @param email The email address of the user.
        /// @param password The desired new password.
        /// @param args A pre-serialized list of arguments. Must be a JSON array.
        /// @param completion_block A callback to be invoked once the call is complete.
        void call_reset_password_function(const std::string& email,
                                          const std::string& password,
                                          const std::string& args,
                                          std::function<void(Optional<AppError>)> completion_block);
    private:
        friend class App;
        UsernamePasswordProviderClient(App* app)
        : m_parent(app)
        {
            REALM_ASSERT(app);
        }
        App* m_parent;
    };

    /// Log in a user and asynchronously retrieve a user object.
    /// If the log in completes successfully, the completion block will be called, and a
    /// `SyncUser` representing the logged-in user will be passed to it. This user object
    /// can be used to open `Realm`s and retrieve `SyncSession`s. Otherwise, the
    /// completion block will be called with an error.
    ///
    /// @param credentials A `SyncCredentials` object representing the user to log in.
    /// @param completion_block A callback block to be invoked once the log in completes.
    void log_in_with_credentials(const AppCredentials& credentials,
                                 std::function<void(std::shared_ptr<SyncUser>, Optional<AppError>)> completion_block) const;

    /// Logout the current user.
    void log_out(std::function<void(Optional<AppError>)>) const;
            
    /// Refreshes the custom data for a specified user
    /// @param sync_user The user you want to refresh
    void refresh_custom_data(std::shared_ptr<SyncUser> sync_user,
                             std::function<void(Optional<AppError>)>);

    /// Log out the given user if they are not already logged out.
    void log_out(std::shared_ptr<SyncUser> user, std::function<void(Optional<AppError>)> completion_block) const;
    
    /// Links the currently authenticated user with a new identity, where the identity is defined by the credential
    /// specified as a parameter. This will only be successful if this `SyncUser` is the currently authenticated
    /// with the client from which it was created. On success the user will be returned with the new identity.
    ///
    /// @param user The user which will have the credentials linked to, the user must be logged in
    /// @param credentials The `AppCredentials` used to link the user to a new identity.
    /// @param completion_block The completion handler to call when the linking is complete.
    ///                         If the operation is  successful, the result will contain the original
    ///                         `SyncUser` object representing the user.
    void link_user(std::shared_ptr<SyncUser> user, const AppCredentials& credentials,
                   std::function<void(std::shared_ptr<SyncUser>, Optional<AppError>)> completion_block) const;

    /// Switches the active user with the specified one. The user must
    /// exist in the list of all users who have logged into this application, and
    /// the user must be currently logged in, otherwise this will throw an
    /// AppError.
    ///
    /// @param user The user to switch to
    /// @returns A shared pointer to the new current user
    std::shared_ptr<SyncUser> switch_user(std::shared_ptr<SyncUser> user) const;
    
    /// Logs out and removes the provided user
    /// this is a local operation and does not invoke any server side function
    /// @param user the user to remove
    /// @param completion_block Will return an error if the user is not found
    void remove_user(std::shared_ptr<SyncUser> user,
                     std::function<void(Optional<AppError>)> completion_block) const;

    // Get a provider client for the given class type.
    template <class T>
    T provider_client() {
        return T(this);
    }

    /// Retrieves a general-purpose service client for the Realm Cloud service
    /// @param service_name The name of the cluster
    RemoteMongoClient remote_mongo_client(const std::string& service_name) const;
    
private:
    Config m_config;
    std::string m_base_route;
    std::string m_app_route;
    std::string m_auth_route;
    uint64_t m_request_timeout_ms;
    
    /// Refreshes the access token for a specified `SyncUser`
    /// @param completion_block Passes an error should one occur.
    void refresh_access_token(std::shared_ptr<SyncUser> sync_user,
                              std::function<void(Optional<AppError>)> completion_block) const;
    
    /// Checks if an auth failure has taken place and if so it will attempt to refresh the
    /// access token and then perform the orginal request again with the new access token
    /// @param error The error to check for auth failures
    /// @param response The original response to pass back should this not be an auth error
    /// @param request The request to perform
    /// @param completion_block returns the original response in the case it is not an auth error, or if a failure occurs,
    /// if the refresh was a success the newly attempted response will be passed back
    void handle_auth_failure(const AppError& error,
                             const Response& response,
                             Request request,
                             std::shared_ptr<SyncUser> sync_user,
                             std::function<void (Response)> completion_block) const;

    std::string url_for_path(const std::string& path) const override;
    
    /// Performs an authenticated request to the Stitch server, using the current authentication state
    /// @param request The request to be performed
    /// @param completion_block Returns the response from the server
    void do_authenticated_request(Request request,
                                  std::shared_ptr<SyncUser> sync_user,
                                  std::function<void (Response)> completion_block) const override;
        
    
    /// Gets the social profile for a `SyncUser`
    /// @param completion_block Callback will pass the `SyncUser` with the social profile details
    void get_profile(std::shared_ptr<SyncUser> sync_user,
                     std::function<void(std::shared_ptr<SyncUser>, Optional<AppError>)> completion_block) const;
    
    /// Log in a user and asynchronously retrieve a user object.
    /// If the log in completes successfully, the completion block will be called, and a
    /// `SyncUser` representing the logged-in user will be passed to it. This user object
    /// can be used to open `Realm`s and retrieve `SyncSession`s. Otherwise, the
    /// completion block will be called with an error.
    ///
    /// @param credentials A `SyncCredentials` object representing the user to log in.
    /// @param linking_user A `SyncUser` you want to link these credentials too
    /// @param completion_block A callback block to be invoked once the log in completes.
    void log_in_with_credentials(const AppCredentials& credentials,
                                 const std::shared_ptr<SyncUser> linking_user,
                                 std::function<void(std::shared_ptr<SyncUser>, Optional<AppError>)> completion_block) const;

};

// MARK: Provider client templates
template<>
App::UsernamePasswordProviderClient App::provider_client<App::UsernamePasswordProviderClient>();
template<>
App::UserAPIKeyProviderClient App::provider_client<App::UserAPIKeyProviderClient>();

} // namespace app
} // namespace realm

#endif /* REALM_APP_HPP */
