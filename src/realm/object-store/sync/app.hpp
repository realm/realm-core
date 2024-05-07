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

#include <realm/object-store/sync/app_config.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/sync/app_service_client.hpp>
#include <realm/object-store/sync/auth_request_client.hpp>
#include <realm/object-store/sync/push_client.hpp>
#include <realm/object-store/sync/subscribable.hpp>
#include <realm/object-store/sync/sync_user.hpp>

#include <realm/object_id.hpp>
#include <realm/sync/config.hpp>
#include <realm/util/checked_mutex.hpp>
#include <realm/util/functional.hpp>

namespace realm {
class SyncSession;
class SyncManager;
class SyncFileManager;

namespace app {

class App;
class MetadataStore;
class User;

typedef std::shared_ptr<App> SharedApp;

/// The `App` has the fundamental set of methods for communicating with a Atlas App Services backend.
///
/// This class provides access to login and authentication.
///
/// You can also use it to execute [Functions](https://docs.mongodb.com/stitch/functions/).
class App : public std::enable_shared_from_this<App>,
            private AuthRequestClient,
            private AppServiceClient,
            public Subscribable<App> {
    struct Private {};

public:
    // MARK: - App Initialization
    enum class CacheMode {
        Enabled, // Return a cached app instance if one was previously generated for `config`'s app_id+base_url combo,
        Disabled // Bypass the app cache; return a new app instance.
    };
    /// Get a shared pointer to a configured App instance. Sync is fully enabled and the external backing store
    /// factory provided is used to create a store if the cache is not used. If you want the
    /// default storage engine, construct a RealmMetadataStore instance in the factory.
    static SharedApp get_app(CacheMode mode, const AppConfig& config);

    // Returns the default base_url for SDKs to use instead of defining their own
    static std::string_view default_base_url();

    /// Return a cached app instance if one was previously generated for the `app_id`+`base_url` combo using
    /// `App::get_app()`.
    /// If base_url is not provided, and there are multiple cached apps with the same app_id but different base_urls,
    /// then a non-determinstic one will be returned.
    ///
    /// Prefer using `App::get_app()` or populating `base_url` to avoid the non-deterministic behavior.
    static SharedApp get_cached_app(const std::string& app_id,
                                    const std::optional<std::string>& base_url = std::nullopt);

    /// Clear the cache used for `get_app(CacheMode::Enable)` and `get_cached_app()`.
    static void clear_cached_apps();

    explicit App(Private, const AppConfig& config);
    App(App&&) noexcept = delete;
    App& operator=(App&&) noexcept = delete;
    ~App();

    const AppConfig& config() const
    {
        return m_config;
    }

    const std::string& app_id() const noexcept
    {
        return m_config.app_id;
    }

    // MARK: - Other objects owned by App
    const std::shared_ptr<SyncManager>& sync_manager() const
    {
        return m_sync_manager;
    }

    std::shared_ptr<AuthRequestClient> auth_request_client()
    {
        return std::shared_ptr<AuthRequestClient>(shared_from_this(), this);
    }

    std::shared_ptr<AppServiceClient> app_service_client()
    {
        return std::shared_ptr<AppServiceClient>(shared_from_this(), this);
    }

    // MARK: - User Management

    /// Get the last used user.
    std::shared_ptr<User> current_user() REQUIRES(!m_user_mutex);
    /// Get the user object for the given `user_id` if a user with that id is logged in, or nullptr if not.
    std::shared_ptr<User> get_existing_logged_in_user(std::string_view user_id) REQUIRES(!m_user_mutex);
    /// Get all users.
    std::vector<std::shared_ptr<User>> all_users() REQUIRES(!m_user_mutex);
    /// Set the current user to the given one. The user must be logged in and have been obtained from this `App`
    /// instance.
    void switch_user(const std::shared_ptr<User>& user) REQUIRES(!m_user_mutex);

    /// Log in a user and asynchronously retrieve a user object.
    /// If the log in completes successfully, the completion block will be called, and a
    /// `User` representing the logged-in user will be passed to it. This user object
    /// can be used to open `Realm`s and retrieve `SyncSession`s. Otherwise, the
    /// completion block will be called with an error.
    ///
    /// @param credentials An `AppCredentials` object representing the user to log in.
    /// @param completion A callback block to be invoked once the log in completes.
    void log_in_with_credentials(
        const AppCredentials& credentials,
        util::UniqueFunction<void(const std::shared_ptr<User>&, std::optional<AppError>)>&& completion)
        REQUIRES(!m_route_mutex, !m_user_mutex);

    /// Logout the current user.
    void log_out(util::UniqueFunction<void(std::optional<AppError>)>&&) REQUIRES(!m_route_mutex, !m_user_mutex);

    /// Refreshes the custom data for a specified user
    /// @param user The user you want to refresh
    /// @param update_location If true, the location metadata will be updated before refresh
    void refresh_custom_data(const std::shared_ptr<User>& user, bool update_location,
                             util::UniqueFunction<void(std::optional<AppError>)>&& completion)
        REQUIRES(!m_route_mutex);
    void refresh_custom_data(const std::shared_ptr<User>& user,
                             util::UniqueFunction<void(std::optional<AppError>)>&& completion)
        REQUIRES(!m_route_mutex);

    /// Log out the given user if they are not already logged out.
    void log_out(const std::shared_ptr<User>& user, util::UniqueFunction<void(std::optional<AppError>)>&& completion)
        REQUIRES(!m_route_mutex);

    /// Links the currently authenticated user with a new identity, where the identity is defined by the credential
    /// specified as a parameter. This will only be successful if this `User` is the currently authenticated
    /// with the client from which it was created. On success the user will be returned with the new identity.
    ///
    /// @param user The user which will have the credentials linked to, the user must be logged in
    /// @param credentials The `AppCredentials` used to link the user to a new identity.
    /// @param completion The completion handler to call when the linking is complete.
    ///                         If the operation is  successful, the result will contain the original
    ///                         `User` object representing the user.
    void link_user(const std::shared_ptr<User>& user, const AppCredentials& credentials,
                   util::UniqueFunction<void(const std::shared_ptr<User>&, std::optional<AppError>)>&& completion)
        REQUIRES(!m_route_mutex, !m_user_mutex);


    /// Logs out and removes the provided user.
    /// This invokes logout on the server.
    /// @param user the user to remove
    /// @param completion Will return an error if the user is not found or the http request failed.
    void remove_user(const std::shared_ptr<User>& user,
                     util::UniqueFunction<void(std::optional<AppError>)>&& completion)
        REQUIRES(!m_route_mutex, !m_user_mutex);

    /// Deletes a user and all its data from the server.
    /// @param user The user to delete
    /// @param completion Will return an error if the user is not found or the http request failed.
    void delete_user(const std::shared_ptr<User>& user,
                     util::UniqueFunction<void(std::optional<AppError>)>&& completion)
        REQUIRES(!m_route_mutex, !m_user_mutex);

    /// Creates a fake user with the provided access and refresh tokens. No validation is done to ensure that the
    /// credentials are actually valid and as such, this should only be used for testing purposes.
    /// @param user_id The id of the user that will be created
    /// @param access_token The access token of the user
    /// @param refresh_token The refresh token of the user
    std::shared_ptr<User> create_fake_user_for_testing(const std::string& user_id, const std::string& access_token,
                                                       const std::string& refresh_token) REQUIRES(!m_user_mutex);

    // MARK: - Provider Clients

    /// A struct representing a user API key as returned by the App server.
    struct UserAPIKey {
        // The ID of the key.
        ObjectId id;

        /// The actual key. Will only be included in
        /// the response when an API key is first created.
        std::optional<std::string> key;

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
        /// @param completion A callback to be invoked once the call is complete.
        void create_api_key(const std::string& name, const std::shared_ptr<User>& user,
                            util::UniqueFunction<void(UserAPIKey&&, std::optional<AppError>)>&& completion);

        /// Fetches a user API key associated with the current user.
        /// @param id The id of the API key to fetch.
        /// @param completion A callback to be invoked once the call is complete.
        void fetch_api_key(const realm::ObjectId& id, const std::shared_ptr<User>& user,
                           util::UniqueFunction<void(UserAPIKey&&, std::optional<AppError>)>&& completion);

        /// Fetches the user API keys associated with the current user.
        /// @param completion A callback to be invoked once the call is complete.
        void
        fetch_api_keys(const std::shared_ptr<User>& user,
                       util::UniqueFunction<void(std::vector<UserAPIKey>&&, std::optional<AppError>)>&& completion);

        /// Deletes a user API key associated with the current user.
        /// @param id The id of the API key to delete.
        /// @param user The user to perform this operation.
        /// @param completion A callback to be invoked once the call is complete.
        void delete_api_key(const realm::ObjectId& id, const std::shared_ptr<User>& user,
                            util::UniqueFunction<void(std::optional<AppError>)>&& completion);

        /// Enables a user API key associated with the current user.
        /// @param id The id of the API key to enable.
        /// @param user The user to perform this operation.
        /// @param completion A callback to be invoked once the call is complete.
        void enable_api_key(const realm::ObjectId& id, const std::shared_ptr<User>& user,
                            util::UniqueFunction<void(std::optional<AppError>)>&& completion);

        /// Disables a user API key associated with the current user.
        /// @param id The id of the API key to disable.
        /// @param user The user to perform this operation.
        /// @param completion A callback to be invoked once the call is complete.
        void disable_api_key(const realm::ObjectId& id, const std::shared_ptr<User>& user,
                             util::UniqueFunction<void(std::optional<AppError>)>&& completion);

    private:
        friend class App;
        UserAPIKeyProviderClient(AuthRequestClient& auth_request_client)
            : m_auth_request_client(auth_request_client)
        {
        }

        std::string url_for_path(const std::string& path) const;
        AuthRequestClient& m_auth_request_client;
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
        /// @param completion A callback to be invoked once the call is complete.
        void register_email(const std::string& email, const std::string& password,
                            util::UniqueFunction<void(std::optional<AppError>)>&& completion);

        /// Confirms an email identity with the username/password provider.
        /// @param token The confirmation token that was emailed to the user.
        /// @param token_id The confirmation token id that was emailed to the user.
        /// @param completion A callback to be invoked once the call is complete.
        void confirm_user(const std::string& token, const std::string& token_id,
                          util::UniqueFunction<void(std::optional<AppError>)>&& completion);

        /// Re-sends a confirmation email to a user that has registered but
        /// not yet confirmed their email address.
        /// @param email The email address of the user to re-send a confirmation for.
        /// @param completion A callback to be invoked once the call is complete.
        void resend_confirmation_email(const std::string& email,
                                       util::UniqueFunction<void(std::optional<AppError>)>&& completion);

        void send_reset_password_email(const std::string& email,
                                       util::UniqueFunction<void(std::optional<AppError>)>&& completion);

        /// Retries the custom confirmation function on a user for a given email.
        /// @param email The email address of the user to retry the custom confirmation for.
        /// @param completion A callback to be invoked once the retry is complete.
        void retry_custom_confirmation(const std::string& email,
                                       util::UniqueFunction<void(std::optional<AppError>)>&& completion);

        /// Resets the password of an email identity using the
        /// password reset token emailed to a user.
        /// @param password The desired new password.
        /// @param token The password reset token that was emailed to the user.
        /// @param token_id The password reset token id that was emailed to the user.
        /// @param completion A callback to be invoked once the call is complete.
        void reset_password(const std::string& password, const std::string& token, const std::string& token_id,
                            util::UniqueFunction<void(std::optional<AppError>)>&& completion);

        /// Resets the password of an email identity using the
        /// password reset function set up in the application.
        /// @param email The email address of the user.
        /// @param password The desired new password.
        /// @param args A bson array of arguments.
        /// @param completion A callback to be invoked once the call is complete.
        void call_reset_password_function(const std::string& email, const std::string& password,
                                          const bson::BsonArray& args,
                                          util::UniqueFunction<void(std::optional<AppError>)>&& completion);

    private:
        friend class App;
        UsernamePasswordProviderClient(SharedApp app)
            : m_parent(app)
        {
            REALM_ASSERT(app);
        }
        SharedApp m_parent;
    };


    // Get a provider client for the given class type.
    template <class T>
    T provider_client()
    {
        return T(this);
    }

    // MARK: - App Services

    // Return the base url path used for HTTP AppServices requests
    std::string get_host_url() REQUIRES(!m_route_mutex);

    /// Get the current base URL for the AppServices server used for http requests and sync
    /// connections.
    /// If an update_base_url() operation is currently in progress, this value will not be
    /// updated with the new value until that operation is complete.
    /// @return String containing the current base url value
    std::string get_base_url() const REQUIRES(!m_route_mutex);

    /// Update the base URL after the app has been created. The location info will be retrieved
    /// using the provided base URL. If this operation fails, the app will continue to use the original base URL and
    /// the error will be provided to the completion callback. If the operation is successful, the app and sync
    /// client will use the new location info for future connections.
    /// NOTE: If another App operation is started while this function is in progress, that request will use the
    ///       original base URL location information.
    /// @param base_url The new base URL to use for future AppServices requests and sync websocket connections. If
    ///                 an empty string, the default Device Sync base_url will be used.
    /// @param completion A callback block to be invoked once the location update completes.
    void update_base_url(std::string_view base_url, util::UniqueFunction<void(util::Optional<AppError>)>&& completion)
        REQUIRES(!m_route_mutex);

    void call_function(const std::shared_ptr<User>& user, const std::string& name, std::string_view args_ejson,
                       const std::optional<std::string>& service_name,
                       util::UniqueFunction<void(const std::string*, std::optional<AppError>)>&& completion) final
        REQUIRES(!m_route_mutex);

    void
    call_function(const std::shared_ptr<User>& user, const std::string& name, const bson::BsonArray& args_bson,
                  const std::optional<std::string>& service_name,
                  util::UniqueFunction<void(std::optional<bson::Bson>&&, std::optional<AppError>)>&& completion) final
        REQUIRES(!m_route_mutex);

    void
    call_function(const std::shared_ptr<User>& user, const std::string&, const bson::BsonArray& args_bson,
                  util::UniqueFunction<void(std::optional<bson::Bson>&&, std::optional<AppError>)>&& completion) final
        REQUIRES(!m_route_mutex);

    void
    call_function(const std::string& name, const bson::BsonArray& args_bson,
                  const std::optional<std::string>& service_name,
                  util::UniqueFunction<void(std::optional<bson::Bson>&&, std::optional<AppError>)>&& completion) final
        REQUIRES(!m_route_mutex, !m_user_mutex);

    void
    call_function(const std::string&, const bson::BsonArray& args_bson,
                  util::UniqueFunction<void(std::optional<bson::Bson>&&, std::optional<AppError>)>&& completion) final
        REQUIRES(!m_route_mutex, !m_user_mutex);

    template <typename T>
    void call_function(const std::shared_ptr<User>& user, const std::string& name, const bson::BsonArray& args_bson,
                       util::UniqueFunction<void(std::optional<T>&&, std::optional<AppError>)>&& completion)
        REQUIRES(!m_route_mutex)
    {
        call_function(
            user, name, args_bson, util::none,
            [completion = std::move(completion)](std::optional<bson::Bson>&& value, std::optional<AppError> error) {
                if (value) {
                    return completion(util::some<T>(static_cast<T>(*value)), std::move(error));
                }

                return completion(util::none, std::move(error));
            });
    }

    template <typename T>
    void call_function(const std::string& name, const bson::BsonArray& args_bson,
                       util::UniqueFunction<void(std::optional<T>&&, std::optional<AppError>)>&& completion)
        REQUIRES(!m_route_mutex, !m_user_mutex)
    {

        call_function(current_user(), name, args_bson, std::move(completion));
    }

    // NOTE: only sets "Accept: text/event-stream" header. If you use an API that sets that but doesn't support
    // setting other headers (eg. EventSource() in JS), you can ignore the headers field on the request.
    Request make_streaming_request(const std::shared_ptr<User>& user, const std::string& name,
                                   const bson::BsonArray& args_bson,
                                   const std::optional<std::string>& service_name) const REQUIRES(!m_route_mutex);

    PushClient push_notification_client(const std::string& service_name);

    // MARK: - Sync

    // Immediately close all open sync sessions for all cached apps.
    // Used by JS SDK to ensure no sync clients remain open when a developer
    // reloads an app (#5411).
    static void close_all_sync_sessions();

    // Return the base url path used for Sync Session Websocket requests
    std::string get_ws_host_url() REQUIRES(!m_route_mutex);

    static std::string create_ws_host_url(const std::string_view host_url);

    // Get the default path for a Realm for the given configuration.
    // The default value is `<rootDir>/<appId>/<userId>/<partitionValue>.realm`.
    // If the file cannot be created at this location, for example due to path length restrictions,
    // this function may pass back `<rootDir>/<hashedFileName>.realm`
    std::string path_for_realm(const SyncConfig& config,
                               std::optional<std::string> custom_file_name = std::nullopt) const;

    // Attempt to perform all pending file actions for the given path. Returns
    // true if any were performed.
    bool immediately_run_file_actions(std::string_view realm_path);

private:
    const AppConfig m_config;

    util::CheckedMutex m_route_mutex;
    // The following variables hold the different paths to Atlas, depending on the
    // request being performed
    // Base hostname from config.base_url or update_base_url() for querying location info
    // (e.g. "https://services.cloud.mongodb.com")
    std::string m_base_url GUARDED_BY(m_route_mutex);
    // Baseline URL for AppServices and Device Sync requests
    // (e.g. "https://us-east-1.aws.services.cloud.mongodb.com/api/client/v2.0" or
    // "wss://us-east-1.aws.ws.services.cloud.mongodb.com/api/client/v2.0")
    std::string m_base_route GUARDED_BY(m_route_mutex);
    // URL for app-based AppServices and Device Sync requests using config.app_id
    // (e.g. "https://us-east-1.aws.services.cloud.mongodb.com/api/client/v2.0/app/<app_id>"
    // or "wss://us-east-1.aws.ws.services.cloud.mongodb.com/api/client/v2.0/app/<app_id>")
    std::string m_app_route GUARDED_BY(m_route_mutex);
    // URL for app-based AppServices authentication requests (e.g. email/password)
    // (e.g. "https://us-east-1.aws.services.cloud.mongodb.com/api/client/v2.0/app/<app_id>/auth")
    std::string m_auth_route GUARDED_BY(m_route_mutex);
    // If false, the location info will be updated upon the next AppServices request
    bool m_location_updated GUARDED_BY(m_route_mutex) = false;
    // Storage for the location info returned by the base URL location endpoint
    // Base hostname for AppServices HTTP requests
    std::string m_host_url GUARDED_BY(m_route_mutex);
    // Base hostname for Device Sync websocket requests
    std::string m_ws_host_url GUARDED_BY(m_route_mutex);

    const uint64_t m_request_timeout_ms;
    std::unique_ptr<SyncFileManager> m_file_manager;
    std::unique_ptr<MetadataStore> m_metadata_store;
    std::shared_ptr<SyncManager> m_sync_manager;
    std::shared_ptr<util::Logger> m_logger_ptr;

    /// m_logger_ptr is not set until the first call to one of these functions.
    /// If configure() not been called, a logger will not be available yet.
    /// @returns true if the logger was set, otherwise false.
    bool init_logger();
    /// These helpers prevent all the checks for if(m_logger_ptr) throughout the
    /// code.
    bool would_log(util::Logger::Level level);
    template <class... Params>
    void log_debug(const char* message, Params&&... params);
    template <class... Params>
    void log_error(const char* message, Params&&... params);

    /// Refreshes the access token for a specified `User`
    /// @param completion Passes an error should one occur.
    /// @param update_location If true, the location metadata will be updated before refresh
    void refresh_access_token(const std::shared_ptr<User>& user, bool update_location,
                              util::UniqueFunction<void(std::optional<AppError>)>&& completion)
        REQUIRES(!m_route_mutex);

    /// The completion type for all intermediate operations which occur before performing the original request
    using IntermediateCompletion = util::UniqueFunction<void(std::unique_ptr<Request>&&, const Response&)>;

    /// Checks if an auth failure has taken place and if so it will attempt to refresh the
    /// access token and then perform the orginal request again with the new access token
    /// @param error The error to check for auth failures
    /// @param request The request to perform
    /// @param completion returns the original response in the case it is not an auth error, or if a failure
    /// occurs, if the refresh was a success the newly attempted response will be passed back
    void handle_auth_failure(const AppError& error, std::unique_ptr<Request>&& request, const Response& response,
                             const std::shared_ptr<User>& user, RequestTokenType token_type,
                             util::UniqueFunction<void(const Response&)>&& completion) REQUIRES(!m_route_mutex);

    std::string url_for_path(const std::string& path) const override REQUIRES(!m_route_mutex);

    /// Return the app route for this App instance, or creates a new app route string if
    /// a new hostname is provided
    /// @param hostname The hostname to generate a new app route
    std::string get_app_route(const std::optional<std::string>& hostname = util::none) const REQUIRES(m_route_mutex);

    /// Request the app metadata information from the server if it has not been processed yet. If
    /// a new hostname is provided, the app metadata will be refreshed using the new hostname.
    /// @param completion The callback that will be called with the error on failure or empty on success
    /// @param new_hostname The (Original) new hostname to request the location from
    /// @param redir_location The location provided by the last redirect response when querying location
    /// @param redirect_count The current number of redirects that have occurred in a row
    void request_location(util::UniqueFunction<void(std::optional<AppError>)>&& completion,
                          std::optional<std::string>&& new_hostname = std::nullopt,
                          std::optional<std::string>&& redir_location = std::nullopt, int redirect_count = 0)
        REQUIRES(!m_route_mutex);

    /// Update the location metadata from the location response
    /// @param response The response returned from the location request
    /// @param base_url The base URL to use when setting the location metadata
    /// @return std::nullopt if the updated was successful, otherwise an AppError with the error
    std::optional<AppError> update_location(const Response& response, const std::string& base_url)
        REQUIRES(!m_route_mutex);

    /// Update the app metadata and resend the request with the updated metadata
    /// @param request The original request object that needs to be sent after the update
    /// @param completion The original completion object that will be called with the response to the request
    /// @param new_hostname If provided, the metadata will be requested from this hostname
    void update_location_and_resend(std::unique_ptr<Request>&& request, IntermediateCompletion&& completion,
                                    std::optional<std::string>&& new_hostname = util::none) REQUIRES(!m_route_mutex);

    void post(std::string&& route, util::UniqueFunction<void(std::optional<AppError>)>&& completion,
              const bson::BsonDocument& body) REQUIRES(!m_route_mutex);

    /// Performs a request to the Stitch server. This request does not contain authentication state.
    /// @param request The request to be performed
    /// @param completion Returns the response from the server
    /// @param update_location Force the location metadata to be updated prior to sending the request
    void do_request(std::unique_ptr<Request>&& request, IntermediateCompletion&& completion,
                    bool update_location = false) REQUIRES(!m_route_mutex);

    std::unique_ptr<Request> make_request(HttpMethod method, std::string&& url, const std::shared_ptr<User>& user,
                                          RequestTokenType, std::string&& body) const;

    /// Process the redirect response received from the last request that was sent to the server
    /// @param request The request to be performed (in case it needs to be sent again)
    /// @param response The response from the send_request_to_server operation
    /// @param completion Returns the response from the server if not a redirect
    void check_for_redirect_response(std::unique_ptr<Request>&& request, const Response& response,
                                     IntermediateCompletion&& completion) REQUIRES(!m_route_mutex);

    void do_authenticated_request(HttpMethod, std::string&& route, std::string&& body,
                                  const std::shared_ptr<User>& user, RequestTokenType,
                                  util::UniqueFunction<void(const Response&)>&&) override REQUIRES(!m_route_mutex);


    /// Gets the social profile for a `User`.
    ///
    /// @param completion Callback will pass the `User` with the social profile details
    void get_profile(const std::shared_ptr<User>& user,
                     util::UniqueFunction<void(const std::shared_ptr<User>&, std::optional<AppError>)>&& completion)
        REQUIRES(!m_route_mutex);

    /// Log in a user and asynchronously retrieve a user object.
    /// If the log in completes successfully, the completion block will be called, and a
    /// `User` representing the logged-in user will be passed to it. This user object
    /// can be used to open `Realm`s and retrieve `SyncSession`s. Otherwise, the
    /// completion block will be called with an error.
    ///
    /// @param credentials An `AppCredentials` object representing the user to log in.
    /// @param linking_user A `User` you want to link these credentials too
    /// @param completion A callback block to be invoked once the log in completes.
    void log_in_with_credentials(
        const AppCredentials& credentials, const std::shared_ptr<User>& linking_user,
        util::UniqueFunction<void(const std::shared_ptr<User>&, std::optional<AppError>)>&& completion)
        REQUIRES(!m_route_mutex, !m_user_mutex);

    /// Provides MongoDB Realm Cloud with metadata related to the users session
    void attach_auth_options(bson::BsonDocument& body);

    std::string function_call_url_path() const REQUIRES(!m_route_mutex);

    static SharedApp do_get_app(CacheMode mode, const AppConfig& config,
                                util::FunctionRef<void(SharedApp)> do_config);

    void configure_backing_store(std::unique_ptr<MetadataStore> store) REQUIRES(!m_route_mutex);

    std::string make_sync_route(util::Optional<std::string> ws_host_url = util::none) REQUIRES(m_route_mutex);
    void configure_route(const std::string& host_url, const std::string& ws_host_url) REQUIRES(m_route_mutex);
    void update_hostname(const std::string& host_url, const std::string& ws_host_url, const std::string& new_base_url)
        REQUIRES(m_route_mutex);
    std::string auth_route() REQUIRES(!m_route_mutex);
    std::string base_url() REQUIRES(!m_route_mutex);

    bool verify_user_present(const std::shared_ptr<User>& user) const REQUIRES(m_user_mutex);

    // UserProvider implementation
    friend class User;

    util::CheckedMutex m_user_mutex;
    mutable std::unordered_map<std::string, User*> m_users GUARDED_BY(m_user_mutex);
    User* m_current_user GUARDED_BY(m_user_mutex) = nullptr;

    void register_sync_user(User& sync_user) REQUIRES(m_user_mutex);
    void unregister_sync_user(User& user) REQUIRES(!m_user_mutex);

    // user helpers
    std::shared_ptr<User> get_user_for_id(const std::string& user_id) REQUIRES(m_user_mutex);
    void user_data_updated(const std::string& user_id) REQUIRES(m_user_mutex);
    void log_out(const std::shared_ptr<User>& user, SyncUser::State new_state,
                 util::UniqueFunction<void(std::optional<AppError>)>&& completion) REQUIRES(!m_route_mutex);
};

// MARK: Provider client templates
template <>
App::UsernamePasswordProviderClient App::provider_client<App::UsernamePasswordProviderClient>();
template <>
App::UserAPIKeyProviderClient App::provider_client<App::UserAPIKeyProviderClient>();

} // namespace app
} // namespace realm

#endif /* REALM_APP_HPP */
