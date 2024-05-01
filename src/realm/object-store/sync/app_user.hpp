////////////////////////////////////////////////////////////////////////////
//
// Copyright 2024 Realm Inc.
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

#ifndef REALM_OS_APP_USER_HPP
#define REALM_OS_APP_USER_HPP

#include <realm/object-store/sync/sync_user.hpp>
#include <realm/object-store/sync/subscribable.hpp>
#include <realm/util/bson/bson.hpp>
#include <realm/util/checked_mutex.hpp>
#include <realm/util/function_ref.hpp>

#include <memory>
#include <string>
#include <vector>

namespace realm {
struct SyncConfig;
}

namespace realm::app {
class App;
struct AppError;
class MetadataStore;
class MongoClient;

struct UserProfile {
    // The full name of the user.
    std::optional<std::string> name() const
    {
        return get_field("name");
    }
    // The email address of the user.
    std::optional<std::string> email() const
    {
        return get_field("email");
    }
    // A URL to the user's profile picture.
    std::optional<std::string> picture_url() const
    {
        return get_field("picture_url");
    }
    // The first name of the user.
    std::optional<std::string> first_name() const
    {
        return get_field("first_name");
    }
    // The last name of the user.
    std::optional<std::string> last_name() const
    {
        return get_field("last_name");
    }
    // The gender of the user.
    std::optional<std::string> gender() const
    {
        return get_field("gender");
    }
    // The birthdate of the user.
    std::optional<std::string> birthday() const
    {
        return get_field("birthday");
    }
    // The minimum age of the user.
    std::optional<std::string> min_age() const
    {
        return get_field("min_age");
    }
    // The maximum age of the user.
    std::optional<std::string> max_age() const
    {
        return get_field("max_age");
    }

    bson::Bson operator[](const std::string& key) const
    {
        return m_data.at(key);
    }

    const bson::BsonDocument& data() const
    {
        return m_data;
    }

    UserProfile(bson::BsonDocument&& data)
        : m_data(std::move(data))
    {
    }
    UserProfile() = default;

private:
    bson::BsonDocument m_data;

    std::optional<std::string> get_field(const char* name) const
    {
        if (auto val = m_data.find(name)) {
            return static_cast<std::string>((*val));
        }
        return util::none;
    }
};

// A struct that represents an identity that a `User` is linked to
struct UserIdentity {
    // the id of the identity
    std::string id;
    // the associated provider type of the identity
    std::string provider_type;

    UserIdentity(const std::string& id, const std::string& provider_type);

    bool operator==(const UserIdentity& other) const
    {
        return id == other.id && provider_type == other.provider_type;
    }

    bool operator!=(const UserIdentity& other) const
    {
        return id != other.id || provider_type != other.provider_type;
    }
};

struct UserData {
    // Current refresh token or empty if user is logged out
    RealmJWT refresh_token;
    // Current access token or empty if user is logged out
    RealmJWT access_token;
    // UUIDs which used to be used to generate local Realm file paths. Now only
    // used to locate existing files.
    std::vector<std::string> legacy_identities;
    // Identities which were used to log into this user
    std::vector<UserIdentity> identities;
    // Id for the device which this user was logged in on. Users are not
    // portable between devices so this cannot be changed after the user
    // is created
    std::string device_id;
    // Server-stored user profile
    UserProfile profile;
};

class User final : public SyncUser, public std::enable_shared_from_this<User>, public Subscribable<User> {
    struct Private {};

public:
    // ------------------------------------------------------------------------
    // SyncUser implementation

    std::string user_id() const noexcept override;
    std::string app_id() const noexcept override;
    std::vector<std::string> legacy_identities() const override REQUIRES(!m_mutex);

    std::string access_token() const override REQUIRES(!m_mutex);
    std::string refresh_token() const override REQUIRES(!m_mutex);
    SyncUser::State state() const override REQUIRES(!m_mutex);

    /// Checks the expiry on the access token against the local time and if it is invalid or expires soon, returns
    /// true.
    bool access_token_refresh_required() const override REQUIRES(!m_mutex);

    SyncManager* sync_manager() override REQUIRES(!m_mutex);
    void request_log_out() override REQUIRES(!m_mutex);
    void request_refresh_location(util::UniqueFunction<void(std::optional<app::AppError>)>&&) override
        REQUIRES(!m_mutex);
    void request_access_token(util::UniqueFunction<void(std::optional<app::AppError>)>&&) override REQUIRES(!m_mutex);

    void track_realm(std::string_view path) override REQUIRES(!m_mutex);
    std::string create_file_action(SyncFileAction action, std::string_view original_path,
                                   std::optional<std::string> requested_recovery_dir) override REQUIRES(!m_mutex);

    // ------------------------------------------------------------------------
    // SDK public API

    /// Returns true if the user's only identity is anonymous.
    bool is_anonymous() const REQUIRES(!m_mutex);

    std::string device_id() const REQUIRES(!m_mutex);
    bool has_device_id() const REQUIRES(!m_mutex);
    UserProfile user_profile() const REQUIRES(!m_mutex);
    std::vector<UserIdentity> identities() const REQUIRES(!m_mutex);

    // Custom user data embedded in the access token.
    std::optional<bson::BsonDocument> custom_data() const REQUIRES(!m_mutex);

    // Get the app instance that this user belongs to.
    std::shared_ptr<app::App> app() const REQUIRES(!m_mutex);

    /// Retrieves a general-purpose service client for the Realm Cloud service
    /// @param service_name The name of the cluster
    app::MongoClient mongo_client(const std::string& service_name) REQUIRES(!m_mutex);

    // Log the user out and mark it as such. This will also close its associated Sessions.
    void log_out() REQUIRES(!m_mutex);

    // Get the default path for a Realm for the given configuration.
    // The default value is `<rootDir>/<appId>/<userId>/<partitionValue>.realm`.
    // If the file cannot be created at this location, for example due to path length restrictions,
    // this function may pass back `<rootDir>/<hashedFileName>.realm`
    std::string path_for_realm(const SyncConfig& config,
                               std::optional<std::string> custom_file_name = std::nullopt) const REQUIRES(!m_mutex);

    // ------------------------------------------------------------------------
    // All of the following are called by `RealmMetadataStore` and are public only for
    // testing purposes. SDKs should not call these directly in non-test code
    // or expose them in the public API.

    static std::shared_ptr<User> make(std::shared_ptr<app::App> app, std::string_view user_id)
    {
        return std::make_shared<User>(Private(), std::move(app), user_id);
    }

    User(Private, std::shared_ptr<app::App> app, std::string_view user_id);
    ~User();

    void update_backing_data(std::optional<UserData>&& data) REQUIRES(!m_mutex);
    void update_data_for_testing(util::FunctionRef<void(UserData&)>) REQUIRES(!m_mutex);
    void detach_and_tear_down() REQUIRES(!m_mutex);

    /// Refreshes the custom data for this user
    /// If `update_location` is true, the location metadata will be queried before the request
    void refresh_custom_data(bool update_location,
                             util::UniqueFunction<void(std::optional<app::AppError>)> completion_block)
        REQUIRES(!m_mutex);
    void refresh_custom_data(util::UniqueFunction<void(std::optional<app::AppError>)> completion_block)
        REQUIRES(!m_mutex);

    // Hook for testing access token timeouts
    void set_seconds_to_adjust_time_for_testing(int seconds)
    {
        m_seconds_to_adjust_time_for_testing.store(seconds);
    }

private:
    util::CheckedMutex m_mutex;
    std::shared_ptr<App> m_app GUARDED_BY(m_mutex);
    const std::string m_app_id;
    const std::string m_user_id;
    UserData m_data GUARDED_BY(m_mutex);
    std::atomic<int> m_seconds_to_adjust_time_for_testing = 0;

    bool do_is_anonymous() const REQUIRES(m_mutex);
};

} // namespace realm::app

namespace std {
template <>
struct hash<realm::app::UserIdentity> {
    size_t operator()(realm::app::UserIdentity const&) const;
};
} // namespace std

#endif // REALM_OS_SYNC_USER_HPP
