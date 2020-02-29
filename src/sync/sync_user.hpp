////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
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

#ifndef REALM_OS_SYNC_USER_HPP
#define REALM_OS_SYNC_USER_HPP

#include "util/atomic_shared_ptr.hpp"
#include <realm/util/any.hpp>
#include <realm/util/optional.hpp>
#include <realm/table.hpp>

#include "object_schema.hpp"

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace realm {

class SyncSession;

// A superclass that bindings can inherit from in order to store information
// upon a `SyncUser` object.
class SyncUserContext {
public:
    virtual ~SyncUserContext() = default;
};

using SyncUserContextFactory = std::function<std::shared_ptr<SyncUserContext>()>;

// A struct that uniquely identifies a user. Consists of ROS identity and auth server URL.
struct SyncUserIdentifier {
    std::string user_id;
    std::string auth_server_url;

    bool operator==(const SyncUserIdentifier& other) const
    {
        return user_id == other.user_id && auth_server_url == other.auth_server_url;
    }
};

// A struct that decodes a given JWT.
struct RealmJWT {
    // The token being decoded from.
    std::string token;

    // When the token expires.
    long expires_at;
    // When the token was issued.
    long issued_at;
    // Custom user data embedded in the encoded token.
    util::Optional<std::map<std::string, util::Any>> user_data;

    RealmJWT(const std::string& token);

    bool operator==(const RealmJWT& other) const
    {
        return token == other.token;
    }
};

struct SyncUserProfile {
    // The full name of the user.
    util::Optional<std::string> name;
    // The email address of the user.
    util::Optional<std::string> email;
    // A URL to the user's profile picture.
    util::Optional<std::string> picture_url;
    // The first name of the user.
    util::Optional<std::string> first_name;
    // The last name of the user.
    util::Optional<std::string> last_name;
    // The gender of the user.
    util::Optional<std::string> gender;
    // The birthdate of the user.
    util::Optional<std::string> birthday;
    // The minimum age of the user.
    util::Optional<std::string> min_age;
    // The maximum age of the user.
    util::Optional<std::string> max_age;

    SyncUserProfile(util::Optional<std::string> name,
                    util::Optional<std::string> email,
                    util::Optional<std::string> picture_url,
                    util::Optional<std::string> first_name,
                    util::Optional<std::string> last_name,
                    util::Optional<std::string> gender,
                    util::Optional<std::string> birthday,
                    util::Optional<std::string> min_age,
                    util::Optional<std::string> max_age);
    SyncUserProfile() = default;
};

// A struct that represents an identity that a `User` is linked to
struct SyncUserIdentity {
    // the id of the identity
    std::string id;
    // the associated provider type of the identity
    std::string provider_type;

    SyncUserIdentity(const std::string& id, const std::string& provider_type);
};

// A `SyncUser` represents a single user account. Each user manages the sessions that
// are associated with it.
class SyncUser {
friend class SyncSession;
public:
    enum class State {
        LoggedOut,
        Active,
        Error,
    };

    // Don't use this directly; use the `SyncManager` APIs. Public for use with `make_shared`.
    SyncUser(std::string refresh_token,
             std::string identity,
             util::Optional<std::string> server_url,
             std::string access_token);

    // Return a list of all sessions belonging to this user.
    std::vector<std::shared_ptr<SyncSession>> all_sessions();

    // Return a session for a given on disk path.
    // In most cases, bindings shouldn't expose this to consumers, since the on-disk
    // path for a synced Realm is an opaque implementation detail. This API is retained
    // for testing purposes, and for bindings for consumers that are servers or tools.
    std::shared_ptr<SyncSession> session_for_on_disk_path(const std::string& path);

    // Update the user's refresh token. If the user is logged out, it will log itself back in.
    // Note that this is called by the SyncManager, and should not be directly called.
    void update_refresh_token(std::string token);

    // Update the user's access token. If the user is logged out, it will log itself back in.
    // Note that this is called by the SyncManager, and should not be directly called.
    void update_access_token(std::string token);

    // Update the user's profile.
    void update_user_profile(const SyncUserProfile& profile);

    // Update the user's identities.
    void update_identities(std::vector<SyncUserIdentity> identities);

    // Log the user out and mark it as such. This will also close its associated Sessions.
    void log_out();

    std::string const& identity() const noexcept
    {
        return m_identity;
    }

    const std::string& server_url() const noexcept
    {
        return m_server_url;
    }

    const std::string& local_identity() const noexcept
    {
        return m_local_identity;
    }

    std::string access_token() const;

    std::string refresh_token() const;

    SyncUserProfile user_profile() const;

    std::vector<SyncUserIdentity> identities() const;

    // Custom user data embedded in the access token.
    util::Optional<std::map<std::string, util::Any>> custom_data() const;

    State state() const;

    std::shared_ptr<SyncUserContext> binding_context() const
    {
        return m_binding_context.load();
    }

    // Register a session to this user.
    // A registered session will be bound at the earliest opportunity: either
    // immediately, or upon the user becoming Active.
    // Note that this is called by the SyncManager, and should not be directly called.
    void register_session(std::shared_ptr<SyncSession>);

    // Optionally set a context factory. If so, must be set before any sessions are created.
    static void set_binding_context_factory(SyncUserContextFactory factory);

    // Internal APIs. Do not call.
    void register_management_session(const std::string&);
    void register_permission_session(const std::string&);

private:
    static SyncUserContextFactory s_binding_context_factory;
    static std::mutex s_binding_context_factory_mutex;

    State m_state;

    util::AtomicSharedPtr<SyncUserContext> m_binding_context;

    // A locally assigned UUID intended to provide a level of indirection for various features.
    std::string m_local_identity;

    std::weak_ptr<SyncSession> m_management_session;
    std::weak_ptr<SyncSession> m_permission_session;

    // The auth server URL associated with this user. Set upon creation. The empty string for
    // auth token users.
    std::string m_server_url;

    // Mark the user as invalid, since a fatal user-related error was encountered.
    void invalidate();

    mutable std::mutex m_mutex;

    // The user's refresh token.
    RealmJWT m_refresh_token;

    // Set by the server. The unique ID of the user account on the Realm Object Server.
    std::string m_identity;

    // Sessions are owned by the SyncManager, but the user keeps a map of weak references
    // to them.
    std::unordered_map<std::string, std::weak_ptr<SyncSession>> m_sessions;

    // Waiting sessions are those that should be asked to connect once this user is logged in.
    std::unordered_map<std::string, std::weak_ptr<SyncSession>> m_waiting_sessions;

    // The user's access token.
    RealmJWT m_access_token;

    // The identities associated with this user.
    std::vector<SyncUserIdentity> m_user_identities;

    SyncUserProfile m_user_profile;
};

}

namespace std {
template<> struct hash<realm::SyncUserIdentifier> {
    size_t operator()(realm::SyncUserIdentifier const&) const;
};
}

#endif // REALM_OS_SYNC_USER_HPP
