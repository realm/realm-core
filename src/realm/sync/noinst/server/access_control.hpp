#ifndef REALM_SYNC_ACCESS_CONTROL_HPP
#define REALM_SYNC_ACCESS_CONTROL_HPP

#include <realm/string_data.hpp>
#include <realm/sync/noinst/server/access_token.hpp>
#include <realm/sync/noinst/server/crypto_server.hpp>
#include <realm/sync/noinst/server/permissions.hpp>

namespace realm {
namespace sync {

struct AccessControl {
    /// Opens the Realm database at path \a db_path and initializes this
    /// AccessControl object to verify access tokens using \a public_key.
    ///
    /// If \a public_key is not present, access tokens without a signature
    /// will pass verification.
    AccessControl(util::Optional<PKey> public_key);
    ~AccessControl();

    /// Verify a string representing an access token.
    ///
    /// If \a error is non-null, it will be set to indicate the type of failure.
    ///
    /// NOTE: This method is thread-safe.
    util::Optional<AccessToken> verify_access_token(StringData access_token,
                                                    AccessToken::ParseError* error = nullptr) const;

    //@{
    /// Check whether user has the requested permission for the given
    /// Realm file using the particular access token.
    ///
    /// In the version accepting \a mask, it is a bitfield representing
    /// AND'ed values of the `Permission` enum, and it will return true if all
    /// the permissions are granted.
    ///
    /// NOTE: This method is thread-safe.
    bool can(const AccessToken&, Privilege, const RealmFileIdent&) const noexcept;
    bool can(const AccessToken&, unsigned int mask, const RealmFileIdent&) const noexcept;
    //@}

    bool is_admin(const AccessToken&) const noexcept;

    AccessToken::Verifier& verifier() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> m_impl;
};

} // namespace sync
} // namespace realm

#endif // REALM_SYNC_ACCESS_CONTROL_HPP
