#include <realm/sync/noinst/server/access_control.hpp>

using namespace realm;
using namespace realm::sync;

struct AccessControl::Impl final : public AccessToken::Verifier {
    util::Optional<PKey> m_public_key;

    Impl(util::Optional<PKey> public_key)
        : m_public_key(std::move(public_key))
    {
    }

    // Overriding members of AccessToken::Verifier
    bool verify(BinaryData access_token, BinaryData signature) const override final
    {
        REALM_ASSERT(m_public_key);
        return m_public_key->verify(access_token, signature); // Throws
    }
};

AccessControl::AccessControl(util::Optional<PKey> public_key)
    : m_impl(new Impl(std::move(public_key)))
{
}

AccessControl::~AccessControl() {}

util::Optional<AccessToken> AccessControl::verify_access_token(StringData signed_token,
                                                               AccessToken::ParseError* out_error) const
{
    AccessToken::ParseError error;
    AccessToken token;
    // For the purpose of testing, public key is allowed to be absent. When it
    // is absent, we set `out_error` to
    // `AccessToken::ParseError::invalid_signature` but still pass the parsed
    // token back to the caller.
    AccessToken::Verifier* verifier = nullptr;
    if (REALM_LIKELY(m_impl->m_public_key))
        verifier = &*m_impl;
    if (REALM_LIKELY(AccessToken::parse(signed_token, token, error, verifier))) {
        if (REALM_LIKELY(out_error)) {
            if (REALM_LIKELY(m_impl->m_public_key)) {
                *out_error = AccessToken::ParseError::none;
            }
            else {
                *out_error = AccessToken::ParseError::invalid_signature;
            }
        }
        return token;
    }
    if (out_error)
        *out_error = error;
    return util::none;
}

bool AccessControl::can(const AccessToken& token, Privilege permission,
                        const RealmFileIdent& realm_file) const noexcept
{
    if (token.path && *token.path != realm_file) {
        return false;
    }
    unsigned int p = static_cast<unsigned int>(permission);
    return (token.access & p) == p;
}

bool AccessControl::can(const AccessToken& token, unsigned int mask, const RealmFileIdent& realm_file) const noexcept
{
    if (token.path && *token.path != realm_file) {
        return false;
    }
    return (token.access & mask) == mask;
}

AccessToken::Verifier& AccessControl::verifier() const noexcept
{
    return *m_impl;
}

// This is_admin() function is more complicated than it should be due to
// the current format of the tokens and behavior of ROS.
// This function can be simplified with new a token format.
bool AccessControl::is_admin(const AccessToken& token) const noexcept
{
    if (token.admin_field)
        return token.admin;

    if (!token.path)
        return true;

    // This will catch admins due to the way ROS makes access tokens.
    // It is not safe since it might be too liberal. This function will be
    // replaced as described above.
    if (token.access & (Privilege::ModifySchema | Privilege::SetPermissions))
        return true;

    return false;
}
