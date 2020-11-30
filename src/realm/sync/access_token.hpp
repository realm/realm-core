#ifndef REALM_SYNC_ACCESS_TOKEN_HPP
#define REALM_SYNC_ACCESS_TOKEN_HPP

#include <cstdint>
#include <string>
#include <chrono>

#include <realm/util/optional.hpp>
#include <realm/string_data.hpp>
#include <realm/binary_data.hpp>

namespace realm {
namespace sync {

using UserIdent = std::string;
using AppIdent = std::string;
using SyncLabel = std::string;
using RealmFileIdent = std::string; // path

struct AccessToken {
    class Verifier;

    enum class ParseError {
        none = 0, // std::error_code compatibility
        invalid_base64,
        invalid_json,
        invalid_signature,
        invalid_jwt,
    };

    UserIdent identity;

    // If the admin_field is absent in the token, the token is of the old type.
    //
    // FIXME: Remove this field later.
    bool admin_field = false;
    bool admin = false;

    AppIdent app_id;

    // The label used for load balancing. It is only used by the server to
    // implement the LoadBalancing feature gating.
    util::Optional<SyncLabel> sync_label;

    /// If the access token is missing a 'path' field, the permissions encoded
    /// therein are presumed to be valid for ALL paths! I.e. the user is an
    /// admin or global listener.
    util::Optional<RealmFileIdent> path;

    /// The number of seconds since Jan 1 00:00:00 UTC 1970 (UNIX epoch)
    /// according to the Gregorian calendar, and while not taking leap seconds
    /// into account. This agrees with the definition of UNIX time. For example,
    /// 1483257600 means Jan 1 00:00:00 PST 2017.
    std::int_fast64_t timestamp = 0;
    std::int_fast64_t expires = 0;

    std::uint_least32_t access = 0; // bitfield, see sync::PrivilegeLevel

    bool expired(std::chrono::system_clock::time_point now) const noexcept;

    static bool parseJWT(StringData signed_access_token, AccessToken&, ParseError&, Verifier* = nullptr);

    static bool parse(StringData signed_access_token, AccessToken&, ParseError&, Verifier* = nullptr);
};


class AccessToken::Verifier {
public:
    virtual bool verify(BinaryData access_token, BinaryData signature) const = 0;

protected:
    ~Verifier() = default;
};


// Implementation

inline bool AccessToken::expired(std::chrono::system_clock::time_point now) const noexcept
{
    if (!expires)
        return false;

    return now > std::chrono::system_clock::time_point{std::chrono::seconds{expires}};
}

} // namespace sync
} // namespace realm

#endif // REALM_SYNC_ACCESS_TOKEN_HPP
