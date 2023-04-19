
#ifndef REALM_NOINST_SERVER_IMPL_BASE_HPP
#define REALM_NOINST_SERVER_IMPL_BASE_HPP

#include <realm/sync/protocol.hpp>

namespace realm {
namespace _impl {

class ServerImplBase {
public:
    static constexpr int get_oldest_supported_protocol_version() noexcept;
};

constexpr int ServerImplBase::get_oldest_supported_protocol_version() noexcept
{
    // See sync::get_current_protocol_version() for information about the
    // individual protocol versions.
    return 2;
}

static_assert(ServerImplBase::get_oldest_supported_protocol_version() >= 1, "");
static_assert(ServerImplBase::get_oldest_supported_protocol_version() <= sync::get_current_protocol_version(), "");

} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_SERVER_IMPL_BASE_HPP
