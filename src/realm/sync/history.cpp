#include <realm/history.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>

namespace realm {
namespace sync {

std::unique_ptr<ClientReplication> make_client_replication()
{
    return std::make_unique<ClientReplication>(); // Throws
}

} // namespace sync
} // namespace realm
