#include <realm/history.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>

namespace realm {
namespace sync {

std::unique_ptr<ClientReplication> make_client_replication(const std::string& realm_path)
{
    return std::make_unique<_impl::ClientHistoryImpl>(realm_path); // Throws
}

} // namespace sync
} // namespace realm
