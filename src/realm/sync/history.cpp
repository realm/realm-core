#include <realm/history.hpp>
#include <realm/noinst/client_history_impl.hpp>

namespace realm {
namespace sync {

std::unique_ptr<ClientReplication> make_client_replication(const std::string& realm_path,
                                                   ClientReplication::Config config)
{
    return std::make_unique<_impl::ClientHistoryImpl>(realm_path, std::move(config)); // Throws
}

} // namespace sync
} // namespace realm
