#include <atomic>

#include <realm/disable_sync_to_disk.hpp>

using namespace realm;

namespace {

std::atomic<bool> g_disable_sync_to_disk(false);

} // anonymous namespace

void realm::disable_sync_to_disk()
{
    g_disable_sync_to_disk = true;
}

bool realm::get_disable_sync_to_disk() REALM_NOEXCEPT
{
    return g_disable_sync_to_disk;
}
