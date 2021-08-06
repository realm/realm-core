#include <realm/sync/noinst/client_file_access_cache.hpp>

using namespace realm;
using namespace _impl;


void ClientFileAccessCache::access(Slot& slot)
{
    if (slot.is_open()) {
        m_logger.trace("Using already open Realm file: %1", slot.realm_path); // Throws

        // Move to front
        REALM_ASSERT(m_first_open_file);
        if (&slot != m_first_open_file) {
            remove(slot);
            insert(slot); // At front
        }
        REALM_ASSERT(&slot == m_first_open_file);
        return;
    }

    // Close least recently accessed Realm file
    if (m_num_open_files == m_max_open_files) {
        REALM_ASSERT(m_first_open_file);
        Slot& least_recently_accessed = *m_first_open_file->m_prev_open_file;
        least_recently_accessed.proper_close(); // Throws
    }

    slot.open(); // Throws
}

void ClientFileAccessCache::Slot::proper_close()
{
    if (is_open()) {
        m_cache.m_logger.debug("Closing Realm file: %1", realm_path); // Throws
        do_close();
    }
}

void ClientFileAccessCache::Slot::open()
{
    REALM_ASSERT(!is_open());

    m_cache.m_logger.debug("Opening Realm file: %1", realm_path); // Throws

    sync::ClientReplication::Config config;
    config.owner_is_sync_agent = true;
    std::unique_ptr<sync::ClientReplication> history =
        sync::make_client_replication(realm_path, std::move(config)); // Throws
    DBOptions shared_group_options;
    if (m_encryption_key)
        shared_group_options.encryption_key = m_encryption_key->data();
    if (m_cache.m_disable_sync_to_disk)
        shared_group_options.durability = DBOptions::Durability::Unsafe;
    DBRef shared_group = DB::create(*history, shared_group_options); // Throws

    m_history = std::move(history);
    m_shared_group = std::move(shared_group);

    m_cache.insert(*this);
    m_cache.m_first_open_file = this;
    ++m_cache.m_num_open_files;
}
