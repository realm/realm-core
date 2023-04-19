
#ifndef REALM_NOINST_SERVER_FILE_ACCESS_CACHE_HPP
#define REALM_NOINST_SERVER_FILE_ACCESS_CACHE_HPP

#include <utility>
#include <memory>
#include <string>
#include <random>

#include <realm/util/assert.hpp>
#include <realm/util/logger.hpp>
#include <realm/db.hpp>
#include <realm/util/optional.hpp>
#include <realm/sync/noinst/server/server_history.hpp>

namespace realm {
namespace _impl {

/// This class maintains a list of open Realm files ordered according to the
/// time when they were last accessed.
class ServerFileAccessCache {
public:
    class Slot;
    class File;

    /// \param max_open_files The maximum number of Realm files to keep open
    /// concurrently. Must be greater than or equal to 1.
    ///
    /// The specified history context will not be accessed on behalf of this
    /// cache object before the first invocation of Slot::access() on an
    /// associated file file slot.
    ServerFileAccessCache(long max_open_files, util::Logger&, ServerHistory::Context&,
                          util::Optional<std::array<char, 64>> encryption_key);

    ~ServerFileAccessCache() noexcept;

    void proper_close_all();

private:
    /// Null if `m_num_open_files == 0`, otherwise it points to the most
    /// recently accessed open Realm file. `m_first_open_file->m_next_open_file`
    /// is the next most recently accessed open Realm
    /// file. `m_first__open_file->m_prev_open_file` is the least recently
    /// accessed open Realm file.
    Slot* m_first_open_file = nullptr;

    /// Current number of open Realm files.
    long m_num_open_files = 0;

    const long m_max_open_files;
    const util::Optional<std::array<char, 64>> m_encryption_key;
    // The ServerFileAccessCache is tied to the lifetime of the Server, so no shared_ptr needed
    util::Logger& m_logger;
    ServerHistory::Context& m_history_context;

    void access(Slot&);
    void remove(Slot&) noexcept;
    void insert(Slot&) noexcept;
};


/// ServerFileAccessCache::Slot objects are associated with a particular
/// ServerFileAccessCache object, and the application must ensure that all slot
/// objects associated with a particular cache object are destroyed before the
/// cache object is destroyed.
class ServerFileAccessCache::Slot {
public:
    const std::string realm_path;
    const std::string virt_path;

    Slot(ServerFileAccessCache&, std::string realm_path, std::string virt_path, bool claim_sync_agent,
         bool disable_sync_to_disk) noexcept;

    Slot(Slot&&) = default;

    /// Closes the file if it is open (as if by calling close()).
    ~Slot() noexcept;

    /// Returns true if the associated Realm file is currently open.
    bool is_open() const noexcept;

    /// Open the Realm file at `realm_path` if it is not already open. The
    /// returned reference is guaranteed to remain valid until access() is
    /// called again on this slot or on any other slot associated with the same
    /// ServerFileAccessCache object, or until close() is called on this slot,
    /// or the Slot object is destroyed, whichever comes first.
    ///
    /// Calling this function may cause Realm files associated with other Slot
    /// objects of the same ServerFileAccessCache object to be closed.
    File& access();

    /// Same as close() but also generates a log message. This function throws
    /// if logging throws.
    void proper_close();

    /// Close the Realm file now if it is open (idempotency).
    void close() noexcept;

    DBOptions make_shared_group_options() const noexcept;

private:
    ServerFileAccessCache& m_cache;
    const bool m_disable_sync_to_disk;
    const bool m_claim_sync_agent;

    Slot* m_prev_open_file = nullptr;
    Slot* m_next_open_file = nullptr;

    std::unique_ptr<File> m_file;

    void open();
    void do_close() noexcept;

    friend class ServerFileAccessCache;
};


class ServerFileAccessCache::File {
public:
    ServerHistory history;
    DBRef shared_group;

private:
    File(const Slot&);

    friend class Slot;
};


// Implementation

inline ServerFileAccessCache::ServerFileAccessCache(long max_open_files, util::Logger& logger,
                                                    ServerHistory::Context& history_context,
                                                    util::Optional<std::array<char, 64>> encryption_key)
    : m_max_open_files{max_open_files}
    , m_encryption_key{encryption_key}
    , m_logger{logger}
    , m_history_context{history_context}
{
    REALM_ASSERT(m_max_open_files >= 1);
}

inline ServerFileAccessCache::~ServerFileAccessCache() noexcept
{
    REALM_ASSERT(!m_first_open_file);
}

inline void ServerFileAccessCache::remove(Slot& slot) noexcept
{
    // FIXME: Consider using a generic intrusive double-linked list instead.

    REALM_ASSERT(m_first_open_file);
    if (&slot == m_first_open_file) {
        bool no_other_open_file = (slot.m_next_open_file == &slot);
        if (no_other_open_file) {
            m_first_open_file = nullptr;
        }
        else {
            m_first_open_file = slot.m_next_open_file;
        }
    }
    slot.m_prev_open_file->m_next_open_file = slot.m_next_open_file;
    slot.m_next_open_file->m_prev_open_file = slot.m_prev_open_file;
    slot.m_prev_open_file = nullptr;
    slot.m_next_open_file = nullptr;
}

inline void ServerFileAccessCache::insert(Slot& slot) noexcept
{
    REALM_ASSERT(!slot.m_next_open_file);
    REALM_ASSERT(!slot.m_prev_open_file);
    if (m_first_open_file) {
        slot.m_prev_open_file = m_first_open_file->m_prev_open_file;
        slot.m_next_open_file = m_first_open_file;
        slot.m_prev_open_file->m_next_open_file = &slot;
        slot.m_next_open_file->m_prev_open_file = &slot;
    }
    else {
        slot.m_prev_open_file = &slot;
        slot.m_next_open_file = &slot;
    }
    m_first_open_file = &slot;
}

inline ServerFileAccessCache::Slot::Slot(ServerFileAccessCache& cache, std::string rp, std::string vp,
                                         bool claim_sync_agent, bool dstd) noexcept
    : realm_path{std::move(rp)}
    , virt_path{std::move(vp)}
    , m_cache{cache}
    , m_disable_sync_to_disk{dstd}
    , m_claim_sync_agent{claim_sync_agent}
{
}

inline ServerFileAccessCache::Slot::~Slot() noexcept
{
    close();
}

inline bool ServerFileAccessCache::Slot::is_open() const noexcept
{
    if (m_file) {
        REALM_ASSERT(m_prev_open_file);
        REALM_ASSERT(m_next_open_file);
        return true;
    }

    REALM_ASSERT(!m_prev_open_file);
    REALM_ASSERT(!m_next_open_file);
    return false;
}

inline auto ServerFileAccessCache::Slot::access() -> File&
{
    m_cache.access(*this); // Throws
    return *m_file;
}

inline void ServerFileAccessCache::Slot::close() noexcept
{
    if (is_open())
        do_close();
}

inline DBOptions ServerFileAccessCache::Slot::make_shared_group_options() const noexcept
{
    DBOptions options;
    if (m_cache.m_encryption_key)
        options.encryption_key = m_cache.m_encryption_key->data();
    if (m_disable_sync_to_disk)
        options.durability = DBOptions::Durability::Unsafe;
    return options;
}

inline void ServerFileAccessCache::Slot::do_close() noexcept
{
    REALM_ASSERT(is_open());
    --m_cache.m_num_open_files;
    m_cache.remove(*this);
    m_file.reset();
}

inline ServerFileAccessCache::File::File(const Slot& slot)
    : history{slot.m_cache.m_history_context}                                              // Throws
    , shared_group{DB::create(history, slot.realm_path, slot.make_shared_group_options())} // Throws
{
    if (slot.m_claim_sync_agent) {
        shared_group->claim_sync_agent();
    }
}

} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_SERVER_FILE_ACCESS_CACHE_HPP
