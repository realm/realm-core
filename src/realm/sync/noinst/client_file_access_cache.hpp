
#ifndef REALM_NOINST_CLIENT_FILE_ACCESS_CACHE_HPP
#define REALM_NOINST_CLIENT_FILE_ACCESS_CACHE_HPP

#include <utility>
#include <memory>
#include <string>

#include <realm/util/assert.hpp>
#include <realm/util/logger.hpp>
#include <realm/db.hpp>
#include <realm/sync/history.hpp>

namespace realm {
namespace _impl {


/// This class maintains a list of open Realm files ordered according to the
/// time when they were last accessed.
class ClientFileAccessCache {
public:
    class Slot;

    /// \param max_open_files The maximum number of Realm files to keep open
    /// concurrently. Must be greater than or equal to 1.
    ClientFileAccessCache(long max_open_files, bool disable_sync_to_disk, util::Logger& logger);

    ~ClientFileAccessCache() noexcept;

private:
    /// Null if `m_num_open_files == 0`, otherwise it points to the most
    /// recently accessed open Realm file. `m_first_open_file->m_next_open_file`
    /// is the next most recently accessed open Realm
    /// file. `m_first_open_file->m_prev_open_file` is the least recently
    /// accessed open Realm file.
    Slot* m_first_open_file = nullptr;

    /// Current number of open Realm files.
    long m_num_open_files = 0;

    const long m_max_open_files;
    const bool m_disable_sync_to_disk;

    util::Logger& m_logger;

    void access(Slot&);
    void remove(Slot&) noexcept;
    void insert(Slot&) noexcept;
};


/// ClientFileAccessCache::Slot objects are associated with a particular
/// ClientFileAccessCache object, and the application must ensure that all slot
/// objects associated with a particular cache object are destroyed, or at least
/// closed, before the cache object is destroyed.
///
/// The mere construction of a new slot is thread-safe, and, as long as the slot
/// is already closed, the destruction is also thread-safe. Any other operation,
/// including closing is not thread-safe.
class ClientFileAccessCache::Slot {
public:
    const std::string realm_path;

    /// The mere creation of the slot is guaranteed to not involve any access to
    /// the file system.
    Slot(ClientFileAccessCache&, std::string realm_path, util::Optional<std::array<char, 64>> encryption_key = none,
         std::shared_ptr<sync::ClientReplication::ChangesetCooker> = nullptr) noexcept;

    /// Closes the file if it is open (as if by calling close()).
    ~Slot() noexcept;

    struct RefPair {
        sync::ClientReplication& history;
        DB& shared_group;
        RefPair(sync::ClientReplication&, DB&) noexcept;
    };

    /// Open the Realm file at `realm_path` if it is not already open. The
    /// returned references are guaranteed to remain valid until access() is
    /// called again on this slot or on any other slot associated with the same
    /// ClientFileAccessCache object, or until close() is called on this slot, or the Slot
    /// object is destroyed, whichever comes first.
    ///
    /// Calling this function may cause Realm files associated with other Slot
    /// objects of the same ClientFileAccessCache object to be closed.
    RefPair access();

    /// Same as close() but also generates a log message. This function throws
    /// if logging throws.
    void proper_close();

    /// Close the Realm file now if it is open (idempotency).
    void close() noexcept;

private:
    ClientFileAccessCache& m_cache;

    Slot* m_prev_open_file = nullptr;
    Slot* m_next_open_file = nullptr;

    std::unique_ptr<sync::ClientReplication> m_history;
    DBRef m_shared_group;
    util::Optional<std::array<char, 64>> m_encryption_key;

    std::shared_ptr<sync::ClientReplication::ChangesetCooker> m_changeset_cooker;

    bool is_open() const noexcept;
    void open();
    void do_close() noexcept;

    friend class ClientFileAccessCache;
};


inline ClientFileAccessCache::ClientFileAccessCache(long max_open_files, bool disable_sync_to_disk,
                                                    util::Logger& logger)
    : m_max_open_files{max_open_files}
    , m_disable_sync_to_disk{disable_sync_to_disk}
    , m_logger{logger}
{
    REALM_ASSERT(m_max_open_files >= 1);
}

inline ClientFileAccessCache::~ClientFileAccessCache() noexcept
{
    REALM_ASSERT(!m_first_open_file);
}

inline void ClientFileAccessCache::remove(Slot& slot) noexcept
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

inline void ClientFileAccessCache::insert(Slot& slot) noexcept
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

inline ClientFileAccessCache::Slot::Slot(ClientFileAccessCache& cache, std::string rp,
                                         util::Optional<std::array<char, 64>> ek,
                                         std::shared_ptr<sync::ClientReplication::ChangesetCooker> cc) noexcept
    : realm_path{std::move(rp)}
    , m_cache{cache}
    , m_encryption_key{std::move(ek)}
    , m_changeset_cooker{std::move(cc)}
{
}

inline ClientFileAccessCache::Slot::~Slot() noexcept
{
    close();
}

inline ClientFileAccessCache::Slot::RefPair ClientFileAccessCache::Slot::access()
{
    m_cache.access(*this); // Throws
    return RefPair(*m_history, *m_shared_group);
}

inline void ClientFileAccessCache::Slot::close() noexcept
{
    if (is_open())
        do_close();
}

inline bool ClientFileAccessCache::Slot::is_open() const noexcept
{
    if (m_shared_group) {
        REALM_ASSERT(m_prev_open_file);
        REALM_ASSERT(m_next_open_file);
        REALM_ASSERT(m_history);
        return true;
    }

    REALM_ASSERT(!m_prev_open_file);
    REALM_ASSERT(!m_next_open_file);
    REALM_ASSERT(!m_history);
    return false;
}

inline void ClientFileAccessCache::Slot::do_close() noexcept
{
    REALM_ASSERT(is_open());
    --m_cache.m_num_open_files;
    m_cache.remove(*this);
    // We are about to delete the Replication implementation passed to
    // DB::create(), so we must make sure that we are the only users of the
    // DBRef.
    REALM_ASSERT(m_shared_group.use_count() == 1);
    m_shared_group.reset();
    m_history.reset();
}

inline ClientFileAccessCache::Slot::RefPair::RefPair(sync::ClientReplication& h, DB& sg) noexcept
    : history(h)
    , shared_group(sg)
{
}


} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_CLIENT_FILE_ACCESS_CACHE_HPP
