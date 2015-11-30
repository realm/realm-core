/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/

#include <cerrno>
#include <algorithm>
#include <iostream>

#include <fcntl.h>
#include <atomic>

#include <realm/util/features.h>
#include <realm/util/errno.hpp>
#include <realm/util/safe_int_ops.hpp>
#include <realm/util/thread.hpp>
#include <realm/group_writer.hpp>
#include <realm/group_shared.hpp>
#include <realm/group_writer.hpp>
#include <realm/link_view.hpp>
#include <realm/replication.hpp>
#include <realm/impl/simulated_failure.hpp>

#ifndef _WIN32
#  include <sys/wait.h>
#  include <sys/time.h>
#  include <unistd.h>
#else
#  define NOMINMAX
#  include <windows.h>
#endif

//#define REALM_ENABLE_LOGFILE


using namespace realm;
using namespace realm::util;


namespace {

// Constants controlling the amount of uncommited writes in flight:
const uint16_t max_write_slots = 100;
const uint16_t relaxed_sync_threshold = 50;
#define SHAREDINFO_VERSION 4

// The following functions are carefully designed for minimal overhead
// in case of contention among read transactions. In case of contention,
// they consume roughly 90% of the cycles used to start and end a read transaction.
//
// Each live version carries a "count" field, which combines a reference count
// of the readers bound to that version, and a single-bit "free" flag, which
// indicates that the entry does not hold valid data.
//
// The usage patterns are as follows:
//
// Read transactions guard their access to the version information by
// increasing the count field for the duration of the transaction.
// A non-zero count field also indicates that the free space associated
// with the version must remain intact. A zero count field indicates that
// no one refers to that version, so it's free lists can be merged into
// older free space and recycled.
//
// Only write transactions allocate and write new version entries. Also,
// Only write transactions scan the ringbuffer for older versions which
// are not used (count is zero) and free them. As write transactions are
// atomic (ensured by mutex), there is no race between freeing entries
// in the ringbuffer and allocating and writing them.
//
// There are no race conditions between read transactions. Read transactions
// never change the versioning information, only increment or decrement the
// count (and do so solely through the use of atomic operations).
//
// There is a race between read transactions incrementing the count field and
// a write transaction setting the free field. These are mutually exclusive:
// if a read sees the free field set, it cannot use the entry. As it has already
// incremented the count field (optimistically, anticipating that the free bit
// was clear), it must immediately decrement it again. Likewise, it is possible
// for one thread to set the free bit (anticipating a count of zero) while another
// thread increments the count (anticipating a clear free bit). In such cases,
// both threads undo their changes and back off.
//
// For all changes to the free field and the count field: It is important that changes
// to the free field takes the count field into account and vice versa, because they
// are changed optimistically but atomically. This is implemented by modifying the
// count field only by atomic add/sub of '2', and modifying the free field only by
// atomic add/sub of '1'.
//
// The following *memory* ordering is required for correctness:
//
// 1 Accesses within a transaction assumes the version info is valid *before*
//   reading it. This is achieved by synchronizing on the count field. Reading
//   the count field is an *acquire*, while clearing the free field is a *release*.
//
// 2 Accesses within a transaction assumes the version *remains* valid, so
//   all memory accesses with a read transaction must happen before
//   the changes to memory (by a write transaction). This is achieved
//   by use of *release* when the count field is decremented, and use of
//   *acquire* when the free field is set (by the write transaction).
//
// 3 Reads of the counter is synchronized by accesses to the put_pos variable
//   in the ringbuffer. Reading put_pos is an acquire and writing put_pos is
//   a release. Put pos is only ever written when a write transaction updates
//   the ring buffer.
//
// Discussion:
//
// - The design forces release/acquire style synchronization on every
//   begin_read/end_read. This feels like a bit too much, because *only* a write
//   transaction ever changes memory contents. Read transactions do not communicate,
//   so with the right scheme, synchronization should only be proportional to the
//   number of write transactions, not all transactions. The original design achieved
//   this by ONLY synchronizing on the put_pos (case 3 above), BUT the following
//   problems forced the addition of further synchronization:
//
//   - during begin_read, after reading put_pos, a thread may be arbitrarily delayed.
//     While delayed, the entry selected by put_pos may be freed and reused, and then
//     we will lack synchronization. Hence case 1 was added.
//
//   - a read transaction must complete all reads of memory before it can be changed
//     by another thread (this is an example of an anti-dependency). This requires
//     the solution described as case 2 above.
//
// - The use of release (in case 2 above) could - in principle - be replaced
//   by a read memory barrier which would be faster on some architectures, but
//   there is no standardized support for it.
//

template<typename T>
bool atomic_double_inc_if_even(std::atomic<T>& counter)
{
    T oldval = counter.fetch_add(2, std::memory_order_acquire);
    if (oldval & 1) {
        // oooops! was odd, adjust
        counter.fetch_sub(2, std::memory_order_relaxed);
        return false;
    }
    return true;
}

template<typename T>
inline void atomic_double_dec(std::atomic<T>& counter)
{
    counter.fetch_sub(2, std::memory_order_release);
}

template<typename T>
bool atomic_one_if_zero(std::atomic<T>& counter)
{
    T old_val = counter.fetch_add(1, std::memory_order_acquire);
    if (old_val != 0) {
        counter.fetch_sub(1, std::memory_order_relaxed);
        return false;
    }
    return true;
}

template<typename T>
void atomic_dec(std::atomic<T>& counter)
{
    counter.fetch_sub(1, std::memory_order_release);
}

// nonblocking ringbuffer
class Ringbuffer {
public:
    // the ringbuffer is a circular list of ReadCount structures.
    // Entries from old_pos to put_pos are considered live and may
    // have an even value in 'count'. The count indicates the
    // number of referring transactions times 2.
    // Entries from after put_pos up till (not including) old_pos
    // are free entries and must have a count of ONE.
    // Cleanup is performed by starting at old_pos and incrementing
    // (atomically) from 0 to 1 and moving the put_pos. It stops
    // if count is non-zero. This approach requires that only a single thread
    // at a time tries to perform cleanup. This is ensured by doing the cleanup
    // as part of write transactions, where mutual exclusion is assured by the
    // write mutex.
    struct ReadCount {
        uint64_t version;
        uint64_t filesize;
        uint64_t current_top;
        // The count field acts as synchronization point for accesses to the above
        // fields. A succesfull inc implies acquire with regard to memory consistency.
        // Release is triggered by explicitly storing into count whenever a
        // new entry has been initialized.
        mutable std::atomic<uint32_t> count;
        uint32_t next;
    };

    Ringbuffer() noexcept
    {
        entries = init_readers_size;
        for (int i=0; i < init_readers_size; i++) {
            data[i].version = 1;
            data[i].count.store( 1, std::memory_order_relaxed );
            data[i].current_top = 0;
            data[i].filesize = 0;
            data[i].next = i + 1;
        }
        old_pos = 0;
        data[ 0 ].count.store( 0, std::memory_order_relaxed );
        data[ init_readers_size-1 ].next = 0 ;
        put_pos.store( 0, std::memory_order_release );
    }

    void dump()
    {
        uint_fast32_t i = old_pos;
        std::cout << "--- " << std::endl;
         while (i != put_pos.load()) {
            std::cout << "  used " << i << " : "
                 << data[i].count.load() << " | "
                 << data[i].version
                 << std::endl;
            i = data[i].next;
        }
        std::cout << "  LAST " << i << " : "
             << data[i].count.load() << " | "
             << data[i].version
             << std::endl;
        i = data[i].next;
        while (i != old_pos) {
            std::cout << "  free " << i << " : "
                 << data[i].count.load() << " | "
                 << data[i].version
                 << std::endl;
            i = data[i].next;
        }
        std::cout << "--- Done" << std::endl;
    }

    void expand_to(uint_fast32_t new_entries) noexcept
    {
        // std::cout << "expanding to " << new_entries << std::endl;
        // dump();
        for (uint_fast32_t i = entries; i < new_entries; i++) {
            data[i].version = 1;
            data[i].count.store( 1, std::memory_order_relaxed );
            data[i].current_top = 0;
            data[i].filesize = 0;
            data[i].next = i + 1;
        }
        data[ new_entries - 1 ].next = old_pos;
        data[ put_pos.load(std::memory_order_relaxed) ].next = entries;
        entries = new_entries;
        // dump();
    }

    static size_t compute_required_space(uint_fast32_t num_entries) noexcept
    {
        // get space required for given number of entries beyond the initial count.
        // NB: this not the size of the ringbuffer, it is the size minus whatever was
        // the initial size.
        return sizeof(ReadCount) * (num_entries - init_readers_size);
    }

    uint_fast32_t get_num_entries() const noexcept
    {
        return entries;
    }

    uint_fast32_t last() const noexcept
    {
        return put_pos.load(std::memory_order_acquire);
    }

    const ReadCount& get(uint_fast32_t idx) const noexcept
    {
        return data[idx];
    }

    const ReadCount& get_last() const noexcept
    {
        return get(last());
    }

    // This method re-initialises the last used ringbuffer entry to hold a new entry.
    // Precondition: This should *only* be done if the caller has established that she
    // is the only thread/process that has access to the ringbuffer. It is currently
    // called from init_versioning(), which is called by SharedGroup::open() under the
    // condition that it is the session initiator and under guard by the control mutex,
    // thus ensuring the precondition.
    // It is most likely not suited for any other use.
    ReadCount& reinit_last() noexcept
    {
        ReadCount& r = data[last()];
        // r.count is an atomic<> due to other usage constraints. Right here, we're
        // operating under mutex protection, so the use of an atomic store is immaterial
        // and just forced on us by the type of r.count.
        // You'll find the full discussion of how r.count is operated and why it must be
        // an atomic earlier in this file.
        r.count.store(0, std::memory_order_relaxed);
        return r;
    }

    const ReadCount& get_oldest() const noexcept
    {
        return get(old_pos.load(std::memory_order_relaxed));
    }

    bool is_full() const noexcept
    {
        uint_fast32_t idx = get(last()).next;
        return idx == old_pos.load(std::memory_order_relaxed);
    }

    uint_fast32_t next() const noexcept
    { // do not call this if the buffer is full!
        uint_fast32_t idx = get(last()).next;
        return idx;
    }

    ReadCount& get_next() noexcept
    {
        REALM_ASSERT(!is_full());
        return data[ next() ];
    }

    void use_next() noexcept
    {
        atomic_dec(get_next().count); // .store_release(0);
        put_pos.store(next(), std::memory_order_release);
    }

    void cleanup() noexcept
    {   // invariant: entry held by put_pos has count > 1.
        // std::cout << "cleanup: from " << old_pos << " to " << put_pos.load_relaxed();
        // dump();
        while (old_pos.load(std::memory_order_relaxed) != put_pos.load(std::memory_order_relaxed)) {
            const ReadCount& r = get(old_pos.load(std::memory_order_relaxed));
            if (! atomic_one_if_zero( r.count ))
                break;
            auto next = get(old_pos.load(std::memory_order_relaxed)).next;
            old_pos.store(next, std::memory_order_relaxed);
        }
    }

private:
    // number of entries. Access synchronized through put_pos.
    uint32_t entries;
    std::atomic<uint32_t> put_pos; // only changed under lock, but accessed outside lock
    std::atomic<uint32_t> old_pos; // only changed during write transactions and under lock

    const static int init_readers_size = 32;

    // IMPORTANT: The actual data comprising the linked list MUST BE PLACED LAST in
    // the RingBuffer structure, as the linked list area is extended at run time.
    // Similarly, the RingBuffer must be the final element of the SharedInfo structure.
    // IMPORTANT II:
    // To ensure proper alignment across all platforms, the SharedInfo structure
    // should NOT have a stricter alignment requirement than the ReadCount structure.
    ReadCount data[init_readers_size];

};

} // anonymous namespace



struct SharedGroup::SharedInfo
{
    // indicates lock file has valid content, implying that all the following member
    // variables have been initialized. All member variables, except for the Ringbuffer,
    // are protected by 'controlmutex', except during initialization, where access is
    // guarded by the exclusive file lock.
    bool init_complete;

    // size of critical structures. Must match among all participants in a session
    uint8_t size_of_mutex;
    uint8_t size_of_condvar;

    // set when a participant decides to start the daemon, cleared by the daemon
    // when it decides to exit. Participants check during open() and start the
    // daemon if running in async mode.
    bool daemon_started;

    // set by the daemon when it is ready to handle commits. Participants must
    // wait during open() on 'daemon_becomes_ready' for this to become true.
    // Cleared by the daemon when it decides to exit.
    bool daemon_ready; // offset 4

    // Tracks the most recent version number.
    uint16_t version;
    uint16_t durability; // offset 8
    uint16_t free_write_slots;

    // number of participating shared groups:
    uint32_t num_participants; // offset 12

    // Latest version number. Guarded by the controlmutex (for lock-free access, use
    // get_current_version() instead)
    uint64_t latest_version_number; // offset 16

    // Pid of process initiating the session, but only if that process runs with encryption
    // enabled, zero otherwise. Other processes cannot join a session wich uses encryption,
    // because interprocess sharing is not supported by our current encryption mechanisms.
    uint64_t session_initiator_pid;

    uint64_t number_of_versions;
    RobustMutex writemutex;
    RobustMutex balancemutex;
    RobustMutex controlmutex;
#ifndef _WIN32
    // FIXME: windows pthread support for condvar not ready
    PlatformSpecificCondVar::SharedPart room_to_write;
    PlatformSpecificCondVar::SharedPart work_to_do;
    PlatformSpecificCondVar::SharedPart daemon_becomes_ready;
    PlatformSpecificCondVar::SharedPart new_commit_available;
#endif
    // IMPORTANT: The ringbuffer MUST be the last field in SharedInfo - see above.
    Ringbuffer readers;
    SharedInfo(DurabilityLevel);
    ~SharedInfo() noexcept {}
    void init_versioning(ref_type top_ref, size_t file_size, uint64_t initial_version)
    {
        // Create our first versioning entry:
        Ringbuffer::ReadCount& r = readers.reinit_last();
        r.filesize = file_size;
        r.version = initial_version;
        r.current_top = top_ref;
    }
    uint_fast64_t get_current_version_unchecked() const
    {
        return readers.get_last().version;
    }
};


SharedGroup::SharedInfo::SharedInfo(DurabilityLevel dura):
#ifndef _WIN32
    size_of_mutex(sizeof(writemutex)),
    size_of_condvar(sizeof(room_to_write)),
    writemutex(), // Throws
    balancemutex(), // Throws
    controlmutex() // Throws
#else
    size_of_mutex(sizeof(writemutex)),
    size_of_condvar(0),
    writemutex(), // Throws
    balancemutex() // Throws
#endif
{
    version = SHAREDINFO_VERSION;
    durability = dura; // durability level is fixed from creation
#ifndef _WIN32
    PlatformSpecificCondVar::init_shared_part(room_to_write); // Throws
    PlatformSpecificCondVar::init_shared_part(work_to_do); // Throws
    PlatformSpecificCondVar::init_shared_part(daemon_becomes_ready); // Throws
    PlatformSpecificCondVar::init_shared_part(new_commit_available); // Throws
#endif
    free_write_slots = 0;
    num_participants = 0;
    session_initiator_pid = 0;
    daemon_started = false;
    daemon_ready = false;
    init_complete = 1;
}


namespace {

void recover_from_dead_write_transact()
{
    // Nothing needs to be done
}

#ifdef REALM_ASYNC_DAEMON

void spawn_daemon(const std::string& file)
{
    // determine maximum number of open descriptors
    errno = 0;
    int m = int(sysconf(_SC_OPEN_MAX));
    if (m < 0) {
        if (errno) {
            int err = errno; // Eliminate any risk of clobbering
            throw std::runtime_error(get_errno_msg("'sysconf(_SC_OPEN_MAX)' failed: ", err));
        }
        throw std::runtime_error("'sysconf(_SC_OPEN_MAX)' failed with no reason");
    }

    int pid = fork();
    if (0 == pid) { // child process:

        // close all descriptors:
        int i;
        for (i=m-1;i>=0;--i)
            close(i);
        i = ::open("/dev/null",O_RDWR);
#ifdef REALM_ENABLE_LOGFILE
        // FIXME: Do we want to always open the log file? Should it be configurable?
        i = ::open((file+".log").c_str(),O_RDWR | O_CREAT | O_APPEND | O_SYNC, S_IRWXU);
#else
        i = dup(i);
#endif
        i = dup(i); static_cast<void>(i);
#ifdef REALM_ENABLE_LOGFILE
        std::cerr << "Detaching" << std::endl;
#endif
        // detach from current session:
        setsid();

        // start commit daemon executable
        // Note that getenv (which is not thread safe) is called in a
        // single threaded context. This is ensured by the fork above.
        const char* async_daemon = getenv("REALM_ASYNC_DAEMON");
        if (!async_daemon) {
#ifndef REALM_DEBUG
            async_daemon = REALM_INSTALL_LIBEXECDIR "/realmd";
#else
            async_daemon = REALM_INSTALL_LIBEXECDIR "/realmd-dbg";
#endif
        }
        execl(async_daemon, async_daemon, file.c_str(), static_cast<char*>(0));

        // if we continue here, exec has failed so return error
        // if exec succeeds, we don't come back here.
#if REALM_ANDROID
        _exit(1);
#else
        _Exit(1);
#endif
        // child process ends here

    }
    else if (pid > 0) { // parent process, fork succeeded:

        // use childs exit code to catch and report any errors:
        int status;
        int pid_changed;
        do {
            pid_changed = waitpid(pid, &status, 0);
        }
        while (pid_changed == -1 && errno == EINTR);
        if (pid_changed != pid) {
            std::cerr << "Waitpid returned pid = " << pid_changed
                      << " and status = " << std::hex << status << std::endl;
            throw std::runtime_error("call to waitpid failed");
        }
        if (!WIFEXITED(status))
            throw std::runtime_error("failed starting async commit (exit)");
        if (WEXITSTATUS(status) == 1) {
            // FIXME: Or `ld` could not find a required shared library
            throw std::runtime_error("async commit daemon not found");
        }
        if (WEXITSTATUS(status) == 2)
            throw std::runtime_error("async commit daemon failed");
        if (WEXITSTATUS(status) == 3)
            throw std::runtime_error("wrong db given to async daemon");

    }
    else { // Parent process, fork failed!

        throw std::runtime_error("Failed to spawn async commit");
    }
}
#endif


} // anonymous namespace


void SharedGroup::do_open_1(const std::string& path, bool no_create_file, DurabilityLevel durability,
                            bool is_backend, const char* encryption_key, bool allow_upgrafe_file_format)
{
    // Exception safety: Since do_open_1() is called from constructors, if it
    // throws, it must leave the file closed.

    do_open_2(path, no_create_file, durability, is_backend, encryption_key); // Throws
    try {
        upgrade_file_format(allow_upgrafe_file_format); // Throws
    }
    catch (...) {
        close();
        throw;
    }
}


// NOTES ON CREATION AND DESTRUCTION OF SHARED MUTEXES:
//
// According to the 'process-sharing example' in the POSIX man page
// for pthread_mutexattr_init() other processes may continue to use a
// process-shared mutex after exit of the process that initialized
// it. Also, the example does not contain any call to
// pthread_mutex_destroy(), so apparently a process-shared mutex need
// not be destroyed at all, nor can it be that a process-shared mutex
// is associated with any resources that are local to the initializing
// process, because that would imply a leak.
//
// While it is not explicitely guaranteed in the man page, we shall
// assume that is is valid to initialize a process-shared mutex twice
// without an intervending call to pthread_mutex_destroy(). We need to
// be able to reinitialize a process-shared mutex if the first
// initializing process crashes and leaves the shared memory in an
// undefined state.

void SharedGroup::do_open_2(const std::string& path, bool no_create_file, DurabilityLevel durability,
                            bool is_backend, const char* encryption_key)
{
    // Exception safety: Since do_open_2() is called from constructors, if it
    // throws, it must leave the file closed.

    // FIXME: Asses the exception safety of this function.

    REALM_ASSERT(!is_attached());

#ifndef REALM_ASYNC_DAEMON
    if (durability == durability_Async)
        throw std::runtime_error("Async mode not yet supported on Windows, iOS and watchOS");
#endif

    m_db_path = path;
    m_key = encryption_key;
    m_lockfile_path = path + ".lock";
    SlabAlloc& alloc = m_group.m_alloc;

    while (1) {

        m_file.open(m_lockfile_path, File::access_ReadWrite, File::create_Auto, 0);
        File::CloseGuard fcg(m_file);
        if (m_file.try_lock_exclusive()) {

            File::UnlockGuard ulg(m_file);

            // We're alone in the world, and it is Ok to initialize the file:
            char empty_buf[sizeof (SharedInfo)];
            std::fill(empty_buf, empty_buf+sizeof(SharedInfo), 0);
            m_file.write(empty_buf, sizeof(SharedInfo));

            // Complete initialization of shared info via the memory mapping:
            m_file_map.map(m_file, File::access_ReadWrite, sizeof (SharedInfo), File::map_NoSync);
            File::UnmapGuard fug_1(m_file_map);
            SharedInfo* info = m_file_map.get_addr();
            new (info) SharedInfo(durability); // Throws
        }

        // we hold the shared lock from here until we close the file!
        m_file.lock_shared();

        // Once we get the shared lock, we'll need to verify that the initialization of the
        // lock file has been completed succesfully. The initializing process could have crashed
        // during initialization. If so we must detect it and start all over again.

        // wait for file to at least contain the basic shared info block
        // NB! it might be larger due to expansion of the ring buffer.
        size_t info_size;
        if (int_cast_with_overflow_detect(m_file.get_size(), info_size))
            throw std::runtime_error("Lock file too large");

        // Compile time validate the alignment of the first three fields in SharedInfo
        static_assert(offsetof(SharedInfo,init_complete) == 0, "misalignment of init_complete");
        static_assert(offsetof(SharedInfo,size_of_mutex) == 1, "misalignment of size_of_mutex");
        static_assert(offsetof(SharedInfo,size_of_condvar) == 2, "misalignment of size_of_condvar");

        // If this ever triggers we are on a really weird architecture
        static_assert(offsetof(SharedInfo,latest_version_number) == 16, "misalignment of latest_version_number");

        // we need to have the size_of_mutex, size_of_condvar and init_complete
        // fields available before we can check for compatibility
        if (info_size < 4)
            continue;

        {
            // Map the first fields to memory and validate them
            m_file_map.map(m_file, File::access_ReadOnly, 4, File::map_NoSync);
            File::UnmapGuard fug_1(m_file_map);

            // validate initialization complete:
            SharedInfo* info = m_file_map.get_addr();
            if (info->init_complete == 0) {
                continue;
            }

            // validate compatible sizes of mutex and condvar types. Sizes
            // of all other fields are architecture independent, so if condvar
            // and mutex sizes match, the entire struct matches.
            if (info->size_of_mutex != sizeof(info->controlmutex))
                throw IncompatibleLockFile();

#ifndef _WIN32
            if (info->size_of_condvar != sizeof(info->room_to_write))
                throw IncompatibleLockFile();
#endif
        }

        // initialisation is complete and size and alignment matches for all fields in SharedInfo.
        // so we can map the entire structure.
        m_file_map.map(m_file, File::access_ReadWrite, sizeof (SharedInfo), File::map_NoSync);
        File::UnmapGuard fug_1(m_file_map);
        SharedInfo* info = m_file_map.get_addr();

        // even though fields match wrt alignment and size, there may still be incompatibilities
        // between implementations, so lets ask one of the mutexes if it thinks it'll work.
        if (!info->controlmutex.is_valid())
            throw IncompatibleLockFile();

        // OK! lock file appears valid. We can now continue operations under the protection
        // of the controlmutex. The controlmutex protects the following activities:
        // - attachment of the database file
        // - start of the async daemon
        // - stop of the async daemon
        // - SharedGroup beginning/ending a session
        // - Waiting for and signalling database changes
        {
            RobustLockGuard lock(info->controlmutex, &recover_from_dead_write_transact); // Throws
            // Even though we checked init_complete before grabbing the write mutex,
            // we do not need to check it again, because it is only changed under
            // an exclusive file lock, and we checked it under a shared file lock

            // proceed to initialize versioning and other metadata information related to
            // the database. Also create the database if we're beginning a new session
            bool begin_new_session = info->num_participants == 0;
            SlabAlloc::Config cfg;
            cfg.session_initiator = begin_new_session;
            cfg.is_shared = true;
            cfg.read_only = false;
            cfg.skip_validate = !begin_new_session;

            // only the session initiator is allowed to create the database, all other
            // must assume that it already exists.
            cfg.no_create = begin_new_session ? no_create_file : true;

            // if we're opening a MemOnly file that isn't already opened by
            // someone else then it's a file which should have been deleted on
            // close previously, but wasn't (perhaps due to the process crashing)
            cfg.clear_file = durability == durability_MemOnly && begin_new_session;

            // If replication is enabled, we need to ask it whether we're in server-sync mode
            // and check that the database is operated in the same mode.
            cfg.server_sync_mode = false;
            Replication* repl = _impl::GroupFriend::get_replication(m_group);
            if (repl)
                cfg.server_sync_mode = repl->is_in_server_synchronization_mode();
            cfg.encryption_key = encryption_key;
            ref_type top_ref = alloc.attach_file(path, cfg); // Throws
            size_t file_size = alloc.get_baseline();

            if (begin_new_session) {

                // determine version
                uint_fast64_t version;
                Array top(alloc);
                if (top_ref) {
                    // top_ref is non-zero implying that the database has seen at least one commit,
                    // so we can get the versioning info from the database
                    top.init_from_ref(top_ref);
                    if (top.size() <= 5) {
                        // the database wasn't written by shared group, so no versioning info
                        version = 1;
                        REALM_ASSERT(! cfg.server_sync_mode);
                    }
                    else {
                        // the database was written by shared group, so it has versioning info
                        REALM_ASSERT(top.size() >= 7);
                        version = top.get(6) / 2;
                        // In case this was written by an older version of shared group, it
                        // will have version 0. Version 0 is not a legal initial version, so
                        // it has to be set to 1 instead.
                        if (version == 0)
                            version = 1;
                    }
                }
                else {
                    // the database was just created, no metadata has been written yet.
                    version = 1;
                }
                // If replication is enabled, we need to inform it of the latest version,
                // allowing it to discard any surplus log entries
                repl = _impl::GroupFriend::get_replication(m_group);
                if (repl)
                    repl->reset_log_management(version);

#ifndef _WIN32
                if (encryption_key) {
                    static_assert(sizeof(pid_t) <= sizeof(uint64_t), "process identifiers too large");
                    info->session_initiator_pid = uint64_t(getpid());
                }
#endif

                info->latest_version_number = version;
                info->init_versioning(top_ref, file_size, version);

            }
            else { // not the session initiator!
#ifndef _WIN32
                if (encryption_key && info->session_initiator_pid != uint64_t(getpid()))
                    throw std::runtime_error(path + ": Encrypted interprocess sharing is currently unsupported");
#endif

            }
#ifndef _WIN32
            m_daemon_becomes_ready.set_shared_part(info->daemon_becomes_ready,m_db_path,0);
            m_work_to_do.set_shared_part(info->work_to_do,m_db_path,1);
            m_room_to_write.set_shared_part(info->room_to_write,m_db_path,2);
            m_new_commit_available.set_shared_part(info->new_commit_available,m_db_path,3);
#ifdef REALM_ASYNC_DAEMON
            // In async mode, we need to make sure the daemon is running and ready:
            if (durability == durability_Async && !is_backend) {
                while (info->daemon_ready == false) {
                    if (info->daemon_started == false) {
                        spawn_daemon(path);
                        info->daemon_started = true;
                    }
                    // FIXME: It might be more robust to sleep a little, then restart the loop
                    // std::cerr << "Waiting for daemon" << std::endl;
                    m_daemon_becomes_ready.wait(info->controlmutex, &recover_from_dead_write_transact, 0);
                    // std::cerr << " - notified" << std::endl;
                }
            }
            // std::cerr << "daemon should be ready" << std::endl;
#endif
#endif
            // we need a thread-local copy of the number of ringbuffer entries in order
            // to detect concurrent expansion of the ringbuffer.
            m_local_max_entry = 0;

            // We need to map the info file once more for the readers part
            // since that part can be resized and as such remapped which
            // could move our mutexes (which we don't want to risk moving while
            // they are locked)
            m_reader_map.map(m_file, File::access_ReadWrite, sizeof (SharedInfo), File::map_NoSync);
            File::UnmapGuard fug_2(m_reader_map);

            // Set initial version so we can track if other instances
            // change the db
            m_readlock.m_version = get_current_version();

            if (info->version != SHAREDINFO_VERSION)
                throw std::runtime_error("Unsupported version");

            // Durability level cannot be changed at runtime
            if (info->durability != durability)
                throw std::runtime_error("Inconsistent durability level");

            // make our presence noted:
            ++info->num_participants;

            // Initially there is a single version in the file
            info->number_of_versions = 1;

            // Initially wait_for_change is enabled
            m_wait_for_change_enabled = true;

            // Keep the mappings and file open:
            fug_2.release(); // Do not unmap
            fug_1.release(); // Do not unmap
            fcg.release(); // Do not close
        }
        break;
    }

    m_transact_stage = transact_Ready;
    // std::cerr << "open completed" << std::endl;

#ifdef REALM_ASYNC_DAEMON
    if (durability == durability_Async) {
        if (is_backend) {
            do_async_commits();
        }
    }
#else
    static_cast<void>(is_backend);
#endif
}


bool SharedGroup::compact()
{
    // FIXME: ExcetionSafety: This function must be rewritten with exception
    // safety in mind.

    // Verify that the database file is attached
    if (is_attached() == false) {
        throw std::runtime_error(m_db_path + ": compact must be done on an open/attached SharedGroup");
    }
    // Verify that preconditions for compacting is met:
    if (m_transact_stage != transact_Ready) {
        throw std::runtime_error(m_db_path + ": compact is not supported whithin a transaction");
    }
    std::string tmp_path = m_db_path + ".tmp_compaction_space";
    SharedInfo* info = m_file_map.get_addr();
    RobustLockGuard lock(info->controlmutex, &recover_from_dead_write_transact); // Throws
    if (info->num_participants > 1)
        return false;

    // group::write() will throw if the file already exists.
    // To prevent this, we have to remove the file (should it exist)
    // before calling group::write().
    File::try_remove(tmp_path);

    // Using begin_read here ensures that we have access to the latest entry
    // in the ringbuffer. We need to have access to that later to update top_ref and file_size.
    begin_read(); // Throws

    // Compact by writing a new file holding only live data, then renaming the new file
    // so it becomes the database file, replacing the old one in the process.
    m_group.write(tmp_path, m_key, info->latest_version_number);
    rename(tmp_path.c_str(), m_db_path.c_str());
    {
        SharedInfo* r_info = m_reader_map.get_addr();
        Ringbuffer::ReadCount& rc = const_cast<Ringbuffer::ReadCount&>(r_info->readers.get_last());
        REALM_ASSERT_3(rc.version, ==, info->latest_version_number);
        static_cast<void>(rc); // rc unused if ENABLE_ASSERTION is unset
    }
    end_read();

    SlabAlloc& alloc = m_group.m_alloc;

    // close and reopen the database file.
    alloc.detach();
    SlabAlloc::Config cfg;
    cfg.skip_validate = true;
    cfg.no_create = true;
    cfg.is_shared = true;
    cfg.session_initiator = true;
    cfg.encryption_key = m_key;
    ref_type top_ref = alloc.attach_file(m_db_path, cfg);
    size_t file_size = alloc.get_baseline();

    // update the versioning info to match
    SharedInfo* r_info = m_reader_map.get_addr();
    Ringbuffer::ReadCount& rc = const_cast<Ringbuffer::ReadCount&>(r_info->readers.get_last());
    REALM_ASSERT_3(rc.version, ==, info->latest_version_number);
    rc.filesize = file_size;
    rc.current_top = top_ref;
    return true;
}

uint_fast64_t SharedGroup::get_number_of_versions()
{
    SharedInfo* info = m_file_map.get_addr();
    RobustLockGuard lock(info->controlmutex, &recover_from_dead_write_transact); // Throws
    return info->number_of_versions;
}

SharedGroup::~SharedGroup() noexcept
{
    close();
}

void SharedGroup::close() noexcept
{
    if (!is_attached())
        return;

    switch (m_transact_stage) {
        case transact_Ready:
            break;
        case transact_Reading:
            end_read();
            break;
        case transact_Writing:
            rollback();
            break;
    }
    m_group.detach();
    m_transact_stage = transact_Ready;
    SharedInfo* info = m_file_map.get_addr();
    {
        RobustLockGuard lock(info->controlmutex, recover_from_dead_write_transact);

        if (m_group.m_alloc.is_attached())
            m_group.m_alloc.detach();

        --info->num_participants;
        bool end_of_session = info->num_participants == 0;
        // std::cerr << "closing" << std::endl;
        if (end_of_session) {

            // If the db file is just backing for a transient data structure,
            // we can delete it when done.
            if (info->durability == durability_MemOnly) {
                try {
                    util::File::remove(m_db_path.c_str());
                }
                catch(...) {} // ignored on purpose.
            }
            // If replication is enabled, we need to stop log management:
            Replication* repl = _impl::GroupFriend::get_replication(m_group);
            if (repl) {
#ifdef _WIN32
                try {
                    repl->stop_logging();
                }
                catch(...) {} // FIXME, on Windows, stop_logging() fails to delete a file because it's open
#else
                repl->stop_logging();
#endif

            }
        }
    }
#ifndef _WIN32
    m_room_to_write.close();
    m_work_to_do.close();
    m_daemon_becomes_ready.close();
    m_new_commit_available.close();
#endif
    m_file.unlock();
    // info->~SharedInfo(); // DO NOT Call destructor
    m_file.close();
    m_file_map.unmap();
    m_reader_map.unmap();
}

bool SharedGroup::has_changed()
{
    bool changed = m_readlock.m_version != get_current_version();
    return changed;
}

#ifndef _WIN32
#if !REALM_PLATFORM_APPLE
bool SharedGroup::wait_for_change()
{
    SharedInfo* info = m_file_map.get_addr();
    RobustLockGuard lock(info->controlmutex, recover_from_dead_write_transact);
    while (m_readlock.m_version == info->latest_version_number && m_wait_for_change_enabled) {
        m_new_commit_available.wait(info->controlmutex, &recover_from_dead_write_transact, 0);
    }
    return m_readlock.m_version != info->latest_version_number;
}


void SharedGroup::wait_for_change_release()
{
    SharedInfo* info = m_file_map.get_addr();
    RobustLockGuard lock(info->controlmutex, recover_from_dead_write_transact);
    m_wait_for_change_enabled = false;
    m_new_commit_available.notify_all();
}


void SharedGroup::enable_wait_for_change()
{
    SharedInfo* info = m_file_map.get_addr();
    RobustLockGuard lock(info->controlmutex, recover_from_dead_write_transact);
    m_wait_for_change_enabled = true;
}
#endif // !REALM_PLATFORM_APPLE

void SharedGroup::do_async_commits()
{
    bool shutdown = false;
    SharedInfo* info = m_file_map.get_addr();

    // We always want to keep a read lock on the last version
    // that was commited to disk, to protect it against being
    // overwritten by commits being made to memory by others.
    bool dummy;
    grab_latest_readlock(m_readlock, dummy);
    // we must treat version and version_index the same way:
    {
        RobustLockGuard lock(info->controlmutex, &recover_from_dead_write_transact);
        info->free_write_slots = max_write_slots;
        info->daemon_ready = true;
        m_daemon_becomes_ready.notify_all();
    }
    using gf = _impl::GroupFriend;
    gf::detach(m_group);

    while(true) {
        if (m_file.is_removed()) { // operator removed the lock file. take a hint!

            shutdown = true;
#ifdef REALM_ENABLE_LOGFILE
            std::cerr << "Lock file removed, initiating shutdown" << std::endl;
#endif
        }

        bool is_same;
        ReadLockInfo next_readlock = m_readlock;
        {
            // detect if we're the last "client", and if so, shutdown (must be under lock):
            RobustLockGuard lock2(info->writemutex, &recover_from_dead_write_transact);
            RobustLockGuard lock(info->controlmutex, &recover_from_dead_write_transact);
            grab_latest_readlock(next_readlock, is_same);
            if (is_same && (shutdown || info->num_participants == 1)) {
#ifdef REALM_ENABLE_LOGFILE
                std::cerr << "Daemon exiting nicely" << std::endl << std::endl;
#endif
                release_readlock(next_readlock);
                release_readlock(m_readlock);
                info->daemon_started = false;
                info->daemon_ready = false;
                return;
            }
        }

        if (!is_same) {

#ifdef REALM_ENABLE_LOGFILE
            std::cerr << "Syncing from version " << m_readlock.m_version
                 << " to " << next_readlock.m_version << std::endl;
#endif
            GroupWriter writer(m_group);
            writer.commit(next_readlock.m_top_ref);

#ifdef REALM_ENABLE_LOGFILE
            std::cerr << "..and Done" << std::endl;
#endif
        }

        // Now we can release the version that was previously commited
        // to disk and just keep the lock on the latest version.
        release_readlock(m_readlock);
        m_readlock = next_readlock;

        info->balancemutex.lock(&recover_from_dead_write_transact);

        // We have caught up with the writers, let them know that there are
        // now free write slots, wakeup any that has been suspended.
        uint16_t free_write_slots = info->free_write_slots;
        info->free_write_slots = max_write_slots;
        if (free_write_slots <= 0) {
            m_room_to_write.notify_all();
        }

        // If we have plenty of write slots available, relax and wait a bit before syncing
        if (free_write_slots > relaxed_sync_threshold) {
            timespec ts;
            timeval tv;
            // clock_gettime(CLOCK_REALTIME, &ts); <- would like to use this, but not there on mac
            gettimeofday(&tv, nullptr);
            ts.tv_sec = tv.tv_sec;
            ts.tv_nsec = tv.tv_usec * 1000;
            ts.tv_nsec += 10000000; // 10 msec
            if (ts.tv_nsec >= 1000000000) { // overflow
                ts.tv_nsec -= 1000000000;
                ts.tv_sec += 1;
            }

            // no timeout support if the condvars are only emulated, so this will assert
            m_work_to_do.wait(info->balancemutex, &recover_from_dead_write_transact, &ts);
        }
        info->balancemutex.unlock();

    }
}
#endif // _WIN32


SharedGroup::VersionID SharedGroup::get_version_of_current_transaction()
{
    return VersionID(m_readlock.m_version, m_readlock.m_reader_idx);
}

void SharedGroup::grab_latest_readlock(ReadLockInfo& readlock, bool& same_as_before)
{
    for (;;) {
        SharedInfo* r_info = m_reader_map.get_addr();
        readlock.m_reader_idx = r_info->readers.last();
        if (grow_reader_mapping(readlock.m_reader_idx)) { // Throws
            // remapping takes time, so retry with a fresh entry
            continue;
        }
        const Ringbuffer::ReadCount& r = r_info->readers.get(readlock.m_reader_idx);
        // if the entry is stale and has been cleared by the cleanup process,
        // we need to start all over again. This is extremely unlikely, but possible.
        if (! atomic_double_inc_if_even(r.count)) // <-- most of the exec time spent here!
            continue;
        same_as_before       = readlock.m_version == r.version;
        readlock.m_version   = r.version;
        readlock.m_top_ref   = to_size_t(r.current_top);
        readlock.m_file_size = to_size_t(r.filesize);
        return;
    }
}

bool SharedGroup::grab_specific_readlock(ReadLockInfo& readlock, bool& same_as_before,
                                         VersionID specific_version)
{
    for (;;) {
        SharedInfo* r_info = m_reader_map.get_addr();
        readlock.m_reader_idx = specific_version.index;
        if (grow_reader_mapping(readlock.m_reader_idx)) { // Throws
            // remapping takes time, so retry with a fresh entry
            continue;
        }
        const Ringbuffer::ReadCount& r = r_info->readers.get(readlock.m_reader_idx);

        // if the entry is stale and has been cleared by the cleanup process,
        // the requested version is no longer available
        while (! atomic_double_inc_if_even(r.count)) { // <-- most of the exec time spent here!
            // we failed to lock the version. This could be because the version
            // is being cleaned up, but also because the cleanup is probing for access
            // to it. If it's being probed, the tail ptr of the ringbuffer will point
            // to it. If so we retry. If the tail ptr points somewhere else, the entry
            // has been cleaned up.
            if (& r_info->readers.get_oldest() != &r)
	        return false;
	}
	// we managed to lock an entry in the ringbuffer, but it may be so old that
        // the version doesn't match the specific request. In that case we must release and fail
        if (r.version != specific_version.version) {
            atomic_double_dec(r.count); // <-- release
            return false;
        }
        same_as_before       = readlock.m_version == r.version;
        readlock.m_version   = r.version;
        readlock.m_top_ref   = to_size_t(r.current_top);
        readlock.m_file_size = to_size_t(r.filesize);
        return true;
    }
}


const Group& SharedGroup::begin_read(VersionID version)
{
    if (m_transact_stage != transact_Ready)
        throw LogicError(LogicError::wrong_transact_state);

    do_begin_read(version); // Throws

    m_transact_stage = transact_Reading;
    return m_group;
}


void SharedGroup::end_read() noexcept
{
    if (m_transact_stage == transact_Ready)
        return; // Idempotency

    if (m_transact_stage != transact_Reading)
        throw LogicError(LogicError::wrong_transact_state);

    do_end_read();

    m_transact_stage = transact_Ready;
}


Group& SharedGroup::begin_write()
{
    if (m_transact_stage != transact_Ready)
        throw LogicError(LogicError::wrong_transact_state);

    do_begin_write(); // Throws
    try {
        // We can be sure that do_begin_read() will bind to the latest snapshot,
        // since no other write transaction can be initated while we hold the
        // write mutex.
        VersionID version = VersionID(); // Latest
        do_begin_read(version); // Throws

        Replication* repl = m_group.get_replication();
        if (repl) {
            version_type current_version = m_readlock.m_version;
            repl->initiate_transact(*this, current_version); // Throws
        }
    }
    catch (...) {
        do_end_write();
        throw;
    }

    m_transact_stage = transact_Writing;
    return m_group;
}


SharedGroup::version_type SharedGroup::commit()
{
    if (m_transact_stage != transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);

    REALM_ASSERT(m_group.is_attached());

    version_type new_version = do_commit(); // Throws
    do_end_write();
    do_end_read();

    m_transact_stage = transact_Ready;
    return new_version;
}


void SharedGroup::rollback() noexcept
{
    if (m_transact_stage == transact_Ready)
        return; // Idempotency

    if (m_transact_stage != transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);

    do_end_write();
    do_end_read();

    if (Replication* repl = m_group.get_replication())
        repl->abort_transact(*this);

    m_transact_stage = transact_Ready;
}


void SharedGroup::do_begin_read(VersionID version)
{
    // FIXME: BadVersion must be thrown in every case where the specified
    // version is not tethered in accordance with the documentation of
    // begin_read().

    if (version.version == 0) {
        bool dummy;
        grab_latest_readlock(m_readlock, dummy); // Throws
    }
    else {
        bool dummy;
        bool success = grab_specific_readlock(m_readlock, dummy, version); // Throws
        if (!success)
            throw BadVersion();
    }

    ReadLockUnlockGuard rlug(*this, m_readlock);

    using gf = _impl::GroupFriend;
    gf::attach_shared(m_group, m_readlock.m_top_ref, m_readlock.m_file_size); // Throws

    rlug.release();
}


void SharedGroup::do_end_read() noexcept
{
    REALM_ASSERT(m_readlock.m_version != std::numeric_limits<size_t>::max());
    release_readlock(m_readlock);
    using gf = _impl::GroupFriend;
    gf::detach(m_group);
}


void SharedGroup::do_begin_write()
{
    SharedInfo* info = m_file_map.get_addr();

    // Get write lock
    // Note that this will not get released until we call
    // commit() or rollback()
    info->writemutex.lock(&recover_from_dead_write_transact); // Throws

#ifdef REALM_ASYNC_DAEMON
    if (info->durability == durability_Async) {

        info->balancemutex.lock(&recover_from_dead_write_transact); // Throws

        // if we are running low on write slots, kick the sync daemon
        if (info->free_write_slots < relaxed_sync_threshold)
            m_work_to_do.notify();
        // if we are out of write slots, wait for the sync daemon to catch up
        while (info->free_write_slots <= 0) {
            m_room_to_write.wait(info->balancemutex, recover_from_dead_write_transact);
        }

        info->free_write_slots--;
        info->balancemutex.unlock();
    }
#endif // _WIN32
}


void SharedGroup::do_end_write() noexcept
{
    SharedInfo* info = m_file_map.get_addr();
    info->writemutex.unlock();
}


Replication::version_type SharedGroup::do_commit()
{
    REALM_ASSERT(m_transact_stage == transact_Writing);

    SharedInfo* r_info = m_reader_map.get_addr();

    version_type current_version = r_info->get_current_version_unchecked();
    version_type new_version = current_version + 1;
    if (Replication* repl = m_group.get_replication()) {
        // If Replication::prepare_commit() fails, then the entire transaction
        // fails. The application then has the option of terminating the
        // transaction with a call to SharedGroup::rollback(), which in turn
        // must call Replication::abort_transact().
        new_version = repl->prepare_commit(*this, current_version); // Throws
        try {
            low_level_commit(new_version); // Throws
        }
        catch (...) {
            repl->abort_transact(*this);
            throw;
        }
        repl->finalize_commit(*this);
    }
    else {
        low_level_commit(new_version); // Throws
    }

    return new_version;
}


void SharedGroup::release_readlock(ReadLockInfo& readlock) noexcept
{
    SharedInfo* r_info = m_reader_map.get_addr();
    const Ringbuffer::ReadCount& r = r_info->readers.get(readlock.m_reader_idx);
    atomic_double_dec(r.count); // <-- most of the exec time spent here
}


std::unique_ptr<BinaryData[]>
SharedGroup::advance_readlock(History& history,VersionID specific_version)
{
    bool same_as_before;
    ReadLockInfo old_readlock = m_readlock;

    // FIXME: BadVersion must be thrown in every case where the specified
    // version is not tethered in accordance with the documentation of
    // begin_read().

    // we cannot move backward in time (yet)
    if (specific_version.version && specific_version.version < m_readlock.m_version)
        throw BadVersion();

    // advance current readlock while holding onto old one - we MUST hold onto
    // the old readlock until after the call to advance_transact(). Once a readlock
    // is released, the release may propagate to the commit log management, causing
    // it to reclaim memory for old commit logs. We must finished use of the commit log
    // before allowing that to happen.
    if (specific_version.version) {
        bool success = grab_specific_readlock(m_readlock, same_as_before, specific_version);
        if (!success)
            throw BadVersion();
    }
    else {
        grab_latest_readlock(m_readlock, same_as_before); // Throws
    }
    if (same_as_before) {
        return nullptr;
    }

    // If the new top-ref is zero, then the previous top-ref must have
    // been zero too, and we are still seing an empty Realm file
    // (note that this is possible even if the version has
    // changed). The purpose of the early-out in this case, is to
    // retain the temporary arrays that were created earlier by
    // Group::init_for_transact() to put the group accessor into a
    // valid state.
    if (m_readlock.m_top_ref == 0) {
        return nullptr;
    }

    // We know that the log_registry already knows about the new_version,
    // because in order for us to get the new version when we grab the
    // readlock, the new version must have been entered into the ringbuffer.
    // commit always updates the replication log BEFORE updating the ringbuffer.
    size_t num_changesets = m_readlock.m_version - old_readlock.m_version;
    std::unique_ptr<BinaryData[]> changesets(new BinaryData[num_changesets]); // Throws
    history.get_changesets(old_readlock.m_version, m_readlock.m_version, changesets.get());
    return changesets;
}


bool SharedGroup::grow_reader_mapping(uint_fast32_t index)
{
    using _impl::SimulatedFailure;
    SimulatedFailure::check(SimulatedFailure::shared_group__grow_reader_mapping); // Throws

    if (index >= m_local_max_entry) {
        // handle mapping expansion if required
        SharedInfo* r_info = m_reader_map.get_addr();
        m_local_max_entry = r_info->readers.get_num_entries();
        size_t info_size = sizeof(SharedInfo) + r_info->readers.compute_required_space(m_local_max_entry);
        // std::cout << "Growing reader mapping to " << infosize << std::endl;
        m_reader_map.remap(m_file, util::File::access_ReadWrite, info_size); // Throws
        return true;
    }
    return false;
}


uint_fast64_t SharedGroup::get_current_version()
{
    // As get_current_version may be called outside of the write mutex, another
    // thread may be performing changes to the ringbuffer concurrently. It may
    // even cleanup and recycle the current entry from under our feet, so we need
    // to protect the entry by temporarily incrementing the reader ref count until
    // we've got a safe reading of the version number.
    while (1) {
        uint_fast32_t index;
        SharedInfo* r_info;
        do {
            // make sure that the index we are about to dereference falls within
            // the portion of the ringbuffer that we have mapped - if not, extend
            // the mapping to fit.
            r_info = m_reader_map.get_addr();
            index = r_info->readers.last();
        }
        while (grow_reader_mapping(index)); // throws

        // now (double) increment the read count so that no-one cleans up the entry
        // while we read it.
        const Ringbuffer::ReadCount& r = r_info->readers.get(index);
        if (! atomic_double_inc_if_even(r.count)) {

            continue;
        }
        uint_fast64_t version = r.version;
        // release the entry again:
        atomic_double_dec(r.count);
        return version;
    }
}

void SharedGroup::low_level_commit(uint_fast64_t new_version)
{
    SharedInfo* info = m_file_map.get_addr();
    uint_fast64_t readlock_version;
    {
        SharedInfo* r_info = m_reader_map.get_addr();

        // the cleanup process may access the entire ring buffer, so make sure it is mapped.
        // this is not ensured as part of begin_read, which only makes sure that the current
        // last entry in the buffer is available.
        if (grow_reader_mapping(r_info->readers.get_num_entries())) { // throws
            r_info = m_reader_map.get_addr();
        }
        r_info->readers.cleanup();
        const Ringbuffer::ReadCount& r = r_info->readers.get_oldest();
        readlock_version = r.version;
        // If replication is enabled, we need to propagate knowledge of the earliest
        // available version:
        Replication* repl = _impl::GroupFriend::get_replication(m_group);
        if (repl)
            repl->set_last_version_seen_locally(readlock_version);
    }

    // Do the actual commit
    REALM_ASSERT(m_group.m_top.is_attached());
    REALM_ASSERT(readlock_version <= new_version);
    // info->readers.dump();
    GroupWriter out(m_group); // Throws
    out.set_versions(new_version, readlock_version);
    // Recursively write all changed arrays to end of file
    ref_type new_top_ref = out.write_group(); // Throws
    // std::cout << "Writing version " << new_version << ", Topptr " << new_top_ref
    //     << " Readlock at version " << readlock_version << std::endl;
    switch (DurabilityLevel(info->durability)) {
        case durability_Full:
            out.commit(new_top_ref); // Throws
            break;
        case durability_MemOnly:
        case durability_Async:
            // In durability_MemOnly mode, we just use the file as backing for
            // the shared memory. So we never actually flush the data to disk
            // (the OS may do so opportinisticly, or when swapping). So in this
            // mode the file on disk may very likely be in an invalid state.
            break;
    }
    size_t new_file_size = out.get_file_size();
    // Update reader info
    {
        SharedInfo* r_info = m_reader_map.get_addr();
        if (r_info->readers.is_full()) {
            // buffer expansion
            uint_fast32_t entries = r_info->readers.get_num_entries();
            entries = entries + 32;
            size_t new_info_size = sizeof(SharedInfo) + r_info->readers.compute_required_space( entries );
            // std::cout << "resizing: " << entries << " = " << new_info_size << std::endl;
            m_file.prealloc(0, new_info_size); // Throws
            m_reader_map.remap(m_file, util::File::access_ReadWrite, new_info_size); // Throws
            r_info = m_reader_map.get_addr();
            m_local_max_entry = entries;
            r_info->readers.expand_to(entries);
        }
        Ringbuffer::ReadCount& r = r_info->readers.get_next();
        r.current_top = new_top_ref;
        r.filesize    = new_file_size;
        r.version     = new_version;
        r_info->readers.use_next();
    }
    {
        RobustLockGuard lock(info->controlmutex, recover_from_dead_write_transact);
        info->number_of_versions = new_version - readlock_version + 1;
        info->latest_version_number = new_version;
#ifndef _WIN32
        m_new_commit_available.notify_all();
#endif
    }
}


void SharedGroup::reserve(size_t size)
{
    REALM_ASSERT(is_attached());
    // FIXME: There is currently no synchronization between this and
    // concurrent commits in progress. This is so because it is
    // believed that the OS guarantees race free behavior when
    // util::File::prealloc_if_supported() (posix_fallocate() on
    // Linux) runs concurrently with modfications via a memory map of
    // the file. This assumption must be verified though.
    m_group.m_alloc.reserve_disk_space(size); // Throws
}



std::unique_ptr<SharedGroup::Handover<LinkView>> 
SharedGroup::export_linkview_for_handover(const LinkViewRef& accessor)
{
    LockGuard lg(m_handover_lock);
    if (m_transact_stage != transact_Reading) {
        throw LogicError(LogicError::wrong_transact_state);
    }
    std::unique_ptr<Handover<LinkView>> result(new Handover<LinkView>());
    LinkView::generate_patch(accessor, result->patch);
    result->clone = 0; // not used for LinkView - maybe specialize Handover<LinkView> ?
    result->version = get_version_of_current_transaction();
    return result;
}


LinkViewRef SharedGroup::import_linkview_from_handover(std::unique_ptr<Handover<LinkView>> handover)
{
    if (handover->version != get_version_of_current_transaction()) {
        throw BadVersion();
    }
    // move data
    LinkViewRef result = LinkView::create_from_and_consume_patch(handover->patch, m_group);
    return result;
}

