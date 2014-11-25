/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/

#include <cerrno>
#include <algorithm>
#include <iostream>

#include <fcntl.h>

#include <tightdb/util/features.h>
#include <tightdb/util/errno.hpp>
#include <tightdb/util/safe_int_ops.hpp>
#include <tightdb/util/thread.hpp>
#include <tightdb/group_writer.hpp>
#include <tightdb/group_shared.hpp>
#include <tightdb/group_writer.hpp>
#ifdef TIGHTDB_ENABLE_REPLICATION
#  include <tightdb/replication.hpp>
#endif

#ifndef _WIN32
#  include <sys/wait.h>
#  include <sys/time.h>
#  include <unistd.h>
#else
#  define NOMINMAX
#  include <windows.h>
#endif

//#define TIGHTDB_ENABLE_LOGFILE


using namespace std;
using namespace tightdb;
using namespace tightdb::util;


namespace {

// Constants controlling the amount of uncommited writes in flight:
const uint16_t max_write_slots = 100;
const uint16_t relaxed_sync_threshold = 50;
#define SHAREDINFO_VERSION 2

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

template<typename T> bool atomic_double_inc_if_even(Atomic<T>& counter)
{
    T oldval = counter.fetch_add_acquire(2);
    if (oldval & 1) {
        // oooops! was odd, adjust
        counter.fetch_sub_relaxed(2);
        return false;
    }
    return true;
}

template<typename T> inline void atomic_double_dec(Atomic<T>& counter)
{
    counter.fetch_sub_release(2);
}

template<typename T> bool atomic_one_if_zero(Atomic<T>& counter)
{
    T old_val = counter.fetch_add_acquire(1);
    if (old_val != 0) {
        counter.fetch_sub_relaxed(1);
        return false;
    }
    return true;
}

template<typename T> void atomic_dec(Atomic<T>& counter)
{
    counter.fetch_sub_release(1);
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
        uint_fast64_t version;
        uint_fast64_t filesize;
        uint_fast64_t current_top;
        // The count field acts as synchronization point for accesses to the above
        // fields. A succesfull inc implies acquire wrt memory consistency.
        // Release is triggered by explicitly storing into count whenever a
        // new entry has been initialized.
        mutable Atomic<uint_fast32_t> count;
        uint_fast32_t next;
    };

    Ringbuffer() TIGHTDB_NOEXCEPT
    {
        entries = init_readers_size;
        for (int i=0; i < init_readers_size; i++) {
            data[i].version = 1;
            data[i].count.store_relaxed( 1 );
            data[i].current_top = 0;
            data[i].filesize = 0;
            data[i].next = i + 1;
        }
        old_pos = 0;
        data[ 0 ].count.store_relaxed( 0 );
        data[ init_readers_size-1 ].next = 0 ;
        put_pos.store_release( 0 );
    }

    void dump()
    {
        uint_fast32_t i = old_pos;
        cout << "--- " << endl;
         while (i != put_pos.load_relaxed()) {
            cout << "  used " << i << " : "
                 << data[i].count.load_relaxed() << " | "
                 << data[i].version
                 << endl;
            i = data[i].next;
        }
        cout << "  LAST " << i << " : "
             << data[i].count.load_relaxed() << " | "
             << data[i].version
             << endl;
        i = data[i].next;
        while (i != old_pos) {
            cout << "  free " << i << " : "
                 << data[i].count.load_relaxed() << " | "
                 << data[i].version
                 << endl;
            i = data[i].next;
        }
        cout << "--- Done" << endl;
    }

    void expand_to(uint_fast32_t new_entries)  TIGHTDB_NOEXCEPT
    {
        // cout << "expanding to " << new_entries << endl;
        // dump();
        for (uint_fast32_t i = entries; i < new_entries; i++) {
            data[i].version = 1;
            data[i].count.store_relaxed( 1 );
            data[i].current_top = 0;
            data[i].filesize = 0;
            data[i].next = i + 1;
        }
        data[ new_entries - 1 ].next = old_pos;
        data[ put_pos.load_relaxed() ].next = entries;
        entries = new_entries;
        // dump();
    }

    static size_t compute_required_space(uint_fast32_t num_entries)  TIGHTDB_NOEXCEPT
    {
        // get space required for given number of entries beyond the initial count.
        // NB: this not the size of the ringbuffer, it is the size minus whatever was
        // the initial size.
        return sizeof(ReadCount) * (num_entries - init_readers_size);
    }

    uint_fast32_t get_num_entries() const  TIGHTDB_NOEXCEPT
    {
        return entries;
    }

    uint_fast32_t last() const  TIGHTDB_NOEXCEPT
    {
        return put_pos.load_acquire();
    }

    const ReadCount& get(uint_fast32_t idx) const  TIGHTDB_NOEXCEPT
    {
        return data[idx];
    }

    const ReadCount& get_last() const  TIGHTDB_NOEXCEPT
    {
        return get(last());
    }

    const ReadCount& get_oldest() const  TIGHTDB_NOEXCEPT
    {
        return get(old_pos);
    }

    bool is_full() const  TIGHTDB_NOEXCEPT
    {
        uint_fast32_t idx = get(last()).next;
        return idx == old_pos;
    }

    uint_fast32_t next() const  TIGHTDB_NOEXCEPT
    { // do not call this if the buffer is full!
        uint_fast32_t idx = get(last()).next;
        return idx;
    }

    ReadCount& get_next()  TIGHTDB_NOEXCEPT
    {
        TIGHTDB_ASSERT(!is_full());
        return data[ next() ];
    }

    void use_next()  TIGHTDB_NOEXCEPT
    {
        atomic_dec(get_next().count); // .store_release(0);
        put_pos.store_release(next());
    }

    void cleanup()  TIGHTDB_NOEXCEPT
    {   // invariant: entry held by put_pos has count > 1.
        // cout << "cleanup: from " << old_pos << " to " << put_pos.load_relaxed();
        // dump();
        while (old_pos != put_pos.load_relaxed()) {
            const ReadCount& r = get(old_pos);
            if (! atomic_one_if_zero( r.count ))
                break;
            old_pos = get(old_pos).next;
        }
    }

private:
    // number of entries. Access synchronized through put_pos.
    uint_fast32_t entries;
    Atomic<uint_fast32_t> put_pos; // only changed under lock, but accessed outside lock
    uint_fast32_t old_pos; // only accessed during write transactions and under lock

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

    // number of participating shared groups:
    uint32_t num_participants;

    // set when a participant decides to start the daemon, cleared by the daemon
    // when it decides to exit. Participants check during open() and start the
    // daemon if running in async mode.
    bool daemon_started;

    // set by the daemon when it is ready to handle commits. Participants must
    // wait during open() on 'daemon_becomes_ready' for this to become true.
    // Cleared by the daemon when it decides to exit.
    bool daemon_ready;

    // Set when the database and the .lock file is in sync with respect to versioning
    // (and a few other metadata)
    bool versioning_ready;

    // Set when some thread wants to suspend waiting for a change. Cleared when all
    // waiting threads are woken.
    bool waiting_for_change;

    // Latest version number. Guarded by the controlmutex (for lock-free access, use
    // get_current_version() instead)
    uint64_t latest_version_number;

    // Tracks the most recent version number. Should only be accessed 
    uint16_t version;
    uint16_t flags;
    uint16_t free_write_slots;

    RobustMutex writemutex;
    RobustMutex balancemutex;
    RobustMutex controlmutex;
#ifndef _WIN32
    // FIXME: windows pthread support for condvar not ready
    CondVar room_to_write;
    CondVar work_to_do;
    CondVar daemon_becomes_ready;
    CondVar new_commit_available;
#endif
    // IMPORTANT: The ringbuffer MUST be the last field in SharedInfo - see above.
    Ringbuffer readers;
    SharedInfo(DurabilityLevel);
    ~SharedInfo() TIGHTDB_NOEXCEPT {}
    void init_versioning(ref_type top_ref, size_t file_size)
    {
        // Create our first versioning entry:
        Ringbuffer::ReadCount& r = readers.get_next();
        r.filesize = file_size;
        r.version = 1;
        r.current_top = top_ref;
        readers.use_next();
    }
    uint_fast64_t get_current_version_unchecked() const
    {
        return readers.get_last().version;
    }
};


SharedGroup::SharedInfo::SharedInfo(DurabilityLevel dlevel):
#ifndef _WIN32
    writemutex(), // Throws
    balancemutex(), // Throws
    controlmutex(), // Throws
    room_to_write(CondVar::process_shared_tag()), // Throws
    work_to_do(CondVar::process_shared_tag()), // Throws
    daemon_becomes_ready(CondVar::process_shared_tag()), // Throws
    new_commit_available(CondVar::process_shared_tag()) // Throws
#else
    writemutex(), // Throws
    balancemutex() // Throws
#endif
{
    version = SHAREDINFO_VERSION;
    flags = dlevel; // durability level is fixed from creation
    free_write_slots = 0;
    num_participants = 0;
    daemon_started = false;
    daemon_ready = false;
    versioning_ready = false;
    waiting_for_change = false;
    init_complete = 1;
}


namespace {

void recover_from_dead_write_transact()
{
    // Nothing needs to be done
}

#ifndef _WIN32

void spawn_daemon(const string& file)
{
    // determine maximum number of open descriptors
    errno = 0;
    int m = int(sysconf(_SC_OPEN_MAX));
    if (m < 0) {
        if (errno) {
            int err = errno; // Eliminate any risk of clobbering
            throw runtime_error(get_errno_msg("'sysconf(_SC_OPEN_MAX)' failed: ", err));
        }
        throw runtime_error("'sysconf(_SC_OPEN_MAX)' failed with no reason");
    }

    int pid = fork();
    if (0 == pid) { // child process:

        // close all descriptors:
        int i;
        for (i=m-1;i>=0;--i)
            close(i);
        i = ::open("/dev/null",O_RDWR);
#ifdef TIGHTDB_ENABLE_LOGFILE
        // FIXME: Do we want to always open the log file? Should it be configurable?
        i = ::open((file+".log").c_str(),O_RDWR | O_CREAT | O_APPEND | O_SYNC, S_IRWXU);
#else
        i = dup(i);
#endif
        i = dup(i); static_cast<void>(i);
#ifdef TIGHTDB_ENABLE_LOGFILE
        cerr << "Detaching" << endl;
#endif
        // detach from current session:
        setsid();

        // start commit daemon executable
        // Note that getenv (which is not thread safe) is called in a
        // single threaded context. This is ensured by the fork above.
        const char* async_daemon = getenv("TIGHTDB_ASYNC_DAEMON");
        if (!async_daemon) {
#ifndef TIGTHDB_DEBUG
            async_daemon = TIGHTDB_INSTALL_LIBEXECDIR "/tightdbd";
#else
            async_daemon = TIGHTDB_INSTALL_LIBEXECDIR "/tightdbd-dbg";
#endif
        }
        execl(async_daemon, async_daemon, file.c_str(), static_cast<char*>(0));

        // if we continue here, exec has failed so return error
        // if exec succeeds, we don't come back here.
#if TIGHTDB_ANDROID
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
            throw runtime_error("call to waitpid failed");
        }
        if (!WIFEXITED(status))
            throw runtime_error("failed starting async commit (exit)");
        if (WEXITSTATUS(status) == 1) {
            // FIXME: Or `ld` could not find a required shared library
            throw runtime_error("async commit daemon not found");
        }
        if (WEXITSTATUS(status) == 2)
            throw runtime_error("async commit daemon failed");
        if (WEXITSTATUS(status) == 3)
            throw runtime_error("wrong db given to async daemon");

    }
    else { // Parent process, fork failed!

        throw runtime_error("Failed to spawn async commit");
    }
}
#else
void spawn_daemon(const string& file) {}
#endif


} // anonymous namespace


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

void SharedGroup::open(const string& path, bool no_create_file,
                       DurabilityLevel dlevel, bool is_backend, const uint8_t* key)
{
    TIGHTDB_ASSERT(!is_attached());

    m_file_path = path + ".lock";
    SlabAlloc& alloc = m_group.m_alloc;

    while (1) {

        m_file.open(m_file_path, File::access_ReadWrite, File::create_Auto, 0);
        File::CloseGuard fcg(m_file);
        if (m_file.try_lock_exclusive()) {

            File::UnlockGuard ulg(m_file);

            // We're alone in the world, and it is Ok to initialize the file:
            char empty_buf[sizeof (SharedInfo)];
            fill(empty_buf, empty_buf+sizeof(SharedInfo), 0);
            m_file.write(empty_buf, sizeof(SharedInfo));

            // Complete initialization of shared info via the memory mapping:
            m_file_map.map(m_file, File::access_ReadWrite, sizeof (SharedInfo), File::map_NoSync);
            File::UnmapGuard fug_1(m_file_map);
            SharedInfo* info = m_file_map.get_addr();
            new (info) SharedInfo(dlevel); // Throws
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
            throw runtime_error("Lock file too large");
        if (info_size < sizeof (SharedInfo)) {
            continue;
        }
        // Map to memory
        m_file_map.map(m_file, File::access_ReadWrite, sizeof (SharedInfo), File::map_NoSync);
        File::UnmapGuard fug_1(m_file_map);

        // validate initialization complete:
        SharedInfo* info = m_file_map.get_addr();
        if (info->init_complete == 0) {
            continue;
        }

        // OK! lock file appears valid. We can now continue operations under the protection
        // of the controlmutex. The controlmutex protects the following activities:
        // - attachment of the database file
        // - start of the async daemon
        // - stop of the async daemon
        // - SharedGroup joining/leaving the sharing scheme
        // - Waiting for and signalling database changes
        {
            RobustLockGuard lock(info->controlmutex, &recover_from_dead_write_transact); // Throws
            // Even though we checked init_complete before grabbing the write mutex,
            // we do not need to check it again, because it is only changed under
            // an exclusive file lock, and we checked it under a shared file lock

            // proceed to initialize versioning and other metadata information related to
            // the database. Also create the database if we're first to get here.
            bool is_shared = true;
            bool read_only = false;
            bool no_create = true;
            bool skip_validate = false;
            if (info->versioning_ready == false) {
                no_create = no_create_file;
            }
            ref_type top_ref = alloc.attach_file(path, is_shared, read_only,
                                                 no_create, skip_validate, key); // Throws
            size_t file_size = alloc.get_baseline();
            if (info->versioning_ready == false) {
                info->latest_version_number = 1;
                info->init_versioning(top_ref, file_size);
                info->versioning_ready = true;
            }

#ifndef _WIN32
            // In async mode, we need to make sure the daemon is running and ready:
            if (dlevel == durability_Async && !is_backend) {
                while (info->daemon_ready == false) {
                    if (info->daemon_started == false) {
                        spawn_daemon(path);
                        info->daemon_started = true;
                    }
                    // FIXME: It might be more robust to sleep a little, then restart the loop
                    // cerr << "Waiting for daemon" << endl;
                    info->daemon_becomes_ready.wait(info->controlmutex,
                                                    &recover_from_dead_write_transact,
                                                    0);
                    // cerr << " - notified" << endl;
                }
            }
            // cerr << "daemon should be ready" << endl;
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
                throw runtime_error("Unsupported version");

            // Durability level cannot be changed at runtime
            if (info->flags != dlevel)
                throw runtime_error("Inconsistent durability level");

            // make our presence noted:
            ++info->num_participants;

            // Keep the mappings and file open:
            fug_2.release(); // Do not unmap
            fug_1.release(); // Do not unmap
            fcg.release(); // Do not close
        }
        break;
    }

    m_transact_stage = transact_Ready;
    // cerr << "open completed" << endl;

#ifndef _WIN32
    if (dlevel == durability_Async) {
        if (is_backend) {
            do_async_commits();
        }
    }
#endif
}


#ifdef TIGHTDB_ENABLE_REPLICATION

void SharedGroup::open(Replication& repl, DurabilityLevel dlevel, const uint8_t* key)
{
    TIGHTDB_ASSERT(!is_attached());
    string file = repl.get_database_path();
    bool no_create   = false;
    bool is_backend  = false;
    open(file, no_create, dlevel, is_backend, key); // Throws
    typedef _impl::GroupFriend gf;
    gf::set_replication(m_group, &repl);
}

#endif


SharedGroup::~SharedGroup() TIGHTDB_NOEXCEPT
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

    SharedInfo* info = m_file_map.get_addr();
    {
        RobustLockGuard lock(info->controlmutex, recover_from_dead_write_transact);
        --info->num_participants;
        // cerr << "closing" << endl;
        if (info->num_participants == 0) {

            // If the db file is just backing for a transient data structure,
            // we can delete it when done.
            if (info->flags == durability_MemOnly) {
                try {
                    size_t path_len = m_file_path.size()-5; // remove ".lock"
                    string db_path = m_file_path.substr(0, path_len); // Throws
                    m_group.m_alloc.detach();
                    util::File::remove(db_path.c_str());
                }
                catch(...) {} // ignored on purpose.
            }
        }
    }
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
void SharedGroup::wait_for_change()
{
    SharedInfo* info = m_file_map.get_addr();
    RobustLockGuard lock(info->controlmutex, recover_from_dead_write_transact);
    while (m_readlock.m_version == info->latest_version_number) {
        if (info->waiting_for_change == false)
            info->waiting_for_change = true;
        info->new_commit_available.wait(info->controlmutex,
                                        &recover_from_dead_write_transact,
                                        0);
    }
}

void SharedGroup::do_async_commits()
{
    bool shutdown = false;
    SharedInfo* info = m_file_map.get_addr();
    // NO client are allowed to proceed through open and update current_version
    // until they see 'daemon_running == true'
    // As we haven't set daemon_running yet, the following must hold:
    TIGHTDB_ASSERT(get_current_version() == 0 || get_current_version() == 1);

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
        info->daemon_becomes_ready.notify_all();
    }
    m_group.detach();

    while(true) {

        if (m_file.is_removed()) { // operator removed the lock file. take a hint!

            shutdown = true;
#ifdef TIGHTDB_ENABLE_LOGFILE
            cerr << "Lock file removed, initiating shutdown" << endl;
#endif
        }

        bool is_same;
        ReadLockInfo next_readlock = m_readlock;
        {
            // detect if we're the last "client", and if so, shutdown (must be under lock):
            RobustLockGuard lock(info->controlmutex, &recover_from_dead_write_transact);
            grab_latest_readlock(next_readlock, is_same);
            if (is_same && (shutdown || info->num_participants == 1)) {
#ifdef TIGHTDB_ENABLE_LOGFILE
                cerr << "Daemon exiting nicely" << endl << endl;
#endif
                release_readlock(next_readlock);
                release_readlock(m_readlock);
                info->daemon_started = false;
                info->daemon_ready = false;
                return;
            }
        }

        if (!is_same) {

#ifdef TIGHTDB_ENABLE_LOGFILE
            cerr << "Syncing from version " << m_readlock.m_version 
                 << " to " << next_readlock.m_version << endl;
#endif
            GroupWriter writer(m_group);
            writer.commit(next_readlock.m_top_ref);

#ifdef TIGHTDB_ENABLE_LOGFILE
            cerr << "..and Done" << endl;
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
            info->room_to_write.notify_all();
        }

        // If we have plenty of write slots available, relax and wait a bit before syncing
        if (free_write_slots > relaxed_sync_threshold) {
            timespec ts;
            timeval tv;
            // clock_gettime(CLOCK_REALTIME, &ts); <- would like to use this, but not there on mac
            gettimeofday(&tv, null_ptr);
            ts.tv_sec = tv.tv_sec;
            ts.tv_nsec = tv.tv_usec * 1000;
            ts.tv_nsec += 10000000; // 10 msec
            if (ts.tv_nsec >= 1000000000) { // overflow
                ts.tv_nsec -= 1000000000;
                ts.tv_sec += 1;
            }

            // we do a conditional wait instead of a sleep, allowing writers to wake us up
            // immediately if we run low on write slots.
            info->work_to_do.wait(info->balancemutex,
                                  &recover_from_dead_write_transact,
                                  &ts);
        }
        info->balancemutex.unlock();

    }
}
#endif // _WIN32


void SharedGroup::grab_latest_readlock(ReadLockInfo& readlock, bool& same_as_before)
{
    for (;;) {
        SharedInfo* r_info = m_reader_map.get_addr();
        readlock.m_reader_idx = r_info->readers.last();
        if (grow_reader_mapping(readlock.m_reader_idx)) { // throws
            // remapping takes time, so retry with a fresh entry
            continue;
        }
        const Ringbuffer::ReadCount& r = r_info->readers.get(readlock.m_reader_idx);
        // if the entry is stale and has been cleared by the cleanup process,
        // we need to start all over again. This is extremely unlikely, but possible.
        if (! atomic_double_inc_if_even(r.count)) // <-- most of the exec time spent here!
            continue;
        same_as_before = readlock.m_version == r.version;
        readlock.m_version      = r.version;
        readlock.m_top_ref    = to_size_t(r.current_top);
        readlock.m_file_size  = to_size_t(r.filesize);
        return;
    }
}


const Group& SharedGroup::begin_read()
{
    TIGHTDB_ASSERT(m_transact_stage == transact_Ready);

    bool same_version_as_before;
    grab_latest_readlock(m_readlock, same_version_as_before);
    if (same_version_as_before && m_group.may_reattach_if_same_version()) {
        m_group.reattach_from_retained_data();
    }
    else {
        // Prepare the group for a new transaction. A zero top ref
        // means that the file has just been created.
        try {
            m_group.init_for_transact(m_readlock.m_top_ref, m_readlock.m_file_size); // Throws
        }
        catch (...) {
            end_read();
            throw;
        }
    }
    m_transact_stage = transact_Reading;

    return m_group;
}


void SharedGroup::release_readlock(ReadLockInfo& readlock) TIGHTDB_NOEXCEPT
{
    SharedInfo* r_info = m_reader_map.get_addr();
    const Ringbuffer::ReadCount& r = r_info->readers.get(readlock.m_reader_idx);
    atomic_double_dec(r.count); // <-- most of the exec time spent here
}


void SharedGroup::end_read() TIGHTDB_NOEXCEPT
{
    if (!m_group.is_attached())
        return;

    TIGHTDB_ASSERT(m_transact_stage == transact_Reading);
    TIGHTDB_ASSERT(m_readlock.m_version != numeric_limits<size_t>::max());

    release_readlock(m_readlock);

    // The read may have allocated some temporary state
    m_group.detach_but_retain_data();
    m_transact_stage = transact_Ready;
}

#ifdef TIGHTDB_ENABLE_REPLICATION

void SharedGroup::promote_to_write(TransactLogRegistry& write_logs)
{
    TIGHTDB_ASSERT(m_transact_stage == transact_Reading);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = m_group.get_replication()) {
        repl->begin_write_transact(*this); // Throws
        try {
            do_begin_write();
            advance_read(write_logs);
        }
        catch (...) {
            repl->rollback_write_transact(*this);
            throw;
        }
        m_transact_stage = transact_Writing;
        return;
    }
#endif

    do_begin_write();

    // Advance to latest state (accessor update)
    advance_read(write_logs);
    m_transact_stage = transact_Writing;
}


void SharedGroup::advance_read(TransactLogRegistry& log_registry)
{
    TIGHTDB_ASSERT(m_transact_stage == transact_Reading);

    ReadLockInfo old_readlock = m_readlock;
    bool same_as_before;

    // advance current readlock while holding onto old one.
    grab_latest_readlock(m_readlock, same_as_before); // Throws
    release_readlock(old_readlock);

    if (same_as_before)
        return;

    // If the new top-ref is zero, then the previous top-ref must have
    // been zero too, and we are still seing an empty TightDB file
    // (note that this is possible even if the version has
    // changed). The purpose of the early-out in this case, is to
    // retain the temporary arrays that were created earlier by
    // Group::init_for_transact() to put the group accessor into a
    // valid state.
    if (m_readlock.m_top_ref == 0)
        return;

    // We know that the log_registry already knows about the new_version,
    // because in order for us to get the new version when we grab the
    // readlock, the new version must have been entered into the ringbuffer.
    // commit always updates the replication log BEFORE updating the ringbuffer.
    UniquePtr<BinaryData[]>
        logs(new BinaryData[m_readlock.m_version-old_readlock.m_version]); // Throws

    log_registry.get_commit_entries(old_readlock.m_version,
                                    m_readlock.m_version, logs.get());

    m_group.advance_transact(m_readlock.m_top_ref, m_readlock.m_file_size,
                             logs.get(),
                             logs.get() + (m_readlock.m_version-old_readlock.m_version)); // Throws

    log_registry.release_commit_entries(m_readlock.m_version);
}

#endif // TIGHTDB_ENABLE_REPLICATION

Group& SharedGroup::begin_write()
{
    TIGHTDB_ASSERT(m_transact_stage == transact_Ready);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = m_group.get_replication())
        repl->begin_write_transact(*this); // Throws
#endif

    try {
        do_begin_write();
        begin_read();
    }
    catch (...) {
#ifdef TIGHTDB_ENABLE_REPLICATION
        if (Replication* repl = m_group.get_replication())
            repl->rollback_write_transact(*this);
#endif
        throw;
    }

    m_transact_stage = transact_Writing;
    return m_group;
}


void SharedGroup::do_begin_write()
{
    SharedInfo* info = m_file_map.get_addr();

    // Get write lock
    // Note that this will not get released until we call
    // commit() or rollback()
    info->writemutex.lock(&recover_from_dead_write_transact); // Throws

#ifndef _WIN32
    if (info->flags == durability_Async) {

        info->balancemutex.lock(&recover_from_dead_write_transact); // Throws

        // if we are running low on write slots, kick the sync daemon
        if (info->free_write_slots < relaxed_sync_threshold)
            info->work_to_do.notify();

        // if we are out of write slots, wait for the sync daemon to catch up
        while (info->free_write_slots <= 0) {
            info->room_to_write.wait(info->balancemutex,
                                     recover_from_dead_write_transact);
        }

        info->free_write_slots--;
        info->balancemutex.unlock();
    }
#endif // _WIN32
}


void SharedGroup::commit()
{
    do_commit();

    end_read();
    // complete detach
    // (end_read allows group to retain data, but accessors become invalid after commit):
    m_group.complete_detach();
}


#ifdef TIGHTDB_ENABLE_REPLICATION

void SharedGroup::commit_and_continue_as_read()
{
    do_commit();

    // Mark all managed space (beyond the attached file) as free.
    m_group.m_alloc.reset_free_space_tracking(); // Throws

    size_t old_baseline = m_group.m_alloc.get_baseline();

    // Remap file if it has grown
    if (m_readlock.m_file_size > old_baseline) {
        bool addr_changed = m_group.m_alloc.remap(m_readlock.m_file_size); // Throws
        // If the file was mapped to a new address, all array accessors must be
        // updated.
        if (addr_changed)
            old_baseline = 0;
    }
    m_group.update_refs(m_readlock.m_top_ref, old_baseline);
}

void SharedGroup::rollback_and_continue_as_read()
{
    // Mark all managed space (beyond the attached file) as free.
    m_group.m_alloc.reset_free_space_tracking(); // Throws

    m_transact_stage = transact_Reading;

    // get the commit log and use it to rollback all accessors:
    if (Replication* repl = m_group.get_replication()) {

        // this call is vectored through to do_rollback_and....
        repl->rollback_write_transact(*this);
    }

    // release exclusive write access: (FIXME: do this earlier?)
    SharedInfo* info = m_file_map.get_addr();
    info->writemutex.unlock();
}

void SharedGroup::do_rollback_and_continue_as_read(const char* begin, const char* end)
{
    BinaryData buffer(begin, end-begin);
    m_group.reverse_transact(m_readlock.m_top_ref, buffer);
}



#endif // TIGHTDB_ENABLE_REPLICATION


void SharedGroup::do_commit()
{
    TIGHTDB_ASSERT(m_transact_stage == transact_Writing);

    // FIXME: This fails when replication is enabled and the first transaction
    // in a lock-file session is rolled back (aborted), because then the first
    // committed transaction will have m_readlock.m_version > 1.
    if (m_readlock.m_version == 1)
        m_group.reset_free_space_versions(); // Throws

    SharedInfo* info = m_file_map.get_addr();
    SharedInfo* r_info = m_reader_map.get_addr();

    // FIXME: ExceptionSafety: Corruption has happened if
    // low_level_commit() throws, because we have already told the
    // replication manager to commit. It is not yet clear how this
    // conflict should be solved. The solution is probably to catch
    // the exception from low_level_commit() and when caught, mark the
    // local database as not-up-to-date. The exception should not be
    // rethrown, because the commit was effectively successful.

    {
        uint_fast64_t new_version;
#ifdef TIGHTDB_ENABLE_REPLICATION
        // It is essential that if Replication::commit_write_transact() fails,
        // then the transaction is not completed. In that case, a subsequent
        // call to rollback() must still roll the transaction back.
        if (Replication* repl = m_group.get_replication()) {
            uint_fast64_t current_version = r_info->get_current_version_unchecked();
            new_version = repl->commit_write_transact(*this, current_version); // Throws
        }
        else {
            new_version = r_info->get_current_version_unchecked() + 1;
        }
#else
        new_version = r_info->get_current_version_unchecked() + 1;
#endif
        low_level_commit(new_version); // Throws
    }

    // advance readlock but dont update accessors:
    // As this is done under lock, along with the addition above of the newest commit,
    // we know for certain that the readlock we will grab WILL refer to our own newly
    // completed commit.
    release_readlock(m_readlock);
    // FIXME: Why grab a new read-lock as part of a regular commit? It seems
    // wrong.
    //
    // FIXME: We need to find a way to give SharedGroup::commit() a sound and
    // intelligable exception behavior. The desirable exception behavior is
    // probably that the commit has occured if, and only if it does not
    // throw. The possible exception from grab_latest_readlock() makes this
    // harder than it would otherwise have been.
    bool ignored;
    grab_latest_readlock(m_readlock, ignored); // Throws

    // downgrade to a read transaction (if not, assert in end_read)
    m_transact_stage = transact_Reading;
    // Release write lock
    info->writemutex.unlock();
}


// FIXME: This method must work correctly even if it is called after a
// failed call to commit(). A failed call to commit() is any that
// returns to the caller by throwing an exception. As it is right now,
// rollback() does not handle all cases.
void SharedGroup::rollback() TIGHTDB_NOEXCEPT
{
    if (m_group.is_attached()) {
        TIGHTDB_ASSERT(m_transact_stage == transact_Writing);

#ifdef TIGHTDB_ENABLE_REPLICATION
        if (Replication* repl = m_group.get_replication())
            repl->rollback_write_transact(*this);
#endif
        m_transact_stage = transact_Reading;
        end_read();

        SharedInfo* info = m_file_map.get_addr();

        // Release write lock
        info->writemutex.unlock();

        // Clear all changes made during transaction
        m_group.detach();
    }
}



// given an index (which the caller wants to used to index the ringbuffer), verify
// that the given entry is within the memory mapped. If not, remap it!
bool SharedGroup::grow_reader_mapping(uint_fast32_t index)
{
    if (index >= m_local_max_entry) {
        // handle mapping expansion if required
        SharedInfo* r_info = m_reader_map.get_addr();
        m_local_max_entry = r_info->readers.get_num_entries();
        size_t info_size = sizeof(SharedInfo) + r_info->readers.compute_required_space(m_local_max_entry);
        // cout << "Growing reader mapping to " << infosize << endl;
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
    }

    // Do the actual commit
    TIGHTDB_ASSERT(m_group.m_top.is_attached());
    TIGHTDB_ASSERT(readlock_version <= new_version);
    // info->readers.dump();
    GroupWriter out(m_group); // Throws
    out.set_versions(new_version, readlock_version);
    // Recursively write all changed arrays to end of file
    ref_type new_top_ref = out.write_group(); // Throws
    // cout << "Writing version " << new_version << ", Topptr " << new_top_ref
    //     << " Readlock at version " << readlock_version << endl;
    // In durability_Full mode, we just use the file as backing for
    // the shared memory. So we never actually flush the data to disk
    // (the OS may do so opportinisticly, or when swapping). So in
    // this mode the file on disk may very likely be in an invalid
    // state.
    if (info->flags == durability_Full)
        out.commit(new_top_ref); // Throws
    size_t new_file_size = out.get_file_size();
    // Update reader info
    {
        SharedInfo* r_info = m_reader_map.get_addr();
        if (r_info->readers.is_full()) {
            // buffer expansion
            uint_fast32_t entries = r_info->readers.get_num_entries();
            entries = entries + 32;
            size_t new_info_size = sizeof(SharedInfo) + r_info->readers.compute_required_space( entries );
            // cout << "resizing: " << entries << " = " << new_info_size << endl;
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
#ifndef _WIN32
    {
        RobustLockGuard lock(info->controlmutex, recover_from_dead_write_transact);
        info->latest_version_number = new_version;
        if (info->waiting_for_change) {
            info->waiting_for_change = false;
            info->new_commit_available.notify_all();
        }
    }
#endif
}


void SharedGroup::reserve(size_t size)
{
    TIGHTDB_ASSERT(is_attached());
    // FIXME: There is currently no synchronization between this and
    // concurrent commits in progress. This is so because it is
    // believed that the OS guarantees race free behavior when
    // util::File::prealloc_if_supported() (posix_fallocate() on
    // Linux) runs concurrently with modfications via a memory map of
    // the file. This assumption must be verified though.
    m_group.m_alloc.reserve(size);
}
