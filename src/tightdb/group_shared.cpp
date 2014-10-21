#include <cerrno>
#include <algorithm>
#include <iostream>

#include <fcntl.h>

#include <tightdb/util/features.h>
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

// Constants controlling timeout behaviour during opening of a shared group
const int max_retries_awaiting_shutdown = 5;
// rough limits, milliseconds:
const int max_wait_for_ok_filesize = 500;
const int max_wait_for_sharedinfo_valid = 500;
const int max_wait_for_daemon_start = 500;

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

    // FIXME: The use of uint_fast64_t is in principle wrong
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

    Atomic<uint16_t> init_complete; // indicates lock file has valid content
    Atomic<uint16_t> shutdown_started; // indicates that shutdown is in progress
    uint16_t version;
    uint16_t flags;

    RobustMutex writemutex;
    RobustMutex balancemutex;
#ifndef _WIN32
    // FIXME: windows pthread support for condvar not ready
    CondVar room_to_write;
    CondVar work_to_do;
#endif
    uint16_t free_write_slots;
    // IMPORTANT: The ringbuffer MUST be the last field in SharedInfo - see above.
    Ringbuffer readers;
    SharedInfo(ref_type top_ref, size_t file_size, DurabilityLevel);
    ~SharedInfo() TIGHTDB_NOEXCEPT {}
    uint_fast64_t get_current_version_unchecked() const
    {
        return readers.get_last().version;
    }
};


SharedGroup::SharedInfo::SharedInfo(ref_type top_ref, size_t file_size, DurabilityLevel dlevel):
#ifndef _WIN32
    writemutex(), // Throws
    balancemutex(), // Throws
    room_to_write(CondVar::process_shared_tag()), // Throws
    work_to_do(CondVar::process_shared_tag()) // Throws
#else
    writemutex(), // Throws
    balancemutex() // Throws
#endif
{
    version  = 0;
    flags    = dlevel; // durability level is fixed from creation
    // Create our first versioning entry:
    Ringbuffer::ReadCount& r = readers.get_next();
    r.filesize = file_size;
    r.version = 1;
    r.current_top = top_ref;
    readers.use_next();
    shutdown_started.store_release(0);
    free_write_slots = 0;
    init_complete.store_release(1);
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
            // int err = errno; // TODO: include err in exception string
            throw runtime_error("'sysconf(_SC_OPEN_MAX)' failed ");
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


inline void micro_sleep(uint_fast64_t microsec_delay)
{
#ifdef _WIN32
    // FIXME: this is not optimal, but it should work
    Sleep(static_cast<DWORD>(microsec_delay/1000+1));
#else
    usleep(useconds_t(microsec_delay));
#endif
}

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
                       DurabilityLevel dlevel, bool is_backend)
{
    TIGHTDB_ASSERT(!is_attached());

    m_file_path = path + ".lock";
    bool must_retry;
    int retry_count = max_retries_awaiting_shutdown;
    do {
        bool need_init = false;
        size_t info_size = 0;
        must_retry = false;

        // Open shared coordination buffer - non-excepting approach

        m_file.open(m_file_path, need_init);
        if (need_init) {
            info_size = sizeof (SharedInfo);
            // Make sure to initialize the file in such a way, that when it reaches the
            // size of SharedInfo, it contains just zeroes.
            char empty_buf[sizeof (SharedInfo)];
            fill(empty_buf, empty_buf+sizeof(SharedInfo), 0);
            m_file.write(empty_buf, info_size);
            need_init = true;
        }

        using namespace tightdb::util;
        File::CloseGuard fcg(m_file);
        int time_left = max_wait_for_ok_filesize;
        while (1) {

            time_left--;
            // need to validate the size of the file before trying to map it
            // possibly waiting a little for size to go nonzero, if another
            // process is creating the file in parallel.
            if (int_cast_with_overflow_detect(m_file.get_size(), info_size))
                throw runtime_error("Lock file too large");
            if (time_left <= 0)
                throw PresumablyStaleLockFile(m_file_path);
            // wait for file to at least contain the basic shared info block
            // NB! it might be larger due to expansion of the ring buffer.
            if (info_size < sizeof (SharedInfo))
                micro_sleep(1000);
            else
                break;
        }
        // File is now guaranteed to be large enough that we can map it and access all fields
        // of the SharedInfo structure.

        // Map to memory
        m_file_map.map(m_file, File::access_ReadWrite, sizeof (SharedInfo), File::map_NoSync);
        File::UnmapGuard fug_1(m_file_map);

        // We need to map the info file once more for the readers part
        // since that part can be resized and as such remapped which
        // could move our mutexes (which we don't want to risk moving while
        // they are locked)
        m_reader_map.map(m_file, File::access_ReadWrite, sizeof (SharedInfo), File::map_NoSync);
        File::UnmapGuard fug_2(m_reader_map);

        SharedInfo* info = m_file_map.get_addr();
        m_local_max_entry = 0;
        m_readlock.m_version = 0;
        SlabAlloc& alloc = m_group.m_alloc;

        // If we are the process that *Created* the coordination buffer, we are obliged to
        // initialize it. All other processer will wait for os to signal completion of the
        // initialization.
        if (need_init) {
            // file locks are used solely to detect if/when all clients
            // are done accessing the database. We grab them here and hold
            // them until the destructor is called, where we try to promote
            // them to exclusive to detect if we can shutdown.
            m_file.lock_shared();

            // If we are the first we may have to create the database file
            // but we invalidate the internals right after to avoid conflicting
            // with old state when starting transactions
            bool is_shared = true;
            bool read_only = false;
            bool skip_validate = false;
            ref_type top_ref;
            try {
                top_ref = alloc.attach_file(path, is_shared, read_only, no_create_file,
                                                     skip_validate); // Throws
            }
            catch (...) {

                // something went wrong. We need to clean up the .lock file carefully to prevent
                // other processes from getting to it and waiting for us to complete initialization
                // We bypass normal initialization
                info->shutdown_started.store_relaxed(1);
                info->init_complete.store_relaxed(1);

                // remove the file - due to windows file semantics, we have to manually close it
                fug_2.release(); // we need to unmap manually
                fug_1.release(); // - do -
                fcg.release();   // we need to close manually
                m_file_map.unmap();
                m_reader_map.unmap();
                m_file.unlock();
                m_file.close();
                File::try_remove(m_file_path.c_str());

                // rethrow whatever went wrong
                throw;
            }

#ifdef TIGHTDB_ENABLE_REPLICATION
            // If replication is enabled, we need to reset log management:
            Replication* repl = _impl::GroupFriend::get_replication(m_group);
            if (repl)
                repl->reset_log_management();
#endif
            // Call SharedInfo::SharedInfo() (placement new)
            // This also sets init_complete, which allows other accessors to proceed,
            // hence must happen after completing initialization of the log management.
            size_t file_size = alloc.get_baseline();
            new (info) SharedInfo(top_ref, file_size, dlevel); // Throws
            // we need a thread-local copy of the number of ringbuffer entries in order
            // to detect concurrent expansion of the ringbuffer.
            m_local_max_entry = info->readers.get_num_entries();

            // Set initial version so we can track if other instances
            // change the db
            m_readlock.m_version = info->get_current_version_unchecked();
#ifndef _WIN32
            // In async mode we need a separate process to do the async commits
            // We start it up here during init so that it only get started once
            if (dlevel == durability_Async) {
                spawn_daemon(path);
            }
#endif

        }
        else {
            // wait for init complete:
            int wait_count = max_wait_for_sharedinfo_valid;
            while (wait_count && (info->init_complete.load_acquire() == 0)) {
                wait_count--;
                micro_sleep(1000);
            }

            // If we exceed our wait without even seeing init complete, then it is most likely
            // that the initializing process has crashed. HOWEVER - it may just be delayed so
            // far, that it hasn't taken the initial shared lock yet. So, we dare not declare
            // the .lock file stale, although it is very likely.
            if (info->init_complete.load_acquire() == 0)
                throw PresumablyStaleLockFile(m_file_path);

            // use file locking in an attempt to determine if we have exclusive access to the file.
            // We need to wait for init_complete to be signalled, before we can safely manipulate
            // the file contents - among others, the shutdown_started flag cannot be trusted and
            // cannot be modified earlier.
            if (m_file.try_lock_exclusive()) {

                // At this point no other process can be trying to initialize the file.
                // Somebody else may be executing in the destructor and probing to see if they are
                // alone - they can get a false success (because they don't see "us"). Because of this
                // possibility of false successes, the only allowed action is to close and TRY to remove the
                // file - even if multiple processes does this in parallel, the end result is the same.
                //
                // Poison the file. As we have exclusive access, no other will examine or change
                // the shutdown_started field.
                info->shutdown_started.store_release(1);
                fug_2.release(); // we need to unmap manually
                fug_1.release(); // - do -
                fcg.release();   // we need to close manually
                m_file.unlock();
                // <- from this point another thread/process may open the file, BUT it will encounter
                // a set shutdown_started field, back out and retry
                m_file.close();
                m_file_map.unmap();
                m_reader_map.unmap();
                File::try_remove(m_file_path.c_str());
                must_retry = true;
                continue; // retry, now with stale file removed
            }
            else {

                m_file.lock_shared();
            }
            if (info->shutdown_started.load_acquire()) {
                retry_count--;
                if (retry_count == 0)
                    throw PresumablyStaleLockFile(m_file_path);
                must_retry = true;
                micro_sleep(1000);
                continue;
                // this will unmap and close the lock file. Then we retry
            }
            if (info->version != 0)
                throw runtime_error("Unsupported version");

            // Durability level cannot be changed at runtime
            if (info->flags != dlevel)
                throw runtime_error("Inconsistent durability level");

            // Setup the group, but leave it in invalid state
            bool is_shared = true;
            bool read_only = false;
            bool no_create = true;
            bool skip_validate = true; // To avoid race conditions
            try {
                alloc.attach_file(path, is_shared, read_only, no_create, skip_validate); // Throws
            }
            catch (File::NotFound) {
                throw LockFileButNoData(path);
            }

        }

        fug_2.release(); // Do not unmap
        fug_1.release(); // Do not unmap
        fcg.release(); // Do not close
    }
    while (must_retry);

    m_transact_stage = transact_Ready;

#ifndef _WIN32
    if (dlevel == durability_Async) {
        if (is_backend) {
            do_async_commits();
        }
        else {
            // In async mode we need to wait for the commit process to get ready
            SharedInfo* const info = m_file_map.get_addr();
            int maxwait = max_wait_for_daemon_start;
            while (maxwait--) {
                if (info->init_complete.load_acquire() == 2) {
                    return;
                }
                micro_sleep(1000);
            }
            throw runtime_error("Failed to observe async commit starting");
        }
    }
#endif
}


#ifdef TIGHTDB_ENABLE_REPLICATION

void SharedGroup::open(Replication& repl)
{
    TIGHTDB_ASSERT(!is_attached());
    string file = repl.get_database_path();
    bool no_create   = false;
    DurabilityLevel dlevel = durability_Full;
    // we must install replication in group before calling open,
    // because open depends on it. FIXME.
    typedef _impl::GroupFriend gf;
    gf::set_replication(m_group, &repl);
    open(file, no_create, dlevel); // Throws
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

#ifndef _WIN32
    if (info->flags == durability_Async) {
        m_file.unlock();
        return;
    }
#endif

    m_file.unlock();
    if (!m_file.try_lock_exclusive())
        return;

    if (info->shutdown_started.load_acquire()) {
        m_file.unlock();
        return;
    }
    info->shutdown_started.store_release(1);

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

    // info->~SharedInfo(); // DO NOT Call destructor

    m_file.close();
    m_file_map.unmap();
    m_reader_map.unmap();
#ifdef TIGHTDB_ENABLE_REPLICATION
            // If replication is enabled, we need to stop log management:
            Replication* repl = _impl::GroupFriend::get_replication(m_group);
            if (repl)
                repl->stop_logging();
#endif
    try {
        util::File::remove(m_file_path.c_str());
    }
    catch (...) {} // ignored on purpose
}

bool SharedGroup::pin_read_transactions()
{
    if (m_transactions_are_pinned) {
        throw runtime_error("transactions are already pinned, cannot pin again");
    }
    if (m_transact_stage != transact_Ready) {
        throw runtime_error("pinning transactions not allowed inside a transaction");
    }
    bool same_as_before;
    grab_latest_readlock(m_readlock, same_as_before);

    // Prepare the group for a new transaction. A zero top ref means
    // that the file has just been created.
    try {
        m_group.init_for_transact(m_readlock.m_top_ref, m_readlock.m_file_size); // Throws
    }
    catch (...) {
        end_read();
        throw;
    }
    m_group.detach_but_retain_data();
    m_transactions_are_pinned = true;
    return !same_as_before;
}

void SharedGroup::unpin_read_transactions()
{
    if (! m_transactions_are_pinned) {
        throw runtime_error("transactions are not pinned, cannot unpin");
    }
    if (m_transact_stage != transact_Ready) {
        throw runtime_error("unpinning transactions not allowed inside a transaction");
    }
    m_transactions_are_pinned = false;
    release_readlock(m_readlock);
}

bool SharedGroup::has_changed()
{
    bool changed = m_readlock.m_version != get_current_version();
    return changed;
}

#ifndef _WIN32

void SharedGroup::do_async_commits()
{
    bool shutdown = false;
    bool file_already_removed = false;
    SharedInfo* info = m_file_map.get_addr();
    // NO client are allowed to proceed and update current_version
    // until they see the init_complete == 2.
    // As we haven't set init_complete to 2 yet, it is safe to assert the following:
    TIGHTDB_ASSERT(get_current_version() == 0 || get_current_version() == 1);

    // We always want to keep a read lock on the last version
    // that was commited to disk, to protect it against being
    // overwritten by commits being made to memory by others.
    // Note that taking this lock also signals to the other
    // processes that that they can start commiting to the db.
    begin_read(); // Throws
    // we must treat version and version_index the same way:
    ReadLockInfo last_readlock = m_readlock;

    info->free_write_slots = max_write_slots;
    info->init_complete.store_release(2); // allow waiting clients to proceed
    m_group.detach();

    while(true) {

        if (m_file.is_removed()) { // operator removed the lock file. take a hint!

            file_already_removed = true; // don't remove what is already gone
            info->shutdown_started.store_release(1);
            shutdown = true;
#ifdef TIGHTDB_ENABLE_LOGFILE
            cerr << "Lock file removed, initiating shutdown" << endl;
#endif
        }

        // detect if we're the last "client", and if so mark the
        // lock file invalid. Any client coming along before we
        // finish syncing will see the lock file, but detect that
        // the daemon has abandoned it. It can then wait for the
        // lock file to be removed (with a timeout).
        m_file.unlock();
        if (m_file.try_lock_exclusive()) {
            info->shutdown_started.store_release(1);
            shutdown = true;
        }
        // if try_lock_exclusive fails, we loose our read lock, so
        // reacquire it! At the moment this is not used for anything,
        // because ONLY the daemon ever asks for an exclusive lock
        // when async commits are used.
        else
            m_file.lock_shared();

        if (has_changed()) {

#ifdef TIGHTDB_ENABLE_LOGFILE
            cerr << "Syncing...";
#endif
            // Get a read lock on the (current) version that we want
            // to commit to disk.
            m_transact_stage = transact_Ready; // FAKE stage to prevent begin_read from failing

            begin_read(); // Throws

            ReadLockInfo current_readlock = m_readlock;

            GroupWriter writer(m_group);
            writer.commit(current_readlock.m_top_ref);

            // Now we can release the version that was previously commited
            // to disk and just keep the lock on the latest version.
            m_readlock = last_readlock;
            end_read();
            last_readlock = m_readlock;
#ifdef TIGHTDB_ENABLE_LOGFILE
            cerr << "..and Done" << endl;
#endif
        }
        else
            sched_yield(); // prevent spinning on has_changed!

        if (shutdown) {
            // Being the backend process, we own the lock file, so we
            // have to clean up when we shut down.
            // info->~SharedInfo(); // DO NOT Call destructor
            m_file_map.unmap();
#ifdef TIGHTDB_ENABLE_LOGFILE
            cerr << "Removing coordination file" << endl;
#endif
            if (!file_already_removed)
                util::File::remove(m_file_path);
#ifdef TIGHTDB_ENABLE_LOGFILE
            cerr << "Daemon exiting nicely" << endl << endl;
#endif
            return;
        }

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

    if (m_transactions_are_pinned) {
        m_group.reattach_from_retained_data();
    }
    else {

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

    if (! m_transactions_are_pinned) {
        release_readlock(m_readlock);
    }

    // The read may have allocated some temporary state
    m_group.detach_but_retain_data();
    m_transact_stage = transact_Ready;
}

#ifdef TIGHTDB_ENABLE_REPLICATION

void SharedGroup::promote_to_write()
{
    TIGHTDB_ASSERT(m_transact_stage == transact_Reading);

    if (m_transactions_are_pinned)
        throw runtime_error("Write transactions are not allowed while transactions are pinned");

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = m_group.get_replication()) {
        repl->begin_write_transact(*this); // Throws
        try {
            do_begin_write();
            advance_read();
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
    advance_read();
    m_transact_stage = transact_Writing;
}


void SharedGroup::advance_read()
{
    TIGHTDB_ASSERT(m_transact_stage == transact_Reading);
    TIGHTDB_ASSERT(!m_transactions_are_pinned);

    ReadLockInfo old_readlock = m_readlock;
    bool same_as_before;
    Replication* repl = _impl::GroupFriend::get_replication(m_group);

    // advance current readlock while holding onto old one - we MUST hold onto
    // the old readlock until after the call to advance_transact. Once a readlock
    // is released, the release may propagate to the commit log management, causing
    // it to reclaim memory for old commit logs. We must finished use of the commit log
    // before allowing that to happen.
    grab_latest_readlock(m_readlock, same_as_before); // Throws

    if (same_as_before) {
        release_readlock(old_readlock);
        return;
    }

    // If the new top-ref is zero, then the previous top-ref must have
    // been zero too, and we are still seing an empty TightDB file
    // (note that this is possible even if the version has
    // changed). The purpose of the early-out in this case, is to
    // retain the temporary arrays that were created earlier by
    // Group::init_for_transact() to put the group accessor into a
    // valid state.
    if (m_readlock.m_top_ref == 0) {
        release_readlock(old_readlock);
        return;
    }

    // We know that the log_registry already knows about the new_version,
    // because in order for us to get the new version when we grab the
    // readlock, the new version must have been entered into the ringbuffer.
    // commit always updates the replication log BEFORE updating the ringbuffer.
    UniquePtr<BinaryData[]>
        logs(new BinaryData[m_readlock.m_version-old_readlock.m_version]); // Throws

    repl->get_commit_entries(old_readlock.m_version, m_readlock.m_version, logs.get());

    m_group.advance_transact(m_readlock.m_top_ref, m_readlock.m_file_size,
                             logs.get(),
                             logs.get() + (m_readlock.m_version-old_readlock.m_version)); // Throws

    // OK to release the readlock here:
    release_readlock(old_readlock);
}

#endif // TIGHTDB_ENABLE_REPLICATION

Group& SharedGroup::begin_write()
{
    TIGHTDB_ASSERT(m_transact_stage == transact_Ready);

    if (m_transactions_are_pinned)
        throw runtime_error("Write transactions are not allowed while transactions are pinned");

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
#ifdef TIGHTDB_ENABLE_REPLICATION
        // If replication is enabled, we need to propagate knowledge of the earliest 
        // available version:
        Replication* repl = _impl::GroupFriend::get_replication(m_group);
        if (repl)
            repl->set_oldest_version_needed(readlock_version);
#endif

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
