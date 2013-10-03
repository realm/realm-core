#include <fcntl.h>
#include <errno.h>
#include <algorithm>

#include <tightdb/safe_int_ops.hpp>
#include <tightdb/terminate.hpp>
#include <tightdb/thread.hpp>
#include <tightdb/group_writer.hpp>
#include <tightdb/group_shared.hpp>
#include <tightdb/group_writer.hpp>

#ifndef _WIN32
#include <sys/wait.h>
#include <sys/time.h>
#include <unistd.h>
#else
#define NOMINMAX
#include <windows.h>
#endif

// #define TIGHTDB_ENABLE_LOGFILE

using namespace std;
using namespace tightdb;

// Constants controlling the amount of uncommited writes in flight:
static const uint16_t MAX_WRITE_SLOTS = 100;
static const uint16_t RELAXED_SYNC_THRESHOLD = 50;

// Constants controlling timeout behaviour during opening of a shared group
static const int MAX_RETRIES_AWAITING_SHUTDOWN = 5;
// rough limits, milliseconds:
static const int MAX_WAIT_FOR_OK_FILESIZE = 100;
static const int MAX_WAIT_FOR_SHAREDINFO_VALID = 100;
static const int MAX_WAIT_FOR_DAEMON_START = 100;

struct SharedGroup::ReadCount {
    uint64_t version;
    uint32_t count;
};

struct SharedGroup::SharedInfo {
    Relaxed<uint64_t> current_version;
    Atomic<uint16_t> init_complete; // indicates lock file has valid content
    Atomic<uint16_t> shutdown_started; // indicates that shutdown is in progress
    uint16_t version;
    uint16_t flags;

    Mutex readmutex;
    RobustMutex writemutex;
    RobustMutex balancemutex;
#ifndef _WIN32
    // FIXME: windows pthread support for condvar not ready
    CondVar room_to_write;
    CondVar work_to_do;
#endif
    uint16_t free_write_slots;
    uint64_t filesize;

    uint64_t current_top;

    uint32_t infosize;
    uint32_t capacity_mask; // Must be on the form 2**n - 1
    uint32_t put_pos;
    uint32_t get_pos;

    static const int init_readers_size = 32; // Must be a power of two
    ReadCount readers[init_readers_size];

    SharedInfo(const SlabAlloc&, size_t file_size, DurabilityLevel);
    ~SharedInfo() TIGHTDB_NOEXCEPT {}
};

SharedGroup::SharedInfo::SharedInfo(const SlabAlloc& alloc, size_t file_size,
                                    DurabilityLevel dlevel):
#ifndef _WIN32
    readmutex(Mutex::process_shared_tag()), // Throws
    writemutex(), // Throws
    balancemutex(), // Throws
    room_to_write(CondVar::process_shared_tag()), // Throws
    work_to_do(CondVar::process_shared_tag()) // Throws
#else
    readmutex(Mutex::process_shared_tag()), // Throws
    writemutex(), // Throws
    balancemutex() // Throws
#endif
{
    version  = 0;
    flags    = dlevel; // durability level is fixed from creation
    filesize = alloc.get_baseline();
    infosize = uint32_t(file_size);
    current_top     = alloc.get_top_ref();
    current_version.store_relaxed(1);
    capacity_mask   = init_readers_size - 1;
    put_pos = 0;
    get_pos = 0;
    shutdown_started.store_release(0);
    free_write_slots = 0;
    init_complete.store_release(1);
}


namespace {

void recover_from_dead_write_transact()
{
    // Nothing needs to be done
}

} // anonymous namespace

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
        for (i=m-1;i>=0;--i) close(i); 
        i=::open("/dev/null",O_RDWR);
#ifdef TIGHTDB_ENABLE_LOGFILE
        // FIXME: Do we want to always open the log file? Should it be configurable?
        i=::open((file+".log").c_str(),O_RDWR | O_CREAT | O_APPEND | O_SYNC, S_IRWXU);
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
        const char* exe = getenv("TIGHTDBD_PATH");
        if (exe == NULL)
#ifndef TIGTHDB_DEBUG
            exe = "/usr/local/bin/tightdbd";
#else
            exe = "/usr/local/bin/tightdbd-dbg";
#endif
        execl(exe, exe, file.c_str(), (char*) NULL);

        // if we continue here, exec has failed so return error
        // if exec succeeds, we don't come back here.
        _Exit(1);
        // child process ends here

    } else if (pid > 0) { // parent process, fork succeeded:
        
        // use childs exit code to catch and report any errors:
        int status;
        int pid_changed;
        do {
            pid_changed = waitpid(pid, &status, 0);
        } while (pid_changed == -1 && errno == EINTR);
        if (pid_changed != pid)
            throw runtime_error("failed to wait for daemon start");
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

    } else { // Parent process, fork failed!

        throw runtime_error("Failed to spawn async commit");
    }
}
#else
void spawn_daemon(const string& file) {}
#endif


inline void micro_sleep(uint64_t microsec_delay)
{
#ifdef _WIN32
    // FIXME: this is not optimal, but it should work
    Sleep(static_cast<DWORD>(microsec_delay/1000+1));
#else
    usleep(useconds_t(microsec_delay));
#endif
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

void SharedGroup::open(const string& path, bool no_create_file,
                       DurabilityLevel dlevel, bool is_backend)
{
    TIGHTDB_ASSERT(!is_attached());

    m_file_path = path + ".lock";
    bool must_retry;
    int retry_count = MAX_RETRIES_AWAITING_SHUTDOWN;
    do {
        bool need_init = false;
        size_t file_size = 0;
        must_retry = false;

        // Open shared coordination buffer
        try {

            m_file.open(m_file_path, 
                        File::access_ReadWrite, File::create_Must, 0);
            file_size = sizeof (SharedInfo);
            m_file.prealloc(0,file_size);
            need_init = true;

        } 
        catch (File::Exists&) {

            // if this one throws, just propagate it:
            m_file.open(m_file_path, 
                        File::access_ReadWrite, File::create_Never, 0);
        }

        // file locks are used solely to detect if/when all clients
        // are done accessing the database. We grab them here and hold
        // them until the destructor is called, where we try to promote
        // them to exclusive to detect if we can shutdown.
        m_file.lock_shared();
        File::CloseGuard fcg(m_file);
        int time_left = MAX_WAIT_FOR_OK_FILESIZE;
        while (1) {
            time_left--;
            // need to validate the size of the file before trying to map it
            // possibly waiting a little for size to go nonzero, if another
            // process is creating the file in parallel.
            if (int_cast_with_overflow_detect(m_file.get_size(), file_size))
                throw runtime_error("Lock file too large");
            if (time_left <= 0)
                throw PresumablyStaleLockFile(m_file_path);
            // wait for file to at least contain the basic shared info block
            // NB! it might be larger due to expansion of the ring buffer.
            if (file_size < sizeof (SharedInfo))
                micro_sleep(1000);
            else
                break;
        }

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
        SlabAlloc& alloc = m_group.m_alloc;

        if (need_init) {
            // If we are the first we may have to create the database file
            // but we invalidate the internals right after to avoid conflicting
            // with old state when starting transactions
            bool is_shared = true;
            bool read_only = false;
            bool skip_validate = false;
            alloc.attach_file(path, is_shared, read_only, no_create_file, skip_validate); // Throws

            // Call SharedInfo::SharedInfo() (placement new)
            new (info) SharedInfo(alloc, file_size, dlevel); // Throws

            // Set initial version so we can track if other instances
            // change the db
            m_version = info->current_version.load_relaxed();

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
            int wait_count = MAX_WAIT_FOR_SHAREDINFO_VALID;
            while (wait_count && (info->init_complete.load_acquire() == 0)) {
                wait_count--;
                micro_sleep(1000);
            }
            if (info->init_complete.load_acquire() == 0)
                throw PresumablyStaleLockFile(m_file_path);
            if (info->version != 0)
                throw runtime_error("Unsupported version");
            if (info->shutdown_started.load_acquire()) {
                retry_count--;
                if (retry_count == 0)
                    throw PresumablyStaleLockFile(m_file_path);
                must_retry = true;
                micro_sleep(1000);
                continue;
                // this will unmap and close the lock file. Then we retry
            }

            // Durability level cannot be changed at runtime
            if (info->flags != dlevel)
                throw runtime_error("Inconsistent durability level");

            // Setup the group, but leave it in invalid state
            bool is_shared = true;
            bool read_only = false;
            bool no_create = true;
            bool skip_validate = true; // To avoid race conditions
            alloc.attach_file(path, is_shared, read_only, no_create, skip_validate); // Throws

        }

        fug_2.release(); // Do not unmap
        fug_1.release(); // Do not unmap
        fcg.release(); // Do not close
    } while (must_retry);

#ifdef TIGHTDB_DEBUG
    m_transact_stage = transact_Ready;
#endif
#ifndef _WIN32
    if (dlevel == durability_Async) {
        if (is_backend) {
            do_async_commits();
        }
        else {
            // In async mode we need to wait for the commit process to get ready
            SharedInfo* const info = m_file_map.get_addr();
            int maxwait = MAX_WAIT_FOR_DAEMON_START;
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


SharedGroup::~SharedGroup() TIGHTDB_NOEXCEPT
{
    if (!is_attached())
        return;

    TIGHTDB_ASSERT(m_transact_stage == transact_Ready);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = m_group.get_replication())
        delete repl;
#endif

    SharedInfo* info = m_file_map.get_addr();

#ifndef _WIN32
    if (info->flags == durability_Async) {
        m_file.unlock();
        return;
    }
#endif
    m_file.unlock();
    if (!m_file.try_lock_exclusive()) {
        return;
    }

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
            File::remove(db_path.c_str());
        } 
        catch(...) { } // ignored on purpose
    }

    // info->~SharedInfo(); // DO NOT Call destructor
    m_file.close();
    m_file_map.unmap();
    m_reader_map.unmap();
    try {
        File::remove(m_file_path.c_str());
    }
    catch (...) { } // ignored on purpose
}


bool SharedGroup::has_changed() const TIGHTDB_NOEXCEPT
{
    const SharedInfo* info = m_file_map.get_addr();
    // this variable is changed under lock (the readmutex), but
    // inspected here without taking a lock. This is intentional.
    // The variable is only compared for inequality against a value
    // it is known to have had once, let's call it INIT. 
    // Any change to info->current_version, even if it is
    // only a partially communicated one, is indicative of a change
    // in the value away from INIT. info->current_version is only
    // ever incremented, and it is so large (64 bit) that it does
    // not wrap around until hell freezes over. The net result is
    // that even though there is formally a data race on this variable,
    // the code can still be considered correct.
    bool changed = m_version != info->current_version.load_relaxed();
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
    TIGHTDB_ASSERT(info->current_version.load_relaxed() == 0 || info->current_version.load_relaxed() == 1);

    // We always want to keep a read lock on the last version
    // that was commited to disk, to protect it against being
    // overwritten by commits being made to memory by others.
    // Note that taking this lock also signals to the other
    // processes that that they can start commiting to the db.
    begin_read();
    uint64_t last_version = m_version;
    info->free_write_slots = MAX_WRITE_SLOTS;
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
#ifdef TIGHTDB_DEBUG
            m_transact_stage = transact_Ready;
#endif
            begin_read();
#ifdef TIGHTDB_ENABLE_LOGFILE
            cerr << "(version " << m_version << " from " 
                 << last_version << "), ringbuf_size = " 
                 << ringbuf_size() << "...";
#endif
            uint64_t current_version = m_version;
            size_t current_top_ref = m_group.m_top.get_ref();

            GroupWriter writer(m_group);
            writer.commit(current_top_ref);

            // Now we can release the version that was previously commited
            // to disk and just keep the lock on the latest version.
            m_version = last_version;
            end_read();
            last_version = current_version;
#ifdef TIGHTDB_ENABLE_LOGFILE
            cerr << "..and Done" << endl;
#endif
        }

        if (shutdown) {
            // Being the backend process, we own the lock file, so we
            // have to clean up when we shut down.
            // info->~SharedInfo(); // DO NOT Call destructor
            m_file_map.unmap();
#ifdef TIGHTDB_ENABLE_LOGFILE
            cerr << "Removing coordination file" << endl;
#endif
            if (!file_already_removed)
                File::remove(m_file_path);
#ifdef TIGHTDB_ENABLE_LOGFILE
            cerr << "Daemon exiting nicely" << endl << endl;
#endif
            return;
        }

        info->balancemutex.lock(&recover_from_dead_write_transact);

        // We have caught up with the writers, let them know that there are
        // now free write slots, wakeup any that has been suspended.
        uint16_t free_write_slots = info->free_write_slots;
        info->free_write_slots = MAX_WRITE_SLOTS;
        if (free_write_slots <= 0) {
            info->room_to_write.notify_all();
        }

        // If we have plenty of write slots available, relax and wait a bit before syncing
        if (free_write_slots > RELAXED_SYNC_THRESHOLD) {
            timespec ts;
            timeval tv;
            // clock_gettime(CLOCK_REALTIME, &ts); <- would like to use this, but not there on mac
            gettimeofday(&tv, NULL);
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

const Group& SharedGroup::begin_read()
{
    TIGHTDB_ASSERT(m_transact_stage == transact_Ready);

    ref_type new_top_ref = 0;
    size_t new_file_size = 0;

    {
        SharedInfo* info = m_file_map.get_addr();
        Mutex::Lock lock(info->readmutex);

        if (TIGHTDB_UNLIKELY(info->infosize > m_reader_map.get_size()))
            m_reader_map.remap(m_file, File::access_ReadWrite, info->infosize); // Throws

        // Get the current top ref
        new_top_ref   = to_size_t(info->current_top);
        new_file_size = to_size_t(info->filesize);
        m_version     = info->current_version.load_relaxed();

        // Update reader list
        if (ringbuf_is_empty()) {
            ReadCount r2 = { m_version, 1 };
            ringbuf_put(r2); // Throws
        }
        else {
            ReadCount& r = ringbuf_get_last();
            if (r.version == m_version) {
                ++r.count;
            }
            else {
                ReadCount r2 = { m_version, 1 };
                ringbuf_put(r2); // Throws
            }
        }
    }

#ifdef TIGHTDB_DEBUG
    m_transact_stage = transact_Reading;
#endif

    // Make sure the group is up-to-date.
    // A zero ref means that the file has just been created.
    try {
        m_group.update_from_shared(new_top_ref, new_file_size); // Throws
    }
    catch (...) {
        end_read();
        throw;
    }

#ifdef TIGHTDB_DEBUG
    m_group.Verify();
#endif

    return m_group;
}


void SharedGroup::end_read() TIGHTDB_NOEXCEPT
{
    if (!m_group.is_attached())
        return;

    TIGHTDB_ASSERT(m_transact_stage == transact_Reading);
    TIGHTDB_ASSERT(m_version != numeric_limits<size_t>::max());

    {
        SharedInfo* info = m_file_map.get_addr();
        Mutex::Lock lock(info->readmutex);

        if (TIGHTDB_UNLIKELY(info->infosize > m_reader_map.get_size()))
            m_reader_map.remap(m_file, File::access_ReadWrite, info->infosize);

        // Find entry for current version
        size_t ndx = ringbuf_find(m_version);
        TIGHTDB_ASSERT(ndx != not_found);
        ReadCount& r = ringbuf_get(ndx);

        // Decrement count and remove as many entries as possible
        if (r.count == 1 && ringbuf_is_first(ndx)) {
            ringbuf_remove_first();
            while (!ringbuf_is_empty() && ringbuf_get_first().count == 0)
                ringbuf_remove_first();
        }
        else {
            TIGHTDB_ASSERT(r.count > 0);
            --r.count;
        }
    }

    // The read may have allocated some temporary state
    m_group.detach();

#ifdef TIGHTDB_DEBUG
    m_transact_stage = transact_Ready;
#endif
}


Group& SharedGroup::begin_write()
{
    TIGHTDB_ASSERT(m_transact_stage == transact_Ready);

    SharedInfo* info = m_file_map.get_addr();

    // Get write lock
    // Note that this will not get released until we call
    // commit() or rollback()
    info->writemutex.lock(&recover_from_dead_write_transact); // Throws

#ifndef _WIN32
    if (info->flags == durability_Async) {

        info->balancemutex.lock(&recover_from_dead_write_transact); // Throws

        // if we are running low on write slots, kick the sync daemon
        if (info->free_write_slots < RELAXED_SYNC_THRESHOLD)
            info->work_to_do.notify();

        // if we are out of write slots, wait for the sync daemon to catch up
        while (info->free_write_slots <= 0) {
            info->room_to_write.wait(info->balancemutex,
                                     recover_from_dead_write_transact);
        }

        info->free_write_slots--;
        info->balancemutex.unlock();
    }
#endif

    // Get the current top ref
    ref_type new_top_ref = to_size_t(info->current_top);
    size_t new_file_size = to_size_t(info->filesize);

    // Make sure the group is up-to-date
    // zero ref means that the file has just been created
    m_group.update_from_shared(new_top_ref, new_file_size); // Throws

#ifdef TIGHTDB_DEBUG
    m_group.Verify();
    m_transact_stage = transact_Writing;
#endif

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = m_group.get_replication()) {
        try {
            repl->begin_write_transact(*this); // Throws
        }
        catch (...) {
            rollback();
            throw;
        }
    }
#endif

    return m_group;
}


void SharedGroup::commit()
{
    TIGHTDB_ASSERT(m_transact_stage == transact_Writing);

    SharedInfo* info = m_file_map.get_addr();

    // FIXME: ExceptionSafety: Corruption has happened if
    // low_level_commit() throws, because we have already told the
    // replication manager to commit. It is not yet clear how this
    // conflict should be solved. The solution is probably to catch
    // the exception from low_level_commit() and when caught, mark the
    // local database as not-up-to-date. The exception should not be
    // rethrown, because the commit was effectively successful.

    {
        uint64_t new_version;
#ifdef TIGHTDB_ENABLE_REPLICATION
        // It is essential that if Replicatin::commit_write_transact()
        // fails, then the transaction is not completed. A subsequent call
        // to rollback() must roll it back.
        if (Replication* repl = m_group.get_replication()) {
            new_version = repl->commit_write_transact(*this, info->current_version); // Throws
        }
        else {
            new_version = info->current_version.load_relaxed() + 1; 
        }
#else
        new_version = info->current_version.load_relaxed() + 1; 
#endif
        // Reset version tracking in group if we are
        // starting from a new lock file
        if (new_version == 2) {
            // The reason this is not done in begin_write is that a rollback will
            // leave the versioning unchanged, hence a new begin_write following
            // a rollback would call init_shared again.
            m_group.init_shared();
        }

        low_level_commit(new_version); // Throws
    }

    // Release write lock
    info->writemutex.unlock();

    m_group.detach();

#ifdef TIGHTDB_DEBUG
    m_transact_stage = transact_Ready;
#endif
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

        SharedInfo* info = m_file_map.get_addr();

        // Release write lock
        info->writemutex.unlock();

        // Clear all changes made during transaction
        m_group.detach();

#ifdef TIGHTDB_DEBUG
        m_transact_stage = transact_Ready;
#endif
    }
}


bool SharedGroup::ringbuf_is_empty() const TIGHTDB_NOEXCEPT
{
    return ringbuf_size() == 0;
}


size_t SharedGroup::ringbuf_size() const TIGHTDB_NOEXCEPT
{
    SharedInfo* info = m_reader_map.get_addr();
    return (info->put_pos - info->get_pos) & info->capacity_mask;
}


size_t SharedGroup::ringbuf_capacity() const TIGHTDB_NOEXCEPT
{
    SharedInfo* info = m_reader_map.get_addr();
    return info->capacity_mask;
}


bool SharedGroup::ringbuf_is_first(size_t ndx) const TIGHTDB_NOEXCEPT
{
    SharedInfo* info = m_reader_map.get_addr();
    return ndx == info->get_pos;
}


SharedGroup::ReadCount& SharedGroup::ringbuf_get(size_t ndx) TIGHTDB_NOEXCEPT
{
    SharedInfo* info = m_reader_map.get_addr();
    return info->readers[ndx];
}


SharedGroup::ReadCount& SharedGroup::ringbuf_get_first() TIGHTDB_NOEXCEPT
{
    SharedInfo* info = m_reader_map.get_addr();
    return info->readers[info->get_pos];
}


SharedGroup::ReadCount& SharedGroup::ringbuf_get_last() TIGHTDB_NOEXCEPT
{
    SharedInfo* info = m_reader_map.get_addr();
    uint32_t lastPos = (info->put_pos - 1) & info->capacity_mask;
    return info->readers[lastPos];
}


void SharedGroup::ringbuf_remove_first() TIGHTDB_NOEXCEPT
{
    SharedInfo* info = m_reader_map.get_addr();
    info->get_pos = (info->get_pos + 1) & info->capacity_mask;
}


void SharedGroup::ringbuf_put(const ReadCount& v)
{
    SharedInfo* info = m_reader_map.get_addr();

    // Check if the ringbuf is full
    // (there should always be one empty entry)
    size_t size = ringbuf_size();
    bool is_full = size == info->capacity_mask;

    if (TIGHTDB_UNLIKELY(is_full)) {
        ringbuf_expand(); // Throws
        info = m_reader_map.get_addr();
    }

    info->readers[info->put_pos] = v;
    info->put_pos = (info->put_pos + 1) & info->capacity_mask;
}


void SharedGroup::ringbuf_expand()
{
    SharedInfo* info = m_reader_map.get_addr();
    size_t old_buffer_size = info->capacity_mask + 1; // FIXME: Why size_t and not uint32 as capacity?
    size_t base_file_size = sizeof (SharedInfo) - sizeof (ReadCount[SharedInfo::init_readers_size]);

    // Be sure that the new file size, after doubling the size of the
    // ring buffer, can be stored in a size_t. This also guarantees
    // that there is no arithmetic overflow in the calculation of the
    // new file size. We must always double the size of the ring
    // buffer, such that we can continue to use the capacity as a bit
    // mask. Note that the capacity of the ring buffer is one less
    // than the size of the containing linear buffer.
    //
    // FIXME: It is no good that we convert back and forth between
    // uint32_t and size_t, because that defeats the purpose of this
    // check.
    if (old_buffer_size > (numeric_limits<size_t>::max() -
                           base_file_size) / (2 * sizeof (ReadCount)))
        throw runtime_error("Overflow in size of 'readers' buffer");
    size_t new_buffer_size = 2 * old_buffer_size;
    size_t new_file_size = base_file_size + (sizeof (ReadCount) * new_buffer_size);

    // Extend lock file
    m_file.prealloc(0, new_file_size); // Throws
    m_reader_map.remap(m_file, File::access_ReadWrite, new_file_size); // Throws
    info = m_reader_map.get_addr();

    // If the contents of the ring buffer crosses the end of the
    // containing linear buffer (continuing at the beginning) then the
    // section whose end coincided with the old end of the linear
    // buffer, must be moved forward such that its end becomes
    // coincident with the end of the expanded linear buffer.
    if (info->put_pos < info->get_pos) {
        // Since we always double the size of the linear buffer, there
        // is never any risk of aliasing/clobbering during copying.
        ReadCount* begin = info->readers + info->get_pos;
        ReadCount* end   = info->readers + old_buffer_size;
        ReadCount* new_begin = begin + old_buffer_size;
        copy(begin, end, new_begin);
        info->get_pos += uint32_t(old_buffer_size);
    }

    info->infosize = uint32_t(new_file_size); // notify other processes of expansion
    info->capacity_mask = uint32_t(new_buffer_size) - 1;
}


size_t SharedGroup::ringbuf_find(uint64_t version) const TIGHTDB_NOEXCEPT
{
    const SharedInfo* info = m_reader_map.get_addr();
    uint32_t pos = info->get_pos;
    while (pos != info->put_pos) {
        const ReadCount& r = info->readers[pos];
        if (r.version == version)
            return pos;

        pos = (pos + 1) & info->capacity_mask;
    }

    return not_found;
}


#ifdef TIGHTDB_DEBUG

void SharedGroup::test_ringbuf()
{
    TIGHTDB_ASSERT(ringbuf_is_empty());

    ReadCount rc = { 1, 1 };
    ringbuf_put(rc);
    TIGHTDB_ASSERT(ringbuf_size() == 1);

    ringbuf_remove_first();
    TIGHTDB_ASSERT(ringbuf_is_empty());

    // Fill buffer (within capacity)
    size_t capacity = ringbuf_capacity();
    for (size_t i = 0; i < capacity; ++i) {
        ReadCount r = { 1, uint32_t(i) };
        ringbuf_put(r);
        TIGHTDB_ASSERT(ringbuf_get_last().count == i);
    }
    TIGHTDB_ASSERT(ringbuf_size() == capacity);
    for (size_t i = 0; i < capacity; ++i) {
        const ReadCount& r = ringbuf_get_first();
        TIGHTDB_ASSERT(r.count == i);

        ringbuf_remove_first();
    }
    TIGHTDB_ASSERT(ringbuf_is_empty());

    // Fill buffer and force split
    for (size_t i = 0; i < capacity; ++i) {
        ReadCount r = { 1, uint32_t(i) };
        ringbuf_put(r);
        TIGHTDB_ASSERT(ringbuf_get_last().count == i);
    }
    for (size_t i = 0; i < capacity/2; ++i) {
        const ReadCount& r = ringbuf_get_first();
        TIGHTDB_ASSERT(r.count == i);

        ringbuf_remove_first();
    }
    for (size_t i = 0; i < capacity/2; ++i) {
        ReadCount r = { 1, uint32_t(i) };
        ringbuf_put(r);
    }
    for (size_t i = 0; i < capacity; ++i) {
        ringbuf_remove_first();
    }
    TIGHTDB_ASSERT(ringbuf_is_empty());

    // Fill buffer above capacity (forcing it to expand)
    size_t capacity_plus = ringbuf_capacity() + (1+16);
    for (size_t i = 0; i < capacity_plus; ++i) {
        ReadCount r = { 1, uint32_t(i) };
        ringbuf_put(r);
        TIGHTDB_ASSERT(ringbuf_get_last().count == i);
    }
    for (size_t i = 0; i < capacity_plus; ++i) {
        const ReadCount& r = ringbuf_get_first();
        TIGHTDB_ASSERT(r.count == i);
        ringbuf_remove_first();
    }
    TIGHTDB_ASSERT(ringbuf_is_empty());

    // Fill buffer above capacity again (forcing it to expand with overlap)
    capacity_plus = ringbuf_capacity() + (1+16);
    for (size_t i = 0; i < capacity_plus; ++i) {
        ReadCount r = { 1, uint32_t(i) };
        ringbuf_put(r);
        TIGHTDB_ASSERT(ringbuf_get_last().count == i);
    }
    for (size_t i = 0; i < capacity_plus; ++i) {
        const ReadCount& r = ringbuf_get_first();
        TIGHTDB_ASSERT(r.count == i);

        ringbuf_remove_first();
    }
    TIGHTDB_ASSERT(ringbuf_is_empty());
}


void SharedGroup::zero_free_space()
{
    SharedInfo* info = m_file_map.get_addr();

    // Get version info
    uint64_t current_version;
    size_t readlock_version;
    size_t file_size;

    {
        Mutex::Lock lock(info->readmutex);
        current_version = info->current_version.load_relaxed() + 1;
        file_size = to_size_t(info->filesize);

        if (ringbuf_is_empty()) {
            readlock_version = current_version;
        }
        else {
            const ReadCount& r = ringbuf_get_first();
            readlock_version = r.version;
        }
    }

    m_group.zero_free_space(file_size, readlock_version);
}

#endif // TIGHTDB_DEBUG


uint64_t SharedGroup::get_current_version() TIGHTDB_NOEXCEPT
{
    SharedInfo* info = m_file_map.get_addr();
    return info->current_version.load_relaxed();
}

void SharedGroup::low_level_commit(uint64_t new_version)
{
    SharedInfo* info = m_file_map.get_addr();
    uint64_t readlock_version;
    {
        Mutex::Lock lock(info->readmutex);

        if (TIGHTDB_UNLIKELY(info->infosize > m_reader_map.get_size()))
            m_reader_map.remap(m_file, File::access_ReadWrite, info->infosize); // Throws

        if (ringbuf_is_empty()) {
            readlock_version = new_version;
        }
        else {
            const ReadCount& r = ringbuf_get_first();
            readlock_version = r.version;
        }
    }

    // Do the actual commit
    TIGHTDB_ASSERT(m_group.m_top.is_attached());
    TIGHTDB_ASSERT(readlock_version <= new_version);
    GroupWriter out(m_group); // Throws
    m_group.m_readlock_version = readlock_version;
    out.set_versions(new_version, readlock_version);
    // Recursively write all changed arrays to end of file
    ref_type new_top_ref = out.write_group(); // Throws
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
        Mutex::Lock lock(info->readmutex);
        info->current_top = new_top_ref;
        info->filesize    = new_file_size;
        info->current_version.store_relaxed(new_version);
    }

    // Save last version for has_changed()
    m_version = new_version;
}
