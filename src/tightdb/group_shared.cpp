#include <fcntl.h>
#include <errno.h>
#include <sys/wait.h>
#include <unistd.h>
#include <algorithm>

#include <tightdb/safe_int_ops.hpp>
#include <tightdb/terminate.hpp>
#include <tightdb/thread.hpp>
#include <tightdb/group_writer.hpp>
#include <tightdb/group_shared.hpp>
#include <tightdb/group_writer.hpp>


using namespace std;
using namespace tightdb;


struct SharedGroup::ReadCount {
    uint32_t version;
    uint32_t count;
};

struct SharedGroup::SharedInfo {
    Atomic<uint16_t> init_complete; // indicates lock file has valid content
    uint16_t shutdown_started; // indicates that shutdown is in progress
    uint16_t version;
    uint16_t flags;

    Mutex readmutex;
    RobustMutex writemutex;
    uint64_t filesize;

    uint64_t current_top;
    Atomic<uint32_t> current_version;

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
    readmutex(Mutex::process_shared_tag()), // Throws
    writemutex() // Throws
{
    version  = 0;
    flags    = dlevel; // durability level is fixed from creation
    filesize = alloc.get_baseline();
    infosize = uint32_t(file_size);
    current_top     = alloc.get_top_ref();
    current_version.store(1);
    capacity_mask   = init_readers_size - 1;
    put_pos = 0;
    get_pos = 0;
    shutdown_started = 0;
    init_complete.store(1);
}


namespace {

void recover_from_dead_write_transact()
{
    // Nothing needs to be done
}

} // anonymous namespace


void spawn_daemon(const string& file)
{
    // determine maximum number of open descriptors
    errno = 0; 
    int m = sysconf(_SC_OPEN_MAX); 
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
        // FIXME: Do we want to always open the log file? Should it be configurable?
        i=::open((file+".log").c_str(),O_RDWR | O_CREAT | O_APPEND | O_SYNC, S_IRWXU);
        i = dup(i); static_cast<void>(i);
        cerr << "Detaching" << endl;
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
        exit(1);
        // child process ends here

    } else if (pid > 0) { // parent process, fork succeeded:
        
        // use childs exit code to catch and report any errors:
        int status;
        int pid_changed = waitpid(pid, &status, 0);
        if (pid_changed != pid)
            throw runtime_error("failed to wait for daemon start");
        if (!WIFEXITED(status))
            throw runtime_error("failed starting async commit (exit)");
        if (WEXITSTATUS(status) == 1)
            throw runtime_error("async commit daemon not found");
        if (WEXITSTATUS(status) == 2)
            throw runtime_error("async commit daemon failed");
        if (WEXITSTATUS(status) == 3)
            throw runtime_error("wrong db given to async daemon");

    } else { // Parent process, fork failed!

        throw runtime_error("Failed to spawn async commit");
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

void SharedGroup::open(const string& path, bool no_create_file,
                       DurabilityLevel dlevel, bool is_backend)
{
    TIGHTDB_ASSERT(!is_attached());

    m_file_path = path + ".lock";
    bool must_retry;
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

        } catch (runtime_error e) {

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
        int time_left = 100000;
        while (1) {
            time_left--;
            // need to validate the size of the file before trying to map it
            // possibly waiting a little for size to go nonzero, if another
            // process is creating the file in parallel.
            if (int_cast_with_overflow_detect(m_file.get_size(), file_size))
                throw runtime_error("Lock file too large");
            if (time_left <= 0)
                throw runtime_error("Stale lock file");
            // wait for file to at least contain the basic shared info block
            // NB! it might be larger due to expansion of the ring buffer.
            if (file_size < sizeof(SharedInfo))
                usleep(10);
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
            alloc.attach_file(path, is_shared, read_only, no_create_file); // Throws

            // Call SharedInfo::SharedInfo() (placement new)
            new (info) SharedInfo(alloc, file_size, dlevel); // Throws

            // Set initial version so we can track if other instances
            // change the db
            m_version = info->current_version.load();
            // In async mode we need a separate process to do the async commits
            // We start it up here during init so that it only get started once
            if (dlevel == durability_Async) {
                spawn_daemon(path);
            }
        }
        else {
            // wait for init complete:
            int wait_count = 100000;
            while (wait_count && (info->init_complete.load() == 0)) {
                wait_count--;
                usleep(10);
            }
            if (info->init_complete.load() == 0)
                throw runtime_error("Lock file initialization incomplete");
            if (info->version != 0)
                throw runtime_error("Unsupported version");
            if (info->shutdown_started) {
                must_retry = true;
                usleep(100);
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
            alloc.attach_file(path, is_shared, read_only, no_create); // Throws

        }

        fug_2.release(); // Do not unmap
        fug_1.release(); // Do not unmap
        fcg.release(); // Do not close
    } while (must_retry);

#ifdef TIGHTDB_DEBUG
    m_transact_stage = transact_Ready;
#endif

    if (dlevel == durability_Async) {
        if (is_backend) {
            do_async_commits(); // will never return
        }
        else {
            // In async mode we need to wait for the commit process to get ready
            // so we wait for first begin_read being made by async_commit process
            SharedInfo* const info = m_file_map.get_addr();
            int maxwait = 100000;
            while (maxwait--) {
                if (info->init_complete.load() == 2) {
                    // NOTE: access to info following the return is separeted
                    // from the above check by synchronization primitives. Were
                    // that not the case, a read barrier would be needed here.
                    return;
                }
                // this function call prevents access to init_complete from beeing
                // optimized into a register - if removed, a read barrier is
                // required instead to prevent optimization.
                usleep(10);
            }
            throw runtime_error("Failed to observe async commit starting");
        }
    }
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
    if (info->flags == durability_Async) {
        m_file.unlock();
        return;
    }

    if (!m_file.try_lock_exclusive()) {
        m_file.unlock();
        return;
    }

    if (info->shutdown_started) {
        m_file.unlock();
        return;
    }
    info->shutdown_started = 1;

    // If the db file is just backing for a transient data structure,
    // we can delete it when done.
    if (info->flags == durability_MemOnly) {
        size_t path_len = m_file_path.size()-5; // remove ".lock"
        // FIXME: Find a way to avoid the possible exception from
        // m_file_path.substr(). Currently, if it throws, the program
        // will be terminated due to 'noexcept' on ~SharedGroup().
        string db_path = m_file_path.substr(0, path_len); // Throws
        File::remove(db_path.c_str());
    }

    info->~SharedInfo(); // Call destructor

    File::remove(m_file_path.c_str());
}


bool SharedGroup::has_changed() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_SYNC_IF_NO_CACHE_COHERENCE
    const SharedInfo* info = m_file_map.get_addr();
    // this variable is changed under lock (the readmutex), but
    // inspected here without taking a lock. This is intentional.
    bool changed = m_version != info->current_version.load();
    return changed;
}

void SharedGroup::do_async_commits()
{
    bool shutdown = false;
    SharedInfo* info = m_file_map.get_addr();
    // NO client are allowed to proceed and update current_version
    // until they see the init_complete == 2. 
    // As we haven't set init_complete to 2 yet, it is safe to assert the following:
    TIGHTDB_ASSERT(info->current_version.load() == 0);

    // We always want to keep a read lock on the last version
    // that was commited to disk, to protect it against being
    // overwritten by commits being made to memory by others.
    // Note that taking this lock also signals to the other
    // processes that that they can start commiting to the db.
    begin_read();
    size_t last_version = m_version;
    info->init_complete.store(2); // allow waiting clients to proceed
    m_group.detach();
    while(true) {

        if (m_file.is_removed()) { // operator removed the lock file. take a hint!

            info->shutdown_started = 1;
            // FIXME: barrier?
            shutdown = true;
            cerr << "Lock file removed, initiating shutdown" << endl;
        }

        // detect if we're the last "client", and if so mark the
        // lock file invalid. Any client coming along before we
        // finish syncing will see the lock file, but detect that
        // the daemon has abandoned it. It can then wait for the
        // lock file to be removed (with a timeout). 
        if (m_file.try_lock_exclusive()) {
            info->shutdown_started = 1;
            // FIXME: barrier?
            shutdown = true;
        }
        // if try_lock_exclusive fails, we loose our read lock, so
        // reacquire it! At the moment this is not used for anything,
        // because ONLY the daemon ever asks for an exclusive lock
        // when async commits are used.
        else
            m_file.lock_shared();

        if (has_changed()) {

            cerr << "Syncing...";
            // Get a read lock on the (current) version that we want
            // to commit to disk.
#ifdef TIGHTDB_DEBUG
            m_transact_stage = transact_Ready;
#endif
            begin_read();
            cerr << "(version " << m_version << ")...";
            size_t current_version = m_version;
            size_t current_top_ref = m_group.m_top.get_ref();

            GroupWriter writer(m_group);
            writer.commit(current_top_ref);

            // Now we can release the version that was previously commited
            // to disk and just keep the lock on the latest version.
            m_version = last_version;
            end_read();
            last_version = current_version;
            cerr << "..and Done" << endl;
        }
        else if (!shutdown) {
            usleep(100);
        }

        if (shutdown) {
            // Being the backend process, we own the lock file, so we
            // have to clean up when we shut down.
            info->~SharedInfo(); // Call destructor
            cerr << "Removing coordination file" << endl;
            File::remove(m_file_path);
            cerr << "Daemon exiting nicely";
            exit(EXIT_SUCCESS);
        }
    }
}


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
        m_version     = to_size_t(info->current_version.load()); // fixme, remember to remove to_size_t when m_version becomes 64 bit

        // Update reader list
        if (ringbuf_is_empty()) {
            ReadCount r2 = { info->current_version.load(), 1 };
            ringbuf_put(r2); // Throws
        }
        else {
            ReadCount& r = ringbuf_get_last();
            if (r.version == info->current_version.load()) {
                ++r.count;
            }
            else {
                ReadCount r2 = { info->current_version.load(), 1 };
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

        if (TIGHTDB_UNLIKELY(info->infosize > m_reader_map.get_size())) {
            m_reader_map.remap(m_file, File::access_ReadWrite, info->infosize);
        }


        // FIXME: m_version may well be a 64-bit integer so this cast
        // to uint32_t seems quite dangerous. Should the type of
        // m_version be changed to uint32_t? The problem with uint32_t
        // is that it is not part of C++03. It was introduced in C++11
        // though.

        // Find entry for current version
        size_t ndx = ringbuf_find(uint32_t(m_version));
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
        size_t new_version;
#ifdef TIGHTDB_ENABLE_REPLICATION
        // It is essential that if Replicatin::commit_write_transact()
        // fails, then the transaction is not completed. A subsequent call
        // to rollback() must roll it back.
        if (Replication* repl = m_group.get_replication()) {
            new_version = repl->commit_write_transact(*this); // Throws
        }
        else {
            new_version = info->current_version.load() + 1; // FIXME: Eventual overflow
        }
#else
        new_version = info->current_version.load() + 1; // FIXME: Eventual overflow
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


size_t SharedGroup::ringbuf_find(uint32_t version) const TIGHTDB_NOEXCEPT
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
    size_t current_version;
    size_t readlock_version;
    size_t file_size;

    {
        Mutex::Lock lock(info->readmutex);
        current_version = info->current_version.load() + 1;
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


size_t SharedGroup::get_current_version() TIGHTDB_NOEXCEPT
{
    SharedInfo* info = m_file_map.get_addr();
    return info->current_version.load();
}


// FIXME: What type is the right type for storing the database
// version? Somtimes we use size_t, other times we use uint32_t?
// uint32_t is manifestly a bad choice, since it doesn't always exist,
// and even if it exists, it may not be efficient on the target
// platform. If 32 bits are are enough to hold a database version,
// then the type should be 'unsigned long', 'uint_fast32_t', or
// 'uint_least32_t'.
void SharedGroup::low_level_commit(size_t new_version)
{
    SharedInfo* info = m_file_map.get_addr();
    size_t readlock_version;
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
        info->current_version.store(new_version);//FIXME src\tightdb\group_shared.cpp(772): warning C4267: '=' : conversion from 'size_t' to 'volatile uint32_t', possible loss of data
    }

    // Save last version for has_changed()
    m_version = new_version;
}
