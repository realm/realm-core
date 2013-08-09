#include <pthread.h>
#include <fcntl.h>

#include <tightdb/terminate.hpp>
#include <tightdb/safe_int_ops.hpp>
#include <tightdb/string_buffer.hpp>
#include <tightdb/group_shared.hpp>
#include <tightdb/group_writer.hpp>

// FIXME: We should not include files from the test directory here. A
// solution would probably be to move the definition of
// TIGHTDB_PTHREADS_TEST to <config.h> and move <pthread_test.hpp>
// into the "src/tightdb" directory.

// Wrap pthread function calls with the pthread bug finding tool (program execution will be slower).
// Works both in debug and release mode. Define the flag in testsettings.h
#include "../test/testsettings.hpp"
#ifdef TIGHTDB_PTHREADS_TEST
#  include "../test/pthread_test.hpp"
#endif

using namespace std;
using namespace tightdb;

struct SharedGroup::ReadCount {
    uint32_t version;
    uint32_t count;
};

struct SharedGroup::SharedInfo {
    uint16_t version;
    uint16_t flags;

    pthread_mutex_t readmutex;
    pthread_mutex_t writemutex;
    uint64_t filesize;

    uint64_t current_top;
    volatile uint32_t current_version;

    uint32_t infosize;
    uint32_t capacity; // -1 so it can also be used as mask
    uint32_t put_pos;
    uint32_t get_pos;
    ReadCount readers[32]; // has to be power of two
};

namespace {

class ScopedMutexLock {
public:
    ScopedMutexLock(pthread_mutex_t* mutex) TIGHTDB_NOEXCEPT : m_mutex(mutex)
    {
        int r = pthread_mutex_lock(m_mutex);
        TIGHTDB_ASSERT(r == 0);
        static_cast<void>(r);
    }

    ~ScopedMutexLock() TIGHTDB_NOEXCEPT
    {
        const int r = pthread_mutex_unlock(m_mutex);
        TIGHTDB_ASSERT(r == 0);
        static_cast<void>(r);
    }

private:
    pthread_mutex_t* m_mutex;
};

} // anonymous namespace


// NOTES ON CREATION AND DESTRUCTION OF SHARED MUTEXES:
//
// According to the 'process sharing example' in the POSIX man page
// for pthread_mutexattr_init() other processes may continue to use a
// shared mutex after exit of the process that initialized it. Also,
// the example does not contain any call to pthread_mutex_destroy(),
// so apparently a shared mutex need not be destroyed at all, nor can
// it be that a shared mutex is associated with any resources that are
// local to the initializing process.
//
// While it is not explicitely stated in the man page, we shall also
// assume that is is valid to initialize a shared mutex twice without
// an intervending call to pthread_mutex_destroy(). We need to be able
// to reinitialize a shared mutex if the first initializing process
// craches and leaves the shared memory in an undefined state.

// FIXME: Issues with current implementation:
//
// - Possible reinitialization due to temporary unlocking during downgrade of file lock

void SharedGroup::open(const string& file, bool no_create_file,
                       DurabilityLevel dlevel, bool is_backend)
{
    TIGHTDB_ASSERT(!is_attached());

    m_file_path = file + ".lock";

retry:
    {
        // Open shared coordination buffer
        m_file.open(m_file_path, File::access_ReadWrite, File::create_Auto, 0);
        File::CloseGuard fcg(m_file);

        // FIXME: Handle lock file removal in case of failure below

        bool need_init = false;
        size_t len     = 0;

        // If we can get an exclusive lock we know that the file is
        // either new (empty) or a leftover from a previously
        // crashed process (needing re-initialization)
        if (m_file.try_lock_exclusive()) {
            // There is a slight window between opening the file and getting the
            // lock where another process could have deleted the file
            if (m_file.is_removed()) {
                goto retry;
            }
            // Get size
            if (int_cast_with_overflow_detect(m_file.get_size(), len))
                throw runtime_error("Lock file too large");

            // Handle empty files (first user)
            if (len == 0) {
                // Create new file
                len = sizeof (SharedInfo);
                m_file.resize(len);
            }
            need_init = true;
        }
        else {
            m_file.lock_shared();

            // Get size
            if (int_cast_with_overflow_detect(m_file.get_size(), len))
                throw runtime_error("Lock file too large");

            // There is a slight window between opening the file and getting the
            // lock where another process could have deleted the file
            if (len == 0 || m_file.is_removed()) {
                goto retry;
            }
        }

        // Map to memory
        m_file_map.map(m_file, File::access_ReadWrite, sizeof (SharedInfo), File::map_NoSync);
        File::UnmapGuard fug(m_file_map);

        SharedInfo* const info = m_file_map.get_addr();

        if (need_init) {
            // If we are the first we may have to create the database file
            // but we invalidate the internals right after to avoid conflicting
            // with old state when starting transactions
            const Group::OpenMode group_open_mode =
                no_create_file ? Group::mode_ReadWriteNoCreate : Group::mode_ReadWrite;
            m_group.create_from_file(file, group_open_mode, true);
            m_group.invalidate();

            // Initialize mutexes so that they can be shared between processes
            pthread_mutexattr_t mattr;
            pthread_mutexattr_init(&mattr);
            // FIXME: Must verify availability of optional feature: #ifdef _POSIX_THREAD_PROCESS_SHARED
            pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);
            // FIXME: Should also do pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST). Check for availability with: #if _POSIX_THREADS >= 200809L
            pthread_mutex_init(&info->readmutex, &mattr);
            pthread_mutex_init(&info->writemutex, &mattr);
            pthread_mutexattr_destroy(&mattr);

            SlabAlloc& alloc = m_group.get_allocator();

            // Set initial values
            info->version  = 0;
            info->flags    = dlevel; // durability level is fixed from creation
            info->filesize = alloc.get_base_size();
            info->infosize = uint32_t(len);
            info->current_top = alloc.get_top_ref();
            info->current_version = 0;
            info->capacity = 32-1;
            info->put_pos  = 0;
            info->get_pos  = 0;

            // Set initial version so we can track if other instances
            // change the db
            m_version = 0;

            // In async mode we need a separate process to do the async commits
            // We start it up here during init so that it only get started once
            if (dlevel == durability_Async) {

                if (fork() == 0) {
                    /* close all descriptors: */
                    int i,k;
                    for (i=getdtablesize();i>=0;--i) close(i); 
                    i=::open("/dev/null",O_RDWR); 
                    dup(i); dup(i);
                    execl("/usr/local/bin/tightdbd", 
                          "/usr/local/bin/tightdbd", 
                          file.c_str(), (char*) NULL);
                    printf("ERROR: Failed to start tightdb async commit daemon\n");
                    exit(1);
                    // FIXME: undetectable if daemon dies/fails to start
                }

            }

            // FIXME: This downgrading of the lock is not guaranteed to be atomic

            // Downgrade lock to shared now that it is initialized,
            // so other processes can share it as well
            m_file.unlock();
            m_file.lock_shared();
        }
        else {
            if (info->version != 0)
                throw runtime_error("Unsupported version");

            // Durability level cannot be changed at runtime
            if (info->flags != dlevel)
                throw runtime_error("Inconsistent durability level");

            // Setup the group, but leave it in invalid state
            m_group.create_from_file(file, Group::mode_ReadWriteNoCreate, false);
        }

        // We need to map the info file once more for the readers part
        // since that part can be resized and as such remapped which
        // could move our mutexes (which we don't want to risk moving while
        // they are locked)
        m_reader_map.map(m_file, File::access_ReadWrite, sizeof (SharedInfo), File::map_NoSync);

        fug.release(); // Do not unmap
        fcg.release(); // Do not close
    }

#ifdef TIGHTDB_DEBUG
    m_transact_stage = transact_Ready;
#endif

    if (dlevel == durability_Async) {
        if (is_backend) {
            do_async_commits(); // will never return
        }
        else {
            // In async mode we need to wait for the commit process to get ready
            // so we wait for first read lock being made by async_commit process
            SharedInfo* const info = m_file_map.get_addr();
            while (info->put_pos == 0) usleep(2);
        }
    }
}


SharedGroup::~SharedGroup()
{
    if (!is_attached()) return;

    TIGHTDB_ASSERT(m_transact_stage == transact_Ready);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = m_group.get_replication())
        delete repl;
#endif

    // If we can get an exclusive lock on the file we know that we are
    // the only user (since all users take at least shared locks on
    // the file.  So that means that we have to delete it when done
    // (to avoid someone later opening a stale file with uinitialized
    // mutexes)

    // FIXME: This upgrading of the lock is not guaranteed to be atomic
    m_file.unlock();
    if (!m_file.try_lock_exclusive()) return;

    SharedInfo* info = m_file_map.get_addr();

    // In sync mode, cleanup will be handled by the async_commit process
    // (but we might still be able to get exclusive lock, as it will
    //  release it while doing its own try_lock_exclusive())
    if (info->flags == durability_Async) return;

    // If the db file is just backing for a transient data structure,
    // we can delete it when done.
    if (info->flags == durability_MemOnly) {
        const size_t path_len = m_file_path.size()-5; // remove ".lock"
        const string db_path = m_file_path.substr(0, path_len);
        remove(db_path.c_str());
    }

    pthread_mutex_destroy(&info->readmutex);
    pthread_mutex_destroy(&info->writemutex);

    remove(m_file_path.c_str());
}


bool SharedGroup::has_changed() const TIGHTDB_NOEXCEPT
{
    // FIXME: Due to lack of adequate synchronization, the following
    // read of 'info->current_version' effectively participates in a
    // "data race". See also SharedGroup::low_level_commit(). First of
    // all, portable C++ code must always operate under the assumption
    // that a data race is an error. The point is that the memory
    // model defined by, and assumed by the C++ standard does not
    // allow for data races at all. The purpose is to give harware
    // designers more freedom to optimize their hardware. This also
    // means that even if your 'data race' works for you today, it may
    // malfunction and cause havoc on the next revision of that
    // platform. According to Hans Boehm (one of the major
    // contributors to the design of the C++ memory model) we should
    // think of a data race as something so grave that it could cause
    // you computer to burst into flames (see
    // http://channel9.msdn.com/Events/GoingNative/GoingNative-2012/Threads-and-Shared-Variables-in-C-11)
    // On top of that, if a customer chooses to run a data race
    // detector on their application, our data race migh show up, and
    // that may rightfully cause unwanted alarm.

    // Have we changed since last transaction?
    // Visibility of changes can be delayed when using has_changed() because m_info->current_version is tested
    // outside mutexes. However, the delay is finite on architectures that have hardware cache coherency (ARM, x64, x86,
    // POWER, UltraSPARC, etc) because it guarantees write propagation (writes to m_info->current_version occur on
    // system bus and make cache controllers invalidate caches of reader). Some excotic architectures may need
    // explicit synchronization which isn't implemented yet.
    TIGHTDB_SYNC_IF_NO_CACHE_COHERENCE
    const SharedInfo* info = m_file_map.get_addr();
    bool is_changed = (m_version != info->current_version);
    return is_changed;
}

void SharedGroup::do_async_commits()
{
    bool shutdown = false;
    SharedInfo* info = m_file_map.get_addr();
    TIGHTDB_ASSERT(info->current_version == 0);

    // We always want to keep a read lock on the last version
    // that was commited to disk, to protect it against being
    // overwritten by commits being made to memory by others.
    // Note that taking this lock also signals to the other
    // processes that that they can start commiting to the db.
    begin_read();
    size_t last_version = m_version;
    m_group.invalidate();
    while(true) {
        // If we can get an exclusive lock, we know that we are
        // the last process using the db so we can close down
        // (all other processes using the db holds shared locks)
        if (m_file.try_lock_exclusive()) {
            shutdown = true;
        }

        if (has_changed()) {
            // Get a read lock on the (current) version that we want
            // to commit to disk.
#ifdef TIGHTDB_DEBUG
            m_transact_stage = transact_Ready;
#endif
            begin_read();
            size_t current_version = m_version;
            size_t current_top_ref = m_group.get_top_array().get_ref();

            GroupWriter writer(m_group, true);
            writer.DoCommit(current_top_ref);

            // Now we can release the version that was previously commited
            // to disk and just keep the lock on the latest version.
            m_version = last_version;
            end_read();
            last_version = current_version;
        }
        else if (!shutdown) {
            usleep(20);
        }

        if (shutdown) {
            // Being the backend process, we own the lock file, so we
            // have to clean up when we shut down.
            pthread_mutex_destroy(&info->readmutex);
            pthread_mutex_destroy(&info->writemutex);
            remove(m_file_path.c_str());
            exit(EXIT_SUCCESS);
        }
    }
}


const Group& SharedGroup::begin_read()
{
    TIGHTDB_ASSERT(m_transact_stage == transact_Ready);
    TIGHTDB_ASSERT(m_group.get_allocator().is_all_free());

    size_t new_topref = 0;
    size_t new_filesize = 0;

    {
        SharedInfo* info = m_file_map.get_addr();
        ScopedMutexLock lock(&info->readmutex);

        if (TIGHTDB_UNLIKELY(info->infosize > m_reader_map.get_size())) {
            m_reader_map.remap(m_file, File::access_ReadWrite, info->infosize);
        }

        // Get the current top ref
        new_topref   = to_size_t(info->current_top);
        new_filesize = to_size_t(info->filesize);
        m_version    = to_size_t(info->current_version); // fixme, remember to remove to_size_t when m_version becomes 64 bit

        // Update reader list
        if (ringbuf_is_empty()) {
            ReadCount r2 = {info->current_version, 1};
            ringbuf_put(r2);
        }
        else {
            ReadCount& r = ringbuf_get_last();
            if (r.version == info->current_version)
                ++(r.count);
            else {
                ReadCount r2 = {info->current_version, 1};
                ringbuf_put(r2);
            }
        }
    }

    // Make sure the group is up-to-date
    // zero ref means that the file has just been created
    m_group.update_from_shared(new_topref, new_filesize);

#ifdef TIGHTDB_DEBUG
    m_group.Verify();
    m_transact_stage = transact_Reading;
#endif

    return m_group;
}


void SharedGroup::end_read()
{
    TIGHTDB_ASSERT(m_transact_stage == transact_Reading);
    TIGHTDB_ASSERT(m_version != numeric_limits<size_t>::max());

    {
        SharedInfo* info = m_file_map.get_addr();
        ScopedMutexLock lock(&info->readmutex);

        // FIXME: m_version may well be a 64-bit integer so this cast
        // to uint32_t seems quite dangerous. Should the type of
        // m_version be changed to uint32_t? The problem with uint32_t
        // is that it is not part of C++03. It was introduced in C++11
        // though.

        // Find entry for current version
        size_t ndx = ringbuf_find(uint32_t(m_version));
        TIGHTDB_ASSERT(ndx != size_t(-1));
        ReadCount& r = ringbuf_get(ndx);

        // Decrement count and remove as many entries as possible
        if (r.count == 1 && ringbuf_is_first(ndx)) {
            ringbuf_remove_first();
            while (!ringbuf_is_empty() && ringbuf_get_first().count == 0) {
                ringbuf_remove_first();
            }
        }
        else {
            TIGHTDB_ASSERT(r.count > 0);
            --r.count;
        }
    }

    // The read may have allocated some temporary state
    m_group.invalidate();

#ifdef TIGHTDB_DEBUG
    m_transact_stage = transact_Ready;
#endif
}


Group& SharedGroup::begin_write()
{
    TIGHTDB_ASSERT(m_transact_stage == transact_Ready);
    TIGHTDB_ASSERT(m_group.get_allocator().is_all_free());

    SharedInfo* info = m_file_map.get_addr();

    // Get write lock
    // Note that this will not get released until we call
    // end_write().
    pthread_mutex_lock(&info->writemutex);

    // Get the current top ref
    size_t new_topref   = to_size_t(info->current_top);
    size_t new_filesize = to_size_t(info->filesize);

    // Make sure the group is up-to-date
    // zero ref means that the file has just been created
    m_group.update_from_shared(new_topref, new_filesize);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = m_group.get_replication())
        repl->begin_write_transact(*this); // Throws
#endif

#ifdef TIGHTDB_DEBUG
    m_group.Verify();
    m_transact_stage = transact_Writing;
#endif

    return m_group;
}


void SharedGroup::commit()
{
    TIGHTDB_ASSERT(m_transact_stage == transact_Writing);

    SharedInfo* info = m_file_map.get_addr();

    {
        size_t new_version;
#ifdef TIGHTDB_ENABLE_REPLICATION
        // It is essential that if Replicatin::commit_write_transact()
        // fails, then the transaction is not completed. A following call
        // to rollback() must roll it back.
        if (Replication* repl = m_group.get_replication()) {
            new_version = repl->commit_write_transact(*this); // Throws
        }
        else {
            new_version = info->current_version + 1; // FIXME: Eventual overflow
        }
#else
        new_version = info->current_version + 1; // FIXME: Eventual overflow
#endif
        low_level_commit(new_version); // Throws
    }

    // Release write lock
    pthread_mutex_unlock(&info->writemutex);

    m_group.invalidate();

#ifdef TIGHTDB_DEBUG
    m_transact_stage = transact_Ready;
#endif
}


// FIXME: This method must work correctly even if it is called after a
// failed call to commit(). A failed call to commit() is any that
// returns to the caller by throwing an exception. As it is right now,
// rollback() does not handle all cases.
void SharedGroup::rollback()
{
    TIGHTDB_ASSERT(m_transact_stage == transact_Writing);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = m_group.get_replication())
        repl->rollback_write_transact(*this);
#endif

    // Clear all changes made during transaction
    m_group.rollback();

    SharedInfo* const info = m_file_map.get_addr();

    // Release write lock
    pthread_mutex_unlock(&info->writemutex);

    m_group.invalidate();

#ifdef TIGHTDB_DEBUG
    m_transact_stage = transact_Ready;
#endif
}


bool SharedGroup::ringbuf_is_empty() const TIGHTDB_NOEXCEPT
{
    return (ringbuf_size() == 0);
}

size_t SharedGroup::ringbuf_size() const TIGHTDB_NOEXCEPT
{
    SharedInfo* const info = m_reader_map.get_addr();
    return ((info->put_pos - info->get_pos) & info->capacity);
}

size_t SharedGroup::ringbuf_capacity() const TIGHTDB_NOEXCEPT
{
    SharedInfo* const info = m_reader_map.get_addr();
    return info->capacity+1;
}

bool SharedGroup::ringbuf_is_first(size_t ndx) const TIGHTDB_NOEXCEPT
{
    SharedInfo* const info = m_reader_map.get_addr();
    return (ndx == info->get_pos);
}

SharedGroup::ReadCount& SharedGroup::ringbuf_get(size_t ndx) TIGHTDB_NOEXCEPT
{
    SharedInfo* const info = m_reader_map.get_addr();
    return info->readers[ndx];
}

SharedGroup::ReadCount& SharedGroup::ringbuf_get_first() TIGHTDB_NOEXCEPT
{
    SharedInfo* const info = m_reader_map.get_addr();
    return info->readers[info->get_pos];
}

SharedGroup::ReadCount& SharedGroup::ringbuf_get_last() TIGHTDB_NOEXCEPT
{
    SharedInfo* const info = m_reader_map.get_addr();
    const uint32_t lastPos = (info->put_pos - 1) & info->capacity;
    return info->readers[lastPos];
}

void SharedGroup::ringbuf_remove_first() TIGHTDB_NOEXCEPT
{
    SharedInfo* const info = m_reader_map.get_addr();
    info->get_pos = (info->get_pos + 1) & info->capacity;
}

void SharedGroup::ringbuf_put(const ReadCount& v)
{
    SharedInfo* info = m_reader_map.get_addr();

    // Check if the ringbuf is full
    // (there should always be one empty entry)
    const size_t size = ringbuf_size();
    const bool isFull = (size == (info->capacity));

    if (TIGHTDB_UNLIKELY(isFull)) {
        ringbuf_expand();
        info = m_reader_map.get_addr();
    }

    info->readers[info->put_pos] = v;
    info->put_pos = (info->put_pos + 1) & info->capacity;
}

void SharedGroup::ringbuf_expand()
{
    const SharedInfo* const info = m_reader_map.get_addr();

    // Calculate size of file with more entries
    const size_t current_entry_count = info->capacity + 1;  // FIXME: Why size_t and not uint32 as capacity?
    const size_t excount = current_entry_count; // Always double so we can mask for index
    const size_t new_entry_count = current_entry_count + excount;
    const size_t base_filesize = sizeof(SharedInfo) - sizeof(ReadCount[32]);
    const size_t new_filesize = base_filesize + (sizeof(ReadCount) * new_entry_count);

    // Extend file
    m_file.alloc(0, new_filesize);
    m_reader_map.remap(m_file, File::access_ReadWrite, new_filesize);
    SharedInfo* const info2 = m_reader_map.get_addr();

    // Move existing entries (if there is a split)
    if (info2->put_pos < info2->get_pos) {
        const ReadCount* const low_start = &info2->readers[info2->get_pos];
        const ReadCount* const low_end   = &info2->readers[current_entry_count];
        const bool has_overlap = (current_entry_count - info2->get_pos) > excount;

        if (has_overlap) {
            ReadCount* const new_end = &info2->readers[new_entry_count];
            copy_backward(low_start, low_end, new_end);
        }
        else {
            ReadCount* const new_start = &info2->readers[info2->get_pos + excount];
            copy(low_start, low_end, new_start);
        }
        // FIXME: warning for adding size_t to uint32!
        info2->get_pos += excount;
    }

    info2->infosize = uint32_t(new_filesize); // notify other processes of expansion
    info2->capacity = uint32_t(new_entry_count) - 1;
}

size_t SharedGroup::ringbuf_find(uint32_t version) const TIGHTDB_NOEXCEPT
{
    const SharedInfo* const info = m_reader_map.get_addr();
    uint32_t pos = info->get_pos;
    while (pos != info->put_pos) {
        const ReadCount& r = info->readers[pos];
        if (r.version == version)
            return pos;

        pos = (pos + 1) & info->capacity;
    }

    return not_found;
}

#ifdef TIGHTDB_DEBUG

void SharedGroup::test_ringbuf()
{
    TIGHTDB_ASSERT(ringbuf_is_empty());

    const ReadCount rc = {1, 1};
    ringbuf_put(rc);
    TIGHTDB_ASSERT(ringbuf_size() == 1);

    ringbuf_remove_first();
    TIGHTDB_ASSERT(ringbuf_is_empty());

    // Fill buffer (within capacity)
    const size_t capacity = ringbuf_capacity()-1;
    for (size_t i = 0; i < capacity; ++i) {
        const ReadCount r = {1, uint32_t(i)};
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
        const ReadCount r = {1, uint32_t(i)};
        ringbuf_put(r);
        TIGHTDB_ASSERT(ringbuf_get_last().count == i);
    }
    for (size_t i = 0; i < capacity/2; ++i) {
        const ReadCount& r = ringbuf_get_first();
        TIGHTDB_ASSERT(r.count == i);

        ringbuf_remove_first();
    }
    for (size_t i = 0; i < capacity/2; ++i) {
        const ReadCount r = {1, uint32_t(i)};
        ringbuf_put(r);
    }
    for (size_t i = 0; i < capacity; ++i) {
        ringbuf_remove_first();
    }
    TIGHTDB_ASSERT(ringbuf_is_empty());

    // Fill buffer above capacity (forcing it to expand)
    size_t capacity_plus = ringbuf_capacity() + 16;
    for (size_t i = 0; i < capacity_plus; ++i) {
        const ReadCount r = {1, uint32_t(i)};
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
    capacity_plus = ringbuf_capacity() + 16;
    for (size_t i = 0; i < capacity_plus; ++i) {
        const ReadCount r = {1, uint32_t(i)};
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
    pthread_mutex_lock(&info->readmutex);
    {
        current_version = info->current_version + 1;
        file_size = to_size_t(info->filesize);

        if (ringbuf_is_empty())
            readlock_version = current_version;
        else {
            const ReadCount& r = ringbuf_get_first();
            readlock_version = r.version;
        }
    }
    pthread_mutex_unlock(&info->readmutex);

    m_group.zero_free_space(file_size, readlock_version);
}

#endif // TIGHTDB_DEBUG


size_t SharedGroup::get_current_version() TIGHTDB_NOEXCEPT
{
    SharedInfo* info = m_file_map.get_addr();
    return info->current_version;
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
        ScopedMutexLock lock(&info->readmutex);

        if (TIGHTDB_UNLIKELY(info->infosize > m_reader_map.get_size())) {
            m_reader_map.remap(m_file, File::access_ReadWrite, info->infosize);
        }

        if (ringbuf_is_empty())
            readlock_version = new_version;
        else {
            const ReadCount& r = ringbuf_get_first();
            readlock_version = r.version;
        }
    }

    // Reset version tracking in group if we are
    // starting from a new lock file
    if (new_version == 1) {
        // FIXME: Why is this not dealt with in begin_write()? Note
        // that we can read the value of info->current_version without
        // a lock on info->readmutex as long as we have a lock on
        // info->writemutex. This is true (not a data race) becasue
        // info->current_version is modified only while
        // info->writemutex is locked.
        m_group.init_shared();
    }

    // Do the actual commit
    bool do_persist = info->flags == durability_Full;
    size_t new_topref = m_group.commit(new_version, readlock_version, do_persist);

    // Get the new top ref
    const SlabAlloc& alloc = m_group.get_allocator();
    size_t new_filesize = alloc.get_base_size();

    // Update reader info
    {
        ScopedMutexLock lock(&info->readmutex);
        info->current_top     = new_topref;
        info->filesize        = new_filesize;
        // FIXME: Due to lack of adequate synchronization, the
        // following modification of 'info->current_version'
        // effectively participates in a "data race". Please see the
        // 'FIXME' in SharedGroup::has_changed() for more info.
        info->current_version = new_version;//FIXME src\tightdb\group_shared.cpp(772): warning C4267: '=' : conversion from 'size_t' to 'volatile uint32_t', possible loss of data
    }

    // Save last version for has_changed()
    m_version = new_version;
}
