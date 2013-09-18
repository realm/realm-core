#include <algorithm>

#include <tightdb/safe_int_ops.hpp>
#include <tightdb/terminate.hpp>
#include <tightdb/thread.hpp>
#include <tightdb/group_writer.hpp>
#include <tightdb/group_shared.hpp>


using namespace std;
using namespace tightdb;


struct SharedGroup::ReadCount {
    uint32_t version;
    uint32_t count;
};

struct SharedGroup::SharedInfo {
    uint16_t version;
    uint16_t flags;

    Mutex readmutex;
    RobustMutex writemutex;
    uint64_t filesize;

    uint64_t current_top;
    volatile uint32_t current_version;

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
    current_version = 0;
    capacity_mask   = init_readers_size - 1;
    put_pos = 0;
    get_pos = 0;
}


namespace {

void recover_from_dead_write_transact()
{
    // Nothing needs to be done
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

// FIXME: Issues with current implementation:
//
// - Possible reinitialization due to temporary unlocking during downgrade of file lock

void SharedGroup::open(const string& path, bool no_create_file,
                       DurabilityLevel dlevel)
{
    TIGHTDB_ASSERT(!is_attached());

    m_file_path = path + ".lock";

retry:
    {
        // Open shared coordination buffer
        m_file.open(m_file_path, File::access_ReadWrite, File::create_Auto, 0);
        File::CloseGuard fcg(m_file);

        // FIXME: Handle lock file removal in case of failure below

        bool need_init = false;
        size_t file_size = 0;

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
            if (int_cast_with_overflow_detect(m_file.get_size(), file_size))
                throw runtime_error("Lock file too large");

            // Handle empty files (first user)
            if (file_size == 0) {
                // Create new file
                file_size = sizeof (SharedInfo);
                m_file.resize(file_size);
            }
            need_init = true;
        }
        else {
            m_file.lock_shared();

            // Get size
            if (int_cast_with_overflow_detect(m_file.get_size(), file_size))
                throw runtime_error("Lock file too large");

            // There is a slight window between opening the file and getting the
            // lock where another process could have deleted the file
            if (file_size == 0 || m_file.is_removed())
                goto retry;
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
            m_version = 0;

            // FIXME: This downgrading of the lock is not guaranteed to be atomic

            // Downgrade lock to shared now that it is initialized,
            // so other processes can share it as well
            m_file.unlock();
            // FIXME: Must detach file from allocator if this fails
            m_file.lock_shared(); // Throws
        }
        else {
            if (info->version != 0)
                throw runtime_error("Unsupported version");

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
    }

#ifdef TIGHTDB_DEBUG
    m_transact_stage = transact_Ready;
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

    // If we can get an exclusive lock on the file we know that we are
    // the only user (since all users take at least shared locks on
    // the file.  So that means that we have to delete it when done
    // (to avoid someone later opening a stale file with uinitialized
    // mutexes)

    // FIXME: This upgrading of the lock is not guaranteed to be atomic
    m_file.unlock();

    // FIXME: File::try_lock_exclusive() can throw. We cannot allow
    // the exception to escape, because that would terminate the
    // program (due to 'noexcept' on the destructor).
    if (!m_file.try_lock_exclusive()) // Throws
        return;

    SharedInfo* info = m_file_map.get_addr();

    // If the db file is just backing for a transient data structure,
    // we can delete it when done.
    if (info->flags == durability_MemOnly) {
        size_t path_len = m_file_path.size()-5; // remove ".lock"
        // FIXME: Find a way to avoid the possible exception from
        // m_file_path.substr(). Currently, if it throws, the program
        // will be terminated due to 'noexcept' on ~SharedGroup().
        string db_path = m_file_path.substr(0, path_len); // Throws
        remove(db_path.c_str());
    }

    info->~SharedInfo(); // Call destructor

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
    // that may rightfully cause alarm.
    //
    // See also
    // http://stackoverflow.com/questions/12878344/volatile-in-c11
    //
    // Please note that the definition of a data race inf C++11 also
    // effectively applies to C++03. In this regard C++11 is just a
    // standardization of the existing paradigm.

    // Have we changed since last transaction?
    // Visibility of changes can be delayed when using has_changed() because m_info->current_version is tested
    // outside mutexes. However, the delay is finite on architectures that have hardware cache coherency (ARM, x64, x86,
    // POWER, UltraSPARC, etc) because it guarantees write propagation (writes to m_info->current_version occur on
    // system bus and make cache controllers invalidate caches of reader). Some excotic architectures may need
    // explicit synchronization which isn't implemented yet.
    TIGHTDB_SYNC_IF_NO_CACHE_COHERENCE
    const SharedInfo* info = m_file_map.get_addr();
    bool changed = m_version != info->current_version;
    return changed;
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
        m_version     = to_size_t(info->current_version); // fixme, remember to remove to_size_t when m_version becomes 64 bit

        // Update reader list
        if (ringbuf_is_empty()) {
            ReadCount r2 = { info->current_version, 1 };
            ringbuf_put(r2); // Throws
        }
        else {
            ReadCount& r = ringbuf_get_last();
            if (r.version == info->current_version) {
                ++r.count;
            }
            else {
                ReadCount r2 = { info->current_version, 1 };
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
    // Note that this will not get released until we call end_write().
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
            new_version = info->current_version + 1; // FIXME: Eventual overflow
        }
#else
        new_version = info->current_version + 1; // FIXME: Eventual overflow
#endif
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
        current_version = info->current_version + 1;
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

    // Reset version tracking in group if we are
    // starting from a new lock file
    if (new_version == 1) {
        // FIXME: Why is this not dealt with in begin_write()? Note
        // that we can read the value of info->current_version without
        // a lock on info->readmutex as long as we have a lock on
        // info->writemutex. This is true (not a data race) becasue
        // info->current_version is modified only while
        // info->writemutex is locked.
        m_group.init_shared(); // Throws
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
        // FIXME: Due to lack of adequate synchronization, the
        // following modification of 'info->current_version'
        // effectively participates in a "data race". Please see the
        // 'FIXME' in SharedGroup::has_changed() for more info.
        info->current_version = new_version;//FIXME src\tightdb\group_shared.cpp(772): warning C4267: '=' : conversion from 'size_t' to 'volatile uint32_t', possible loss of data
    }

    // Save last version for has_changed()
    m_version = new_version;
}
