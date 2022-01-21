/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/db.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <fcntl.h>
#include <iostream>
#include <mutex>
#include <sstream>
#include <type_traits>
#include <random>
#include <deque>
#include <thread>
#include <condition_variable>

#include <realm/disable_sync_to_disk.hpp>
#include <realm/group_writer.hpp>
#include <realm/group_writer.hpp>
#include <realm/impl/simulated_failure.hpp>
#include <realm/replication.hpp>
#include <realm/set.hpp>
#include <realm/dictionary.hpp>
#include <realm/table_view.hpp>
#include <realm/util/errno.hpp>
#include <realm/util/features.h>
#include <realm/util/file_mapper.hpp>
#include <realm/util/safe_int_ops.hpp>
#include <realm/util/scope_exit.hpp>
#include <realm/util/thread.hpp>
#include <realm/util/to_string.hpp>
#include "impl/copy_replication.hpp"

#ifndef _WIN32
#include <sys/wait.h>
#include <sys/time.h>
#include <unistd.h>
#else
#include <windows.h>
#include <process.h>
#endif

//#define REALM_ENABLE_LOGFILE


using namespace realm;
using namespace realm::metrics;
using namespace realm::util;
using Durability = DBOptions::Durability;

namespace {

// value   change
// --------------------
//  4      Unknown
//  5      Introduction of SharedInfo::file_format_version and
//         SharedInfo::history_type.
//  6      Using new robust mutex emulation where applicable
//  7      Introducing `commit_in_critical_phase` and `sync_agent_present`, and
//         changing `daemon_started` and `daemon_ready` from 1-bit to 8-bit
//         fields.
//  8      Placing the commitlog history inside the Realm file.
//  9      Fair write transactions requires an additional condition variable,
//         `write_fairness`
// 10      Introducing SharedInfo::history_schema_version.
// 11      New impl of InterprocessCondVar on windows.
#ifdef _WIN32
const uint_fast16_t g_shared_info_version = 11;
#else
const uint_fast16_t g_shared_info_version = 10; // version 11 didn't change anything on non-windows platforms
#endif

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

template <typename T>
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

template <typename T>
inline void atomic_double_dec(std::atomic<T>& counter)
{
    counter.fetch_sub(2, std::memory_order_release);
}

template <typename T>
bool atomic_one_if_zero(std::atomic<T>& counter)
{
    T old_val = counter.fetch_add(1, std::memory_order_acquire);
    if (old_val != 0) {
        counter.fetch_sub(1, std::memory_order_relaxed);
        return false;
    }
    return true;
}

template <typename T>
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
        for (int i = 0; i < init_readers_size; i++) {
            data[i].version = 1;
            data[i].count.store(1, std::memory_order_relaxed);
            data[i].current_top = 0;
            data[i].filesize = 0;
            data[i].next = i + 1;
        }
        old_pos = 0;
        data[0].count.store(0, std::memory_order_relaxed);
        data[init_readers_size - 1].next = 0;
        put_pos.store(0, std::memory_order_release);
    }

    void dump()
    {
        uint_fast32_t i = old_pos;
        std::cout << "--- " << std::endl;
        while (i != put_pos.load()) {
            std::cout << "  used " << i << " : " << data[i].count.load() << " | " << data[i].version << std::endl;
            i = data[i].next;
        }
        std::cout << "  LAST " << i << " : " << data[i].count.load() << " | " << data[i].version << std::endl;
        i = data[i].next;
        while (i != old_pos) {
            std::cout << "  free " << i << " : " << data[i].count.load() << " | " << data[i].version << std::endl;
            i = data[i].next;
        }
        std::cout << "--- Done" << std::endl;
    }

    void expand_to(uint_fast32_t new_entries) noexcept
    {
        // std::cout << "expanding to " << new_entries << std::endl;
        // dump();
        for (uint32_t i = entries; i < new_entries; i++) {
            data[i].version = 1;
            data[i].count.store(1, std::memory_order_relaxed);
            data[i].current_top = 0;
            data[i].filesize = 0;
            data[i].next = i + 1;
        }
        data[new_entries - 1].next = old_pos;
        data[put_pos.load(std::memory_order_relaxed)].next = entries;
        entries = uint32_t(new_entries);
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
    // called from init_versioning(), which is called by DB::open() under the
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
    {
        // do not call this if the buffer is full!
        uint_fast32_t idx = get(last()).next;
        return idx;
    }

    ReadCount& get_next() noexcept
    {
        REALM_ASSERT(!is_full());
        return data[next()];
    }

    void use_next() noexcept
    {
        atomic_dec(get_next().count); // .store_release(0);
        put_pos.store(uint32_t(next()), std::memory_order_release);
    }

    void cleanup() noexcept
    {
        // invariant: entry held by put_pos has count > 1.
        // std::cout << "cleanup: from " << old_pos << " to " << put_pos.load_relaxed();
        // dump();
        while (old_pos.load(std::memory_order_relaxed) != put_pos.load(std::memory_order_relaxed)) {
            const ReadCount& r = get(old_pos.load(std::memory_order_relaxed));
            if (!atomic_one_if_zero(r.count))
                break;
            auto next_ndx = get(old_pos.load(std::memory_order_relaxed)).next;
            old_pos.store(next_ndx, std::memory_order_relaxed);
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

// Using lambda rather than function so that shared_ptr shared state doesn't need to hold a function pointer.
constexpr auto TransactionDeleter = [](Transaction* t) {
    t->close();
    delete t;
};

template <typename... Args>
TransactionRef make_transaction_ref(Args&&... args)
{
    return TransactionRef(new Transaction(std::forward<Args>(args)...), TransactionDeleter);
}

} // anonymous namespace


/// The structure of the contents of the per session `.lock` file. Note that
/// this file is transient in that it is recreated/reinitialized at the
/// beginning of every session. A session is any sequence of temporally
/// overlapping openings of a particular Realm file via DB objects. For
/// example, if there are two DB objects, A and B, and the file is
/// first opened via A, then opened via B, then closed via A, and finally closed
/// via B, then the session streaches from the opening via A to the closing via
/// B.
///
/// IMPORTANT: Remember to bump `g_shared_info_version` if anything is changed
/// in the memory layout of this class, or if the meaning of any of the stored
/// values change.
///
/// Members `init_complete`, `shared_info_version`, `size_of_mutex`, and
/// `size_of_condvar` may only be modified only while holding an exclusive lock
/// on the file, and may be read only while holding a shared (or exclusive) lock
/// on the file. All other members (except for the Ringbuffer) may be accessed
/// only while holding a lock on `controlmutex`.
///
/// SharedInfo must be 8-byte aligned. On 32-bit Apple platforms, mutexes store their
/// alignment as part of the mutex state. We're copying the SharedInfo (including
/// embedded but alway unlocked mutexes) and it must retain the same alignment
/// throughout.
struct alignas(8) DB::SharedInfo {
    /// Indicates that initialization of the lock file was completed
    /// sucessfully.
    ///
    /// CAUTION: This member must never move or change type, as that would
    /// compromize safety of the the session initiation process.
    std::atomic<uint8_t> init_complete; // Offset 0

    /// The size in bytes of a mutex member of SharedInfo. This allows all
    /// session participants to be in agreement. Obviously, a size match is not
    /// enough to guarantee identical layout internally in the mutex object, but
    /// it is hoped that it will catch some (if not most) of the cases where
    /// there is a layout discrepancy internally in the mutex object.
    uint8_t size_of_mutex; // Offset 1

    /// Like size_of_mutex, but for condition variable members of SharedInfo.
    uint8_t size_of_condvar; // Offset 2

    /// Set during the critical phase of a commit, when the logs, the ringbuffer
    /// and the database may be out of sync with respect to each other. If a
    /// writer crashes during this phase, there is no safe way of continuing
    /// with further write transactions. When beginning a write transaction,
    /// this must be checked and an exception thrown if set.
    ///
    /// Note that std::atomic<uint8_t> is guaranteed to have standard layout.
    std::atomic<uint8_t> commit_in_critical_phase = {0}; // Offset 3

    /// The target Realm file format version for the current session. This
    /// allows all session participants to be in agreement. It can only differ
    /// from what is returned by Group::get_file_format_version() temporarily,
    /// and only during the Realm file opening process. If it differs, it means
    /// that the file format needs to be upgraded from its current format
    /// (Group::get_file_format_version()), the format specified by this member
    /// of SharedInfo.
    uint8_t file_format_version; // Offset 4

    /// Stores a value of type Replication::HistoryType. Must match across all
    /// session participants.
    int8_t history_type; // Offset 5

    /// The SharedInfo layout version. This allows all session participants to
    /// be in agreement. Must be bumped if the layout of the SharedInfo
    /// structure is changed. Note, however, that only the part that lies beyond
    /// SharedInfoUnchangingLayout can have its layout changed.
    ///
    /// CAUTION: This member must never move or change type, as that would
    /// compromize version agreement checking.
    uint16_t shared_info_version = g_shared_info_version; // Offset 6

    uint16_t durability;           // Offset 8
    uint16_t free_write_slots = 0; // Offset 10

    /// Number of participating shared groups
    uint32_t num_participants = 0; // Offset 12

    /// Latest version number. Guarded by the controlmutex (for lock-free
    /// access, use get_version_of_latest_snapshot() instead)
    uint64_t latest_version_number; // Offset 16

    /// Pid of process initiating the session, but only if that process runs
    /// with encryption enabled, zero otherwise. Other processes cannot join a
    /// session wich uses encryption, because interprocess sharing is not
    /// supported by our current encryption mechanisms.
    uint64_t session_initiator_pid = 0; // Offset 24

    uint64_t number_of_versions; // Offset 32

    /// True (1) if there is a sync agent present (a session participant acting
    /// as sync client). It is an error to have a session with more than one
    /// sync agent. The purpose of this flag is to prevent that from ever
    /// happening. If the sync agent crashes and leaves the flag set, the
    /// session will need to be restarted (lock file reinitialized) before a new
    /// sync agent can be started.
    uint8_t sync_agent_present = 0; // Offset 40

    /// Set when a participant decides to start the daemon, cleared by the
    /// daemon when it decides to exit. Participants check during open() and
    /// start the daemon if running in async mode.
    uint8_t daemon_started = 0; // Offset 41

    /// Set by the daemon when it is ready to handle commits. Participants must
    /// wait during open() on 'daemon_becomes_ready' for this to become true.
    /// Cleared by the daemon when it decides to exit.
    uint8_t daemon_ready = 0; // Offset 42

    uint8_t filler_1; // Offset 43

    /// Stores a history schema version (as returned by
    /// Replication::get_history_schema_version()). Must match across all
    /// session participants.
    uint16_t history_schema_version; // Offset 44

    uint16_t filler_2; // Offset 46

    InterprocessMutex::SharedPart shared_writemutex; // Offset 48
    InterprocessMutex::SharedPart shared_controlmutex;
    InterprocessCondVar::SharedPart room_to_write;
    InterprocessCondVar::SharedPart work_to_do;
    InterprocessCondVar::SharedPart daemon_becomes_ready;
    InterprocessCondVar::SharedPart new_commit_available;
    InterprocessCondVar::SharedPart pick_next_writer;
    std::atomic<uint32_t> next_ticket;
    std::atomic<uint32_t> next_served = 0;

    // IMPORTANT: The ringbuffer MUST be the last field in SharedInfo - see above.
    Ringbuffer readers;

    SharedInfo(Durability, Replication::HistoryType, int history_schema_version);
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


DB::SharedInfo::SharedInfo(Durability dura, Replication::HistoryType ht, int hsv)
    : size_of_mutex(sizeof(shared_writemutex))
    , size_of_condvar(sizeof(room_to_write))
    , shared_writemutex() // Throws
    , shared_controlmutex() // Throws
{
    durability = static_cast<uint16_t>(dura); // durability level is fixed from creation
    REALM_ASSERT(!util::int_cast_has_overflow<decltype(history_type)>(ht + 0));
    REALM_ASSERT(!util::int_cast_has_overflow<decltype(history_schema_version)>(hsv));
    history_type = ht;
    history_schema_version = static_cast<uint16_t>(hsv);
    InterprocessCondVar::init_shared_part(new_commit_available); // Throws
    InterprocessCondVar::init_shared_part(pick_next_writer);     // Throws
    next_ticket = 0;

// IMPORTANT: The offsets, types (, and meanings) of these members must
// never change, not even when the SharedInfo layout version is bumped. The
// eternal constancy of this part of the layout is what ensures that a
// joining session participant can reliably verify that the actual format is
// as expected.
#ifndef _WIN32
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winvalid-offsetof"
#endif
    static_assert(offsetof(SharedInfo, init_complete) == 0 && ATOMIC_BOOL_LOCK_FREE == 2 &&
                      std::is_same<decltype(init_complete), std::atomic<uint8_t>>::value &&
                      offsetof(SharedInfo, shared_info_version) == 6 &&
                      std::is_same<decltype(shared_info_version), uint16_t>::value,
                  "Forbidden change in SharedInfo layout");

    // Try to catch some of the memory layout changes that requires bumping of
    // the SharedInfo file format version (shared_info_version).
    static_assert(
        offsetof(SharedInfo, size_of_mutex) == 1 && std::is_same<decltype(size_of_mutex), uint8_t>::value &&
            offsetof(SharedInfo, size_of_condvar) == 2 && std::is_same<decltype(size_of_condvar), uint8_t>::value &&
            offsetof(SharedInfo, commit_in_critical_phase) == 3 &&
            std::is_same<decltype(commit_in_critical_phase), std::atomic<uint8_t>>::value &&
            offsetof(SharedInfo, file_format_version) == 4 &&
            std::is_same<decltype(file_format_version), uint8_t>::value && offsetof(SharedInfo, history_type) == 5 &&
            std::is_same<decltype(history_type), int8_t>::value && offsetof(SharedInfo, durability) == 8 &&
            std::is_same<decltype(durability), uint16_t>::value && offsetof(SharedInfo, free_write_slots) == 10 &&
            std::is_same<decltype(free_write_slots), uint16_t>::value &&
            offsetof(SharedInfo, num_participants) == 12 &&
            std::is_same<decltype(num_participants), uint32_t>::value &&
            offsetof(SharedInfo, latest_version_number) == 16 &&
            std::is_same<decltype(latest_version_number), uint64_t>::value &&
            offsetof(SharedInfo, session_initiator_pid) == 24 &&
            std::is_same<decltype(session_initiator_pid), uint64_t>::value &&
            offsetof(SharedInfo, number_of_versions) == 32 &&
            std::is_same<decltype(number_of_versions), uint64_t>::value &&
            offsetof(SharedInfo, sync_agent_present) == 40 &&
            std::is_same<decltype(sync_agent_present), uint8_t>::value &&
            offsetof(SharedInfo, daemon_started) == 41 && std::is_same<decltype(daemon_started), uint8_t>::value &&
            offsetof(SharedInfo, daemon_ready) == 42 && std::is_same<decltype(daemon_ready), uint8_t>::value &&
            offsetof(SharedInfo, filler_1) == 43 && std::is_same<decltype(filler_1), uint8_t>::value &&
            offsetof(SharedInfo, history_schema_version) == 44 &&
            std::is_same<decltype(history_schema_version), uint16_t>::value && offsetof(SharedInfo, filler_2) == 46 &&
            std::is_same<decltype(filler_2), uint16_t>::value && offsetof(SharedInfo, shared_writemutex) == 48 &&
            std::is_same<decltype(shared_writemutex), InterprocessMutex::SharedPart>::value,
        "Caught layout change requiring SharedInfo file format bumping");
#ifndef _WIN32
#pragma GCC diagnostic pop
#endif
}

#if REALM_HAVE_STD_FILESYSTEM
std::string DBOptions::sys_tmp_dir = std::filesystem::temp_directory_path().string();
#else
std::string DBOptions::sys_tmp_dir = getenv("TMPDIR") ? getenv("TMPDIR") : "";
#endif

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
// While it is not explicitly guaranteed in the man page, we shall
// assume that is is valid to initialize a process-shared mutex twice
// without an intervening call to pthread_mutex_destroy(). We need to
// be able to reinitialize a process-shared mutex if the first
// initializing process crashes and leaves the shared memory in an
// undefined state.

void DB::open(const std::string& path, bool no_create_file, const DBOptions options)
{
    // Exception safety: Since do_open() is called from constructors, if it
    // throws, it must leave the file closed.

    REALM_ASSERT(!is_attached());

    m_db_path = path;
    SlabAlloc& alloc = m_alloc;
    if (options.is_immutable) {
        SlabAlloc::Config cfg;
        cfg.read_only = true;
        cfg.no_create = true;
        cfg.encryption_key = options.encryption_key;
        auto top_ref = alloc.attach_file(path, cfg);
        SlabAlloc::DetachGuard dg(alloc);
        Group::read_only_version_check(alloc, top_ref, path);
        m_fake_read_lock_if_immutable = ReadLockInfo::make_fake(top_ref, m_alloc.get_baseline());
        dg.release();
        return;
    }
    m_lockfile_path = get_core_file(path, CoreFileType::Lock);
    m_coordination_dir = get_core_file(path, CoreFileType::Management);
    m_lockfile_prefix = m_coordination_dir + "/access_control";
    m_alloc.set_read_only(false);

#if REALM_METRICS
    if (options.enable_metrics) {
        m_metrics = std::make_shared<Metrics>(options.metrics_buffer_size);
    }
#endif // REALM_METRICS

    Replication::HistoryType openers_hist_type = Replication::hist_None;
    int openers_hist_schema_version = 0;
    if (Replication* repl = get_replication()) {
        openers_hist_type = repl->get_history_type();
        openers_hist_schema_version = repl->get_history_schema_version();
    }

    int current_file_format_version;
    int target_file_format_version;
    int stored_hist_schema_version = -1; // Signals undetermined

    int retries_left = 10; // number of times to retry before throwing exceptions
    // in case there is something wrong with the .lock file... the retries allows
    // us to pick a new lockfile initializer in case the first one crashes without
    // completing the initialization
    std::default_random_engine random_gen;
    for (;;) {

        // if we're retrying, we first wait a random time
        if (retries_left < 10) {
            if (retries_left == 9) { // we seed it from a true random source if possible
                std::random_device r;
                random_gen.seed(r());
            }
            int max_delay = (10 - retries_left) * 10;
            int msecs = random_gen() % max_delay;
            millisleep(msecs);
        }

        m_file.open(m_lockfile_path, File::access_ReadWrite, File::create_Auto, 0); // Throws
        File::CloseGuard fcg(m_file);
        m_file.set_fifo_path(m_coordination_dir, "lock.fifo");

        if (m_file.try_lock_exclusive()) { // Throws
            File::UnlockGuard ulg(m_file);

            // We're alone in the world, and it is Ok to initialize the
            // file. Start by truncating the file to zero to ensure that
            // the following resize will generate a file filled with zeroes.
            //
            // This will in particular set m_init_complete to 0.
            m_file.resize(0);
            m_file.prealloc(sizeof(SharedInfo));

            // We can crash anytime during this process. A crash prior to
            // the first resize could allow another thread which could not
            // get the exclusive lock because we hold it, and hence were
            // waiting for the shared lock instead, to observe and use an
            // old lock file.
            m_file_map.map(m_file, File::access_ReadWrite, sizeof(SharedInfo), File::map_NoSync); // Throws
            File::UnmapGuard fug(m_file_map);
            SharedInfo* info_2 = m_file_map.get_addr();

            new (info_2) SharedInfo{options.durability, openers_hist_type, openers_hist_schema_version}; // Throws

            // Because init_complete is an std::atomic, it's guaranteed not to be observable by others
            // as being 1 before the entire SharedInfo header has been written.
            info_2->init_complete = 1;
        }

// We hold the shared lock from here until we close the file!
#if REALM_PLATFORM_APPLE
        // macOS has a bug which can cause a hang waiting to obtain a lock, even
        // if the lock is already open in shared mode, so we work around it by
        // busy waiting. This should occur only briefly during session initialization.
        while (!m_file.try_lock_shared()) {
            sched_yield();
        }
#else
        m_file.lock_shared(); // Throws
#endif
        // The coordination/management dir is created as a side effect of the lock
        // operation above if needed for lock emulation. But it may also be needed
        // for other purposes, so make sure it exists.
        // in worst case there'll be a race on creating this directory.
        // This should be safe but a waste of resources.
        // Unfortunately it cannot be created at an earlier point, because
        // it may then be deleted during the above lock_shared() operation.
        try_make_dir(m_coordination_dir);

        // If the file is not completely initialized at this point in time, the
        // preceeding initialization attempt must have failed. We know that an
        // initialization process was in progress, because this thread (or
        // process) failed to get an exclusive lock on the file. Because this
        // thread (or process) currently has a shared lock on the file, we also
        // know that the initialization process can no longer be in progress, so
        // the initialization must either have completed or failed at this time.

        // The file is taken to be completely initialized if it is large enough
        // to contain the `init_complete` field, and `init_complete` is true. If
        // the file was not completely initialized, this thread must give up its
        // shared lock, and retry to become the initializer. Eventually, one of
        // two things must happen; either this thread, or another thread
        // succeeds in completing the initialization, or this thread becomes the
        // initializer, and fails the initialization. In either case, the retry
        // loop will eventually terminate.

        // An empty file is (and was) never a successfully initialized file.
        size_t info_size = sizeof(SharedInfo);
        {
            auto file_size = m_file.get_size();
            if (util::int_less_than(file_size, info_size)) {
                if (file_size == 0)
                    continue; // Retry
                info_size = size_t(file_size);
            }
        }

        // Map the initial section of the SharedInfo file that corresponds to
        // the SharedInfo struct, or less if the file is smaller. We know that
        // we have at least one byte, and that is enough to read the
        // `init_complete` flag.
        m_file_map.map(m_file, File::access_ReadWrite, info_size, File::map_NoSync);
        File::UnmapGuard fug_1(m_file_map);
        SharedInfo* info = m_file_map.get_addr();

#ifndef _WIN32
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winvalid-offsetof"
#endif
        static_assert(offsetof(SharedInfo, init_complete) + sizeof SharedInfo::init_complete <= 1,
                      "Unexpected position or size of SharedInfo::init_complete");
#ifndef _WIN32
#pragma GCC diagnostic pop
#endif
        if (info->init_complete == 0)
            continue;
        REALM_ASSERT(info->init_complete == 1);

        // At this time, we know that the file was completely initialized, but
        // we still need to verify that is was initialized with the memory
        // layout expected by this session participant. We could find that it is
        // initializaed with a different memory layout if other concurrent
        // session participants use different versions of the core library.
        if (info_size < sizeof(SharedInfo)) {
            if (retries_left) {
                --retries_left;
                continue;
            }
            std::stringstream ss;
            ss << "Info size doesn't match, " << info_size << " " << sizeof(SharedInfo) << ".";
            throw IncompatibleLockFile(ss.str());
        }
        if (info->shared_info_version != g_shared_info_version) {
            if (retries_left) {
                --retries_left;
                continue;
            }
            std::stringstream ss;
            ss << "Shared info version doesn't match, " << info->shared_info_version << " " << g_shared_info_version
               << ".";
            throw IncompatibleLockFile(ss.str());
        }
        // Validate compatible sizes of mutex and condvar types. Sizes of all
        // other fields are architecture independent, so if condvar and mutex
        // sizes match, the entire struct matches. The offsets of
        // `size_of_mutex` and `size_of_condvar` are known to be as expected due
        // to the preceeding check in `shared_info_version`.
        if (info->size_of_mutex != sizeof info->shared_controlmutex) {
            if (retries_left) {
                --retries_left;
                continue;
            }
            std::stringstream ss;
            ss << "Mutex size doesn't match: " << info->size_of_mutex << " " << sizeof(info->shared_controlmutex)
               << ".";
            throw IncompatibleLockFile(ss.str());
        }

        if (info->size_of_condvar != sizeof info->room_to_write) {
            if (retries_left) {
                --retries_left;
                continue;
            }
            std::stringstream ss;
            ss << "Condtion var size doesn't match: " << info->size_of_condvar << " " << sizeof(info->room_to_write)
               << ".";
            throw IncompatibleLockFile(ss.str());
        }
        m_writemutex.set_shared_part(info->shared_writemutex, m_lockfile_prefix, "write");
        m_controlmutex.set_shared_part(info->shared_controlmutex, m_lockfile_prefix, "control");

        // even though fields match wrt alignment and size, there may still be incompatibilities
        // between implementations, so lets ask one of the mutexes if it thinks it'll work.
        if (!m_controlmutex.is_valid()) {
            throw IncompatibleLockFile("Control mutex is invalid.");
        }

        // OK! lock file appears valid. We can now continue operations under the protection
        // of the controlmutex. The controlmutex protects the following activities:
        // - attachment of the database file
        // - start of the async daemon
        // - stop of the async daemon
        // - restore of a backup, if desired
        // - backup of the realm file in preparation of file format upgrade
        // - DB beginning/ending a session
        // - Waiting for and signalling database changes
        {
            std::lock_guard<InterprocessMutex> lock(m_controlmutex); // Throws
            // we need a thread-local copy of the number of ringbuffer entries in order
            // to later detect concurrent expansion of the ringbuffer.
            m_local_max_entry = info->readers.get_num_entries();

            // We need to map the info file once more for the readers part
            // since that part can be resized and as such remapped which
            // could move our mutexes (which we don't want to risk moving while
            // they are locked)
            size_t reader_info_size = sizeof(SharedInfo) + info->readers.compute_required_space(m_local_max_entry);
            m_reader_map.map(m_file, File::access_ReadWrite, reader_info_size, File::map_NoSync);
            File::UnmapGuard fug_2(m_reader_map);

            // proceed to initialize versioning and other metadata information related to
            // the database. Also create the database if we're beginning a new session
            bool begin_new_session = (info->num_participants == 0);
            SlabAlloc::Config cfg;
            cfg.session_initiator = begin_new_session;
            cfg.is_shared = true;
            cfg.read_only = false;
            cfg.skip_validate = !begin_new_session;
            cfg.disable_sync = options.durability == Durability::MemOnly || options.durability == Durability::Unsafe;

            // only the session initiator is allowed to create the database, all other
            // must assume that it already exists.
            cfg.no_create = (begin_new_session ? no_create_file : true);

            // if we're opening a MemOnly file that isn't already opened by
            // someone else then it's a file which should have been deleted on
            // close previously, but wasn't (perhaps due to the process crashing)
            cfg.clear_file = (options.durability == Durability::MemOnly && begin_new_session);

            cfg.encryption_key = m_key;
            ref_type top_ref;
            try {
                top_ref = alloc.attach_file(path, cfg); // Throws
            }
            catch (const SlabAlloc::Retry&) {
                // On a SlabAlloc::Retry file mappings are already unmapped, no
                // need to do more
                continue;
            }

            // Determine target file format version for session (upgrade
            // required if greater than file format version of attached file).
            current_file_format_version = alloc.get_committed_file_format_version();
            target_file_format_version =
                Group::get_target_file_format_version_for_session(current_file_format_version, openers_hist_type);
            BackupHandler backup(path, options.accepted_versions, options.to_be_deleted);
            if (backup.must_restore_from_backup(current_file_format_version)) {
                // we need to unmap before any file ops that'll change the realm
                // file:
                // (only strictly needed for Windows)
                alloc.detach();
                backup.restore_from_backup();
                // finally, retry with the restored file instead of the original
                // one:
                continue;
            }
            backup.cleanup_backups();

            // From here on, if we fail in any way, we must detach the
            // allocator.
            SlabAlloc::DetachGuard alloc_detach_guard(alloc);
            alloc.note_reader_start(this);
            // must come after the alloc detach guard
            auto handler = [this, &alloc]() noexcept {
                alloc.note_reader_end(this);
            };
            auto reader_end_guard = make_scope_exit(handler);

            // Check validity of top array (to give more meaningful errors
            // early)
            if (top_ref) {
                try {
                    alloc.note_reader_start(this);
                    auto reader_end_guard = make_scope_exit([&]() noexcept {
                        alloc.note_reader_end(this);
                    });
                    Array top{alloc};
                    top.init_from_ref(top_ref);
                    Group::validate_top_array(top, alloc);
                }
                catch (InvalidDatabase& e) {
                    if (e.get_path().empty()) {
                        e.set_path(path);
                    }
                    throw;
                }
            }
            if (options.backup_at_file_format_change) {
                backup.backup_realm_if_needed(current_file_format_version, target_file_format_version);
            }
            using gf = _impl::GroupFriend;
            bool file_format_ok;
            // In shared mode (Realm file opened via a DB instance) this
            // version of the core library is able to open Realms using file format
            // versions listed below. Please see Group::get_file_format_version() for
            // information about the individual file format versions.
            if (current_file_format_version == 0) {
                file_format_ok = (top_ref == 0);
            }
            else {
                file_format_ok = backup.is_accepted_file_format(current_file_format_version);
            }

            if (REALM_UNLIKELY(!file_format_ok)) {
                throw UnsupportedFileFormatVersion(current_file_format_version);
            }

            if (begin_new_session) {
                // Determine version (snapshot number) and check history
                // compatibility
                version_type version = 0;
                int stored_hist_type = 0;
                gf::get_version_and_history_info(alloc, top_ref, version, stored_hist_type,
                                                 stored_hist_schema_version);
                bool good_history_type = false;
                switch (openers_hist_type) {
                    case Replication::hist_None:
                        good_history_type = (stored_hist_type == Replication::hist_None);
                        if (!good_history_type)
                            throw IncompatibleHistories(
                                util::format("Expected a Realm without history, but found history type %1",
                                             stored_hist_type),
                                path);
                        break;
                    case Replication::hist_OutOfRealm:
                        REALM_ASSERT(false); // No longer in use
                        break;
                    case Replication::hist_InRealm:
                        good_history_type = (stored_hist_type == Replication::hist_InRealm ||
                                             stored_hist_type == Replication::hist_None);
                        if (!good_history_type)
                            throw IncompatibleHistories(
                                util::format(
                                    "Expected a Realm with no or in-realm history, but found history type %1",
                                    stored_hist_type),
                                path);
                        break;
                    case Replication::hist_SyncClient:
                        good_history_type = ((stored_hist_type == Replication::hist_SyncClient) || (top_ref == 0));
                        if (!good_history_type)
                            throw IncompatibleHistories(
                                util::format(
                                    "Expected an empty or synced Realm, but found history type %1, top ref %2",
                                    stored_hist_type, top_ref),
                                path);
                        break;
                    case Replication::hist_SyncServer:
                        good_history_type = ((stored_hist_type == Replication::hist_SyncServer) || (top_ref == 0));
                        if (!good_history_type)
                            throw IncompatibleHistories(util::format("Expected a Realm containing a server-side "
                                                                     "history, but found history type %1, top ref %2",
                                                                     stored_hist_type, top_ref),
                                                        path);
                        break;
                }

                REALM_ASSERT(stored_hist_schema_version >= 0);
                if (stored_hist_schema_version > openers_hist_schema_version)
                    throw IncompatibleHistories(
                        util::format("Unexpected future history schema version %1, current schema %2",
                                     stored_hist_schema_version, openers_hist_schema_version),
                        path);
                bool need_hist_schema_upgrade =
                    (stored_hist_schema_version < openers_hist_schema_version && top_ref != 0);
                if (need_hist_schema_upgrade) {
                    Replication* repl = get_replication();
                    if (!repl->is_upgradable_history_schema(stored_hist_schema_version))
                        throw IncompatibleHistories(util::format("Nonupgradable history schema %1, current schema %2",
                                                                 stored_hist_schema_version,
                                                                 openers_hist_schema_version),
                                                    path);
                }

                if (m_key) {
#ifdef _WIN32
                    uint64_t pid = GetCurrentProcessId();
#else
                    static_assert(sizeof(pid_t) <= sizeof(uint64_t), "process identifiers too large");
                    uint64_t pid = getpid();
#endif
                    info->session_initiator_pid = pid;
                }

                info->file_format_version = uint_fast8_t(target_file_format_version);

                // Initially there is a single version in the file
                info->number_of_versions = 1;

                info->latest_version_number = version;
                alloc.init_mapping_management(version);

                SharedInfo* r_info = m_reader_map.get_addr();
                size_t file_size = alloc.get_baseline();
                // REALM_ASSERT(m_alloc.matches_section_boundary(file_size));
                r_info->init_versioning(top_ref, file_size, version);
            }
            else { // Not the session initiator
                // Durability setting must be consistent across a session. An
                // inconsistency is a logic error, as the user is required to
                // make sure that all possible concurrent session participants
                // use the same durability setting for the same Realm file.
                if (Durability(info->durability) != options.durability)
                    throw LogicError(LogicError::mixed_durability);

                // History type must be consistent across a session. An
                // inconsistency is a logic error, as the user is required to
                // make sure that all possible concurrent session participants
                // use the same history type for the same Realm file.
                if (info->history_type != openers_hist_type)
                    throw LogicError(LogicError::mixed_history_type);

                // History schema version must be consistent across a
                // session. An inconsistency is a logic error, as the user is
                // required to make sure that all possible concurrent session
                // participants use the same history schema version for the same
                // Realm file.
                if (info->history_schema_version != openers_hist_schema_version)
                    throw LogicError(LogicError::mixed_history_schema_version);
#ifdef _WIN32
                uint64_t pid = GetCurrentProcessId();
#else
                uint64_t pid = getpid();
#endif

                if (m_key && info->session_initiator_pid != pid) {
                    std::stringstream ss;
                    ss << path << ": Encrypted interprocess sharing is currently unsupported."
                       << "DB has been opened by pid: " << info->session_initiator_pid << ". Current pid is " << pid
                       << ".";
                    throw std::runtime_error(ss.str());
                }

                // We need per session agreement among all participants on the
                // target Realm file format. From a technical perspective, the
                // best way to ensure that, would be to require a bumping of the
                // SharedInfo file format version on any change that could lead
                // to a different result from
                // get_target_file_format_for_session() given the same current
                // Realm file format version and the same history type, as that
                // would prevent the outcome of the Realm opening process from
                // depending on race conditions. However, for practical reasons,
                // we shall instead simply check that there is agreement, and
                // throw the same kind of exception, as would have been thrown
                // with a bumped SharedInfo file format version, if there isn't.
                if (info->file_format_version != target_file_format_version) {
                    std::stringstream ss;
                    ss << "File format version doesn't match: " << info->file_format_version << " "
                       << target_file_format_version << ".";
                    throw IncompatibleLockFile(ss.str());
                }

                // Even though this session participant is not the session initiator,
                // it may be the one that has to perform the history schema upgrade.
                // See upgrade_file_format(). However we cannot get the actual value
                // at this point as the allocator is not synchronized with the file.
                // The value will be read in a ReadTransaction later.

                // We need to setup the allocators version information, as it is needed
                // to correctly age and later reclaim memory mappings.
                version_type version = info->latest_version_number;
                alloc.init_mapping_management(version);
            }

            m_new_commit_available.set_shared_part(info->new_commit_available, m_lockfile_prefix, "new_commit",
                                                   options.temp_dir);
            m_pick_next_writer.set_shared_part(info->pick_next_writer, m_lockfile_prefix, "pick_writer",
                                               options.temp_dir);

            // make our presence noted:
            ++info->num_participants;

            // Keep the mappings and file open:
            alloc_detach_guard.release();
            fug_2.release(); // Do not unmap
            fug_1.release(); // Do not unmap
            fcg.release();   // Do not close
        }
        break;
    }

    // Upgrade file format and/or history schema
    try {
        if (stored_hist_schema_version == -1) {
            // current_hist_schema_version has not been read. Read it now
            stored_hist_schema_version = start_read()->get_history_schema_version();
        }
        if (current_file_format_version == 0) {
            // If the current file format is still undecided, no upgrade is
            // necessary, but we still need to make the chosen file format
            // visible to the rest of the core library by updating the value
            // that will be subsequently returned by
            // Group::get_file_format_version(). For this to work, all session
            // participants must adopt the chosen target Realm file format when
            // the stored file format version is zero regardless of the version
            // of the core library used.
            m_file_format_version = target_file_format_version;
        }
        else {
            m_file_format_version = current_file_format_version;
            upgrade_file_format(options.allow_file_format_upgrade, target_file_format_version,
                                stored_hist_schema_version, openers_hist_schema_version); // Throws
        }
    }
    catch (...) {
        close();
        throw;
    }

    m_alloc.set_read_only(true);
}

void DB::open(BinaryData buffer, bool take_ownership)
{
    auto top_ref = m_alloc.attach_buffer(buffer.data(), buffer.size());
    m_fake_read_lock_if_immutable = ReadLockInfo::make_fake(top_ref, buffer.size());
    if (take_ownership)
        m_alloc.own_buffer();
}

void DB::open(Replication& repl, const std::string& file, const DBOptions options)
{
    // Exception safety: Since open() is called from constructors, if it throws,
    // it must leave the file closed.

    REALM_ASSERT(!is_attached());

    repl.initialize(*this); // Throws

    set_replication(&repl);

    bool no_create = false;
    open(file, no_create, options); // Throws
}

namespace {

using ColInfo = std::vector<std::pair<ColKey, Table*>>;

ColInfo get_col_info(const Table* table)
{
    std::vector<std::pair<ColKey, Table*>> cols;
    if (table) {
        for (auto col : table->get_column_keys()) {
            Table* embedded_table = nullptr;
            if (auto target_table = table->get_opposite_table(col)) {
                if (target_table->is_embedded())
                    embedded_table = target_table.unchecked_ptr();
            }
            cols.emplace_back(col, embedded_table);
        }
    }
    return cols;
}

void generate_properties_for_obj(Replication& repl, const Obj& obj, const ColInfo& cols)
{
    for (auto elem : cols) {
        auto col = elem.first;
        auto embedded_table = elem.second;
        auto cols_2 = get_col_info(embedded_table);
        auto update_embedded = [&](Mixed val) {
            REALM_ASSERT(val.is_type(type_Link, type_TypedLink));
            Obj embedded_obj = embedded_table->get_object(val.get<ObjKey>());
            generate_properties_for_obj(repl, embedded_obj, cols_2);
        };

        if (col.is_list()) {
            auto list = obj.get_listbase_ptr(col);
            auto sz = list->size();
            repl.list_clear(*list);
            for (size_t n = 0; n < sz; n++) {
                auto val = list->get_any(n);
                repl.list_insert(*list, n, val, n);
                if (embedded_table) {
                    update_embedded(val);
                }
            }
        }
        else if (col.is_set()) {
            auto set = obj.get_setbase_ptr(col);
            auto sz = set->size();
            for (size_t n = 0; n < sz; n++) {
                repl.set_insert(*set, n, set->get_any(n));
                // Sets cannot have embedded objects
            }
        }
        else if (col.is_dictionary()) {
            auto dict = obj.get_dictionary(col);
            size_t n = 0;
            for (auto [key, value] : dict) {
                repl.dictionary_insert(dict, n++, key, value);
                if (embedded_table) {
                    update_embedded(value);
                }
            }
        }
        else {
            auto val = obj.get_any(col);
            repl.set(obj.get_table().unchecked_ptr(), col, obj.get_key(), val);
            if (embedded_table) {
                update_embedded(val);
            }
        }
    }
}

} // namespace

void Transaction::replicate(Transaction* dest, Replication& repl) const
{
    // We should only create entries for public tables
    std::vector<TableKey> public_table_keys;
    for (auto tk : get_table_keys()) {
        if (table_is_public(tk))
            public_table_keys.push_back(tk);
    }

    // Create tables
    for (auto tk : public_table_keys) {
        auto table = get_table(tk);
        auto table_name = table->get_name();
        if (!table->is_embedded()) {
            auto pk_col = table->get_primary_key_column();
            if (!pk_col)
                throw std::runtime_error(
                    util::format("Class '%1' must have a primary key", Group::table_name_to_class_name(table_name)));
            auto pk_name = table->get_column_name(pk_col);
            if (pk_name != "_id")
                throw std::runtime_error(
                    util::format("Primary key of class '%1' must be named '_id'. Current is '%2'",
                                 Group::table_name_to_class_name(table_name), pk_name));
            repl.add_class_with_primary_key(tk, table_name, DataType(pk_col.get_type()), pk_name,
                                            pk_col.is_nullable());
        }
        else {
            repl.add_class(tk, table_name, true);
        }
    }
    // Create columns
    for (auto tk : public_table_keys) {
        auto table = get_table(tk);
        auto pk_col = table->get_primary_key_column();
        auto cols = table->get_column_keys();
        for (auto col : cols) {
            if (col == pk_col)
                continue;
            repl.insert_column(table.unchecked_ptr(), col, DataType(col.get_type()), table->get_column_name(col),
                               table->get_opposite_table(col).unchecked_ptr());
        }
    }
    dest->commit_and_continue_writing();
    // Now the schema should be in place - create the objects
#ifdef REALM_DEBUG
    constexpr int number_of_objects_to_create_before_committing = 100;
#else
    constexpr int number_of_objects_to_create_before_committing = 1000;
#endif
    auto n = number_of_objects_to_create_before_committing;
    for (auto tk : public_table_keys) {
        auto table = get_table(tk);
        if (table->is_embedded())
            continue;
        // std::cout << "Table: " << table->get_name() << std::endl;
        auto pk_col = table->get_primary_key_column();
        auto cols = get_col_info(table.unchecked_ptr());
        for (auto o : *table) {
            auto obj_key = o.get_key();
            Mixed pk = o.get_any(pk_col);
            // std::cout << "    Object: " << pk << std::endl;
            repl.create_object_with_primary_key(table.unchecked_ptr(), obj_key, pk);
            generate_properties_for_obj(repl, o, cols);
            if (--n == 0) {
                dest->commit_and_continue_writing();
                n = number_of_objects_to_create_before_committing;
            }
        }
    }
}


void Transaction::copy_to(TransactionRef dest) const
{
    impl::CopyReplication repl(dest);
    replicate(dest.get(), repl);
}

void DB::create_new_history(Replication& repl)
{
    Replication* old_repl = get_replication();
    try {
        repl.initialize(*this);
        set_replication(&repl);

        auto tr = start_write();
        tr->clear_history();
        tr->replicate(tr.get(), repl);
        tr->commit();
    }
    catch (...) {
        set_replication(old_repl);
        throw;
    }
}

void DB::create_new_history(std::unique_ptr<Replication> repl)
{
    create_new_history(*repl);
    m_history = std::move(repl);
}


// WARNING / FIXME: compact() should NOT be exposed publicly on Windows because it's not crash safe! It may
// corrupt your database if something fails.
// Tracked by https://github.com/realm/realm-core/issues/4111

// A note about lock ordering.
// The local mutex, m_mutex, guards transaction start/stop and map/unmap of the lock file.
// Except for compact(), open() and close(), it should only be held briefly.
// The controlmutex guards operations which change the file size, session initialization
// and session exit.
// The writemutex guards the integrity of the (write) transaction data.
// The controlmutex and writemutex resides in the .lock file and thus requires
// the mapping of the .lock file to work. A straightforward approach would be to lock
// the m_mutex whenever the other mutexes are taken or released...but that would be too
// bad for performance of transaction start/stop.
//
// The locks are to be taken in this order: writemutex->controlmutex->m_mutex
//
// The .lock file is mapped during DB::create() and unmapped by a call to DB::close().
// Once unmapped, it is never mapped again. Hence any observer with a valid DBRef may
// only see the transition from mapped->unmapped, never the opposite.
//
// Trying to create a transaction if the .lock file is unmapped will result in an assert.
// Unmapping (during close()) while transactions are live, is not considered an error. There
// is a potential race between unmapping during close() and any operation carried out by a live
// transaction. The user must ensure that this race never happens if she uses DB::close().
bool DB::compact(bool bump_version_number, util::Optional<const char*> output_encryption_key)
{
    REALM_ASSERT(!m_fake_read_lock_if_immutable);
    std::string tmp_path = m_db_path + ".tmp_compaction_space";

    // To enter compact, the DB object must already have been attached to a file,
    // since this happens in DB::create().

    // Verify that the lock file is still attached. There is no attempt to guard against
    // a race between close() and compact().
    if (is_attached() == false) {
        throw std::runtime_error(m_db_path + ": compact must be done on an open/attached DB");
    }
    SharedInfo* info = m_file_map.get_addr();
    Durability dura = Durability(info->durability);
    const char* write_key = bool(output_encryption_key) ? *output_encryption_key : m_key;
    {
        std::unique_lock<InterprocessMutex> lock(m_controlmutex); // Throws

        // We must be the ONLY DB object attached if we're to do compaction
        if (info->num_participants > 1)
            return false;

        // Holding the controlmutex prevents any other DB from attaching to the file.

        // local lock blocking any transaction from starting (and stopping)
        std::lock_guard<std::recursive_mutex> local_lock(m_mutex);

        // We should be the only transaction active - otherwise back out
        if (m_transaction_count != 0)
            return false;

        // group::write() will throw if the file already exists.
        // To prevent this, we have to remove the file (should it exist)
        // before calling group::write().
        File::try_remove(tmp_path);

        // Using start_read here ensures that we have access to the latest entry
        // in the ringbuffer. We need to have access to that later to update top_ref and file_size.
        // This is also needed to attach the group (get the proper top pointer, etc)
        TransactionRef tr = start_read();

        // Compact by writing a new file holding only live data, then renaming the new file
        // so it becomes the database file, replacing the old one in the process.
        try {
            File file;
            file.open(tmp_path, File::access_ReadWrite, File::create_Must, 0);
            int incr = bump_version_number ? 1 : 0;
            Group::DefaultTableWriter writer;
            tr->write(file, write_key, info->latest_version_number + incr, writer); // Throws
            // Data needs to be flushed to the disk before renaming.
            bool disable_sync = get_disable_sync_to_disk();
            if (!disable_sync && dura != Durability::Unsafe)
                file.sync(); // Throws
        }
        catch (...) {
            // If writing the compact version failed in any way, delete the partially written file to clean up disk
            // space. This is so that we don't fail with 100% disk space used when compacting on a mostly full disk.
            if (File::exists(tmp_path)) {
                File::remove(tmp_path);
            }
            throw;
        }
        {
            SharedInfo* r_info = m_reader_map.get_addr();
            Ringbuffer::ReadCount& rc = const_cast<Ringbuffer::ReadCount&>(r_info->readers.get_last());
            REALM_ASSERT_3(rc.version, ==, info->latest_version_number);
            static_cast<void>(rc); // rc unused if ENABLE_ASSERTION is unset
        }
        // if we've written a file with a bumped version number, we need to update the lock file to match.
        if (bump_version_number) {
            ++info->latest_version_number;
        }
        // We need to release any shared mapping *before* releasing the control mutex.
        // When someone attaches to the new database file, they *must* *not* see and
        // reuse any existing memory mapping of the stale file.
        tr->close();
        m_alloc.detach();

#ifdef _WIN32
        util::File::copy(tmp_path, m_db_path);
#else
        util::File::move(tmp_path, m_db_path);
#endif

        SlabAlloc::Config cfg;
        cfg.session_initiator = true;
        cfg.is_shared = true;
        cfg.read_only = false;
        cfg.skip_validate = false;
        cfg.no_create = true;
        cfg.clear_file = false;
        cfg.encryption_key = write_key;
        ref_type top_ref;
        top_ref = m_alloc.attach_file(m_db_path, cfg);
        m_alloc.init_mapping_management(info->latest_version_number);
        info->number_of_versions = 1;
        SharedInfo* r_info = m_reader_map.get_addr();
        size_t file_size = m_alloc.get_baseline();
        r_info->init_versioning(top_ref, file_size, info->latest_version_number);
    }
    return true;
}

void DB::write_copy(StringData path, util::Optional<const char*> output_encryption_key, bool allow_overwrite)
{
    SharedInfo* info = m_file_map.get_addr();
    const char* write_key = bool(output_encryption_key) ? *output_encryption_key : m_key;

    auto tr = start_read();
    if (auto hist = tr->get_history()) {
        if (!hist->no_pending_local_changes(tr->get_version())) {
            throw std::runtime_error("Could not write file as not all client changes are integrated in server");
        }
    }

    class NoClientFileIdWriter : public Group::DefaultTableWriter {
    public:
        NoClientFileIdWriter()
            : Group::DefaultTableWriter(true)
        {
        }
        HistoryInfo write_history(_impl::OutputStream& out) override
        {
            auto hist = Group::DefaultTableWriter::write_history(out);
            hist.sync_file_id = 0;
            return hist;
        }
    } writer;

    File file;
    file.open(path, File::access_ReadWrite, allow_overwrite ? File::create_Auto : File::create_Must, 0);
    file.resize(0);

    tr->write(file, write_key, info->latest_version_number, writer);
}

uint_fast64_t DB::get_number_of_versions()
{
    if (m_fake_read_lock_if_immutable)
        return 1;
    SharedInfo* info = m_file_map.get_addr();
    std::lock_guard<InterprocessMutex> lock(m_controlmutex); // Throws
    return info->number_of_versions;
}

size_t DB::get_allocated_size() const
{
    return m_alloc.get_allocated_size();
}

DB::~DB() noexcept
{
    close();
}

void DB::release_all_read_locks() noexcept
{
    REALM_ASSERT(!m_fake_read_lock_if_immutable);
    std::lock_guard<std::recursive_mutex> local_lock(m_mutex);
    SharedInfo* r_info = m_reader_map.get_addr();
    for (auto& read_lock : m_local_locks_held) {
        --m_transaction_count;
        const Ringbuffer::ReadCount& r = r_info->readers.get(read_lock.m_reader_idx);
        atomic_double_dec(r.count);
    }
    m_local_locks_held.clear();
    REALM_ASSERT(m_transaction_count == 0);
}

// Note: close() and close_internal() may be called from the DB::~DB().
// in that case, they will not throw. Throwing can only happen if called
// directly.
void DB::close(bool allow_open_read_transactions)
{
    // make helper thread terminate
    m_commit_helper.reset();

    if (m_fake_read_lock_if_immutable) {
        if (!is_attached())
            return;
        {
            std::lock_guard<std::recursive_mutex> local_lock(m_mutex);
            if (!allow_open_read_transactions && m_transaction_count)
                throw LogicError(LogicError::wrong_transact_state);
        }
        if (m_alloc.is_attached())
            m_alloc.detach();
        m_fake_read_lock_if_immutable.reset();
    }
    else {
        close_internal(std::unique_lock<InterprocessMutex>(m_controlmutex, std::defer_lock),
                       allow_open_read_transactions);
    }
}

void DB::close_internal(std::unique_lock<InterprocessMutex> lock, bool allow_open_read_transactions)
{
    if (!is_attached())
        return;

    {
        std::lock_guard<std::recursive_mutex> local_lock(m_mutex);
        if (m_write_transaction_open)
            throw LogicError(LogicError::wrong_transact_state);
        if (!allow_open_read_transactions && m_transaction_count)
            throw LogicError(LogicError::wrong_transact_state);
    }
    SharedInfo* info = m_file_map.get_addr();
    {
        if (!lock.owns_lock())
            lock.lock();

        if (m_alloc.is_attached())
            m_alloc.detach();

        if (m_is_sync_agent) {
            REALM_ASSERT(info->sync_agent_present);
            info->sync_agent_present = 0; // Set to false
        }
        release_all_read_locks();
        --info->num_participants;
        bool end_of_session = info->num_participants == 0;
        // std::cerr << "closing" << std::endl;
        if (end_of_session) {

            // If the db file is just backing for a transient data structure,
            // we can delete it when done.
            if (Durability(info->durability) == Durability::MemOnly) {
                try {
                    util::File::remove(m_db_path.c_str());
                }
                catch (...) {
                } // ignored on purpose.
            }
        }
        lock.unlock();
    }
    {
        std::lock_guard<std::recursive_mutex> local_lock(m_mutex);

        m_new_commit_available.close();
        m_pick_next_writer.close();

        // On Windows it is important that we unmap before unlocking, else a SetEndOfFile() call from another thread
        // may
        // interleave which is not permitted on Windows. It is permitted on *nix.
        m_file_map.unmap();
        m_reader_map.unmap();
        m_file.unlock();
        // info->~SharedInfo(); // DO NOT Call destructor
        m_file.close();
    }
}

class DB::AsyncCommitHelper {
public:
    AsyncCommitHelper(DB* db)
        : m_db(db)
    {
    }
    ~AsyncCommitHelper()
    {
        {
            std::unique_lock lg(m_mutex);
            if (!m_running) {
                return;
            }
            m_running = false;
            m_cv_worker.notify_one();
        }
        m_thread.join();
    }

    void begin_write(util::UniqueFunction<void()> fn)
    {
        std::unique_lock lg(m_mutex);
        start_thread();
        m_pending_writes.emplace_back(std::move(fn));
        m_cv_worker.notify_one();
    }

    void blocking_begin_write()
    {
        std::unique_lock lg(m_mutex);

        // If we support unlocking InterprocessMutex from a different thread
        // than it was locked on, we can sometimes just begin the write on
        // the current thread. This requires that no one is currently waiting
        // for the worker thread to acquire the write lock, as we'll deadlock
        // if we try to async commit while the worker is waiting for the lock.
        bool can_lock_on_caller =
            !InterprocessMutex::is_thread_confined && (!m_owns_write_mutex && m_pending_writes.empty() &&
                                                       m_write_lock_claim_ticket == m_write_lock_claim_fulfilled);

        // If we support cross-thread unlocking and m_running is false,
        // can_lock_on_caller should always be true or we forgot to launch the thread
        REALM_ASSERT(can_lock_on_caller || m_running || InterprocessMutex::is_thread_confined);

        // If possible, just begin the write on the current thread
        if (can_lock_on_caller) {
            m_waiting_for_write_mutex = true;
            lg.unlock();
            m_db->do_begin_write();
            lg.lock();
            m_waiting_for_write_mutex = false;
            m_has_write_mutex = true;
            m_owns_write_mutex = false;
            return;
        }

        // Otherwise we have to ask the worker thread to acquire it and wait
        // for that
        start_thread();
        size_t ticket = ++m_write_lock_claim_ticket;
        m_cv_worker.notify_one();
        m_cv_callers.wait(lg, [this, ticket] {
            return ticket == m_write_lock_claim_fulfilled;
        });
    }

    void end_write()
    {
        std::unique_lock lg(m_mutex);
        REALM_ASSERT(m_has_write_mutex);
        REALM_ASSERT(m_owns_write_mutex || !InterprocessMutex::is_thread_confined);

        // If we acquired the write lock on the worker thread, also release it
        // there even if our mutex supports unlocking cross-thread as it simplifies things.
        if (m_owns_write_mutex) {
            m_pending_mx_release = true;
            m_cv_worker.notify_one();
        }
        else {
            m_db->do_end_write();
            m_has_write_mutex = false;
        }
    }

    bool blocking_end_write()
    {
        std::unique_lock lg(m_mutex);
        if (!m_has_write_mutex) {
            return false;
        }
        REALM_ASSERT(m_owns_write_mutex || !InterprocessMutex::is_thread_confined);

        // If we acquired the write lock on the worker thread, also release it
        // there even if our mutex supports unlocking cross-thread as it simplifies things.
        if (m_owns_write_mutex) {
            m_pending_mx_release = true;
            m_cv_worker.notify_one();
            m_cv_callers.wait(lg, [this] {
                return !m_pending_mx_release;
            });
        }
        else {
            m_db->do_end_write();
            m_has_write_mutex = false;

            // The worker thread may have ignored a request for the write mutex
            // while we were acquiring it, so we need to wake up the thread
            if (has_pending_write_requests()) {
                lg.unlock();
                m_cv_worker.notify_one();
            }
        }
        return true;
    }


    void sync_to_disk(util::UniqueFunction<void()> fn)
    {
        REALM_ASSERT(fn);
        std::unique_lock lg(m_mutex);
        REALM_ASSERT(!m_pending_sync);
        start_thread();
        m_pending_sync = std::move(fn);
        m_cv_worker.notify_one();
    }

private:
    DB* m_db;
    std::thread m_thread;
    std::mutex m_mutex;
    std::condition_variable m_cv_worker;
    std::condition_variable m_cv_callers;
    std::deque<util::UniqueFunction<void()>> m_pending_writes;
    util::UniqueFunction<void()> m_pending_sync;
    size_t m_write_lock_claim_ticket = 0;
    size_t m_write_lock_claim_fulfilled = 0;
    bool m_pending_mx_release = false;
    bool m_running = false;
    bool m_has_write_mutex = false;
    bool m_owns_write_mutex = false;
    bool m_waiting_for_write_mutex = false;

    void main();

    void start_thread()
    {
        if (m_running) {
            return;
        }
        m_running = true;
        m_thread = std::thread([this]() {
            main();
        });
    }

    bool has_pending_write_requests()
    {
        return m_write_lock_claim_fulfilled < m_write_lock_claim_ticket || !m_pending_writes.empty();
    }
};

void DB::AsyncCommitHelper::main()
{
    std::unique_lock lg(m_mutex);
    while (m_running) {
#if 0 // Enable for testing purposes
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
#endif
        if (m_has_write_mutex) {
            if (auto cb = std::move(m_pending_sync)) {
                // Only one of sync_to_disk(), end_write(), or blocking_end_write()
                // should be called, so we should never have both a pending sync
                // and pending release.
                REALM_ASSERT(!m_pending_mx_release);
                lg.unlock();
                cb();
                cb = nullptr; // Release things captured by the callback before reacquiring the lock
                lg.lock();
                m_pending_mx_release = true;
            }
            if (m_pending_mx_release) {
                REALM_ASSERT(!InterprocessMutex::is_thread_confined || m_owns_write_mutex);
                m_db->do_end_write();
                m_pending_mx_release = false;
                m_has_write_mutex = false;
                m_owns_write_mutex = false;

                lg.unlock();
                m_cv_callers.notify_all();
                lg.lock();
                continue;
            }
        }
        else {
            REALM_ASSERT(!m_pending_sync && !m_pending_mx_release);

            // Acquire the write lock if anyone has requested it, but only if
            // another thread is not already waiting for it. If there's another
            // thread requesting and they get it while we're waiting, we'll
            // deadlock if they ask us to perform the sync.
            if (!m_waiting_for_write_mutex && has_pending_write_requests()) {
                lg.unlock();
                m_db->do_begin_write();
                lg.lock();

                REALM_ASSERT(!m_has_write_mutex);
                m_has_write_mutex = true;
                m_owns_write_mutex = true;

                // Synchronous transaction requests get priority over async
                if (m_write_lock_claim_fulfilled < m_write_lock_claim_ticket) {
                    ++m_write_lock_claim_fulfilled;
                    m_cv_callers.notify_all();
                    continue;
                }

                REALM_ASSERT(!m_pending_writes.empty());
                auto callback = std::move(m_pending_writes.front());
                m_pending_writes.pop_front();
                lg.unlock();
                callback();
                // Release things captured by the callback before reacquiring the lock
                callback = nullptr;
                lg.lock();
                continue;
            }
        }
        m_cv_worker.wait(lg);
    }
    if (m_has_write_mutex && m_owns_write_mutex) {
        m_db->do_end_write();
    }
}


void DB::async_begin_write(util::UniqueFunction<void()> fn)
{
    REALM_ASSERT(m_commit_helper);
    m_commit_helper->begin_write(std::move(fn));
}

void DB::async_end_write()
{
    REALM_ASSERT(m_commit_helper);
    m_commit_helper->end_write();
}

void DB::async_sync_to_disk(util::UniqueFunction<void()> fn)
{
    REALM_ASSERT(m_commit_helper);
    m_commit_helper->sync_to_disk(std::move(fn));
}

bool DB::has_changed(TransactionRef& tr)
{
    if (m_fake_read_lock_if_immutable)
        return false; // immutable doesn't change
    bool changed = tr->m_read_lock.m_version != get_version_of_latest_snapshot();
    return changed;
}

bool DB::wait_for_change(TransactionRef& tr)
{
    REALM_ASSERT(!m_fake_read_lock_if_immutable);
    SharedInfo* info = m_file_map.get_addr();
    std::lock_guard<InterprocessMutex> lock(m_controlmutex);
    while (tr->m_read_lock.m_version == info->latest_version_number && m_wait_for_change_enabled) {
        m_new_commit_available.wait(m_controlmutex, 0);
    }
    return tr->m_read_lock.m_version != info->latest_version_number;
}


void DB::wait_for_change_release()
{
    if (m_fake_read_lock_if_immutable)
        return;
    std::lock_guard<InterprocessMutex> lock(m_controlmutex);
    m_wait_for_change_enabled = false;
    m_new_commit_available.notify_all();
}


void DB::enable_wait_for_change()
{
    REALM_ASSERT(!m_fake_read_lock_if_immutable);
    std::lock_guard<InterprocessMutex> lock(m_controlmutex);
    m_wait_for_change_enabled = true;
}

void Transaction::set_transact_stage(DB::TransactStage stage) noexcept
{
#if REALM_METRICS
    REALM_ASSERT(m_metrics == db->m_metrics);
    if (m_metrics) { // null if metrics are disabled
        size_t total_size = db->m_used_space + db->m_free_space;
        size_t free_space = db->m_free_space;
        size_t num_objects = m_total_rows;
        size_t num_available_versions = static_cast<size_t>(db->get_number_of_versions());
        size_t num_decrypted_pages = realm::util::get_num_decrypted_pages();

        if (stage == DB::transact_Reading) {
            if (m_transact_stage == DB::transact_Writing) {
                m_metrics->end_write_transaction(total_size, free_space, num_objects, num_available_versions,
                                                 num_decrypted_pages);
            }
            m_metrics->start_read_transaction();
        }
        else if (stage == DB::transact_Writing) {
            if (m_transact_stage == DB::transact_Reading) {
                m_metrics->end_read_transaction(total_size, free_space, num_objects, num_available_versions,
                                                num_decrypted_pages);
            }
            m_metrics->start_write_transaction();
        }
        else if (stage == DB::transact_Ready) {
            m_metrics->end_read_transaction(total_size, free_space, num_objects, num_available_versions,
                                            num_decrypted_pages);
            m_metrics->end_write_transaction(total_size, free_space, num_objects, num_available_versions,
                                             num_decrypted_pages);
        }
    }
#endif

    m_transact_stage = stage;
}

void DB::upgrade_file_format(bool allow_file_format_upgrade, int target_file_format_version,
                             int current_hist_schema_version, int target_hist_schema_version)
{
    // In a multithreaded scenario multiple threads may initially see a need to
    // upgrade (maybe_upgrade == true) even though one onw thread is supposed to
    // perform the upgrade, but that is ok, because the condition is rechecked
    // in a fully reliable way inside a transaction.

    // First a non-threadsafe but fast check
    int current_file_format_version = m_file_format_version;
    REALM_ASSERT(current_file_format_version <= target_file_format_version);
    REALM_ASSERT(current_hist_schema_version <= target_hist_schema_version);
    bool maybe_upgrade_file_format = (current_file_format_version < target_file_format_version);
    bool maybe_upgrade_hist_schema = (current_hist_schema_version < target_hist_schema_version);
    bool maybe_upgrade = maybe_upgrade_file_format || maybe_upgrade_hist_schema;
    if (maybe_upgrade) {

#ifdef REALM_DEBUG
// This sleep() only exists in order to increase the quality of the
// TEST(Upgrade_Database_2_3_Writes_New_File_Format_new) unit test.
// The unit test creates multiple threads that all call
// upgrade_file_format() simultaneously. This sleep() then acts like
// a simple thread barrier that makes sure the threads meet here, to
// increase the likelyhood of detecting any potential race problems.
// See the unit test for details.
//
// NOTE: This sleep has been disabled because no problems have been found with
// this code in a long while, and it was dramatically slowing down a unit test
// in realm-sync.

// millisleep(200);
#endif

        // WriteTransaction wt(*this);
        auto wt = start_write();
        bool dirty = false;

        // We need to upgrade history first. We may need to access it during migration
        // when processing the !OID columns
        int current_hist_schema_version_2 = wt->get_history_schema_version();
        // The history must either still be using its initial schema or have
        // been upgraded already to the chosen target schema version via a
        // concurrent DB object.
        REALM_ASSERT(current_hist_schema_version_2 == current_hist_schema_version ||
                     current_hist_schema_version_2 == target_hist_schema_version);
        bool need_hist_schema_upgrade = (current_hist_schema_version_2 < target_hist_schema_version);
        if (need_hist_schema_upgrade) {
            if (!allow_file_format_upgrade)
                throw FileFormatUpgradeRequired("Database upgrade required but prohibited", this->m_db_path);

            Replication* repl = get_replication();
            repl->upgrade_history_schema(current_hist_schema_version_2); // Throws
            wt->set_history_schema_version(target_hist_schema_version);  // Throws
            dirty = true;
        }

        // File format upgrade
        int current_file_format_version_2 = m_alloc.get_committed_file_format_version();
        // The file must either still be using its initial file_format or have
        // been upgraded already to the chosen target file format via a
        // concurrent DB object.
        REALM_ASSERT(current_file_format_version_2 == current_file_format_version ||
                     current_file_format_version_2 == target_file_format_version);
        bool need_file_format_upgrade = (current_file_format_version_2 < target_file_format_version);
        if (need_file_format_upgrade) {
            if (!allow_file_format_upgrade)
                throw FileFormatUpgradeRequired("Database upgrade required but prohibited", this->m_db_path);
            wt->upgrade_file_format(target_file_format_version); // Throws
            // Note: The file format version stored in the Realm file will be
            // updated to the new file format version as part of the following
            // commit operation. This happens in GroupWriter::commit().
            if (m_upgrade_callback)
                m_upgrade_callback(current_file_format_version_2, target_file_format_version); // Throws
            dirty = true;
        }
        wt->set_file_format_version(target_file_format_version);
        m_file_format_version = target_file_format_version;

        if (dirty)
            wt->commit(); // Throws
    }
}


void DB::release_read_lock(ReadLockInfo& read_lock) noexcept
{
    // ignore if opened with immutable file (then we have no lockfile)
    if (m_fake_read_lock_if_immutable)
        return;
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    bool found_match = false;
    // simple linear search and move-last-over if a match is found.
    // common case should have only a modest number of transactions in play..
    for (size_t j = 0; j < m_local_locks_held.size(); ++j) {
        if (m_local_locks_held[j].m_version == read_lock.m_version) {
            m_local_locks_held[j] = m_local_locks_held.back();
            m_local_locks_held.pop_back();
            found_match = true;
            break;
        }
    }
    if (!found_match) {
        REALM_ASSERT(!is_attached());
        // it's OK, someone called close() and all locks where released
        return;
    }
    --m_transaction_count;
    SharedInfo* r_info = m_reader_map.get_addr();
    const Ringbuffer::ReadCount& r = r_info->readers.get(read_lock.m_reader_idx);
    atomic_double_dec(r.count); // <-- most of the exec time spent here
}


void DB::grab_read_lock(ReadLockInfo& read_lock, VersionID version_id)
{
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    REALM_ASSERT_RELEASE(is_attached());
    if (version_id.version == std::numeric_limits<version_type>::max()) {
        for (;;) {
            SharedInfo* r_info = m_reader_map.get_addr();
            read_lock.m_reader_idx = r_info->readers.last();
            if (grow_reader_mapping(read_lock.m_reader_idx)) { // Throws
                // remapping takes time, so retry with a fresh entry
                continue;
            }
            r_info = m_reader_map.get_addr();
            const Ringbuffer::ReadCount& r = r_info->readers.get(read_lock.m_reader_idx);
            // if the entry is stale and has been cleared by the cleanup process,
            // we need to start all over again. This is extremely unlikely, but possible.
            if (!atomic_double_inc_if_even(r.count)) // <-- most of the exec time spent here!
                continue;
            read_lock.m_version = r.version;
            read_lock.m_top_ref = to_size_t(r.current_top);
            read_lock.m_file_size = to_size_t(r.filesize);
            m_local_locks_held.emplace_back(read_lock);
            ++m_transaction_count;
            // REALM_ASSERT(m_alloc.matches_section_boundary(read_lock.m_file_size));
            REALM_ASSERT(read_lock.m_file_size > read_lock.m_top_ref);
            return;
        }
    }

    for (;;) {
        SharedInfo* r_info = m_reader_map.get_addr();
        read_lock.m_reader_idx = version_id.index;
        if (grow_reader_mapping(read_lock.m_reader_idx)) { // Throws
            // remapping takes time, so retry with a fresh entry
            continue;
        }
        r_info = m_reader_map.get_addr();
        const Ringbuffer::ReadCount& r = r_info->readers.get(read_lock.m_reader_idx);

        // if the entry is stale and has been cleared by the cleanup process,
        // the requested version is no longer available
        while (!atomic_double_inc_if_even(r.count)) { // <-- most of the exec time spent here!
            // we failed to lock the version. This could be because the version
            // is being cleaned up, but also because the cleanup is probing for access
            // to it. If it's being probed, the tail ptr of the ringbuffer will point
            // to it. If so we retry. If the tail ptr points somewhere else, the entry
            // has been cleaned up.
            if (&r_info->readers.get_oldest() != &r)
                throw BadVersion();
        }
        // we managed to lock an entry in the ringbuffer, but it may be so old that
        // the version doesn't match the specific request. In that case we must release and fail
        if (r.version != version_id.version) {
            atomic_double_dec(r.count); // <-- release
            throw BadVersion();
        }
        read_lock.m_version = r.version;
        read_lock.m_top_ref = to_size_t(r.current_top);
        read_lock.m_file_size = to_size_t(r.filesize);
        m_local_locks_held.emplace_back(read_lock);
        ++m_transaction_count;
        // REALM_ASSERT(m_alloc.matches_section_boundary(read_lock.m_file_size));
        REALM_ASSERT(read_lock.m_file_size > read_lock.m_top_ref);
        return;
    }
}

void DB::leak_read_lock(ReadLockInfo& read_lock) noexcept
{
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    // simple linear search and move-last-over if a match is found.
    // common case should have only a modest number of transactions in play..
    for (size_t j = 0; j < m_local_locks_held.size(); ++j) {
        if (m_local_locks_held[j].m_version == read_lock.m_version) {
            m_local_locks_held[j] = m_local_locks_held.back();
            m_local_locks_held.pop_back();
            --m_transaction_count;
            return;
        }
    }
}

bool DB::do_try_begin_write()
{
    // In the non-blocking case, we will only succeed if there is no contention for
    // the write mutex. For this case we are trivially fair and can ignore the
    // fairness machinery.
    bool got_the_lock = m_writemutex.try_lock();
    if (got_the_lock) {
        finish_begin_write();
    }
    return got_the_lock;
}

void DB::do_begin_write()
{
    SharedInfo* info = m_file_map.get_addr();

    // Get write lock - the write lock is held until do_end_write().
    //
    // We use a ticketing scheme to ensure fairness wrt performing write transactions.
    // (But cannot do that on Windows until we have interprocess condition variables there)
    uint32_t my_ticket = info->next_ticket.fetch_add(1, std::memory_order_relaxed);
    m_writemutex.lock(); // Throws

    // allow for comparison even after wrap around of ticket numbering:
    int32_t diff = int32_t(my_ticket - info->next_served.load(std::memory_order_relaxed));
    bool should_yield = diff > 0; // ticket is in the future
    // a) the above comparison is only guaranteed to be correct, if the distance
    //    between my_ticket and info->next_served is less than 2^30. This will
    //    be the case since the distance will be bounded by the number of threads
    //    and each thread cannot ever hold more than one ticket.
    // b) we could use 64 bit counters instead, but it is unclear if all platforms
    //    have support for interprocess atomics for 64 bit values.

    timespec time_limit; // only compute the time limit if we're going to use it:
    if (should_yield) {
        // This clock is not monotonic, so time can move backwards. This can lead
        // to a wrong time limit, but the only effect of a wrong time limit is that
        // we momentarily lose fairness, so we accept it.
        timeval tv;
        gettimeofday(&tv, nullptr);
        time_limit.tv_sec = tv.tv_sec;
        time_limit.tv_nsec = tv.tv_usec * 1000;
        time_limit.tv_nsec += 500000000;        // 500 msec wait
        if (time_limit.tv_nsec >= 1000000000) { // overflow
            time_limit.tv_nsec -= 1000000000;
            time_limit.tv_sec += 1;
        }
    }

    while (should_yield) {

        m_pick_next_writer.wait(m_writemutex, &time_limit);
        timeval tv;
        gettimeofday(&tv, nullptr);
        if (time_limit.tv_sec < tv.tv_sec ||
            (time_limit.tv_sec == tv.tv_sec && time_limit.tv_nsec < tv.tv_usec * 1000)) {
            // Timeout!
            break;
        }
        diff = int32_t(my_ticket - info->next_served);
        should_yield = diff > 0; // ticket is in the future, so yield to someone else
    }

    // we may get here because a) it's our turn, b) we timed out
    // we don't distinguish, satisfied that event b) should be rare.
    // In case b), we have to *make* it our turn. Failure to do so could leave us
    // with 'next_served' permanently trailing 'next_ticket'.
    //
    // In doing so, we may bypass other waiters, hence the condition for yielding
    // should take this situation into account by comparing with '>' instead of '!='
    info->next_served = my_ticket;
    finish_begin_write();
}

void DB::finish_begin_write()
{
    SharedInfo* info = m_file_map.get_addr();
    if (info->commit_in_critical_phase) {
        m_writemutex.unlock();
        throw std::runtime_error("Crash of other process detected, session restart required");
    }


    {
        std::lock_guard local_lock(m_mutex);
        m_write_transaction_open = true;
    }
    m_alloc.set_read_only(false);
}

void DB::do_end_write() noexcept
{
    SharedInfo* info = m_file_map.get_addr();
    info->next_served.fetch_add(1, std::memory_order_relaxed);

    {
        std::lock_guard<std::recursive_mutex> local_lock(m_mutex);
        REALM_ASSERT(m_write_transaction_open);
        m_alloc.set_read_only(true);
        m_write_transaction_open = false;
        m_writemutex.unlock();
    }
    m_pick_next_writer.notify_all();
}


Replication::version_type DB::do_commit(Transaction& transaction, bool commit_to_disk)
{
    version_type current_version;
    {
        std::lock_guard<std::recursive_mutex> lock(m_mutex);
        SharedInfo* r_info = m_reader_map.get_addr();
        current_version = r_info->get_current_version_unchecked();
    }
    version_type new_version = current_version + 1;

    if (Replication* repl = get_replication()) {
        // If Replication::prepare_commit() fails, then the entire transaction
        // fails. The application then has the option of terminating the
        // transaction with a call to Transaction::Rollback(), which in turn
        // must call Replication::abort_transact().
        new_version = repl->prepare_commit(current_version); // Throws
        low_level_commit(new_version, transaction, commit_to_disk); // Throws
        repl->finalize_commit();
    }
    else {
        low_level_commit(new_version, transaction); // Throws
    }
    return new_version;
}


VersionID Transaction::commit_and_continue_as_read(bool commit_to_disk)
{
    if (!is_attached())
        throw LogicError(LogicError::wrong_transact_state);
    if (m_transact_stage != DB::transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);

    flush_accessors_for_commit();

    DB::version_type version = db->do_commit(*this, commit_to_disk); // Throws

    // advance read lock but dont update accessors:
    // As this is done under lock, along with the addition above of the newest commit,
    // we know for certain that the read lock we will grab WILL refer to our own newly
    // completed commit.

    DB::ReadLockInfo new_read_lock;
    VersionID version_id = VersionID(); // Latest available snapshot
    // Grabbing the new lock before releasing the old one prevents m_transaction_count
    // from going shortly to zero
    db->grab_read_lock(new_read_lock, version_id); // Throws

    if (commit_to_disk || m_oldest_version_not_persisted) {
        // Here we are either committing to disk or we are already
        // holding on to an older version. In either case there is
        // no need to hold onto this now historic version.
        db->release_read_lock(m_read_lock);
    }
    else {
        // We are not commiting to disk and there is no older
        // version not persisted, so hold onto this one
        m_oldest_version_not_persisted = m_read_lock;
    }

    if (commit_to_disk && m_oldest_version_not_persisted) {
        // We are committing to disk so we can release the
        // version we are holding on to
        db->release_read_lock(*m_oldest_version_not_persisted);
        m_oldest_version_not_persisted.reset();
    }
    m_read_lock = new_read_lock;
    // We can be sure that m_read_lock != m_oldest_version_not_persisted
    // because m_oldest_version_not_persisted is either equal to former m_read_lock
    // or older and former m_read_lock is older than current m_read_lock
    REALM_ASSERT(!m_oldest_version_not_persisted ||
                 m_read_lock.m_version != m_oldest_version_not_persisted->m_version);

    {
        util::CheckedLockGuard lock(m_async_mutex);
        REALM_ASSERT(m_async_stage != AsyncState::Syncing);
        if (commit_to_disk) {
            if (m_async_stage == AsyncState::Requesting) {
                m_async_stage = AsyncState::HasLock;
            }
            else {
                db->end_write_on_correct_thread();
                m_async_stage = AsyncState::Idle;
            }
        }
        else {
            m_async_stage = AsyncState::HasCommits;
        }
    }

    // Remap file if it has grown, and update refs in underlying node structure
    remap_and_update_refs(m_read_lock.m_top_ref, m_read_lock.m_file_size, false); // Throws

    m_history = nullptr;
    set_transact_stage(DB::transact_Reading);

    return VersionID{version, new_read_lock.m_reader_idx};
}

// Caller must lock m_mutex.
bool DB::grow_reader_mapping(uint_fast32_t index)
{
    using _impl::SimulatedFailure;
    SimulatedFailure::trigger(SimulatedFailure::shared_group__grow_reader_mapping); // Throws

    if (index >= m_local_max_entry) {
        // handle mapping expansion if required
        SharedInfo* r_info = m_reader_map.get_addr();
        m_local_max_entry = r_info->readers.get_num_entries();
        REALM_ASSERT(index < m_local_max_entry);
        size_t info_size = sizeof(SharedInfo) + r_info->readers.compute_required_space(m_local_max_entry);
        // std::cout << "Growing reader mapping to " << infosize << std::endl;
        m_reader_map.remap(m_file, util::File::access_ReadWrite, info_size); // Throws
        return true;
    }
    return false;
}


VersionID DB::get_version_id_of_latest_snapshot()
{
    if (m_fake_read_lock_if_immutable)
        return {m_fake_read_lock_if_immutable->m_version, 0};
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    // As get_version_of_latest_snapshot() may be called outside of the write
    // mutex, another thread may be performing changes to the ringbuffer
    // concurrently. It may even cleanup and recycle the current entry from
    // under our feet, so we need to protect the entry by temporarily
    // incrementing the reader ref count until we've got a safe reading of the
    // version number.
    while (1) {
        uint_fast32_t index;
        SharedInfo* r_info;
        do {
            // make sure that the index we are about to dereference falls within
            // the portion of the ringbuffer that we have mapped - if not, extend
            // the mapping to fit.
            r_info = m_reader_map.get_addr();
            index = r_info->readers.last();
        } while (grow_reader_mapping(index)); // throws

        // now (double) increment the read count so that no-one cleans up the entry
        // while we read it.
        const Ringbuffer::ReadCount& r = r_info->readers.get(index);
        if (!atomic_double_inc_if_even(r.count)) {
            continue;
        }
        VersionID version{r.version, index};
        // release the entry again:
        atomic_double_dec(r.count);
        return version;
    }
}


DB::version_type DB::get_version_of_latest_snapshot()
{
    return get_version_id_of_latest_snapshot().version;
}


void DB::low_level_commit(uint_fast64_t new_version, Transaction& transaction, bool commit_to_disk)
{
    SharedInfo* info = m_file_map.get_addr();

    // Version of oldest snapshot currently (or recently) bound in a transaction
    // of the current session.
    uint_fast64_t oldest_version;
    {
        std::lock_guard<std::recursive_mutex> lock(m_mutex);
        SharedInfo* r_info = m_reader_map.get_addr();

        // the cleanup process may access the entire ring buffer, so make sure it is mapped.
        // this is not ensured as part of begin_read, which only makes sure that the current
        // last entry in the buffer is available. (Last entry is a get_num_entries() - 1).
        if (grow_reader_mapping(r_info->readers.get_num_entries() - 1)) { // throws
            r_info = m_reader_map.get_addr();
        }
        r_info->readers.cleanup();
        const Ringbuffer::ReadCount& rc = r_info->readers.get_oldest();
        oldest_version = rc.version;

        // Allow for trimming of the history. Some types of histories do not
        // need store changesets prior to the oldest bound snapshot.
        if (auto hist = transaction.get_history())
            hist->set_oldest_bound_version(oldest_version); // Throws

        // Cleanup any stale mappings
        m_alloc.purge_old_mappings(oldest_version, new_version);
    }

    // Do the actual commit
    REALM_ASSERT(oldest_version <= new_version);
#if REALM_METRICS
    transaction.update_num_objects();
#endif // REALM_METRICS

    // info->readers.dump();
    GroupWriter out(transaction, Durability(info->durability)); // Throws
    out.set_versions(new_version, oldest_version);
    ref_type new_top_ref;
    // Recursively write all changed arrays to end of file
    {
        // protect against race with any other DB trying to attach to the file
        std::lock_guard<InterprocessMutex> lock(m_controlmutex); // Throws
        new_top_ref = out.write_group();                         // Throws
    }
    {
        // protect access to shared variables and m_reader_mapping from here
        std::lock_guard<std::recursive_mutex> lock_guard(m_mutex);
        m_free_space = out.get_free_space_size();
        m_locked_space = out.get_locked_space_size();
        m_used_space = out.get_file_size() - m_free_space;
        // std::cout << "Writing version " << new_version << ", Topptr " << new_top_ref
        //     << " Read lock at version " << oldest_version << std::endl;
        switch (Durability(info->durability)) {
            case Durability::Full:
            case Durability::Unsafe:
                if (commit_to_disk)
                    out.commit(new_top_ref); // Throws
                break;
            case Durability::MemOnly:
                // In Durability::MemOnly mode, we just use the file as backing for
                // the shared memory. So we never actually flush the data to disk
                // (the OS may do so opportinisticly, or when swapping). So in this
                // mode the file on disk may very likely be in an invalid state.
                break;
        }
        size_t new_file_size = out.get_file_size();
        // We must reset the allocators free space tracking before communicating the new
        // version through the ring buffer. If not, a reader may start updating the allocators
        // mappings while the allocator is in dirty state.
        reset_free_space_tracking();
        // Update reader info. If this fails in any way, the ringbuffer may be corrupted.
        // This can lead to other readers seing invalid data which is likely to cause them
        // to crash. Other writers *must* be prevented from writing any further updates
        // to the database. The flag "commit_in_critical_phase" is used to prevent such updates.
        info->commit_in_critical_phase = 1;
        {
            SharedInfo* r_info = m_reader_map.get_addr();
            if (r_info->readers.is_full()) {
                // buffer expansion
                uint_fast32_t entries = r_info->readers.get_num_entries();
                entries = entries + 32;
                size_t new_info_size = sizeof(SharedInfo) + r_info->readers.compute_required_space(entries);
                // std::cout << "resizing: " << entries << " = " << new_info_size << std::endl;
                m_file.prealloc(new_info_size);                                          // Throws
                m_reader_map.remap(m_file, util::File::access_ReadWrite, new_info_size); // Throws
                r_info = m_reader_map.get_addr();
                m_local_max_entry = entries;
                r_info->readers.expand_to(entries);
            }
            Ringbuffer::ReadCount& r = r_info->readers.get_next();
            r.current_top = new_top_ref;
            r.filesize = new_file_size;
            r.version = new_version;
            r_info->readers.use_next();

            // REALM_ASSERT(m_alloc.matches_section_boundary(new_file_size));
            REALM_ASSERT(new_top_ref < new_file_size);
        }
        // At this point, the ringbuffer has been succesfully updated, and the next writer
        // can safely proceed once the writemutex has been lifted.
        info->commit_in_critical_phase = 0;
    }
    {
        // protect against concurrent updates to the .lock file.
        // must release m_mutex before this point to obey lock order
        std::lock_guard<InterprocessMutex> lock(m_controlmutex);
        info->number_of_versions = new_version - oldest_version + 1;
        info->latest_version_number = new_version;

        m_new_commit_available.notify_all();
    }
}

#ifdef REALM_DEBUG
void DB::reserve(size_t size)
{
    REALM_ASSERT(is_attached());
    m_alloc.reserve_disk_space(size); // Throws
}
#endif

bool DB::call_with_lock(const std::string& realm_path, CallbackWithLock&& callback)
{
    auto lockfile_path = get_core_file(realm_path, CoreFileType::Lock);

    File lockfile;
    lockfile.open(lockfile_path, File::access_ReadWrite, File::create_Auto, 0); // Throws
    File::CloseGuard fcg(lockfile);
    lockfile.set_fifo_path(realm_path + ".management", "lock.fifo");
    if (lockfile.try_lock_exclusive()) { // Throws
        callback(realm_path);
        return true;
    }
    return false;
}

std::string DB::get_core_file(const std::string& base_path, CoreFileType type)
{
    switch (type) {
        case CoreFileType::Lock:
            return base_path + ".lock";
        case CoreFileType::Storage:
            return base_path;
        case CoreFileType::Management:
            return base_path + ".management";
        case CoreFileType::Note:
            return base_path + ".note";
        case CoreFileType::Log:
            return base_path + ".log";
    }
    REALM_UNREACHABLE();
}

void DB::delete_files(const std::string& base_path, bool* did_delete, bool delete_lockfile)
{
    if (File::try_remove(get_core_file(base_path, CoreFileType::Storage)) && did_delete) {
        *did_delete = true;
    }

    File::try_remove(get_core_file(base_path, CoreFileType::Note));
    File::try_remove(get_core_file(base_path, CoreFileType::Log));
    util::try_remove_dir_recursive(get_core_file(base_path, CoreFileType::Management));

    if (delete_lockfile) {
        File::try_remove(get_core_file(base_path, CoreFileType::Lock));
    }
}

TransactionRef DB::start_read(VersionID version_id)
{
    if (!is_attached())
        throw LogicError(LogicError::wrong_transact_state);
    TransactionRef tr;
    if (m_fake_read_lock_if_immutable) {
        tr = make_transaction_ref(shared_from_this(), &m_alloc, *m_fake_read_lock_if_immutable, DB::transact_Reading);
    }
    else {
        ReadLockInfo read_lock;
        grab_read_lock(read_lock, version_id);
        ReadLockGuard g(*this, read_lock);
        read_lock.check();
        tr = make_transaction_ref(shared_from_this(), &m_alloc, read_lock, DB::transact_Reading);
        g.release();
    }
    tr->set_file_format_version(get_file_format_version());
    return tr;
}

TransactionRef DB::start_frozen(VersionID version_id)
{
    if (!is_attached())
        throw LogicError(LogicError::wrong_transact_state);
    TransactionRef tr;
    if (m_fake_read_lock_if_immutable) {
        tr = make_transaction_ref(shared_from_this(), &m_alloc, *m_fake_read_lock_if_immutable, DB::transact_Frozen);
    }
    else {
        ReadLockInfo read_lock;
        grab_read_lock(read_lock, version_id);
        ReadLockGuard g(*this, read_lock);
        read_lock.check();
        tr = make_transaction_ref(shared_from_this(), &m_alloc, read_lock, DB::transact_Frozen);
        g.release();
    }
    tr->set_file_format_version(get_file_format_version());
    return tr;
}

Transaction::Transaction(DBRef _db, SlabAlloc* alloc, DB::ReadLockInfo& rli, DB::TransactStage stage)
    : Group(alloc)
    , db(_db)
    , m_read_lock(rli)
{
    bool writable = stage == DB::transact_Writing;
    m_transact_stage = DB::transact_Ready;
    set_metrics(db->m_metrics);
    set_transact_stage(stage);
    m_alloc.note_reader_start(this);
    attach_shared(m_read_lock.m_top_ref, m_read_lock.m_file_size, writable);
}

void Transaction::close()
{
    if (m_transact_stage == DB::transact_Writing) {
        rollback();
    }
    if (m_transact_stage == DB::transact_Reading || m_transact_stage == DB::transact_Frozen) {
        do_end_read();
    }
}

void Transaction::end_read()
{
    if (m_transact_stage == DB::transact_Ready)
        return;
    if (m_transact_stage == DB::transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);
    do_end_read();
}

void Transaction::do_end_read() noexcept
{
    prepare_for_close();
    detach();

    // We should always be ensuring that async commits finish before we get here,
    // but if the fsync() failed or we failed to update the top pointer then
    // there's not much we can do and we have to just accept that we're losing
    // those commits.
    if (m_oldest_version_not_persisted) {
        REALM_ASSERT(m_async_commit_has_failed);
        // We need to not release our read lock on m_oldest_version_not_persisted
        // as that's the version the top pointer is referencing and overwriting
        // that version will corrupt the Realm file.
        db->leak_read_lock(*m_oldest_version_not_persisted);
    }
    db->release_read_lock(m_read_lock);

    m_alloc.note_reader_end(this);
    set_transact_stage(DB::transact_Ready);
    // reset the std::shared_ptr to allow the DB object to release resources
    // as early as possible.
    db.reset();
}

TransactionRef Transaction::freeze()
{
    if (m_transact_stage != DB::transact_Reading)
        throw LogicError(LogicError::wrong_transact_state);
    auto version = VersionID(m_read_lock.m_version, m_read_lock.m_reader_idx);
    return db->start_frozen(version);
}

TransactionRef Transaction::duplicate()
{
    auto version = VersionID(m_read_lock.m_version, m_read_lock.m_reader_idx);
    if (m_transact_stage == DB::transact_Reading)
        return db->start_read(version);
    if (m_transact_stage == DB::transact_Frozen)
        return db->start_frozen(version);

    throw LogicError(LogicError::wrong_transact_state);
}

_impl::History* Transaction::get_history() const
{
    if (!m_history) {
        if (auto repl = db->get_replication()) {
            switch (m_transact_stage) {
                case DB::transact_Reading:
                case DB::transact_Frozen:
                    if (!m_history_read)
                        m_history_read = repl->_create_history_read();
                    m_history = m_history_read.get();
                    m_history->set_group(const_cast<Transaction*>(this), false);
                    break;
                case DB::transact_Writing:
                    m_history = repl->_get_history_write();
                    break;
                case DB::transact_Ready:
                    break;
            }
        }
    }
    return m_history;
}

void Transaction::rollback()
{
    // rollback may happen as a consequence of exception handling in cases where
    // the DB has detached. If so, just back out without trying to change state.
    // the DB object has already been closed and no further processing is possible.
    if (!is_attached())
        return;
    if (m_transact_stage == DB::transact_Ready)
        return; // Idempotency

    if (m_transact_stage != DB::transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);
    db->reset_free_space_tracking();
    if (!holds_write_mutex())
        db->end_write_on_correct_thread();

    do_end_read();
}

size_t Transaction::get_commit_size() const
{
    size_t sz = 0;
    if (m_transact_stage == DB::transact_Writing) {
        sz = m_alloc.get_commit_size();
    }
    return sz;
}

DB::version_type Transaction::commit()
{
    if (!is_attached())
        throw LogicError(LogicError::wrong_transact_state);
    if (m_transact_stage != DB::transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);

    REALM_ASSERT(is_attached());

    // before committing, allow any accessors at group level or below to sync
    flush_accessors_for_commit();

    DB::version_type new_version = db->do_commit(*this); // Throws

    // We need to set m_read_lock in order for wait_for_change to work.
    // To set it, we grab a readlock on the latest available snapshot
    // and release it again.
    VersionID version_id = VersionID(); // Latest available snapshot
    DB::ReadLockInfo lock_after_commit;
    db->grab_read_lock(lock_after_commit, version_id);
    db->release_read_lock(lock_after_commit);

    db->end_write_on_correct_thread();

    do_end_read();
    m_read_lock = lock_after_commit;

    return new_version;
}

void Transaction::commit_and_continue_writing()
{
    if (!is_attached())
        throw LogicError(LogicError::wrong_transact_state);
    if (m_transact_stage != DB::transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);

    REALM_ASSERT(is_attached());

    // before committing, allow any accessors at group level or below to sync
    flush_accessors_for_commit();

    db->do_commit(*this); // Throws

    // We need to set m_read_lock in order for wait_for_change to work.
    // To set it, we grab a readlock on the latest available snapshot
    // and release it again.
    VersionID version_id = VersionID(); // Latest available snapshot
    DB::ReadLockInfo lock_after_commit;
    db->grab_read_lock(lock_after_commit, version_id);
    db->release_read_lock(m_read_lock);
    m_read_lock = lock_after_commit;
    if (Replication* repl = db->get_replication()) {
        bool history_updated = false;
        repl->initiate_transact(*this, lock_after_commit.m_version, history_updated); // Throws
    }

    bool writable = true;
    remap_and_update_refs(m_read_lock.m_top_ref, m_read_lock.m_file_size, writable); // Throws
}

void Transaction::initialize_replication()
{
    if (m_transact_stage == DB::transact_Writing) {
        if (Replication* repl = get_replication()) {
            auto current_version = m_read_lock.m_version;
            bool history_updated = false;
            repl->initiate_transact(*this, current_version, history_updated); // Throws
        }
    }
}

Transaction::~Transaction()
{
    // Note that this does not call close() - calling close() is done
    // implicitly by the deleter.
}


TransactionRef DB::start_write(bool nonblocking)
{
    if (m_fake_read_lock_if_immutable) {
        REALM_ASSERT(false && "Can't write an immutable DB");
    }
    if (nonblocking) {
        bool success = do_try_begin_write();
        if (!success) {
            return TransactionRef();
        }
    }
    else {
        do_begin_write();
    }
    {
        std::lock_guard<std::recursive_mutex> local_lock(m_mutex);
        if (!is_attached()) {
            end_write_on_correct_thread();
            throw LogicError(LogicError::wrong_transact_state);
        }
        m_write_transaction_open = true;
    }
    ReadLockInfo read_lock;
    TransactionRef tr;
    try {
        grab_read_lock(read_lock, VersionID());
        ReadLockGuard g(*this, read_lock);
        read_lock.check();
        tr = make_transaction_ref(shared_from_this(), &m_alloc, read_lock, DB::transact_Writing);
        tr->set_file_format_version(get_file_format_version());
        version_type current_version = read_lock.m_version;
        m_alloc.init_mapping_management(current_version);
        if (Replication* repl = get_replication()) {
            bool history_updated = false;
            repl->initiate_transact(*tr, current_version, history_updated); // Throws
        }
        g.release();
    }
    catch (...) {
        end_write_on_correct_thread();
        throw;
    }

    return tr;
}

void DB::async_request_write_mutex(TransactionRef& tr, util::UniqueFunction<void()>&& when_acquired)
{
    {
        util::CheckedLockGuard lck(tr->m_async_mutex);
        REALM_ASSERT(tr->m_async_stage == Transaction::AsyncState::Idle);
        tr->m_async_stage = Transaction::AsyncState::Requesting;
    }
    std::weak_ptr<Transaction> weak_tr = tr;
    async_begin_write([weak_tr, cb = std::move(when_acquired)]() {
        if (auto tr = weak_tr.lock()) {
            util::CheckedLockGuard lck(tr->m_async_mutex);
            // If a synchronous transaction happened while we were pending
            // we may be in HasCommits
            if (tr->m_async_stage == Transaction::AsyncState::Requesting) {
                tr->m_async_stage = Transaction::AsyncState::HasLock;
            }
            if (tr->m_waiting_for_write_lock) {
                tr->m_waiting_for_write_lock = false;
                tr->m_async_cv.notify_one();
            }
            else if (cb) {
                cb();
            }
            tr.reset(); // Release pointer while lock is held
        }
    });
}

Obj Transaction::import_copy_of(const Obj& original)
{
    if (bool(original) && original.is_valid()) {
        TableKey tk = original.get_table_key();
        ObjKey rk = original.get_key();
        auto table = get_table(tk);
        if (table->is_valid(rk))
            return table->get_object(rk);
    }
    return {};
}

TableRef Transaction::import_copy_of(ConstTableRef original)
{
    TableKey tk = original->get_key();
    return get_table(tk);
}

LnkLst Transaction::import_copy_of(const LnkLst& original)
{
    if (Obj obj = import_copy_of(original.get_obj())) {
        ColKey ck = original.get_col_key();
        return obj.get_linklist(ck);
    }
    return LnkLst();
}

LstBasePtr Transaction::import_copy_of(const LstBase& original)
{
    if (Obj obj = import_copy_of(original.get_obj())) {
        ColKey ck = original.get_col_key();
        return obj.get_listbase_ptr(ck);
    }
    return {};
}

SetBasePtr Transaction::import_copy_of(const SetBase& original)
{
    if (Obj obj = import_copy_of(original.get_obj())) {
        ColKey ck = original.get_col_key();
        return obj.get_setbase_ptr(ck);
    }
    return {};
}

CollectionBasePtr Transaction::import_copy_of(const CollectionBase& original)
{
    if (Obj obj = import_copy_of(original.get_obj())) {
        ColKey ck = original.get_col_key();
        return obj.get_collection_ptr(ck);
    }
    return {};
}

LnkLstPtr Transaction::import_copy_of(const LnkLstPtr& original)
{
    if (!bool(original))
        return nullptr;
    if (Obj obj = import_copy_of(original->get_obj())) {
        ColKey ck = original->get_col_key();
        return obj.get_linklist_ptr(ck);
    }
    return std::make_unique<LnkLst>();
}

LnkSetPtr Transaction::import_copy_of(const LnkSetPtr& original)
{
    if (!original)
        return nullptr;
    if (Obj obj = import_copy_of(original->get_obj())) {
        ColKey ck = original->get_col_key();
        return obj.get_linkset_ptr(ck);
    }
    return std::make_unique<LnkSet>();
}

LinkCollectionPtr Transaction::import_copy_of(const LinkCollectionPtr& original)
{
    if (!original)
        return nullptr;
    if (Obj obj = import_copy_of(original->get_owning_obj())) {
        ColKey ck = original->get_owning_col_key();
        return obj.get_linkcollection_ptr(ck);
    }
    // return some empty collection where size() == 0
    // the type shouldn't matter
    return std::make_unique<LnkLst>();
}

std::unique_ptr<Query> Transaction::import_copy_of(Query& query, PayloadPolicy policy)
{
    return query.clone_for_handover(this, policy);
}

std::unique_ptr<TableView> Transaction::import_copy_of(TableView& tv, PayloadPolicy policy)
{
    return tv.clone_for_handover(this, policy);
}

inline DB::DB(const DBOptions& options)
    : m_key(options.encryption_key)
    , m_upgrade_callback(std::move(options.upgrade_callback))
{
    if (options.enable_async_writes) {
        m_commit_helper = std::make_unique<AsyncCommitHelper>(this);
    }
}

namespace {
class DBInit : public DB {
public:
    explicit DBInit(const DBOptions& options)
        : DB(options)
    {
    }
};
} // namespace

DBRef DB::create(const std::string& file, bool no_create, const DBOptions options)
{
    DBRef retval = std::make_shared<DBInit>(options);
    retval->open(file, no_create, options);
    return retval;
}

DBRef DB::create(Replication& repl, const std::string& file, const DBOptions options)
{
    DBRef retval = std::make_shared<DBInit>(options);
    retval->open(repl, file, options);
    return retval;
}

DBRef DB::create(std::unique_ptr<Replication> repl, const std::string& file, const DBOptions options)
{
    REALM_ASSERT(repl);
    DBRef retval = std::make_shared<DBInit>(options);
    retval->m_history = std::move(repl);
    retval->open(*retval->m_history, file, options);
    return retval;
}

DBRef DB::create(BinaryData buffer, bool take_ownership)
{
    DBOptions options;
    options.is_immutable = true;
    DBRef retval = std::make_shared<DBInit>(options);
    retval->open(buffer, take_ownership);
    return retval;
}

void DB::claim_sync_agent()
{
    REALM_ASSERT(is_attached());
    std::unique_lock<InterprocessMutex> lock(m_controlmutex);
    SharedInfo* info = m_file_map.get_addr();
    if (info->sync_agent_present)
        throw MultipleSyncAgents{};
    info->sync_agent_present = 1; // Set to true
    m_is_sync_agent = true;
}

void DB::release_sync_agent()
{
    REALM_ASSERT(is_attached());
    std::unique_lock<InterprocessMutex> lock(m_controlmutex);
    if (!m_is_sync_agent)
        return;
    SharedInfo* info = m_file_map.get_addr();
    REALM_ASSERT(info->sync_agent_present);
    info->sync_agent_present = 0;
    m_is_sync_agent = false;
}

void DB::do_begin_possibly_async_write()
{
    if (m_commit_helper) {
        m_commit_helper->blocking_begin_write();
    }
    else {
        do_begin_write();
    }
}

void DB::end_write_on_correct_thread() noexcept
{
    //    m_local_write_mutex.unlock();
    if (!m_commit_helper || !m_commit_helper->blocking_end_write()) {
        do_end_write();
    }
}
void Transaction::promote_to_async()
{
    util::CheckedLockGuard lck(m_async_mutex);
    if (m_async_stage == AsyncState::Idle) {
        m_async_stage = AsyncState::HasLock;
    }
}

void Transaction::complete_async_commit()
{
    // sync to disk:
    DB::ReadLockInfo read_lock;
    try {
        db->grab_read_lock(read_lock, VersionID());
        GroupWriter out(*this);
        out.commit(read_lock.m_top_ref); // Throws
        // we must release the write mutex before the callback, because the callback
        // is allowed to re-request it.
        db->release_read_lock(read_lock);
        if (m_oldest_version_not_persisted) {
            db->release_read_lock(*m_oldest_version_not_persisted);
            m_oldest_version_not_persisted.reset();
        }
    }
    catch (...) {
        m_commit_exception = std::current_exception();
        m_async_commit_has_failed = true;
        db->release_read_lock(read_lock);
    }
}

void Transaction::async_complete_writes(util::UniqueFunction<void()> when_synchronized)
{
    util::CheckedLockGuard lck(m_async_mutex);
    if (m_async_stage == AsyncState::HasLock) {
        // Nothing to commit to disk - just release write lock
        m_async_stage = AsyncState::Idle;
        db->async_end_write();
    }
    else if (m_async_stage == AsyncState::HasCommits) {
        m_async_stage = AsyncState::Syncing;
        m_commit_exception = std::exception_ptr();
        // get a callback on the helper thread, in which to sync to disk
        db->async_sync_to_disk([this, cb = std::move(when_synchronized)]() noexcept {
            complete_async_commit();
            util::CheckedLockGuard lck(m_async_mutex);
            m_async_stage = AsyncState::Idle;
            if (m_waiting_for_sync) {
                m_waiting_for_sync = false;
                m_async_cv.notify_all();
            }
            else {
                cb();
            }
        });
    }
}

void Transaction::prepare_for_close()
{
    util::CheckedLockGuard lck(m_async_mutex);
    switch (m_async_stage) {
        case AsyncState::Idle:
            break;

        case AsyncState::Requesting:
            // We don't have the ability to cancel a wait on the write lock, so
            // unfortunately we have to wait for it to be acquired.
            REALM_ASSERT(m_transact_stage == DB::transact_Reading);
            REALM_ASSERT(!m_oldest_version_not_persisted);
            m_waiting_for_write_lock = true;
            m_async_cv.wait(lck.native_handle(), [this]() REQUIRES(m_async_mutex) {
                return !m_waiting_for_write_lock;
            });
            db->end_write_on_correct_thread();
            break;

        case AsyncState::HasLock:
            // We have the lock and are currently in a write transaction, and
            // also may have some pending previous commits to write
            if (m_transact_stage == DB::transact_Writing) {
                db->reset_free_space_tracking();
                m_transact_stage = DB::transact_Reading;
            }
            if (m_oldest_version_not_persisted) {
                complete_async_commit();
            }
            db->end_write_on_correct_thread();
            break;

        case AsyncState::HasCommits:
            // We have commits which need to be synced to disk, so do that
            REALM_ASSERT(m_transact_stage == DB::transact_Reading);
            complete_async_commit();
            db->end_write_on_correct_thread();
            break;

        case AsyncState::Syncing:
            // The worker thread is currently writing, so wait for it to complete
            REALM_ASSERT(m_transact_stage == DB::transact_Reading);
            m_waiting_for_sync = true;
            m_async_cv.wait(lck.native_handle(), [this]() REQUIRES(m_async_mutex) {
                return !m_waiting_for_sync;
            });
            break;
    }
    m_async_stage = AsyncState::Idle;
}

void Transaction::acquire_write_lock()
{
    util::CheckedUniqueLock lck(m_async_mutex);
    switch (m_async_stage) {
        case AsyncState::Idle:
            lck.unlock();
            db->do_begin_possibly_async_write();
            return;

        case AsyncState::Requesting:
            m_waiting_for_write_lock = true;
            m_async_cv.wait(lck.native_handle(), [this]() REQUIRES(m_async_mutex) {
                return !m_waiting_for_write_lock;
            });
            return;

        case AsyncState::HasLock:
        case AsyncState::HasCommits:
            return;

        case AsyncState::Syncing:
            m_waiting_for_sync = true;
            m_async_cv.wait(lck.native_handle(), [this]() REQUIRES(m_async_mutex) {
                return !m_waiting_for_sync;
            });
            lck.unlock();
            db->do_begin_possibly_async_write();
            break;
    }
}
