#include <realm/sync/noinst/server/server.hpp>

#include <realm/binary_data.hpp>
#include <realm/impl/simulated_failure.hpp>
#include <realm/object_id.hpp>
#include <realm/string_data.hpp>
#include <realm/sync/changeset.hpp>
#include <realm/sync/trigger.hpp>
#include <realm/sync/impl/clamped_hex_dump.hpp>
#include <realm/sync/impl/clock.hpp>
#include <realm/sync/network/http.hpp>
#include <realm/sync/network/network_ssl.hpp>
#include <realm/sync/network/websocket.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/protocol_codec.hpp>
#include <realm/sync/noinst/server/access_control.hpp>
#include <realm/sync/noinst/server/server_dir.hpp>
#include <realm/sync/noinst/server/server_file_access_cache.hpp>
#include <realm/sync/noinst/server/server_impl_base.hpp>
#include <realm/sync/transform.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/bind_ptr.hpp>
#include <realm/util/buffer_stream.hpp>
#include <realm/util/circular_buffer.hpp>
#include <realm/util/compression.hpp>
#include <realm/util/file.hpp>
#include <realm/util/json_parser.hpp>
#include <realm/util/memory_stream.hpp>
#include <realm/util/optional.hpp>
#include <realm/util/platform_info.hpp>
#include <realm/util/random.hpp>
#include <realm/util/safe_int_ops.hpp>
#include <realm/util/scope_exit.hpp>
#include <realm/util/scratch_allocator.hpp>
#include <realm/util/thread.hpp>
#include <realm/util/thread_exec_guard.hpp>
#include <realm/util/value_reset_guard.hpp>
#include <realm/version.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <locale>
#include <map>
#include <memory>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

// NOTE: The protocol specification is in `/doc/protocol.md`


// FIXME: Verify that session identifier spoofing cannot be used to get access
// to sessions belonging to other network conections in any way.
// FIXME: Seems that server must close connection with zero sessions after a
// certain timeout.


using namespace realm;
using namespace realm::sync;
using namespace realm::util;

// clang-format off
using ServerHistory         = _impl::ServerHistory;
using ServerProtocol        = _impl::ServerProtocol;
using ServerFileAccessCache = _impl::ServerFileAccessCache;
using ServerImplBase = _impl::ServerImplBase;

using IntegratableChangeset = ServerHistory::IntegratableChangeset;
using IntegratableChangesetList = ServerHistory::IntegratableChangesetList;
using IntegratableChangesets = ServerHistory::IntegratableChangesets;
using IntegrationResult = ServerHistory::IntegrationResult;
using BootstrapError = ServerHistory::BootstrapError;
using ExtendedIntegrationError = ServerHistory::ExtendedIntegrationError;
using ClientType = ServerHistory::ClientType;
using FileIdentAllocSlot = ServerHistory::FileIdentAllocSlot;
using FileIdentAllocSlots = ServerHistory::FileIdentAllocSlots;

using UploadChangeset = ServerProtocol::UploadChangeset;
// clang-format on


using UploadChangesets = std::vector<UploadChangeset>;

using EventLoopMetricsHandler = network::Service::EventLoopMetricsHandler;


static_assert(std::numeric_limits<session_ident_type>::digits >= 63, "Bad session identifier type");
static_assert(std::numeric_limits<file_ident_type>::digits >= 63, "Bad file identifier type");
static_assert(std::numeric_limits<version_type>::digits >= 63, "Bad version type");
static_assert(std::numeric_limits<timestamp_type>::digits >= 63, "Bad timestamp type");


namespace {

enum class SchedStatus { done = 0, pending, in_progress };

// Only used by the Sync Server to support older pbs sync clients (prior to protocol v8)
constexpr std::string_view get_old_pbs_websocket_protocol_prefix() noexcept
{
    return "com.mongodb.realm-sync/";
}

std::string short_token_fmt(const std::string& str, size_t cutoff = 30)
{
    if (str.size() > cutoff) {
        return "..." + str.substr(str.size() - cutoff);
    }
    else {
        return str;
    }
}


class HttpListHeaderValueParser {
public:
    HttpListHeaderValueParser(std::string_view string) noexcept
        : m_string{string}
    {
    }
    bool next(std::string_view& elem) noexcept
    {
        while (m_pos < m_string.size()) {
            size_type i = m_pos;
            size_type j = m_string.find(',', i);
            if (j != std::string_view::npos) {
                m_pos = j + 1;
            }
            else {
                j = m_string.size();
                m_pos = j;
            }

            // Exclude leading and trailing white space
            while (i < j && is_http_lws(m_string[i]))
                ++i;
            while (j > i && is_http_lws(m_string[j - 1]))
                --j;

            if (i != j) {
                elem = m_string.substr(i, j - i);
                return true;
            }
        }
        return false;
    }

private:
    using size_type = std::string_view::size_type;
    const std::string_view m_string;
    size_type m_pos = 0;
    static bool is_http_lws(char ch) noexcept
    {
        return (ch == '\t' || ch == '\n' || ch == '\r' || ch == ' ');
    }
};


using SteadyClock = std::conditional<std::chrono::high_resolution_clock::is_steady,
                                     std::chrono::high_resolution_clock, std::chrono::steady_clock>::type;
using SteadyTimePoint = SteadyClock::time_point;

SteadyTimePoint steady_clock_now() noexcept
{
    return SteadyClock::now();
}

milliseconds_type steady_duration(SteadyTimePoint start_time, SteadyTimePoint end_time = steady_clock_now()) noexcept
{
    auto duration = end_time - start_time;
    auto millis_duration = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    return milliseconds_type(millis_duration);
}


bool determine_try_again(ProtocolError error_code) noexcept
{
    return (error_code == ProtocolError::connection_closed);
}


class ServerFile;
class ServerImpl;
class HTTPConnection;
class SyncConnection;
class Session;


using Formatter = util::ResettableExpandableBufferOutputStream;
using OutputBuffer = util::ResettableExpandableBufferOutputStream;

using ProtocolVersionRange = std::pair<int, int>;

class MiscBuffers {
public:
    Formatter formatter;
    OutputBuffer download_message;

    using ProtocolVersionRanges = std::vector<ProtocolVersionRange>;
    ProtocolVersionRanges protocol_version_ranges;

    std::vector<char> compress;

    MiscBuffers()
    {
        formatter.imbue(std::locale::classic());
        download_message.imbue(std::locale::classic());
    }
};


struct DownloadCache {
    std::unique_ptr<char[]> body;
    std::size_t uncompressed_body_size;
    std::size_t compressed_body_size;
    bool body_is_compressed;
    version_type end_version;
    DownloadCursor download_progress;
    std::uint_fast64_t downloadable_bytes;
    std::size_t num_changesets;
    std::size_t accum_original_size;
    std::size_t accum_compacted_size;
};


// An unblocked work unit is comprised of one Work object for each of the files
// that contribute work to the work unit, generally one reference file and a
// number of partial files.
class Work {
public:
    // In general, primary work is all forms of modifying work, including file
    // deletion.
    bool has_primary_work = false;

    // Only for reference files
    bool might_produce_new_sync_version = false;

    bool produced_new_realm_version = false;
    bool produced_new_sync_version = false;
    bool expired_reference_version = false;

    // True if, and only if changesets_from_downstream contains at least one
    // changeset.
    bool have_changesets_from_downstream = false;

    FileIdentAllocSlots file_ident_alloc_slots;
    std::vector<std::unique_ptr<char[]>> changeset_buffers;
    IntegratableChangesets changesets_from_downstream;

    VersionInfo version_info;

    // Result of integration of changesets from downstream clients
    IntegrationResult integration_result;

    void reset() noexcept
    {
        has_primary_work = false;

        might_produce_new_sync_version = false;

        produced_new_realm_version = false;
        produced_new_sync_version = false;
        expired_reference_version = false;
        have_changesets_from_downstream = false;

        file_ident_alloc_slots.clear();
        changeset_buffers.clear();
        changesets_from_downstream.clear();

        version_info = {};
        integration_result = {};
    }
};


class WorkerState {
public:
    FileIdentAllocSlots file_ident_alloc_slots;
    util::ScratchMemory scratch_memory;
    bool use_file_cache = true;
    std::unique_ptr<ServerHistory> reference_hist;
    DBRef reference_sg;
};


// ============================ SessionQueue ============================

class SessionQueue {
public:
    void push_back(Session*) noexcept;
    Session* pop_front() noexcept;
    void clear() noexcept;

private:
    Session* m_back = nullptr;
};


// ============================ FileIdentReceiver ============================

class FileIdentReceiver {
public:
    virtual void receive_file_ident(SaltedFileIdent) = 0;

protected:
    ~FileIdentReceiver() {}
};


// ============================ WorkerBox =============================

class WorkerBox {
public:
    using JobType = util::UniqueFunction<void(WorkerState&)>;
    void add_work(WorkerState& state, JobType job)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_jobs.size() >= m_queue_limit) {
            // Once we have many queued jobs, it is better to use this thread to run a new job
            // than to queue it.
            run_a_job(lock, state, job);
        }
        else {
            // Create worker threads on demand (if all existing threads are active):
            if (m_threads.size() < m_max_num_threads && m_active >= m_threads.size()) {
                m_threads.emplace_back([this]() {
                    WorkerState state;
                    state.use_file_cache = false;
                    JobType the_job;
                    std::unique_lock<std::mutex> lock(m_mutex);
                    for (;;) {
                        while (m_jobs.empty() && !m_finish_up)
                            m_changes.wait(lock);
                        if (m_finish_up)
                            break; // terminate thread
                        the_job = std::move(m_jobs.back());
                        m_jobs.pop_back();
                        run_a_job(lock, state, the_job);
                        m_changes.notify_all();
                    }
                });
            }

            // Submit the job for execution:
            m_jobs.emplace_back(std::move(job));
            m_changes.notify_all();
        }
    }

    // You should call wait_completion() before trying to destroy a WorkerBox to get proper
    // propagation of exceptions.
    void wait_completion(WorkerState& state)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        while (!m_jobs.empty() || m_active > 0) {
            if (!m_jobs.empty()) { // if possible, make this thread participate in running m_jobs
                JobType the_job = std::move(m_jobs.back());
                m_jobs.pop_back();
                run_a_job(lock, state, the_job);
            }
            else {
                m_changes.wait(lock);
            }
        }
        if (m_epr) {
            std::rethrow_exception(m_epr);
        }
    }

    WorkerBox(unsigned int num_threads)
    {
        m_queue_limit = num_threads * 10; // fudge factor for job size variation
        m_max_num_threads = num_threads;
    }

    ~WorkerBox()
    {
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_finish_up = true;
            m_changes.notify_all();
        }
        for (auto& e : m_threads)
            e.join();
    }

private:
    std::mutex m_mutex;
    std::condition_variable m_changes;
    std::vector<std::thread> m_threads;
    std::vector<JobType> m_jobs;
    unsigned int m_active = 0;
    bool m_finish_up = false;
    unsigned int m_queue_limit = 0;
    unsigned int m_max_num_threads = 0;
    std::exception_ptr m_epr;

    void run_a_job(std::unique_lock<std::mutex>& lock, WorkerState& state, JobType& job)
    {
        ++m_active;
        lock.unlock();
        try {
            job(state);
            lock.lock();
        }
        catch (...) {
            lock.lock();
            if (!m_epr)
                m_epr = std::current_exception();
        }
        --m_active;
    }
};


// ============================ ServerFile ============================

class ServerFile : public util::RefCountBase {
public:
    util::PrefixLogger logger;

    // Logger to be used by the worker thread
    util::PrefixLogger wlogger;

    ServerFile(ServerImpl& server, ServerFileAccessCache& cache, const std::string& virt_path, std::string real_path,
               bool disable_sync_to_disk);
    ~ServerFile() noexcept;

    void initialize();
    void activate();

    ServerImpl& get_server() noexcept
    {
        return m_server;
    }

    const std::string& get_real_path() const noexcept
    {
        return m_file.realm_path;
    }

    const std::string& get_virt_path() const noexcept
    {
        return m_file.virt_path;
    }

    ServerFileAccessCache::File& access()
    {
        return m_file.access(); // Throws
    }

    ServerFileAccessCache::File& worker_access()
    {
        return m_worker_file.access(); // Throws
    }

    version_type get_realm_version() const noexcept
    {
        return m_version_info.realm_version;
    }

    version_type get_sync_version() const noexcept
    {
        return m_version_info.sync_version.version;
    }

    SaltedVersion get_salted_sync_version() const noexcept
    {
        return m_version_info.sync_version;
    }

    DownloadCache& get_download_cache() noexcept;

    void register_client_access(file_ident_type client_file_ident);

    using file_ident_request_type = std::int_fast64_t;

    // Initiate a request for a new client file identifier.
    //
    // Unless the request is cancelled, the identifier will be delivered to the
    // receiver by way of an invocation of
    // FileIdentReceiver::receive_file_ident().
    //
    // FileIdentReceiver::receive_file_ident() is guaranteed to not be called
    // until after request_file_ident() has returned (no callback reentrance).
    //
    // New client file identifiers will be delivered to receivers in the order
    // that they were requested.
    //
    // The returned value is a nonzero integer that can be used to cancel the
    // request before the file identifier is delivered using
    // cancel_file_ident_request().
    auto request_file_ident(FileIdentReceiver&, file_ident_type proxy_file, ClientType) -> file_ident_request_type;

    // Cancel the specified file identifier request.
    //
    // It is an error to call this function after the identifier has been
    // delivered.
    void cancel_file_ident_request(file_ident_request_type) noexcept;

    void add_unidentified_session(Session*);
    void identify_session(Session*, file_ident_type client_file_ident);

    void remove_unidentified_session(Session*) noexcept;
    void remove_identified_session(file_ident_type client_file_ident) noexcept;

    Session* get_identified_session(file_ident_type client_file_ident) noexcept;

    bool can_add_changesets_from_downstream() const noexcept;
    void add_changesets_from_downstream(file_ident_type client_file_ident, UploadCursor upload_progress,
                                        version_type locked_server_version, const UploadChangeset*,
                                        std::size_t num_changesets);

    // bootstrap_client_session calls the function of same name in server_history
    // but corrects the upload_progress with information from pending
    // integratable changesets. A situation can occur where a client terminates
    // a session and starts a new session and re-uploads changesets that are known
    // by the ServerFile object but not by the ServerHistory.
    BootstrapError bootstrap_client_session(SaltedFileIdent client_file_ident, DownloadCursor download_progress,
                                            SaltedVersion server_version, ClientType client_type,
                                            UploadCursor& upload_progress, version_type& locked_server_version,
                                            Logger&);

    // NOTE: This function is executed by the worker thread
    void worker_process_work_unit(WorkerState&);

    void recognize_external_change();

private:
    ServerImpl& m_server;
    ServerFileAccessCache::Slot m_file;

    // In general, `m_version_info` refers to the last snapshot of the Realm
    // file that is supposed to be visible to remote peers engaging in regular
    // Realm file synchronization.
    VersionInfo m_version_info;

    file_ident_request_type m_last_file_ident_request = 0;

    // The set of sessions whose client file identifier is not yet known, i.e.,
    // those for which an IDENT message has not yet been received,
    std::set<Session*> m_unidentified_sessions;

    // A map of the sessions whose client file identifier is known, i.e, those
    // for which an IDENT message has been received.
    std::map<file_ident_type, Session*> m_identified_sessions;

    // Used when a file used as partial view wants to allocate a client file
    // identifier from the reference Realm.
    file_ident_request_type m_file_ident_request = 0;

    struct FileIdentRequestInfo {
        FileIdentReceiver* receiver;
        file_ident_type proxy_file;
        ClientType client_type;
    };

    // When nonempty, it counts towards outstanding blocked work (see
    // `m_has_blocked_work`).
    std::map<file_ident_request_type, FileIdentRequestInfo> m_file_ident_requests;

    // Changesets received from the downstream clients, and waiting to be
    // integrated, as well as information about the clients progress in terms of
    // integrating changesets received from the server. When nonempty, it counts
    // towards outstanding blocked work (see `m_has_blocked_work`).
    //
    // At any given time, the set of changesets from a particular client-side
    // file may be comprised of changesets received via distinct sessions.
    //
    // See also `m_num_changesets_from_downstream`.
    IntegratableChangesets m_changesets_from_downstream;

    // Keeps track of the number of changesets in `m_changesets_from_downstream`.
    //
    // Its purpose is also to initialize
    // `Work::have_changesets_from_downstream`.
    std::size_t m_num_changesets_from_downstream = 0;

    // The total size, in bytes, of the changesets that were received from
    // clients, are targeting this file, and are currently part of the blocked
    // work unit.
    //
    // Together with `m_unblocked_changesets_from_downstream_byte_size`, its
    // purpose is to allow the server to keep track of the accumulated size of
    // changesets being processed, or waiting to be processed (metric
    // `upload.pending.bytes`) (see
    // ServerImpl::inc_byte_size_for_pending_downstream_changesets()).
    //
    // Its purpose is also to enable the "very poor man's" backpressure solution
    // (see can_add_changesets_from_downstream()).
    std::size_t m_blocked_changesets_from_downstream_byte_size = 0;

    // Same as `m_blocked_changesets_from_downstream_byte_size` but for the
    // currently unblocked work unit.
    std::size_t m_unblocked_changesets_from_downstream_byte_size = 0;

    // When nonempty, it counts towards outstanding blocked work (see
    // `m_has_blocked_work`).
    std::vector<std::string> m_permission_changes;

    // True iff this file, or any of its associated partial files (when
    // applicable), has a nonzero amount of outstanding work that is currently
    // held back from being passed to the worker thread because a previously
    // accumulated chunk of work related to this file is currently in progress.
    bool m_has_blocked_work = false;

    // A file, that is not a partial file, is considered *exposed to the worker
    // thread* from the point in time where it is submitted to the worker
    // (Worker::enqueue()) and up until the point in time where
    // group_postprocess_stage_1() starts to execute. A partial file is
    // considered *exposed to the worker thread* precisely when the associated
    // reference file is exposed to the worker thread, but only if it was in
    // `m_reference_file->m_work.partial_files` at the point in time where the
    // reference file was passed to the worker.
    //
    // While this file is exposed to the worker thread, all members of `m_work`
    // other than `changesets_from_downstream` may be accessed and modified by
    // the worker thread only.
    //
    // While this file is exposed to the worker thread,
    // `m_work.changesets_from_downstream` may be accessed by all threads, but
    // must not be modified by any thread. This special status of
    // `m_work.changesets_from_downstream` is required to allow
    // ServerFile::bootstrap_client_session() to read from it at any time.
    Work m_work;

    // For reference files, set to true when work is unblocked, and reset back
    // to false when the work finalization process completes
    // (group_postprocess_stage_3()). Always zero for partial files.
    bool m_has_work_in_progress = 0;

    // This one must only be accessed by the worker thread.
    //
    // More specifically, `m_worker_file.access()` must only be called by the
    // worker thread, and if it was ever called, it must be closed by the worker
    // thread before the ServerFile object is destroyed, if destruction happens
    // before the destruction of the server object itself.
    ServerFileAccessCache::Slot m_worker_file;

    std::vector<std::int_fast64_t> m_deleting_connections;

    DownloadCache m_download_cache;

    void on_changesets_from_downstream_added(std::size_t num_changesets, std::size_t num_bytes);
    void on_work_added();
    void group_unblock_work();
    void unblock_work();

    /// Resume history scanning in all sessions bound to this file. To be called
    /// after a successfull integration of a changeset.
    void resume_download() noexcept;

    // NOTE: These functions are executed by the worker thread
    void worker_allocate_file_identifiers();
    bool worker_integrate_changes_from_downstream(WorkerState&);
    ServerHistory& get_client_file_history(WorkerState& state, std::unique_ptr<ServerHistory>& hist_ptr,
                                           DBRef& sg_ptr);
    ServerHistory& get_reference_file_history(WorkerState& state);
    void group_postprocess_stage_1();
    void group_postprocess_stage_2();
    void group_postprocess_stage_3();
    void group_finalize_work_stage_1();
    void group_finalize_work_stage_2();
    void finalize_work_stage_1();
    void finalize_work_stage_2();
};


inline DownloadCache& ServerFile::get_download_cache() noexcept
{
    return m_download_cache;
}

inline void ServerFile::group_finalize_work_stage_1()
{
    finalize_work_stage_1(); // Throws
}

inline void ServerFile::group_finalize_work_stage_2()
{
    finalize_work_stage_2(); // Throws
}


// ============================ Worker ============================

// All write transaction on server-side Realm files performed on behalf of the
// server, must be performed by the worker thread, not the network event loop
// thread. This is to ensure that the network event loop thread never gets
// blocked waiting for the worker thread to end a long running write
// transaction.
//
// FIXME: Currently, the event loop thread does perform a number of write
// transactions, but only on subtier nodes of a star topology server cluster.
class Worker : public ServerHistory::Context {
public:
    std::shared_ptr<util::Logger> logger_ptr;
    util::Logger& logger;

    explicit Worker(ServerImpl&);

    ServerFileAccessCache& get_file_access_cache() noexcept;

    void enqueue(ServerFile*);

    // Overriding members of ServerHistory::Context
    std::mt19937_64& server_history_get_random() noexcept override final;

private:
    ServerImpl& m_server;
    std::mt19937_64 m_random;
    ServerFileAccessCache m_file_access_cache;

    util::Mutex m_mutex;
    util::CondVar m_cond; // Protected by `m_mutex`

    bool m_stop = false; // Protected by `m_mutex`

    util::CircularBuffer<ServerFile*> m_queue; // Protected by `m_mutex`

    WorkerState m_state;

    void run();
    void stop() noexcept;

    friend class util::ThreadExecGuardWithParent<Worker, ServerImpl>;
};


inline ServerFileAccessCache& Worker::get_file_access_cache() noexcept
{
    return m_file_access_cache;
}


// ============================ ServerImpl ============================

class ServerImpl : public ServerImplBase, public ServerHistory::Context {
public:
    std::uint_fast64_t errors_seen = 0;

    std::atomic<milliseconds_type> m_par_time;
    std::atomic<milliseconds_type> m_seq_time;

    util::Mutex last_client_accesses_mutex;

    const std::shared_ptr<util::Logger> logger_ptr;
    util::Logger& logger;

    network::Service& get_service() noexcept
    {
        return m_service;
    }

    const network::Service& get_service() const noexcept
    {
        return m_service;
    }

    std::mt19937_64& get_random() noexcept
    {
        return m_random;
    }

    const Server::Config& get_config() const noexcept
    {
        return m_config;
    }

    std::size_t get_max_upload_backlog() const noexcept
    {
        return m_max_upload_backlog;
    }

    const std::string& get_root_dir() const noexcept
    {
        return m_root_dir;
    }

    network::ssl::Context& get_ssl_context() noexcept
    {
        return *m_ssl_context;
    }

    const AccessControl& get_access_control() const noexcept
    {
        return m_access_control;
    }

    ProtocolVersionRange get_protocol_version_range() const noexcept
    {
        return m_protocol_version_range;
    }

    ServerProtocol& get_server_protocol() noexcept
    {
        return m_server_protocol;
    }

    compression::CompressMemoryArena& get_compress_memory_arena() noexcept
    {
        return m_compress_memory_arena;
    }

    MiscBuffers& get_misc_buffers() noexcept
    {
        return m_misc_buffers;
    }

    int_fast64_t get_current_server_session_ident() const noexcept
    {
        return m_current_server_session_ident;
    }

    util::ScratchMemory& get_scratch_memory() noexcept
    {
        return m_scratch_memory;
    }

    Worker& get_worker() noexcept
    {
        return m_worker;
    }

    void get_workunit_timers(milliseconds_type& parallel_section, milliseconds_type& sequential_section)
    {
        parallel_section = m_par_time;
        sequential_section = m_seq_time;
    }

    ServerImpl(const std::string& root_dir, util::Optional<sync::PKey>, Server::Config);
    ~ServerImpl() noexcept;

    void start();

    void start(std::string listen_address, std::string listen_port, bool reuse_address)
    {
        m_config.listen_address = listen_address;
        m_config.listen_port = listen_port;
        m_config.reuse_address = reuse_address;

        start(); // Throws
    }

    network::Endpoint listen_endpoint() const
    {
        return m_acceptor.local_endpoint();
    }

    void run();
    void stop() noexcept;

    void remove_http_connection(std::int_fast64_t conn_id) noexcept;

    void add_sync_connection(int_fast64_t connection_id, std::unique_ptr<SyncConnection>&& sync_conn);
    void remove_sync_connection(int_fast64_t connection_id);

    size_t get_number_of_http_connections()
    {
        return m_http_connections.size();
    }

    size_t get_number_of_sync_connections()
    {
        return m_sync_connections.size();
    }

    bool is_sync_stopped()
    {
        return m_sync_stopped;
    }

    const std::set<std::string>& get_realm_names() const noexcept
    {
        return m_realm_names;
    }

    // virt_path must be valid when get_or_create_file() is called.
    util::bind_ptr<ServerFile> get_or_create_file(const std::string& virt_path)
    {
        util::bind_ptr<ServerFile> file = get_file(virt_path);
        if (REALM_LIKELY(file))
            return file;

        _impl::VirtualPathComponents virt_path_components =
            _impl::parse_virtual_path(m_root_dir, virt_path); // Throws
        REALM_ASSERT(virt_path_components.is_valid);

        _impl::make_dirs(m_root_dir, virt_path); // Throws
        m_realm_names.insert(virt_path);         // Throws
        {
            bool disable_sync_to_disk = m_config.disable_sync_to_disk;
            file.reset(new ServerFile(*this, m_file_access_cache, virt_path, virt_path_components.real_realm_path,
                                      disable_sync_to_disk)); // Throws
        }

        file->initialize();
        m_files[virt_path] = file; // Throws
        file->activate();          // Throws
        return file;
    }

    std::unique_ptr<ServerHistory> make_history_for_path()
    {
        return std::make_unique<ServerHistory>(*this);
    }

    util::bind_ptr<ServerFile> get_file(const std::string& virt_path) noexcept
    {
        auto i = m_files.find(virt_path);
        if (REALM_LIKELY(i != m_files.end()))
            return i->second;
        return {};
    }

    // Returns the number of seconds since the Epoch of
    // std::chrono::system_clock.
    std::chrono::system_clock::time_point token_expiration_clock_now() const noexcept
    {
        if (REALM_UNLIKELY(m_config.token_expiration_clock))
            return m_config.token_expiration_clock->now();
        return std::chrono::system_clock::now();
    }

    void set_connection_reaper_timeout(milliseconds_type);

    void close_connections();
    bool map_virtual_to_real_path(const std::string& virt_path, std::string& real_path);

    void recognize_external_change(const std::string& virt_path);

    void stop_sync_and_wait_for_backup_completion(util::UniqueFunction<void(bool did_backup)> completion_handler,
                                                  milliseconds_type timeout);

    // Server global outputbuffers that can be reused.
    // The server is single threaded, so there are no
    // synchronization issues.
    // output_buffers_count is equal to the
    // maximum number of buffers needed at any point.
    static constexpr int output_buffers_count = 1;
    OutputBuffer output_buffers[output_buffers_count];

    bool is_load_balancing_allowed() const
    {
        return m_allow_load_balancing;
    }

    // inc_byte_size_for_pending_downstream_changesets() must be called by
    // ServerFile objects when changesets from downstream clients have been
    // received.
    //
    // dec_byte_size_for_pending_downstream_changesets() must be called by
    // ServerFile objects when changesets from downstream clients have been
    // processed or discarded.
    //
    // ServerImpl uses this information to keep a running tally (metric
    // `upload.pending.bytes`) of the total byte size of pending changesets from
    // downstream clients.
    //
    // These functions must be called on the network thread.
    void inc_byte_size_for_pending_downstream_changesets(std::size_t byte_size);
    void dec_byte_size_for_pending_downstream_changesets(std::size_t byte_size);

    // Overriding member functions in _impl::ServerHistory::Context
    std::mt19937_64& server_history_get_random() noexcept override final;

private:
    Server::Config m_config;
    network::Service m_service;
    std::mt19937_64 m_random;
    const std::size_t m_max_upload_backlog;
    const std::string m_root_dir;
    const AccessControl m_access_control;
    const ProtocolVersionRange m_protocol_version_range;

    // The reserved files will be closed in situations where the server
    // runs out of file descriptors.
    std::unique_ptr<File> m_reserved_files[5];

    // The set of all Realm files known to this server, represented by their
    // virtual path.
    //
    // INVARIANT: If a Realm file is in the servers directory (i.e., it would be
    // reported by an invocation of _impl::get_realm_names()), then the
    // corresponding virtual path is in `m_realm_names`, assuming no external
    // file-system level intervention.
    std::set<std::string> m_realm_names;

    std::unique_ptr<network::ssl::Context> m_ssl_context;
    ServerFileAccessCache m_file_access_cache;
    Worker m_worker;
    std::map<std::string, util::bind_ptr<ServerFile>> m_files; // Key is virtual path
    network::Acceptor m_acceptor;
    std::int_fast64_t m_next_conn_id = 0;
    std::unique_ptr<HTTPConnection> m_next_http_conn;
    network::Endpoint m_next_http_conn_endpoint;
    std::map<std::int_fast64_t, std::unique_ptr<HTTPConnection>> m_http_connections;
    std::map<std::int_fast64_t, std::unique_ptr<SyncConnection>> m_sync_connections;
    ServerProtocol m_server_protocol;
    compression::CompressMemoryArena m_compress_memory_arena;
    MiscBuffers m_misc_buffers;
    int_fast64_t m_current_server_session_ident;
    Optional<network::DeadlineTimer> m_connection_reaper_timer;
    bool m_allow_load_balancing = false;

    util::Mutex m_mutex;

    bool m_stopped = false; // Protected by `m_mutex`

    // m_sync_stopped is used by stop_sync_and_wait_for_backup_completion().
    // When m_sync_stopped is true, the server does not perform any sync.
    bool m_sync_stopped = false;

    std::atomic<bool> m_running{false}; // Debugging facility

    std::size_t m_pending_changesets_from_downstream_byte_size = 0;

    util::CondVar m_wait_or_service_stopped_cond; // Protected by `m_mutex`

    util::ScratchMemory m_scratch_memory;

    void listen();
    void initiate_accept();
    void handle_accept(std::error_code);

    void reap_connections();
    void initiate_connection_reaper_timer(milliseconds_type timeout);
    void do_close_connections();

    static std::size_t determine_max_upload_backlog(Server::Config& config) noexcept
    {
        if (config.max_upload_backlog == 0)
            return 4294967295; // 4GiB - 1 (largest allowable number on a 32-bit platform)
        return config.max_upload_backlog;
    }

    static ProtocolVersionRange determine_protocol_version_range(Server::Config& config)
    {
        const int actual_min = ServerImplBase::get_oldest_supported_protocol_version();
        const int actual_max = get_current_protocol_version();
        static_assert(actual_min <= actual_max, "");
        int min = actual_min;
        int max = actual_max;
        if (config.max_protocol_version != 0 && config.max_protocol_version < max) {
            if (config.max_protocol_version < min)
                throw Server::NoSupportedProtocolVersions();
            max = config.max_protocol_version;
        }
        return {min, max};
    }

    void do_recognize_external_change(const std::string& virt_path);

    void do_stop_sync_and_wait_for_backup_completion(util::UniqueFunction<void(bool did_complete)> completion_handler,
                                                     milliseconds_type timeout);
};

// ============================ SyncConnection ============================

class SyncConnection : public websocket::Config {
public:
    const std::shared_ptr<util::Logger> logger_ptr;
    util::Logger& logger;

    // Clients with sync protocol version 8 or greater support pbs->flx migration
    static constexpr int PBS_FLX_MIGRATION_PROTOCOL_VERSION = 8;
    // Clients with sync protocol version less than 10 do not support log messages
    static constexpr int SERVER_LOG_PROTOCOL_VERSION = 10;

    SyncConnection(ServerImpl& serv, std::int_fast64_t id, std::unique_ptr<network::Socket>&& socket,
                   std::unique_ptr<network::ssl::Stream>&& ssl_stream,
                   std::unique_ptr<network::ReadAheadBuffer>&& read_ahead_buffer, int client_protocol_version,
                   std::string client_user_agent, std::string remote_endpoint, std::string appservices_request_id)
        : logger_ptr{std::make_shared<util::PrefixLogger>(util::LogCategory::server, make_logger_prefix(id),
                                                          serv.logger_ptr)} // Throws
        , logger{*logger_ptr}
        , m_server{serv}
        , m_id{id}
        , m_socket{std::move(socket)}
        , m_ssl_stream{std::move(ssl_stream)}
        , m_read_ahead_buffer{std::move(read_ahead_buffer)}
        , m_websocket{*this}
        , m_client_protocol_version{client_protocol_version}
        , m_client_user_agent{std::move(client_user_agent)}
        , m_remote_endpoint{std::move(remote_endpoint)}
        , m_appservices_request_id{std::move(appservices_request_id)}
        , m_send_trigger{m_server.get_service(), &SyncConnection::send_next_message, this}
    {
        // Make the output buffer stream throw std::bad_alloc if it fails to
        // expand the buffer
        m_output_buffer.exceptions(std::ios_base::badbit | std::ios_base::failbit);
    }

    ~SyncConnection() noexcept;

    ServerImpl& get_server() noexcept
    {
        return m_server;
    }

    ServerProtocol& get_server_protocol() noexcept
    {
        return m_server.get_server_protocol();
    }

    int get_client_protocol_version()
    {
        return m_client_protocol_version;
    }

    const std::string& get_client_user_agent() const noexcept
    {
        return m_client_user_agent;
    }

    const std::string& get_remote_endpoint() const noexcept
    {
        return m_remote_endpoint;
    }

    const std::shared_ptr<util::Logger>& websocket_get_logger() noexcept final
    {
        return logger_ptr;
    }

    std::mt19937_64& websocket_get_random() noexcept final override
    {
        return m_server.get_random();
    }

    bool websocket_binary_message_received(const char* data, size_t size) final override
    {
        using sf = _impl::SimulatedFailure;
        if (sf::check_trigger(sf::sync_server__read_head)) {
            // Suicide
            read_error(sf::sync_server__read_head);
            return false;
        }
        // After a connection level error has occurred, all incoming messages
        // will be ignored. By continuing to read until end of input, the server
        // is able to know when the client closes the connection, which in
        // general means that is has received the ERROR message.
        if (REALM_LIKELY(!m_is_closing)) {
            m_last_activity_at = steady_clock_now();
            handle_message_received(data, size);
        }
        return true;
    }

    bool websocket_ping_message_received(const char* data, size_t size) final override
    {
        if (REALM_LIKELY(!m_is_closing)) {
            m_last_activity_at = steady_clock_now();
            handle_ping_received(data, size);
        }
        return true;
    }

    void async_write(const char* data, size_t size, websocket::WriteCompletionHandler handler) final override
    {
        if (m_ssl_stream) {
            m_ssl_stream->async_write(data, size, std::move(handler)); // Throws
        }
        else {
            m_socket->async_write(data, size, std::move(handler)); // Throws
        }
    }

    void async_read(char* buffer, size_t size, websocket::ReadCompletionHandler handler) final override
    {
        if (m_ssl_stream) {
            m_ssl_stream->async_read(buffer, size, *m_read_ahead_buffer, std::move(handler)); // Throws
        }
        else {
            m_socket->async_read(buffer, size, *m_read_ahead_buffer, std::move(handler)); // Throws
        }
    }

    void async_read_until(char* buffer, size_t size, char delim,
                          websocket::ReadCompletionHandler handler) final override
    {
        if (m_ssl_stream) {
            m_ssl_stream->async_read_until(buffer, size, delim, *m_read_ahead_buffer,
                                           std::move(handler)); // Throws
        }
        else {
            m_socket->async_read_until(buffer, size, delim, *m_read_ahead_buffer,
                                       std::move(handler)); // Throws
        }
    }

    void websocket_read_error_handler(std::error_code ec) final override
    {
        read_error(ec);
    }

    void websocket_write_error_handler(std::error_code ec) final override
    {
        write_error(ec);
    }

    void websocket_handshake_error_handler(std::error_code ec, const HTTPHeaders*, std::string_view) final override
    {
        // WebSocket class has already logged a message for this error
        close_due_to_error(ec); // Throws
    }

    void websocket_protocol_error_handler(std::error_code ec) final override
    {
        logger.error("WebSocket protocol error (%1): %2", ec, ec.message()); // Throws
        close_due_to_error(ec);                                              // Throws
    }

    void websocket_handshake_completion_handler(const HTTPHeaders&) final override
    {
        // This is not called since we handle HTTP request in handle_request_for_sync()
        REALM_TERMINATE("websocket_handshake_completion_handler should not have been called");
    }

    int_fast64_t get_id() const noexcept
    {
        return m_id;
    }

    network::Socket& get_socket() noexcept
    {
        return *m_socket;
    }

    void initiate();

    // Commits suicide
    template <class... Params>
    void terminate(Logger::Level, const char* log_message, Params... log_params);

    // Commits suicide
    void terminate_if_dead(SteadyTimePoint now);

    void enlist_to_send(Session*) noexcept;

    // Sessions should get the output_buffer and insert a message, after which
    // they call initiate_write_output_buffer().
    OutputBuffer& get_output_buffer()
    {
        m_output_buffer.reset();
        return m_output_buffer;
    }

    // More advanced memory strategies can be implemented if needed.
    void release_output_buffer() {}

    // When this function is called, the connection will initiate a write with
    // its output_buffer. Sessions use this method.
    void initiate_write_output_buffer();

    void initiate_pong_output_buffer();

    void handle_protocol_error(Status status);

    void receive_bind_message(session_ident_type, std::string path, std::string signed_user_token,
                              bool need_client_file_ident, bool is_subserver);

    void receive_ident_message(session_ident_type, file_ident_type client_file_ident,
                               salt_type client_file_ident_salt, version_type scan_server_version,
                               version_type scan_client_version, version_type latest_server_version,
                               salt_type latest_server_version_salt);

    void receive_upload_message(session_ident_type, version_type progress_client_version,
                                version_type progress_server_version, version_type locked_server_version,
                                const UploadChangesets&);

    void receive_mark_message(session_ident_type, request_ident_type);

    void receive_unbind_message(session_ident_type);

    void receive_ping(milliseconds_type timestamp, milliseconds_type rtt);

    void receive_error_message(session_ident_type, int error_code, std::string_view error_body);

    void protocol_error(ProtocolError, Session* = nullptr);

    void initiate_soft_close();

    void discard_session(session_ident_type) noexcept;

    void send_log_message(util::Logger::Level level, const std::string&& message, session_ident_type sess_ident = 0,
                          std::optional<std::string> co_id = std::nullopt);

private:
    ServerImpl& m_server;
    const int_fast64_t m_id;
    std::unique_ptr<network::Socket> m_socket;
    std::unique_ptr<network::ssl::Stream> m_ssl_stream;
    std::unique_ptr<network::ReadAheadBuffer> m_read_ahead_buffer;

    websocket::Socket m_websocket;
    std::unique_ptr<char[]> m_input_body_buffer;
    OutputBuffer m_output_buffer;
    std::map<session_ident_type, std::unique_ptr<Session>> m_sessions;

    // The protocol version in use by the connected client.
    const int m_client_protocol_version;

    // The user agent description passed by the client.
    const std::string m_client_user_agent;

    const std::string m_remote_endpoint;

    const std::string m_appservices_request_id;

    // A queue of sessions that have enlisted for an opportunity to send a
    // message. Sessions will be served in the order that they enlist. A session
    // can only occur once in this queue (linked list). If the queue is not
    // empty, and no message is currently being written to the socket, the first
    // session is taken out of the queue, and then granted an opportunity to
    // send a message.
    //
    // Sessions will never be destroyed while in this queue. This is ensured
    // because the connection owns the sessions that are associated with it, and
    // the connection only removes a session from m_sessions at points in time
    // where that session is guaranteed to not be in m_sessions_enlisted_to_send
    // (Connection::send_next_message() and Connection::~Connection()).
    SessionQueue m_sessions_enlisted_to_send;

    Session* m_receiving_session = nullptr;

    bool m_is_sending = false;
    bool m_is_closing = false;

    bool m_send_pong = false;
    bool m_sending_pong = false;

    Trigger<network::Service> m_send_trigger;

    milliseconds_type m_last_ping_timestamp = 0;

    // If `m_is_closing` is true, this is the time at which `m_is_closing` was
    // set to true (initiation of soft close). Otherwise, if no messages have
    // been received from the client, this is the time at which the connection
    // object was initiated (completion of WebSocket handshake). Otherwise this
    // is the time at which the last message was received from the client.
    SteadyTimePoint m_last_activity_at;

    // These are initialized by do_initiate_soft_close().
    //
    // With recent versions of the protocol (when the version is greater than,
    // or equal to 23), `m_error_session_ident` is always zero.
    ProtocolError m_error_code = {};
    session_ident_type m_error_session_ident = 0;

    struct LogMessage {
        session_ident_type sess_ident;
        util::Logger::Level level;
        std::string message;
        std::optional<std::string> co_id;
    };

    std::mutex m_log_mutex;
    std::queue<LogMessage> m_log_messages;

    static std::string make_logger_prefix(int_fast64_t id)
    {
        std::ostringstream out;
        out.imbue(std::locale::classic());
        out << "Sync Connection[" << id << "]: "; // Throws
        return out.str();                         // Throws
    }

    // The return value of handle_message_received() designates whether
    // message processing should continue. If the connection object is
    // destroyed during execution of handle_message_received(), the return
    // value must be false.
    void handle_message_received(const char* data, size_t size);

    void handle_ping_received(const char* data, size_t size);

    void send_next_message();
    void send_pong(milliseconds_type timestamp);
    void send_log_message(const LogMessage& log_msg);

    void handle_write_output_buffer();
    void handle_pong_output_buffer();

    void initiate_write_error(ProtocolError, session_ident_type);
    void handle_write_error(std::error_code ec);

    void do_initiate_soft_close(ProtocolError, session_ident_type);
    void read_error(std::error_code);
    void write_error(std::error_code);

    void close_due_to_close_by_client(std::error_code);
    void close_due_to_error(std::error_code);

    void terminate_sessions();

    void bad_session_ident(const char* message_type, session_ident_type);
    void message_after_unbind(const char* message_type, session_ident_type);
    void message_before_ident(const char* message_type, session_ident_type);
};


inline void SyncConnection::read_error(std::error_code ec)
{
    REALM_ASSERT(ec != util::error::operation_aborted);
    if (ec == util::MiscExtErrors::end_of_input || ec == util::error::connection_reset) {
        // Suicide
        close_due_to_close_by_client(ec); // Throws
        return;
    }
    if (ec == util::MiscExtErrors::delim_not_found) {
        logger.error("Input message head delimited not found"); // Throws
        protocol_error(ProtocolError::limits_exceeded);         // Throws
        return;
    }

    logger.error("Reading failed: %1", ec.message()); // Throws

    // Suicide
    close_due_to_error(ec); // Throws
}

inline void SyncConnection::write_error(std::error_code ec)
{
    REALM_ASSERT(ec != util::error::operation_aborted);
    if (ec == util::error::broken_pipe || ec == util::error::connection_reset) {
        // Suicide
        close_due_to_close_by_client(ec); // Throws
        return;
    }
    logger.error("Writing failed: %1", ec.message()); // Throws

    // Suicide
    close_due_to_error(ec); // Throws
}


// ============================ HTTPConnection ============================

std::string g_user_agent = "User-Agent";

class HTTPConnection {
public:
    const std::shared_ptr<Logger> logger_ptr;
    util::Logger& logger;

    HTTPConnection(ServerImpl& serv, int_fast64_t id, bool is_ssl)
        : logger_ptr{std::make_shared<PrefixLogger>(util::LogCategory::server, make_logger_prefix(id),
                                                    serv.logger_ptr)} // Throws
        , logger{*logger_ptr}
        , m_server{serv}
        , m_id{id}
        , m_socket{new network::Socket{serv.get_service()}} // Throws
        , m_read_ahead_buffer{new network::ReadAheadBuffer} // Throws
        , m_http_server{*this, logger_ptr}
    {
        // Make the output buffer stream throw std::bad_alloc if it fails to
        // expand the buffer
        m_output_buffer.exceptions(std::ios_base::badbit | std::ios_base::failbit);

        if (is_ssl) {
            using namespace network::ssl;
            Context& ssl_context = serv.get_ssl_context();
            m_ssl_stream = std::make_unique<Stream>(*m_socket, ssl_context,
                                                    Stream::server); // Throws
        }
    }

    ServerImpl& get_server() noexcept
    {
        return m_server;
    }

    int_fast64_t get_id() const noexcept
    {
        return m_id;
    }

    network::Socket& get_socket() noexcept
    {
        return *m_socket;
    }

    template <class H>
    void async_write(const char* data, size_t size, H handler)
    {
        if (m_ssl_stream) {
            m_ssl_stream->async_write(data, size, std::move(handler)); // Throws
        }
        else {
            m_socket->async_write(data, size, std::move(handler)); // Throws
        }
    }

    template <class H>
    void async_read(char* buffer, size_t size, H handler)
    {
        if (m_ssl_stream) {
            m_ssl_stream->async_read(buffer, size, *m_read_ahead_buffer,
                                     std::move(handler)); // Throws
        }
        else {
            m_socket->async_read(buffer, size, *m_read_ahead_buffer,
                                 std::move(handler)); // Throws
        }
    }

    template <class H>
    void async_read_until(char* buffer, size_t size, char delim, H handler)
    {
        if (m_ssl_stream) {
            m_ssl_stream->async_read_until(buffer, size, delim, *m_read_ahead_buffer,
                                           std::move(handler)); // Throws
        }
        else {
            m_socket->async_read_until(buffer, size, delim, *m_read_ahead_buffer,
                                       std::move(handler)); // Throws
        }
    }

    void initiate(std::string remote_endpoint)
    {
        m_last_activity_at = steady_clock_now();
        m_remote_endpoint = std::move(remote_endpoint);

        logger.detail("Connection from %1", m_remote_endpoint); // Throws

        if (m_ssl_stream) {
            initiate_ssl_handshake(); // Throws
        }
        else {
            initiate_http(); // Throws
        }
    }

    void respond_200_ok()
    {
        handle_text_response(HTTPStatus::Ok, "OK"); // Throws
    }

    void respond_404_not_found()
    {
        handle_text_response(HTTPStatus::NotFound, "Not found"); // Throws
    }

    void respond_503_service_unavailable()
    {
        handle_text_response(HTTPStatus::ServiceUnavailable, "Service unavailable"); // Throws
    }

    // Commits suicide
    template <class... Params>
    void terminate(Logger::Level log_level, const char* log_message, Params... log_params)
    {
        logger.log(log_level, log_message, log_params...); // Throws
        m_ssl_stream.reset();
        m_socket.reset();
        m_server.remove_http_connection(m_id); // Suicide
    }

    // Commits suicide
    void terminate_if_dead(SteadyTimePoint now)
    {
        milliseconds_type time = steady_duration(m_last_activity_at, now);
        const Server::Config& config = m_server.get_config();
        if (m_is_sending) {
            if (time >= config.http_response_timeout) {
                // Suicide
                terminate(Logger::Level::detail,
                          "HTTP connection closed (request timeout)"); // Throws
            }
        }
        else {
            if (time >= config.http_request_timeout) {
                // Suicide
                terminate(Logger::Level::detail,
                          "HTTP connection closed (response timeout)"); // Throws
            }
        }
    }

    std::string get_appservices_request_id() const
    {
        return m_appservices_request_id.to_string();
    }

private:
    ServerImpl& m_server;
    const int_fast64_t m_id;
    const ObjectId m_appservices_request_id = ObjectId::gen();
    std::unique_ptr<network::Socket> m_socket;
    std::unique_ptr<network::ssl::Stream> m_ssl_stream;
    std::unique_ptr<network::ReadAheadBuffer> m_read_ahead_buffer;
    HTTPServer<HTTPConnection> m_http_server;
    OutputBuffer m_output_buffer;
    bool m_is_sending = false;
    SteadyTimePoint m_last_activity_at;
    std::string m_remote_endpoint;
    int m_negotiated_protocol_version = 0;

    void initiate_ssl_handshake()
    {
        auto handler = [this](std::error_code ec) {
            if (ec != util::error::operation_aborted)
                handle_ssl_handshake(ec); // Throws
        };
        m_ssl_stream->async_handshake(std::move(handler)); // Throws
    }

    void handle_ssl_handshake(std::error_code ec)
    {
        if (ec) {
            logger.error("SSL handshake error (%1): %2", ec, ec.message()); // Throws
            close_due_to_error(ec);                                         // Throws
            return;
        }
        initiate_http(); // Throws
    }

    void initiate_http()
    {
        logger.debug("Connection initiates HTTP receipt");

        auto handler = [this](HTTPRequest request, std::error_code ec) {
            if (REALM_UNLIKELY(ec == util::error::operation_aborted))
                return;
            if (REALM_UNLIKELY(ec == HTTPParserError::MalformedRequest)) {
                logger.error("Malformed HTTP request");
                close_due_to_error(ec); // Throws
                return;
            }
            if (REALM_UNLIKELY(ec == HTTPParserError::BadRequest)) {
                logger.error("Bad HTTP request");
                const char* body = "The HTTP request was corrupted";
                handle_400_bad_request(body); // Throws
                return;
            }
            if (REALM_UNLIKELY(ec)) {
                read_error(ec); // Throws
                return;
            }
            handle_http_request(std::move(request)); // Throws
        };
        m_http_server.async_receive_request(std::move(handler)); // Throws
    }

    void handle_http_request(const HTTPRequest& request)
    {
        StringData path = request.path;

        logger.debug("HTTP request received, request = %1", request);

        m_is_sending = true;
        m_last_activity_at = steady_clock_now();

        // FIXME: When thinking of this function as a switching device, it seem
        // wrong that it requires a `%2F` after `/realm-sync/`. If `%2F` is
        // supposed to be mandatory, then that check ought to be delegated to
        // handle_request_for_sync(), as that will yield a sharper separation of
        // concerns.
        if (path == "/realm-sync" || path.begins_with("/realm-sync?") || path.begins_with("/realm-sync/%2F")) {
            handle_request_for_sync(request); // Throws
        }
        else {
            handle_404_not_found(request); // Throws
        }
    }

    void handle_request_for_sync(const HTTPRequest& request)
    {
        if (m_server.is_sync_stopped()) {
            logger.debug("Attempt to create a sync connection to a server that has been "
                         "stopped"); // Throws
            handle_503_service_unavailable(request, "The server does not accept sync "
                                                    "connections"); // Throws
            return;
        }

        util::Optional<std::string> sec_websocket_protocol = websocket::read_sec_websocket_protocol(request);

        // Figure out whether there are any protocol versions supported by both
        // the client and the server, and if so, choose the newest one of them.
        MiscBuffers& misc_buffers = m_server.get_misc_buffers();
        using ProtocolVersionRanges = MiscBuffers::ProtocolVersionRanges;
        ProtocolVersionRanges& protocol_version_ranges = misc_buffers.protocol_version_ranges;
        {
            protocol_version_ranges.clear();
            util::MemoryInputStream in;
            in.imbue(std::locale::classic());
            in.unsetf(std::ios_base::skipws);
            std::string_view value;
            if (sec_websocket_protocol)
                value = *sec_websocket_protocol;
            HttpListHeaderValueParser parser{value};
            std::string_view elem;
            while (parser.next(elem)) {
                // FIXME: Use std::string_view::begins_with() in C++20.
                const StringData protocol{elem};
                std::string_view prefix;
                if (protocol.begins_with(get_pbs_websocket_protocol_prefix()))
                    prefix = get_pbs_websocket_protocol_prefix();
                else if (protocol.begins_with(get_old_pbs_websocket_protocol_prefix()))
                    prefix = get_old_pbs_websocket_protocol_prefix();
                if (!prefix.empty()) {
                    auto parse_version = [&](std::string_view str) {
                        in.set_buffer(str.data(), str.data() + str.size());
                        int version = 0;
                        in >> version;
                        if (REALM_LIKELY(in && in.eof() && version >= 0))
                            return version;
                        return -1;
                    };
                    int min, max;
                    std::string_view range = elem.substr(prefix.size());
                    auto i = range.find('-');
                    if (i != std::string_view::npos) {
                        min = parse_version(range.substr(0, i));
                        max = parse_version(range.substr(i + 1));
                    }
                    else {
                        min = parse_version(range);
                        max = min;
                    }
                    if (REALM_LIKELY(min >= 0 && max >= 0 && min <= max)) {
                        protocol_version_ranges.emplace_back(min, max); // Throws
                        continue;
                    }
                    logger.error("Protocol version negotiation failed: Client sent malformed "
                                 "specification of supported protocol versions: '%1'",
                                 elem); // Throws
                    handle_400_bad_request("Protocol version negotiation failed: Malformed "
                                           "specification of supported protocol "
                                           "versions\n"); // Throws
                    return;
                }
                logger.warn("Unrecognized protocol token in HTTP response header "
                            "Sec-WebSocket-Protocol: '%1'",
                            elem); // Throws
            }
            if (protocol_version_ranges.empty()) {
                logger.error("Protocol version negotiation failed: Client did not send a "
                             "specification of supported protocol versions"); // Throws
                handle_400_bad_request("Protocol version negotiation failed: Missing specification "
                                       "of supported protocol versions\n"); // Throws
                return;
            }
        }
        {
            ProtocolVersionRange server_range = m_server.get_protocol_version_range();
            int server_min = server_range.first;
            int server_max = server_range.second;
            int best_match = 0;
            int overall_client_min = std::numeric_limits<int>::max();
            int overall_client_max = std::numeric_limits<int>::min();
            for (const auto& range : protocol_version_ranges) {
                int client_min = range.first;
                int client_max = range.second;
                if (client_max >= server_min && client_min <= server_max) {
                    // Overlap
                    int version = std::min(client_max, server_max);
                    if (version > best_match) {
                        best_match = version;
                    }
                }
                if (client_min < overall_client_min)
                    overall_client_min = client_min;
                if (client_max > overall_client_max)
                    overall_client_max = client_max;
            }
            Formatter& formatter = misc_buffers.formatter;
            if (REALM_UNLIKELY(best_match == 0)) {
                const char* elaboration = "No version supported by both client and server";
                auto format_ranges = [&](const auto& list) {
                    bool nonfirst = false;
                    for (auto range : list) {
                        if (nonfirst)
                            formatter << ", "; // Throws
                        int min = range.first, max = range.second;
                        REALM_ASSERT(min <= max);
                        formatter << min;
                        if (max != min)
                            formatter << "-" << max;
                        nonfirst = true;
                    }
                };
                using Range = ProtocolVersionRange;
                formatter.reset();
                format_ranges(protocol_version_ranges); // Throws
                logger.error("Protocol version negotiation failed: %1 "
                             "(client supports: %2)",
                             elaboration, std::string_view(formatter.data(), formatter.size())); // Throws
                formatter.reset();
                formatter << "Protocol version negotiation failed: "
                             ""
                          << elaboration << ".\n\n";                                   // Throws
                formatter << "Server supports: ";                                      // Throws
                format_ranges(std::initializer_list<Range>{{server_min, server_max}}); // Throws
                formatter << "\n";                                                     // Throws
                formatter << "Client supports: ";                                      // Throws
                format_ranges(protocol_version_ranges);                                // Throws
                formatter << "\n";                                                     // Throws
                handle_400_bad_request({formatter.data(), formatter.size()});          // Throws
                return;
            }
            m_negotiated_protocol_version = best_match;
            logger.debug("Received: Sync HTTP request (negotiated_protocol_version=%1)",
                         m_negotiated_protocol_version); // Throws
            formatter.reset();
        }

        std::string sec_websocket_protocol_2;
        {
            std::string_view prefix =
                m_negotiated_protocol_version < SyncConnection::PBS_FLX_MIGRATION_PROTOCOL_VERSION
                    ? get_old_pbs_websocket_protocol_prefix()
                    : get_pbs_websocket_protocol_prefix();
            std::ostringstream out;
            out.imbue(std::locale::classic());
            out << prefix << m_negotiated_protocol_version; // Throws
            sec_websocket_protocol_2 = std::move(out).str();
        }

        std::error_code ec;
        util::Optional<HTTPResponse> response =
            websocket::make_http_response(request, sec_websocket_protocol_2, ec); // Throws

        if (ec) {
            if (ec == websocket::HttpError::bad_request_header_upgrade) {
                logger.error("There must be a header of the form 'Upgrade: websocket'");
            }
            else if (ec == websocket::HttpError::bad_request_header_connection) {
                logger.error("There must be a header of the form 'Connection: Upgrade'");
            }
            else if (ec == websocket::HttpError::bad_request_header_websocket_version) {
                logger.error("There must be a header of the form 'Sec-WebSocket-Version: 13'");
            }
            else if (ec == websocket::HttpError::bad_request_header_websocket_key) {
                logger.error("The header Sec-WebSocket-Key is missing");
            }

            logger.error("The HTTP request with the error is:\n%1", request);
            logger.error("Check the proxy configuration and make sure that the "
                         "HTTP request is a valid Websocket request.");
            close_due_to_error(ec);
            return;
        }
        REALM_ASSERT(response);
        add_common_http_response_headers(*response);

        std::string user_agent;
        {
            auto i = request.headers.find(g_user_agent);
            if (i != request.headers.end())
                user_agent = i->second; // Throws (copy)
        }

        auto handler = [protocol_version = m_negotiated_protocol_version, user_agent = std::move(user_agent),
                        this](std::error_code ec) {
            // If the operation is aborted, the socket object may have been destroyed.
            if (ec != util::error::operation_aborted) {
                if (ec) {
                    write_error(ec);
                    return;
                }

                std::unique_ptr<SyncConnection> sync_conn = std::make_unique<SyncConnection>(
                    m_server, m_id, std::move(m_socket), std::move(m_ssl_stream), std::move(m_read_ahead_buffer),
                    protocol_version, std::move(user_agent), std::move(m_remote_endpoint),
                    get_appservices_request_id()); // Throws
                SyncConnection& sync_conn_ref = *sync_conn;
                m_server.add_sync_connection(m_id, std::move(sync_conn));
                m_server.remove_http_connection(m_id);
                sync_conn_ref.initiate();
            }
        };
        m_http_server.async_send_response(*response, std::move(handler));
    }

    void handle_text_response(HTTPStatus http_status, std::string_view body)
    {
        std::string body_2 = std::string(body); // Throws

        HTTPResponse response;
        response.status = http_status;
        add_common_http_response_headers(response);
        response.headers["Connection"] = "close";

        if (!body_2.empty()) {
            response.headers["Content-Length"] = util::to_string(body_2.size());
            response.body = std::move(body_2);
        }

        auto handler = [this](std::error_code ec) {
            if (REALM_UNLIKELY(ec == util::error::operation_aborted))
                return;
            if (REALM_UNLIKELY(ec)) {
                write_error(ec);
                return;
            }
            terminate(Logger::Level::detail, "HTTP connection closed"); // Throws
        };
        m_http_server.async_send_response(response, std::move(handler));
    }

    void handle_400_bad_request(std::string_view body)
    {
        logger.detail("400 Bad Request");
        handle_text_response(HTTPStatus::BadRequest, body); // Throws
    }

    void handle_404_not_found(const HTTPRequest&)
    {
        logger.detail("404 Not Found"); // Throws
        handle_text_response(HTTPStatus::NotFound,
                             "Realm sync server\n\nPage not found\n"); // Throws
    }

    void handle_503_service_unavailable(const HTTPRequest&, std::string_view message)
    {
        logger.debug("503 Service Unavailable");                       // Throws
        handle_text_response(HTTPStatus::ServiceUnavailable, message); // Throws
    }

    void add_common_http_response_headers(HTTPResponse& response)
    {
        response.headers["Server"] = "RealmSync/" REALM_VERSION_STRING; // Throws
        if (m_negotiated_protocol_version < SyncConnection::SERVER_LOG_PROTOCOL_VERSION) {
            // This isn't a real X-Appservices-Request-Id, but it should be enough to test with
            response.headers["X-Appservices-Request-Id"] = get_appservices_request_id();
        }
    }

    void read_error(std::error_code ec)
    {
        REALM_ASSERT(ec != util::error::operation_aborted);
        if (ec == util::MiscExtErrors::end_of_input || ec == util::error::connection_reset) {
            // Suicide
            close_due_to_close_by_client(ec); // Throws
            return;
        }
        if (ec == util::MiscExtErrors::delim_not_found) {
            logger.error("Input message head delimited not found"); // Throws
            close_due_to_error(ec);                                 // Throws
            return;
        }

        logger.error("Reading failed: %1", ec.message()); // Throws

        // Suicide
        close_due_to_error(ec); // Throws
    }

    void write_error(std::error_code ec)
    {
        REALM_ASSERT(ec != util::error::operation_aborted);
        if (ec == util::error::broken_pipe || ec == util::error::connection_reset) {
            // Suicide
            close_due_to_close_by_client(ec); // Throws
            return;
        }
        logger.error("Writing failed: %1", ec.message()); // Throws

        // Suicide
        close_due_to_error(ec); // Throws
    }

    void close_due_to_close_by_client(std::error_code ec)
    {
        auto log_level = (ec == util::MiscExtErrors::end_of_input ? Logger::Level::detail : Logger::Level::info);
        // Suicide
        terminate(log_level, "HTTP connection closed by client: %1", ec.message()); // Throws
    }

    void close_due_to_error(std::error_code ec)
    {
        // Suicide
        terminate(Logger::Level::error, "HTTP connection closed due to error: %1",
                  ec.message()); // Throws
    }

    static std::string make_logger_prefix(int_fast64_t id)
    {
        std::ostringstream out;
        out.imbue(std::locale::classic());
        out << "HTTP Connection[" << id << "]: "; // Throws
        return out.str();                         // Throws
    }
};


class DownloadHistoryEntryHandler : public ServerHistory::HistoryEntryHandler {
public:
    std::size_t num_changesets = 0;
    std::size_t accum_original_size = 0;
    std::size_t accum_compacted_size = 0;

    DownloadHistoryEntryHandler(ServerProtocol& protocol, OutputBuffer& buffer, util::Logger& logger) noexcept
        : m_protocol{protocol}
        , m_buffer{buffer}
        , m_logger{logger}
    {
    }

    void handle(version_type server_version, const HistoryEntry& entry, size_t original_size) override
    {
        version_type client_version = entry.remote_version;
        ServerProtocol::ChangesetInfo info{server_version, client_version, entry, original_size};
        m_protocol.insert_single_changeset_download_message(m_buffer, info, m_logger); // Throws
        ++num_changesets;
        accum_original_size += original_size;
        accum_compacted_size += entry.changeset.size();
    }

private:
    ServerProtocol& m_protocol;
    OutputBuffer& m_buffer;
    util::Logger& m_logger;
};


// ============================ Session ============================

//                        Need cli-   Send     IDENT     UNBIND              ERROR
//   Protocol             ent file    IDENT    message   message   Error     message
//   state                identifier  message  received  received  occurred  sent
// ---------------------------------------------------------------------------------
//   AllocatingIdent      yes         yes      no        no        no        no
//   SendIdent            no          yes      no        no        no        no
//   WaitForIdent         no          no       no        no        no        no
//   WaitForUnbind        maybe       no       yes       no        no        no
//   SendError            maybe       maybe    maybe     no        yes       no
//   WaitForUnbindErr     maybe       maybe    maybe     no        yes       yes
//   SendUnbound          maybe       maybe    maybe     yes       maybe     no
//
//
//   Condition                      Expression
// ----------------------------------------------------------
//   Need client file identifier    need_client_file_ident()
//   Send IDENT message             must_send_ident_message()
//   IDENT message received         ident_message_received()
//   UNBIND message received        unbind_message_received()
//   Error occurred                 error_occurred()
//   ERROR message sent             m_error_message_sent
//
//
//   Protocol
//   state                Will send              Can receive
// -----------------------------------------------------------------------
//   AllocatingIdent      none                   UNBIND
//   SendIdent            IDENT                  UNBIND
//   WaitForIdent         none                   IDENT, UNBIND
//   WaitForUnbind        DOWNLOAD, TRANSACT,    UPLOAD, TRANSACT, MARK,
//                        MARK, ALLOC            ALLOC, UNBIND
//   SendError            ERROR                  any
//   WaitForUnbindErr     none                   any
//   SendUnbound          UNBOUND                none
//
class Session final : private FileIdentReceiver {
public:
    util::PrefixLogger logger;

    Session(SyncConnection& conn, session_ident_type session_ident)
        : logger{util::LogCategory::server, make_logger_prefix(session_ident), conn.logger_ptr} // Throws
        , m_connection{conn}
        , m_session_ident{session_ident}
    {
    }

    ~Session() noexcept
    {
        REALM_ASSERT(!is_enlisted_to_send());
        detach_from_server_file();
    }

    SyncConnection& get_connection() noexcept
    {
        return m_connection;
    }

    const Optional<std::array<char, 64>>& get_encryption_key()
    {
        return m_connection.get_server().get_config().encryption_key;
    }

    session_ident_type get_session_ident() const noexcept
    {
        return m_session_ident;
    }

    ServerProtocol& get_server_protocol() noexcept
    {
        return m_connection.get_server_protocol();
    }

    bool need_client_file_ident() const noexcept
    {
        return (m_file_ident_request != 0);
    }

    bool must_send_ident_message() const noexcept
    {
        return m_send_ident_message;
    }

    bool ident_message_received() const noexcept
    {
        return m_client_file_ident != 0;
    }

    bool unbind_message_received() const noexcept
    {
        return m_unbind_message_received;
    }

    bool error_occurred() const noexcept
    {
        return int(m_error_code) != 0;
    }

    bool relayed_alloc_request_in_progress() const noexcept
    {
        return (need_client_file_ident() || m_allocated_file_ident.ident != 0);
    }

    // Returns the file identifier (always a nonzero value) of the client side
    // file if ident_message_received() returns true. Otherwise it returns zero.
    file_ident_type get_client_file_ident() const noexcept
    {
        return m_client_file_ident;
    }

    void initiate()
    {
        logger.detail("Session initiated", m_session_ident); // Throws
    }

    void terminate()
    {
        logger.detail("Session terminated", m_session_ident); // Throws
    }

    // Initiate the deactivation process, if it has not been initiated already
    // by the client.
    //
    // IMPORTANT: This function must not be called with protocol versions
    // earlier than 23.
    //
    // The deactivation process will eventually lead to termination of the
    // session.
    //
    // The session will detach itself from the server file when the deactivation
    // process is initiated, regardless of whether it is initiated by the
    // client, or by calling this function.
    void initiate_deactivation(ProtocolError error_code)
    {
        REALM_ASSERT(is_session_level_error(error_code));
        REALM_ASSERT(!error_occurred()); // Must only be called once

        // If the UNBIND message has been received, then the client has
        // initiated the deactivation process already.
        if (REALM_LIKELY(!unbind_message_received())) {
            detach_from_server_file();
            m_error_code = error_code;
            // Protocol state is now SendError
            ensure_enlisted_to_send();
            return;
        }
        // Protocol state was SendUnbound, and remains unchanged
    }

    bool is_enlisted_to_send() const noexcept
    {
        return m_next != nullptr;
    }

    void ensure_enlisted_to_send() noexcept
    {
        if (!is_enlisted_to_send())
            enlist_to_send();
    }

    void enlist_to_send() noexcept
    {
        m_connection.enlist_to_send(this);
    }

    // Overriding memeber function in FileIdentReceiver
    void receive_file_ident(SaltedFileIdent file_ident) override final
    {
        // Protocol state must be AllocatingIdent or WaitForUnbind
        if (!ident_message_received()) {
            REALM_ASSERT(need_client_file_ident());
            REALM_ASSERT(m_send_ident_message);
        }
        else {
            REALM_ASSERT(!m_send_ident_message);
        }
        REALM_ASSERT(!unbind_message_received());
        REALM_ASSERT(!error_occurred());
        REALM_ASSERT(!m_error_message_sent);

        m_file_ident_request = 0;
        m_allocated_file_ident = file_ident;

        // If the protocol state was AllocatingIdent, it is now SendIdent,
        // otherwise it continues to be WaitForUnbind.

        logger.debug("Acquired outbound salted file identifier (%1, %2)", file_ident.ident,
                     file_ident.salt); // Throws

        ensure_enlisted_to_send();
    }

    // Called by the associated connection object when this session is granted
    // an opportunity to initiate the sending of a message.
    //
    // This function may lead to the destruction of the session object
    // (suicide).
    void send_message()
    {
        if (REALM_LIKELY(!unbind_message_received())) {
            if (REALM_LIKELY(!error_occurred())) {
                if (REALM_LIKELY(ident_message_received())) {
                    // State is WaitForUnbind.
                    bool relayed_alloc = (m_allocated_file_ident.ident != 0);
                    if (REALM_LIKELY(!relayed_alloc)) {
                        // Send DOWNLOAD or MARK.
                        continue_history_scan(); // Throws
                        // Session object may have been
                        // destroyed at this point (suicide)
                        return;
                    }
                    send_alloc_message(); // Throws
                    return;
                }
                // State is SendIdent
                send_ident_message(); // Throws
                return;
            }
            // State is SendError
            send_error_message(); // Throws
            return;
        }
        // State is SendUnbound
        send_unbound_message(); // Throws
        terminate();            // Throws
        m_connection.discard_session(m_session_ident);
        // This session is now destroyed!
    }

    bool receive_bind_message(std::string path, std::string signed_user_token, bool need_client_file_ident,
                              bool is_subserver, ProtocolError& error)
    {
        if (logger.would_log(util::Logger::Level::info)) {
            logger.detail("Received: BIND(server_path=%1, signed_user_token='%2', "
                          "need_client_file_ident=%3, is_subserver=%4)",
                          path, short_token_fmt(signed_user_token), int(need_client_file_ident),
                          int(is_subserver)); // Throws
        }

        ServerImpl& server = m_connection.get_server();
        _impl::VirtualPathComponents virt_path_components =
            _impl::parse_virtual_path(server.get_root_dir(), path); // Throws

        if (!virt_path_components.is_valid) {
            logger.error("Bad virtual path (message_type='bind', path='%1', "
                         "signed_user_token='%2')",
                         path,
                         short_token_fmt(signed_user_token)); // Throws
            error = ProtocolError::illegal_realm_path;
            return false;
        }

        // The user has proper permissions at this stage.

        m_server_file = server.get_or_create_file(path); // Throws

        m_server_file->add_unidentified_session(this); // Throws

        logger.info("Client info: (path='%1', from=%2, protocol=%3) %4", path, m_connection.get_remote_endpoint(),
                    m_connection.get_client_protocol_version(),
                    m_connection.get_client_user_agent()); // Throws

        m_is_subserver = is_subserver;
        if (REALM_LIKELY(!need_client_file_ident)) {
            // Protocol state is now WaitForUnbind
            return true;
        }

        // FIXME: We must make a choice about client file ident for read only
        // sessions. They should have a special read-only client file ident.
        file_ident_type proxy_file = 0; // No proxy
        ClientType client_type = (is_subserver ? ClientType::subserver : ClientType::regular);
        m_file_ident_request = m_server_file->request_file_ident(*this, proxy_file, client_type); // Throws
        m_send_ident_message = true;
        // Protocol state is now AllocatingIdent

        return true;
    }

    bool receive_ident_message(file_ident_type client_file_ident, salt_type client_file_ident_salt,
                               version_type scan_server_version, version_type scan_client_version,
                               version_type latest_server_version, salt_type latest_server_version_salt,
                               ProtocolError& error)
    {
        // Protocol state must be WaitForIdent
        REALM_ASSERT(!need_client_file_ident());
        REALM_ASSERT(!m_send_ident_message);
        REALM_ASSERT(!ident_message_received());
        REALM_ASSERT(!unbind_message_received());
        REALM_ASSERT(!error_occurred());
        REALM_ASSERT(!m_error_message_sent);

        logger.debug("Received: IDENT(client_file_ident=%1, client_file_ident_salt=%2, "
                     "scan_server_version=%3, scan_client_version=%4, latest_server_version=%5, "
                     "latest_server_version_salt=%6)",
                     client_file_ident, client_file_ident_salt, scan_server_version, scan_client_version,
                     latest_server_version, latest_server_version_salt); // Throws

        SaltedFileIdent client_file_ident_2 = {client_file_ident, client_file_ident_salt};
        DownloadCursor download_progress = {scan_server_version, scan_client_version};
        SaltedVersion server_version_2 = {latest_server_version, latest_server_version_salt};
        ClientType client_type = (m_is_subserver ? ClientType::subserver : ClientType::regular);
        UploadCursor upload_threshold = {0, 0};
        version_type locked_server_version = 0;
        BootstrapError error_2 =
            m_server_file->bootstrap_client_session(client_file_ident_2, download_progress, server_version_2,
                                                    client_type, upload_threshold, locked_server_version,
                                                    logger); // Throws
        switch (error_2) {
            case BootstrapError::no_error:
                break;
            case BootstrapError::client_file_expired:
                logger.warn("Client (%1) expired", client_file_ident); // Throws
                error = ProtocolError::client_file_expired;
                return false;
            case BootstrapError::bad_client_file_ident:
                logger.error("Bad client file ident (%1) in IDENT message",
                             client_file_ident); // Throws
                error = ProtocolError::bad_client_file_ident;
                return false;
            case BootstrapError::bad_client_file_ident_salt:
                logger.error("Bad client file identifier salt (%1) in IDENT message",
                             client_file_ident_salt); // Throws
                error = ProtocolError::diverging_histories;
                return false;
            case BootstrapError::bad_download_server_version:
                logger.error("Bad download progress server version in IDENT message"); // Throws
                error = ProtocolError::bad_server_version;
                return false;
            case BootstrapError::bad_download_client_version:
                logger.error("Bad download progress client version in IDENT message"); // Throws
                error = ProtocolError::bad_client_version;
                return false;
            case BootstrapError::bad_server_version:
                logger.error("Bad server version (message_type='ident')"); // Throws
                error = ProtocolError::bad_server_version;
                return false;
            case BootstrapError::bad_server_version_salt:
                logger.error("Bad server version salt in IDENT message"); // Throws
                error = ProtocolError::diverging_histories;
                return false;
            case BootstrapError::bad_client_type:
                logger.error("Bad client type (%1) in IDENT message", int(client_type)); // Throws
                error = ProtocolError::bad_client_file_ident; // FIXME: Introduce new protocol-level error
                                                              // `bad_client_type`.
                return false;
        }

        // Make sure there is no other session currently associcated with the
        // same client-side file
        if (Session* other_sess = m_server_file->get_identified_session(client_file_ident)) {
            SyncConnection& other_conn = other_sess->get_connection();
            // It is a protocol violation if the other session is associated
            // with the same connection
            if (&other_conn == &m_connection) {
                logger.error("Client file already bound in other session associated with "
                             "the same connection"); // Throws
                error = ProtocolError::bound_in_other_session;
                return false;
            }
            // When the other session is associated with a different connection
            // (`other_conn`), the clash may be due to the server not yet having
            // realized that the other connection has been closed by the
            // client. If so, the other connention is a "zombie". In the
            // interest of getting rid of zombie connections as fast as
            // possible, we shall assume that a clash with a session in another
            // connection is always due to that other connection being a
            // zombie. And when such a situation is detected, we want to close
            // the zombie connection immediately.
            auto log_level = Logger::Level::detail;
            other_conn.terminate(log_level,
                                 "Sync connection closed (superseded session)"); // Throws
        }

        logger.info("Bound to client file (client_file_ident=%1)", client_file_ident); // Throws

        send_log_message(util::Logger::Level::debug, util::format("Session %1 bound to client file ident %2",
                                                                  m_session_ident, client_file_ident));

        m_server_file->identify_session(this, client_file_ident); // Throws

        m_client_file_ident = client_file_ident;
        m_download_progress = download_progress;
        m_upload_threshold = upload_threshold;
        m_locked_server_version = locked_server_version;

        ServerImpl& server = m_connection.get_server();
        const Server::Config& config = server.get_config();
        m_disable_download = (config.disable_download_for.count(client_file_ident) != 0);

        if (REALM_UNLIKELY(config.session_bootstrap_callback)) {
            config.session_bootstrap_callback(m_server_file->get_virt_path(),
                                              client_file_ident); // Throws
        }

        // Protocol  state is now WaitForUnbind
        enlist_to_send();
        return true;
    }

    bool receive_upload_message(version_type progress_client_version, version_type progress_server_version,
                                version_type locked_server_version, const UploadChangesets& upload_changesets,
                                ProtocolError& error)
    {
        // Protocol state must be WaitForUnbind
        REALM_ASSERT(!m_send_ident_message);
        REALM_ASSERT(ident_message_received());
        REALM_ASSERT(!unbind_message_received());
        REALM_ASSERT(!error_occurred());
        REALM_ASSERT(!m_error_message_sent);

        logger.detail("Received: UPLOAD(progress_client_version=%1, progress_server_version=%2, "
                      "locked_server_version=%3, num_changesets=%4)",
                      progress_client_version, progress_server_version, locked_server_version,
                      upload_changesets.size()); // Throws

        // We are unable to reproduce the cursor object for the upload progress
        // when the protocol version is less than 29, because the client does
        // not provide the required information. When the protocol version is
        // less than 25, we can always get a consistent cursor by taking it from
        // the changeset that was uploaded last, but in protocol versions 25,
        // 26, 27, and 28, things are more complicated. Here, we receive new
        // values for `last_integrated_server_version` which we cannot afford to
        // ignore, but we do not know what client versions they correspond
        // to. Fortunately, we can produce a cursor that works, and is mutually
        // consistent with previous cursors, by simply bumping
        // `upload_progress.client_version` when
        // `upload_progress.last_intgerated_server_version` grows.
        //
        // To see that this scheme works, consider the last changeset, A, that
        // will have already been uploaded and integrated at the beginning of
        // the next session, and the first changeset, B, that follows A in the
        // client side history, and is not upload skippable (of local origin and
        // nonempty). We then need to show that A will be skipped, if uploaded
        // in the next session, but B will not.
        //
        // Let V be the client version produced by A, and let T be the value of
        // `upload_progress.client_version` as determined in this session, which
        // is used as threshold in the next session. Then we know that A is
        // skipped during the next session if V is less than, or equal to T. If
        // the protocol version is at least 29, the protocol requires that T is
        // greater than, or equal to V. If the protocol version is less than 25,
        // T will be equal to V. Finally, if the protocol version is 25, 26, 27,
        // or 28, we construct T such that it is always greater than, or equal
        // to V, so in all cases, A will be skipped during the next session.
        //
        // Let W be the client version on which B is based. We then know that B
        // will be retained if, and only if W is greater than, or equalto T. If
        // the protocol version is at least 29, we know that T is less than, or
        // equal to W, since B is not integrated until the next session. If the
        // protocol version is less tahn 25, we know that T is V. Since V must
        // be less than, or equal to W, we again know that T is less than, or
        // equal to W. Finally, if the protocol version is 25, 26, 27, or 28, we
        // construct T such that it is equal to V + N, where N is the number of
        // observed increments in `last_integrated_server_version` since the
        // client version prodiced by A. For each of these observed increments,
        // there must have been a distinct new client version, but all these
        // client versions must be less than, or equal to W, since B is not
        // integrated until the next session. Therefore, we know that T = V + N
        // is less than, or qual to W. So, in all cases, B will not skipped
        // during the next session.
        int protocol_version = m_connection.get_client_protocol_version();
        static_cast<void>(protocol_version); // No protocol diversion (yet)

        UploadCursor upload_progress;
        upload_progress = {progress_client_version, progress_server_version};

        // `upload_progress.client_version` must be nondecreasing across the
        // session.
        bool good_1 = (upload_progress.client_version >= m_upload_progress.client_version);
        if (REALM_UNLIKELY(!good_1)) {
            logger.error("Decreasing client version in upload progress (%1 < %2)", upload_progress.client_version,
                         m_upload_progress.client_version); // Throws
            error = ProtocolError::bad_client_version;
            return false;
        }
        // `upload_progress.last_integrated_server_version` must be a version
        // that the client can have heard about.
        bool good_2 = (upload_progress.last_integrated_server_version <= m_download_progress.server_version);
        if (REALM_UNLIKELY(!good_2)) {
            logger.error("Bad last integrated server version in upload progress (%1 > %2)",
                         upload_progress.last_integrated_server_version,
                         m_download_progress.server_version); // Throws
            error = ProtocolError::bad_server_version;
            return false;
        }

        // `upload_progress` must be consistent.
        if (REALM_UNLIKELY(!is_consistent(upload_progress))) {
            logger.error("Upload progress is inconsistent (%1, %2)", upload_progress.client_version,
                         upload_progress.last_integrated_server_version); // Throws
            error = ProtocolError::bad_server_version;
            return false;
        }
        // `upload_progress` and `m_upload_threshold` must be mutually
        // consistent.
        if (REALM_UNLIKELY(!are_mutually_consistent(upload_progress, m_upload_threshold))) {
            logger.error("Upload progress (%1, %2) is mutually inconsistent with "
                         "threshold (%3, %4)",
                         upload_progress.client_version, upload_progress.last_integrated_server_version,
                         m_upload_threshold.client_version,
                         m_upload_threshold.last_integrated_server_version); // Throws
            error = ProtocolError::bad_server_version;
            return false;
        }
        // `upload_progress` and `m_upload_progress` must be mutually
        // consistent.
        if (REALM_UNLIKELY(!are_mutually_consistent(upload_progress, m_upload_progress))) {
            logger.error("Upload progress (%1, %2) is mutually inconsistent with previous "
                         "upload progress (%3, %4)",
                         upload_progress.client_version, upload_progress.last_integrated_server_version,
                         m_upload_progress.client_version,
                         m_upload_progress.last_integrated_server_version); // Throws
            error = ProtocolError::bad_server_version;
            return false;
        }

        version_type locked_server_version_2 = locked_server_version;

        // `locked_server_version_2` must be nondecreasing over the lifetime of
        // the client-side file.
        if (REALM_UNLIKELY(locked_server_version_2 < m_locked_server_version)) {
            logger.error("Decreasing locked server version (%1 < %2)", locked_server_version_2,
                         m_locked_server_version); // Throws
            error = ProtocolError::bad_server_version;
            return false;
        }
        // `locked_server_version_2` must be a version that the client can have
        // heard about.
        if (REALM_UNLIKELY(locked_server_version_2 > m_download_progress.server_version)) {
            logger.error("Bad locked server version (%1 > %2)", locked_server_version_2,
                         m_download_progress.server_version); // Throws
            error = ProtocolError::bad_server_version;
            return false;
        }

        std::size_t num_previously_integrated_changesets = 0;
        if (!upload_changesets.empty()) {
            UploadCursor up = m_upload_progress;
            for (const ServerProtocol::UploadChangeset& uc : upload_changesets) {
                // `uc.upload_cursor.client_version` must be increasing across
                // all the changesets in this UPLOAD message, and all must be
                // greater than upload_progress.client_version of previous
                // UPLOAD message.
                if (REALM_UNLIKELY(uc.upload_cursor.client_version <= up.client_version)) {
                    logger.error("Nonincreasing client version in upload cursor of uploaded "
                                 "changeset (%1 <= %2)",
                                 uc.upload_cursor.client_version,
                                 up.client_version); // Throws
                    error = ProtocolError::bad_client_version;
                    return false;
                }
                // `uc.upload_progress` must be consistent.
                if (REALM_UNLIKELY(!is_consistent(uc.upload_cursor))) {
                    logger.error("Upload cursor of uploaded changeset is inconsistent (%1, %2)",
                                 uc.upload_cursor.client_version,
                                 uc.upload_cursor.last_integrated_server_version); // Throws
                    error = ProtocolError::bad_server_version;
                    return false;
                }
                // `uc.upload_progress` must be mutually consistent with
                // previous upload cursor.
                if (REALM_UNLIKELY(!are_mutually_consistent(uc.upload_cursor, up))) {
                    logger.error("Upload cursor of uploaded changeset (%1, %2) is mutually "
                                 "inconsistent with previous upload cursor (%3, %4)",
                                 uc.upload_cursor.client_version, uc.upload_cursor.last_integrated_server_version,
                                 up.client_version, up.last_integrated_server_version); // Throws
                    error = ProtocolError::bad_server_version;
                    return false;
                }
                // `uc.upload_progress` must be mutually consistent with
                // threshold, that is, for changesets that have not previously
                // been integrated, it is important that the specified value of
                // `last_integrated_server_version` is greater than, or equal to
                // the reciprocal history base version.
                bool consistent_with_threshold = are_mutually_consistent(uc.upload_cursor, m_upload_threshold);
                if (REALM_UNLIKELY(!consistent_with_threshold)) {
                    logger.error("Upload cursor of uploaded changeset (%1, %2) is mutually "
                                 "inconsistent with threshold (%3, %4)",
                                 uc.upload_cursor.client_version, uc.upload_cursor.last_integrated_server_version,
                                 m_upload_threshold.client_version,
                                 m_upload_threshold.last_integrated_server_version); // Throws
                    error = ProtocolError::bad_server_version;
                    return false;
                }
                bool previously_integrated = (uc.upload_cursor.client_version <= m_upload_threshold.client_version);
                if (previously_integrated)
                    ++num_previously_integrated_changesets;
                up = uc.upload_cursor;
            }
            // `upload_progress.client_version` must be greater than, or equal
            // to client versions produced by each of the changesets in this
            // UPLOAD message.
            if (REALM_UNLIKELY(up.client_version > upload_progress.client_version)) {
                logger.error("Upload progress less than client version produced by uploaded "
                             "changeset (%1 > %2)",
                             up.client_version,
                             upload_progress.client_version); // Throws
                error = ProtocolError::bad_client_version;
                return false;
            }
            // The upload cursor of last uploaded changeset must be mutually
            // consistent with the reported upload progress.
            if (REALM_UNLIKELY(!are_mutually_consistent(up, upload_progress))) {
                logger.error("Upload cursor (%1, %2) of last uploaded changeset is mutually "
                             "inconsistent with upload progress (%3, %4)",
                             up.client_version, up.last_integrated_server_version, upload_progress.client_version,
                             upload_progress.last_integrated_server_version); // Throws
                error = ProtocolError::bad_server_version;
                return false;
            }
        }

        // FIXME: Part of a very poor man's substitute for a proper backpressure
        // scheme.
        if (REALM_UNLIKELY(!m_server_file->can_add_changesets_from_downstream())) {
            logger.debug("Terminating uploading session because buffer is full"); // Throws
            // Using this exact error code, because it causes `try_again` flag
            // to be set to true, which causes the client to wait for about 5
            // minuites before trying to connect again.
            error = ProtocolError::connection_closed;
            return false;
        }

        m_upload_progress = upload_progress;

        bool have_real_upload_progress = (upload_progress.client_version > m_upload_threshold.client_version);
        bool bump_locked_server_version = (locked_server_version_2 > m_locked_server_version);

        std::size_t num_changesets_to_integrate = upload_changesets.size() - num_previously_integrated_changesets;
        REALM_ASSERT(have_real_upload_progress || num_changesets_to_integrate == 0);

        bool have_anything_to_do = (have_real_upload_progress || bump_locked_server_version);
        if (!have_anything_to_do)
            return true;

        if (!have_real_upload_progress)
            upload_progress = m_upload_threshold;

        if (num_previously_integrated_changesets > 0) {
            logger.detail("Ignoring %1 previously integrated changesets",
                          num_previously_integrated_changesets); // Throws
        }
        if (num_changesets_to_integrate > 0) {
            logger.detail("Initiate integration of %1 remote changesets",
                          num_changesets_to_integrate); // Throws
        }

        REALM_ASSERT(m_server_file);
        ServerFile& file = *m_server_file;
        std::size_t offset = num_previously_integrated_changesets;
        file.add_changesets_from_downstream(m_client_file_ident, upload_progress, locked_server_version_2,
                                            upload_changesets.data() + offset, num_changesets_to_integrate); // Throws

        m_locked_server_version = locked_server_version_2;
        return true;
    }

    bool receive_mark_message(request_ident_type request_ident, ProtocolError&)
    {
        // Protocol state must be WaitForUnbind
        REALM_ASSERT(!m_send_ident_message);
        REALM_ASSERT(ident_message_received());
        REALM_ASSERT(!unbind_message_received());
        REALM_ASSERT(!error_occurred());
        REALM_ASSERT(!m_error_message_sent);

        logger.debug("Received: MARK(request_ident=%1)", request_ident); // Throws

        m_download_completion_request = request_ident;

        ensure_enlisted_to_send();
        return true;
    }

    // Returns true if the deactivation process has been completed, at which
    // point the caller (SyncConnection::receive_unbind_message()) should
    // terminate the session.
    //
    // CAUTION: This function may commit suicide!
    void receive_unbind_message()
    {
        // Protocol state may be anything but SendUnbound
        REALM_ASSERT(!m_unbind_message_received);

        logger.detail("Received: UNBIND"); // Throws

        detach_from_server_file();
        m_unbind_message_received = true;

        // Detect completion of the deactivation process
        if (m_error_message_sent) {
            // Deactivation process completed
            terminate(); // Throws
            m_connection.discard_session(m_session_ident);
            // This session is now destroyed!
            return;
        }

        // Protocol state is now SendUnbound
        ensure_enlisted_to_send();
    }

    void receive_error_message(session_ident_type, int, std::string_view)
    {
        REALM_ASSERT(!m_unbind_message_received);

        logger.detail("Received: ERROR"); // Throws
    }

private:
    SyncConnection& m_connection;

    const session_ident_type m_session_ident;

    // Not null if, and only if this session is in
    // m_connection.m_sessions_enlisted_to_send.
    Session* m_next = nullptr;

    // Becomes nonnull when the BIND message is received, if no error occurs. Is
    // reset to null when the deactivation process is initiated, either when the
    // UNBIND message is recieved, or when initiate_deactivation() is called.
    util::bind_ptr<ServerFile> m_server_file;

    bool m_disable_download = false;
    bool m_is_subserver = false;

    using file_ident_request_type = ServerFile::file_ident_request_type;

    // When nonzero, this session has an outstanding request for a client file
    // identifier.
    file_ident_request_type m_file_ident_request = 0;

    // Payload for next outgoing ALLOC message.
    SaltedFileIdent m_allocated_file_ident = {0, 0};

    // Zero until the session receives an IDENT message from the client.
    file_ident_type m_client_file_ident = 0;

    // Zero until initiate_deactivation() is called.
    ProtocolError m_error_code = {};

    // The current point of progression of the download process. Set to (<server
    // version>, <client version>) of the IDENT message when the IDENT message
    // is received. At the time of return from continue_history_scan(), it
    // points to the latest server version such that all preceding changesets in
    // the server-side history have been downloaded, are currently being
    // downloaded, or are *download excluded*.
    DownloadCursor m_download_progress = {0, 0};

    request_ident_type m_download_completion_request = 0;

    // Records the progress of the upload process. Used to check that the client
    // uploads changesets in order. Also, when m_upload_progress >
    // m_upload_threshold, m_upload_progress works as a cache of the persisted
    // version of the upload progress.
    UploadCursor m_upload_progress = {0, 0};

    // Initialized on reception of the IDENT message. Specifies the actual
    // upload progress (as recorded on the server-side) at the beginning of the
    // session, and it remains fixed throughout the session.
    //
    // m_upload_threshold includes the progress resulting from the received
    // changesets that have not yet been integrated (only relevant for
    // synchronous backup).
    UploadCursor m_upload_threshold = {0, 0};

    // Works partially as a cache of the persisted value, and partially as a way
    // of checking that the client respects that it can never decrease.
    version_type m_locked_server_version = 0;

    bool m_send_ident_message = false;
    bool m_unbind_message_received = false;
    bool m_error_message_sent = false;

    /// m_one_download_message_sent denotes whether at least one DOWNLOAD message
    /// has been sent in the current session. The variable is used to ensure
    /// that a DOWNLOAD message is always sent in a session. The received
    /// DOWNLOAD message is needed by the client to ensure that its current
    /// download progress is up to date.
    bool m_one_download_message_sent = false;

    static std::string make_logger_prefix(session_ident_type session_ident)
    {
        std::ostringstream out;
        out.imbue(std::locale::classic());
        out << "Session[" << session_ident << "]: "; // Throws
        return out.str();                            // Throws
    }

    // Scan the history for changesets to be downloaded.
    // If the history is longer than the end point of the previous scan,
    // a DOWNLOAD message will be sent.
    // A MARK message is sent if no DOWNLOAD message is sent, and the client has
    // requested to be notified about download completion.
    // In case neither a DOWNLOAD nor a MARK is sent, no message is sent.
    //
    // This function may lead to the destruction of the session object
    // (suicide).
    void continue_history_scan()
    {
        // Protocol state must be WaitForUnbind
        REALM_ASSERT(!m_send_ident_message);
        REALM_ASSERT(ident_message_received());
        REALM_ASSERT(!unbind_message_received());
        REALM_ASSERT(!error_occurred());
        REALM_ASSERT(!m_error_message_sent);
        REALM_ASSERT(!is_enlisted_to_send());

        SaltedVersion last_server_version = m_server_file->get_salted_sync_version();
        REALM_ASSERT(last_server_version.version >= m_download_progress.server_version);

        ServerImpl& server = m_connection.get_server();
        const Server::Config& config = server.get_config();
        if (REALM_UNLIKELY(m_disable_download))
            return;

        bool have_more_to_scan =
            (last_server_version.version > m_download_progress.server_version || !m_one_download_message_sent);
        if (have_more_to_scan) {
            m_server_file->register_client_access(m_client_file_ident);     // Throws
            const ServerHistory& history = m_server_file->access().history; // Throws
            const char* body;
            std::size_t uncompressed_body_size;
            std::size_t compressed_body_size = 0;
            bool body_is_compressed = false;
            version_type end_version = last_server_version.version;
            DownloadCursor download_progress;
            UploadCursor upload_progress = {0, 0};
            std::uint_fast64_t downloadable_bytes = 0;
            std::size_t num_changesets;
            std::size_t accum_original_size;
            std::size_t accum_compacted_size;
            ServerProtocol& protocol = get_server_protocol();
            bool enable_cache = (config.enable_download_bootstrap_cache && m_download_progress.server_version == 0 &&
                                 m_upload_progress.client_version == 0 && m_upload_threshold.client_version == 0);
            DownloadCache& cache = m_server_file->get_download_cache();
            bool fetch_from_cache = (enable_cache && cache.body && end_version == cache.end_version);
            if (fetch_from_cache) {
                body = cache.body.get();
                uncompressed_body_size = cache.uncompressed_body_size;
                compressed_body_size = cache.compressed_body_size;
                body_is_compressed = cache.body_is_compressed;
                download_progress = cache.download_progress;
                downloadable_bytes = cache.downloadable_bytes;
                num_changesets = cache.num_changesets;
                accum_original_size = cache.accum_original_size;
                accum_compacted_size = cache.accum_compacted_size;
            }
            else {
                // Discard the old cached DOWNLOAD body before generating a new
                // one to be cached. This can make a big difference because the
                // size of that body can be very large (10GiB has been seen in a
                // real-world case).
                if (enable_cache)
                    cache.body = {};

                OutputBuffer& out = server.get_misc_buffers().download_message;
                out.reset();
                download_progress = m_download_progress;
                auto fetch_and_compress = [&](std::size_t max_download_size) {
                    DownloadHistoryEntryHandler handler{protocol, out, logger};
                    std::uint_fast64_t cumulative_byte_size_current;
                    std::uint_fast64_t cumulative_byte_size_total;
                    bool not_expired = history.fetch_download_info(
                        m_client_file_ident, download_progress, end_version, upload_progress, handler,
                        cumulative_byte_size_current, cumulative_byte_size_total,
                        max_download_size); // Throws
                    REALM_ASSERT(upload_progress.client_version >= download_progress.last_integrated_client_version);
                    SyncConnection& conn = get_connection();
                    if (REALM_UNLIKELY(!not_expired)) {
                        logger.debug("History scanning failed: Client file entry "
                                     "expired during session"); // Throws
                        conn.protocol_error(ProtocolError::client_file_expired, this);
                        // Session object may have been destroyed at this point
                        // (suicide).
                        return false;
                    }

                    downloadable_bytes = cumulative_byte_size_total - cumulative_byte_size_current;
                    uncompressed_body_size = out.size();
                    BinaryData uncompressed = {out.data(), uncompressed_body_size};
                    body = uncompressed.data();
                    std::size_t max_uncompressed = 1024;
                    if (uncompressed.size() > max_uncompressed) {
                        compression::CompressMemoryArena& arena = server.get_compress_memory_arena();
                        std::vector<char>& buffer = server.get_misc_buffers().compress;
                        compression::allocate_and_compress(arena, uncompressed, buffer); // Throws
                        if (buffer.size() < uncompressed.size()) {
                            body = buffer.data();
                            compressed_body_size = buffer.size();
                            body_is_compressed = true;
                        }
                    }
                    num_changesets = handler.num_changesets;
                    accum_original_size = handler.accum_original_size;
                    accum_compacted_size = handler.accum_compacted_size;
                    return true;
                };
                if (enable_cache) {
                    std::size_t max_download_size = std::numeric_limits<size_t>::max();
                    if (!fetch_and_compress(max_download_size)) { // Throws
                        // Session object may have been destroyed at this point
                        // (suicide).
                        return;
                    }
                    REALM_ASSERT(upload_progress.client_version == 0);
                    std::size_t body_size = (body_is_compressed ? compressed_body_size : uncompressed_body_size);
                    cache.body = std::make_unique<char[]>(body_size); // Throws
                    std::copy(body, body + body_size, cache.body.get());
                    cache.uncompressed_body_size = uncompressed_body_size;
                    cache.compressed_body_size = compressed_body_size;
                    cache.body_is_compressed = body_is_compressed;
                    cache.end_version = end_version;
                    cache.download_progress = download_progress;
                    cache.downloadable_bytes = downloadable_bytes;
                    cache.num_changesets = num_changesets;
                    cache.accum_original_size = accum_original_size;
                    cache.accum_compacted_size = accum_compacted_size;
                }
                else {
                    std::size_t max_download_size = config.max_download_size;
                    if (!fetch_and_compress(max_download_size)) { // Throws
                        // Session object may have been destroyed at this point
                        // (suicide).
                        return;
                    }
                }
            }

            OutputBuffer& out = m_connection.get_output_buffer();
            protocol.make_download_message(
                m_connection.get_client_protocol_version(), out, m_session_ident, download_progress.server_version,
                download_progress.last_integrated_client_version, last_server_version.version,
                last_server_version.salt, upload_progress.client_version,
                upload_progress.last_integrated_server_version, downloadable_bytes, num_changesets, body,
                uncompressed_body_size, compressed_body_size, body_is_compressed, logger); // Throws

            m_download_progress = download_progress;
            logger.debug("Setting of m_download_progress.server_version = %1",
                         m_download_progress.server_version); // Throws
            send_download_message();
            m_one_download_message_sent = true;

            enlist_to_send();
        }
        else if (m_download_completion_request) {
            // Send a MARK message
            request_ident_type request_ident = m_download_completion_request;
            send_mark_message(request_ident);  // Throws
            m_download_completion_request = 0; // Request handled
            enlist_to_send();
        }
    }

    void send_ident_message()
    {
        // Protocol state must be SendIdent
        REALM_ASSERT(!need_client_file_ident());
        REALM_ASSERT(m_send_ident_message);
        REALM_ASSERT(!ident_message_received());
        REALM_ASSERT(!unbind_message_received());
        REALM_ASSERT(!error_occurred());
        REALM_ASSERT(!m_error_message_sent);

        REALM_ASSERT(m_allocated_file_ident.ident != 0);

        file_ident_type client_file_ident = m_allocated_file_ident.ident;
        salt_type client_file_ident_salt = m_allocated_file_ident.salt;

        logger.debug("Sending: IDENT(client_file_ident=%1, client_file_ident_salt=%2)", client_file_ident,
                     client_file_ident_salt); // Throws

        ServerProtocol& protocol = get_server_protocol();
        OutputBuffer& out = m_connection.get_output_buffer();
        int protocol_version = m_connection.get_client_protocol_version();
        protocol.make_ident_message(protocol_version, out, m_session_ident, client_file_ident,
                                    client_file_ident_salt); // Throws
        m_connection.initiate_write_output_buffer();         // Throws

        m_allocated_file_ident.ident = 0; // Consumed
        m_send_ident_message = false;
        // Protocol state is now WaitForStateRequest or WaitForIdent
    }

    void send_download_message()
    {
        m_connection.initiate_write_output_buffer(); // Throws
    }

    void send_mark_message(request_ident_type request_ident)
    {
        logger.debug("Sending: MARK(request_ident=%1)", request_ident); // Throws

        ServerProtocol& protocol = get_server_protocol();
        OutputBuffer& out = m_connection.get_output_buffer();
        protocol.make_mark_message(out, m_session_ident, request_ident); // Throws
        m_connection.initiate_write_output_buffer();                     // Throws
    }

    void send_alloc_message()
    {
        // Protocol state must be WaitForUnbind
        REALM_ASSERT(!m_send_ident_message);
        REALM_ASSERT(ident_message_received());
        REALM_ASSERT(!unbind_message_received());
        REALM_ASSERT(!error_occurred());
        REALM_ASSERT(!m_error_message_sent);

        REALM_ASSERT(m_allocated_file_ident.ident != 0);

        // Relayed allocations are only allowed from protocol version 23 (old protocol).
        REALM_ASSERT(false);

        file_ident_type file_ident = m_allocated_file_ident.ident;

        logger.debug("Sending: ALLOC(file_ident=%1)", file_ident); // Throws

        ServerProtocol& protocol = get_server_protocol();
        OutputBuffer& out = m_connection.get_output_buffer();
        protocol.make_alloc_message(out, m_session_ident, file_ident); // Throws
        m_connection.initiate_write_output_buffer();                   // Throws

        m_allocated_file_ident.ident = 0; // Consumed

        // Other messages may be waiting to be sent.
        enlist_to_send();
    }

    void send_unbound_message()
    {
        // Protocol state must be SendUnbound
        REALM_ASSERT(unbind_message_received());
        REALM_ASSERT(!m_error_message_sent);

        logger.debug("Sending: UNBOUND"); // Throws

        ServerProtocol& protocol = get_server_protocol();
        OutputBuffer& out = m_connection.get_output_buffer();
        protocol.make_unbound_message(out, m_session_ident); // Throws
        m_connection.initiate_write_output_buffer();         // Throws
    }

    void send_error_message()
    {
        // Protocol state must be SendError
        REALM_ASSERT(!unbind_message_received());
        REALM_ASSERT(error_occurred());
        REALM_ASSERT(!m_error_message_sent);

        REALM_ASSERT(is_session_level_error(m_error_code));

        ProtocolError error_code = m_error_code;
        const char* message = get_protocol_error_message(int(error_code));
        std::size_t message_size = std::strlen(message);
        bool try_again = determine_try_again(error_code);

        logger.detail("Sending: ERROR(error_code=%1, message_size=%2, try_again=%3)", int(error_code), message_size,
                      try_again); // Throws

        ServerProtocol& protocol = get_server_protocol();
        OutputBuffer& out = m_connection.get_output_buffer();
        int protocol_version = m_connection.get_client_protocol_version();
        protocol.make_error_message(protocol_version, out, error_code, message, message_size, try_again,
                                    m_session_ident); // Throws
        m_connection.initiate_write_output_buffer();  // Throws

        m_error_message_sent = true;
        // Protocol state is now WaitForUnbindErr
    }

    void send_log_message(util::Logger::Level level, const std::string&& message)
    {
        if (m_connection.get_client_protocol_version() < SyncConnection::SERVER_LOG_PROTOCOL_VERSION) {
            return logger.log(level, message.c_str());
        }

        m_connection.send_log_message(level, std::move(message), m_session_ident);
    }

    // Idempotent
    void detach_from_server_file() noexcept
    {
        if (!m_server_file)
            return;
        ServerFile& file = *m_server_file;
        if (ident_message_received()) {
            file.remove_identified_session(m_client_file_ident);
        }
        else {
            file.remove_unidentified_session(this);
        }
        if (m_file_ident_request != 0)
            file.cancel_file_ident_request(m_file_ident_request);
        m_server_file.reset();
    }

    friend class SessionQueue;
};


// ============================ SessionQueue implementation ============================

void SessionQueue::push_back(Session* sess) noexcept
{
    REALM_ASSERT(!sess->m_next);
    if (m_back) {
        sess->m_next = m_back->m_next;
        m_back->m_next = sess;
    }
    else {
        sess->m_next = sess;
    }
    m_back = sess;
}


Session* SessionQueue::pop_front() noexcept
{
    Session* sess = nullptr;
    if (m_back) {
        sess = m_back->m_next;
        if (sess != m_back) {
            m_back->m_next = sess->m_next;
        }
        else {
            m_back = nullptr;
        }
        sess->m_next = nullptr;
    }
    return sess;
}


void SessionQueue::clear() noexcept
{
    if (m_back) {
        Session* sess = m_back;
        for (;;) {
            Session* next = sess->m_next;
            sess->m_next = nullptr;
            if (next == m_back)
                break;
            sess = next;
        }
        m_back = nullptr;
    }
}


// ============================ ServerFile implementation ============================

ServerFile::ServerFile(ServerImpl& server, ServerFileAccessCache& cache, const std::string& virt_path,
                       std::string real_path, bool disable_sync_to_disk)
    : logger{util::LogCategory::server, "ServerFile[" + virt_path + "]: ", server.logger_ptr}               // Throws
    , wlogger{util::LogCategory::server, "ServerFile[" + virt_path + "]: ", server.get_worker().logger_ptr} // Throws
    , m_server{server}
    , m_file{cache, real_path, virt_path, false, disable_sync_to_disk} // Throws
    , m_worker_file{server.get_worker().get_file_access_cache(), real_path, virt_path, true, disable_sync_to_disk}
{
}


ServerFile::~ServerFile() noexcept
{
    REALM_ASSERT(m_unidentified_sessions.empty());
    REALM_ASSERT(m_identified_sessions.empty());
    REALM_ASSERT(m_file_ident_request == 0);
}


void ServerFile::initialize()
{
    const ServerHistory& history = access().history; // Throws
    file_ident_type partial_file_ident = 0;
    version_type partial_progress_reference_version = 0;
    bool has_upstream_sync_status;
    history.get_status(m_version_info, has_upstream_sync_status, partial_file_ident,
                       partial_progress_reference_version); // Throws
    REALM_ASSERT(!has_upstream_sync_status);
    REALM_ASSERT(partial_file_ident == 0);
}


void ServerFile::activate() {}


// This function must be called only after a completed invocation of
// initialize(). Both functinos must only ever be called by the network event
// loop thread.
void ServerFile::register_client_access(file_ident_type) {}


auto ServerFile::request_file_ident(FileIdentReceiver& receiver, file_ident_type proxy_file,
                                    ClientType client_type) -> file_ident_request_type
{
    auto request = ++m_last_file_ident_request;
    m_file_ident_requests[request] = {&receiver, proxy_file, client_type}; // Throws

    on_work_added(); // Throws
    return request;
}


void ServerFile::cancel_file_ident_request(file_ident_request_type request) noexcept
{
    auto i = m_file_ident_requests.find(request);
    REALM_ASSERT(i != m_file_ident_requests.end());
    FileIdentRequestInfo& info = i->second;
    REALM_ASSERT(info.receiver);
    info.receiver = nullptr;
}


void ServerFile::add_unidentified_session(Session* sess)
{
    REALM_ASSERT(m_unidentified_sessions.count(sess) == 0);
    m_unidentified_sessions.insert(sess); // Throws
}


void ServerFile::identify_session(Session* sess, file_ident_type client_file_ident)
{
    REALM_ASSERT(m_unidentified_sessions.count(sess) == 1);
    REALM_ASSERT(m_identified_sessions.count(client_file_ident) == 0);

    m_identified_sessions[client_file_ident] = sess; // Throws
    m_unidentified_sessions.erase(sess);
}


void ServerFile::remove_unidentified_session(Session* sess) noexcept
{
    REALM_ASSERT(m_unidentified_sessions.count(sess) == 1);
    m_unidentified_sessions.erase(sess);
}


void ServerFile::remove_identified_session(file_ident_type client_file_ident) noexcept
{
    REALM_ASSERT(m_identified_sessions.count(client_file_ident) == 1);
    m_identified_sessions.erase(client_file_ident);
}


Session* ServerFile::get_identified_session(file_ident_type client_file_ident) noexcept
{
    auto i = m_identified_sessions.find(client_file_ident);
    if (i == m_identified_sessions.end())
        return nullptr;
    return i->second;
}

bool ServerFile::can_add_changesets_from_downstream() const noexcept
{
    return (m_blocked_changesets_from_downstream_byte_size < m_server.get_max_upload_backlog());
}


void ServerFile::add_changesets_from_downstream(file_ident_type client_file_ident, UploadCursor upload_progress,
                                                version_type locked_server_version, const UploadChangeset* changesets,
                                                std::size_t num_changesets)
{
    register_client_access(client_file_ident); // Throws

    bool dirty = false;

    IntegratableChangesetList& list = m_changesets_from_downstream[client_file_ident]; // Throws
    std::size_t num_bytes = 0;
    for (std::size_t i = 0; i < num_changesets; ++i) {
        const UploadChangeset& uc = changesets[i];
        auto& changesets = list.changesets;
        changesets.emplace_back(client_file_ident, uc.origin_timestamp, uc.origin_file_ident, uc.upload_cursor,
                                uc.changeset); // Throws
        num_bytes += uc.changeset.size();
        dirty = true;
    }

    REALM_ASSERT(upload_progress.client_version >= list.upload_progress.client_version);
    REALM_ASSERT(are_mutually_consistent(upload_progress, list.upload_progress));
    if (upload_progress.client_version > list.upload_progress.client_version) {
        list.upload_progress = upload_progress;
        dirty = true;
    }

    REALM_ASSERT(locked_server_version >= list.locked_server_version);
    if (locked_server_version > list.locked_server_version) {
        list.locked_server_version = locked_server_version;
        dirty = true;
    }

    if (REALM_LIKELY(dirty)) {
        if (num_changesets > 0) {
            on_changesets_from_downstream_added(num_changesets, num_bytes); // Throws
        }
        else {
            on_work_added(); // Throws
        }
    }
}


BootstrapError ServerFile::bootstrap_client_session(SaltedFileIdent client_file_ident,
                                                    DownloadCursor download_progress, SaltedVersion server_version,
                                                    ClientType client_type, UploadCursor& upload_progress,
                                                    version_type& locked_server_version, Logger& logger)
{
    // The Realm file may contain a later snapshot than the one reflected by
    // `m_sync_version`, but if so, the client cannot "legally" know about it.
    if (server_version.version > m_version_info.sync_version.version)
        return BootstrapError::bad_server_version;

    const ServerHistory& hist = access().history; // Throws
    BootstrapError error = hist.bootstrap_client_session(client_file_ident, download_progress, server_version,
                                                         client_type, upload_progress, locked_server_version,
                                                         logger); // Throws

    // FIXME: Rather than taking previously buffered changesets from the same
    // client file into account when determining the upload progress, and then
    // allowing for an error during the integration of those changesets to be
    // reported to, and terminate the new session, consider to instead postpone
    // the bootstrapping of the new session until all previously buffered
    // changesets from same client file have been fully processed.

    if (error == BootstrapError::no_error) {
        register_client_access(client_file_ident.ident); // Throws

        // If upload, or releaseing of server versions progressed further during
        // previous sessions than the persisted points, take that into account
        auto i = m_work.changesets_from_downstream.find(client_file_ident.ident);
        if (i != m_work.changesets_from_downstream.end()) {
            const IntegratableChangesetList& list = i->second;
            REALM_ASSERT(list.upload_progress.client_version >= upload_progress.client_version);
            upload_progress = list.upload_progress;
            REALM_ASSERT(list.locked_server_version >= locked_server_version);
            locked_server_version = list.locked_server_version;
        }
        auto j = m_changesets_from_downstream.find(client_file_ident.ident);
        if (j != m_changesets_from_downstream.end()) {
            const IntegratableChangesetList& list = j->second;
            REALM_ASSERT(list.upload_progress.client_version >= upload_progress.client_version);
            upload_progress = list.upload_progress;
            REALM_ASSERT(list.locked_server_version >= locked_server_version);
            locked_server_version = list.locked_server_version;
        }
    }

    return error;
}

// NOTE: This function is executed by the worker thread
void ServerFile::worker_process_work_unit(WorkerState& state)
{
    SteadyTimePoint start_time = steady_clock_now();
    milliseconds_type parallel_time = 0;

    Work& work = m_work;
    wlogger.debug("Work unit execution started"); // Throws

    if (work.has_primary_work) {
        if (REALM_UNLIKELY(!m_work.file_ident_alloc_slots.empty()))
            worker_allocate_file_identifiers(); // Throws

        if (!m_work.changesets_from_downstream.empty())
            worker_integrate_changes_from_downstream(state); // Throws
    }

    wlogger.debug("Work unit execution completed"); // Throws

    milliseconds_type time = steady_duration(start_time);
    milliseconds_type seq_time = time - parallel_time;
    m_server.m_seq_time.fetch_add(seq_time, std::memory_order_relaxed);
    m_server.m_par_time.fetch_add(parallel_time, std::memory_order_relaxed);

    // Pass control back to the network event loop thread
    network::Service& service = m_server.get_service();
    service.post([this](Status) {
        // FIXME: The safety of capturing `this` here, relies on the fact
        // that ServerFile objects currently are not destroyed until the
        // server object is destroyed.
        group_postprocess_stage_1(); // Throws
        // Suicide may have happened at this point
    }); // Throws
}


void ServerFile::on_changesets_from_downstream_added(std::size_t num_changesets, std::size_t num_bytes)
{
    m_num_changesets_from_downstream += num_changesets;

    if (num_bytes > 0) {
        m_blocked_changesets_from_downstream_byte_size += num_bytes;
        get_server().inc_byte_size_for_pending_downstream_changesets(num_bytes); // Throws
    }

    on_work_added(); // Throws
}


void ServerFile::on_work_added()
{
    if (m_has_blocked_work)
        return;
    m_has_blocked_work = true;
    // Reference file
    if (m_has_work_in_progress)
        return;
    group_unblock_work(); // Throws
}


void ServerFile::group_unblock_work()
{
    REALM_ASSERT(!m_has_work_in_progress);
    if (REALM_LIKELY(!m_server.is_sync_stopped())) {
        unblock_work(); // Throws
        const Work& work = m_work;
        if (REALM_LIKELY(work.has_primary_work)) {
            logger.trace("Work unit unblocked"); // Throws
            m_has_work_in_progress = true;
            Worker& worker = m_server.get_worker();
            worker.enqueue(this); // Throws
        }
    }
}


void ServerFile::unblock_work()
{
    REALM_ASSERT(m_has_blocked_work);

    m_work.reset();

    // Discard requests for file identifiers whose receiver is no longer
    // waiting.
    {
        auto i = m_file_ident_requests.begin();
        auto end = m_file_ident_requests.end();
        while (i != end) {
            auto j = i++;
            const FileIdentRequestInfo& info = j->second;
            if (!info.receiver)
                m_file_ident_requests.erase(j);
        }
    }
    std::size_t n = m_file_ident_requests.size();
    if (n > 0) {
        m_work.file_ident_alloc_slots.resize(n); // Throws
        std::size_t i = 0;
        for (const auto& pair : m_file_ident_requests) {
            const FileIdentRequestInfo& info = pair.second;
            FileIdentAllocSlot& slot = m_work.file_ident_alloc_slots[i];
            slot.proxy_file = info.proxy_file;
            slot.client_type = info.client_type;
            ++i;
        }
        m_work.has_primary_work = true;
    }

    // FIXME: `ServerFile::m_changesets_from_downstream` and
    // `Work::changesets_from_downstream` should be renamed to something else,
    // as it may contain kinds of data other than changesets.

    using std::swap;
    swap(m_changesets_from_downstream, m_work.changesets_from_downstream);
    m_work.have_changesets_from_downstream = (m_num_changesets_from_downstream > 0);
    bool has_changesets = !m_work.changesets_from_downstream.empty();
    if (has_changesets) {
        m_work.has_primary_work = true;
    }

    // Keep track of the size of pending changesets
    REALM_ASSERT(m_unblocked_changesets_from_downstream_byte_size == 0);
    m_unblocked_changesets_from_downstream_byte_size = m_blocked_changesets_from_downstream_byte_size;
    m_blocked_changesets_from_downstream_byte_size = 0;

    m_num_changesets_from_downstream = 0;
    m_has_blocked_work = false;
}


void ServerFile::resume_download() noexcept
{
    for (const auto& entry : m_identified_sessions) {
        Session& sess = *entry.second;
        sess.ensure_enlisted_to_send();
    }
}


void ServerFile::recognize_external_change()
{
    VersionInfo prev_version_info = m_version_info;
    const ServerHistory& history = access().history;       // Throws
    bool has_upstream_status;                              // Dummy
    sync::file_ident_type partial_file_ident;              // Dummy
    sync::version_type partial_progress_reference_version; // Dummy
    history.get_status(m_version_info, has_upstream_status, partial_file_ident,
                       partial_progress_reference_version); // Throws

    REALM_ASSERT(m_version_info.realm_version >= prev_version_info.realm_version);
    REALM_ASSERT(m_version_info.sync_version.version >= prev_version_info.sync_version.version);
    if (m_version_info.sync_version.version > prev_version_info.sync_version.version) {
        REALM_ASSERT(m_version_info.realm_version > prev_version_info.realm_version);
        resume_download();
    }
}


// NOTE: This function is executed by the worker thread
void ServerFile::worker_allocate_file_identifiers()
{
    Work& work = m_work;
    REALM_ASSERT(!work.file_ident_alloc_slots.empty());
    ServerHistory& hist = worker_access().history;                                      // Throws
    hist.allocate_file_identifiers(m_work.file_ident_alloc_slots, m_work.version_info); // Throws
    m_work.produced_new_realm_version = true;
}


// Returns true when, and only when this function produces a new sync version
// (adds a new entry to the sync history).
//
// NOTE: This function is executed by the worker thread
bool ServerFile::worker_integrate_changes_from_downstream(WorkerState& state)
{
    REALM_ASSERT(!m_work.changesets_from_downstream.empty());

    std::unique_ptr<ServerHistory> hist_ptr;
    DBRef sg_ptr;
    ServerHistory& hist = get_client_file_history(state, hist_ptr, sg_ptr);
    bool backup_whole_realm = false;
    bool produced_new_realm_version = hist.integrate_client_changesets(
        m_work.changesets_from_downstream, m_work.version_info, backup_whole_realm, m_work.integration_result,
        wlogger); // Throws
    bool produced_new_sync_version = !m_work.integration_result.integrated_changesets.empty();
    REALM_ASSERT(!produced_new_sync_version || produced_new_realm_version);
    if (produced_new_realm_version) {
        m_work.produced_new_realm_version = true;
        if (produced_new_sync_version) {
            m_work.produced_new_sync_version = true;
        }
    }
    return produced_new_sync_version;
}

ServerHistory& ServerFile::get_client_file_history(WorkerState& state, std::unique_ptr<ServerHistory>& hist_ptr,
                                                   DBRef& sg_ptr)
{
    if (state.use_file_cache)
        return worker_access().history; // Throws
    const std::string& path = m_worker_file.realm_path;
    hist_ptr = m_server.make_history_for_path();                   // Throws
    DBOptions options = m_worker_file.make_shared_group_options(); // Throws
    sg_ptr = DB::create(*hist_ptr, path, options);                 // Throws
    sg_ptr->claim_sync_agent();                                    // Throws
    return *hist_ptr;                                              // Throws
}


// When worker thread finishes work unit.
void ServerFile::group_postprocess_stage_1()
{
    REALM_ASSERT(m_has_work_in_progress);

    group_finalize_work_stage_1(); // Throws
    group_finalize_work_stage_2(); // Throws
    group_postprocess_stage_2();   // Throws
}


void ServerFile::group_postprocess_stage_2()
{
    REALM_ASSERT(m_has_work_in_progress);
    group_postprocess_stage_3(); // Throws
    // Suicide may have happened at this point
}


// When all files, including the reference file, have been backed up.
void ServerFile::group_postprocess_stage_3()
{
    REALM_ASSERT(m_has_work_in_progress);
    m_has_work_in_progress = false;

    logger.trace("Work unit postprocessing complete"); // Throws
    if (m_has_blocked_work)
        group_unblock_work(); // Throws
}


void ServerFile::finalize_work_stage_1()
{
    if (m_unblocked_changesets_from_downstream_byte_size > 0) {
        // Report the byte size of completed downstream changesets.
        std::size_t byte_size = m_unblocked_changesets_from_downstream_byte_size;
        get_server().dec_byte_size_for_pending_downstream_changesets(byte_size); // Throws
        m_unblocked_changesets_from_downstream_byte_size = 0;
    }

    // Deal with errors (bad changesets) pertaining to downstream clients
    std::size_t num_changesets_removed = 0;
    std::size_t num_bytes_removed = 0;
    for (const auto& entry : m_work.integration_result.excluded_client_files) {
        file_ident_type client_file_ident = entry.first;
        ExtendedIntegrationError error = entry.second;
        ProtocolError error_2 = ProtocolError::other_session_error;
        switch (error) {
            case ExtendedIntegrationError::client_file_expired:
                logger.debug("Changeset integration failed: Client file entry "
                             "expired during session"); // Throws
                error_2 = ProtocolError::client_file_expired;
                break;
            case ExtendedIntegrationError::bad_origin_file_ident:
                error_2 = ProtocolError::bad_origin_file_ident;
                break;
            case ExtendedIntegrationError::bad_changeset:
                error_2 = ProtocolError::bad_changeset;
                break;
        }
        auto i = m_identified_sessions.find(client_file_ident);
        if (i != m_identified_sessions.end()) {
            Session& sess = *i->second;
            SyncConnection& conn = sess.get_connection();
            conn.protocol_error(error_2, &sess); // Throws
        }
        const IntegratableChangesetList& list = m_changesets_from_downstream[client_file_ident];
        std::size_t num_changesets = list.changesets.size();
        std::size_t num_bytes = 0;
        for (const IntegratableChangeset& ic : list.changesets)
            num_bytes += ic.changeset.size();
        logger.info("Excluded %1 changesets of combined byte size %2 for client file %3", num_changesets, num_bytes,
                    client_file_ident); // Throws
        num_changesets_removed += num_changesets;
        num_bytes_removed += num_bytes;
        m_changesets_from_downstream.erase(client_file_ident);
    }

    REALM_ASSERT(num_changesets_removed <= m_num_changesets_from_downstream);
    REALM_ASSERT(num_bytes_removed <= m_blocked_changesets_from_downstream_byte_size);

    if (num_changesets_removed == 0)
        return;

    m_num_changesets_from_downstream -= num_changesets_removed;

    // The byte size of the blocked changesets must be decremented.
    if (num_bytes_removed > 0) {
        m_blocked_changesets_from_downstream_byte_size -= num_bytes_removed;
        get_server().dec_byte_size_for_pending_downstream_changesets(num_bytes_removed); // Throws
    }
}


void ServerFile::finalize_work_stage_2()
{
    // Expose new snapshot to remote peers
    REALM_ASSERT(m_work.produced_new_realm_version || m_work.version_info.realm_version == 0);
    if (m_work.version_info.realm_version > m_version_info.realm_version) {
        REALM_ASSERT(m_work.version_info.sync_version.version >= m_version_info.sync_version.version);
        m_version_info = m_work.version_info;
    }

    bool resume_download_and_upload = m_work.produced_new_sync_version;

    // Deliver allocated file identifiers to requesters
    REALM_ASSERT(m_file_ident_requests.size() >= m_work.file_ident_alloc_slots.size());
    auto begin = m_file_ident_requests.begin();
    auto i = begin;
    for (const FileIdentAllocSlot& slot : m_work.file_ident_alloc_slots) {
        FileIdentRequestInfo& info = i->second;
        REALM_ASSERT(info.proxy_file == slot.proxy_file);
        REALM_ASSERT(info.client_type == slot.client_type);
        if (FileIdentReceiver* receiver = info.receiver) {
            info.receiver = nullptr;
            receiver->receive_file_ident(slot.file_ident); // Throws
        }
        ++i;
    }
    m_file_ident_requests.erase(begin, i);

    // Resume download to downstream clients
    if (resume_download_and_upload) {
        resume_download();
    }
}

// ============================ Worker implementation ============================

Worker::Worker(ServerImpl& server)
    : logger_ptr{std::make_shared<util::PrefixLogger>(util::LogCategory::server, "Worker: ", server.logger_ptr)}
    // Throws
    , logger(*logger_ptr)
    , m_server{server}
    , m_file_access_cache{server.get_config().max_open_files, logger, *this, server.get_config().encryption_key}
{
    util::seed_prng_nondeterministically(m_random); // Throws
}


void Worker::enqueue(ServerFile* file)
{
    util::LockGuard lock{m_mutex};
    m_queue.push_back(file); // Throws
    m_cond.notify_all();
}


std::mt19937_64& Worker::server_history_get_random() noexcept
{
    return m_random;
}


void Worker::run()
{
    for (;;) {
        ServerFile* file = nullptr;
        {
            util::LockGuard lock{m_mutex};
            for (;;) {
                if (REALM_UNLIKELY(m_stop))
                    return;
                if (!m_queue.empty()) {
                    file = m_queue.front();
                    m_queue.pop_front();
                    break;
                }
                m_cond.wait(lock);
            }
        }
        file->worker_process_work_unit(m_state); // Throws
    }
}


void Worker::stop() noexcept
{
    util::LockGuard lock{m_mutex};
    m_stop = true;
    m_cond.notify_all();
}


// ============================ ServerImpl implementation ============================

ServerImpl::ServerImpl(const std::string& root_dir, util::Optional<sync::PKey> pkey, Server::Config config)
    : logger_ptr{std::make_shared<util::CategoryLogger>(util::LogCategory::server, std::move(config.logger))}
    , logger{*logger_ptr}
    , m_config{std::move(config)}
    , m_max_upload_backlog{determine_max_upload_backlog(config)}
    , m_root_dir{root_dir} // Throws
    , m_access_control{std::move(pkey)}
    , m_protocol_version_range{determine_protocol_version_range(config)}                 // Throws
    , m_file_access_cache{m_config.max_open_files, logger, *this, config.encryption_key} // Throws
    , m_worker{*this}                                                                    // Throws
    , m_acceptor{get_service()}
    , m_server_protocol{}       // Throws
    , m_compress_memory_arena{} // Throws
{
    if (m_config.ssl) {
        m_ssl_context = std::make_unique<network::ssl::Context>();                // Throws
        m_ssl_context->use_certificate_chain_file(m_config.ssl_certificate_path); // Throws
        m_ssl_context->use_private_key_file(m_config.ssl_certificate_key_path);   // Throws
    }
}


ServerImpl::~ServerImpl() noexcept
{
    bool server_destroyed_while_still_running = m_running;
    REALM_ASSERT_RELEASE(!server_destroyed_while_still_running);
}


void ServerImpl::start()
{
    logger.info("Realm sync server started (%1)", REALM_VER_CHUNK); // Throws
    logger.info("Supported protocol versions: %1-%2 (%3-%4 configured)",
                ServerImplBase::get_oldest_supported_protocol_version(), get_current_protocol_version(),
                m_protocol_version_range.first,
                m_protocol_version_range.second); // Throws
    logger.info("Platform: %1", util::get_platform_info());
    bool is_debug_build = false;
#if REALM_DEBUG
    is_debug_build = true;
#endif
    {
        const char* lead_text = "Build mode";
        if (is_debug_build) {
            logger.info("%1: Debug", lead_text); // Throws
        }
        else {
            logger.info("%1: Release", lead_text); // Throws
        }
    }
    if (is_debug_build) {
        logger.warn("Build mode is Debug! CAN SEVERELY IMPACT PERFORMANCE - "
                    "NOT RECOMMENDED FOR PRODUCTION"); // Throws
    }
    logger.info("Directory holding persistent state: %1", m_root_dir);        // Throws
    logger.info("Maximum number of open files: %1", m_config.max_open_files); // Throws
    {
        const char* lead_text = "Encryption";
        if (m_config.encryption_key) {
            logger.info("%1: Yes", lead_text); // Throws
        }
        else {
            logger.info("%1: No", lead_text); // Throws
        }
    }
    logger.info("Log level: %1", logger.get_level_threshold()); // Throws
    {
        const char* lead_text = "Disable sync to disk";
        if (m_config.disable_sync_to_disk) {
            logger.info("%1: All files", lead_text); // Throws
        }
        else {
            logger.info("%1: No", lead_text); // Throws
        }
    }
    if (m_config.disable_sync_to_disk) {
        logger.warn("Testing/debugging feature 'disable sync to disk' enabled - "
                    "never do this in production!"); // Throws
    }
    logger.info("Download bootstrap caching: %1",
                (m_config.enable_download_bootstrap_cache ? "Yes" : "No"));                // Throws
    logger.info("Max download size: %1 bytes", m_config.max_download_size);                // Throws
    logger.info("Max upload backlog: %1 bytes", m_max_upload_backlog);                     // Throws
    logger.info("HTTP request timeout: %1 ms", m_config.http_request_timeout);             // Throws
    logger.info("HTTP response timeout: %1 ms", m_config.http_response_timeout);           // Throws
    logger.info("Connection reaper timeout: %1 ms", m_config.connection_reaper_timeout);   // Throws
    logger.info("Connection reaper interval: %1 ms", m_config.connection_reaper_interval); // Throws
    logger.info("Connection soft close timeout: %1 ms", m_config.soft_close_timeout);      // Throws
    logger.debug("Authorization header name: %1", m_config.authorization_header_name);     // Throws

    m_realm_names = _impl::find_realm_files(m_root_dir); // Throws

    initiate_connection_reaper_timer(m_config.connection_reaper_interval); // Throws

    listen(); // Throws
}


void ServerImpl::run()
{
    auto ta = util::make_temp_assign(m_running, true);

    {
        auto worker_thread = util::make_thread_exec_guard(m_worker, *this); // Throws
        std::string name;
        if (util::Thread::get_name(name)) {
            name += "-worker";
            worker_thread.start_with_signals_blocked(name); // Throws
        }
        else {
            worker_thread.start_with_signals_blocked(); // Throws
        }

        m_service.run(); // Throws

        worker_thread.stop_and_rethrow(); // Throws
    }

    logger.info("Realm sync server stopped");
}


void ServerImpl::stop() noexcept
{
    util::LockGuard lock{m_mutex};
    if (m_stopped)
        return;
    m_stopped = true;
    m_wait_or_service_stopped_cond.notify_all();
    m_service.stop();
}


void ServerImpl::inc_byte_size_for_pending_downstream_changesets(std::size_t byte_size)
{
    m_pending_changesets_from_downstream_byte_size += byte_size;
    logger.debug("Byte size for pending downstream changesets incremented by "
                 "%1 to reach a total of %2",
                 byte_size,
                 m_pending_changesets_from_downstream_byte_size); // Throws
}


void ServerImpl::dec_byte_size_for_pending_downstream_changesets(std::size_t byte_size)
{
    REALM_ASSERT(byte_size <= m_pending_changesets_from_downstream_byte_size);
    m_pending_changesets_from_downstream_byte_size -= byte_size;
    logger.debug("Byte size for pending downstream changesets decremented by "
                 "%1 to reach a total of %2",
                 byte_size,
                 m_pending_changesets_from_downstream_byte_size); // Throws
}


std::mt19937_64& ServerImpl::server_history_get_random() noexcept
{
    return get_random();
}


void ServerImpl::listen()
{
    network::Resolver resolver{get_service()};
    network::Resolver::Query query(m_config.listen_address, m_config.listen_port,
                                   network::Resolver::Query::passive | network::Resolver::Query::address_configured);
    network::Endpoint::List endpoints = resolver.resolve(query); // Throws

    auto i = endpoints.begin();
    auto end = endpoints.end();
    for (;;) {
        std::error_code ec;
        m_acceptor.open(i->protocol(), ec);
        if (!ec) {
            using SocketBase = network::SocketBase;
            m_acceptor.set_option(SocketBase::reuse_address(m_config.reuse_address), ec);
            if (!ec) {
                m_acceptor.bind(*i, ec);
                if (!ec)
                    break;
            }
            m_acceptor.close();
        }
        if (i + 1 == end) {
            for (auto i2 = endpoints.begin(); i2 != i; ++i2) {
                // FIXME: We don't have the error code for previous attempts, so
                // can't print a nice message.
                logger.error("Failed to bind to %1:%2", i2->address(),
                             i2->port()); // Throws
            }
            logger.error("Failed to bind to %1:%2: %3", i->address(), i->port(),
                         ec.message()); // Throws
            throw std::runtime_error("Could not create a listening socket: All endpoints failed");
        }
    }

    m_acceptor.listen(m_config.listen_backlog);

    network::Endpoint local_endpoint = m_acceptor.local_endpoint();
    const char* ssl_mode = (m_ssl_context ? "TLS" : "non-TLS");
    logger.info("Listening on %1:%2 (max backlog is %3, %4)", local_endpoint.address(), local_endpoint.port(),
                m_config.listen_backlog, ssl_mode); // Throws

    initiate_accept();
}


void ServerImpl::initiate_accept()
{
    auto handler = [this](std::error_code ec) {
        if (ec != util::error::operation_aborted)
            handle_accept(ec);
    };
    bool is_ssl = bool(m_ssl_context);
    m_next_http_conn.reset(new HTTPConnection(*this, ++m_next_conn_id, is_ssl));                            // Throws
    m_acceptor.async_accept(m_next_http_conn->get_socket(), m_next_http_conn_endpoint, std::move(handler)); // Throws
}


void ServerImpl::handle_accept(std::error_code ec)
{
    if (ec) {
        if (ec != util::error::connection_aborted) {
            REALM_ASSERT(ec != util::error::operation_aborted);

            // We close the reserved files to get a few extra file descriptors.
            for (size_t i = 0; i < sizeof(m_reserved_files) / sizeof(m_reserved_files[0]); ++i) {
                m_reserved_files[i].reset();
            }

            // FIXME: There are probably errors that need to be treated
            // specially, and not cause the server to "crash".

            if (ec == make_basic_system_error_code(EMFILE)) {
                logger.error("Failed to accept a connection due to the file descriptor limit, "
                             "consider increasing the limit in your system config"); // Throws
                throw OutOfFilesError(ec);
            }
            else {
                throw std::system_error(ec);
            }
        }
        logger.debug("Skipping aborted connection"); // Throws
    }
    else {
        HTTPConnection& conn = *m_next_http_conn;
        if (m_config.tcp_no_delay)
            conn.get_socket().set_option(network::SocketBase::no_delay(true));  // Throws
        m_http_connections.emplace(conn.get_id(), std::move(m_next_http_conn)); // Throws
        Formatter& formatter = m_misc_buffers.formatter;
        formatter.reset();
        formatter << "[" << m_next_http_conn_endpoint.address() << "]:" << m_next_http_conn_endpoint.port(); // Throws
        std::string remote_endpoint = {formatter.data(), formatter.size()};                                  // Throws
        conn.initiate(std::move(remote_endpoint));                                                           // Throws
    }
    initiate_accept(); // Throws
}


void ServerImpl::remove_http_connection(std::int_fast64_t conn_id) noexcept
{
    m_http_connections.erase(conn_id);
}


void ServerImpl::add_sync_connection(int_fast64_t connection_id, std::unique_ptr<SyncConnection>&& sync_conn)
{
    m_sync_connections.emplace(connection_id, std::move(sync_conn));
}


void ServerImpl::remove_sync_connection(int_fast64_t connection_id)
{
    m_sync_connections.erase(connection_id);
}


void ServerImpl::set_connection_reaper_timeout(milliseconds_type timeout)
{
    get_service().post([this, timeout](Status) {
        m_config.connection_reaper_timeout = timeout;
    });
}


void ServerImpl::close_connections()
{
    get_service().post([this](Status) {
        do_close_connections(); // Throws
    });
}


bool ServerImpl::map_virtual_to_real_path(const std::string& virt_path, std::string& real_path)
{
    return _impl::map_virt_to_real_realm_path(m_root_dir, virt_path, real_path); // Throws
}


void ServerImpl::recognize_external_change(const std::string& virt_path)
{
    std::string virt_path_2 = virt_path; // Throws (copy)
    get_service().post([this, virt_path = std::move(virt_path_2)](Status) {
        do_recognize_external_change(virt_path); // Throws
    });                                          // Throws
}


void ServerImpl::stop_sync_and_wait_for_backup_completion(
    util::UniqueFunction<void(bool did_backup)> completion_handler, milliseconds_type timeout)
{
    logger.info("stop_sync_and_wait_for_backup_completion() called with "
                "timeout = %1",
                timeout); // Throws

    get_service().post([this, completion_handler = std::move(completion_handler), timeout](Status) mutable {
        do_stop_sync_and_wait_for_backup_completion(std::move(completion_handler),
                                                    timeout); // Throws
    });
}


void ServerImpl::initiate_connection_reaper_timer(milliseconds_type timeout)
{
    m_connection_reaper_timer.emplace(get_service());
    m_connection_reaper_timer->async_wait(std::chrono::milliseconds(timeout), [this, timeout](Status status) {
        if (status != ErrorCodes::OperationAborted) {
            reap_connections();                        // Throws
            initiate_connection_reaper_timer(timeout); // Throws
        }
    }); // Throws
}


void ServerImpl::reap_connections()
{
    logger.debug("Discarding dead connections"); // Throws
    SteadyTimePoint now = steady_clock_now();
    {
        auto end = m_http_connections.end();
        auto i = m_http_connections.begin();
        while (i != end) {
            HTTPConnection& conn = *i->second;
            ++i;
            // Suicide
            conn.terminate_if_dead(now); // Throws
        }
    }
    {
        auto end = m_sync_connections.end();
        auto i = m_sync_connections.begin();
        while (i != end) {
            SyncConnection& conn = *i->second;
            ++i;
            // Suicide
            conn.terminate_if_dead(now); // Throws
        }
    }
}


void ServerImpl::do_close_connections()
{
    for (auto& entry : m_sync_connections) {
        SyncConnection& conn = *entry.second;
        conn.initiate_soft_close(); // Throws
    }
}


void ServerImpl::do_recognize_external_change(const std::string& virt_path)
{
    auto i = m_files.find(virt_path);
    if (i == m_files.end())
        return;
    ServerFile& file = *i->second;
    file.recognize_external_change();
}


void ServerImpl::do_stop_sync_and_wait_for_backup_completion(
    util::UniqueFunction<void(bool did_complete)> completion_handler, milliseconds_type timeout)
{
    static_cast<void>(timeout);
    if (m_sync_stopped)
        return;
    do_close_connections(); // Throws
    m_sync_stopped = true;
    bool completion_reached = false;
    completion_handler(completion_reached); // Throws
}


// ============================ SyncConnection implementation ============================

SyncConnection::~SyncConnection() noexcept
{
    m_sessions_enlisted_to_send.clear();
    m_sessions.clear();
}


void SyncConnection::initiate()
{
    m_last_activity_at = steady_clock_now();
    logger.debug("Sync Connection initiated");
    m_websocket.initiate_server_websocket_after_handshake();
    send_log_message(util::Logger::Level::info, "Client connection established with server", 0,
                     m_appservices_request_id);
}


template <class... Params>
void SyncConnection::terminate(Logger::Level log_level, const char* log_message, Params... log_params)
{
    terminate_sessions();                              // Throws
    logger.log(log_level, log_message, log_params...); // Throws
    m_websocket.stop();
    m_ssl_stream.reset();
    m_socket.reset();
    // Suicide
    m_server.remove_sync_connection(m_id);
}


void SyncConnection::terminate_if_dead(SteadyTimePoint now)
{
    milliseconds_type time = steady_duration(m_last_activity_at, now);
    const Server::Config& config = m_server.get_config();
    if (m_is_closing) {
        if (time >= config.soft_close_timeout) {
            // Suicide
            terminate(Logger::Level::detail,
                      "Sync connection closed (timeout during soft close)"); // Throws
        }
    }
    else {
        if (time >= config.connection_reaper_timeout) {
            // Suicide
            terminate(Logger::Level::detail,
                      "Sync connection closed (no heartbeat)"); // Throws
        }
    }
}


void SyncConnection::enlist_to_send(Session* sess) noexcept
{
    REALM_ASSERT(!m_is_closing);
    REALM_ASSERT(!sess->is_enlisted_to_send());
    m_sessions_enlisted_to_send.push_back(sess);
    m_send_trigger.trigger();
}


void SyncConnection::handle_protocol_error(Status status)
{
    logger.error("%1", status);
    switch (status.code()) {
        case ErrorCodes::SyncProtocolInvariantFailed:
            protocol_error(ProtocolError::bad_syntax); // Throws
            break;
        case ErrorCodes::LimitExceeded:
            protocol_error(ProtocolError::limits_exceeded); // Throws
            break;
        default:
            protocol_error(ProtocolError::other_error);
            break;
    }
}

void SyncConnection::receive_bind_message(session_ident_type session_ident, std::string path,
                                          std::string signed_user_token, bool need_client_file_ident,
                                          bool is_subserver)
{
    auto p = m_sessions.emplace(session_ident, nullptr); // Throws
    bool was_inserted = p.second;
    if (REALM_UNLIKELY(!was_inserted)) {
        logger.error("Overlapping reuse of session identifier %1 in BIND message",
                     session_ident);                           // Throws
        protocol_error(ProtocolError::reuse_of_session_ident); // Throws
        return;
    }
    try {
        p.first->second.reset(new Session(*this, session_ident)); // Throws
    }
    catch (...) {
        m_sessions.erase(p.first);
        throw;
    }

    Session& sess = *p.first->second;
    sess.initiate(); // Throws
    ProtocolError error;
    bool success =
        sess.receive_bind_message(std::move(path), std::move(signed_user_token), need_client_file_ident, is_subserver,
                                  error); // Throws
    if (REALM_UNLIKELY(!success))         // Throws
        protocol_error(error, &sess);     // Throws
}


void SyncConnection::receive_ident_message(session_ident_type session_ident, file_ident_type client_file_ident,
                                           salt_type client_file_ident_salt, version_type scan_server_version,
                                           version_type scan_client_version, version_type latest_server_version,
                                           salt_type latest_server_version_salt)
{
    auto i = m_sessions.find(session_ident);
    if (REALM_UNLIKELY(i == m_sessions.end())) {
        bad_session_ident("IDENT", session_ident); // Throws
        return;
    }
    Session& sess = *i->second;
    if (REALM_UNLIKELY(sess.unbind_message_received())) {
        message_after_unbind("IDENT", session_ident); // Throws
        return;
    }
    if (REALM_UNLIKELY(sess.error_occurred())) {
        // Protocol state is SendError or WaitForUnbindErr. In these states, all
        // messages, other than UNBIND, must be ignored.
        return;
    }
    if (REALM_UNLIKELY(sess.must_send_ident_message())) {
        logger.error("Received IDENT message before IDENT message was sent"); // Throws
        protocol_error(ProtocolError::bad_message_order);                     // Throws
        return;
    }
    if (REALM_UNLIKELY(sess.ident_message_received())) {
        logger.error("Received second IDENT message for session"); // Throws
        protocol_error(ProtocolError::bad_message_order);          // Throws
        return;
    }

    ProtocolError error = {};
    bool success = sess.receive_ident_message(client_file_ident, client_file_ident_salt, scan_server_version,
                                              scan_client_version, latest_server_version, latest_server_version_salt,
                                              error); // Throws
    if (REALM_UNLIKELY(!success))                     // Throws
        protocol_error(error, &sess);                 // Throws
}

void SyncConnection::receive_upload_message(session_ident_type session_ident, version_type progress_client_version,
                                            version_type progress_server_version, version_type locked_server_version,
                                            const UploadChangesets& upload_changesets)
{
    auto i = m_sessions.find(session_ident);
    if (REALM_UNLIKELY(i == m_sessions.end())) {
        bad_session_ident("UPLOAD", session_ident); // Throws
        return;
    }
    Session& sess = *i->second;
    if (REALM_UNLIKELY(sess.unbind_message_received())) {
        message_after_unbind("UPLOAD", session_ident); // Throws
        return;
    }
    if (REALM_UNLIKELY(sess.error_occurred())) {
        // Protocol state is SendError or WaitForUnbindErr. In these states, all
        // messages, other than UNBIND, must be ignored.
        return;
    }
    if (REALM_UNLIKELY(!sess.ident_message_received())) {
        message_before_ident("UPLOAD", session_ident); // Throws
        return;
    }

    ProtocolError error = {};
    bool success = sess.receive_upload_message(progress_client_version, progress_server_version,
                                               locked_server_version, upload_changesets, error); // Throws
    if (REALM_UNLIKELY(!success))                                                                // Throws
        protocol_error(error, &sess);                                                            // Throws
}


void SyncConnection::receive_mark_message(session_ident_type session_ident, request_ident_type request_ident)
{
    auto i = m_sessions.find(session_ident);
    if (REALM_UNLIKELY(i == m_sessions.end())) {
        bad_session_ident("MARK", session_ident);
        return;
    }
    Session& sess = *i->second;
    if (REALM_UNLIKELY(sess.unbind_message_received())) {
        message_after_unbind("MARK", session_ident); // Throws
        return;
    }
    if (REALM_UNLIKELY(sess.error_occurred())) {
        // Protocol state is SendError or WaitForUnbindErr. In these states, all
        // messages, other than UNBIND, must be ignored.
        return;
    }
    if (REALM_UNLIKELY(!sess.ident_message_received())) {
        message_before_ident("MARK", session_ident); // Throws
        return;
    }

    ProtocolError error;
    bool success = sess.receive_mark_message(request_ident, error); // Throws
    if (REALM_UNLIKELY(!success))                                   // Throws
        protocol_error(error, &sess);                               // Throws
}


void SyncConnection::receive_unbind_message(session_ident_type session_ident)
{
    auto i = m_sessions.find(session_ident); // Throws
    if (REALM_UNLIKELY(i == m_sessions.end())) {
        bad_session_ident("UNBIND", session_ident); // Throws
        return;
    }
    Session& sess = *i->second;
    if (REALM_UNLIKELY(sess.unbind_message_received())) {
        message_after_unbind("UNBIND", session_ident); // Throws
        return;
    }

    sess.receive_unbind_message(); // Throws
    // NOTE: The session might have gotten destroyed at this time!
}


void SyncConnection::receive_ping(milliseconds_type timestamp, milliseconds_type rtt)
{
    logger.debug("Received: PING(timestamp=%1, rtt=%2)", timestamp, rtt); // Throws
    m_send_pong = true;
    m_last_ping_timestamp = timestamp;
    if (!m_is_sending)
        send_next_message();
}


void SyncConnection::receive_error_message(session_ident_type session_ident, int error_code,
                                           std::string_view error_body)
{
    logger.debug("Received: ERROR(error_code=%1, message_size=%2, session_ident=%3)", error_code, error_body.size(),
                 session_ident); // Throws
    auto i = m_sessions.find(session_ident);
    if (REALM_UNLIKELY(i == m_sessions.end())) {
        bad_session_ident("ERROR", session_ident);
        return;
    }
    Session& sess = *i->second;
    if (REALM_UNLIKELY(sess.unbind_message_received())) {
        message_after_unbind("ERROR", session_ident); // Throws
        return;
    }

    sess.receive_error_message(session_ident, error_code, error_body); // Throws
}

void SyncConnection::send_log_message(util::Logger::Level level, const std::string&& message,
                                      session_ident_type sess_ident, std::optional<std::string> co_id)
{
    if (get_client_protocol_version() < SyncConnection::SERVER_LOG_PROTOCOL_VERSION) {
        return logger.log(level, message.c_str());
    }

    LogMessage log_msg{sess_ident, level, std::move(message), std::move(co_id)};
    {
        std::lock_guard lock(m_log_mutex);
        m_log_messages.push(std::move(log_msg));
    }
    m_send_trigger.trigger();
}


void SyncConnection::bad_session_ident(const char* message_type, session_ident_type session_ident)
{
    logger.error("Bad session identifier in %1 message, session_ident = %2", message_type,
                 session_ident);                      // Throws
    protocol_error(ProtocolError::bad_session_ident); // Throws
}


void SyncConnection::message_after_unbind(const char* message_type, session_ident_type session_ident)
{
    logger.error("Received %1 message after UNBIND message, session_ident = %2", message_type,
                 session_ident);                      // Throws
    protocol_error(ProtocolError::bad_message_order); // Throws
}


void SyncConnection::message_before_ident(const char* message_type, session_ident_type session_ident)
{
    logger.error("Received %1 message before IDENT message, session_ident = %2", message_type,
                 session_ident);                      // Throws
    protocol_error(ProtocolError::bad_message_order); // Throws
}


void SyncConnection::handle_message_received(const char* data, size_t size)
{
    // parse_message_received() parses the message and calls the
    // proper handler on the SyncConnection object (this).
    get_server_protocol().parse_message_received<SyncConnection>(*this, std::string_view(data, size));
    return;
}


void SyncConnection::handle_ping_received(const char* data, size_t size)
{
    // parse_message_received() parses the message and calls the
    // proper handler on the SyncConnection object (this).
    get_server_protocol().parse_ping_received<SyncConnection>(*this, std::string_view(data, size));
    return;
}


void SyncConnection::send_next_message()
{
    if (m_is_sending)
        return;
    REALM_ASSERT(!m_sending_pong);
    if (m_send_pong) {
        send_pong(m_last_ping_timestamp);
        if (m_sending_pong)
            return;
    }
    for (;;) {
        Session* sess = m_sessions_enlisted_to_send.pop_front();
        if (!sess) {
            // No sessions were enlisted to send
            if (REALM_LIKELY(!m_is_closing))
                break; // Check to see if there are any log messages to go out
            // Send a connection level ERROR
            REALM_ASSERT(!is_session_level_error(m_error_code));
            initiate_write_error(m_error_code, m_error_session_ident); // Throws
            return;
        }
        sess->send_message(); // Throws
        // NOTE: The session might have gotten destroyed at this time!

        // At this point, `m_is_sending` is true if, and only if the session
        // chose to send a message. If it chose to not send a message, we must
        // loop back and give the next session in `m_sessions_enlisted_to_send`
        // a chance.
        if (m_is_sending)
            return;
    }
    {
        std::lock_guard lock(m_log_mutex);
        if (!m_log_messages.empty()) {
            send_log_message(m_log_messages.front());
            m_log_messages.pop();
        }
    }
    // Otherwise, nothing to do
}


void SyncConnection::initiate_write_output_buffer()
{
    auto handler = [this](std::error_code ec, size_t) {
        if (!ec) {
            handle_write_output_buffer();
        }
    };

    m_websocket.async_write_binary(m_output_buffer.data(), m_output_buffer.size(),
                                   std::move(handler)); // Throws
    m_is_sending = true;
}


void SyncConnection::initiate_pong_output_buffer()
{
    auto handler = [this](std::error_code ec, size_t) {
        if (!ec) {
            handle_pong_output_buffer();
        }
    };

    REALM_ASSERT(!m_is_sending);
    REALM_ASSERT(!m_sending_pong);
    m_websocket.async_write_binary(m_output_buffer.data(), m_output_buffer.size(),
                                   std::move(handler)); // Throws

    m_is_sending = true;
    m_sending_pong = true;
}


void SyncConnection::send_pong(milliseconds_type timestamp)
{
    REALM_ASSERT(m_send_pong);
    REALM_ASSERT(!m_sending_pong);
    m_send_pong = false;
    logger.debug("Sending: PONG(timestamp=%1)", timestamp); // Throws

    OutputBuffer& out = get_output_buffer();
    get_server_protocol().make_pong(out, timestamp); // Throws

    initiate_pong_output_buffer(); // Throws
}

void SyncConnection::send_log_message(const LogMessage& log_msg)
{
    OutputBuffer& out = get_output_buffer();
    get_server_protocol().make_log_message(out, log_msg.level, log_msg.message, log_msg.sess_ident,
                                           log_msg.co_id); // Throws

    initiate_write_output_buffer(); // Throws
}


void SyncConnection::handle_write_output_buffer()
{
    release_output_buffer();
    m_is_sending = false;
    send_next_message(); // Throws
}


void SyncConnection::handle_pong_output_buffer()
{
    release_output_buffer();
    REALM_ASSERT(m_is_sending);
    REALM_ASSERT(m_sending_pong);
    m_is_sending = false;
    m_sending_pong = false;
    send_next_message(); // Throws
}


void SyncConnection::initiate_write_error(ProtocolError error_code, session_ident_type session_ident)
{
    const char* message = get_protocol_error_message(int(error_code));
    std::size_t message_size = std::strlen(message);
    bool try_again = determine_try_again(error_code);

    logger.detail("Sending: ERROR(error_code=%1, message_size=%2, try_again=%3, session_ident=%4)", int(error_code),
                  message_size, try_again, session_ident); // Throws

    OutputBuffer& out = get_output_buffer();
    int protocol_version = get_client_protocol_version();
    get_server_protocol().make_error_message(protocol_version, out, error_code, message, message_size, try_again,
                                             session_ident); // Throws

    auto handler = [this](std::error_code ec, size_t) {
        handle_write_error(ec); // Throws
    };
    m_websocket.async_write_binary(out.data(), out.size(), std::move(handler));
    m_is_sending = true;
}


void SyncConnection::handle_write_error(std::error_code ec)
{
    m_is_sending = false;
    REALM_ASSERT(m_is_closing);
    if (!m_ssl_stream) {
        m_socket->shutdown(network::Socket::shutdown_send, ec);
        if (ec && ec != make_basic_system_error_code(ENOTCONN))
            throw std::system_error(ec);
    }
}


// For connection level errors, `sess` is ignored. For session level errors, a
// session must be specified.
//
// If a session is specified, that session object will have been detached from
// the ServerFile object (and possibly destroyed) upon return from
// protocol_error().
//
// If a session is specified for a protocol level error, that session object
// will have been destroyed upon return from protocol_error(). For session level
// errors, the specified session will have been destroyed upon return from
// protocol_error() if, and only if the negotiated protocol version is less than
// 23.
void SyncConnection::protocol_error(ProtocolError error_code, Session* sess)
{
    REALM_ASSERT(!m_is_closing);
    bool session_level = is_session_level_error(error_code);
    REALM_ASSERT(!session_level || sess);
    REALM_ASSERT(!sess || m_sessions.count(sess->get_session_ident()) == 1);
    if (logger.would_log(util::Logger::Level::debug)) {
        const char* message = get_protocol_error_message(int(error_code));
        Logger& logger_2 = (session_level ? sess->logger : logger);
        logger_2.debug("Protocol error: %1 (error_code=%2)", message, int(error_code)); // Throws
    }
    session_ident_type session_ident = (session_level ? sess->get_session_ident() : 0);
    if (session_level) {
        sess->initiate_deactivation(error_code); // Throws
        return;
    }
    do_initiate_soft_close(error_code, session_ident); // Throws
}


void SyncConnection::do_initiate_soft_close(ProtocolError error_code, session_ident_type session_ident)
{
    REALM_ASSERT(get_protocol_error_message(int(error_code)));

    // With recent versions of the protocol (when the version is greater than,
    // or equal to 23), this function will only be called for connection level
    // errors, never for session specific errors. However, for the purpose of
    // emulating earlier protocol versions, this function might be called for
    // session specific errors too.
    REALM_ASSERT(is_session_level_error(error_code) == (session_ident != 0));
    REALM_ASSERT(!is_session_level_error(error_code));

    REALM_ASSERT(!m_is_closing);
    m_is_closing = true;

    m_error_code = error_code;
    m_error_session_ident = session_ident;

    // Don't waste time and effort sending any other messages
    m_send_pong = false;
    m_sessions_enlisted_to_send.clear();

    m_receiving_session = nullptr;

    terminate_sessions(); // Throws

    m_send_trigger.trigger();
}


void SyncConnection::close_due_to_close_by_client(std::error_code ec)
{
    auto log_level = (ec == util::MiscExtErrors::end_of_input ? Logger::Level::detail : Logger::Level::info);
    // Suicide
    terminate(log_level, "Sync connection closed by client: %1", ec.message()); // Throws
}


void SyncConnection::close_due_to_error(std::error_code ec)
{
    // Suicide
    terminate(Logger::Level::error, "Sync connection closed due to error: %1",
              ec.message()); // Throws
}


void SyncConnection::terminate_sessions()
{
    for (auto& entry : m_sessions) {
        Session& sess = *entry.second;
        sess.terminate(); // Throws
    }
    m_sessions_enlisted_to_send.clear();
    m_sessions.clear();
}


void SyncConnection::initiate_soft_close()
{
    if (!m_is_closing) {
        session_ident_type session_ident = 0;                                    // Not session specific
        do_initiate_soft_close(ProtocolError::connection_closed, session_ident); // Throws
    }
}


void SyncConnection::discard_session(session_ident_type session_ident) noexcept
{
    m_sessions.erase(session_ident);
}

} // anonymous namespace


// ============================ sync::Server implementation ============================

class Server::Implementation : public ServerImpl {
public:
    Implementation(const std::string& root_dir, util::Optional<PKey> pkey, Server::Config config)
        : ServerImpl{root_dir, std::move(pkey), std::move(config)} // Throws
    {
    }
    virtual ~Implementation() {}
};


Server::Server(const std::string& root_dir, util::Optional<sync::PKey> pkey, Config config)
    : m_impl{new Implementation{root_dir, std::move(pkey), std::move(config)}} // Throws
{
}


Server::Server(Server&& serv) noexcept
    : m_impl{std::move(serv.m_impl)}
{
}


Server::~Server() noexcept {}


void Server::start()
{
    m_impl->start(); // Throws
}


void Server::start(const std::string& listen_address, const std::string& listen_port, bool reuse_address)
{
    m_impl->start(listen_address, listen_port, reuse_address); // Throws
}


network::Endpoint Server::listen_endpoint() const
{
    return m_impl->listen_endpoint(); // Throws
}


void Server::run()
{
    m_impl->run(); // Throws
}


void Server::stop() noexcept
{
    m_impl->stop();
}


uint_fast64_t Server::errors_seen() const noexcept
{
    return m_impl->errors_seen;
}


void Server::stop_sync_and_wait_for_backup_completion(util::UniqueFunction<void(bool did_backup)> completion_handler,
                                                      milliseconds_type timeout)
{
    m_impl->stop_sync_and_wait_for_backup_completion(std::move(completion_handler), timeout); // Throws
}


void Server::set_connection_reaper_timeout(milliseconds_type timeout)
{
    m_impl->set_connection_reaper_timeout(timeout);
}


void Server::close_connections()
{
    m_impl->close_connections();
}


bool Server::map_virtual_to_real_path(const std::string& virt_path, std::string& real_path)
{
    return m_impl->map_virtual_to_real_path(virt_path, real_path); // Throws
}


void Server::recognize_external_change(const std::string& virt_path)
{
    m_impl->recognize_external_change(virt_path); // Throws
}


void Server::get_workunit_timers(milliseconds_type& parallel_section, milliseconds_type& sequential_section)
{
    m_impl->get_workunit_timers(parallel_section, sequential_section);
}
