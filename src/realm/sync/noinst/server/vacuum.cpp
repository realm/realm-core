#include <realm/sync/noinst/server/vacuum.hpp>

#include <realm/group.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/noinst/server/server_history.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>

#include <realm/version.hpp>

using namespace realm;
using namespace realm::sync;
using namespace realm::_impl;
using realm::util::File;

struct Vacuum::VacuumFile {
    util::Logger& logger;

    explicit VacuumFile(util::Logger& logger, const Options& options, const std::string& path)
        : logger{logger}
        , m_options(options)
        , m_path(path)
    {
    }
    virtual ~VacuumFile() {}

    const Options& m_options;
    std::string m_path;
    DBRef m_sg;

    virtual void dry_run(Results& results) = 0;
    virtual void vacuum(Results& results) = 0;

    virtual std::string get_type_description() const = 0;
};

namespace {

struct PlainVacuumFile : Vacuum::VacuumFile {
    PlainVacuumFile(util::Logger& logger, const Vacuum::Options& options, const std::string& path)
        : VacuumFile(logger, options, path)
    {
        bool no_create_file = true;
        DBOptions sg_options;
        sg_options.allow_file_format_upgrade = !options.no_file_upgrade;
        if (options.encryption_key)
            sg_options.encryption_key = options.encryption_key->data();
        m_sg = DB::create(path, no_create_file, sg_options);
    }

    std::string get_type_description() const override
    {
        return "Plain";
    }

    void dry_run(Vacuum::Results& results) override
    {
        ReadTransaction tr{m_sg};
        results.after_size = tr.get_group().compute_aggregated_byte_size();
    }

    void vacuum(Vacuum::Results& results) override
    {
        if (!m_options.no_file_compaction) {
            if (m_options.bump_realm_version)
                throw std::runtime_error("Option 'bump_realm_version' not supported for the plain Realm: '" + m_path +
                                         "'");
            if (!m_sg->compact())
                throw VacuumError{std::string{"Another process is using '" + m_path + "'. Aborting vacuum."}};
        }
        // Get "after" size
        File f{m_path, File::mode_Read};
        results.after_size = size_t(f.get_size());
    }
};

struct SyncClientVacuumFile : Vacuum::VacuumFile {
    SyncClientVacuumFile(util::Logger& logger, const Vacuum::Options& options, const std::string& path)
        : VacuumFile(logger, options, path)
    {
        auto client_history = sync::make_client_replication();
        DBOptions sg_options;
        sg_options.allow_file_format_upgrade = !options.no_file_upgrade;
        if (options.encryption_key)
            sg_options.encryption_key = options.encryption_key->data();
        m_sg = DB::create(std::move(client_history), path, sg_options);
    }

    std::string get_type_description() const override
    {
        return "Sync Client";
    }

    void dry_run(Vacuum::Results& results) override
    {
        ReadTransaction tr{m_sg};
        results.after_size = tr.get_group().compute_aggregated_byte_size();
    }

    void vacuum(Vacuum::Results& results) override
    {
        if (!m_options.no_file_compaction) {
            if (m_options.bump_realm_version)
                throw std::runtime_error("Option 'bump_realm_version' not supported for the client Realm: '" +
                                         m_path + "'");
            if (!m_sg->compact())
                throw VacuumError{std::string{"Another process is using '" + m_path + "'. Aborting vacuum."}};
        }
        // Get "after" size
        File f{m_path, File::mode_Read};
        results.after_size = size_t(f.get_size());
    }
};

class ServerHistoryContext : public _impl::ServerHistory::Context {
public:
    explicit ServerHistoryContext(bool enable_compaction, bool ignore_clients, std::chrono::seconds time_to_live)
        : m_enable_compaction{enable_compaction}
        , m_ignore_clients{ignore_clients}
        , m_time_to_live{time_to_live}
    {
    }

    std::mt19937_64& server_history_get_random() noexcept override final
    {
        return m_random;
    }

    bool get_compaction_params(bool& ignore_clients, std::chrono::seconds& time_to_live,
                               std::chrono::seconds& compaction_interval) noexcept override final
    {
        if (m_enable_compaction) {
            ignore_clients = m_ignore_clients;
            time_to_live = m_time_to_live;
            compaction_interval = std::chrono::seconds::max();
            return true;
        }
        return false;
    }

private:
    std::mt19937_64 m_random;
    const bool m_enable_compaction;
    const bool m_ignore_clients;
    std::chrono::seconds m_time_to_live;
};

struct SyncServerVacuumFile : Vacuum::VacuumFile, _impl::ServerHistory::DummyCompactionControl {
    SyncServerVacuumFile(util::Logger& logger, const Vacuum::Options& options, const std::string& path)
        : VacuumFile(logger, options, path)
        , m_context{!options.no_log_compaction, options.ignore_clients, options.server_history_ttl}
    {
        auto server_history = std::make_unique<ServerHistory>(m_context, *this);
        m_server_history = server_history.get();
        DBOptions sg_options;
        sg_options.allow_file_format_upgrade = !options.no_file_upgrade;
        if (options.encryption_key)
            sg_options.encryption_key = options.encryption_key->data();
        m_sg = DB::create(std::move(server_history), path, sg_options);
        m_sg->claim_sync_agent();
    }

    std::string get_type_description() const override
    {
        return "Sync Server";
    }

    void dry_run(Vacuum::Results& results) override
    {
        TransactionRef tr = m_sg->start_write(); // Throws
        if (!m_options.no_log_compaction) {
            m_server_history->compact_history(tr, logger); // Throws
        }
        results.after_size = tr->compute_aggregated_byte_size();
        // Rollback transaction
    }

    void vacuum(Vacuum::Results& results) override
    {
        if (!m_options.no_log_compaction) {
            TransactionRef tr = m_sg->start_write(); // Throws
            m_server_history->compact_history(tr, logger);
            tr->commit(); // Throws
        }
        if (!m_options.no_file_compaction) {
            bool bump_version_number = m_options.bump_realm_version;
            if (!m_sg->compact(bump_version_number)) {
                throw VacuumError{std::string{"Another process is using '" + m_path + "'. Aborting vacuum."}};
            }
        }
        // Get "after" size
        File f{m_path, File::mode_Read};
        results.after_size = size_t(f.get_size());
    }

    ServerHistoryContext m_context;
    ServerHistory* m_server_history;
};


Replication::HistoryType detect_history_type(const std::string& file, const char* encryption_key)
{
    // Open in read-only mode to detect the history type.
    Group group{file, encryption_key};
    ref_type top_ref = GroupFriend::get_top_ref(group);
    if (top_ref == 0)
        return Replication::hist_None;
    History::version_type version;
    int history_type;
    int history_schema_version;
    GroupFriend::get_version_and_history_info(GroupFriend::get_alloc(group), top_ref, version, history_type,
                                              history_schema_version);
    switch (history_type) {
        case Replication::hist_None:
            if (version == 1)
                throw VacuumError{std::string{"Auto detection of history is not allowed for a Realm "
                                              "with History type None and version = 1: " +
                                              file}};
            else
                return Replication::hist_None;
        case Replication::hist_InRealm:
            return Replication::hist_InRealm;
        case Replication::hist_SyncClient:
            return Replication::hist_SyncClient;
        case Replication::hist_SyncServer:
            return Replication::hist_SyncServer;
        case Replication::hist_OutOfRealm:
            return Replication::hist_OutOfRealm;
        default:
            throw VacuumError{std::string{"Unknown history type: "} + file};
    }
}

std::unique_ptr<Vacuum::VacuumFile> make_vacuum_file(util::Logger& logger, const Vacuum::Options& options,
                                                     Replication::HistoryType type, const std::string& realm_path)
{
    switch (type) {
        case Replication::hist_None:
            return std::make_unique<PlainVacuumFile>(logger, options, realm_path);
        case Replication::hist_InRealm:
            return std::make_unique<PlainVacuumFile>(logger, options, realm_path);
            break;
        case Replication::hist_SyncClient:
            return std::make_unique<SyncClientVacuumFile>(logger, options, realm_path);
            break;
        case Replication::hist_SyncServer:
            return std::make_unique<SyncServerVacuumFile>(logger, options, realm_path);
            break;
        case Replication::hist_OutOfRealm:
            return std::make_unique<PlainVacuumFile>(logger, options, realm_path);
            break;
    }
    REALM_TERMINATE("Invalid history type.");
    return nullptr;
}

} // unnamed namespace

Vacuum::Results Vacuum::vacuum(const std::string& path)
{
    using steady_clock = std::chrono::steady_clock;
    steady_clock::time_point t_0 = steady_clock::now();

    Results results;

    // "Before" file size
    {
        File f{path, File::mode_Read}; // Throws
        results.before_size = size_t(f.get_size());
    }

    const char* encryption_key = nullptr;
    if (m_options.encryption_key)
        encryption_key = m_options.encryption_key->data();
    Replication::HistoryType history_type =
        m_options.history_type ? *m_options.history_type : detect_history_type(path, encryption_key); // Throws
    auto vacuum_file = make_vacuum_file(logger, m_options, history_type, path);
    results.type_description = vacuum_file->get_type_description(); // Throws
    vacuum_file->vacuum(results);                                   // Throws
    steady_clock::time_point t_1 = steady_clock::now();
    results.time = std::chrono::duration_cast<std::chrono::microseconds>(t_1 - t_0);
    return results;
}

Vacuum::Results Vacuum::dry_run(const std::string& path)
{
    Results results;

    // "Before" file size
    {
        File f{path, File::mode_Read}; // Throws
        results.before_size = size_t(f.get_size());
    }

    const char* encryption_key = nullptr;
    if (m_options.encryption_key)
        encryption_key = m_options.encryption_key->data();
    Replication::HistoryType history_type =
        m_options.history_type ? *m_options.history_type : detect_history_type(path, encryption_key); // Throws
    auto vacuum_file = make_vacuum_file(logger, m_options, history_type, path);
    results.type_description = vacuum_file->get_type_description(); // Throws
    vacuum_file->dry_run(results);                                  // Throws
    return results;
}
