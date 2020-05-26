#include <realm/sync/noinst/vacuum.hpp>

#include <realm/group.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/noinst/server_history.hpp>
#include <realm/sync/history.hpp>

#include <realm/version.hpp>

using namespace realm;
using namespace realm::sync;
using namespace realm::_impl;

// FIXME: This constexpr check belongs in Core's version.hpp
static constexpr bool core_version_at_least(int major, int minor, int patch)
{
    // FIXME: Also compare the 'extra' component of the version. To do this, we
    // need the C++20 constexpr version of std::equal().
    return (REALM_VERSION_MAJOR > major) ||
           (REALM_VERSION_MAJOR == major &&
            ((REALM_VERSION_MINOR > minor) || (REALM_VERSION_MINOR == minor && REALM_VERSION_PATCH >= patch)));
}

static_assert(core_version_at_least(5, 6, 0), "Vacuum is only supported on Core version >= 5.6.0");

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
    std::unique_ptr<Replication> m_repl;
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
        ClientReplication::Config history_config;
        history_config.owner_is_sync_agent = false; // Prevent "multiple sync agents" error
        auto client_history = sync::make_client_replication(path, history_config);
        m_client_history = client_history.get();
        m_repl = std::move(client_history);
        DBOptions sg_options;
        sg_options.allow_file_format_upgrade = !options.no_file_upgrade;
        if (options.encryption_key)
            sg_options.encryption_key = options.encryption_key->data();
        m_sg = DB::create(*m_repl, sg_options);
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

    ClientReplication* m_client_history = nullptr;
};

class ServerHistoryContext : public _impl::ServerHistory::Context {
public:
    explicit ServerHistoryContext(bool enable_compaction, bool ignore_clients, std::chrono::seconds time_to_live)
        : m_enable_compaction{enable_compaction}
        , m_ignore_clients{ignore_clients}
        , m_time_to_live{time_to_live}
    {
    }

    bool owner_is_sync_server() const noexcept override final
    {
        return true;
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
        auto server_history = std::make_unique<ServerHistory>(path, m_context, *this);
        m_server_history = server_history.get();
        m_repl = std::move(server_history);
        DBOptions sg_options;
        sg_options.allow_file_format_upgrade = !options.no_file_upgrade;
        if (options.encryption_key)
            sg_options.encryption_key = options.encryption_key->data();
        m_sg = DB::create(*m_repl, sg_options);
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
    Group group{file, encryption_key, Group::OpenMode::mode_ReadOnly};
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
