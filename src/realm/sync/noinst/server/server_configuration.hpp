#ifndef REALM_SYNC_SERVER_CONFIGURATION_HPP
#define REALM_SYNC_SERVER_CONFIGURATION_HPP

#include <vector>

#include <realm/util/logger.hpp>
#include <realm/util/optional.hpp>
#include <realm/util/file.hpp>
#include <realm/sync/noinst/server/metrics.hpp>
#include <realm/sync/noinst/server/server.hpp>

namespace realm {
namespace config {

/// Returns `realm.<hostname>` where `<hostname>` is whatever is returned by
/// util::network::host_name().
std::string get_default_metrics_prefix();

struct Configuration {
    std::string id = "";
    std::string listen_address = "127.0.0.1";
    std::string listen_port = ""; // Empty means choose default based on `ssl`.
    realm::util::Optional<std::string> root_dir;
    std::string user_data_dir;
    realm::util::Optional<std::string> public_key_path;
    realm::util::Optional<std::string> config_file_path;
    bool reuse_address = true;
    realm::util::Logger::Level log_level = realm::util::Logger::Level::info;
    bool log_include_timestamp = false;
    long max_open_files = 256;
    std::string authorization_header_name = "Authorization";
    bool ssl = false;
    std::string ssl_certificate_path;
    std::string ssl_certificate_key_path;
    std::string dashboard_stats_endpoint = "localhost:28125";
    sync::milliseconds_type http_request_timeout = sync::Server::default_http_request_timeout;
    sync::milliseconds_type http_response_timeout = sync::Server::default_http_response_timeout;
    sync::milliseconds_type connection_reaper_timeout = sync::Server::default_connection_reaper_timeout;
    sync::milliseconds_type connection_reaper_interval = sync::Server::default_connection_reaper_interval;
    sync::milliseconds_type soft_close_timeout = sync::Server::default_soft_close_timeout;
    bool disable_history_compaction = false;
    std::chrono::seconds history_ttl = std::chrono::seconds::max();
    std::chrono::seconds history_compaction_interval = std::chrono::seconds{3600};
    bool history_compaction_ignore_clients = false;
    bool disable_download_compaction = false;
    bool enable_download_bootstrap_cache = false;
    std::size_t max_download_size = 0x1000000; // 16 MB
    int listen_backlog = util::network::Acceptor::max_connections;
    bool tcp_no_delay = false;
    bool is_subtier_server = false;
    std::string upstream_url;
    std::string upstream_access_token;
    util::Optional<std::array<char, 64>> encryption_key;
    std::size_t max_upload_backlog = 0;
    bool disable_sync_to_disk = false;
    int max_protocol_version = 0;

    /// If set to true, the partial sync completion mechanism will be disabled.
    bool disable_psync_completer = false;

    /// If nonempty, the effective prefix will be what you specify plus a dot
    /// (`.`). If empty, there will be no prefix.
    std::string metrics_prefix = get_default_metrics_prefix();

    /// A blacklist of metrics options.
    /// The exclusions can be a bitwise OR of different options.
    /// This can reduce noise in the network, but can also be a way to
    /// increase performance, as some metrics are costly to compute.
    realm::sync::MetricsOptions::OptionType metrics_exclusions = realm::sync::MetricsOptions::Core_All;

    /// In the case of the Node.js wrapper, if \a log_to_file is set to true,
    /// all logging from the sync server will be forwarded both to a file
    /// (`<root>/var/server.log`) and to Node.js. If left as false, log messages
    /// will only be forwarded to Node.js.
    ///
    /// In the case of the stand-alone server command, if \a log_to_file is set
    /// to true, the log will be sent to the log file (`<root>/var/server.log`)
    /// and only to that file. If left as false, log messages will instead be
    /// sent to STDERR.
    bool log_to_file = false;
};

#if !REALM_MOBILE
void show_help(const std::string& program_name);
void build_configuration(int argc, char* argv[], Configuration&);
#endif

Configuration load_configuration(std::string configuration_file_path);

} // namespace config


namespace sync {

/// Initialise the directory structure as required (create missing directory
/// structure) for correct operation of the server.
void ensure_server_workdir(const config::Configuration&, util::Logger&);

std::string get_workdir_lockfile_path(const config::Configuration&);

std::string get_log_file_path(const config::Configuration&);

class ServerWorkdirLock {
public:
    ServerWorkdirLock(const std::string& lockfile_path);

private:
    util::File m_file;
};

/// Perform server-wide migrations and Realm file prechecking. This function is
/// supposed to be executed prior to instantiating the \c Server object.
///
/// Note: This function also handles migration of server-side Realm files from
/// the legacy format (see _impl::ensure_legacy_migration_1()).
///
/// The type of migration performed by this function is nonatomic, and it
/// therefore requires that no other thread or process has any of the servers
/// Realm files open concurrently. The application is advised to make sure that
/// all agents (including the sync server), that might open server-side Realm
/// files are not started until after this function has completed sucessfully.
void prepare_server_workdir(const config::Configuration&, util::Logger&, Metrics&);

Server::ClientFileBlacklists load_client_file_blacklists(const config::Configuration&, util::Logger&);

} // namespace sync
} // namespace realm

#endif // REALM_SYNC_SERVER_CONFIGURATION_HPP
