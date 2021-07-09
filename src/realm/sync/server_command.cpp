#include <realm/util/features.h>
#include <realm/util/optional.hpp>
#include <realm/util/logger.hpp>
#include <realm/util/file.hpp>
#include <realm/util/load_file.hpp>
#include <realm/util/timestamp_logger.hpp>
#include <realm/sync/noinst/reopening_file_logger.hpp>
#include <realm/sync/metrics.hpp>
#include <realm/sync/server.hpp>
#include <realm/sync/server_configuration.hpp>

#include <iostream>
#include <memory>
#include <signal.h>
#include <string>

using namespace realm;

namespace {

volatile sig_atomic_t g_reopen_log_file = 0;

void hup_signal_handler(int)
{
    g_reopen_log_file = 1;
}

} // unnamed namespace


int main(int argc, char* argv[])
{
    config::Configuration config;
    realm::config::build_configuration(argc, argv, config); // Throws

    // Preliminary logger
    std::unique_ptr<util::RootLogger> root_logger = std::make_unique<util::StderrLogger>(); // Throws
    root_logger->set_level_threshold(config.log_level);

    // This creates missing directory structure.
    sync::ensure_server_workdir(config, *root_logger); // Throws

    // Set up the requested type of logger
    if (config.log_to_file) {
        std::string path = sync::get_log_file_path(config); // Throws

#ifndef _WIN32
        signal(SIGHUP, hup_signal_handler);
#endif
        _impl::ReopeningFileLogger::TimestampConfig config_2;
        config_2.precision = _impl::ReopeningFileLogger::Precision::milliseconds;
        config_2.format = "%FT%T";
        root_logger = std::make_unique<_impl::ReopeningFileLogger>(path, g_reopen_log_file,
                                                                   std::move(config_2)); // Throws
    }
    else if (config.log_include_timestamp) {
        util::TimestampStderrLogger::Config config_2;
        config_2.precision = util::TimestampStderrLogger::Precision::milliseconds;
        config_2.format = "%FT%T";
        root_logger = std::make_unique<util::TimestampStderrLogger>(std::move(config_2)); // Throws
    }

    util::ThreadSafeLogger logger{*root_logger, config.log_level};

    util::Optional<sync::PKey> pkey;
    try {
        pkey = sync::PKey::load_public(*config.public_key_path);
    }
    catch (sync::CryptoError& e) {
        std::cerr << "Error while loading public key file: " << e.what() << '\n';
        std::exit(EXIT_FAILURE);
    }

    std::string lockfile_path = sync::get_workdir_lockfile_path(config);
    sync::ServerWorkdirLock workdir_lock{lockfile_path}; // Throws

    std::unique_ptr<sync::Metrics> metrics =
        sync::make_buffered_statsd_metrics(config.dashboard_stats_endpoint, config.metrics_prefix,
                                           config.metrics_exclusions); // Throws

    // This performs prechecking and migration from legacy format if needed.
    sync::prepare_server_workdir(config, logger, *metrics); // Throws

    sync::Server::ClientFileBlacklists client_file_blacklists =
        sync::load_client_file_blacklists(config, logger); // Throws

    // We use a unique_ptr here so that we can handle file access exceptions
    // for the constructor differently than exceptions that might occur later
    // on in the server's execution (`AccessError` can be raised during
    // `sync::Server`'s ctor, while opening the root directory, but can also be
    // raised later on while accessing individual realm files.
    std::unique_ptr<sync::Server> server;
    try {
        sync::Server::Config config_2;
        config_2.listen_address = config.listen_address;
        config_2.listen_port = config.listen_port;
        config_2.reuse_address = config.reuse_address;
        config_2.http_request_timeout = config.http_request_timeout;
        config_2.http_response_timeout = config.http_response_timeout;
        config_2.connection_reaper_timeout = config.connection_reaper_timeout;
        config_2.connection_reaper_interval = config.connection_reaper_interval;
        config_2.soft_close_timeout = config.soft_close_timeout;
        config_2.max_open_files = config.max_open_files;
        config_2.logger = &logger;
        config_2.metrics = &*metrics;
        config_2.ssl = config.ssl;
        config_2.ssl_certificate_path = config.ssl_certificate_path;
        config_2.ssl_certificate_key_path = config.ssl_certificate_key_path;
        config_2.disable_download_compaction = config.disable_download_compaction;
        config_2.enable_download_bootstrap_cache = config.enable_download_bootstrap_cache;
        config_2.max_download_size = config.max_download_size;
        config_2.listen_backlog = config.listen_backlog;
        config_2.tcp_no_delay = config.tcp_no_delay;
        config_2.log_lsof_period = config.log_lsof_period;
        config_2.disable_history_compaction = config.disable_history_compaction;
        config_2.history_ttl = config.history_ttl;
        config_2.history_compaction_interval = config.history_compaction_interval;
        config_2.history_compaction_ignore_clients = config.history_compaction_ignore_clients;
        config_2.encryption_key = config.encryption_key;
        config_2.client_file_blacklists = std::move(client_file_blacklists);
        config_2.max_upload_backlog = config.max_upload_backlog;
        config_2.disable_sync_to_disk = config.disable_sync_to_disk;
        config_2.max_protocol_version = config.max_protocol_version;
        server.reset(new sync::Server(config.user_data_dir, std::move(pkey), config_2)); // Throws
    }
    catch (util::File::AccessError& e) {
        std::cerr << "Error while opening root directory `" << *config.root_dir
                  << "': "
                     ""
                  << e.what() << '\n';
        std::exit(EXIT_FAILURE);
    }

    server->start(config.listen_address, config.listen_port, config.reuse_address);
    server->run();
}
