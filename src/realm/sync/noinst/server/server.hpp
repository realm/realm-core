#ifndef REALM_SYNC_SERVER_HPP
#define REALM_SYNC_SERVER_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <map>
#include <set>
#include <exception>

#include <realm/util/logger.hpp>
#include <realm/util/optional.hpp>
#include <realm/util/time.hpp>
#include <realm/sync/network/network.hpp>
#include <realm/sync/noinst/server/clock.hpp>
#include <realm/sync/noinst/server/crypto_server.hpp>
#include <realm/sync/client.hpp>

namespace realm {
namespace sync {

// FIXME: Currently this exception is only used when the server runs out of
// file descriptors at connection accept.
class OutOfFilesError : public std::exception {
public:
    OutOfFilesError(const std::error_code ec)
        : m_ec{ec}
    {
    }
    std::error_code code() const noexcept
    {
        return m_ec;
    }
    const char* what() const noexcept override
    {
        return "Out of file despriptors (EMFILE)";
    }

private:
    std::error_code m_ec;
};


/// \brief Server of the Realm synchronization protocol.
///
/// Instances of this class are servers of the WebSocket-based Realm
/// synchronization protocol (`/doc/protocol.md`), and are generally referred to
/// simply as *sync servers*.
///
/// No agent external to a sync server is allowed to open Realm files belonging
/// to that sync server (in \a root_dir as passed to the constructor) while that
/// sync server is running.
class Server {
public:
    using SessionBootstrapCallback = void(std::string_view virt_path, file_ident_type client_file_ident);

    // FIXME: The default values for `http_request_timeout`,
    // `http_response_timeout`, `connection_reaper_timeout`, and
    // `soft_close_timeout` ought to be much lower (1 minute, 30 seconds, 3
    // minutes, and 30 seconds respectively) than they are. Their current values
    // are due to the fact that the server is single threaded, and that some
    // operations take more than 5 minutes to complete.
    static constexpr milliseconds_type default_http_request_timeout = 600000;       // 10 minutes
    static constexpr milliseconds_type default_http_response_timeout = 600000;      // 10 minutes
    static constexpr milliseconds_type default_connection_reaper_timeout = 1800000; // 30 minutes
    static constexpr milliseconds_type default_connection_reaper_interval = 60000;  //  1 minute
    static constexpr milliseconds_type default_soft_close_timeout = 600000;         // 10 minutes

    struct Config {
        Config() {}

        /// The maximum number of Realm files that will be kept open
        /// concurrently by each major thread inside the server. The server
        /// currently has two major threads (foreground and background). The
        /// server keeps a cache of open Realm files for efficiency reasons (one
        /// for each major thread).
        long max_open_files = 256;

        /// An optional custom clock to be used for token expiration checks. If
        /// no clock is specified, the server will use the system clock.
        Clock* token_expiration_clock = nullptr;

        /// An optional thread-safe logger to be used by the server. If no
        /// logger is specified, the server will use an instance of
        /// util::StderrLogger with the log level threshold set to
        /// util::Logger::Level::info.
        std::shared_ptr<util::Logger> logger;

        /// A unique id of this server. Used in the backup protocol to tell
        /// slaves apart.
        std::string id = "unknown";

        /// The address at which the listening socket is bound.
        /// The address can be a name or on numerical form.
        /// Use "localhost" to listen on the loopback interface.
        std::string listen_address;

        /// The port at which the listening socket is bound.
        /// The port can be a name or on numerical form.
        /// Use the empty string to have the system assign a dynamic
        /// listening port.
        std::string listen_port;

        bool reuse_address = true;

        /// authorization_header_name sets the name of the HTTP header used to
        /// receive the Realm access token. The value of the HTTP header is
        /// "Bearer <token>"
        std::string authorization_header_name = "Authorization";

        /// The listening socket accepts TLS/SSL connections if `ssl` is
        /// true, and non-secure tcp connections otherwise.
        bool ssl = false;

        /// The path of the certificate that will be sent to clients during
        /// the SSL/TLS handshake.
        ///
        /// From the point of view of OpenSSL, this file will be passed to
        /// `SSL_CTX_use_certificate_chain_file()`.
        ///
        /// This option is ignored if `ssl` is false.
        std::string ssl_certificate_path;

        /// The path of the private key corresponding to the certificate.
        ///
        /// From the point of view of OpenSSL, this file will be passed to
        /// `SSL_CTX_use_PrivateKey_file()`.
        ///
        /// This option is ignored if `ssl` is false.
        std::string ssl_certificate_key_path;

        /// The time allotted to the reception of a complete HTTP request. This
        /// counts from the point in time where the raw TCP connection is
        /// accepted by the server, or, in case of HTTP pipelining, from the
        /// point in time where writing of the previous response completed. If
        /// this time is exceeded, the connection will be terminated by the
        /// server.
        milliseconds_type http_request_timeout = default_http_request_timeout;

        /// The time allotted to the transmission of the complete HTTP
        /// response. If this time is exceeded, the connection will be
        /// terminated by the server.
        milliseconds_type http_response_timeout = default_http_response_timeout;

        /// If no heartbeat, and no other message has been received via a
        /// connection for a certain amount of time, that connection will be
        /// discarded by the connection reaper. This option specifies that
        /// amount of time. See also \ref connection_reaper_interval.
        milliseconds_type connection_reaper_timeout = default_connection_reaper_timeout;

        /// The time between activations of the connection reaper. On each
        /// activation, every connection is checked for vitality (see \ref
        /// connection_reaper_timeout).
        milliseconds_type connection_reaper_interval = default_connection_reaper_interval;

        /// In some cases, the server attempts so send an ERROR message to the
        /// client before closing the connection (a soft close). The server will
        /// then wait for the client to close the
        /// connection. `soft_close_timeout` specifies the maximum amount of
        /// time, that the server will wait before terminating the connection
        /// itself. This counts from when writing of the ERROR message is
        /// initiated.
        milliseconds_type soft_close_timeout = default_soft_close_timeout;

        /// If set to true, the server will cache the contents of the DOWNLOAD
        /// message(s) used for client bootstrapping.
        bool enable_download_bootstrap_cache = false;

        /// The accumulated size of changesets that are included in download
        /// messages. The size of the changesets is calculated before log
        /// compaction (if enabled). A larger value leads to more efficient
        /// log compaction and download, at the expense of higher memory pressure,
        /// higher latency for sending the first changeset, and a higher probability
        /// for the need to resend the same changes after network disconnects.
        std::size_t max_download_size = 0x1000000; // 16 MiB

        /// The maximum number of connections that can be queued up waiting to
        /// be accepted by the server. This corresponds to the `backlog`
        /// argument of the `listen()` function as described by POSIX.
        ///
        /// On Linux, the specified value will be clamped to the value of the
        /// kernel parameter `net.core.somaxconn` (also available as
        /// `/proc/sys/net/core/somaxconn`). You can change the value of that
        /// parameter using `sysctl -w net.core.somaxconn=...`. It is usually
        /// 128 by default.
        int listen_backlog = network::Acceptor::max_connections;

        /// Set the `TCP_NODELAY` option on all TCP/IP sockets. This disables
        /// the Nagle algorithm. Disabling it, can in some cases be used to
        /// decrease latencies, but possibly at the expense of scalability. Be
        /// sure to research the subject before you enable this option.
        bool tcp_no_delay = false;

        /// An optional 64 byte key to encrypt all files with.
        std::optional<std::array<char, 64>> encryption_key;

        /// Sets a limit on the allowed accumulated size in bytes of buffered
        /// incoming changesets waiting to be processed. If left at zero, an
        /// implementation defined default value will be chosen.
        ///
        /// If the accumulated size of the currently buffered incoming
        /// changesets exceeds this limit, then no additional UPLOAD messages
        /// will be accepted by the server. Instead, if an UPLOAD message is
        /// received, the server will terminate the session, and its
        /// corresponding connection by sending
        /// `ProtocolError::connection_closed`, which in this case, can be taken
        /// to mean, "try again later".
        ///
        /// FIXME: Part of a very poor man's substitute for a proper
        /// backpressure scheme.
        std::size_t max_upload_backlog = 0;

        /// Disable sync to disk (fsync(), msync()) for all realm files managed
        /// by this server.
        ///
        /// Testing/debugging feature. Should never be enabled in production.
        bool disable_sync_to_disk = false;

        /// Restrict the range of protocol versions that the server will offer
        /// during negotiation with clients.
        ///
        /// A value of zero means 'unspecified'.
        ///
        /// If a nonzero value is specified, then all versions that are actually
        /// supported, but are greater than the specified value, will be
        /// excluded from the set of effectively supported versions.
        ///
        /// If this leaves the effective set of supported versions empty, the
        /// server constructor will throw NoSupportedProtocolVersions.
        ///
        /// \sa get_current_protocol_version()
        int max_protocol_version = 0;

        /// Disable the download process for the specified client files. This
        /// includes the sending of empty DOWNLOAD messages.
        ///
        /// This feature exists exclusively for testing purposes.
        std::set<file_ident_type> disable_download_for;

        /// If specified, this function will be called for each synchronization
        /// session that is successfully bootstrapped at the time of reception
        /// of the IDENT message.
        ///
        /// This feature exists exclusively for testing purposes.
        std::function<SessionBootstrapCallback> session_bootstrap_callback;
    };

    /// See Config::max_protocol_version.
    class NoSupportedProtocolVersions;

    /// \throw NoSupportedProtocolVersions See Config::max_protocol_version.
    Server(const std::string& root_dir, util::Optional<PKey> public_key, Config = {});

    Server(Server&&) noexcept;
    virtual ~Server() noexcept;

    /// start() binds a listening socket to the address and port specified in
    /// Config and starts accepting connections.
    /// The resolved endpoint (including the dynamically assigned port, if requested)
    /// can be obtained by calling listen_endpoint().
    /// This can be done immediately after start() returns.
    void start();

    /// A helper function, for backwards compatibility, that starts a listening
    /// socket without SSL at the specified address and port.
    void start(const std::string& listen_address, const std::string& listen_port, bool reuse_address = true);

    /// Return the resolved and bound endpoint of the listening socket.
    network::Endpoint listen_endpoint() const;

    /// Run the internal network event-loop of the server. At most one thread
    /// may execute run() at any given time. It is an error if run() is called
    /// before start() has been successfully executed. The call to run() will
    /// not return until somebody calls stop() or an exception is thrown.
    void run();

    /// Stop any thread that is currently executing run(). This function may be
    /// called by any thread.
    void stop() noexcept;

    /// Must not be called while run() is executing.
    std::uint_fast64_t errors_seen() const noexcept;

    /// stop_sync_and_wait_for_backup_completion() will immediately drop all
    /// client connections, and wait for backup completion. New client
    /// connections will be rejected. The completion handler is called either
    /// when a backup slave has a full copy of all Realms or after timing out.
    /// The function is only supposed to be used on the backup master.
    ///
    /// The function times out depending on the value of the argument
    /// 'timeout'.  If 'timeout' is non-negative, the function times out after
    /// 'timeout' milliseconds.  The completion handler is guaranteed to be
    /// called after time out. If 'timeout' is -1, the function never times
    /// out.
    ///
    /// The signature of the completion handler is void(bool did_complete),
    /// where did_complete is true if the master knows that the slave is up to
    /// date, and false otherwise. If the server is not a backup master, the
    /// completion handler will be called with did_complete set to false.
    ///
    /// If there is no connected backup slave, or the backup slave disconnects,
    /// the server will wait for the backup slave to reconnect and obtain a
    /// full copy of all Realms.
    ///
    /// After the completion handler is called, the user will still have to stop
    /// the server event loop by calling stop() and destroy the server
    /// object. stop_sync_and_wait_for_backup_completion() can be called from
    /// any thread. If the function is called multiple times before completion,
    /// only one of the completion handlers will be called.
    ///
    /// The completion handler will be called by the thread that executes run().
    ///
    /// CAUTION: The completion handler may be called before
    /// stop_sync_and_wait_for_backup_completion() returns.
    void stop_sync_and_wait_for_backup_completion(util::UniqueFunction<void(bool did_complete)> completion_handler,
                                                  milliseconds_type timeout);

    /// See Config::connection_reaper_timeout..
    void set_connection_reaper_timeout(milliseconds_type);

    /// Close all connections with error code ProtocolError::connection_closed.
    ///
    /// This function exists mainly for debugging purposes.
    void close_connections();

    /// Map the specified virtual Realm path to a real file system path. The
    /// returned path will be absolute if, and only if the root directory path
    /// passed to the server constructor was absolute.
    ///
    /// If the specified virtual path is valid, this function assigns the
    /// corresponding file system path to `real_path` and returns
    /// true. Otherwise it returns false.
    ///
    /// This function is fully thread-safe and may be called at any time during
    /// the life of the server object.
    bool map_virtual_to_real_path(const std::string& virt_path, std::string& real_path);

    /// Inform the server about an external change to one of the Realm files
    /// managed by the server.
    ///
    /// CAUTION: On a server where backup is enabled, Realm files are not
    /// allowed to be modified by agents external to the server, i.e., they are
    /// not allowed to be modified, except by the server itself.
    ///
    /// This function is fully thread-safe and may be called at any time during
    /// the life of the server object.
    void recognize_external_change(const std::string& virt_path);

    /// Get accumulated time spent on runs of the worker thread(s) since start
    /// of the server.
    void get_workunit_timers(milliseconds_type& parallel_section, milliseconds_type& sequential_section);

private:
    class Implementation;
    std::unique_ptr<Implementation> m_impl;
};


class Server::NoSupportedProtocolVersions : public std::exception {
public:
    const char* what() const noexcept override final
    {
        return "No supported protocol versions";
    }
};

} // namespace sync
} // namespace realm

#endif // REALM_SYNC_SERVER_HPP
