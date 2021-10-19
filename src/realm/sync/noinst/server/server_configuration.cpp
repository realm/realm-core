#include <cstdlib>
#include <memory>
#include <iostream>
#include <thread>

#include <realm/db.hpp>
#include <realm/util/features.h>
#include <realm/util/timestamp_formatter.hpp>
#include <realm/util/file.hpp>
#include <realm/util/network.hpp>
#include <realm/string_data.hpp>
#include <realm/sync/noinst/server/encrypt_fingerprint.hpp>
#include <realm/sync/noinst/server/server_legacy_migration.hpp>
#include <realm/sync/noinst/server/server_configuration.hpp>

#if !REALM_MOBILE
#include <realm/sync/noinst/server/command_line_util.hpp>
using realm::config::show_help;
#endif

using namespace realm;

using config::Configuration;
using util::Logger;

namespace {

#if !REALM_MOBILE

void parse_arguments(int argc, char* argv[], Configuration& configuration)
{
    static struct option long_options[] = {
        // clang-format off
        {"root",                                 required_argument, nullptr, 'r'},
        {"listen-address",                       required_argument, nullptr, 'L'},
        {"listen-port",                          required_argument, nullptr, 'p'},
        {"http-request-timeout",                 required_argument, nullptr, 'J'},
        {"http-response-timeout",                required_argument, nullptr, 'M'},
        {"connection-reaper-timeout",            required_argument, nullptr, 'i'},
        {"connection-reaper-interval",           required_argument, nullptr, 'd'},
        {"soft-close-timeout",                   required_argument, nullptr, 'N'},
        {"log-level",                            required_argument, nullptr, 'l'},
        {"log-include-timestamp",                no_argument,       nullptr, 'Y'},
        {"log-to-file",                          no_argument,       nullptr, 'P'},
        {"public-key",                           required_argument, nullptr, 'k'},
        {"max-open-files",                       required_argument, nullptr, 'm'},
        {"help",                                 no_argument,       nullptr, 'h'},
        {"no-reuse-address",                     no_argument,       nullptr, 'n'},
        {"ssl",                                  no_argument,       nullptr, 's'},
        {"ssl-certificate",                      required_argument, nullptr, 'C'},
        {"ssl-private-key",                      required_argument, nullptr, 'K'},
        {"listen-backlog",                       required_argument, nullptr, 'b'},
        {"tcp-no-delay",                         no_argument,       nullptr, 'D'},
        {"history-ttl",                          required_argument, nullptr, 'H'},
        {"compaction-interval",                  required_argument, nullptr, 'I'},
        {"history-compaction-ignore-clients",    no_argument,       nullptr, 'q'},
        {"encryption-key",                       required_argument, nullptr, 'e'},
        {"max-upload-backlog",                   required_argument, nullptr, 'U'},
        {"enable-download-bootstrap-cache",      no_argument,       nullptr, 'B'},
        {"disable-sync-to-disk",                 no_argument,       nullptr, 'A'},
        {"max-protocol-version",                 required_argument, nullptr, 'o'},
        {"disable-serial-transacts",             no_argument,       nullptr, 'c'},
        {"disable-history-compaction",           no_argument,       nullptr, 'O'},
        {"disable-download-compaction",          no_argument,       nullptr, 'Q'},
        {"max-download-size",                    required_argument, nullptr, 'F'},
        {nullptr,                                0,                 nullptr, 0}
        // clang-format on
    };

    static const char* opt_desc = "r:L:p:J:M:i:d:N:l:YPk:m:hnsC:K:b:DSu:t:f:H:I:qe:jRGEa:g:U:BA12:v:x:o:cOQF:";

    int opt_index = 0;
    int opt;

    while ((opt = getopt_long(argc, argv, opt_desc, long_options, &opt_index)) != -1) {
        switch (opt) {
            case 'r':
                configuration.root_dir = std::string(optarg);
                break;
            case 'L':
                configuration.listen_address = optarg;
                break;
            case 'p': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                unsigned int v = 0;
                in >> v;
                if (in && in.eof()) {
                    configuration.listen_port = optarg;
                }
                else {
                    std::cerr << "Error: Invalid listen port value `" << optarg << "'.\n\n";
                    show_help(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'J': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                sync::milliseconds_type v = 0;
                in >> v;
                if (in && in.eof()) {
                    configuration.http_request_timeout = v;
                }
                else {
                    std::cerr << "Error: Invalid HTTP request timeout value `" << optarg << "'.\n\n";
                    show_help(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'M': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                sync::milliseconds_type v = 0;
                in >> v;
                if (in && in.eof()) {
                    configuration.http_response_timeout = v;
                }
                else {
                    std::cerr << "Error: Invalid HTTP response timeout value `" << optarg << "'.\n\n";
                    show_help(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'i': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                sync::milliseconds_type v = 0;
                in >> v;
                if (in && in.eof()) {
                    configuration.connection_reaper_timeout = v;
                }
                else {
                    std::cerr << "Error: Invalid connection reaper timeout value `" << optarg << "'.\n\n";
                    show_help(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'd': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                sync::milliseconds_type v = 0;
                in >> v;
                if (in && in.eof()) {
                    configuration.connection_reaper_interval = v;
                }
                else {
                    std::cerr << "Error: Invalid connection reaper interval value `" << optarg << "'.\n\n";
                    show_help(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'N': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                sync::milliseconds_type v = 0;
                in >> v;
                if (in && in.eof()) {
                    configuration.soft_close_timeout = v;
                }
                else {
                    std::cerr << "Error: Invalid soft close timeout value `" << optarg << "'.\n\n";
                    show_help(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'l':
                if (!_impl::parse_log_level(optarg, configuration.log_level)) {
                    std::cerr << "Error: Invalid log level value `" << optarg << "'.\n\n";
                    show_help(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
                break;
            case 'Y':
                configuration.log_include_timestamp = true;
                break;
            case 'P':
                configuration.log_to_file = true;
                break;
            case 'k':
                configuration.public_key_path = std::string(optarg);
                break;
            case 'm': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                long v = 0;
                in >> v;
                if (in && in.eof()) {
                    configuration.max_open_files = v;
                }
                else {
                    std::cerr << "Error: Invalid maximum number of open files `" << optarg << "'.\n\n";
                    show_help(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'h':
                show_help(argv[0]);
                std::exit(EXIT_SUCCESS);
                break;
            case 'n':
                configuration.reuse_address = false;
                break;
            case 's':
                configuration.ssl = true;
                break;
            case 'C':
                configuration.ssl_certificate_path = std::string(optarg);
                break;
            case 'K':
                configuration.ssl_certificate_key_path = std::string(optarg);
                break;
            case 'b': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                int v = 0;
                in >> v;
                if (in && in.eof()) {
                    configuration.listen_backlog = v;
                }
                else {
                    std::cerr << "Error: Invalid listen backlog `" << optarg << "'.\n\n";
                    show_help(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'D':
                configuration.tcp_no_delay = true;
                break;
            case 'H': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                uint_fast64_t v = 0;
                in >> v;
                if (in && in.eof()) {
                    configuration.history_ttl = std::chrono::seconds{v};
                }
                else {
                    std::cerr << "Error:: Invalid history_ttl `" << optarg << "'.\n\n";
                    show_help(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'I': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                uint_fast64_t v = 0;
                in >> v;
                if (in && in.eof()) {
                    configuration.history_compaction_interval = std::chrono::seconds{v};
                }
                else {
                    std::cerr << "Error:: Invalid compaction_interval `" << optarg << "'.\n\n";
                    show_help(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'q':
                configuration.history_compaction_ignore_clients = true;
                break;
            case 'e': {
                std::string encryption_key_path = std::string(optarg);
                try {
                    util::File file{encryption_key_path};
                    if (file.get_size() != 64) {
                        std::cerr << "The encryption key file must have size 64 bytes.\n";
                        std::exit(EXIT_FAILURE);
                    }
                    char buf[64];
                    file.read(buf, 64);
                    std::array<char, 64> key = std::array<char, 64>();
                    std::memcpy(key.data(), buf, 64);
                    configuration.encryption_key = key;
                }
                catch (util::File::AccessError& e) {
                    std::cerr << "The encryption key file could not be read: " << e.what() << "\n";
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'U': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                std::size_t v = 0;
                in >> v;
                if (in && in.eof()) {
                    configuration.max_upload_backlog = v;
                }
                else {
                    std::cerr << "Error: Invalid max upload backlog `" << optarg << "'.\n\n";
                    show_help(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'B':
                configuration.enable_download_bootstrap_cache = true;
                break;
            case 'A':
                configuration.disable_sync_to_disk = true;
                break;
            case 'o': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                int x = 0;
                in >> x;
                if (in && in.eof()) {
                    configuration.max_protocol_version = x;
                }
                else {
                    std::cerr << "Error: Invalid protocol version '" << optarg << "'.\n\n";
                    show_help(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            case 'O':
                configuration.disable_history_compaction = true;
                break;
            case 'Q':
                configuration.disable_download_compaction = true;
                break;
            case 'F': {
                std::istringstream in(optarg);
                in.unsetf(std::ios_base::skipws);
                std::size_t v = 0;
                in >> v;
                if (in && in.eof()) {
                    configuration.max_download_size = v;
                }
                else {
                    std::cerr << "Error: Invalid size `" << v << "'.\n\n";
                    show_help(argv[0]);
                    std::exit(EXIT_FAILURE);
                }
            } break;
            default:
                std::cerr << '\n';
                show_help(argv[0]);
                std::exit(EXIT_FAILURE);
                break;
        }
    }

    if (optind != argc) {
        std::cerr << "Error: This command does not support positional arguments (e.g.: `" << argv[optind] << "')\n\n";
        show_help(argv[0]);
        std::exit(EXIT_FAILURE);
    }
}

#endif // !REALM_MOBILE


void deduce_paths(Configuration& c)
{
    if (c.root_dir) {
        c.user_data_dir = *c.root_dir + "/user_data";
    }
}


void deduce_listen_port(Configuration& c)
{
    if (c.listen_port.empty())
        c.listen_port = (c.ssl ? "7801" : "7800");
}


void create_directory(const std::string& path, const char* description, util::Logger& logger)
{
    try {
        logger.debug("Attempting to create %1 directory at `%2'.", description, path);

        if (!util::try_make_dir(path)) {
            logger.debug("Directory `%1' already exists, continuing.", path);
        }
        else {
            logger.debug("Directory `%1' successfully created.", path);
        }
    }

    catch (const util::File::AccessError& e) {
        logger.fatal("Unable to create a required directory (`%1'): %2", e.get_path(), e.what());
        std::exit(EXIT_FAILURE);
    }
}

void verify_encryption_key_fingerprint(const std::string& root_dir,
                                       const util::Optional<std::array<char, 64>>& encryption_key)
{
    const std::string fingerprint_path = util::File::resolve("encryption_key_fingerprint", root_dir); // Throws
    bool file_exist = true;
    std::string file_content;
    try {
        util::File file{fingerprint_path};
        size_t size = size_t(file.get_size());
        std::unique_ptr<char[]> buf{new char[size]}; // Throws
        file.read(buf.get(), size);
        file_content = std::string{buf.get(), size};
    }
    catch (const util::File::NotFound&) {
        file_exist = false;
    }

    if (file_exist) {
        // The file exists. We verify the fingerprint with the current encryption key.
        bool verified = encrypt::verify_fingerprint(file_content, encryption_key); // Throws
        if (!verified) {
            std::ostringstream os;
            if (encryption_key) {
                os << "The server was started with an encryption key that "
                      "does not match the fingerprint file '"
                   << fingerprint_path
                   << "'. The reason for the mismatch "
                      "is either that the encryption key has been incorrectly"
                      " configured or that the server encryption key has "
                      "been rotated without simultaneously deleting the "
                      "fingerprint file. If the encryption key is known to be "
                      "correct, the fingerprint file should be removed. "
                      "Otherwise, the key should be changed.";
            }
            else {
                os << "The server was started without an encryption key. "
                      "According to the fingerprint file '"
                   << fingerprint_path
                   << "' the server Realms are encrypted. If the Realms "
                      "are known to be unencrypted, it is safe to delete "
                      "the fingerprint file. Otherwise, specify a correct "
                      "encryption key.";
            }
            throw std::runtime_error(os.str());
        }
    }
    else {
        // The file did not exist. This is likely the first incarnation of
        // the server. We create the fingerprint file.
        const std::string fingerprint = encrypt::calculate_fingerprint(encryption_key);
        util::File file{fingerprint_path, util::File::mode_Write}; // Throws
        file.write(fingerprint.data(), fingerprint.size());        // Throws
    }
}


class ReadAheadBuffer {
public:
    ReadAheadBuffer(std::size_t size = 4096)
        : m_buffer{std::make_unique<char[]>(size)} // Throws
        , m_size{size}
    {
        clear();
    }

    bool next(char& ch, util::File& file)
    {
        if (REALM_LIKELY(m_curr != m_end)) {
            ch = *m_curr++;
            return true;
        }
        std::size_t n = file.read(m_buffer.get(), m_size); // Throws
        if (REALM_LIKELY(n > 0)) {
            m_curr = m_buffer.get();
            m_end = m_curr + n;
            ch = *m_curr++;
            return true;
        }
        return false;
    }

    void clear() noexcept
    {
        m_curr = nullptr;
        m_end = nullptr;
    }

private:
    std::unique_ptr<char[]> m_buffer;
    std::size_t m_size;
    const char* m_curr;
    const char* m_end;
};


bool read_line(util::File& file, std::string& line, ReadAheadBuffer& read_ahead_buffer)
{
    line.clear();
    char ch;
    while (read_ahead_buffer.next(ch, file)) { // Throws
        if (ch == '\n')
            return true;
        line += ch; // Throws
    }
    return !line.empty();
}


class Tokenizer {
public:
    Tokenizer(StringData string) noexcept
        : m_begin{string.data()}
        , m_end{m_begin + string.size()}
    {
    }

    bool next(StringData& token, std::size_t& col_ndx) noexcept
    {
        // Search for beginning of token
        for (;;) {
            if (m_curr == m_end)
                return false;
            if (*m_curr == '#')
                return false; // Rest of input is a comment
            if (*m_curr != ' ')
                break;
            ++m_curr;
        }
        const char* data = m_curr;

        // Search for end of token
        for (;;) {
            ++m_curr;
            if (m_curr == m_end)
                break;
            if (*m_curr == '#')
                break; // Rest of input is a comment
            if (*m_curr == ' ')
                break;
        }

        std::size_t size = std::size_t(m_curr - data);
        token = {data, size};
        col_ndx = size = std::size_t(data - m_begin);
        return true;
    }

private:
    const char* const m_begin;
    const char* const m_end;
    const char* m_curr = m_begin;
};


void save_workdir_locking_debug_info(const std::string& lockfile_path, bool could_lock)
{
    std::string path = DB::get_core_file(lockfile_path, DB::CoreFileType::Log);
    util::File file{path, util::File::mode_Append}; // Throws
    util::File::Streambuf streambuf{&file};         // Throws
    std::ostream out{&streambuf};                   // Throws
    util::TimestampFormatter::Config config;
    config.utc_time = true;
    config.precision = util::TimestampFormatter::Precision::milliseconds;
    config.format = "%FT%TZ";
    util::TimestampFormatter timestamp_formatter{config}; // Throws
    std::string line;
    line += std::string(timestamp_formatter.format(std::chrono::system_clock::now())); // Throws
    line += (could_lock ? "|SUCCESS" : "|FAILURE");                                    // Throws
    line += "|" + util::network::host_name();                                          // Throws
    if (const char* str = std::getenv("REALM_SYNC_SERVER_LOCK_ID"))
        line += std::string("|") + str; // Throws
    line += "\n";                       // Throws
    out << line;                        // Throws
}

std::string load_workdir_locking_debug_info(const std::string& lockfile_path)
{
    std::string path = DB::get_core_file(lockfile_path, DB::CoreFileType::Log);
    util::File file{path};             // Throws
    ReadAheadBuffer read_ahead_buffer; // Throws
    std::vector<std::string> lines;
    std::string line;
    while (read_line(file, line, read_ahead_buffer)) // Throws
        lines.push_back(line);                       // Throws
    std::size_t num_lines = lines.size();
    std::size_t n = 25;
    std::size_t offset = 0;
    std::string result;
    if (num_lines > n) {
        offset = num_lines - n;
        result += util::to_string(offset) + " lines not shown"; // Throws
    }
    for (std::size_t i = offset; i < num_lines; ++i) {
        if (!result.empty())
            result += ", ";
        result += lines[i];
    }
    return result;
}

} // unnamed namespace


namespace realm {
namespace config {

std::string get_default_metrics_prefix()
{
    std::string prefix = util::network::host_name(); // Throws
    if (prefix.empty()) {
        prefix = "realm"; // Throws
    }
    else {
        prefix = "realm." + prefix; // Throws
    }
    return prefix;
}


#if !REALM_MOBILE

void show_help(const std::string& program_name)
{
    // clang-format off
    std::cerr <<
        "Usage: " << program_name << " [-r DIR] [OPTIONS]\n"
        "\n"
        "Arguments:\n"
        "\n"
        "  -r, --root PATH                The directory for server-side Realm files.\n"
        "  -k, --public-key PATH          The public key (PEM file) used to verify\n"
        "                                 access tokens sent by clients.\n"
        "\n"
        "Options:\n"
        "\n"
        "  -L, --listen-address ADDRESS   The listening address/interface. (default\n"
        "                                 127.0.0.1)\n"
        "  -p, --listen-port PORT         The listening port. (default 7800 for non-SSL,\n"
        "                                 and 7801 for SSL)\n"
        "  -J, --http-request-period NUM  The time, in milliseconds, allotted to the reception\n"
        "                                 of a complete HTTP request. This counts from the point\n"
        "                                 in time where the raw TCP connection is accepted by\n"
        "                                 the server, or, in case of HTTP pipelining, from the\n"
        "                                 point in time where writing of the previous response\n"
        "                                 completed. If this time is exceeded, the connection\n"
        "                                 will be terminated by the server. The default value is\n"
        "                                 60'000 (1 minute).\n"
        "  -M, --http-response-timeout NUM  The time, in milliseconds, allotted to the\n"
        "                                 transmission of the complete HTTP response. If this\n"
        "                                 time is exceeded, the connection will be terminated by\n"
        "                                 the server. The default value is 30'000 (30 seconds).\n"
        "  -i, --connection-reaper-timeout NUM  If no heartbeat, and no other message has been\n"
        "                                 received via a connection for a certain amount of\n"
        "                                 time, that connection will be discarded by the\n"
        "                                 connection reaper. This option specifies that amount\n"
        "                                 of time in milliseconds. The default value is 180'000\n"
        "                                 (3 minutes). See also\n"
        "                                 (`--connection-reaper-interval`).\n"
        "  -d, --connection-reaper-interval NUM  The time, in milæliseconds, between activations\n"
        "                                 of the connection reaper. On each activation, every\n"
        "                                 connection is checked for vitality (see\n"
        "                                 `--connection-reaper-timeout`). The default value is\n"
        "                                 60'000 (1 minute).\n"
        "  -N, --soft-close-timeout NUM   In some cases, the server attempts to send an ERROR\n"
        "                                 message to the client before closing the connection (a\n"
        "                                 soft close). The server will then wait for the client\n"
        "                                 to close the connection. This option specifies the\n"
        "                                 maximum amount of time in milliseconds, that the\n"
        "                                 server will wait before terminating the connection\n"
        "                                 itself. This counts from when writing of the ERROR\n"
        "                                 message is initiated. The default value is 30'000 (30\n"
        "                                 seconds).\n"
        "  -l, --log-level                Set log level. Valid values are 'all', 'trace',\n"
        "                                 'debug', 'detail', 'info', 'warn', 'error', 'fatal',\n"
        "                                 or 'off'. (default 'info')\n"
        "  -Y, --log-include-timestamp    Include timestamps in log messages.\n"
        "  -P, --log-to-file              Send log messages to `<root>/var/server.log` instead\n"
        "                                 of to STDERR (see `--root`).\n"
        "  -m, --max-open-files NUM       The maximum number of Realm files that the server will\n"
        "                                 have open concurrently (LRU cache). The default is 256.\n"
        "  -h, --help                     Display command-line synopsis followed by the\n"
        "                                 list of available options.\n"
        "  -n, --no-reuse-address         Disables immediate reuse of listening port.\n"
        "  -s, --ssl                      Communicate with clients over SSL (Secure Socket\n"
        "                                 Layer).\n"
        "  -C, --ssl-certificate PATH     The path of the certificate that will be sent to\n"
        "                                 clients during the SSL/TLS handshake.\n"
        "  -K, --ssl-private-key PATH     The path of the private key corresponding to the\n"
        "                                 certificate (`--ssl-certificate`).\n"
        "  -b, --listen-backlog NUM       The maximum number of connections that can be queued\n"
        "                                 up waiting to be accepted by this server.\n"
        "  -D, --tcp-no-delay             Disables the Nagle algorithm on all sockets accepted\n"
        "                                 by this server.\n"
        "  -H, --history-ttl SECONDS      The time in seconds that clients can be offline\n"
        "                                 before having to perform a reset. Default is\n"
        "                                 forever (never reset).\n"
        "  -q, --history-compaction-ignore-clients\n"
        "                                 If specified, the determination of how far in-place\n"
        "                                 history compaction can proceed will be based entirely\n"
        "                                 on the history itself, and the 'last access'\n"
        "                                 timestamps of client file entries will be completely\n"
        "                                 ignored. This should only be done in emergency\n"
        "                                 situations. Expect it to cause expiration of client\n"
        "                                 files even when they have seen acitivity within the\n"
        "                                 specified time to live (`--history-ttl`).\n"
        "  -e, --encryption-key PATH      The 512 bit key used to encrypt Realms.\n"
        "  -U, --max-upload-backlog NUM   Sets the limit on the allowed accumulated size in\n"
        "                                 bytes of buffered incoming changesets waiting to be\n"
        "                                 processed. If set to zero, an implementation defined\n"
        "                                 default value will be chosen.\n"
        "  -B, --enable-download-bootstrap-cache  Makes the server cache the contents of the\n"
        "                                 DOWNLOAD message(s) used for client bootstrapping.\n"
        "  -A, --disable-sync-to-disk     Disable sync to disk (msync(), fsync()).\n"
        "  -o, --max-protocol-version     Maximum protocol version to allow during negotiation\n"
        "                                 with clients. Zero means unspecified. Default is zero.\n"
        "  -O, --disable-history-compaction  Disable in-place compaction of main synchronziation\n"
        "                                 history.\n"
        "  -Q, --disable-download-compaction\n"
        "                                 Disable compaction during download.\n"
        "  -F, --max-download-size        See `sync::Server::Config::max_download_size`.\n"
        "\n";
    // clang-format on
}


void build_configuration(int argc, char* argv[], Configuration& config)
{
    // Force GNU getopt to behave in a POSIX-y way. This is required so that
    // positional argument detection is handled properly, and the same on all
    // platforms.
#ifdef _WIN32
    _putenv_s("POSIXLY_CORRECT", "1");
#else
    setenv("POSIXLY_CORRECT", "1", 0);
#endif

    parse_arguments(argc, argv, config);

    if (!config.root_dir) {
        std::cerr << "Error: Missing root directory configuration directive.\n";
        std::exit(EXIT_FAILURE);
    }

    else if (!config.public_key_path) {
        std::cerr << "Error: Missing public key configuration directive.\n";
        std::exit(EXIT_FAILURE);
    }

    deduce_paths(config);
    deduce_listen_port(config);
}

#endif // !REALM_MOBILE


Configuration load_configuration(std::string configuration_file_path)
{
    static_cast<void>(configuration_file_path);
    throw std::runtime_error("This version of the library was not built using "
                             "`yaml-cpp`, and hence does not support "
                             "`load_configuration()`.");
}

} // namespace config


void sync::ensure_server_workdir(const config::Configuration& config, util::Logger& logger)
{
    REALM_ASSERT(config.root_dir);
    const std::string& root_dir = *config.root_dir;

    std::string var_dir = util::File::resolve("var", root_dir); // Throws
    util::try_make_dir(var_dir);

    std::string realms_dir = util::File::resolve("user_data", root_dir); // Throws
    create_directory(realms_dir, "user data", logger);                   // Throws
}


std::string sync::get_workdir_lockfile_path(const config::Configuration& config)
{
    REALM_ASSERT(config.root_dir);
    std::string var_dir = util::File::resolve("var", *config.root_dir); // Throws
    return util::File::resolve("lock", var_dir);                        // Throws
}


std::string sync::get_log_file_path(const config::Configuration& config)
{
    REALM_ASSERT(config.root_dir);
    std::string var_dir = util::File::resolve("var", *config.root_dir); // Throws
    return util::File::resolve("server.log", var_dir);                  // Throws
}


sync::ServerWorkdirLock::ServerWorkdirLock(const std::string& lockfile_path)
    : m_file{lockfile_path, util::File::mode_Write} // Throws
{
    bool success = m_file.try_lock_exclusive();              // Throws
    save_workdir_locking_debug_info(lockfile_path, success); // Throws
    if (REALM_LIKELY(success))                               // Throws
        return;
    // Make sure the other party has time to save its debug info before we
    // attempt to read what is available
    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::string debug_info = load_workdir_locking_debug_info(lockfile_path); // Throws
    throw util::File::AccessError("Server's working directory is already in use "
                                  "(" +
                                      debug_info + ")",
                                  lockfile_path);
}


void sync::prepare_server_workdir(const config::Configuration& config, util::Logger& logger, Metrics&)
{
    REALM_ASSERT(config.root_dir);
    const std::string& root_dir = *config.root_dir;
    std::string realms_dir = util::File::resolve("user_data", root_dir); // Throws

    // Check whether the server used another encryption key in the previous session.
    verify_encryption_key_fingerprint(root_dir, config.encryption_key);

    // Migration of legacy files
    std::string migration_dir = util::File::resolve("migration", root_dir); // Throws
    _impl::ensure_legacy_migration_1(realms_dir, migration_dir, logger);    // Throws
}


auto sync::load_client_file_blacklists(const config::Configuration& config, util::Logger& logger)
    -> Server::ClientFileBlacklists
{
    // FIXME: Duplicate client file identifiers are not currently detected, but are also not harmful
    Server::ClientFileBlacklists lists;
    const std::string& root_dir = *config.root_dir;
    std::string path = util::File::resolve("client_file_blacklists", root_dir);
    bool found_file = false;
    std::size_t num_idents = 0;
    if (util::File::exists(path)) {
        try {
            util::File file{path};             // Throws
            ReadAheadBuffer read_ahead_buffer; // Throws
            std::string line;
            long line_number = 1;
            std::size_t col_ndx = 0;
            int num_errors_seen = 0;
            auto error = [&](const char* message) {
                logger.error("%1:%2:%3: %4", path, line_number, col_ndx, message); // Throws
                ++num_errors_seen;
                return (num_errors_seen >= 10);
            };
            std::istringstream in;
            in.imbue(std::locale::classic());
            in.unsetf(std::ios_base::skipws);
            std::string virt_path;
            Server::ClientFileBlacklist list;
            while (read_line(file, line, read_ahead_buffer)) { // Throws
                Tokenizer tokenizer{line};
                StringData token;
                if (!tokenizer.next(token, col_ndx))
                    goto next_line;
                if (REALM_UNLIKELY(token[0] != '/')) {
                    if (error("Bad virtual path")) // Throws
                        goto done_parsing;
                    goto next_line;
                }
                virt_path = token; // Throws
                while (tokenizer.next(token, col_ndx)) {
                    in.str(token); // Throws
                    in.clear();
                    file_ident_type client_file_ident = 0;
                    in >> client_file_ident;
                    if (REALM_UNLIKELY(!in || !in.eof())) {
                        if (error("Bad client file identifier")) // Throws
                            goto done_parsing;
                        continue;
                    }
                    list.push_back(client_file_ident); // Throws
                    ++num_idents;
                }
                if (!list.empty() && num_errors_seen == 0) {
                    Server::ClientFileBlacklist& list_2 = lists[virt_path]; // Throws
                    list_2.insert(list_2.end(), list.begin(), list.end());
                    list.clear();
                }
            next_line:
                ++line_number;
            }
        done_parsing:
            if (num_errors_seen > 0)
                throw std::runtime_error("Failed to parse 'client file blacklists' file");
            found_file = true;
        }
        catch (util::File::NotFound&) {
        }
    }
    if (found_file) {
        logger.info("Loaded %1 client file blacklists from '%2' (%3 client files in total)", lists.size(), path,
                    num_idents); // Throws
    }
    else {
        logger.info("No client file blacklists loaded ('%1' was not found)", path); // Throws
    }
    return lists;
}

} // namespace realm
