#include <stdint.h>
#include <cstring>
#include <algorithm>
#include <stdexcept>
#include <vector>
#include <queue>
#include <set>
#include <map>
#include <string>
#include <sstream>

#include <tightdb/util/bind.hpp>
#include <tightdb/util/unique_ptr.hpp>
#include <tightdb/util/memory_stream.hpp>
#include <tightdb/util/logger.hpp>
#include <tightdb/util/network.hpp>
#include <tightdb/binary_data.hpp>


using namespace std;
using namespace tightdb;


namespace {

typedef unsigned long file_ident_type;
typedef uint_fast64_t version_type;

class connection;


class file {
public:
    file()
    {
        m_earliest_version = 1;
    }

    ~file()
    {
        typedef transact_logs::const_iterator iter;
        const iter end = m_transact_logs.end();
        for (iter i = m_transact_logs.begin(); i != end; ++i)
            delete[] i->data();
    }

    void bind(connection* conn, file_ident_type client_file_ident)
    {
        TIGHTDB_ASSERT(m_client_files.find(make_pair(conn, client_file_ident)) ==
                       m_client_files.end());
        m_client_files.insert(make_pair(conn, client_file_ident));

        // FIXME: Must initiate upload of all newer transaction logs known to the server.
    }

    void unbind(connection* conn, file_ident_type client_file_ident)
    {
        TIGHTDB_ASSERT(m_client_files.find(make_pair(conn, client_file_ident)) !=
                       m_client_files.end());
        m_client_files.erase(make_pair(conn, client_file_ident));
    }

    version_type get_latest_version()
    {
        return m_earliest_version + m_transact_logs.size();
    }

    /// Get the transaction log that takes us from \a version - 1 to \a version.
    BinaryData get_transact_log(version_type version)
    {
        return m_transact_logs.at(version - m_earliest_version - 1);
    }

    void add_transact_log(BinaryData);

private:
    typedef pair<connection*, file_ident_type> client_file;
    typedef set<client_file> client_files;
    client_files m_client_files;

    typedef vector<BinaryData> transact_logs;
    transact_logs m_transact_logs;
    version_type m_earliest_version;
};


class server: private util::Logger {
public:
    util::Logger* const root_logger;
    bool log_everything;

    server(util::Logger* root_logger, bool log_everything);
    ~server();

    void start(string listen_address, string listen_port, bool reuse_address = false);

    util::network::endpoint listen_endpoint() const
    {
        return m_acceptor.local_endpoint();
    }

    void run()
    {
        m_service.run();
    }

    void remove_connection(connection*);

    file* get_file(const string& path)
    {
        typedef files::iterator iter;
        iter i = m_files.insert(pair<string, file*>(path, 0)).first; // Throws
        if (!i->second)
            i->second = new file; // Throws
        return i->second;
    }

private:
    const string m_listen_address;
    const string m_listen_port;
    util::network::io_service m_service;
    util::network::acceptor m_acceptor;
    unsigned short m_assigned_listen_port;
    int_fast64_t m_next_conn_id;
    util::UniquePtr<connection> m_next_conn;
    util::network::endpoint m_next_conn_endpoint;
    typedef set<connection*> connections;
    connections m_connections;
    typedef map<string, file*> files;
    files m_files;

    void initiate_accept();
    void handle_accept(util::error_code);

    void do_log(const std::string& msg) TIGHTDB_OVERRIDE
    {
        if (root_logger)
            Logger::do_log(root_logger, msg);
    }
};


class connection: private util::Logger {
public:
    connection(server& serv, int_fast64_t id, util::network::io_service& service):
        m_server(serv),
        m_id(id),
        m_socket(service),
        m_input_stream(m_socket)
    {
        ostringstream out;
        out << "Connection["<<id<<"]: ";
        m_log_prefix = out.str();
    }

    void start(const util::network::endpoint& ep)
    {
        log("Connection from %2:%3", m_id, ep.address(), ep.port());
        initiate_read_head();
    }

    void resume_transact_log_send(file_ident_type client_file_ident)
    {
        typedef client_files::iterator iter;
        iter i = m_client_files.find(client_file_ident);
        TIGHTDB_ASSERT(i != m_client_files.end());
        client_file& cf = i->second;
        resume_transact_log_send(client_file_ident, cf);
    }

private:
    server& m_server;
    const int_fast64_t m_id;
    util::network::socket m_socket;
    util::network::buffered_input_stream m_input_stream;
    static const size_t s_max_head_size = 32;
    char m_input_head_buffer[s_max_head_size];
    util::UniquePtr<char[]> m_input_body_buffer;
    string m_log_prefix;
    struct client_file {
        file* server_file;
        version_type client_version;
    };
    typedef map<file_ident_type, client_file> client_files;
    client_files m_client_files;

    struct output_chunk {
        const char* data;
        size_t size;
        bool owned;
        output_chunk(const char* d, size_t s, bool o):
            data(d),
            size(s),
            owned(o)
        {
        }
    };

    queue<output_chunk> m_output_chunks;

    void initiate_read_head()
    {
        m_input_stream.async_read_until(m_input_head_buffer, s_max_head_size, '\n',
                                        bind(&connection::handle_read_head, this));
    }

    void handle_read_head(util::error_code& ec, size_t n)
    {
        if (ec) {
            if (ec != util::error::operation_aborted)
                read_error(ec);
            return;
        }
        util::MemoryInputStream in;
        TIGHTDB_ASSERT(n >= 1);
        in.set_buffer(m_input_head_buffer, m_input_head_buffer+(n-1));
        in.unsetf(std::ios_base::skipws);
        string message_type;
        in >> message_type;
        if (message_type == "transact") {
            file_ident_type client_file_ident;
            version_type version;
            size_t log_size;
            char sp_1, sp_2, sp_3;
            in >> sp_1 >> client_file_ident >> sp_2 >> version >> sp_3 >> log_size;
            if (!in || !in.eof() || sp_1 != ' ' || sp_2 != ' ' || sp_3 != ' ') {
                log("ERROR: Bad 'transact' message");
                close();
                return;
            }
            m_input_body_buffer.reset(new char[log_size]); // Throws
            m_input_stream.async_read(m_input_body_buffer.get(), log_size,
                                      bind(&connection::handle_read_transact_log,
                                           this, client_file_ident, version));
            return;
        }
        else if (message_type == "bind") {
            file_ident_type client_file_ident;
            version_type client_version;
            size_t path_size;
            char sp_1, sp_2, sp_3;
            in >> sp_1 >> client_file_ident >> sp_2 >> client_version >> sp_3 >> path_size;
            if (!in || !in.eof() || sp_1 != ' ' || sp_2 != ' ' || sp_3 != ' ') {
                log("ERROR: Bad 'bind' message");
                close();
                return;
            }
            m_input_body_buffer.reset(new char[path_size]); // Throws
            m_input_stream.async_read(m_input_body_buffer.get(), path_size,
                                      bind(&connection::handle_read_bind_path,
                                           this, client_file_ident, client_version));
            return;
        }
        else if (message_type == "unbind") {
            file_ident_type client_file_ident;
            char sp;
            in >> sp >> client_file_ident;
            if (!in || !in.eof() || sp != ' ') {
                log("ERROR: Bad 'unbind' message");
                close();
                return;
            }
            typedef client_files::iterator iter;
            iter i = m_client_files.find(client_file_ident);
            if (i == m_client_files.end()) {
                log("ERROR: Bad client file identifier %1", client_file_ident);
                close();
                return;
            }
            const client_file& cf = i->second;
            cf.server_file->unbind(this, client_file_ident);
            m_client_files.erase(i);
            initiate_read_head();
            return;
        }
        log("ERROR: Message of unknown type '%1'", message_type);
        close();
    }

    void handle_read_transact_log(file_ident_type client_file_ident,
                                  version_type client_version,
                                  util::error_code& ec, size_t n)
    {
        if (ec) {
            if (ec != util::error::operation_aborted)
                read_error(ec);
            return;
        }

        if (m_server.log_everything)
            log("Received: Transaction log %1 -> %2 from client file #%3",
                client_version-1, client_version, client_file_ident);

        util::UniquePtr<char[]> transact_log_owner(m_input_body_buffer.release());
        BinaryData transact_log(transact_log_owner.get(), n);

        typedef client_files::iterator iter;
        iter i = m_client_files.find(client_file_ident);
        if (i == m_client_files.end()) {
            log("ERROR: Bad client file identifier %1", client_file_ident);
            close();
            return;
        }

        client_file& cf = i->second;
        version_type last_server_version = cf.server_file->get_latest_version();
//        log("last_server_version = %1", last_server_version);
        version_type next_server_version = last_server_version + 1;
        if (client_version < 2 || client_version > next_server_version) {
            log("ERROR: Invalid client version %1", client_version);
            close();
            return;
        }
        if (client_version == next_server_version) {
            TIGHTDB_ASSERT(client_version == cf.client_version + 1);
            cf.client_version = client_version;
            cf.server_file->add_transact_log(transact_log); // Throws
            transact_log_owner.release();

            if (m_server.log_everything)
                log("Sending: Accepting transaction %1 -> %2 from client file #%3",
                    client_version-1, client_version, client_file_ident);
            ostringstream out;
            out << "accept "<<client_file_ident<<" "<<client_version<<"\n";
            string head = out.str();
            enqueue_output_message(head);
        }
        else {
            // WARNING: Strictly speaking, the following is not the correct
            // resulution of the conflict between two identical initial
            // transactions, but it is done as a temporary workaround to allow
            // the current version of the Cocoa binding to carry out an initial
            // schema creating transaction without getting into an immediate
            // unrecoverable conflict. It does not work in general as even the
            // initial transaction is allowed to contain elements that are
            // additive rather than idempotent.
            BinaryData servers_log = cf.server_file->get_transact_log(client_version);
            if (client_version > 2 || transact_log != servers_log) {
                log("ERROR: Conflict (%1 vs %2)", transact_log.size(), servers_log.size());
/*
                ofstream out_1("/tmp/conflict.1");
                out_1.write(servers_log.data(), servers_log.size());
                ofstream out_2("/tmp/conflict.2");
                out_2.write(transact_log.data(), transact_log.size());
*/
                close();
                return;
            }
            if (cf.client_version < client_version)
                cf.client_version = client_version;
            log("Conflict resolved %1 -> %2 improperly (identical initial transactions)",
                client_version-1, client_version);
        }

        initiate_read_head();
    }

    void handle_read_bind_path(file_ident_type client_file_ident, version_type client_version,
                               util::error_code& ec, size_t n)
    {
        if (ec) {
            if (ec != util::error::operation_aborted)
                read_error(ec);
            return;
        }
        string path(m_input_body_buffer.get(), m_input_body_buffer.get()+n); // Throws
        m_input_body_buffer.reset();

        log("Received: Bind client file #%1 to '%2'", client_file_ident, path);

        client_file cf;
        cf.server_file = m_server.get_file(path);
        cf.client_version = client_version;
        typedef client_files::iterator iter;
        pair<iter, bool> p = m_client_files.insert(make_pair(client_file_ident, cf));
        if (!p.second) {
            log("ERROR: Rebind attempted");
            close();
            return;
        }
        cf.server_file->bind(this, client_file_ident); // Throws

        resume_transact_log_send(client_file_ident, p.first->second);

        initiate_read_head();
    }

    void resume_transact_log_send(file_ident_type client_file_ident, client_file& cf)
    {
        // FIXME: What is done here, is bad in almost every possible way. The
        // root problem is that it goes ahead immediately and generates output
        // messages for all outstanding transaction logs. This potentially
        // allocates **way** too much memory. This becomes even worse due to the
        // fact that most of the queued output messages will be wasted if the
        // file binding is broken shortly after it is established. It is
        // necessary to find a way to generate at most one transaction output
        // message at a time.
        version_type latest_server_version = cf.server_file->get_latest_version();
        while (cf.client_version < latest_server_version) {
            version_type next_client_version = cf.client_version + 1;
            if (m_server.log_everything)
                log("Sending: Transaction %1 -> %2 to client file #%3",
                    next_client_version-1, next_client_version, client_file_ident);
            BinaryData log = cf.server_file->get_transact_log(next_client_version);
            ostringstream out;
            out << "transact "<<client_file_ident<<" "<<next_client_version<<" "<<log.size()<<"\n";
            string head = out.str();
            enqueue_output_message(head, log);
            cf.client_version = next_client_version;
        }
    }

    void enqueue_output_message(const string& head, BinaryData body = BinaryData())
    {
        bool resume = m_output_chunks.empty();
        util::UniquePtr<char[]> head_2(new char[head.size()]); // Throws
        copy(head.begin(), head.end(), head_2.get());
        bool owned = true;
        m_output_chunks.push(output_chunk(head_2.get(), head.size(), owned)); // Throws
        head_2.release();
        // FIXME: Must remove head from queue if push of body throws, but that
        // is impossible with std::queue. Also, std::queue has no reserve()
        // method.
        if (body) {
            owned = false;
            m_output_chunks.push(output_chunk(body.data(), body.size(), owned)); // Throws
        }
        if (resume)
            resume_output();
    }

    void resume_output()
    {
        const output_chunk& chunk = m_output_chunks.front();
        async_write(m_socket, chunk.data, chunk.size, bind(&connection::handle_write, this));
    }

    void handle_write(util::error_code& ec, size_t n)
    {
        if (ec) {
            if (ec != util::error::operation_aborted)
                write_error(ec);
            return;
        }
        TIGHTDB_ASSERT(!m_output_chunks.empty());
        const output_chunk& chunk = m_output_chunks.front();
        TIGHTDB_ASSERT(n == chunk.size);
        if (chunk.owned)
            delete[] chunk.data;
        m_output_chunks.pop();

        if (!m_output_chunks.empty())
            resume_output();
    }

    void read_error(util::error_code ec)
    {
        log("ERROR: Reading failed: %1", ec.message());
        close();
    }

    void write_error(util::error_code ec)
    {
        log("ERROR: Writing failed: %1", ec.message());
        close();
    }

    void close()
    {
        m_socket.close();
        m_server.remove_connection(this);

        typedef client_files::const_iterator iter;
        const iter end = m_client_files.end();
        for (iter i = m_client_files.begin(); i != end; ++i) {
            file_ident_type client_file_ident = i->first;
            client_file cf = i->second;
            cf.server_file->unbind(this, client_file_ident);
        }

        log("Connection closed due to error");
        delete this;
    }

    void do_log(const std::string& msg) TIGHTDB_OVERRIDE
    {
        if (m_server.root_logger)
            Logger::do_log(m_server.root_logger, m_log_prefix + msg);
    }

    friend class server;
};


void file::add_transact_log(BinaryData log)
{
    m_transact_logs.push_back(log); // Throws

    typedef client_files::const_iterator iter;
    const iter end = m_client_files.end();
    for (iter i = m_client_files.begin(); i != end; ++i) {
        connection* conn = i->first;
        file_ident_type client_file_ident = i->second;
        conn->resume_transact_log_send(client_file_ident);
    }
}


server::server(util::Logger* root_logger_2, bool log_everything_2):
    root_logger(root_logger_2),
    log_everything(log_everything_2),
    m_acceptor(m_service),
    m_next_conn_id(0)
{
}


server::~server()
{
    typedef connections::const_iterator iter;
    iter end = m_connections.end();
    for (iter i = m_connections.begin(); i != end; ++i)
        delete *i;
}


void server::start(string listen_address, string listen_port, bool reuse_address)
{
    util::network::resolver resolver(m_service);
    util::network::resolver::query query(listen_address, listen_port,
                                         util::network::resolver::query::passive |
                                         util::network::resolver::query::address_configured);
    util::network::endpoint::list endpoints;
    resolver.resolve(query, endpoints);

    typedef util::network::endpoint::list::iterator iter;
    iter i = endpoints.begin();
    iter end = endpoints.end();
    for (;;) {
        util::error_code ec;
        m_acceptor.open(i->protocol(), ec);
        if (!ec) {
            if (reuse_address)
                m_acceptor.set_option(util::network::socket_base::reuse_address(true), ec);
            if (!ec) {
                m_acceptor.bind(*i, ec);
                if (!ec)
                    break;
            }
            m_acceptor.close();
        }
        if (++i == end)
            throw runtime_error("Could not create a listening socket: All endpoints failed");
    }

    m_acceptor.listen();

    util::network::endpoint local_endpoint = m_acceptor.local_endpoint();
    log("Listening on %1:%2", local_endpoint.address(), local_endpoint.port());

    initiate_accept();
}


void server::initiate_accept()
{
    m_next_conn.reset(new connection(*this, ++m_next_conn_id, m_service));
    m_acceptor.async_accept(m_next_conn->m_socket, m_next_conn_endpoint,
                            util::bind(&server::handle_accept, this));
}


void server::handle_accept(util::error_code ec)
{
    if (ec)
        throw util::system_error(ec);
    connection* conn = m_next_conn.get();
    m_connections.insert(conn); // Throws
    m_next_conn.release();
    conn->start(m_next_conn_endpoint);
    initiate_accept();
}

void server::remove_connection(connection* conn)
{
    m_connections.erase(conn);
}

} // anonymous namespace





int main(int argc, char* argv[])
{
    string listen_address = util::network::host_name();
    string listen_port = "7800";
    bool reuse_address = false;
    int log_level = 1;

    // Process command line
    {
        const char* prog = argv[0];
        --argc;
        ++argv;
        bool error = false;
        bool help  = false;
        int argc2 = 0;
        for (int i=0; i<argc; ++i) {
            char* arg = argv[i];
            if (arg[0] != '-') {
                argv[argc2++] = arg;
                continue;
            }
            if (strcmp(arg, "-h") == 0 || strcmp(arg, "--help") == 0) {
                help = true;
                continue;
            }
            else if (strcmp(arg, "-p") == 0 || strcmp(arg, "--listen-port") == 0) {
                if (i+1 < argc) {
                    listen_port = argv[++i];
                    continue;
                }
            }
            else if (strcmp(arg, "-r") == 0 || strcmp(arg, "--reuse-address") == 0) {
                reuse_address = true;
                continue;
            }
            else if (strcmp(arg, "-l") == 0 || strcmp(arg, "--log-level") == 0) {
                if (i+1 < argc) {
                    istringstream in(argv[++i]);
                    in.unsetf(std::ios_base::skipws);
                    int v = 0;
                    in >> v;
                    if (in && in.eof() && v >= 0 && v <= 2) {
                        log_level = v;
                        continue;
                    }
                }
            }
            error = true;
            break;
        }
        argc = argc2;

        if (argc > 0)
            listen_address = argv[0];
        if (argc > 1)
            error = true;

        if (help) {
            cerr <<
                "Synopsis: "<<prog<<"  [ADDRESS]\n"
                "\n"
                "Options:\n"
                "  -h, --help           Display command-line synopsis followed by the list of\n"
                "                       available options.\n"
                "  -p, --listen-port    The listening port. (default '"<<listen_port<<"')\n"
                "  -r, --reuse-address  Allow immediate reuse of listening port (unsafe).\n"
                "  -l, --log-level      Set log level (0 for nothing, 1 for normal, 2 for\n"
                "                       everything).\n";
            return 0;
        }

        if (error) {
            cerr <<
                "ERROR: Bad command line.\n"
                "Try `"<<prog<<" --help`\n";
            return 1;
        }
    }

    util::UniquePtr<util::Logger> logger;
    bool enable_logging = (log_level > 0);
    if (enable_logging)
        logger.reset(new util::Logger);

    bool log_everything = (log_level > 1);
    server serv(logger.get(), log_everything);
    serv.start(listen_address, listen_port, reuse_address);
    serv.run();
}
