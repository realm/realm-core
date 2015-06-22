/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/
#ifndef REALM_UTIL_NETWORK_HPP
#define REALM_UTIL_NETWORK_HPP

#include <cstddef>
#include <string>
#include <ostream>

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <realm/util/features.h>
#include <realm/util/assert.hpp>
#include <realm/util/buffer.hpp>
#include <realm/util/basic_system_errors.hpp>

/// \file The design of this networking API is heavily inspired by the ASIO C++
/// library (http://think-async.com).

namespace realm {
namespace util {
namespace network {

std::string host_name();


class protocol;
class address;
class endpoint;
class io_service;
class resolver;
class socket;
class acceptor;
class buffered_input_stream;


class protocol {
public:
    static protocol ip_v4();
    static protocol ip_v6();

    bool is_ip_v4() const;
    bool is_ip_v6() const;

    protocol();
    ~protocol() REALM_NOEXCEPT {}

private:
    int m_family;
    int m_socktype;
    int m_protocol;

    friend class resolver;
    friend class socket_base;
};


class address {
public:
    bool is_ip_v4() const;
    bool is_ip_v6() const;

    template<class C, class T>
    friend std::basic_ostream<C,T>& operator<<(std::basic_ostream<C,T>&, const address&);

    address();
    ~address() REALM_NOEXCEPT {}

private:
    typedef in_addr  ip_v4_type;
    typedef in6_addr ip_v6_type;
    union union_type {
        ip_v4_type m_ip_v4;
        ip_v6_type m_ip_v6;
    };
    union_type m_union;
    bool m_is_ip_v6;

    friend class endpoint;
};


class endpoint {
public:
    using port_type = uint_fast16_t;
    class list;

    class protocol protocol() const;
    class address address() const;
    port_type port() const;

    endpoint();
    ~endpoint() REALM_NOEXCEPT {}

private:
    class protocol m_protocol;

    typedef sockaddr     sockaddr_base_type;
    typedef sockaddr_in  sockaddr_ip_v4_type;
    typedef sockaddr_in6 sockaddr_ip_v6_type;
    union sockaddr_union_type {
        sockaddr_base_type  m_base;
        sockaddr_ip_v4_type m_ip_v4;
        sockaddr_ip_v6_type m_ip_v6;
    };
    sockaddr_union_type m_sockaddr_union;

    friend class resolver;
    friend class socket_base;
    friend class socket;
    friend class acceptor;
};


class endpoint::list {
public:
    typedef const endpoint* iterator;

    iterator begin() const;
    iterator end() const;

    ~list() REALM_NOEXCEPT {}

private:
    Buffer<endpoint> m_endpoints;

    friend class resolver;
};


/// While a thread is executing run(), all objects with asynchronous operation
/// in progress (such as socket and acceptor) must be considered accessed by
/// that thread. This means that no other thread is allowed to access those
/// objects concurrently. An asynchronous operation is considered complete when
/// the completion handler is called by the thread that executes run() (i.e.,
/// before the completion handler returns).
class io_service {
public:
    io_service();
    ~io_service() REALM_NOEXCEPT;

    /// Wait for asynchronous operations to complete, and execute the associated
    /// completion handlers. Keep doing this until there are no more
    /// asynchronous operations in progress. If asynchronous operations such as
    /// acceptor::async_accept() or post() are used, one thread must call this
    /// function. At most one thread is allowed to execute it concurrently. All
    /// completion handlers associated with asynchronous operations, including
    /// handlers passed to post(), will be executed by the thread that calls
    /// run(), and only by that thread. Exceptions thrown by completion handlers
    /// will propagate back through run().
    void run();

    /// Stop any thread that is currently executing run(). If a thread is
    /// currently executing run() and is blocked, it will be unblocked. Handlers
    /// that can be executed immediately may or may not be executed before the
    /// thread returns from run(), but it is guaranteed that the thread will not
    /// become blocked again before returning. Subsequent calls to run() will
    /// return immediately until reset() is called.
    ///
    /// This function may be called by any thread, even via handlers executed by
    /// the thread that executes run().
    void stop();

    void reset();

    /// Post the specified handler for immediate asynchronous execution. The
    /// specified handler object will be copied as necessary, and will be
    /// executed by an expression on the form `handler()`.
    ///
    /// This function may be called by any thread, even via handlers executed by
    /// the thread that executes run().
    template<class H> void post(const H& handler);

private:
    class async_handler;
    template<class H> class post_handler;

    // Handler ownership is passed from caller to callee in calls to
    // add_io_handler(), add_imm_handler(), and add_post_handler().
    enum io_op { op_Read, op_Write };
    void add_io_handler(int fd, async_handler*, io_op);
    void add_imm_handler(async_handler*);
    void add_post_handler(async_handler*);
    void cancel_io_ops(int fd);

    template<class H> class write_handler;

    class impl;
    const std::unique_ptr<impl> m_impl;

    friend class socket_base;
    friend class acceptor;
    friend class buffered_input_stream;
    template<class H> friend void async_write(socket&, const char*, std::size_t, const H&);
};


class resolver {
public:
    class query;

    resolver(io_service&);
    ~resolver() REALM_NOEXCEPT {}

    io_service& service();

    void resolve(const query&, endpoint::list&);
    std::error_code resolve(const query&, endpoint::list&, std::error_code&);

private:
    io_service& m_service;
};


class resolver::query {
public:
    enum {
        ///< Locally bound socket endpoint (server side)
        passive = AI_PASSIVE,

        ///< Ignore families without a configured non-loopback address
        address_configured = AI_ADDRCONFIG
    };

    query(std::string service, int flags = passive|address_configured);
    query(const protocol&, std::string service, int flags = passive|address_configured);
    query(std::string host, std::string service, int flags = address_configured);
    query(const protocol&, std::string host, std::string service, int flags = address_configured);

    ~query() REALM_NOEXCEPT {}

    int flags() const;
    class protocol protocol() const;
    std::string host() const;
    std::string service() const;

private:
    int m_flags;
    class protocol m_protocol;
    std::string m_host;    // hostname
    std::string m_service; // port

    friend class resolver;
};


class socket_base {
public:
    ~socket_base() REALM_NOEXCEPT;

    io_service& service();

    bool is_open() const;

    void open(const protocol&);
    std::error_code open(const protocol&, std::error_code&);

    void close();
    std::error_code close(std::error_code&);

    template<class O> void get_option(O& option) const;
    template<class O> std::error_code get_option(O& option, std::error_code&) const;

    template<class O> void set_option(const O& option);
    template<class O> std::error_code set_option(const O& option, std::error_code&);

    void bind(const endpoint&);
    std::error_code bind(const endpoint&, std::error_code&);

    endpoint local_endpoint() const;
    endpoint local_endpoint(std::error_code&) const;

private:
    enum opt_enum {
        opt_ReuseAddr ///< `SOL_SOCKET`, `SO_REUSEADDR`
    };

    template<class, int, class> class option;

public:
    typedef option<bool, opt_ReuseAddr, int> reuse_address;

protected:
    io_service& m_service;
    protocol m_protocol;
    int m_sock_fd;

    socket_base(io_service&);

    void get_option(opt_enum, void* value_data, std::size_t& value_size, std::error_code&) const;
    void set_option(opt_enum, const void* value_data, std::size_t value_size, std::error_code&);
    void map_option(opt_enum, int& level, int& option_name) const;

    friend class acceptor;
    friend class buffered_input_stream;
    template<class H> friend void async_write(socket&, const char*, std::size_t, const H&);
};


template<class T, int opt, class U> class socket_base::option {
public:
    option(T value = T()):
        m_value(value)
    {
    }

    T value() const
    {
        return m_value;
    }

private:
    T m_value;

    void get(const socket_base& sock, std::error_code& ec)
    {
        union {
            U value;
            char strut[sizeof (U) + 1];
        };
        size_t value_size = sizeof strut;
        sock.get_option(opt_enum(opt), &value, value_size, ec);
        if (!ec) {
            REALM_ASSERT(value_size == sizeof value);
            m_value = T(value);
        }
    }

    void set(socket_base& sock, std::error_code& ec) const
    {
        U value = U(m_value);
        sock.set_option(opt_enum(opt), &value, sizeof value, ec);
    }

    friend class socket_base;
};


class socket: public socket_base {
public:
    socket(io_service&);
    ~socket() REALM_NOEXCEPT {}

    void connect(const endpoint&);
    std::error_code connect(const endpoint&, std::error_code&);

    std::size_t read_some(char* buffer, std::size_t size);
    std::size_t read_some(char* buffer, std::size_t size, std::error_code&) REALM_NOEXCEPT;

    std::size_t write_some(const char* data, std::size_t size);
    std::size_t write_some(const char* data, std::size_t size, std::error_code&) REALM_NOEXCEPT;
};


class acceptor: public socket_base {
public:
    acceptor(io_service&);
    ~acceptor() REALM_NOEXCEPT {}

    static const int max_connections = SOMAXCONN;

    void listen(int backlog = max_connections);
    std::error_code listen(int backlog, std::error_code&);

    void accept(socket&);
    void accept(socket&, endpoint&);
    std::error_code accept(socket&, std::error_code&);
    std::error_code accept(socket&, endpoint&, std::error_code&);

    template<class H> void async_accept(socket&, const H& handler);
    template<class H> void async_accept(socket&, endpoint&, const H& handler);

private:
    std::error_code accept(socket&, endpoint*, std::error_code&);
    template<class H> class accept_handler;
    template<class H> void async_accept(socket&, endpoint*, const H&);
};


class buffered_input_stream {
public:
    buffered_input_stream(socket&);
    ~buffered_input_stream() REALM_NOEXCEPT {}

    std::size_t read(char* buffer, std::size_t size);
    std::size_t read(char* buffer, std::size_t size, std::error_code&) REALM_NOEXCEPT;

    std::size_t read_until(char* buffer, std::size_t size, char delim);
    std::size_t read_until(char* buffer, std::size_t size, char delim,
                           std::error_code&) REALM_NOEXCEPT;

    template<class H>
    void async_read(char* buffer, std::size_t size, const H& handler);

    template<class H>
    void async_read_until(char* buffer, std::size_t size, char delim, const H& handler);

private:
    class read_handler_base;
    template<class H> class read_handler;

    socket& m_socket;
    static const std::size_t s_buffer_size = 1024;
    std::unique_ptr<char[]> m_buffer;
    char* m_begin;
    char* m_end;

    std::size_t read(char* buffer, std::size_t size, int delim, std::error_code&) REALM_NOEXCEPT;

    template<class H>
    void async_read(char* buffer, std::size_t size, int delim, const H& handler);
    void async_read(read_handler_base*);
};


std::error_code write(socket&, const char* data, std::size_t size, std::error_code&)
    REALM_NOEXCEPT;
void write(socket&, const char* data, std::size_t size);

template<class H> void async_write(socket&, const char* data, std::size_t size, const H& handler);


enum errors {
    /// End of input.
    end_of_input = 1,

    /// Delimiter not found.
    delim_not_found,

    /// Host not found (authoritative).
    host_not_found,

    /// Host not found (non-authoritative).
    host_not_found_try_again,

    /// The query is valid but does not have associated address data.
    no_data,

    /// A non-recoverable error occurred.
    no_recovery,

    /// The service is not supported for the given socket type.
    service_not_found,

    /// The socket type is not supported.
    socket_type_not_supported
};

std::error_code make_error_code(errors);

} // namespace network
} // namespace util
} // namespace realm

namespace std {

template<> struct is_error_code_enum<realm::util::network::errors> {
public:
    static const bool value = true;
};

} // namespace std

namespace realm {
namespace util {
namespace network {





// Implementation

inline protocol protocol::ip_v4()
{
    protocol prot;
    prot.m_family = AF_INET;
    return prot;
}

inline protocol protocol::ip_v6()
{
    protocol prot;
    prot.m_family = AF_INET6;
    return prot;
}

inline bool protocol::is_ip_v4() const
{
    return m_family == AF_INET;
}

inline bool protocol::is_ip_v6() const
{
    return m_family == AF_INET6;
}

inline protocol::protocol():
    m_family(AF_UNSPEC),     // Allow both IPv4 and IPv6
    m_socktype(SOCK_STREAM), // Or SOCK_DGRAM for UDP
    m_protocol(0)            // Any protocol
{
}

inline bool address::is_ip_v4() const
{
    return !m_is_ip_v6;
}

inline bool address::is_ip_v6() const
{
    return m_is_ip_v6;
}

template<class C, class T>
inline std::basic_ostream<C,T>& operator<<(std::basic_ostream<C,T>& out, const address& addr)
{
    union buffer_union {
        char ip_v4[INET_ADDRSTRLEN];
        char ip_v6[INET6_ADDRSTRLEN];
    };
    char buffer[sizeof (buffer_union)];
    int af = addr.m_is_ip_v6 ? AF_INET6 : AF_INET;
    const char* ret = inet_ntop(af, &addr.m_union, buffer, sizeof buffer);
    if (ret == 0) {
        std::error_code ec = make_basic_system_error_code(errno);
        throw std::system_error(ec);
    }
    out << ret;
    return out;
}

inline address::address():
    m_is_ip_v6(false)
{
    m_union.m_ip_v4 = ip_v4_type();
}

inline protocol endpoint::protocol() const
{
    return m_protocol;
}

inline address endpoint::address() const
{
    class address addr;
    if (m_protocol.is_ip_v4()) {
        addr.m_union.m_ip_v4 = m_sockaddr_union.m_ip_v4.sin_addr;
    }
    else {
        addr.m_union.m_ip_v6 = m_sockaddr_union.m_ip_v6.sin6_addr;
        addr.m_is_ip_v6 = true;
    }
    return addr;
}

inline endpoint::port_type endpoint::port() const
{
    return ntohs(m_protocol.is_ip_v4() ? m_sockaddr_union.m_ip_v4.sin_port :
                 m_sockaddr_union.m_ip_v6.sin6_port);
}

inline endpoint::endpoint():
    m_protocol(protocol::ip_v4())
{
    m_sockaddr_union.m_ip_v4 = sockaddr_ip_v4_type();
}

inline endpoint::list::iterator endpoint::list::begin() const
{
    return m_endpoints.data();
}

inline endpoint::list::iterator endpoint::list::end() const
{
    return m_endpoints.data() + m_endpoints.size();
}

class io_service::async_handler {
public:
    virtual bool exec() = 0;
    virtual void cancel() = 0;
    virtual ~async_handler() REALM_NOEXCEPT {}
};

template<class H> class io_service::post_handler:
        public async_handler {
public:
    post_handler(const H& h):
        m_handler(h)
    {
    }
    bool exec() override
    {
        m_handler(); // Throws
        return true;
    }
    void cancel() override
    {
        REALM_ASSERT(false);
    }
private:
    const H m_handler;
};

template<class H> inline void io_service::post(const H& handler)
{
    io_service::post_handler<H>* h = new io_service::post_handler<H>(handler); // Throws
    add_post_handler(h); // Throws
}

inline resolver::resolver(io_service& serv):
    m_service(serv)
{
}

inline io_service& resolver::service()
{
    return m_service;
}

inline void resolver::resolve(const query& q, endpoint::list& l)
{
    std::error_code ec;
    if (resolve(q, l, ec))
        throw std::system_error(ec);
}

inline resolver::query::query(std::string service, int flags):
    m_flags(flags),
    m_service(service)
{
}

inline resolver::query::query(const class protocol& prot, std::string service, int flags):
    m_flags(flags),
    m_protocol(prot),
    m_service(service)
{
}

inline resolver::query::query(std::string host, std::string service, int flags):
    m_flags(flags),
    m_host(host),
    m_service(service)
{
}

inline resolver::query::query(const class protocol& prot, std::string host, std::string service,
                              int flags):
    m_flags(flags),
    m_protocol(prot),
    m_host(host),
    m_service(service)
{
}

inline int resolver::query::flags() const
{
    return m_flags;
}

inline class protocol resolver::query::protocol() const
{
    return m_protocol;
}

inline std::string resolver::query::host() const
{
    return m_host;
}

inline std::string resolver::query::service() const
{
    return m_service;
}

inline socket_base::socket_base(io_service& service):
    m_service(service),
    m_sock_fd(-1)
{
}

inline socket_base::~socket_base() REALM_NOEXCEPT
{
    std::error_code ec;
    close(ec);
    // Ignore errors
}

inline io_service& socket_base::service()
{
    return m_service;
}

inline bool socket_base::is_open() const
{
    return m_sock_fd != -1;
}

inline void socket_base::open(const protocol& prot)
{
    std::error_code ec;
    if (open(prot, ec))
        throw std::system_error(ec);
}

inline void socket_base::close()
{
    std::error_code ec;
    if (close(ec))
        throw std::system_error(ec);
}

template<class O> inline void socket_base::get_option(O& option) const
{
    std::error_code ec;
    if (get_option(option, ec))
        throw std::system_error(ec);
}

template<class O>
inline std::error_code socket_base::get_option(O& option, std::error_code& ec) const
{
    option.get(*this, ec);
    return ec;
}

template<class O> inline void socket_base::set_option(const O& option)
{
    std::error_code ec;
    if (set_option(option, ec))
        throw std::system_error(ec);
}

template<class O>
inline std::error_code socket_base::set_option(const O& option, std::error_code& ec)
{
    option.set(*this, ec);
    return ec;
}

inline void socket_base::bind(const endpoint& ep)
{
    std::error_code ec;
    if (bind(ep, ec))
        throw std::system_error(ec);
}

inline endpoint socket_base::local_endpoint() const
{
    std::error_code ec;
    endpoint ep = local_endpoint(ec);
    if (ec)
        throw std::system_error(ec);
    return ep;
}

inline socket::socket(io_service& service):
    socket_base(service)
{
}

inline void socket::connect(const endpoint& ep)
{
    std::error_code ec;
    if (connect(ep, ec))
        throw std::system_error(ec);
}

inline std::size_t socket::read_some(char* buffer, std::size_t size)
{
    std::error_code ec;
    std::size_t n = read_some(buffer, size, ec);
    if (ec)
        throw std::system_error(ec);
    return n;
}

inline std::size_t socket::write_some(const char* data, std::size_t size)
{
    std::error_code ec;
    std::size_t n = write_some(data, size, ec);
    if (ec)
        throw std::system_error(ec);
    return n;
}

inline acceptor::acceptor(io_service& service):
    socket_base(service)
{
}

inline void acceptor::listen(int backlog)
{
    std::error_code ec;
    if (listen(backlog, ec))
        throw std::system_error(ec);
}

inline void acceptor::accept(socket& sock)
{
    std::error_code ec;
    if (accept(sock, ec))
        throw std::system_error(ec);
}

inline void acceptor::accept(socket& sock, endpoint& ep)
{
    std::error_code ec;
    if (accept(sock, ep, ec))
        throw std::system_error(ec);
}

inline std::error_code acceptor::accept(socket& sock, std::error_code& ec)
{
    endpoint* ep = nullptr;
    return accept(sock, ep, ec);
}

inline std::error_code acceptor::accept(socket& sock, endpoint& ep, std::error_code& ec)
{
    return accept(sock, &ep, ec);
}

template<class H> inline void acceptor::async_accept(socket& sock, const H& handler)
{
    endpoint* ep = nullptr;
    async_accept(sock, ep, handler);
}

template<class H> inline void acceptor::async_accept(socket& sock, endpoint& ep, const H& handler)
{
    async_accept(sock, &ep, handler);
}

template<class H> class acceptor::accept_handler:
        public io_service::async_handler {
public:
    accept_handler(acceptor& a, socket& s, endpoint* e, const H& h):
        m_acceptor(a),
        m_socket(s),
        m_endpoint(e),
        m_handler(h)
    {
    }
    bool exec() override
    {
        std::error_code ec;
        m_acceptor.accept(m_socket, m_endpoint, ec);
        m_handler(ec); // Throws
        return true;
    }
    void cancel() override
    {
        std::error_code ec = error::operation_aborted;
        m_handler(ec); // Throws
    }
private:
    acceptor& m_acceptor;
    socket& m_socket;
    endpoint* const m_endpoint;
    const H m_handler;
};

template<class H> inline void acceptor::async_accept(socket& sock, endpoint* ep, const H& handler)
{
    accept_handler<H>* h = new accept_handler<H>(*this, sock, ep, handler); // Throws
    m_service.add_io_handler(m_sock_fd, h, io_service::op_Read); // Throws
}

inline buffered_input_stream::buffered_input_stream(socket& sock):
    m_socket(sock),
    m_buffer(new char[s_buffer_size]), // Throws
    m_begin(m_buffer.get()),
    m_end(m_buffer.get())
{
}

inline std::size_t buffered_input_stream::read(char* buffer, std::size_t size)
{
    std::error_code ec;
    std::size_t n = read(buffer, size, ec);
    if (ec)
        throw std::system_error(ec);
    return n;
}

inline std::size_t buffered_input_stream::read(char* buffer, std::size_t size,
                                               std::error_code& ec) REALM_NOEXCEPT
{
    return read(buffer, size, std::char_traits<char>::eof(), ec);
}

inline std::size_t buffered_input_stream::read_until(char* buffer, std::size_t size, char delim)
{
    std::error_code ec;
    std::size_t n = read_until(buffer, size, delim, ec);
    if (ec)
        throw std::system_error(ec);
    return n;
}

inline std::size_t buffered_input_stream::read_until(char* buffer, std::size_t size, char delim,
                                                     std::error_code& ec) REALM_NOEXCEPT
{
    return read(buffer, size, std::char_traits<char>::to_int_type(delim), ec);
}

template<class H>
inline void buffered_input_stream::async_read(char* buffer, std::size_t size, const H& handler)
{
    async_read(buffer, size, std::char_traits<char>::eof(), handler);
}

template<class H>
inline void buffered_input_stream::async_read_until(char* buffer, std::size_t size, char delim,
                                                    const H& handler)
{
    async_read(buffer, size, std::char_traits<char>::to_int_type(delim), handler);
}

class buffered_input_stream::read_handler_base:
        public io_service::async_handler {
public:
    read_handler_base(buffered_input_stream& s, char* buffer, std::size_t size, int delim):
        m_stream(s),
        m_out_begin(buffer),
        m_out_end(buffer + size),
        m_out_curr(buffer),
        m_delim(delim),
        m_complete(false)
    {
    }
    void process_input() REALM_NOEXCEPT;
    bool is_complete() const
    {
        return m_complete;
    }
protected:
    buffered_input_stream& m_stream;
    char* const m_out_begin;
    char* const m_out_end;
    char* m_out_curr;
    const int m_delim;
    bool m_complete;
    void read_some(std::error_code& ec) REALM_NOEXCEPT;
};

template<class H>
class buffered_input_stream::read_handler:
        public read_handler_base {
public:
    read_handler(buffered_input_stream& s, char* buffer, std::size_t size, int delim, const H& h):
        read_handler_base(s, buffer, size, delim),
        m_handler(h)
    {
    }
    bool exec() override
    {
        std::error_code ec;
        if (!m_complete) {
            read_some(ec);
            if (!m_complete && !ec)
                return false;
        }
        std::size_t num_bytes_transferred = m_out_curr - m_out_begin;
        if (!ec && m_delim != std::char_traits<char>::eof()) {
            bool delim_found = num_bytes_transferred >= 1 &&
                m_out_curr[-1] == std::char_traits<char>::to_char_type(m_delim);
            if (!delim_found)
                ec = delim_not_found;
        }
        m_handler(ec, num_bytes_transferred); // Throws
        return true;
    }
    void cancel() override
    {
        std::error_code ec = error::operation_aborted;
        std::size_t num_bytes_transferred = m_out_curr - m_out_begin;
        m_handler(ec, num_bytes_transferred); // Throws
    }
private:
    const H m_handler;
};

template<class H>
inline void buffered_input_stream::async_read(char* buffer, std::size_t size, int delim,
                                              const H& handler)
{
    async_read(new read_handler<H>(*this, buffer, size, delim, handler)); // Throws
}

inline void buffered_input_stream::async_read(read_handler_base* handler)
{
    handler->process_input();
    if (handler->is_complete()) {
        m_socket.m_service.add_imm_handler(handler); // Throws
    }
    else {
        m_socket.m_service.add_io_handler(m_socket.m_sock_fd, handler,
                                          io_service::op_Read); // Throws
    }
}

inline void write(socket& sock, const char* data, std::size_t size)
{
    std::error_code ec;
    if (write(sock, data, size, ec))
        throw std::system_error(ec);
}

template<class H> class io_service::write_handler: public async_handler {
public:
    write_handler(socket& s, const char* data, std::size_t size, const H& h):
        m_socket(s),
        m_begin(data),
        m_end(data + size),
        m_curr(data),
        m_handler(h)
    {
    }
    bool exec() override
    {
        std::error_code ec;
        std::size_t n = m_socket.write_some(m_curr, m_end-m_curr, ec);
        m_curr += n;
        bool complete = m_curr == m_end;
        if (!complete && !ec)
            return false;
        std::size_t num_bytes_transferred = m_curr - m_begin;
        m_handler(ec, num_bytes_transferred); // Throws
        return true;
    }
    void cancel() override
    {
        std::error_code ec = error::operation_aborted;
        std::size_t num_bytes_transferred = m_curr - m_begin;
        m_handler(ec, num_bytes_transferred); // Throws
    }
private:
    socket& m_socket;
    const char* const m_begin;
    const char* const m_end;
    const char* m_curr;
    const H m_handler;
};

template<class H>
inline void async_write(socket& sock, const char* data, std::size_t size, const H& handler)
{
    io_service::write_handler<H>* h =
        new io_service::write_handler<H>(sock, data, size, handler); // Throws
    sock.service().add_io_handler(sock.m_sock_fd, h, io_service::op_Write); // Throws
}


} // namespace network
} // namespace util
} // namespace realm

#endif // REALM_UTIL_NETWORK_HPP
