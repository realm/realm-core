#include <cstdlib>
#include <algorithm>
#include <iostream>

#include <realm/util/network.hpp>

#include "../util/timer.hpp"
#include "../util/random.hpp"
#include "../util/benchmark_results.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;


namespace {

network::endpoint bind_acceptor(network::acceptor& acceptor)
{
    network::io_service& service = acceptor.service();
    network::resolver resolver(service);
    network::resolver::query query("localhost",
                                   "", // Assign the port dynamically
                                   network::resolver::query::passive |
                                   network::resolver::query::address_configured);
    network::endpoint::list endpoints;
    resolver.resolve(query, endpoints);
    {
        auto i = endpoints.begin();
        auto end = endpoints.end();
        for (;;) {
            std::error_code ec;
            acceptor.bind(*i, ec);
            if (!ec)
                break;
            acceptor.close();
            if (++i == end)
                throw std::runtime_error("Failed to bind to localhost:*");
        }
    }
    return acceptor.local_endpoint();
}

void connect_sockets(network::socket& socket_1, network::socket& socket_2)
{
    network::io_service& service = socket_1.service();
    network::acceptor acceptor(service);
    network::endpoint ep = bind_acceptor(acceptor);
    acceptor.listen();
    std::error_code ec_1, ec_2;
    acceptor.async_accept(socket_1, [&](std::error_code ec) { ec_1 = ec; });
    socket_2.async_connect(ep, [&](std::error_code ec) { ec_2 = ec; });
    service.run();
    if (ec_1)
        throw std::system_error(ec_1);
    if (ec_2)
        throw std::system_error(ec_2);
}


class Post {
public:
    Post(size_t num)
    {
        m_num_posts = num;
    }

    void run()
    {
        initiate();
        m_service.run();
    }

private:
    network::io_service m_service;
    size_t m_num_posts;

    void initiate()
    {
        if (m_num_posts == 0)
            return;
        --m_num_posts;
        m_service.post([=]() { initiate(); });
    }
};


class Read {
public:
    Read(size_t size, size_t num):
        m_read_size(size)
    {
        if (size > sizeof m_read_buffer)
            throw std::runtime_error("Overflow");
        m_num_bytes_to_write = size;
        if (util::int_multiply_with_overflow_detect(m_num_bytes_to_write, num))
            throw std::runtime_error("Overflow");
        std::fill(m_write_buffer, m_write_buffer + sizeof m_write_buffer, 0);
        connect_sockets(m_read_socket, m_write_socket);
    }

    void run()
    {
        initiate_read();
        initiate_write();
        m_service.run();
    }

private:
    network::io_service m_service;
    network::socket m_read_socket{m_service}, m_write_socket{m_service};
    network::buffered_input_stream m_in{m_read_socket};
    char m_read_buffer[1000];
    char m_write_buffer[10000];
    const size_t m_read_size;
    size_t m_num_bytes_to_write;

    void initiate_read()
    {
        auto handler = [=](std::error_code ec, size_t) {
            if (ec && ec != network::end_of_input)
                throw std::system_error(ec);
            if (ec != network::end_of_input)
                initiate_read();
        };
        m_in.async_read(m_read_buffer, m_read_size, handler);
    }

    void initiate_write()
    {
        if (m_num_bytes_to_write == 0) {
            m_write_socket.close();
            return;
        }
        size_t n = std::min(sizeof m_write_buffer, m_num_bytes_to_write);
        m_num_bytes_to_write -= n;
        auto handler = [=](std::error_code ec, size_t) {
            if (ec)
                throw std::system_error(ec);
            initiate_write();
        };
        m_write_socket.async_write(m_write_buffer, n, handler);
    }
};


class Write {
public:
    Write(size_t size, size_t num):
        m_write_size(size),
        m_num_writes(num)
    {
        if (size > sizeof m_write_buffer)
            throw std::runtime_error("Overflow");
        std::fill(m_write_buffer, m_write_buffer + sizeof m_write_buffer, 0);
        connect_sockets(m_read_socket, m_write_socket);
    }

    void run()
    {
        initiate_read();
        initiate_write();
        m_service.run();
    }

private:
    network::io_service m_service;
    network::socket m_read_socket{m_service}, m_write_socket{m_service};
    network::buffered_input_stream m_in{m_read_socket};
    char m_read_buffer[10000];
    char m_write_buffer[1000];
    const size_t m_write_size;
    size_t m_num_writes;

    void initiate_read()
    {
        auto handler = [=](std::error_code ec, size_t) {
            if (ec && ec != network::end_of_input)
                throw std::system_error(ec);
            if (ec != network::end_of_input)
                initiate_read();
        };
        m_in.async_read(m_read_buffer, sizeof m_read_buffer, handler);
    }

    void initiate_write()
    {
        if (m_num_writes == 0) {
            m_write_socket.close();
            return;
        }
        --m_num_writes;
        auto handler = [=](std::error_code ec, size_t) {
            if (ec)
                throw std::system_error(ec);
            initiate_write();
        };
        m_write_socket.async_write(m_write_buffer, m_write_size, handler);
    }
};

} // unnamed namespace


int main()
{
    int max_lead_text_size = 12;
    BenchmarkResults results(max_lead_text_size);

    Timer timer(Timer::type_UserTime);
    {
        for (int i = 0; i != 100; ++i) {
            Post task(2200000); // (size, num)
            timer.reset();
            task.run();
            results.submit("post", timer);
        }
        results.finish("post", "Post");

        for (int i = 0; i != 100; ++i) {
            Read task(1, 11500000); // (size, num)
            timer.reset();
            task.run();
            results.submit("read_1", timer);
        }
        results.finish("read_1", "Read 1");

        for (int i = 0; i != 100; ++i) {
            Read task(10, 9000000); // (size, num)
            timer.reset();
            task.run();
            results.submit("read_10", timer);
        }
        results.finish("read_10", "Read 10");

        for (int i = 0; i != 100; ++i) {
            Read task(100, 2700000); // (size, num)
            timer.reset();
            task.run();
            results.submit("read_100", timer);
        }
        results.finish("read_100", "Read 100");

        for (int i = 0; i != 100; ++i) {
            Read task(1000, 350000); // (size, num)
            timer.reset();
            task.run();
            results.submit("read_1000", timer);
        }
        results.finish("read_1000", "Read 1000");


        for (int i = 0; i != 100; ++i) {
            Write task(1, 100000); // (size, num)
            timer.reset();
            task.run();
            results.submit("write_1", timer);
        }
        results.finish("write_1", "Write 1");

        for (int i = 0; i != 100; ++i) {
            Write task(10, 100000); // (size, num)
            timer.reset();
            task.run();
            results.submit("write_10", timer);
        }
        results.finish("write_10", "Write 10");

        for (int i = 0; i != 100; ++i) {
            Write task(100, 100000); // (size, num)
            timer.reset();
            task.run();
            results.submit("write_100", timer);
        }
        results.finish("write_100", "Write 100");

        for (int i = 0; i != 100; ++i) {
            Write task(1000, 100000); // (size, num)
            timer.reset();
            task.run();
            results.submit("write_1000", timer);
        }
        results.finish("write_1000", "Write 1000");
    }
}
