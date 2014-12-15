#include "testsettings.hpp"
#ifdef TEST_UTIL_NETWORK

#include <algorithm>
#include <stdexcept>
#include <sstream>

#include <tightdb/util/bind.hpp>
#include <tightdb/util/unique_ptr.hpp>
#include <tightdb/util/memory_stream.hpp>
#include <tightdb/util/network.hpp>

#include "test.hpp"
#include "../test/util/thread_wrapper.hpp"

using std::size_t;
using std::string;
using std::runtime_error;
using std::ostringstream;
using namespace tightdb::util;
using namespace tightdb::test_util;


// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment varible `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.


namespace {

void sync_server(network::acceptor* acceptor, unit_test::TestResults* test_results_ptr)
{
    unit_test::TestResults& test_results = *test_results_ptr;

    network::socket socket;
    network::endpoint endpoint;
    acceptor->accept(socket, endpoint);

    network::buffered_input_stream input_stream(socket);
    const size_t max_header_size = 32;
    char header_buffer[max_header_size];
    size_t n = input_stream.read_until(header_buffer, max_header_size, '\n');
    if (!CHECK_GREATER(n, 0))
        return;
    if (!CHECK_LESS_EQUAL(n, max_header_size))
        return;
    if (!CHECK_EQUAL(header_buffer[n-1], '\n'))
        return;
    MemoryInputStream in;
    in.set_buffer(header_buffer, header_buffer+(n-1));
    in.unsetf(std::ios_base::skipws);
    string message_type;
    in >> message_type;
    if (!CHECK_EQUAL(message_type, "echo"))
        return;
    char sp;
    size_t body_size;
    in >> sp >> body_size;
    if (!CHECK(in) || !CHECK(in.eof()) || !CHECK_EQUAL(sp, ' '))
        return;
    UniquePtr<char[]> body_buffer(new char[body_size]);
    size_t m = input_stream.read(body_buffer.get(), body_size);
    if (!CHECK_EQUAL(m, body_size))
        return;
    MemoryOutputStream out;
    out.set_buffer(header_buffer, header_buffer+max_header_size);
    out << "was " << body_size << '\n';
    write(socket, header_buffer, out.size());
    write(socket, body_buffer.get(), body_size);
}


void sync_client(unsigned short listen_port, unit_test::TestResults* test_results_ptr)
{
    unit_test::TestResults& test_results = *test_results_ptr;

    network::socket socket;
    {
        ostringstream out;
        out << listen_port;
        string service = out.str();
        network::resolver resolver;
        network::resolver::query query("localhost", service);
        network::endpoint::list endpoints;
        resolver.resolve(query, endpoints);
        typedef network::endpoint::list::iterator iter;
        iter i = endpoints.begin();
        iter end = endpoints.end();
        for (;;) {
            error_code ec;
            socket.open(i->protocol(), ec);
            if (!ec) {
                socket.connect(*i, ec);
                if (!ec)
                    break;
                socket.close();
            }
            if (++i == end)
                throw runtime_error("Could not connect to server: All endpoints failed");
        }
    }

    const size_t body_size = 64;
    char body[body_size] = {
        '\xC1', '\x2C', '\xEF', '\x48', '\x8C', '\xCD', '\x41', '\xFA',
        '\x12', '\xF9', '\xF4', '\x72', '\xDF', '\x92', '\x8E', '\x68',
        '\xAB', '\x8F', '\x6B', '\xDF', '\x80', '\x26', '\xD1', '\x60',
        '\x21', '\x91', '\x20', '\xC8', '\x94', '\x0C', '\xDB', '\x07',
        '\xB0', '\x1C', '\x3A', '\xDA', '\x5E', '\x9B', '\x62', '\xDE',
        '\x30', '\xA3', '\x7E', '\xED', '\xB4', '\x30', '\xD7', '\x43',
        '\x3F', '\xDE', '\xF2', '\x6D', '\x9A', '\x1D', '\xAE', '\xF4',
        '\xD5', '\xFB', '\xAC', '\xE8', '\x67', '\x37', '\xFD', '\xF3'
    };

    const size_t max_header_size = 32;
    char header_buffer[max_header_size];
    MemoryOutputStream out;
    out.set_buffer(header_buffer, header_buffer+max_header_size);
    out << "echo " << body_size << '\n';
    write(socket, header_buffer, out.size());
    write(socket, body, body_size);

    network::buffered_input_stream input_stream(socket);
    size_t n = input_stream.read_until(header_buffer, max_header_size, '\n');
    if (!CHECK_GREATER(n, 0))
        return;
    if (!CHECK_LESS_EQUAL(n, max_header_size))
        return;
    if (!CHECK_EQUAL(header_buffer[n-1], '\n'))
        return;
    MemoryInputStream in;
    in.set_buffer(header_buffer, header_buffer+(n-1));
    in.unsetf(std::ios_base::skipws);
    string message_type;
    in >> message_type;
    if (!CHECK_EQUAL(message_type, "was"))
        return;
    char sp;
    size_t echo_size;
    in >> sp >> echo_size;
    if (!CHECK(in) || !CHECK(in.eof()) || !CHECK_EQUAL(sp, ' '))
        return;
    UniquePtr<char[]> echo_buffer(new char[echo_size]);
    size_t m = input_stream.read(echo_buffer.get(), echo_size);
    if (!CHECK_EQUAL(m, echo_size))
        return;
    if (!CHECK_EQUAL(echo_size, body_size))
        return;
    CHECK(std::equal(body, body+body_size, echo_buffer.get()));
}

} // anonymous namespace


TEST(Network_Sync)
{
    network::resolver resolver;
    network::acceptor acceptor;

    network::resolver::query query("localhost", "",
                                   network::resolver::query::passive |
                                   network::resolver::query::address_configured);
    network::endpoint::list endpoints;
    resolver.resolve(query, endpoints);

    typedef network::endpoint::list::iterator iter;
    iter i = endpoints.begin();
    iter end = endpoints.end();
    for (;;) {
        error_code ec;
        acceptor.open(i->protocol(), ec);
        if (!ec) {
            acceptor.bind(*i, ec);
            if (!ec)
                break;
            acceptor.close();
        }
        if (++i == end)
            throw runtime_error("Could not create a listening socket: All endpoints failed");
    }

    network::endpoint listen_endpoint = acceptor.local_endpoint();
    unsigned short listen_port = listen_endpoint.port();

    acceptor.listen();

    ThreadWrapper server_thread, client_thread;
    server_thread.start(bind(&sync_server, &acceptor, &test_results));
    client_thread.start(bind(&sync_client, listen_port, &test_results));
    client_thread.join();
    server_thread.join();
}

#endif // TEST_UTIL_NETWORK
