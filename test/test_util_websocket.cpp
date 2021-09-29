#include "test.hpp"

#include <realm/util/network.hpp>
#include <realm/util/websocket.hpp>

using namespace realm;
using namespace realm::util;

using WriteCompletionHandler = websocket::WriteCompletionHandler;
using ReadCompletionHandler = websocket::ReadCompletionHandler;

namespace {

// A class for connecting two socket endpoints through a memory buffer.
class Pipe {
public:
    Pipe(util::Logger& logger)
        : m_logger(logger)
    {
    }

    Pipe(const Pipe&) = delete;

    void async_write(const char* data, size_t size, WriteCompletionHandler handler)
    {
        m_logger.trace("async_write, size = %1", size);
        m_buffer.insert(m_buffer.end(), data, data + size);
        do_read();
        handler(std::error_code{}, size);
    }

    void async_read(char* buffer, size_t size, ReadCompletionHandler handler)
    {
        m_logger.trace("async_read, size = %1", size);
        REALM_ASSERT(!m_reader_waiting);
        m_reader_waiting = true;
        m_plain_async_read = true;
        m_read_buffer = buffer;
        m_read_size = size;
        m_handler = std::move(handler);
        do_read();
    }

    void async_read_until(char* buffer, size_t size, char delim, ReadCompletionHandler handler)
    {
        m_logger.trace("async_read_until, size = %1, delim = %2", size, delim);
        REALM_ASSERT(!m_reader_waiting);
        m_reader_waiting = true;
        m_plain_async_read = false;
        m_read_buffer = buffer;
        m_read_size = size;
        m_read_delim = delim;
        m_handler = std::move(handler);
        do_read();
    }


private:
    util::Logger& m_logger;
    std::vector<char> m_buffer;

    bool m_reader_waiting = false;

    // discriminates between the two async_read_* functions.
    // true is async_read()
    bool m_plain_async_read;
    char* m_read_buffer = nullptr;
    size_t m_read_size = 0;
    char m_read_delim;
    ReadCompletionHandler m_handler;

    void do_read()
    {
        m_logger.trace("do_read(), m_buffer.size = %1, m_reader_waiting = %2, m_read_size = %3", m_buffer.size(),
                       m_reader_waiting, m_read_size);
        if (!m_reader_waiting)
            return;

        if (m_plain_async_read) {
            if (m_buffer.size() >= m_read_size)
                transfer(m_read_size);
        }
        else {
            size_t min_size = std::min(m_buffer.size(), m_read_size);
            auto found_iter = std::find(m_buffer.begin(), m_buffer.begin() + min_size, m_read_delim);
            if (found_iter != m_buffer.begin() + min_size)
                transfer(found_iter - m_buffer.begin() + 1);
            else if (min_size == m_read_size)
                delim_not_found();
        }
    }

    void transfer(size_t size)
    {
        m_logger.trace("transfer()");
        std::copy(m_buffer.begin(), m_buffer.begin() + size, m_read_buffer);
        m_buffer.erase(m_buffer.begin(), m_buffer.begin() + size);
        m_reader_waiting = false;
        m_handler(std::error_code{}, size);
    }

    void delim_not_found()
    {
        m_logger.trace("delim_not_found");
        m_reader_waiting = false;
        m_handler(MiscExtErrors::delim_not_found, 0);
    }
};

class PipeTest {
public:
    util::Logger& logger;
    Pipe pipe;

    std::string result;
    bool done = false;
    bool error = false;

    PipeTest(util::Logger& logger)
        : logger(logger)
        , pipe(logger)
    {
    }

    void write(std::string input)
    {
        auto write_handler = [=](std::error_code, size_t) {};

        pipe.async_write(input.data(), input.size(), std::move(write_handler));
    }

    void read_plain(size_t size)
    {
        done = false;
        m_read_buffer.resize(size);

        auto handler = [this](std::error_code, size_t) {
            done = true;
            result = std::string{m_read_buffer.begin(), m_read_buffer.end()};
        };

        pipe.async_read(m_read_buffer.data(), size, std::move(handler));
    }

    void read_delim(size_t size, char delim)
    {
        done = false;
        m_read_buffer.resize(size);

        auto handler = [this](std::error_code ec, size_t actual_size) {
            if (!ec) {
                done = true;
                result = std::string{m_read_buffer.begin(), m_read_buffer.begin() + actual_size};
            }
            else {
                error = true;
            }
        };

        pipe.async_read_until(m_read_buffer.data(), size, delim, std::move(handler));
    }

private:
    std::vector<char> m_read_buffer;
};

class WSConfig : public websocket::Config {
public:
    int n_handshake_completed = 0;
    int n_protocol_errors = 0;
    int n_read_errors = 0;
    int n_write_errors = 0;

    std::vector<std::string> text_messages;
    std::vector<std::string> binary_messages;
    std::vector<std::pair<std::error_code, std::string>> close_messages;
    std::vector<std::string> ping_messages;
    std::vector<std::string> pong_messages;

    WSConfig(Pipe& pipe_in, Pipe& pipe_out, util::Logger& logger)
        : m_pipe_in(pipe_in)
        , m_pipe_out(pipe_out)
        , m_logger(logger)
    {
    }

    util::Logger& websocket_get_logger() noexcept override
    {
        return m_logger;
    }

    std::mt19937_64& websocket_get_random() noexcept override
    {
        return m_random;
    }

    void async_write(const char* data, size_t size, WriteCompletionHandler handler) override
    {
        m_pipe_out.async_write(data, size, handler);
    }

    void async_read(char* buffer, size_t size, ReadCompletionHandler handler) override
    {
        m_pipe_in.async_read(buffer, size, handler);
    }

    void async_read_until(char* buffer, size_t size, char delim, ReadCompletionHandler handler) override
    {
        m_pipe_in.async_read_until(buffer, size, delim, handler);
    }

    void websocket_handshake_completion_handler(const HTTPHeaders&) override
    {
        n_handshake_completed++;
    }

    void websocket_read_error_handler(std::error_code) override
    {
        n_read_errors++;
    }

    void websocket_write_error_handler(std::error_code) override
    {
        n_write_errors++;
    }

    void websocket_handshake_error_handler(std::error_code, const HTTPHeaders*, const std::string_view*) override
    {
        n_protocol_errors++;
    }

    void websocket_protocol_error_handler(std::error_code) override
    {
        n_protocol_errors++;
    }

    bool websocket_text_message_received(const char* data, size_t size) override
    {
        text_messages.push_back(std::string{data, size});
        return true;
    }

    bool websocket_binary_message_received(const char* data, size_t size) override
    {
        binary_messages.push_back(std::string{data, size});
        return true;
    }

    bool websocket_close_message_received(std::error_code error_code, StringData error_message) override
    {
        close_messages.push_back(std::make_pair(error_code, std::string{error_message}));
        return true;
    }

    bool websocket_ping_message_received(const char* data, size_t size) override
    {
        ping_messages.push_back(std::string{data, size});
        return true;
    }

    bool websocket_pong_message_received(const char* data, size_t size) override
    {
        pong_messages.push_back(std::string{data, size});
        return true;
    }


private:
    Pipe &m_pipe_in, &m_pipe_out;
    util::Logger& m_logger;
    std::mt19937_64 m_random;
};

class Fixture {
public:
    util::PrefixLogger m_prefix_logger_1, m_prefix_logger_2, m_prefix_logger_3, m_prefix_logger_4;
    Pipe pipe_1, pipe_2;
    WSConfig config_1, config_2;
    websocket::Socket socket_1, socket_2;

    Fixture(util::Logger& logger)
        : m_prefix_logger_1("Socket_1: ", logger)
        , m_prefix_logger_2("Socket_2: ", logger)
        , m_prefix_logger_3("Pipe_1: ", logger)
        , m_prefix_logger_4("Pipe_2: ", logger)
        , pipe_1(m_prefix_logger_3)
        , pipe_2(m_prefix_logger_4)
        , config_1(pipe_1, pipe_2, m_prefix_logger_1)
        , config_2(pipe_2, pipe_1, m_prefix_logger_2)
        , socket_1(config_1)
        , socket_2(config_2)
    {
    }
};
} // namespace


TEST(WebSocket_Pipe)
{
    {
        PipeTest pipe_test{test_context.logger};
        std::string input_1 = "Hello World";
        pipe_test.write(input_1);
        pipe_test.read_plain(input_1.size());
        CHECK(pipe_test.done);
        CHECK_EQUAL(pipe_test.result, input_1);
        std::string input_2 = "Hello again";
        pipe_test.write(input_2);
        pipe_test.read_plain(3);
        CHECK_EQUAL(pipe_test.result, "Hel");
        pipe_test.read_plain(4);
        CHECK_EQUAL(pipe_test.result, "lo a");
        pipe_test.read_plain(1);
        CHECK_EQUAL(pipe_test.result, "g");
        pipe_test.read_plain(4);
        CHECK(!pipe_test.done);
        pipe_test.write("q");
        CHECK(pipe_test.done);
        CHECK_EQUAL(pipe_test.result, "ainq");
        pipe_test.write("line_1\nline_2\n");
        pipe_test.read_delim(100, '\n');
        CHECK(pipe_test.done);
        CHECK_EQUAL(pipe_test.result, "line_1\n");
        pipe_test.read_delim(7, '\n');
        CHECK(pipe_test.done);
        CHECK_EQUAL(pipe_test.result, "line_2\n");
        pipe_test.read_delim(3, '\n');
        CHECK(!pipe_test.done);
        pipe_test.write("a");
        CHECK(!pipe_test.done);
        pipe_test.write("\n");
        CHECK(pipe_test.done);
        CHECK_EQUAL(pipe_test.result, "a\n");
        pipe_test.read_plain(2);
        CHECK(!pipe_test.done);
        pipe_test.write("qwerty");
        CHECK(pipe_test.done);
        CHECK_EQUAL(pipe_test.result, "qw");
        CHECK(!pipe_test.error);
        pipe_test.read_delim(4, '\n');
        CHECK(pipe_test.error);
    }
}


TEST(WebSocket_Messages)
{
    Fixture fixt{test_context.logger};
    WSConfig& config_1 = fixt.config_1;
    WSConfig& config_2 = fixt.config_2;

    websocket::Socket& socket_1 = fixt.socket_1;
    websocket::Socket& socket_2 = fixt.socket_2;

    CHECK_EQUAL(config_1.n_handshake_completed, 0);
    CHECK_EQUAL(config_2.n_handshake_completed, 0);

    socket_1.initiate_client_handshake("/uri", "host", "protocol");
    socket_2.initiate_server_handshake();

    CHECK_EQUAL(config_1.n_handshake_completed, 1);
    CHECK_EQUAL(config_2.n_handshake_completed, 1);

    CHECK_EQUAL(config_1.ping_messages.size(), 0);
    CHECK_EQUAL(config_2.ping_messages.size(), 0);

    auto handler_no_op = [=]() {};
    socket_1.async_write_ping("ping example", 12, handler_no_op);
    CHECK_EQUAL(config_1.ping_messages.size(), 0);
    CHECK_EQUAL(config_2.ping_messages.size(), 1);
    CHECK_EQUAL(config_2.ping_messages[0], "ping example");

    socket_1.async_write_pong("pong example", 12, handler_no_op);
    CHECK_EQUAL(config_1.pong_messages.size(), 0);
    CHECK_EQUAL(config_2.pong_messages.size(), 1);
    CHECK_EQUAL(config_2.pong_messages[0], "pong example");

    socket_1.async_write_text("short text example", 18, handler_no_op);
    CHECK_EQUAL(config_2.text_messages.size(), 1);
    CHECK_EQUAL(config_2.text_messages[0], "short text example");

    socket_1.async_write_ping("ping example 2", 14, handler_no_op);
    CHECK_EQUAL(config_2.ping_messages.size(), 2);
    CHECK_EQUAL(config_2.ping_messages[1], "ping example 2");

    socket_1.async_write_binary("short binary example", 20, handler_no_op);
    CHECK_EQUAL(config_2.binary_messages.size(), 1);
    CHECK_EQUAL(config_2.binary_messages[0], "short binary example");

    socket_2.async_write_close("\x03\xe8"
                               "close message",
                               15, handler_no_op);
    CHECK_EQUAL(config_1.close_messages.size(), 1);
    CHECK_EQUAL(config_1.close_messages[0].first.value(), 1000);
    CHECK_EQUAL(config_1.close_messages[0].second, "close message");

    std::vector<size_t> message_sizes{1, 2, 100, 125, 126, 127, 128, 200, 1000, 65000, 65535, 65536, 100000, 1000000};
    for (size_t i = 0; i < message_sizes.size(); ++i) {
        size_t size = message_sizes[i];
        std::vector<char> message(size, 'c');
        socket_2.async_write_binary(message.data(), size, handler_no_op);
        CHECK_EQUAL(config_1.binary_messages.size(), i + 1);
        std::string str{message.data(), size};
        CHECK_EQUAL(config_1.binary_messages[i], str);
    }
}

TEST(WebSocket_Fragmented_Messages)
{
    Fixture fixt{test_context.logger};
    WSConfig& config_1 = fixt.config_1;
    WSConfig& config_2 = fixt.config_2;

    websocket::Socket& socket_1 = fixt.socket_1;
    websocket::Socket& socket_2 = fixt.socket_2;

    CHECK_EQUAL(config_1.n_handshake_completed, 0);
    CHECK_EQUAL(config_2.n_handshake_completed, 0);

    socket_1.initiate_client_handshake("/uri", "host", "protocol");
    socket_2.initiate_server_handshake();

    CHECK_EQUAL(config_1.n_handshake_completed, 1);
    CHECK_EQUAL(config_2.n_handshake_completed, 1);

    auto handler_no_op = [=]() {};

    socket_1.async_write_frame(false, websocket::Opcode::binary, "abc", 3, handler_no_op);
    CHECK_EQUAL(config_2.binary_messages.size(), 0);
    socket_1.async_write_frame(true, websocket::Opcode::continuation, "defg", 4, handler_no_op);
    CHECK_EQUAL(config_2.binary_messages.size(), 1);
    CHECK_EQUAL(config_2.binary_messages[0], "abcdefg");

    socket_1.async_write_frame(false, websocket::Opcode::binary, "A", 1, handler_no_op);
    CHECK_EQUAL(config_2.binary_messages.size(), 1);
    socket_1.async_write_frame(false, websocket::Opcode::continuation, "B", 1, handler_no_op);
    CHECK_EQUAL(config_2.binary_messages.size(), 1);
    socket_1.async_write_frame(true, websocket::Opcode::continuation, "C", 1, handler_no_op);
    CHECK_EQUAL(config_2.binary_messages.size(), 2);
    CHECK_EQUAL(config_2.binary_messages[1], "ABC");
}

TEST(WebSocket_Interleaved_Fragmented_Messages)
{
    Fixture fixt{test_context.logger};
    WSConfig& config_1 = fixt.config_1;
    WSConfig& config_2 = fixt.config_2;

    websocket::Socket& socket_1 = fixt.socket_1;
    websocket::Socket& socket_2 = fixt.socket_2;

    CHECK_EQUAL(config_1.n_handshake_completed, 0);
    CHECK_EQUAL(config_2.n_handshake_completed, 0);

    socket_2.initiate_server_handshake();
    socket_1.initiate_client_handshake("/uri", "host", "protocol");

    CHECK_EQUAL(config_1.n_handshake_completed, 1);
    CHECK_EQUAL(config_2.n_handshake_completed, 1);

    auto handler_no_op = [=]() {};

    CHECK_EQUAL(config_2.ping_messages.size(), 0);
    socket_1.async_write_frame(false, websocket::Opcode::binary, "a", 1, handler_no_op);
    CHECK_EQUAL(config_2.binary_messages.size(), 0);
    socket_1.async_write_frame(false, websocket::Opcode::continuation, "b", 1, handler_no_op);
    CHECK_EQUAL(config_2.binary_messages.size(), 0);
    CHECK_EQUAL(config_2.ping_messages.size(), 0);
    socket_1.async_write_ping("ping", 4, handler_no_op);
    CHECK_EQUAL(config_2.binary_messages.size(), 0);
    CHECK_EQUAL(config_2.ping_messages.size(), 1);
    CHECK_EQUAL(config_2.ping_messages[0], "ping");
    socket_1.async_write_frame(false, websocket::Opcode::continuation, "c", 1, handler_no_op);
    CHECK_EQUAL(config_2.binary_messages.size(), 0);
    socket_1.async_write_frame(true, websocket::Opcode::continuation, "d", 1, handler_no_op);
    CHECK_EQUAL(config_2.binary_messages.size(), 1);
    CHECK_EQUAL(config_2.binary_messages[0], "abcd");
}
