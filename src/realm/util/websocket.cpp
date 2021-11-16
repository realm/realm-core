#include <cctype>

#include <realm/util/websocket.hpp>
#include <realm/util/buffer.hpp>
#include <realm/util/network.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/sha_crypto.hpp>

using namespace realm;
using namespace util;
using Error = websocket::Error;


namespace {

// case_insensitive_equal is used to compare, ignoring case, certain HTTP
// header values with their expected values.
bool case_insensitive_equal(StringData str1, StringData str2)
{
    if (str1.size() != str2.size())
        return false;

    for (size_t i = 0; i < str1.size(); ++i)
        if (std::tolower(str1[i], std::locale::classic()) != std::tolower(str2[i], std::locale::classic()))
            return false;

    return true;
}

// The WebSocket version is always 13 according to the standard.
const StringData sec_websocket_version = "13";

// The magic string is specified in the WebSocket protocol. It is used in the handshake.
const StringData websocket_magic_string = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

// The Sec-WebSocket-Key is a header in the client HTTP request.
// It is the base64 encoding of 16 random bytes.
std::string make_random_sec_websocket_key(std::mt19937_64& random)
{
    char random_bytes[16];
    std::uniform_int_distribution<> dis(std::numeric_limits<char>::min(), std::numeric_limits<char>::max());
    for (int i = 0; i < 16; ++i) {
        random_bytes[i] = dis(random);
    }

    char out_buffer[24];
    size_t encoded_size = base64_encode(random_bytes, 16, out_buffer, 24);
    REALM_ASSERT(encoded_size == 24);

    return std::string{out_buffer, 24};
}

// Sec-WebSocket-Accept is a header in the server's HTTP response.
// It is calculated from the Sec-WebSocket-Key in the client's HHTP request
// as the base64 encoding of the sha1 of the concatenation of the
// Sec-Websocket-Key and the magic string.
std::string make_sec_websocket_accept(StringData sec_websocket_key)
{
    std::string sha1_input;
    sha1_input.reserve(sec_websocket_key.size() + websocket_magic_string.size());
    sha1_input.append(sec_websocket_key.data(), sec_websocket_key.size());
    sha1_input.append(websocket_magic_string.data(), websocket_magic_string.size());

    unsigned char sha1_output[20];
    util::sha1(sha1_input.data(), sha1_input.length(), sha1_output);

    char base64_output[28];
    size_t base64_output_size = base64_encode(reinterpret_cast<char*>(sha1_output), 20, base64_output, 28);
    REALM_ASSERT(base64_output_size == 28);

    return std::string(base64_output, 28);
}

// find_http_header_value() returns the value of the header \param header in the
// HTTP message \param message if present. If the header is absent, the function returns
// None.
util::Optional<StringData> find_http_header_value(const HTTPHeaders& headers, StringData header)
{
    auto it = headers.find(header);

    if (it != headers.end()) {
        return StringData(it->second);
    }
    return none;
}

// validate_websocket_upgrade() returns true if the HTTP \a headers
// contain the line
// Upgrade: websocket
bool validate_websocket_upgrade(const HTTPHeaders& headers)
{
    util::Optional<StringData> header_value_upgrade = find_http_header_value(headers, "Upgrade");

    return (header_value_upgrade && case_insensitive_equal(*header_value_upgrade, "websocket"));
}

// validate_websocket_connection() returns true if the HTTP \a headers
// contains the line:
// Connection: Upgrade
bool validate_websocket_connection(const HTTPHeaders& headers)
{
    util::Optional<StringData> header_value_connection = find_http_header_value(headers, "Connection");

    return (header_value_connection && case_insensitive_equal(*header_value_connection, "Upgrade"));
}

// validate_sec_websocket_version() returns true if the
// \param http_message contains a header with the
// correct sec_websocket_version.
bool validate_sec_websocket_version(const HTTPHeaders& headers)
{
    return find_http_header_value(headers, "Sec-WebSocket-Version") == sec_websocket_version;
}

// find_sec_websocket_key() returns true if the
// \param headers contains a Sec-Websocket-Key
// header, false otherwise. If the header is found, the
// argument sec_websocket_key is set to its value.
bool find_sec_websocket_key(const HTTPHeaders& headers, std::string& sec_websocket_key)
{
    util::Optional<StringData> header_value = find_http_header_value(headers, "Sec-WebSocket-Key");

    if (!header_value)
        return false;

    sec_websocket_key = *header_value;

    return true;
}

util::Optional<HTTPResponse> do_make_http_response(const HTTPRequest& request,
                                                   const std::string& sec_websocket_protocol, std::error_code& ec)
{
    std::string sec_websocket_key;

    if (!validate_websocket_upgrade(request.headers)) {
        ec = Error::bad_request_header_upgrade;
        return util::none;
    }

    if (!validate_websocket_connection(request.headers)) {
        ec = Error::bad_request_header_connection;
        return util::none;
    }

    if (!validate_sec_websocket_version(request.headers)) {
        ec = Error::bad_request_header_websocket_version;
        return util::none;
    }

    if (!find_sec_websocket_key(request.headers, sec_websocket_key)) {
        ec = Error::bad_request_header_websocket_key;
        return util::none;
    }

    std::string sec_websocket_accept = make_sec_websocket_accept(sec_websocket_key);

    HTTPResponse response;
    response.status = HTTPStatus::SwitchingProtocols;
    response.headers["Upgrade"] = "websocket";
    response.headers["Connection"] = "Upgrade";
    response.headers["Sec-WebSocket-Accept"] = sec_websocket_accept;
    response.headers["Sec-WebSocket-Protocol"] = sec_websocket_protocol;

    return response;
}

// mask_payload masks (and demasks) the payload sent from the client to the server.
void mask_payload(char* masking_key, const char* payload, size_t payload_len, char* output)
{
    for (size_t i = 0; i < payload_len; ++i) {
        output[i] = payload[i] ^ masking_key[i % 4];
    }
}

// make_frame() creates a WebSocket frame according to the WebSocket standard.
// \param fin indicates whether the frame is the final fragment in a message.
// Sync clients and servers will only send unfragmented messages, but they must be
// prepared to receive fragmented messages.
// \param opcode must be one of six values:
// 0  = continuation frame
// 1  = text frame
// 2  = binary frame
// 8  = ping frame
// 9  = pong frame
// 10 = close frame.
// Sync clients and server will only send the last four, but must be prepared to
// receive all.
// \param mask indicates whether the payload of the frame should be masked. Frames
// are masked if and only if they originate from the client.
// The payload is located in the buffer \param payload, and has size \param payload_size.
// \param output is the output buffer. It must be large enough to contain the frame.
// The frame size can at most be payload_size + 14.
// \param random is used to create a random masking key.
// The return value is the size of the frame.
size_t make_frame(bool fin, int opcode, bool mask, const char* payload, size_t payload_size, char* output,
                  std::mt19937_64& random)
{
    int index = 0; // used to keep track of position within the header.
    using uchar = unsigned char;
    output[0] = (fin ? char(uchar(128)) : 0) + opcode; // fin and opcode in the first byte.
    output[1] = (mask ? char(uchar(128)) : 0);         // First bit of the second byte is mask.
    if (payload_size <= 125) {                         // The payload length is contained in the second byte.
        output[1] += static_cast<char>(payload_size);
        index = 2;
    }
    else if (payload_size < 65536) { // The payload length is contained bytes 3-4.
        output[1] += 126;
        // FIXME: Verify that this code works even if one sync-client is on a platform where
        // a 'char' is signed by default and the other client is on a platform where char is
        // unsigned. Note that the result of payload_size / 256 may not fit in a signed char.
        output[2] = static_cast<char>(payload_size / 256);

        // FIXME: Verify that the modulo arithmetic is well defined
        output[3] = payload_size % 256;
        index = 4;
    }
    else { // The payload length is contained in bytes 3-10.
        output[1] += 127;
        size_t fraction = payload_size;
        int remainder = 0;
        for (int i = 0; i < 8; ++i) {
            remainder = fraction % 256;
            fraction /= 256;
            output[9 - i] = remainder;
        }
        index = 10;
    }
    if (mask) {
        char masking_key[4];
        std::uniform_int_distribution<> dis(0, 255);
        for (int i = 0; i < 4; ++i) {
            masking_key[i] = dis(random);
        }
        output[index++] = masking_key[0];
        output[index++] = masking_key[1];
        output[index++] = masking_key[2];
        output[index++] = masking_key[3];
        mask_payload(masking_key, payload, payload_size, output + index);
    }
    else {
        std::copy(payload, payload + payload_size, output + index);
    }

    return payload_size + index;
}

// class FrameReader takes care of parsing the incoming bytes and
// constructing the received WebSocket messages. FrameReader manages
// read buffers internally. FrameReader handles fragmented messages as
// well. FrameRader is used in the following way:
//
// After constructing a FrameReader, the user runs in a loop.
//
// Loop start:
//
// Call frame_reader.next().
// if (frame_reader.protocol_error) {
//      // report a protocol error and
//      // break out of this loop
// }
// else if (frame_reader.delivery_ready) {
//     // use the message
//     // in frame_reader.delivery_buffer
//     // of size
//     // frame_reader.delivery_size
//     // with opcode (type)
//     // frame_reader.delivery_opcode
// }
// else {
//    // read frame_reader.read_size
//    // bytes into the buffer
//    // frame_reader.read_buffer
// }
// Goto Loop start.
//
class FrameReader {
public:
    util::Logger& logger;

    char* delivery_buffer = nullptr;
    size_t delivery_size = 0;
    size_t read_size = 0;
    char* read_buffer = nullptr;
    bool protocol_error = false;
    bool delivery_ready = false;
    websocket::Opcode delivery_opcode = websocket::Opcode::continuation;

    FrameReader(util::Logger& logger, bool& is_client)
        : logger(logger)
        , m_is_client(is_client)
    {
    }

    // reset() resets the frame reader such that it is ready to work on
    // a new WebSocket connection.
    void reset()
    {
        m_stage = Stage::init;
    }

    // next() parses the new information and moves
    // one stage forward.
    void next()
    {
        switch (m_stage) {
            case Stage::init:
                stage_init();
                break;
            case Stage::header_beginning:
                stage_header_beginning();
                break;
            case Stage::header_end:
                stage_header_end();
                break;
            case Stage::payload:
                stage_payload();
                break;
            case Stage::delivery:
                stage_delivery();
                break;
            default:
                break;
        }
    }

private:
    bool& m_is_client;

    char header_buffer[14];
    char* m_masking_key;
    size_t m_payload_size;
    websocket::Opcode m_opcode = websocket::Opcode::continuation;
    bool m_fin = false;
    bool m_mask = false;
    char m_short_payload_size = 0;

    char control_buffer[125]; // close, ping, pong.

    // A text or binary message can be fragmented. The
    // message is built up in m_message_buffer.
    std::vector<char> m_message_buffer;

    // The opcode of the message.
    websocket::Opcode m_message_opcode = websocket::Opcode::continuation;

    // The size of the stored Websocket message.
    // This size is not the same as the size of the buffer.
    size_t m_message_size = 0;

    // The message buffer has a minimum size,
    // and is extended when a large message arrives.
    // The message_buffer is resized to this value after
    // a larger message has been delivered.
    static const size_t s_message_buffer_min_size = 2048;

    enum class Stage { init, header_beginning, header_end, payload, delivery };
    Stage m_stage = Stage::init;

    void set_protocol_error()
    {
        protocol_error = true;
    }

    void set_payload_buffer()
    {
        read_size = m_payload_size;

        if (m_opcode == websocket::Opcode::close || m_opcode == websocket::Opcode::ping ||
            m_opcode == websocket::Opcode::pong) {
            read_buffer = control_buffer;
        }
        else {
            size_t required_size = m_message_size + m_payload_size;
            if (m_message_buffer.size() < required_size)
                m_message_buffer.resize(required_size);

            read_buffer = m_message_buffer.data() + m_message_size;
        }
    }

    void reset_message_buffer()
    {
        if (m_message_buffer.size() != s_message_buffer_min_size)
            m_message_buffer.resize(s_message_buffer_min_size);
        m_message_opcode = websocket::Opcode::continuation;
        m_message_size = 0;
    }


    // stage_init() is only called once for a FrameReader.
    // It just moves the stage to header_beginning and
    // asks for two bytes to be read into the header_buffer.
    void stage_init()
    {
        protocol_error = false;
        delivery_ready = false;
        delivery_buffer = nullptr;
        delivery_size = 0;
        delivery_opcode = websocket::Opcode::continuation;
        m_stage = Stage::header_beginning;
        reset_message_buffer();
        read_buffer = header_buffer;
        read_size = 2;
    }

    // When stage_header_beginning() is called, the
    // first two bytes in the header_buffer are
    // the first two bytes of an incoming WebSocket frame.
    void stage_header_beginning()
    {
        // bit 1.
        m_fin = ((header_buffer[0] & 128) == 128);

        // bit 2,3, and 4.
        char rsv = (header_buffer[0] & 112) >> 4;
        if (rsv != 0)
            return set_protocol_error();

        // bit 5, 6, 7, and 8.
        char op = (header_buffer[0] & 15);

        if (op != 0 && op != 1 && op != 2 && op != 8 && op != 9 && op != 10)
            return set_protocol_error();

        m_opcode = websocket::Opcode(op);

        // bit 9.
        m_mask = ((header_buffer[1] & 128) == 128);
        if ((m_mask && m_is_client) || (!m_mask && !m_is_client))
            return set_protocol_error();

        // Remainder of second byte.
        m_short_payload_size = (header_buffer[1] & 127);

        if (m_opcode == websocket::Opcode::continuation) {
            if (m_message_opcode == websocket::Opcode::continuation)
                return set_protocol_error();
        }
        else if (m_opcode == websocket::Opcode::text || m_opcode == websocket::Opcode::binary) {
            if (m_message_opcode != websocket::Opcode::continuation)
                return set_protocol_error();

            m_message_opcode = m_opcode;
        }
        else { // close, ping, pong.
            if (!m_fin || m_short_payload_size > 125)
                return set_protocol_error();
        }

        if (m_short_payload_size <= 125 && m_mask) {
            m_stage = Stage::header_end;
            m_payload_size = m_short_payload_size;
            read_size = 4;
            read_buffer = header_buffer + 2;
        }
        else if (m_short_payload_size <= 125 && !m_mask) {
            m_stage = Stage::payload;
            m_payload_size = m_short_payload_size;
            set_payload_buffer();
        }
        else if (m_short_payload_size == 126 && m_mask) {
            m_stage = Stage::header_end;
            read_size = 6;
            read_buffer = header_buffer + 2;
        }
        else if (m_short_payload_size == 126 && !m_mask) {
            m_stage = Stage::header_end;
            read_size = 2;
            read_buffer = header_buffer + 2;
        }
        else if (m_short_payload_size == 127 && m_mask) {
            m_stage = Stage::header_end;
            read_size = 12;
            read_buffer = header_buffer + 2;
        }
        else if (m_short_payload_size == 127 && !m_mask) {
            m_stage = Stage::header_end;
            read_size = 8;
            read_buffer = header_buffer + 2;
        }
    }

    void stage_header_end()
    {
        if (m_short_payload_size <= 125) {
            m_masking_key = header_buffer + 2;
        }
        else if (m_short_payload_size == 126) {
            const unsigned char* bytes = reinterpret_cast<unsigned char*>(header_buffer + 2);
            m_payload_size = bytes[0] * 256 + bytes[1];

            if (m_mask)
                m_masking_key = header_buffer + 4;
        }
        else if (m_short_payload_size == 127) {
            if (header_buffer[2] != 0 || header_buffer[3] != 0 || header_buffer[4] != 0 || header_buffer[5] != 0) {
                // Message should be smaller than 4GB
                // FIXME: We should introduce a maximum size for messages.
                set_protocol_error();
                return;
            }

            // Assume size_t is at least 4 bytes wide.
            const unsigned char* bytes = reinterpret_cast<unsigned char*>(header_buffer + 6);
            m_payload_size = bytes[0];
            m_payload_size = 256 * m_payload_size + bytes[1];
            m_payload_size = 256 * m_payload_size + bytes[2];
            m_payload_size = 256 * m_payload_size + bytes[3];

            if (m_mask)
                m_masking_key = header_buffer + 10;
        }

        m_stage = Stage::payload;
        set_payload_buffer();
    }

    void stage_payload()
    {
        if (m_mask)
            mask_payload(m_masking_key, read_buffer, m_payload_size, read_buffer);

        if (m_opcode == websocket::Opcode::close || m_opcode == websocket::Opcode::ping ||
            m_opcode == websocket::Opcode::pong) {
            m_stage = Stage::delivery;
            delivery_ready = true;
            delivery_opcode = m_opcode;
            delivery_buffer = control_buffer;
            delivery_size = m_payload_size;
        }
        else {
            m_message_size += m_payload_size;
            if (m_fin) {
                m_stage = Stage::delivery;
                delivery_ready = true;
                delivery_opcode = m_message_opcode;
                delivery_buffer = m_message_buffer.data();
                delivery_size = m_message_size;
            }
            else {
                m_stage = Stage::header_beginning;
                read_buffer = header_buffer;
                read_size = 2;
            }
        }
    }

    void stage_delivery()
    {
        m_stage = Stage::header_beginning;
        read_buffer = header_buffer;
        read_size = 2;
        delivery_ready = false;
        delivery_buffer = nullptr;
        delivery_size = 0;
        delivery_opcode = websocket::Opcode::continuation;

        if (m_opcode == websocket::Opcode::continuation || m_opcode == websocket::Opcode::text ||
            m_opcode == websocket::Opcode::binary)
            reset_message_buffer();
    }
};


class WebSocket {
public:
    WebSocket(websocket::Config& config)
        : m_config(config)
        , m_logger(config.websocket_get_logger())
        , m_frame_reader(config.websocket_get_logger(), m_is_client)
    {
        m_logger.debug("WebSocket::Websocket()");
    }

    void initiate_client_handshake(const std::string& request_uri, const std::string& host,
                                   const std::string& sec_websocket_protocol, HTTPHeaders headers)
    {
        m_logger.debug("WebSocket::initiate_client_handshake()");

        m_stopped = false;
        m_is_client = true;

        m_sec_websocket_key = make_random_sec_websocket_key(m_config.websocket_get_random());

        m_http_client.reset(new HTTPClient<websocket::Config>(m_config, m_logger));
        m_frame_reader.reset();
        HTTPRequest req;
        req.method = HTTPMethod::Get;
        req.path = std::move(request_uri);
        req.headers = std::move(headers);
        req.headers["Host"] = std::move(host);
        req.headers["Upgrade"] = "websocket";
        req.headers["Connection"] = "Upgrade";
        req.headers["Sec-WebSocket-Key"] = m_sec_websocket_key;
        req.headers["Sec-WebSocket-Version"] = sec_websocket_version;
        req.headers["Sec-WebSocket-Protocol"] = sec_websocket_protocol;

        m_logger.trace("HTTP request =\n%1", req);

        auto handler = [this](HTTPResponse response, std::error_code ec) {
            // If the operation is aborted, the WebSocket object may have been destroyed.
            if (ec != util::error::operation_aborted) {
                if (ec == HTTPParserError::MalformedResponse) {
                    error_client_malformed_response();
                    return;
                }
                if (ec) {
                    stop();

                    // FIXME: Should be read instaed of write???
                    m_config.websocket_write_error_handler(ec);

                    return;
                }
                if (m_stopped)
                    return;
                handle_http_response_received(std::move(response)); // Throws
            }
        };

        m_http_client->async_request(req, std::move(handler));
    }

    void initiate_server_websocket_after_handshake()
    {
        m_stopped = false;
        m_is_client = false;
        m_frame_reader.reset();
        frame_reader_loop(); // Throws
    }

    void initiate_server_handshake()
    {
        m_logger.debug("WebSocket::initiate_server_handshake()");

        m_stopped = false;
        m_is_client = false;
        m_http_server.reset(new HTTPServer<websocket::Config>(m_config, m_logger));
        m_frame_reader.reset();

        auto handler = [this](HTTPRequest request, std::error_code ec) {
            if (ec != util::error::operation_aborted) {
                if (ec == HTTPParserError::MalformedRequest) {
                    error_server_malformed_request();
                    return;
                }
                if (ec) {
                    stop();
                    m_config.websocket_read_error_handler(ec);
                    return;
                }
                if (m_stopped)
                    return;
                handle_http_request_received(std::move(request));
            }
        };

        m_http_server->async_receive_request(std::move(handler));
    }

    void async_write_frame(bool fin, int opcode, const char* data, size_t size,
                           std::function<void()> write_completion_handler)
    {
        REALM_ASSERT(!m_stopped);

        m_write_completion_handler = std::move(write_completion_handler);

        bool mask = m_is_client;

        // 14 is the maximum header length of a Websocket frame.
        size_t required_size = size + 14;
        if (m_write_buffer.size() < required_size)
            m_write_buffer.resize(required_size);

        size_t message_size =
            make_frame(fin, opcode, mask, data, size, m_write_buffer.data(), m_config.websocket_get_random());

        auto handler = [this](std::error_code ec, size_t) {
            // If the operation is aborted, then the write operation was canceled and we should ignore this callback.
            if (ec == util::error::operation_aborted) {
                return;
            }

            auto is_socket_closed_err = (ec == util::error::make_error_code(util::error::connection_reset) ||
                                         ec == util::make_error_code(util::MiscExtErrors::end_of_input));
            // If the socket has been closed then we should continue to read from it until we've drained
            // the receive buffer. Eventually we will either receive an in-band error message from the
            // server about why we got disconnected or we'll receive ECONNRESET on the receive side as well.
            if (is_socket_closed_err) {
                return;
            }

            // Otherwise we've got some other I/O error that we should surface to the sync client.
            if (ec) {
                stop();
                return m_config.websocket_write_error_handler(ec);
            }

            handle_write_message();
        };

        m_config.async_write(m_write_buffer.data(), message_size, handler);
    }

    void handle_write_message()
    {
        if (m_write_buffer.size() > s_write_buffer_stable_size) {
            m_write_buffer.resize(s_write_buffer_stable_size);
            m_write_buffer.shrink_to_fit();
        }

        auto handler = m_write_completion_handler;
        m_write_completion_handler = std::function<void()>{};
        handler();
    }

    void stop() noexcept
    {
        m_stopped = true;
        m_frame_reader.reset();
    }

private:
    websocket::Config& m_config;
    util::Logger& m_logger;
    FrameReader m_frame_reader;

    bool m_stopped = false;
    bool m_is_client;

    // Allocated on demand.
    std::unique_ptr<HTTPClient<websocket::Config>> m_http_client;
    std::unique_ptr<HTTPServer<websocket::Config>> m_http_server;

    std::string m_sec_websocket_key;
    std::string m_sec_websocket_accept;

    std::vector<char> m_write_buffer;
    static const size_t s_write_buffer_stable_size = 2048;

    std::function<void()> m_write_completion_handler;

    void error_client_malformed_response()
    {
        m_stopped = true;
        m_logger.error("WebSocket: Received malformed HTTP response");
        std::error_code ec = Error::bad_response_invalid_http;
        m_config.websocket_handshake_error_handler(ec, nullptr, nullptr); // Throws
    }

    void error_client_response_not_101(const HTTPResponse& response)
    {
        m_stopped = true;

        m_logger.error("Websocket: Expected HTTP response 101 Switching Protocols, "
                       "but received:\n%1",
                       response);

        int status_code = int(response.status);
        std::error_code ec;

        if (status_code == 200)
            ec = Error::bad_response_200_ok;
        else if (status_code >= 200 && status_code < 300)
            ec = Error::bad_response_2xx_successful;
        else if (status_code == 301)
            ec = Error::bad_response_301_moved_permanently;
        else if (status_code >= 300 && status_code < 400)
            ec = Error::bad_response_3xx_redirection;
        else if (status_code == 401)
            ec = Error::bad_response_401_unauthorized;
        else if (status_code == 403)
            ec = Error::bad_response_403_forbidden;
        else if (status_code == 404)
            ec = Error::bad_response_404_not_found;
        else if (status_code == 410)
            ec = Error::bad_response_410_gone;
        else if (status_code >= 400 && status_code < 500)
            ec = Error::bad_response_4xx_client_errors;
        else if (status_code == 500)
            ec = Error::bad_response_500_internal_server_error;
        else if (status_code == 502)
            ec = Error::bad_response_502_bad_gateway;
        else if (status_code == 503)
            ec = Error::bad_response_503_service_unavailable;
        else if (status_code == 504)
            ec = Error::bad_response_504_gateway_timeout;
        else if (status_code >= 500 && status_code < 600)
            ec = Error::bad_response_5xx_server_error;
        else
            ec = Error::bad_response_unexpected_status_code;

        std::string_view body;
        std::string_view* body_ptr = nullptr;
        if (response.body) {
            body = *response.body;
            body_ptr = &body;
        }
        m_config.websocket_handshake_error_handler(ec, &response.headers, body_ptr); // Throws
    }

    void error_client_response_websocket_headers_invalid(const HTTPResponse& response)
    {
        m_stopped = true;

        m_logger.error("Websocket: HTTP response has invalid websocket headers."
                       "HTTP response = \n%1",
                       response);
        std::error_code ec = Error::bad_response_header_protocol_violation;
        std::string_view body;
        std::string_view* body_ptr = nullptr;
        if (response.body) {
            body = *response.body;
            body_ptr = &body;
        }
        m_config.websocket_handshake_error_handler(ec, &response.headers, body_ptr); // Throws
    }

    void error_server_malformed_request()
    {
        m_stopped = true;
        m_logger.error("WebSocket: Received malformed HTTP request");
        std::error_code ec = Error::bad_request_malformed_http;
        m_config.websocket_handshake_error_handler(ec, nullptr, nullptr); // Throws
    }

    void error_server_request_header_protocol_violation(std::error_code ec, const HTTPRequest& request)
    {
        m_stopped = true;

        m_logger.error("Websocket: HTTP request has invalid websocket headers."
                       "HTTP request = \n%1",
                       request);
        m_config.websocket_handshake_error_handler(ec, &request.headers, nullptr); // Throws
    }

    void protocol_error(std::error_code ec)
    {
        m_stopped = true;
        m_config.websocket_protocol_error_handler(ec);
    }

    // The client receives the HTTP response.
    void handle_http_response_received(HTTPResponse response)
    {
        m_logger.debug("WebSocket::handle_http_response_received()");
        m_logger.trace("HTTP response = %1", response);

        if (response.status != HTTPStatus::SwitchingProtocols) {
            error_client_response_not_101(response);
            return;
        }

        bool valid = (find_sec_websocket_accept(response.headers) &&
                      m_sec_websocket_accept == make_sec_websocket_accept(m_sec_websocket_key));
        if (!valid) {
            error_client_response_websocket_headers_invalid(response);
            return;
        }

        m_config.websocket_handshake_completion_handler(response.headers);

        if (m_stopped)
            return;

        frame_reader_loop();
    }

    void handle_http_request_received(HTTPRequest request)
    {
        m_logger.trace("WebSocket::handle_http_request_received()");

        util::Optional<std::string> sec_websocket_protocol = websocket::read_sec_websocket_protocol(request);

        std::error_code ec;
        util::Optional<HTTPResponse> response =
            do_make_http_response(request, sec_websocket_protocol ? *sec_websocket_protocol : "realm.io", ec);

        if (ec) {
            error_server_request_header_protocol_violation(ec, request);
            return;
        }
        REALM_ASSERT(response);

        auto handler = [request, this](std::error_code ec) {
            // If the operation is aborted, the socket object may have been destroyed.
            if (ec != util::error::operation_aborted) {
                if (ec) {
                    stop();
                    m_config.websocket_write_error_handler(ec);
                    return;
                }

                if (m_stopped)
                    return;

                m_config.websocket_handshake_completion_handler(request.headers);

                if (m_stopped)
                    return;

                frame_reader_loop(); // Throws
            }
        };
        m_http_server->async_send_response(*response, std::move(handler));
    }

    // find_sec_websocket_accept is similar to
    // find_sec_websockey_key.
    bool find_sec_websocket_accept(const HTTPHeaders& headers)
    {
        util::Optional<StringData> header_value = find_http_header_value(headers, "Sec-WebSocket-Accept");

        if (!header_value)
            return false;

        m_sec_websocket_accept = *header_value;

        return true;
    }

    std::pair<std::error_code, StringData> parse_close_message(const char* data, size_t size)
    {
        uint16_t error_code;
        StringData error_message;
        if (size < 2) {
            // Error code 1005 is defined as
            //     1005 is a reserved value and MUST NOT be set as a status code in a
            //     Close control frame by an endpoint.  It is designated for use in
            //     applications expecting a status code to indicate that no status
            //     code was actually present.
            // See https://tools.ietf.org/html/rfc6455#section-7.4.1 for more details
            error_code = 1005;
        }
        else {
            // Otherwise, the error code is the first two bytes of the body as a uint16_t in
            // network byte order. See https://tools.ietf.org/html/rfc6455#section-5.5.1 for more
            // details.
            error_code = ntohs((data[1] << 8) | data[0]);
            error_message = StringData(data + 2, size - 2);
        }

        std::error_code error_code_with_category{error_code, websocket::websocket_close_status_category()};
        return std::make_pair(error_code_with_category, error_message);
    }

    // frame_reader_loop() uses the frame_reader to read and process the incoming
    // WebSocket messages.
    void frame_reader_loop()
    {
        // Advance parsing stage.
        m_frame_reader.next();

        if (m_frame_reader.protocol_error) {
            protocol_error(Error::bad_message);
            return;
        }

        if (m_frame_reader.delivery_ready) {
            bool should_continue = true;

            switch (m_frame_reader.delivery_opcode) {
                case websocket::Opcode::text:
                    should_continue = m_config.websocket_text_message_received(m_frame_reader.delivery_buffer,
                                                                               m_frame_reader.delivery_size);
                    break;
                case websocket::Opcode::binary:
                    should_continue = m_config.websocket_binary_message_received(m_frame_reader.delivery_buffer,
                                                                                 m_frame_reader.delivery_size);
                    break;
                case websocket::Opcode::close: {
                    auto [error_code, error_message] =
                        parse_close_message(m_frame_reader.delivery_buffer, m_frame_reader.delivery_size);
                    should_continue = m_config.websocket_close_message_received(error_code, error_message);
                    break;
                }
                case websocket::Opcode::ping:
                    should_continue = m_config.websocket_ping_message_received(m_frame_reader.delivery_buffer,
                                                                               m_frame_reader.delivery_size);
                    break;
                case websocket::Opcode::pong:
                    should_continue = m_config.websocket_pong_message_received(m_frame_reader.delivery_buffer,
                                                                               m_frame_reader.delivery_size);
                    break;
                default:
                    break;
            }

            // The websocket object might not even exist anymore
            if (!should_continue)
                return;

            if (m_stopped)
                return;

            // recursion is harmless, since the depth will be at most 2.
            frame_reader_loop();
            return;
        }

        auto handler = [this](std::error_code ec, size_t) {
            // If the operation is aborted, the socket object may have been destroyed.
            if (ec != util::error::operation_aborted) {
                if (ec) {
                    stop();
                    m_config.websocket_read_error_handler(ec);
                    return;
                }

                if (m_stopped)
                    return;

                frame_reader_loop();
            }
        };

        m_config.async_read(m_frame_reader.read_buffer, m_frame_reader.read_size, std::move(handler));
    }
};


const char* get_error_message(Error error_code)
{
    switch (error_code) {
        case Error::bad_request_malformed_http:
            return "Bad WebSocket request malformed HTTP";
        case Error::bad_request_header_upgrade:
            return "Bad WebSocket request header: Upgrade";
        case Error::bad_request_header_connection:
            return "Bad WebSocket request header: Connection";
        case Error::bad_request_header_websocket_version:
            return "Bad WebSocket request header: Sec-Websocket-Version";
        case Error::bad_request_header_websocket_key:
            return "Bad WebSocket request header: Sec-Websocket-Key";
        case Error::bad_response_invalid_http:
            return "Bad WebSocket response invalid HTTP";
        case Error::bad_response_2xx_successful:
            return "Bad WebSocket response 2xx successful";
        case Error::bad_response_200_ok:
            return "Bad WebSocket response 200 ok";
        case Error::bad_response_3xx_redirection:
            return "Bad WebSocket response 3xx redirection";
        case Error::bad_response_301_moved_permanently:
            return "Bad WebSocket response 301 moved permanently";
        case Error::bad_response_4xx_client_errors:
            return "Bad WebSocket response 4xx client errors";
        case Error::bad_response_401_unauthorized:
            return "Bad WebSocket response 401 unauthorized";
        case Error::bad_response_403_forbidden:
            return "Bad WebSocket response 403 forbidden";
        case Error::bad_response_404_not_found:
            return "Bad WebSocket response 404 not found";
        case Error::bad_response_410_gone:
            return "Bad WebSocket response 410 gone";
        case Error::bad_response_5xx_server_error:
            return "Bad WebSocket response 5xx server error";
        case Error::bad_response_500_internal_server_error:
            return "Bad WebSocket response 500 internal server error";
        case Error::bad_response_502_bad_gateway:
            return "Bad WebSocket response 502 bad gateway";
        case Error::bad_response_503_service_unavailable:
            return "Bad WebSocket response 503 service unavailable";
        case Error::bad_response_504_gateway_timeout:
            return "Bad WebSocket response 504 gateway timeout";
        case Error::bad_response_unexpected_status_code:
            return "Bad Websocket response unexpected status code";
        case Error::bad_response_header_protocol_violation:
            return "Bad WebSocket response header protocol violation";
        case Error::bad_message:
            return "Ill-formed WebSocket message";
    }
    return nullptr;
}


class ErrorCategoryImpl : public std::error_category {
public:
    const char* name() const noexcept override final
    {
        return "realm::util::websocket::Error";
    }
    std::string message(int error_code) const override final
    {
        const char* msg = get_error_message(Error(error_code));
        if (!msg)
            msg = "Unknown error";
        std::string msg_2{msg}; // Throws (copy)
        return msg_2;
    }
};

ErrorCategoryImpl g_error_category;

class CloseStatusErrorCategory : public std::error_category {
    const char* name() const noexcept final
    {
        return "realm::util::websocket::CloseStatus";
    }
    std::string message(int error_code) const final
    {
        // Converts an error_code to one of the pre-defined status codes in
        // https://tools.ietf.org/html/rfc6455#section-7.4.1
        switch (error_code) {
            case 1000:
                return "normal closure";
            case 1001:
                return "endpoint going away";
            case 1002:
                return "protocol error";
            case 1003:
                return "invalid data type";
            case 1004:
                return "reserved";
            case 1005:
                return "no status code present";
            case 1006:
                return "no close control frame sent";
            case 1007:
                return "message data type mis-match";
            case 1008:
                return "policy violation";
            case 1009:
                return "message too big";
            case 1010:
                return "missing extension";
            case 1011:
                return "unexpected error";
            case 1015:
                return "TLS handshake failure";
            default:
                return "unknown error";
        };
    }
};

} // unnamed namespace


bool websocket::Config::websocket_text_message_received(const char*, size_t)
{
    return true;
}

bool websocket::Config::websocket_binary_message_received(const char*, size_t)
{
    return true;
}

bool websocket::Config::websocket_close_message_received(std::error_code, StringData)
{
    return true;
}

bool websocket::Config::websocket_ping_message_received(const char*, size_t)
{
    return true;
}

bool websocket::Config::websocket_pong_message_received(const char*, size_t)
{
    return true;
}


class websocket::Socket::Impl : public WebSocket {
public:
    Impl(Config& config)
        : WebSocket(config) // Throws
    {
    }
};

websocket::Socket::Socket(Config& config)
    : m_impl(new Impl{config})
{
}

websocket::Socket::Socket(Socket&& socket) noexcept
    : m_impl(std::move(socket.m_impl))
{
}

websocket::Socket::~Socket() noexcept {}

void websocket::Socket::initiate_client_handshake(const std::string& request_uri, const std::string& host,
                                                  const std::string& sec_websocket_protocol, HTTPHeaders headers)
{
    m_impl->initiate_client_handshake(request_uri, host, sec_websocket_protocol, std::move(headers));
}

void websocket::Socket::initiate_server_handshake()
{
    m_impl->initiate_server_handshake();
}

void websocket::Socket::initiate_server_websocket_after_handshake()
{
    m_impl->initiate_server_websocket_after_handshake();
}

void websocket::Socket::async_write_frame(bool fin, Opcode opcode, const char* data, size_t size,
                                          std::function<void()> handler)
{
    m_impl->async_write_frame(fin, int(opcode), data, size, handler);
}

void websocket::Socket::async_write_text(const char* data, size_t size, std::function<void()> handler)
{
    async_write_frame(true, Opcode::text, data, size, handler);
}

void websocket::Socket::async_write_binary(const char* data, size_t size, std::function<void()> handler)
{
    async_write_frame(true, Opcode::binary, data, size, handler);
}

void websocket::Socket::async_write_close(const char* data, size_t size, std::function<void()> handler)
{
    async_write_frame(true, Opcode::close, data, size, handler);
}

void websocket::Socket::async_write_ping(const char* data, size_t size, std::function<void()> handler)
{
    async_write_frame(true, Opcode::ping, data, size, handler);
}

void websocket::Socket::async_write_pong(const char* data, size_t size, std::function<void()> handler)
{
    async_write_frame(true, Opcode::pong, data, size, handler);
}

void websocket::Socket::stop() noexcept
{
    m_impl->stop();
}

util::Optional<std::string> websocket::read_sec_websocket_protocol(const HTTPRequest& request)
{
    const HTTPHeaders& headers = request.headers;
    const StringData header = "Sec-WebSocket-Protocol";
    util::Optional<StringData> value = find_http_header_value(headers, header);
    return value ? util::Optional<std::string>(std::string(*value)) : util::none;
}

util::Optional<HTTPResponse> websocket::make_http_response(const HTTPRequest& request,
                                                           const std::string& sec_websocket_protocol,
                                                           std::error_code& ec)
{
    return do_make_http_response(request, sec_websocket_protocol, ec);
}

const std::error_category& websocket::error_category() noexcept
{
    return g_error_category;
}

const std::error_category& websocket::websocket_close_status_category() noexcept
{
    static const CloseStatusErrorCategory category = {};
    return category;
}

std::error_code websocket::make_error_code(Error error_code) noexcept
{
    return std::error_code{int(error_code), g_error_category};
}
