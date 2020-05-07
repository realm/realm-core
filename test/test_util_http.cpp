#include "test.hpp"

#include <realm/util/network.hpp>
#include <realm/util/http.hpp>

#include <thread>

using namespace realm;
using namespace realm::util;

namespace {

struct BufferedSocket : network::Socket {
    BufferedSocket(network::Service& service)
        : network::Socket(service)
    {
    }

    BufferedSocket(network::Service& service, const network::StreamProtocol& protocol,
                   native_handle_type native_handle)
        : network::Socket(service, protocol, native_handle)
    {
    }


    template <class H>
    void async_read_until(char* buffer, std::size_t size, char delim, H handler)
    {
        network::Socket::async_read_until(buffer, size, delim, m_read_buffer, std::move(handler));
    }

    template <class H>
    void async_read(char* buffer, std::size_t size, H handler)
    {
        network::Socket::async_read(buffer, size, m_read_buffer, std::move(handler));
    }

private:
    network::ReadAheadBuffer m_read_buffer;
};

} // anonymous namespace

TEST(HTTP_ParseAuthorization)
{
    {
        auto auth = parse_authorization("");
        CHECK(auth.scheme == "");
        CHECK_EQUAL(auth.values.size(), 0);
    }

    {
        auto auth = parse_authorization("      ");
        CHECK(auth.scheme == "");
        CHECK_EQUAL(auth.values.size(), 0);
    }

    {
        auto auth = parse_authorization("Super-Scheme    ");
        CHECK(auth.scheme == "Super-Scheme");
        CHECK_EQUAL(auth.values.size(), 0);
    }

    {
        auto auth = parse_authorization("Super-Scheme key");
        CHECK(auth.scheme == "Super-Scheme");
        CHECK_EQUAL(auth.values.size(), 0);
    }

    {
        auto auth = parse_authorization("Super-Scheme key   ");
        CHECK(auth.scheme == "Super-Scheme");
        CHECK_EQUAL(auth.values.size(), 0);
    }

    {
        auto auth = parse_authorization("Super-Scheme key=");
        CHECK(auth.scheme == "Super-Scheme");
        CHECK_EQUAL(auth.values.size(), 1);
        CHECK(auth.values.count("key") > 0);
        CHECK(auth.values["key"] == "");
    }

    {
        auto auth = parse_authorization("Super-Scheme key=   ");
        CHECK(auth.scheme == "Super-Scheme");
        CHECK_EQUAL(auth.values.size(), 1);
        CHECK(auth.values.count("key"));
        CHECK(auth.values["key"] == "");
    }

    {
        auto auth = parse_authorization("Super-Scheme key=val");
        CHECK(auth.scheme == "Super-Scheme");
        CHECK_EQUAL(auth.values.size(), 1);
        CHECK(auth.values.count("key"));
        CHECK(auth.values["key"] == "val");
    }

    {
        auto auth = parse_authorization("Super-Scheme key=val   ");
        CHECK(auth.scheme == "Super-Scheme");
        CHECK_EQUAL(auth.values.size(), 1);
        CHECK(auth.values.count("key"));
        CHECK(auth.values["key"] == "val");
    }

    {
        auto auth = parse_authorization("Super-Scheme key1=val1 omitted empty= key2=val2");
        CHECK(auth.scheme == "Super-Scheme");
        CHECK_EQUAL(auth.values.size(), 3);
        CHECK(auth.values["key1"] == "val1");
        CHECK(auth.values["key2"] == "val2");
        CHECK(!auth.values.count("omitted"));
        CHECK(auth.values.count("empty"));
        CHECK(auth.values["empty"] == "");
    }
}

TEST(HTTP_RequestResponse)
{
    util::Logger& logger = test_context.logger;
    network::Service server;
    network::Acceptor acceptor{server};
    network::Endpoint ep;
    acceptor.open(ep.protocol());
    acceptor.bind(ep);
    ep = acceptor.local_endpoint();
    acceptor.listen();

    Optional<HTTPRequest> received_request;
    Optional<HTTPResponse> received_response;

    std::thread server_thread{[&] {
        BufferedSocket c(server);
        HTTPServer<BufferedSocket> http(c, logger);
        acceptor.async_accept(c, [&](std::error_code ec) {
            CHECK(!ec);
            http.async_receive_request([&](HTTPRequest req, std::error_code ec) {
                CHECK(!ec);
                received_request = std::move(req);
                HTTPResponse res;
                res.status = HTTPStatus::Ok;
                res.headers["X-Realm-Foo "] = "Bar";
                res.headers["Content-Type"] = "\tapplication/json";
                res.headers["Content-Length"] = "2";
                res.body = std::string("{}");
                http.async_send_response(res, [&](std::error_code ec) {
                    CHECK(!ec);
                    server.stop();
                });
            });
        });
        server.run();
    }};

    {
        network::Service client;
        BufferedSocket c(client);
        HTTPClient<BufferedSocket> http(c, logger);
        c.async_connect(ep, [&](std::error_code ec) {
            CHECK(!ec);
            HTTPRequest req;
            req.path = "/hello/world/?http=1";
            req.method = HTTPMethod::Get;
            req.headers[" X-Realm-Foo"] = "Bar";
            req.headers["Content-Type"] = "application/json";
            http.async_request(req, [&](HTTPResponse res, std::error_code ec) {
                CHECK(!ec);
                received_response = std::move(res);
            });
        });
        client.run();
    }
    server_thread.join();

    CHECK(received_request);
    CHECK(received_response);

    CHECK(received_request->method == HTTPMethod::Get);
    CHECK(!received_request->body);
    CHECK_EQUAL(received_request->path, "/hello/world/?http=1");
    CHECK_EQUAL(received_request->headers["X-Realm-Foo"], "Bar");
    CHECK_EQUAL(received_request->headers["Content-Type"], "application/json");

    CHECK(received_response->status == HTTPStatus::Ok);
    CHECK_EQUAL(*received_response->body, "{}");
    CHECK_EQUAL(received_response->headers["Content-Length"], "2");
    CHECK_EQUAL(received_response->headers["X-Realm-Foo"], "Bar");
    CHECK_EQUAL(received_response->headers["Content-Type"], "application/json");
}

TEST(HTTPHeaders_CaseInsensitive)
{
    HTTPHeaders headers;
    headers["a"] = "foo";
    headers["A"] = "bar";
    CHECK_EQUAL(headers.size(), 1);
    CHECK_EQUAL(headers["a"], "bar");
    headers["bA"] = "bbb";
    headers["Ba"] = "BBB";
    CHECK_EQUAL(headers.size(), 2);
    CHECK_EQUAL(headers["ba"], "BBB");
    CHECK_EQUAL(headers["BA"], "BBB");
    CHECK_EQUAL(headers["bA"], "BBB");
}

TEST(HTTPParser_RequestLine)
{
    HTTPMethod m;
    StringData uri;

    StringData input[] = {
        "GET / HTTP/1.1",
        "GET HTTP/1.1",
        "POST /",
        "GET  /  HTTP/1.1",
        "GET /  HTTP/1.1",
        "GET  / HTTP/1.1",
        "FOO / HTTP/1.1",
        "get / http/1.1",
        "GET path_without_leading_slash HTTP/1.1",
        "GET path?with=query HTTP/1.1",
        "GET",
    };
    constexpr size_t num_inputs = sizeof(input) / sizeof(input[0]);
    struct expect_t {
        bool success;
        HTTPMethod method;
        StringData uri;
        expect_t(bool s)
            : success(s)
        {
        }
        expect_t(bool s, HTTPMethod m, StringData u)
            : success(s)
            , method(m)
            , uri(u)
        {
        }
    };
    expect_t expectations[num_inputs] = {
        {true, HTTPMethod::Get, "/"},
        {false},
        {false},
        {false},
        {false},
        {false},
        {false},
        {false},
        {true, HTTPMethod::Get, "path_without_leading_slash"},
        {true, HTTPMethod::Get, "path?with=query"},
        {false},
    };

    for (size_t i = 0; i < num_inputs; ++i) {
        bool result = HTTPParserBase::parse_first_line_of_request(input[i], m, uri);
        CHECK_EQUAL(result, expectations[i].success);
        if (expectations[i].success) {
            CHECK_EQUAL(m, expectations[i].method);
            CHECK_EQUAL(uri, expectations[i].uri);
        }
    }
}

TEST(HTTPParser_ResponseLine)
{
    util::Logger& logger = test_context.logger;
    HTTPStatus s;
    struct expect_t {
        bool success;
        HTTPStatus status;
        StringData reason;
        expect_t(bool s)
            : success(s)
        {
        }
        expect_t(bool s, HTTPStatus status, StringData reason)
            : success(s)
            , status(status)
            , reason(reason)
        {
        }
    };

    StringData input[] = {"HTTP/1.1 200 OK", "HTTP 200 OK",  "HTTP/1.1 500 Detailed Reason",
                          "HTTP/1.1",        "HTTP/1.1 200", "HTTP/1.1 non-integer OK"};
    constexpr size_t num_inputs = sizeof(input) / sizeof(input[0]);
    expect_t expectations[num_inputs] = {
        {true, HTTPStatus::Ok, "OK"},
        {false},
        {true, HTTPStatus::InternalServerError, "Detailed Reason"},
        {false},
        {true, HTTPStatus::Ok, ""}, // Status without Reason-Phrase is not
                                    // allowed according to HTTP/1.1, but some
                                    // proxies do it anyway.
        {false},
    };

    for (size_t i = 0; i < num_inputs; ++i) {
        StringData reason = "";
        bool result = HTTPParserBase::parse_first_line_of_response(input[i], s, reason, logger);
        CHECK_EQUAL(result, expectations[i].success);
        if (expectations[i].success) {
            CHECK_EQUAL(s, expectations[i].status);
            CHECK_EQUAL(reason, expectations[i].reason);
        }
    }
}

struct FakeHTTPParser : HTTPParserBase {
    StringData key;
    StringData value;
    StringData body;
    std::error_code error;

    FakeHTTPParser(util::Logger& logger)
        : HTTPParserBase{logger}
    {
    }

    std::error_code on_first_line(StringData) override
    {
        return std::error_code{};
    }
    void on_header(StringData k, StringData v) override
    {
        key = k;
        value = v;
    }
    void on_body(StringData b) override
    {
        body = b;
    }
    void on_complete(std::error_code ec) override
    {
        error = ec;
    }
};

TEST(HTTPParser_ParseHeaderLine)
{
    util::Logger& logger = test_context.logger;
    FakeHTTPParser p{logger};

    struct expect {
        bool success;
        StringData key;
        StringData value;

        expect(bool s)
            : success(s)
        {
        }
        expect(StringData k, StringData v)
            : success(true)
            , key(k)
            , value(v)
        {
        }
    };

    StringData input[] = {
        "My-Header: Value", ":", "", "Header: Value", "Header:", ": Just a value",
    };

    constexpr size_t num_inputs = sizeof(input) / sizeof(input[0]);
    expect expectations[num_inputs] = {
        {"My-Header", "Value"}, {false}, {false}, {"Header", "Value"}, {"Header", ""}, {false},
    };

    for (size_t i = 0; i < num_inputs; ++i) {
        std::copy(input[i].data(), input[i].data() + input[i].size(), p.m_read_buffer.get());
        bool r = p.parse_header_line(input[i].size());
        CHECK_EQUAL(r, expectations[i].success);
        if (expectations[i].success) {
            CHECK_EQUAL(p.key, expectations[i].key);
            CHECK_EQUAL(p.value, expectations[i].value);
        }
    }
}
