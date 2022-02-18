#include <algorithm>
#include <cctype>
#include <sstream>
#include <ostream>

#include <realm/util/http.hpp>

using namespace realm;

namespace {

StringData trim_whitespace(StringData str)
{
    auto p0 = str.data();
    auto p1 = str.data() + str.size();
    while (p1 > p0 && std::isspace(*(p1 - 1)))
        --p1;
    while (p0 < p1 && std::isspace(*p0))
        ++p0;
    return StringData(p0, p1 - p0);
}


struct HTTPParserErrorCategory : std::error_category {
    HTTPParserErrorCategory() {}

    const char* name() const noexcept override
    {
        return "HTTP Parser Error";
    }

    std::string message(int condition) const override
    {
        using util::HTTPParserError;
        switch (HTTPParserError(condition)) {
            case HTTPParserError::None:
                return "None";
            case HTTPParserError::ContentTooLong:
                return "Content too long";
            case HTTPParserError::HeaderLineTooLong:
                return "Header line too long";
            case HTTPParserError::MalformedResponse:
                return "Malformed response";
            case HTTPParserError::MalformedRequest:
                return "Malformed request";
            default:
                REALM_TERMINATE("Invalid HTTP Parser Error");
        }
    }
};

const HTTPParserErrorCategory g_http_parser_error_category;

} // unnamed namespace


namespace realm {
namespace util {

bool valid_http_status_code(unsigned int code)
{
    if (code < 100)
        return false;
    if (code > 101 && code < 200)
        return false;
    if (code > 206 && code < 300)
        return false;
    if (code > 308 && code < 400)
        return false;
    if (code > 451 && code < 500)
        return false;
    if (code > 511)
        return false;

    return true;
}


HTTPAuthorization parse_authorization(const std::string& header_value)
{
    // StringData line{header_value.c_str(), header_value.length()};

    HTTPAuthorization auth;
    auto p = header_value.begin();
    auto end = header_value.end();
    auto space = std::find(p, end, ' ');

    auth.scheme = std::string(p, space);

    while (space != end) {
        p = space + 1;
        space = std::find(p, end, ' ');
        auto eq = std::find(p, space, '=');

        if (eq == space) {
            continue;
        }

        auto key_begin = p;
        auto key_end = eq;
        auto value_begin = eq + 1;
        auto value_end = space;

        std::string key(key_begin, key_end);
        std::string value(value_begin, value_end);

        if (key.size() == 0)
            continue;

        auth.values[key] = value;
    }

    return auth;
}


void HTTPParserBase::set_write_buffer(const HTTPRequest& req)
{
    std::stringstream ss;
    ss << req;
    m_write_buffer = ss.str();
}


void HTTPParserBase::set_write_buffer(const HTTPResponse& res)
{
    std::stringstream ss;
    ss << res;
    m_write_buffer = ss.str();
}


bool HTTPParserBase::parse_header_line(size_t len)
{
    StringData line{m_read_buffer.get(), len};
    auto p = line.data();
    auto end = line.data() + line.size();
    auto colon = std::find(p, end, ':');

    if (colon == end) {
        logger.error("Bad header line in HTTP message:\n%1", line);
        return false;
    }

    auto key_begin = p;
    auto key_end = colon;
    auto value_begin = colon + 1;
    auto value_end = end;

    StringData key(key_begin, key_end - key_begin);
    StringData value(value_begin, value_end - value_begin);

    key = trim_whitespace(key);
    value = trim_whitespace(value);

    if (key.size() == 0) {
        logger.error("Bad header line in HTTP message:\n%1", line);
        return false;
    }

    if (key == "Content-Length") {
        if (value.size() == 0) {
            // We consider the empty Content-Length to mean 0.
            // A warning is logged.
            logger.warn("Empty Content-Length header in HTTP message:\n%1", line);
            m_found_content_length = 0;
        }
        else {
            std::stringstream ss;
            ss.str(value);
            size_t content_length;
            if (ss >> content_length && ss.eof()) {
                m_found_content_length = content_length;
            }
            else {
                logger.error("Bad Content-Length header in HTTP message:\n%1", line);
                return false;
            }
        }
    }

    this->on_header(key, value);
    return true;
}


Optional<HTTPMethod> HTTPParserBase::parse_method_string(StringData method)
{
    if (method == "OPTIONS")
        return HTTPMethod::Options;
    if (method == "GET")
        return HTTPMethod::Get;
    if (method == "HEAD")
        return HTTPMethod::Head;
    if (method == "POST")
        return HTTPMethod::Post;
    if (method == "PUT")
        return HTTPMethod::Put;
    if (method == "DELETE")
        return HTTPMethod::Delete;
    if (method == "TRACE")
        return HTTPMethod::Trace;
    if (method == "CONNECT")
        return HTTPMethod::Connect;
    return none;
}


bool HTTPParserBase::parse_first_line_of_request(StringData line, HTTPMethod& out_method, StringData& out_uri)
{
    line = trim_whitespace(line);
    auto p = line.data();
    auto end = line.data() + line.size();
    auto sp = std::find(p, end, ' ');
    if (sp == end)
        return false;
    StringData method(p, sp - p);
    auto request_uri_begin = sp + 1;
    sp = std::find(request_uri_begin, end, ' ');
    if (sp == end)
        return false;
    out_uri = StringData(request_uri_begin, sp - request_uri_begin);
    auto http_version_begin = sp + 1;
    StringData http_version(http_version_begin, end - http_version_begin);
    if (http_version != "HTTP/1.1") {
        return false;
    }
    auto parsed_method = HTTPParserBase::parse_method_string(method);
    if (!parsed_method)
        return false;
    out_method = *parsed_method;
    return true;
}


bool HTTPParserBase::parse_first_line_of_response(StringData line, HTTPStatus& out_status, StringData& out_reason,
                                                  util::Logger& logger)
{
    line = trim_whitespace(line);
    auto p = line.data();
    auto end = line.data() + line.size();
    auto sp = std::find(p, end, ' ');
    if (sp == end) {
        logger.error("Invalid HTTP response:\n%1", line);
        return false;
    }
    StringData http_version(p, sp - p);
    if (http_version != "HTTP/1.1") {
        logger.error("Invalid version in HTTP response:\n%1", line);
        return false;
    }
    auto status_code_begin = sp + 1;
    sp = std::find(status_code_begin, end, ' ');
    auto status_code_end = sp;
    if (status_code_end != end) {
        // Some proxies don't give a "Reason-Phrase". This is not valid
        // according to the HTTP/1.1 standard, but what are we gonna do...
        auto reason_begin = sp + 1;
        auto reason_end = end;
        out_reason = StringData(reason_begin, reason_end - reason_begin);
    }

    StringData status_code_str(status_code_begin, status_code_end - status_code_begin);
    std::stringstream ss;
    ss << status_code_str;
    unsigned int code;
    if (ss >> code && valid_http_status_code(code)) {
        out_status = static_cast<HTTPStatus>(code);
    }
    else {
        logger.error("Invalid status code in HTTP response:\n%1", line);
        return false;
    }
    return true;
}


std::ostream& operator<<(std::ostream& os, HTTPMethod method)
{
    switch (method) {
        case HTTPMethod::Options:
            return os << "OPTIONS";
        case HTTPMethod::Get:
            return os << "GET";
        case HTTPMethod::Head:
            return os << "HEAD";
        case HTTPMethod::Post:
            return os << "POST";
        case HTTPMethod::Put:
            return os << "PUT";
        case HTTPMethod::Delete:
            return os << "DELETE";
        case HTTPMethod::Trace:
            return os << "TRACE";
        case HTTPMethod::Connect:
            return os << "CONNECT";
    }
    REALM_TERMINATE("Invalid HTTPRequest object.");
}


std::ostream& operator<<(std::ostream& os, HTTPStatus status)
{
    os << int(status) << ' ';
    switch (status) {
        case HTTPStatus::Unknown:
            return os << "Unknown Status";
        case HTTPStatus::Continue:
            return os << "Continue";
        case HTTPStatus::SwitchingProtocols:
            return os << "Switching Protocols";
        case HTTPStatus::Ok:
            return os << "OK";
        case HTTPStatus::Created:
            return os << "Created";
        case HTTPStatus::Accepted:
            return os << "Accepted";
        case HTTPStatus::NonAuthoritative:
            return os << "Non-Authoritative Information";
        case HTTPStatus::NoContent:
            return os << "No Content";
        case HTTPStatus::ResetContent:
            return os << "Reset Content";
        case HTTPStatus::PartialContent:
            return os << "Partial Content";
        case HTTPStatus::MultipleChoices:
            return os << "Multiple Choices";
        case HTTPStatus::MovedPermanently:
            return os << "Moved Permanently";
        case HTTPStatus::Found:
            return os << "Found";
        case HTTPStatus::SeeOther:
            return os << "See Other";
        case HTTPStatus::NotModified:
            return os << "Not Modified";
        case HTTPStatus::UseProxy:
            return os << "Use Proxy";
        case HTTPStatus::SwitchProxy:
            return os << "Switch Proxy";
        case HTTPStatus::TemporaryRedirect:
            return os << "Temporary Redirect";
        case HTTPStatus::PermanentRedirect:
            return os << "Permanent Redirect";
        case HTTPStatus::BadRequest:
            return os << "Bad Request";
        case HTTPStatus::Unauthorized:
            return os << "Unauthorized";
        case HTTPStatus::PaymentRequired:
            return os << "Payment Required";
        case HTTPStatus::Forbidden:
            return os << "Forbidden";
        case HTTPStatus::NotFound:
            return os << "Not Found";
        case HTTPStatus::MethodNotAllowed:
            return os << "Method Not Allowed";
        case HTTPStatus::NotAcceptable:
            return os << "Not Acceptable";
        case HTTPStatus::ProxyAuthenticationRequired:
            return os << "Proxy Authentication Required";
        case HTTPStatus::RequestTimeout:
            return os << "Request Timeout";
        case HTTPStatus::Conflict:
            return os << "Conflict";
        case HTTPStatus::Gone:
            return os << "Gone";
        case HTTPStatus::LengthRequired:
            return os << "Length Required";
        case HTTPStatus::PreconditionFailed:
            return os << "Precondition Failed";
        case HTTPStatus::PayloadTooLarge:
            return os << "Payload Too Large";
        case HTTPStatus::UriTooLong:
            return os << "URI Too Long";
        case HTTPStatus::UnsupportedMediaType:
            return os << "Unsupported Media Type";
        case HTTPStatus::RangeNotSatisfiable:
            return os << "Range Not Satisfiable";
        case HTTPStatus::ExpectationFailed:
            return os << "Expectation Failed";
        case HTTPStatus::ImATeapot:
            return os << "I'm A Teapot";
        case HTTPStatus::MisdirectedRequest:
            return os << "Misdirected Request";
        case HTTPStatus::UpgradeRequired:
            return os << "Upgrade Required";
        case HTTPStatus::PreconditionRequired:
            return os << "Precondition Required";
        case HTTPStatus::TooManyRequests:
            return os << "Too Many Requests";
        case HTTPStatus::RequestHeaderFieldsTooLarge:
            return os << "Request Header Fields Too Large";
        case HTTPStatus::UnavailableForLegalReasons:
            return os << "Unavailable For Legal Reasons";
        case HTTPStatus::InternalServerError:
            return os << "Internal Server Error";
        case HTTPStatus::NotImplemented:
            return os << "Not Implemented";
        case HTTPStatus::BadGateway:
            return os << "Bad Gateway";
        case HTTPStatus::ServiceUnavailable:
            return os << "Service Unavailable";
        case HTTPStatus::GatewayTimeout:
            return os << "Gateway Timeout";
        case HTTPStatus::HttpVersionNotSupported:
            return os << "HTTP Version not supported";
        case HTTPStatus::VariantAlsoNegotiates:
            return os << "Variant Also Negotiates";
        case HTTPStatus::NotExtended:
            return os << "Not Extended";
        case HTTPStatus::NetworkAuthenticationRequired:
            return os << "Network Authentication Required";
    }
    return os;
}


std::ostream& operator<<(std::ostream& os, const HTTPRequest& request)
{
    auto host = request.headers.find("Host");

    os << request.method << ' ';

    if (request.method == HTTPMethod::Connect) {
        REALM_ASSERT_RELEASE(host != request.headers.end());
        os << host->second;
    }
    else if (request.path.size() == 0) {
        os << '/';
    }
    else {
        os << request.path;
    }
    os << " HTTP/1.1\r\n";

    {
        os << "Host:";
        if (host != request.headers.end())
            os << " " << host->second;
        os << "\r\n";
    }

    for (auto& pair : request.headers) {
        if (pair.first == "Host")
            continue;
        // FIXME: No need for trimming here. There should be extra white space
        // when, and only when the application specifies it.
        StringData trimmed_value = trim_whitespace(pair.second);
        os << pair.first << ": " << trimmed_value << "\r\n";
    }
    os << "\r\n";
    if (request.body)
        os.write(request.body->data(), request.body->size());

    bool content_length_exists = request.headers.find("Content-Length") != request.headers.end();
    bool body_exists = bool(request.body);
    REALM_ASSERT(content_length_exists == body_exists);

    return os;
}


std::ostream& operator<<(std::ostream& os, const HTTPResponse& response)
{
    os << "HTTP/1.1 " << response.status;
    os << "\r\n";

    for (auto& pair : response.headers) {
        StringData trimmed_value = trim_whitespace(pair.second);
        os << pair.first << ": " << trimmed_value << "\r\n";
    }
    os << "\r\n";
    if (response.body) {
        os.write(response.body->data(), response.body->size());
    }

    return os;
}


std::error_code make_error_code(HTTPParserError error)
{
    return std::error_code(static_cast<int>(error), g_http_parser_error_category);
}

} // namespace util
} // namespace realm
