#include <algorithm>
#include <utility>
#include <stdexcept>
#include <cctype>
#include <sstream>
#include <iomanip>

#include <realm/util/assert.hpp>
#include <realm/util/uri.hpp>
#include <realm/util/backtrace.hpp>

using namespace realm;


// reserved    = gen-delims sub-delims
// gen-delims  = : / ? # [ ] @
// sub-delims  = ! $ & ' ( ) * + , ; =
// unreserved  = alpha digit - . _ ~
// scheme      = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
// host        = IP-literal / IPv4address / reg-name
// reg-name    = *( unreserved / pct-encoded / sub-delims )


util::Uri::Uri(const std::string& str)
{
    const char* b = str.data();
    const char* e = b + str.size();

    // Scheme
    {
        const char* c = ":/?#";
        const char* p = std::find_first_of(b, e, c, c + 4);
        if (p != e && *p == ':') {
            m_scheme.assign(b, ++p); // Throws
            b = p;
        }
    }

    // Authority
    if (2 <= e - b && b[0] == '/' && b[1] == '/') {
        const char* c = "/?#";
        const char* p = std::find_first_of(b + 2, e, c, c + 3);
        m_auth.assign(b, p); // Throws
        b = p;
    }

    // Path
    {
        const char* c = "?#";
        const char* p = std::find_first_of(b, e, c, c + 2);
        m_path.assign(b, p); // Throws
        b = p;
    }

    // Query
    {
        const char* p = std::find(b, e, '#');
        m_query.assign(b, p); // Throws
        b = p;
    }

    // Fragment
    m_frag.assign(b, e); // Throws
}


void util::Uri::set_scheme(const std::string& val)
{
    if (!val.empty()) {
        if (val.back() != ':')
            throw util::invalid_argument("URI scheme part must have a trailing ':'");
        if (val.substr(0, val.size() - 1).find_first_of(":/?#") != std::string::npos) {
            throw util::invalid_argument("URI scheme part must not contain '/', '?' or '#', "
                                         "nor may it contain more than one ':'");
        }
    }
    m_scheme = val;
}


void util::Uri::set_auth(const std::string& val)
{
    if (!val.empty()) {
        if (val.size() < 2 || val[0] != '/' || val[1] != '/')
            throw util::invalid_argument("URI authority part must have '//' as a prefix");
        if (val.find_first_of("/?#", 2) != std::string::npos) {
            throw util::invalid_argument("URI authority part must not contain '?' or '#', "
                                         "nor may it contain '/' beyond the two in the prefix");
        }
    }
    m_auth = val;
}


void util::Uri::set_path(const std::string& val)
{
    if (val.find_first_of("?#") != std::string::npos)
        throw util::invalid_argument("URI path part must not contain '?' or '#'");
    m_path = val;
}


void util::Uri::set_query(const std::string& val)
{
    if (!val.empty()) {
        if (val.front() != '?')
            throw util::invalid_argument("URI query string must have a leading '?'");
        if (val.find('#', 1) != std::string::npos)
            throw util::invalid_argument("URI query string must not contain '#'");
    }
    m_query = val;
}


void util::Uri::set_frag(const std::string& val)
{
    if (!val.empty() && val.front() != '#')
        throw util::invalid_argument("Fragment identifier must have a leading '#'");
    m_frag = val;
}


void util::Uri::canonicalize()
{
    if (m_scheme.size() == 1)
        m_scheme.clear();
    if (m_auth.size() == 2)
        m_auth.clear();
    if (m_path.empty() && (!m_scheme.empty() || !m_auth.empty()))
        m_path = '/';
    if (m_query.size() == 1)
        m_query.clear();
    if (m_frag.size() == 1)
        m_frag.clear();
}


bool util::Uri::get_auth(std::string& userinfo, std::string& host, std::string& port) const
{
    if (m_auth.empty())
        return false;

    std::string userinfo_2, host_2, port_2;
    using size_type = std::string::size_type;
    REALM_ASSERT(m_auth.size() >= 2);
    size_type i = 2;
    size_type j = m_auth.find('@', i);
    if (j != std::string::npos) {
        userinfo_2 = m_auth.substr(i, j - i); // Throws
        i = j + 1;
    }
    size_type k = m_auth.substr(i).rfind(':');
    if (k == std::string::npos) {
        k = m_auth.size();
    }
    else {
        k = i + k;
        port_2 = m_auth.substr(k + 1); // Throws
    }
    host_2 = m_auth.substr(i, k - i); // Throws

    userinfo = std::move(userinfo_2);
    host = std::move(host_2);
    port = std::move(port_2);
    return true;
}

namespace {

bool is_unreserved(unsigned char ch)
{
    return isalnum(ch) || ch == '-' || ch == '.' || ch == '_' || ch == '~';
}

int decode_char(unsigned char ch)
{
    if (ch >= '0' && ch <= '9')
        return ch - '0';
    if (ch >= 'A' && ch <= 'F')
        return 10 + (ch - 'A');
    if (ch >= 'a' && ch <= 'f')
        return 10 + (ch - 'a');
    return -1;
}

int decode_pair_of_char(unsigned char ch_1, unsigned char ch_2)
{
    int result_1 = decode_char(ch_1);
    if (result_1 == -1)
        return -1;
    int result_2 = decode_char(ch_2);
    if (result_2 == -1)
        return -1;
    return result_1 * 16 + result_2;
}

} // unnamed namespace

std::string util::uri_percent_encode(const std::string& unescaped)
{
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;

    for (size_t i = 0; i < unescaped.size(); ++i) {
        const unsigned char ch = static_cast<unsigned char>(unescaped[i]);
        if (is_unreserved(ch)) {
            escaped << ch;
        }
        else {
            escaped << std::uppercase;
            escaped << '%' << std::setw(2) << static_cast<int>(ch);
            escaped << std::nouppercase;
        }
    }

    return escaped.str();
}

std::string util::uri_percent_decode(const std::string& escaped)
{
    std::ostringstream unescaped;

    size_t pos = 0;
    while (pos < escaped.size()) {
        const unsigned char ch = static_cast<unsigned char>(escaped[pos]);
        if (ch == '%') {
            if (pos + 2 >= escaped.size())
                throw util::runtime_error("Invalid character in escaped string: " + escaped);
            unsigned char ch_1 = static_cast<unsigned char>(escaped[pos + 1]);
            unsigned char ch_2 = static_cast<unsigned char>(escaped[pos + 2]);
            int result = decode_pair_of_char(ch_1, ch_2);
            if (result == -1)
                throw util::runtime_error("Invalid character in escaped string: " + escaped);
            unescaped << static_cast<char>(result);
            pos += 3;
        }
        else if (is_unreserved(ch)) {
            unescaped << ch;
            ++pos;
        }
        else {
            throw util::runtime_error("Invalid character in escaped string: " + escaped);
        }
    }

    return unescaped.str();
}
