#include <algorithm>
#include <utility>
#include <stdexcept>

#include <realm/util/assert.hpp>
#include <realm/util/uri.hpp>

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
        const char* p = std::find_first_of(b, e, c, c+4);
        if (p != e && *p == ':') {
            m_scheme.assign(b, ++p); // Throws
            b = p;
        }
    }

    // Authority
    if (2 <= e-b && b[0] == '/' && b[1] == '/') {
        const char* c = "/?#";
        const char* p = std::find_first_of(b+2, e, c, c+3);
        m_auth.assign(b, p); // Throws
        b = p;
    }

    // Path
    {
        const char* c = "?#";
        const char* p = std::find_first_of(b, e, c, c+2);
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
            throw std::invalid_argument("URI scheme part must have a trailing ':'");
        if (val.substr(0, val.size()-1).find_first_of(":/?#") != std::string::npos) {
            throw std::invalid_argument("URI scheme part must not contain '/', '?' or '#', "
                                        "nor may it contain more than one ':'");
        }
    }
    m_scheme = val;
}


void util::Uri::set_auth(const std::string& val)
{
    if (!val.empty()) {
        if (val.size() < 2 || val[0] != '/' || val[1] != '/')
            throw std::invalid_argument("URI authority part must have '//' as a prefix");
        if (val.find_first_of("/?#", 2) != std::string::npos) {
            throw std::invalid_argument("URI authority part must not contain '?' or '#', "
                                        "nor may it contain '/' beyond the two in the prefix");
        }
    }
    m_auth = val;
}


void util::Uri::set_path(const std::string& val)
{
    if (val.find_first_of("?#") != std::string::npos)
        throw std::invalid_argument("URI path part must not contain '?' or '#'");
    m_path = val;
}


void util::Uri::set_query(const std::string& val)
{
    if (!val.empty()) {
        if (val.front() != '?')
            throw std::invalid_argument("URI query string must have a leading '?'");
        if (val.find('#', 1) != std::string::npos)
            throw std::invalid_argument("URI query string must not contain '#'");
    }
    m_query = val;
}


void util::Uri::set_frag(const std::string& val)
{
    if (!val.empty() && val.front() != '#')
        throw std::invalid_argument("Fragment identifier must have a leading '#'");
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
        userinfo_2 = m_auth.substr(i, j-i); // Throws
        i = j + 1;
    }
    size_type k = m_auth.substr(i).rfind(':');
    if (k == std::string::npos) {
        k = m_auth.size();
    }
    else {
        k = i + k;
        port_2 = m_auth.substr(k+1); // Throws
    }
    host_2 = m_auth.substr(i, k-i); // Throws

    userinfo = std::move(userinfo_2);
    host     = std::move(host_2);
    port     = std::move(port_2);
    return true;
}
