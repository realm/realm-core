/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2016] Realm Inc
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
#include "realm/util/to_string.hpp"

#include <iomanip>
#include <locale>
#include <sstream>

namespace realm {
namespace util {

void Printable::print(std::ostream &out, bool quote) const
{
    switch (m_type) {
        case Printable::Type::Bool:
            out << (m_uint ? "true" : "false");
            break;
        case Printable::Type::Uint:
            out << m_uint;
            break;
        case Printable::Type::Int:
            out << m_int;
            break;
        case Printable::Type::String:
            if (quote) {
#if __cplusplus >= 201402L
                out << std::quoted(m_string);
#else
                out << '"' << m_string << '"';
#endif
            }
            else
                out << m_string;
            break;
    }
}

void Printable::print_all(std::ostream& out, std::initializer_list<Printable>& values, bool quote)
{
    if (values.size() == 0)
        return;

    bool is_first = true;
    out << " [";
    for (auto&& value : values) {
        if (!is_first)
            out << ", ";
        is_first = false;
        value.print(out, quote);

    }
    out << "]";
}

std::string Printable::str() const
{
    std::ostringstream ss;
    ss.imbue(std::locale::classic());
    ss.exceptions(std::ios_base::failbit | std::ios_base::badbit);
    print(ss, true);
    return ss.str();
}

} // namespace util
} // namespace realm
