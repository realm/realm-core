/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/util/to_string.hpp>

#include <realm/util/assert.hpp>
#include <realm/string_data.hpp>

#include <cstring>
#include <iomanip>
#include <locale>
#include <sstream>

namespace {
std::locale locale_classic = std::locale::classic();

template <typename T, typename = decltype(std::quoted(std::declval<T>()))>
void quoted(std::ostream& out, T&& str, int)
{
    out << std::quoted(str);
}
template <typename T>
void quoted(std::ostream& out, T&& str, ...)
{
    out << '"' << str << '"';
}
}

namespace realm {
namespace util {

Printable::Printable(StringData value)
    : m_type(Type::String)
{
    if (value.is_null()) {
        m_string = "<null>";
    }
    else {
        m_string = std::string_view(value.data(), value.size());
    }
}

void Printable::print(std::ostream& out, bool quote) const
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
        case Printable::Type::Double:
            out << m_double;
            break;
        case Printable::Type::String:
            if (quote)
                quoted(out, m_string, 0);
            else
                out << m_string;
            break;
        case Printable::Type::Callback:
            m_callback.fn(out, m_callback.data);
            break;
    }
}

void Printable::print_all(std::ostream& out, const std::initializer_list<Printable>& values, bool quote)
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
    ss.imbue(locale_classic);
    ss.exceptions(std::ios_base::failbit | std::ios_base::badbit);
    print(ss, true);
    return ss.str();
}

void format(std::ostream& os, const char* fmt, std::initializer_list<Printable> values)
{
    while (*fmt) {
        auto next = strchr(fmt, '%');

        // emit the rest of the format string if there are no more percents
        if (!next) {
            os << fmt;
            break;
        }

        // emit everything up to the next percent
        os.write(fmt, next - fmt);
        ++next;
        REALM_ASSERT(*next);

        // %% produces a single escaped %
        if (*next == '%') {
            os << '%';
            fmt = next + 1;
            continue;
        }
        REALM_ASSERT(isdigit(*next));

        // The const_cast is safe because stroul does not actually modify
        // the pointed-to string, but it lacks a const overload
        auto index = strtoul(next, const_cast<char**>(&fmt), 10) - 1;
        REALM_ASSERT(index < values.size());
        (values.begin() + index)->print(os, false);
    }
}

std::string format(const char* fmt, std::initializer_list<Printable> values)
{
    std::stringstream ss;
    ss.imbue(locale_classic);
    format(ss, fmt, values);
    return ss.str();
}

} // namespace util
} // namespace realm
