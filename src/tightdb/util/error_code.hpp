/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_UTIL_ERROR_CODE_HPP
#define TIGHTDB_UTIL_ERROR_CODE_HPP

#include <string>
#include <ostream>

#include <tightdb/util/features.h>

namespace tightdb {
namespace util {

class error_category;


class error_code {
public:
    error_code();
    error_code(int value, const error_category&);
    template<class error_enum> error_code(error_enum);
    ~error_code() TIGHTDB_NOEXCEPT {}

    int value() const;
    const class error_category& category() const;
    std::string message() const;

    friend bool operator==(const error_code&, const error_code&);
    friend bool operator!=(const error_code&, const error_code&);

private:
    struct unspecified_bool_type_t {};
    typedef void (*unspecified_bool_type)(unspecified_bool_type_t);
    static void unspecified_bool_true(unspecified_bool_type_t) {}

public:
    operator unspecified_bool_type() const;

private:
    int m_value;
    const class error_category* m_category;
};


class error_category {
public:
    virtual const char* name() const = 0;

    virtual std::string message(int value) const = 0;

    virtual ~error_category() TIGHTDB_NOEXCEPT {}
};


template<class C, class T>
std::basic_ostream<C,T>& operator<<(std::basic_ostream<C,T>& out, const error_code& ec)
{
    out << ec.category().name() << ':' << ec.value();
    return out;
}


namespace error {

enum misc_errors {
    unknown = 1
};

error_code make_error_code(misc_errors);

} // namespace error




// Implementation

inline error_code::error_code():
    m_value(0),
    m_category(0)
{
}

inline error_code::error_code(int value, const class error_category& cat):
    m_value(value),
    m_category(&cat)
{
}

template<class error_enum> inline error_code::error_code(error_enum err)
{
    error_code ec = make_error_code(err);
    m_value    = ec.m_value;
    m_category = ec.m_category;
}

inline int error_code::value() const
{
    return m_value;
}

inline const class error_category& error_code::category() const
{
    return *m_category;
}

inline std::string error_code::message() const
{
    return m_category->message(m_value);
}

inline bool operator==(const error_code& a, const error_code& b)
{
    return a.m_value == b.m_value && a.m_category == b.m_category;
}

inline bool operator!=(const error_code& a, const error_code& b)
{
    return a.m_value != b.m_value || a.m_category != b.m_category;
}

inline error_code::operator unspecified_bool_type() const
{
    return m_value != 0 ? &error_code::unspecified_bool_true : 0;
}


} // namespace util
} // namespace tightdb

#endif // TIGHTDB_UTIL_ERROR_CODE_HPP
