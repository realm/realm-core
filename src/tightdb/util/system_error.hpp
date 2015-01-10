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
#ifndef TIGHTDB_UTIL_SYSTEM_ERROR_HPP
#define TIGHTDB_UTIL_SYSTEM_ERROR_HPP

#include <exception>
#include <string>

#include <tightdb/util/features.h>
#include <tightdb/util/error_code.hpp>
#include <tightdb/util/unique_ptr.hpp>

namespace tightdb {
namespace util {


class system_error: public std::exception {
public:
    system_error(error_code);

    error_code code() const;

    const char* what() const TIGHTDB_NOEXCEPT_OR_NOTHROW TIGHTDB_OVERRIDE;

    system_error(const system_error&);

    system_error& operator=(const system_error&);

    ~system_error() TIGHTDB_NOEXCEPT_OR_NOTHROW {}

private:
    util::error_code m_code;
    mutable UniquePtr<std::string> m_what;
};



// Implementation

inline system_error::system_error(error_code err):
    m_code(err)
{
}

inline error_code system_error::code() const
{
    return m_code;
}

inline const char* system_error::what() const TIGHTDB_NOEXCEPT_OR_NOTHROW
{
    if (!m_what)
        m_what.reset(new std::string(m_code.message()));
    return m_what->c_str();
}

inline system_error::system_error(const system_error& err):
    std::exception(err),
    m_code(err.m_code)
{
}

inline system_error& system_error::operator=(const system_error& err)
{
    m_code = err.m_code;
    m_what.reset();
    return *this;
}


} // namespace util
} // namespace tightdb

#endif // TIGHTDB_UTIL_SYSTEM_ERROR_HPP
