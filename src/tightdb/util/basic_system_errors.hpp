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
#ifndef TIGHTDB_UTIL_BASIC_SYSTEM_ERRORS_HPP
#define TIGHTDB_UTIL_BASIC_SYSTEM_ERRORS_HPP

#include <cerrno>

#include <tightdb/util/error_code.hpp>

namespace tightdb {
namespace util {

namespace error {

enum basic_system_errors {
    /// Address family not supported by protocol.
    address_family_not_supported = EAFNOSUPPORT,

    /// Invalid argument.
    invalid_argument = EINVAL,

    /// Cannot allocate memory.
    no_memory = ENOMEM
};

error_code make_error_code(basic_system_errors);

} // namespace error


error_code make_basic_system_error_code(int);




// implementation

inline error_code make_basic_system_error_code(int err)
{
    using namespace error;
    return make_error_code(basic_system_errors(err));
}

} // namespace util
} // namespace tightdb

#endif // TIGHTDB_UTIL_BASIC_SYSTEM_ERRORS_HPP
