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

#ifndef TIGHTDB_EXCEPTIONS_HPP
#define TIGHTDB_EXCEPTIONS_HPP

#include <stdexcept>

namespace tightdb {


/// Thrown by various TightDb functions and methods if necessary
/// system resources could not be allocated. Memory allocation errors,
/// specifically, are generally reported by throwing std::bad_alloc.
struct ResourceAllocError: std::runtime_error {
    ResourceAllocError(const std::string& msg): std::runtime_error(msg) {}
};


// Thrown by functions that require a table to **not** be the target of link
// columns, unless those link columns are part of the table itself.
class CrossTableLinkTarget: public std::exception {
public:
    const char* what() const TIGHTDB_NOEXCEPT_OR_NOTHROW TIGHTDB_OVERRIDE
    {
        return "Table is target of cross-table link columns";
    }
};

} // namespace tightdb

#endif // TIGHTDB_EXCEPTIONS_HPP
