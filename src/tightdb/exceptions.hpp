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

#include <exception>

namespace tightdb {


/// Thrown by various functions to indicate that a specified argument is not
/// valid.
class InvalidArgument: public std::exception {
public:
    const char* what() const TIGHTDB_NOEXCEPT_OR_NOTHROW TIGHTDB_OVERRIDE
    {
        return "Invalid argument";
    }
};


/// Thrown by various functions to indicate that the dynamic type of a table
/// does not match a particular other table type (dynamic or static).
class DescriptorMismatch: public std::exception {
public:
    const char* what() const TIGHTDB_NOEXCEPT_OR_NOTHROW TIGHTDB_OVERRIDE
    {
        return "The specified table name is already in use";
    }
};


/// Thrown by various functions to indicate that a specified table name is
/// already in use.
class TableNameInUse: public std::exception {
public:
    const char* what() const TIGHTDB_NOEXCEPT_OR_NOTHROW TIGHTDB_OVERRIDE
    {
        return "The specified table name is already in use";
    }
};


} // namespace tightdb

#endif // TIGHTDB_EXCEPTIONS_HPP
