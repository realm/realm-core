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

#include <tightdb/util/features.h>

namespace tightdb {

class Exception: public std::exception {
public:
    /// message() returns the error description without version info.
    virtual const char* message() const TIGHTDB_NOEXCEPT_OR_NOTHROW = 0;

    /// version() returns the version of the TightDB library that threw this exception.
    const char* version() const TIGHTDB_NOEXCEPT_OR_NOTHROW;
};

class RuntimeError: public std::runtime_error {
public:
    /// RuntimeError prepends the contents of TIGHTDB_VER_CHUNK to the message,
    /// so that what() will contain information about the library version.
    /// Call message()
    RuntimeError(const std::string& message);
    RuntimeError(const RuntimeError& other);

    /// message() returns the error description without embedded release info.
    /// Default implementation has the precondition that what() returns a string
    /// that is prepended with the current release version.
    virtual const char* message() const TIGHTDB_NOEXCEPT_OR_NOTHROW;

    /// version() returns the version of the TightDB library that threw this exception.
    const char* version() const TIGHTDB_NOEXCEPT_OR_NOTHROW;
};

class ExceptionWithVersionInWhat: public Exception {
public:
    /// CAUTION: Deriving from this class means you guarantee that the string
    /// returned from what() contains TIGHTDB_VER_CHUNK + one space at the beginning.
    const char* message() const TIGHTDB_NOEXCEPT_OR_NOTHROW TIGHTDB_OVERRIDE;
};


/// Thrown by various functions to indicate that a specified table does not
/// exist.
class NoSuchTable: public ExceptionWithVersionInWhat {
public:
    const char* what() const TIGHTDB_NOEXCEPT_OR_NOTHROW TIGHTDB_OVERRIDE;
};


/// Thrown by various functions to indicate that a specified table name is
/// already in use.
class TableNameInUse: public ExceptionWithVersionInWhat {
public:
    const char* what() const TIGHTDB_NOEXCEPT_OR_NOTHROW TIGHTDB_OVERRIDE;
};


// Thrown by functions that require a table to **not** be the target of link
// columns, unless those link columns are part of the table itself.
class CrossTableLinkTarget: public ExceptionWithVersionInWhat {
public:
    const char* what() const TIGHTDB_NOEXCEPT_OR_NOTHROW TIGHTDB_OVERRIDE;
};


/// Thrown by various functions to indicate that the dynamic type of a table
/// does not match a particular other table type (dynamic or static).
class DescriptorMismatch: public ExceptionWithVersionInWhat {
public:
    const char* what() const TIGHTDB_NOEXCEPT_OR_NOTHROW TIGHTDB_OVERRIDE;
};


/// Reports errors that are a consequence of faulty logic within the program,
/// such as violating logical preconditions or class invariants, and can be
/// easily predicted.
class LogicError: public ExceptionWithVersionInWhat {
public:
    enum error_kind {
        string_too_big,
        binary_too_big,
        table_name_too_long,
        column_name_too_long,
        table_index_out_of_range,
        row_index_out_of_range,
        column_index_out_of_range,

        /// Indicates that an argument has a value that is illegal in combination
        /// with another argument, or with the state of an involved object.
        illegal_combination,

        /// Indicates a data type mismatch, such as when `Table::find_pkey_int()` is
        /// called and the type of the primary key is not `type_Int`.
        type_mismatch,

        /// Indicates that an involved table is of the wrong kind, i.e., if it is a
        /// subtable, and the function requires a root table.
        wrong_kind_of_table,

        /// Indicates that an involved accessor is was detached, i.e., was not
        /// attached to an underlying object.
        detached_accessor,

        // Indicates that an involved column lacks a search index.
        no_search_index,

        // Indicates that an involved table lacks a primary key.
        no_primary_key,

        // Indicates that an attempt was made to add a primary key to a table that
        // already had a primary key.
        has_primary_key,

        /// Indicates that a modification was attempted that would have produced a
        /// duplicate primary value.
        unique_constraint_violation
    };

    LogicError(error_kind message);

    const char* what() const TIGHTDB_NOEXCEPT_OR_NOTHROW TIGHTDB_OVERRIDE;

    static const char* get_message_for_error(error_kind) TIGHTDB_NOEXCEPT_OR_NOTHROW;
private:
    const char* m_message;
};


// Implementation:

inline LogicError::LogicError(LogicError::error_kind kind):
    m_message(get_message_for_error(kind))
{
}

inline const char* LogicError::what() const TIGHTDB_NOEXCEPT_OR_NOTHROW
{
    return m_message;
}


} // namespace tightdb

#endif // TIGHTDB_EXCEPTIONS_HPP
