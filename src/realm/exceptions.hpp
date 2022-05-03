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

#ifndef REALM_EXCEPTIONS_HPP
#define REALM_EXCEPTIONS_HPP

#include <stdexcept>
#include <system_error>

#include <realm/util/features.h>
#include <realm/status.hpp>

namespace realm {

class Exception : public std::exception {
public:
    const char* what() const noexcept final
    {
        return reason().c_str();
    }

    const Status& to_status() const
    {
        return m_status;
    }

    const std::string& reason() const noexcept
    {
        return m_status.reason();
    }

    ErrorCodes::Error code() const noexcept
    {
        return m_status.code();
    }

    std::string_view code_string() const noexcept
    {
        return m_status.code_string();
    }

    Exception(ErrorCodes::Error err, std::string_view str)
        : m_status(err, str)
    {
    }

    explicit Exception(Status status)
        : m_status(std::move(status))
    {
    }

private:
    Status m_status;
};

/*
 * This will convert an exception in a catch(...) block into a Status. For Exception's, it returns the
 * status held in the exception directly. Otherwise it returns a status with an UnknownError error code and a
 * reason string holding the exception type and message.
 *
 * Currently this works for exceptions that derive from std::exception or Exception only.
 */
Status exception_to_status() noexcept;


/// The UnsupportedFileFormatVersion exception is thrown by DB::open()
/// constructor when opening a database that uses a deprecated file format
/// and/or a deprecated history schema which this version of Realm cannot
/// upgrade from.
class UnsupportedFileFormatVersion : public Exception {
public:
    UnsupportedFileFormatVersion(int version)
        : Exception(ErrorCodes::UnsupportedFileFormatVersion,
                    util::format("Database has an unsupported version (%1) and cannot be upgraded", version))
        , source_version(version)
    {
    }
    /// The unsupported version of the file.
    int source_version = 0;
};


/// Thrown when a key is already existing when trying to create a new object
class KeyAlreadyUsed : public Exception {
public:
    KeyAlreadyUsed(const std::string& msg)
        : Exception(ErrorCodes::KeyAlreadyUsed, msg)
    {
    }
};

/// The \c LogicError exception class is intended to be thrown only when
/// applications (or bindings) violate rules that are stated (or ought to have
/// been stated) in the documentation of the public API, and only in cases
/// where the violation could have been easily and efficiently predicted by the
/// application. In other words, this exception class is for the cases where
/// the error is due to incorrect use of the public API.
///
/// This class is not supposed to be caught by applications. It is not even
/// supposed to be considered part of the public API, and therefore the
/// documentation of the public API should **not** mention the \c LogicError
/// exception class by name. Note how this contrasts with other exception
/// classes, such as \c NoSuchTable, which are part of the public API, and are
/// supposed to be mentioned in the documentation by name. The \c LogicError
/// exception is part of Realm's private API.
///
/// In other words, the \c LogicError class should exclusively be used in
/// replacement (or in addition to) asserts (debug or not) in order to
/// guarantee program interruption, while still allowing for complete
/// test-cases to be written and run.
///
/// To this effect, the special `CHECK_LOGIC_ERROR()` macro is provided as a
/// test framework plugin to allow unit tests to check that the functions in
/// the public API do throw \c LogicError when rules are violated.
///
/// The reason behind hiding this class from the public API is to prevent users
/// from getting used to the idea that "Undefined Behaviour" equates a specific
/// exception being thrown. The whole point of properly documenting "Undefined
/// Behaviour" cases is to help the user know what the limits are, without
/// constraining the database to handle every and any use-case thrown at it.
class LogicError : public Exception {
public:
    LogicError(ErrorCodes::Error code, const std::string& msg)
        : Exception(code, msg)
    {
        REALM_ASSERT(ErrorCodes::error_categories(code).test(ErrorCategory::logic_error));
    }
};

class RuntimeError : public Exception {
public:
    RuntimeError(ErrorCodes::Error code, const std::string& msg)
        : Exception(code, msg)
    {
        REALM_ASSERT(ErrorCodes::error_categories(code).test(ErrorCategory::runtime_error));
    }
};

/// Thrown when creating references that are too large to be contained in our ref_type (size_t)
class MaximumFileSizeExceeded : public RuntimeError {
public:
    MaximumFileSizeExceeded(const std::string& msg)
        : RuntimeError(ErrorCodes::MaximumFileSizeExceeded, msg)
    {
    }
    /// runtime_error::what() returns the msg provided in the constructor.
};

/// Thrown when writing fails because the disk is full.
class OutOfDiskSpace : public RuntimeError {
public:
    OutOfDiskSpace(const std::string& msg)
        : RuntimeError(ErrorCodes::OutOfDiskSpace, msg)
    {
    }
    /// runtime_error::what() returns the msg provided in the constructor.
};

/// Thrown when a sync agent attempts to join a session in which there is
/// already a sync agent. A session may only contain one sync agent at any given
/// time.
class MultipleSyncAgents : public RuntimeError {
public:
    MultipleSyncAgents()
        : RuntimeError(ErrorCodes::MultipleSyncAgents, "Multiple sync agents attempted to join the same session")
    {
    }
};


/// Thrown when memory can no longer be mapped to. When mmap/remap fails.
class AddressSpaceExhausted : public RuntimeError {
public:
    AddressSpaceExhausted(const std::string& msg)
        : RuntimeError(ErrorCodes::AddressSpaceExhausted, msg)
    {
    }
    /// runtime_error::what() returns the msg provided in the constructor.
};

class InvalidArgument : public LogicError {
public:
    InvalidArgument(ErrorCodes::Error code, const std::string& msg)
        : LogicError(code, msg)
    {
        REALM_ASSERT(ErrorCodes::error_categories(code).test(ErrorCategory::invalid_argument));
    }
};

class InvalidColumnKey : public InvalidArgument {
public:
    template <class T>
    InvalidColumnKey(const T& name)
        : InvalidArgument(ErrorCodes::InvalidProperty, util::format("Invalid property for object type %1", name))
    {
    }
    InvalidColumnKey()
        : InvalidArgument(ErrorCodes::InvalidProperty, "Invalid column key")
    {
    }
};

/// Thrown by various functions to indicate that a specified table does not
/// exist.
class NoSuchTable : public InvalidArgument {
public:
    NoSuchTable()
        : InvalidArgument(ErrorCodes::NoSuchTable, "No such table exists")
    {
    }
};

/// Thrown by various functions to indicate that a specified table name is
/// already in use.
class TableNameInUse : public InvalidArgument {
public:
    TableNameInUse()
        : InvalidArgument(ErrorCodes::TableNameInUse, "The specified table name is already in use")
    {
    }
};


/// Thrown when a key can not by found
class KeyNotFound : public InvalidArgument {
public:
    KeyNotFound(const std::string& msg)
        : InvalidArgument(ErrorCodes::KeyNotFound, msg)
    {
    }
};


class NotNullable : public InvalidArgument {
public:
    template <class T, class U>
    NotNullable(const T& object_type, const U& property_name)
        : InvalidArgument(ErrorCodes::PropertyNotNullable,
                          util::format("Property '%2' of class '%1' cannot be NULL", object_type, property_name))
    {
    }
};

class PropertyTypeMismatch : public InvalidArgument {
public:
    template <class T, class U>
    PropertyTypeMismatch(const T& object_type, const U& property_name)
        : InvalidArgument(ErrorCodes::TypeMismatch,
                          util::format("Type mismatch for property '%2' of class '%1'", object_type, property_name))
    {
    }
};

class OutOfBounds : public InvalidArgument {
public:
    OutOfBounds(const std::string& msg, size_t idx, size_t sz);
};

class InvalidEncryptionKey : public InvalidArgument {
public:
    InvalidEncryptionKey()
        : InvalidArgument(ErrorCodes::InvalidEncryptionKey, "Encryption key must be 64 bytes.")
    {
    }
};

class StaleAccessor : public LogicError {
public:
    StaleAccessor(const std::string& msg)
        : LogicError(ErrorCodes::StaleAccessor, msg)
    {
    }
};

class IllegalOperation : public LogicError {
public:
    IllegalOperation(const std::string& msg)
        : LogicError(ErrorCodes::IllegalOperation, msg)
    {
    }
};

class WrongTransactionState : public LogicError {
public:
    WrongTransactionState(const std::string& msg)
        : LogicError(ErrorCodes::WrongTransactionState, msg)
    {
    }
};

class InvalidTableRef : public LogicError {
public:
    InvalidTableRef(const char* cause)
        : LogicError(ErrorCodes::InvalidTableRef, cause)
    {
    }
};

class SerializationError : public LogicError {
public:
    SerializationError(const std::string& msg)
        : LogicError(ErrorCodes::SerializationError, msg)
    {
    }
};

class NotImplemented : public LogicError {
public:
    NotImplemented()
        : LogicError(ErrorCodes::IllegalOperation, "Not implemented")
    {
    }
};

class DuplicatePrimaryKeyValue : public RuntimeError {
public:
    DuplicatePrimaryKeyValue(const std::string& msg)
        : RuntimeError(ErrorCodes::DuplicatePrimaryKeyValue, msg)
    {
    }
};

class ObjectAlreadyExists : public RuntimeError {
public:
    template <class T, class U>
    ObjectAlreadyExists(const U& object_type, T pk_val)
        : RuntimeError(
              ErrorCodes::ObjectAlreadyExists,
              util::format("Attempting to create an object of type '%1' with an existing primary key value '%2'",
                           object_type, pk_val))
    {
    }
};

// Thrown by functions that require a table to **not** be the target of link
// columns, unless those link columns are part of the table itself.
class CrossTableLinkTarget : public LogicError {
public:
    template <class T>
    CrossTableLinkTarget(T table_name)
        : LogicError(ErrorCodes::CrossTableLinkTarget,
                     util::format("Cannot remove %1 that is target of outside links", table_name))
    {
    }
};


/// Used for any I/O related exception. Note the derived exception
/// types that are used for various specific types of errors.
class FileAccessError : public RuntimeError {
public:
    FileAccessError(ErrorCodes::Error code, const std::string& msg, const std::string& path, int err)
        : RuntimeError(code,
                       [&] {
                           if (path.empty())
                               return msg;
                           else
                               return msg + ", path: '" + path + "'";
                       }())
        , m_path(path)
        , m_errno(err)
    {
        REALM_ASSERT(ErrorCodes::error_categories(code).test(ErrorCategory::file_access));
    }

    /// Return the associated file system path, or the empty string if there is
    /// no associated file system path, or if the file system path is unknown.
    const std::string& get_path() const
    {
        return m_path;
    }
    int get_errno() const
    {
        return m_errno;
    }

private:
    std::string m_path;
    int m_errno;
};

class SystemError : public RuntimeError {
public:
    SystemError(std::error_code err, const std::string& msg)
        : RuntimeError(ErrorCodes::SystemError, msg)
        , m_error(err)
    {
    }

    SystemError(int err_no, const std::string& msg)
        : SystemError(std::error_code(err_no, std::system_category()), msg)
    {
    }

    std::error_code get_system_error() const
    {
        return m_error;
    }

private:
    std::error_code m_error;
};

namespace query_parser {

/// Exception thrown when parsing fails due to invalid syntax.
struct SyntaxError : InvalidArgument {
    SyntaxError(const std::string& msg)
        : InvalidArgument(ErrorCodes::SyntaxError, msg)
    {
    }
};

/// Exception thrown when binding a syntactically valid query string in a
/// context where it does not make sense.
struct InvalidQueryError : RuntimeError {
    InvalidQueryError(const std::string& msg)
        : RuntimeError(ErrorCodes::InvalidQuery, msg)
    {
    }
};

/// Exception thrown when there is a problem accessing the arguments in a query string
struct InvalidQueryArgError : InvalidArgument {
    InvalidQueryArgError(const std::string& msg)
        : InvalidArgument(ErrorCodes::InvalidQueryArg, msg)
    {
    }
};

} // namespace query_parser

} // namespace realm


#endif // REALM_EXCEPTIONS_HPP
