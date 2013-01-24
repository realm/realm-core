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


struct FileOpenError: std::runtime_error {
    FileOpenError(const std::string& msg): std::runtime_error(msg) {}
};

/// A specified file system path (or the directory prefix of a
/// specified file system path) was not found in the file system.
struct NoSuchFile: FileOpenError {
    NoSuchFile(): FileOpenError("No such file") {}
};

/// Lacking permissions or insufficient privileges.
struct PermissionDenied: FileOpenError {
    PermissionDenied(): FileOpenError("Permission denied") {}
};

/// Thrown by Group constructors if the specified file or memory
/// buffer does not appear to contain a valid TightDB database.
struct InvalidDatabase: std::runtime_error {
    InvalidDatabase(): std::runtime_error("Invalid database") {}
};


} // namespace tightdb

#endif // TIGHTDB_EXCEPTIONS_HPP
