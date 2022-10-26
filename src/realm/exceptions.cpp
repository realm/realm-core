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

#include <realm/exceptions.hpp>

#include <realm/version.hpp>
#include <realm/util/to_string.hpp>
#include <realm/util/demangle.hpp>

namespace realm {

const char* Exception::what() const noexcept
{
    return reason().data();
}

const Status& Exception::to_status() const
{
    return m_status;
}

std::string_view Exception::reason() const noexcept
{
    return m_status.reason();
}

ErrorCodes::Error Exception::code() const noexcept
{
    return m_status.code();
}

std::string_view Exception::code_string() const noexcept
{
    return m_status.code_string();
}

Exception::Exception(ErrorCodes::Error err, std::string_view str)
    : m_status(err, str)
{
}

Exception::Exception(Status status)
    : m_status(std::move(status))
{
}

Status exception_to_status() noexcept
{
    try {
        throw;
    }
    catch (const Exception& e) {
        return e.to_status();
    }
    catch (const std::exception& e) {
        return Status(ErrorCodes::UnknownError,
                      util::format("Caught std::exception of type %1: %2", util::get_type_name(e), e.what()));
    }
    catch (...) {
        REALM_UNREACHABLE();
    }
}

UnsupportedFileFormatVersion::UnsupportedFileFormatVersion(int version)
    : Exception(ErrorCodes::UnsupportedFileFormatVersion,
                util::format("Database has an unsupported version (%1) and cannot be upgraded", version))
    , source_version(version)
{
}
UnsupportedFileFormatVersion::~UnsupportedFileFormatVersion() noexcept = default;


LogicError::LogicError(ErrorCodes::Error code, std::string_view msg)
    : Exception(code, msg)
{
    REALM_ASSERT(ErrorCodes::error_categories(code).test(ErrorCategory::logic_error));
}
LogicError::~LogicError() noexcept = default;


RuntimeError::RuntimeError(ErrorCodes::Error code, std::string_view msg)
    : Exception(code, msg)
{
    REALM_ASSERT(ErrorCodes::error_categories(code).test(ErrorCategory::runtime_error));
}
RuntimeError::~RuntimeError() noexcept = default;

InvalidArgument::InvalidArgument(std::string_view msg)
    : InvalidArgument(ErrorCodes::InvalidArgument, msg)
{
}

InvalidArgument::InvalidArgument(ErrorCodes::Error code, std::string_view msg)
    : LogicError(code, msg)
{
    REALM_ASSERT(ErrorCodes::error_categories(code).test(ErrorCategory::invalid_argument));
}
InvalidArgument::~InvalidArgument() noexcept = default;


OutOfBounds::OutOfBounds(std::string_view msg, size_t idx, size_t sz)
    : InvalidArgument(ErrorCodes::OutOfBounds,
                      sz == 0 ? util::format("Requested index %1 calling %2 when empty", idx, msg)
                              : util::format("Requested index %1 calling %2 when max is %3", idx, msg, sz - 1))
    , index(idx)
    , size(sz)
{
}
OutOfBounds::~OutOfBounds() noexcept = default;


FileAccessError::FileAccessError(ErrorCodes::Error code, std::string_view msg, std::string_view path, int err)
    : RuntimeError(code, msg)
    , m_path(path)
    , m_errno(err)
{
    REALM_ASSERT(ErrorCodes::error_categories(code).test(ErrorCategory::file_access));
}
FileAccessError::~FileAccessError() noexcept = default;

// Out-of-line virtual destructors for each of the exception types "anchors"
// the vtable and makes it so that it doesn't have to be emitted into each TU
// which uses the exception type
KeyAlreadyUsed::~KeyAlreadyUsed() noexcept = default;
MaximumFileSizeExceeded::~MaximumFileSizeExceeded() noexcept = default;
OutOfDiskSpace::~OutOfDiskSpace() noexcept = default;
MultipleSyncAgents::~MultipleSyncAgents() noexcept = default;
AddressSpaceExhausted::~AddressSpaceExhausted() noexcept = default;
InvalidColumnKey::~InvalidColumnKey() noexcept = default;
NoSuchTable::~NoSuchTable() noexcept = default;
TableNameInUse::~TableNameInUse() noexcept = default;
KeyNotFound::~KeyNotFound() noexcept = default;
NotNullable::~NotNullable() noexcept = default;
PropertyTypeMismatch::~PropertyTypeMismatch() noexcept = default;
InvalidEncryptionKey::~InvalidEncryptionKey() noexcept = default;
StaleAccessor::~StaleAccessor() noexcept = default;
IllegalOperation::~IllegalOperation() noexcept = default;
NoSubscriptionForWrite::~NoSubscriptionForWrite() noexcept = default;
WrongTransactionState::~WrongTransactionState() noexcept = default;
InvalidTableRef::~InvalidTableRef() noexcept = default;
SerializationError::~SerializationError() noexcept = default;
NotImplemented::~NotImplemented() noexcept = default;
MigrationFailed::~MigrationFailed() noexcept = default;
ObjectAlreadyExists::~ObjectAlreadyExists() noexcept = default;
CrossTableLinkTarget::~CrossTableLinkTarget() noexcept = default;
SystemError::~SystemError() noexcept = default;
query_parser::SyntaxError::~SyntaxError() noexcept = default;
query_parser::InvalidQueryError::~InvalidQueryError() noexcept = default;
query_parser::InvalidQueryArgError::~InvalidQueryArgError() noexcept = default;

} // namespace realm
