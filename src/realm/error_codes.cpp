/*************************************************************************
 *
 * Copyright 2021 Realm Inc.
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

#include "realm/error_codes.hpp"
#include <iostream>

namespace realm {

ErrorCategory ErrorCodes::error_categories(Error code)
{
    switch (code) {
        case FileOperationFailed:
        case PermissionDenied:
        case FileNotFound:
        case FileAlreadyExists:
        case InvalidDatabase:
        case DecryptionFailed:
        case IncompatibleHistories:
        case FileFormatUpgradeRequired:
            return ErrorCategory().set(ErrorCategory::runtime_error).set(ErrorCategory::file_access);
        case SystemError:
            return ErrorCategory().set(ErrorCategory::runtime_error).set(ErrorCategory::system_error);
        case RuntimeError:
        case RangeError:
        case IncompatibleSession:
        case IncompatibleLockFile:
        case InvalidQuery:
        case BrokenInvariant:
        case DuplicatePrimaryKeyValue:
        case OutOfMemory:
        case UnsupportedFileFormatVersion:
        case MultipleSyncAgents:
        case AddressSpaceExhausted:
        case ObjectAlreadyExists:
        case OutOfDiskSpace:
        case CallbackFailed:
        case NotCloneable:
            return ErrorCategory().set(ErrorCategory::runtime_error);
        case InvalidArgument:
        case PropertyNotNullable:
        case InvalidProperty:
        case InvalidName:
        case InvalidDictionaryValue:
        case InvalidSortDescriptor:
        case SyntaxError:
        case InvalidQueryArg:
        case KeyNotFound:
        case OutOfBounds:
        case LimitExceeded:
        case UnexpectedPrimaryKey:
        case ModifyPrimaryKey:
        case ReadOnlyProperty:
        case TypeMismatch:
        case MissingPrimaryKey:
        case MissingPropertyValue:
        case NoSuchTable:
            return ErrorCategory().set(ErrorCategory::invalid_argument).set(ErrorCategory::logic_error);
        case LogicError:
        case BrokenPromise:
        case CrossTableLinkTarget:
        case KeyAlreadyUsed:
        case WrongTransactionState:
        case SerializationError:
        case IllegalOperation:
        case StaleAccessor:
        case ReadOnly:
        case InvalidatedObject:
        case NotSuported:
            return ErrorCategory().set(ErrorCategory::logic_error);
        case OK:
        case UnknownError:
            break;
        default:
            break;
    }
    return {};
}

std::string_view ErrorCodes::error_string(Error code)
{
    static_assert(sizeof(Error) == sizeof(int));

    switch (code) {
        case OK:
            return "OK";
        case UnknownError:
            return "UnknownError";
        case RangeError:
            return "RangeError";
        case TypeMismatch:
            return "TypeMismatch";
        case BrokenPromise:
            return "BrokenPromise";
        case InvalidName:
            return "InvalidName";
        case OutOfMemory:
            return "OutOfMemory";
        case NoSuchTable:
            return "NoSuchTable";
        case CrossTableLinkTarget:
            return "CrossTableLinkTarget";
        case UnsupportedFileFormatVersion:
            return "UnsupportedFileFormatVersion";
        case MultipleSyncAgents:
            return "MultipleSyncAgents";
        case AddressSpaceExhausted:
            return "AddressSpaceExhausted";
        case OutOfDiskSpace:
            return "OutOfDiskSpace";
        case KeyNotFound:
            return "KeyNotFound";
        case OutOfBounds:
            return "OutOfBounds";
        case IllegalOperation:
            return "IllegalOperation";
        case KeyAlreadyUsed:
            return "KeyAlreadyUsed";
        case SerializationError:
            return "SerializationError";
        case InvalidPath:
            return "InvalidPath";
        case DuplicatePrimaryKeyValue:
            return "DuplicatePrimaryKeyValue";
        case SyntaxError:
            return "SyntaxError";
        case InvalidQueryArg:
            return "InvalidQueryArg";
        case InvalidQuery:
            return "InvalidQuery";
        case WrongTransactionState:
            return "WrongTransactioState";
        case WrongThread:
            return "WrongThread";
        case InvalidatedObject:
            return "InvalidatedObject";
        case InvalidProperty:
            return "InvalidProperty";
        case MissingPrimaryKey:
            return "MissingPrimaryKey";
        case UnexpectedPrimaryKey:
            return "UnexpectedPrimaryKey";
        case ObjectAlreadyExists:
            return "ObjectAlreadyExists";
        case ModifyPrimaryKey:
            return "ModifyPrimaryKey";
        case ReadOnly:
            return "ReadOnly";
        case PropertyNotNullable:
            return "PropertyNotNullable";
        case MaximumFileSizeExceeded:
            return "MaximumFileSizeExceeded";
        case TableNameInUse:
            return "TableNameInUse";
        case InvalidTableRef:
            return "InvalidTableRef";
        case BadChangeset:
            return "BadChangeset";
        case InvalidDictionaryKey:
            return "InvalidDictionaryKey";
        case InvalidDictionaryValue:
            return "InvalidDictionaryValue";
        case StaleAccessor:
            return "StaleAccessor";
        case IncompatibleLockFile:
            return "IncompatibleLockFile";
        case InvalidSortDescriptor:
            return "InvalidSortDescriptor";
        case DecryptionFailed:
            return "DecryptionFailed";
        case IncompatibleSession:
            return "IncompatibleSession";
        default:
            return "UnknownError";
    }
}

ErrorCodes::Error ErrorCodes::from_string(std::string_view name)
{
    if (name == std::string_view("OK"))
        return OK;
    if (name == std::string_view("UnknownError"))
        return UnknownError;
    if (name == std::string_view("RangeError"))
        return RangeError;
    if (name == std::string_view("TypeMismatch"))
        return TypeMismatch;
    if (name == std::string_view("BrokenPromise"))
        return BrokenPromise;
    if (name == std::string_view("InvalidName"))
        return InvalidName;
    if (name == std::string_view("OutOfMemory"))
        return OutOfMemory;
    if (name == std::string_view("NoSuchTable"))
        return NoSuchTable;
    if (name == std::string_view("CrossTableLinkTarget"))
        return CrossTableLinkTarget;
    if (name == std::string_view("UnsupportedFileFormatVersion"))
        return UnsupportedFileFormatVersion;
    if (name == std::string_view("MultipleSyncAgents"))
        return MultipleSyncAgents;
    if (name == std::string_view("AddressSpaceExhausted"))
        return AddressSpaceExhausted;
    if (name == std::string_view("OutOfDiskSpace"))
        return OutOfDiskSpace;
    if (name == std::string_view("KeyNotFound"))
        return KeyNotFound;
    if (name == std::string_view("OutOfBounds"))
        return OutOfBounds;
    if (name == std::string_view("IllegalOperation"))
        return IllegalOperation;
    if (name == std::string_view("KeyAlreadyUsed"))
        return KeyAlreadyUsed;
    if (name == std::string_view("SerializationError"))
        return SerializationError;
    if (name == std::string_view("InvalidPath"))
        return InvalidPath;
    if (name == std::string_view("DuplicatePrimaryKeyValue"))
        return DuplicatePrimaryKeyValue;
    if (name == std::string_view("SyntaxError"))
        return SyntaxError;
    if (name == std::string_view("InvalidQueryArg"))
        return InvalidQueryArg;
    if (name == std::string_view("InvalidQuery"))
        return InvalidQuery;
    if (name == std::string_view("WrongTransactioState"))
        return WrongTransactionState;
    if (name == std::string_view("WrongThread"))
        return WrongThread;
    if (name == std::string_view("InvalidatedObject"))
        return InvalidatedObject;
    if (name == std::string_view("InvalidProperty"))
        return InvalidProperty;
    if (name == std::string_view("MissingPrimaryKey"))
        return MissingPrimaryKey;
    if (name == std::string_view("UnexpectedPrimaryKey"))
        return UnexpectedPrimaryKey;
    if (name == std::string_view("ObjectAlreadyExists"))
        return ObjectAlreadyExists;
    if (name == std::string_view("ModifyPrimaryKey"))
        return ModifyPrimaryKey;
    if (name == std::string_view("ReadOnly"))
        return ReadOnly;
    if (name == std::string_view("PropertyNotNullable"))
        return PropertyNotNullable;
    if (name == std::string_view("MaximumFileSizeExceeded"))
        return MaximumFileSizeExceeded;
    if (name == std::string_view("TableNameInUse"))
        return TableNameInUse;
    if (name == std::string_view("InvalidTableRef"))
        return InvalidTableRef;
    if (name == std::string_view("BadChangeset"))
        return BadChangeset;
    if (name == std::string_view("InvalidDictionaryKey"))
        return InvalidDictionaryKey;
    if (name == std::string_view("InvalidDictionaryValue"))
        return InvalidDictionaryValue;
    if (name == std::string_view("StaleAccessor"))
        return StaleAccessor;
    if (name == std::string_view("IncompatibleLockFile"))
        return IncompatibleLockFile;
    if (name == std::string_view("InvalidSortDescriptor"))
        return InvalidSortDescriptor;
    if (name == std::string_view("DecryptionFailed"))
        return DecryptionFailed;
    if (name == std::string_view("IncompatibleSession"))
        return IncompatibleSession;
    return UnknownError;
}

std::ostream& operator<<(std::ostream& stream, ErrorCodes::Error code)
{
    return stream << ErrorCodes::error_string(code);
}

} // namespace realm
