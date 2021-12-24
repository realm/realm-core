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


namespace realm {

namespace {
// You can think of this namespace as a compile-time map<ErrorCodes::Error, ErrorExtraInfoParser*>.
namespace parsers {
} // namespace parsers
} // namespace


StringData ErrorCodes::error_string(Error code)
{
    static_assert(sizeof(Error) == sizeof(int));

    switch (code) {
        case OK:
            return "OK";
        case UnknownError:
            return "UnknownError";
        case RuntimeError:
            return "RuntimeError";
        case LogicError:
            return "LogicError";
        case BrokenPromise:
            return "BrokenPromise";
        case InvalidArgument:
            return "InvalidArgument";
        case OutOfMemory:
            return "OutOfMemory";
        case NoSuchTable:
            return "NoSuchTable";
        case NoSuchObject:
            return "NoSuchObject";
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
        case ColumnNotFound:
            return "ColumnNotFound";
        case ColumnExistsAlready:
            return "ColumnExistsAlready";
        case KeyAlreadyUsed:
            return "KeyAlreadyUsed";
        case SerializationError:
            return "SerializationError";
        case InvalidPathError:
            return "InvalidPathError";
        case DuplicatePrimaryKeyValue:
            return "DuplicatePrimaryKeyValue";
        case InvalidQueryString:
            return "InvalidQueryString";
        case InvalidQuery:
            return "InvalidQuery";
        case NotInATransaction:
            return "NotInATransaction";
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
        case WrongPrimaryKeyType:
            return "WrongPrimaryKeyType";
        case ModifyPrimaryKey:
            return "ModifyPrimaryKey";
        case ReadOnlyProperty:
            return "ReadOnlyProperty";
        case PropertyNotNullable:
            return "PropertyNotNullable";
        default:
            return "UnknownError";
    }
}

ErrorCodes::Error ErrorCodes::from_string(StringData name)
{
    if (name == StringData("OK"))
        return OK;
    if (name == StringData("UnknownError"))
        return UnknownError;
    if (name == StringData("RuntimeError"))
        return RuntimeError;
    if (name == StringData("LogicError"))
        return LogicError;
    if (name == StringData("BrokenPromise"))
        return BrokenPromise;
    if (name == StringData("InvalidArgument"))
        return InvalidArgument;
    if (name == StringData("OutOfMemory"))
        return OutOfMemory;
    if (name == StringData("NoSuchTable"))
        return NoSuchTable;
    if (name == StringData("NoSuchObject"))
        return NoSuchObject;
    if (name == StringData("CrossTableLinkTarget"))
        return CrossTableLinkTarget;
    if (name == StringData("UnsupportedFileFormatVersion"))
        return UnsupportedFileFormatVersion;
    if (name == StringData("MultipleSyncAgents"))
        return MultipleSyncAgents;
    if (name == StringData("AddressSpaceExhausted"))
        return AddressSpaceExhausted;
    if (name == StringData("OutOfDiskSpace"))
        return OutOfDiskSpace;
    if (name == StringData("KeyNotFound"))
        return KeyNotFound;
    if (name == StringData("ColumnNotFound"))
        return ColumnNotFound;
    if (name == StringData("ColumnExistsAlready"))
        return ColumnExistsAlready;
    if (name == StringData("KeyAlreadyUsed"))
        return KeyAlreadyUsed;
    if (name == StringData("SerializationError"))
        return SerializationError;
    if (name == StringData("InvalidPathError"))
        return InvalidPathError;
    if (name == StringData("DuplicatePrimaryKeyValue"))
        return DuplicatePrimaryKeyValue;
    if (name == StringData("InvalidQueryString"))
        return InvalidQueryString;
    if (name == StringData("InvalidQuery"))
        return InvalidQuery;
    if (name == StringData("NotInATransaction"))
        return NotInATransaction;
    if (name == StringData("WrongThread"))
        return WrongThread;
    if (name == StringData("InvalidatedObject"))
        return InvalidatedObject;
    if (name == StringData("InvalidProperty"))
        return InvalidProperty;
    if (name == StringData("MissingPrimaryKey"))
        return MissingPrimaryKey;
    if (name == StringData("UnexpectedPrimaryKey"))
        return UnexpectedPrimaryKey;
    if (name == StringData("WrongPrimaryKeyType"))
        return WrongPrimaryKeyType;
    if (name == StringData("ModifyPrimaryKey"))
        return ModifyPrimaryKey;
    if (name == StringData("ReadOnlyProperty"))
        return ReadOnlyProperty;
    if (name == StringData("PropertyNotNullable"))
        return PropertyNotNullable;
    return UnknownError;
}

std::ostream& operator<<(std::ostream& stream, ErrorCodes::Error code)
{
    return stream << ErrorCodes::error_string(code);
}

} // namespace realm
