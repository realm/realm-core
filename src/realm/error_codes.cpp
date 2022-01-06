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

std::string_view ErrorCodes::error_string(Error code)
{
    switch (code) {
        case ErrorCodes::OK:
            return "OK";
        case ErrorCodes::RuntimeError:
            return "RuntimeError";
        case ErrorCodes::LogicError:
            return "LogicError";
        case ErrorCodes::BrokenPromise:
            return "BrokenPromise";
        case ErrorCodes::NoSuchTable:
            return "NoSuchTable";
        case ErrorCodes::MaximumFileSizeExceeded:
            return "MaximumFileSizeExceeded";
        case ErrorCodes::ColumnNotFound:
            return "ColumnNotFound";
        case ErrorCodes::KeyNotFound:
            return "KeyNotFound";
        case ErrorCodes::KeyAlreadyUsed:
            return "KeyAlreadyUsed";
        case ErrorCodes::DuplicatePrimaryKeyValue:
            return "DuplicatePrimaryKeyValue";
        case ErrorCodes::InvalidPath:
            return "InvalidPath";
        case ErrorCodes::UnknownError:
        default:
            return "UnknownError";
    }
}

} // namespace realm
