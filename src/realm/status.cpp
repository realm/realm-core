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

#include "realm/status.hpp"

#include "realm/util/demangle.hpp"

#include <iostream>

namespace realm {

Status::Status(ErrorCodes::Error code, StringData reason)
    : m_error(ErrorInfo::create(code, reason))
{
    // OK status should be created by calling Status::OK() - which is a special case that doesn't allocate
    // anything.
    REALM_ASSERT(code != ErrorCodes::OK);
}

Status::Status(ErrorCodes::Error code, const std::string& reason)
    : Status(code, StringData(reason))
{
}

Status::Status(ErrorCodes::Error code, const char* reason)
    : Status(code, StringData(reason, std::char_traits<char>::length(reason)))
{
}

Status::ErrorInfo::ErrorInfo(ErrorCodes::Error code, StringData reason)
    : m_refs(0)
    , m_code(code)
    , m_reason(reason)
{
}

util::bind_ptr<Status::ErrorInfo> Status::ErrorInfo::create(ErrorCodes::Error code, StringData reason)
{
    return util::bind_ptr<Status::ErrorInfo>(new ErrorInfo(code, reason));
}

std::ostream& operator<<(std::ostream& out, const Status& val)
{
    out << val.code_string() << ": " << val.reason();
    return out;
}

Status exception_to_status() noexcept
{
    try {
        throw;
    }
    catch (const ExceptionForStatus& e) {
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

} // namespace realm
