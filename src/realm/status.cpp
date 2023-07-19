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

#include <iostream>

namespace realm {

Status::ErrorInfo::ErrorInfo(ErrorCodes::Error code, std::string&& reason)
    : m_refs(0)
    , m_code(code)
    , m_reason(std::move(reason))
{
}

util::bind_ptr<Status::ErrorInfo> Status::ErrorInfo::create(ErrorCodes::Error code, std::string&& reason)
{
    // OK status should be created by calling Status::OK() - which is a special case that doesn't allocate
    // anything.
    REALM_ASSERT(code != ErrorCodes::OK);

    return util::bind_ptr<Status::ErrorInfo>(new ErrorInfo(code, std::move(reason)));
}

std::ostream& operator<<(std::ostream& out, const Status& val)
{
    out << val.code_string() << ": " << val.reason();
    return out;
}

} // namespace realm
