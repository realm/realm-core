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

#pragma once

#include <realm/string_data.hpp>

namespace realm {

/*
 * TODO As part of RCORE-720, we'll move all the exception types and error codes we want to expose in the
 * public API here so that each one has a unique error code. For now though, this is here to complete the
 * API of Status/StatusWith.
 */
class ErrorCodes {
public:
    enum Error : int32_t {
        OK = 0,
        UnknownError = 1,
        RuntimeError = 2,
        LogicError = 3,
        BrokenPromise = 4,
    };

    static StringData error_string(Error code);
};

} // namespace realm
