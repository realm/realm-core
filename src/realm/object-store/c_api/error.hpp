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

#include <string>

#include <realm.h>
#include <realm/util/optional.hpp>

namespace realm::c_api {

void set_last_exception(std::exception_ptr eptr);

class ErrorStorage {
public:
    static ErrorStorage* get_thread_local();

    ErrorStorage() = default;
    explicit ErrorStorage(std::exception_ptr eptr) noexcept;
    ErrorStorage(const ErrorStorage& other);
    ErrorStorage& operator=(const ErrorStorage& other);

    ErrorStorage(ErrorStorage&& other);
    ErrorStorage& operator=(ErrorStorage&& other);

    bool operator==(const ErrorStorage& other) const noexcept;

    void assign(std::exception_ptr eptr) noexcept;
    bool has_error() const noexcept;
    bool get_as_realm_error_t(realm_error_t* out) const noexcept;
    bool clear() noexcept;

    void set_usercode_error(void* usercode_error);
    void* get_and_clear_usercode_error();

private:
    util::Optional<realm_error_t> m_err;
    std::string m_message_buf;
    void* m_usercode_error;
};

} // namespace realm::c_api
