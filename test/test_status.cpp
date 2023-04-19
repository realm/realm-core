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

#include "test.hpp"

#include "realm/status.hpp"
#include "realm/status_with.hpp"

namespace realm {
namespace {

TEST(Status)
{
    auto ok_status = Status::OK();
    CHECK_EQUAL(ok_status.code(), ErrorCodes::OK);
    CHECK(ok_status.is_ok());
    auto code_string = ErrorCodes::error_string(ok_status.code());
    CHECK_EQUAL(ok_status.code_string(), code_string);
    CHECK_EQUAL(ErrorCodes::from_string(code_string), ErrorCodes::OK);
    CHECK(ok_status.reason().empty());

    const auto err_status_reason = "runtime error 1";
    auto err_status = Status(ErrorCodes::RuntimeError, err_status_reason);
    CHECK_EQUAL(err_status.code(), ErrorCodes::RuntimeError);
    CHECK(!err_status.is_ok());
    CHECK_EQUAL(err_status.code_string(), ErrorCodes::error_string(err_status.code()));
    CHECK_EQUAL(err_status.reason(), err_status_reason);

    CHECK_NOT_EQUAL(ok_status, err_status);
    CHECK_EQUAL(err_status, Status(ErrorCodes::RuntimeError, "runtime error 2"));
    CHECK_NOT_EQUAL(err_status, Status(ErrorCodes::LogicError, "logic error"));

    auto caught_status = Status::OK();
    try {
        throw Exception(err_status);
    }
    catch (...) {
        caught_status = exception_to_status();
    }

    CHECK_EQUAL(caught_status, err_status);

    struct ExoticError : public std::runtime_error {
        using std::runtime_error::runtime_error;
    };

    const auto exotic_error_reason = "serious error";
    try {
        throw ExoticError(exotic_error_reason);
    }
    catch (...) {
        caught_status = exception_to_status();
    }
    CHECK_EQUAL(caught_status, ErrorCodes::UnknownError);
    CHECK_NOT_EQUAL(caught_status.reason().find(exotic_error_reason), std::string::npos);
}

TEST(StatusWith)
{
    auto result = StatusWith<int>(5);
    CHECK(result.is_ok());
    CHECK_EQUAL(result.get_value(), 5);

    const char* err_status_reason = "runtime error 1";
    result = StatusWith<int>(ErrorCodes::RuntimeError, err_status_reason);
    CHECK_EQUAL(result.get_status().code(), ErrorCodes::RuntimeError);
    CHECK(!result.is_ok());
}

TEST(ErrorCodes)
{
    auto all_codes = ErrorCodes::get_all_codes();
    auto all_names = ErrorCodes::get_all_names();
    for (auto code : all_codes) {
        auto code_string = ErrorCodes::error_string(code);
        CHECK_EQUAL(ErrorCodes::from_string(code_string), code);
    }
    for (auto name : all_names) {
        auto code = ErrorCodes::from_string(name);
        CHECK_EQUAL(ErrorCodes::error_string(code), name);
    }
    CHECK_EQUAL(ErrorCodes::from_string("InvalidDictionary"), ErrorCodes::UnknownError);
    CHECK_EQUAL(ErrorCodes::from_string("Zzzzz"), ErrorCodes::UnknownError);
}

} // namespace
} // namespace realm
