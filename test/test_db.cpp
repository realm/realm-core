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

#include <realm/db.hpp>
#include "test.hpp"

using namespace realm;

std::string path = "path/to/realm/files/";
auto extension_lock = ".lock";
auto extension_storage = "";
auto extension_management = ".management";
auto extension_note = ".note";
auto extension_log = ".log";
auto extension_log_a = ".log_a";
auto extension_log_b = ".log_b";

TEST(DB_getCoreFiles_default)
{
    auto core_files = DB::get_core_files();
    CHECK_EQUAL(core_files.size(), 7);
    CHECK_EQUAL(core_files[DB::CoreFileType::Lock].first, extension_lock);
    CHECK_EQUAL(core_files[DB::CoreFileType::Lock].second, false);
    CHECK_EQUAL(core_files[DB::CoreFileType::Storage].first, extension_storage);
    CHECK_EQUAL(core_files[DB::CoreFileType::Storage].second, false);
    CHECK_EQUAL(core_files[DB::CoreFileType::Management].first, extension_management);
    CHECK_EQUAL(core_files[DB::CoreFileType::Management].second, true);
    CHECK_EQUAL(core_files[DB::CoreFileType::Note].first, extension_note);
    CHECK_EQUAL(core_files[DB::CoreFileType::Note].second, false);
    CHECK_EQUAL(core_files[DB::CoreFileType::Log].first, extension_log);
    CHECK_EQUAL(core_files[DB::CoreFileType::Log].second, false);
    CHECK_EQUAL(core_files[DB::CoreFileType::LogA].first, extension_log_a);
    CHECK_EQUAL(core_files[DB::CoreFileType::LogA].second, false);
    CHECK_EQUAL(core_files[DB::CoreFileType::LogB].first, extension_log_b);
    CHECK_EQUAL(core_files[DB::CoreFileType::LogB].second, false);
}

TEST(DB_getCoreFiles_adding_path)
{
    auto core_files = DB::get_core_files(path);
    CHECK_EQUAL(core_files.size(), 7);
    CHECK_EQUAL(core_files[DB::CoreFileType::Lock].first, path + extension_lock);
    CHECK_EQUAL(core_files[DB::CoreFileType::Lock].second, false);
    CHECK_EQUAL(core_files[DB::CoreFileType::Storage].first, path + extension_storage);
    CHECK_EQUAL(core_files[DB::CoreFileType::Storage].second, false);
    CHECK_EQUAL(core_files[DB::CoreFileType::Management].first, path + extension_management);
    CHECK_EQUAL(core_files[DB::CoreFileType::Management].second, true);
    CHECK_EQUAL(core_files[DB::CoreFileType::Note].first, path + extension_note);
    CHECK_EQUAL(core_files[DB::CoreFileType::Note].second, false);
    CHECK_EQUAL(core_files[DB::CoreFileType::Log].first, path + extension_log);
    CHECK_EQUAL(core_files[DB::CoreFileType::Log].second, false);
    CHECK_EQUAL(core_files[DB::CoreFileType::LogA].first, path + extension_log_a);
    CHECK_EQUAL(core_files[DB::CoreFileType::LogA].second, false);
    CHECK_EQUAL(core_files[DB::CoreFileType::LogB].first, path + extension_log_b);
    CHECK_EQUAL(core_files[DB::CoreFileType::LogB].second, false);
}
