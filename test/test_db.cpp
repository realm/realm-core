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
auto extension_backup = ".realm.backup";

TEST(DB_getCoreFiles_default)
{
    auto core_files = DB::get_core_files();
    CHECK_EQUAL(core_files.size(), 2);
    CHECK_EQUAL(core_files[0].first, extension_storage);
    CHECK_NOT(core_files[0].second);
    CHECK_EQUAL(core_files[1].first, extension_management);
    CHECK(core_files[1].second);
}

TEST(DB_getCoreFiles_adding_path)
{
    auto core_files = DB::get_core_files(path);
    CHECK_EQUAL(core_files.size(), 2);
    CHECK_EQUAL(core_files[0].first, path + extension_storage);
    CHECK_NOT(core_files[0].second);
    CHECK_EQUAL(core_files[1].first, path + extension_management);
    CHECK(core_files[1].second);
}

TEST(DB_getCoreFiles_StateFiles)
{
    auto core_files = DB::get_core_files(path, DB::CoreFileType::StateFiles);
    CHECK_EQUAL(core_files.size(), 2);
    CHECK_EQUAL(core_files[0].first, path + extension_storage);
    CHECK_NOT(core_files[0].second);
    CHECK_EQUAL(core_files[1].first, path + extension_management);
    CHECK(core_files[1].second);
}

TEST(DB_getCoreFiles_TemporaryFiles)
{
    auto core_files = DB::get_core_files(path, DB::CoreFileType::TemporaryFiles);
    CHECK_EQUAL(core_files.size(), 5);
    CHECK_EQUAL(core_files[0].first, path + extension_note);
    CHECK_NOT(core_files[0].second);
    CHECK_EQUAL(core_files[1].first, path + extension_log);
    CHECK_NOT(core_files[1].second);
    CHECK_EQUAL(core_files[2].first, path + extension_log_a);
    CHECK_NOT(core_files[2].second);
    CHECK_EQUAL(core_files[3].first, path + extension_log_b);
    CHECK_NOT(core_files[3].second);
    CHECK_EQUAL(core_files[4].first, path + extension_backup);
    CHECK_NOT(core_files[4].second);
}

TEST(DB_getCoreFiles_CombinedFlags)
{
    auto core_files = DB::get_core_files(path, DB::CoreFileType::StateFiles | DB::CoreFileType::Lock);
    CHECK_EQUAL(core_files.size(), 3);
    CHECK_EQUAL(core_files[0].first, path + extension_lock);
    CHECK_NOT(core_files[0].second);
    CHECK_EQUAL(core_files[1].first, path + extension_storage);
    CHECK_NOT(core_files[1].second);
    CHECK_EQUAL(core_files[2].first, path + extension_management);
    CHECK(core_files[2].second);
}

TEST(DB_getCoreFiles_AllFiles)
{
    auto core_files = DB::get_core_files(path, DB::CoreFileType::All);
    CHECK_EQUAL(core_files.size(), 8);
    CHECK_EQUAL(core_files[0].first, path + extension_lock);
    CHECK_NOT(core_files[0].second);
    CHECK_EQUAL(core_files[1].first, path + extension_storage);
    CHECK_NOT(core_files[1].second);
    CHECK_EQUAL(core_files[2].first, path + extension_management);
    CHECK(core_files[2].second);
    CHECK_EQUAL(core_files[3].first, path + extension_note);
    CHECK_NOT(core_files[3].second);
    CHECK_EQUAL(core_files[4].first, path + extension_log);
    CHECK_NOT(core_files[4].second);
    CHECK_EQUAL(core_files[5].first, path + extension_log_a);
    CHECK_NOT(core_files[5].second);
    CHECK_EQUAL(core_files[6].first, path + extension_log_b);
    CHECK_NOT(core_files[6].second);
    CHECK_EQUAL(core_files[7].first, path + extension_backup);
    CHECK_NOT(core_files[7].second);
}
