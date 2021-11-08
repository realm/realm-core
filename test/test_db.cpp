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

TEST(DB_getCoreFiles_adding_path)
{
    std::string path = "path/to/realm/files/";
    auto extension_lock = ".lock";
    auto extension_storage = "";
    auto extension_management = ".management";
    auto extension_note = ".note";
    auto extension_log = ".log";

    CHECK_EQUAL(DB::get_core_file(path, DB::CoreFileType::Lock), path + extension_lock);
    CHECK_EQUAL(DB::get_core_file(path, DB::CoreFileType::Storage), path + extension_storage);
    CHECK_EQUAL(DB::get_core_file(path, DB::CoreFileType::Management), path + extension_management);
    CHECK_EQUAL(DB::get_core_file(path, DB::CoreFileType::Note), path + extension_note);
    CHECK_EQUAL(DB::get_core_file(path, DB::CoreFileType::Log), path + extension_log);
}
