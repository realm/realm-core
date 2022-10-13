/*************************************************************************
 *
 * Copyright 2022 Realm Inc.
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
#include <realm/history.hpp>

#include <ctime>
#include <cstdio>
#include <fstream>
#include <iostream>

#include "../fuzz_group.hpp"
#include "../util/test_path.hpp"

using namespace realm;
using namespace realm::util;

// This function is the entry point for libfuzzer, main is auto-generated
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* Data, size_t Size)
{
    if (Size == 0) {
        return 0;
    }
    realm::test_util::RealmPathInfo test_context{"libfuzzer_test"};
    SHARED_GROUP_TEST_PATH(path);
    disable_sync_to_disk();
    util::Optional<std::ostream&> log; // logging off
    std::string contents(reinterpret_cast<const char*>(Data), Size);
    parse_and_apply_instructions(contents, path, log);
    return 0; // Non-zero return values are reserved for future use.
}
