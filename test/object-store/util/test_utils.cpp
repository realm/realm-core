////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include "impl/realm_coordinator.hpp"
#include "test_utils.hpp"

#include <realm/util/file.hpp>
#include <realm/string_data.hpp>

namespace realm {

bool create_dummy_realm(std::string path) {
    Realm::Config config;
    config.path = path;
    try {
        _impl::RealmCoordinator::get_coordinator(path)->get_realm(config, none);
        REQUIRE_REALM_EXISTS(path);
        return true;
    } catch (std::exception&) {
        return false;
    }
}

void reset_test_directory(const std::string& base_path) {
    util::try_remove_dir_recursive(base_path);
    util::make_dir(base_path);
}

std::string tmp_dir() {
    const char* dir = getenv("TMPDIR");
    if (dir && *dir)
        return dir;
#if REALM_ANDROID
    return "/data/local/tmp/";
#else
    return "/tmp/";
#endif
}

std::vector<char> make_test_encryption_key(const char start) {
    std::vector<char> vector;
    vector.reserve(64);
    for (int i=0; i<64; i++) {
        vector.emplace_back((start + i) % 128);
    }
    return vector;
}

// FIXME: Catch2 limitation on old compilers (currently our android CI)
// https://github.com/catchorg/Catch2/blob/master/docs/limitations.md#clangg----skipping-leaf-sections-after-an-exception
void catch2_ensure_section_run_workaround(bool did_run_a_section, std::string section_name, std::function<void()> func) {
    if (did_run_a_section) {
        func();
    }
    else {
        std::cout << "Skipping test section '" << section_name << "' on this run." << std::endl;
    }
}

}
