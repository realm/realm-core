/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
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

#include <realm/util/features.h>
#include "test_all.hpp"
#include "util/test_path.hpp"
#ifdef _WIN32
#include <Windows.h>

// PathCchRemoveFileSpec()
#include <pathcch.h>
#pragma comment(lib, "Pathcch.lib")
#elif REALM_PLATFORM_APPLE && (REALM_APPLE_DEVICE || TARGET_OS_SIMULATOR)
#include <CoreFoundation/CoreFoundation.h>
#include <realm/util/cf_ptr.hpp>
using namespace realm::util;

static std::string url_to_path(CFURLRef url)
{
    auto path = adoptCF(CFURLCopyPath(url));
    size_t len = CFStringGetLength(path.get()) + 1;
    char buffer[len];
    bool result = CFStringGetCString(path.get(), buffer, len, kCFStringEncodingUTF8);
    REALM_ASSERT(result);
    return std::string(buffer, len - 1);
}
#else
#include <unistd.h>
#include <libgen.h>
#endif

int main(int argc, char* argv[])
{
#ifdef _MSC_VER
    wchar_t path[MAX_PATH];
    if (GetModuleFileName(NULL, path, MAX_PATH) == 0) {
        fprintf(stderr, "Failed to retrieve path to exectuable.\n");
        return 1;
    }
    PathCchRemoveFileSpec(path, MAX_PATH);
    SetCurrentDirectory(path);
#elif REALM_PLATFORM_APPLE && (REALM_APPLE_DEVICE || TARGET_OS_SIMULATOR)
    auto home = adoptCF(CFCopyHomeDirectoryURL());
    realm::test_util::set_test_path_prefix(url_to_path(home.get()) + "Documents/");

    auto resources_url = adoptCF(CFBundleCopyResourcesDirectoryURL(CFBundleGetMainBundle()));
    realm::test_util::set_test_resource_path(url_to_path(resources_url.get()));
#else
    char executable[PATH_MAX];
    if (realpath(argv[0], executable) == nullptr) {
        fprintf(stderr, "Failed to retrieve path to exectuable.\n");
        return 1;
    }
    const char* directory = dirname(executable);
    if (chdir(directory) < 0) {
        fprintf(stderr, "Failed to change directory.\n");
        return 1;
    }
#endif

    if (argc > 1) {
        realm::test_util::set_test_path_prefix(argv[1]);
    }

    return test_all();
}
