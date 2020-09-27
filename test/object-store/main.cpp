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

#define CATCH_CONFIG_RUNNER
#include "realm/util/features.h"
#if REALM_PLATFORM_APPLE
#define CATCH_CONFIG_NO_CPP17_UNCAUGHT_EXCEPTIONS
#endif
#include <catch2/catch.hpp>

#include <limits.h>

#ifdef _MSC_VER
#include <Windows.h>

// PathCchRemoveFileSpec()
#include <pathcch.h>
#pragma comment(lib, "Pathcch.lib")
#else
#include <libgen.h>
#endif

int main(int argc, char** argv)
{
#ifdef _MSC_VER
    wchar_t path[MAX_PATH];
    if (GetModuleFileName(NULL, path, MAX_PATH) == 0) {
        fprintf(stderr, "Failed to retrieve path to exectuable.\n");
        return 1;
    }
    PathCchRemoveFileSpec(path, MAX_PATH);
    SetCurrentDirectory(path);
#else
    char executable[PATH_MAX];
    if (realpath(argv[0], executable) == NULL) {
        fprintf(stderr, "Failed to resolve path to exectuable.\n");
        return 1;
    }
    const char* directory = dirname(executable);
    if (chdir(directory) < 0) {
        fprintf(stderr, "Failed to change directory.\n");
        return 1;
    }
#endif

    int result = Catch::Session().run(argc, argv);
    return result < 0xff ? result : 0xff;
}
