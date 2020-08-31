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

#include "test_all.hpp"
#include "util/test_path.hpp"
#ifdef _WIN32
#include <Windows.h>

// PathCchRemoveFileSpec()
#include <pathcch.h>
#pragma comment(lib, "Pathcch.lib")
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
    if (argc > 1) {
        realm::test_util::set_test_path_prefix(argv[1]);
    }
#endif

    return test_all(argc, argv);
}
