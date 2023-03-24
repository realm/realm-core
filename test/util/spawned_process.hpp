/*************************************************************************
 *
 * Copyright 2023 Realm Inc.
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

#ifndef REALM_TEST_UTIL_SPAWNED_PROCESS_HPP
#define REALM_TEST_UTIL_SPAWNED_PROCESS_HPP

#ifdef _WIN32
#include <windows.h>
#endif

#include <memory>
#include <string>

namespace realm {
namespace test_util {

// This is a wrapper around UNIX posix_spawn(), and Windows CreateProcess(). This
// provides a way to coordinate multi-process tests. Care must be taken when using
// this class in tests to avoid fork bombs. A completely new test process with no
// shared state is started. Environment variables are used to filter to a specific
// unit test. Further filtering within the test itself is possible by using
// `is_child()` which checks against the `ident` string.
struct SpawnedProcess {
    SpawnedProcess(const std::string& test_name, const std::string& ident);
    ~SpawnedProcess();
    bool is_child();
    static bool is_parent();
    int wait_for_child_to_finish();

#ifdef _WIN32
    void set_pid(PROCESS_INFORMATION pi);
#else
    void set_pid(int id);
#endif

private:
    std::string m_test_name;
    std::string m_identifier;
#ifdef _WIN32
    PROCESS_INFORMATION m_process;
#else
    int m_pid = -1;
#endif // _WIN32
};

// This only creates a process from the root parent process. If the current process is detected to be
// a child process via the REALM_CHILD_IDENT environment variable, then no process is actually started here.
std::unique_ptr<SpawnedProcess> spawn_process(const std::string& test_name, const std::string& process_ident);
int64_t get_pid();

} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_SPAWNED_PROCESS_HPP
