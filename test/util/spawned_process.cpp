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

#include "spawned_process.hpp"

#include <realm/util/assert.hpp>
#include <realm/util/file_mapper.hpp>

#ifndef _WIN32
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <iostream>

namespace realm {
namespace test_util {

SpawnedProcess::SpawnedProcess(const std::string& test_name, const std::string& ident)
    : m_test_name(test_name)
    , m_identifier(ident)
{
#ifdef _WIN32
    ZeroMemory(&m_process, sizeof(m_process));
#endif
}

SpawnedProcess::~SpawnedProcess()
{
#ifdef _WIN32
    if (m_process.hProcess != 0) {
        // Close process and thread handles.
        CloseHandle(m_process.hProcess);
        CloseHandle(m_process.hThread);
    }
#endif
}

#ifdef _WIN32
void SpawnedProcess::set_pid(PROCESS_INFORMATION pi)
{
    m_process = pi;
}
#else
void SpawnedProcess::set_pid(int id)
{
    m_pid = id;
}
#endif

bool SpawnedProcess::is_child()
{
#ifndef _WIN32
    REALM_ASSERT_EX(m_pid >= 0, m_pid);
    return m_pid == 0;
#else
    const char* str = getenv("REALM_CHILD_IDENT");
    return str && str == m_identifier;
#endif
}

bool SpawnedProcess::is_parent()
{
    return !(getenv("REALM_CHILD_IDENT") || getenv("REALM_FORKED"));
}

int SpawnedProcess::wait_for_child_to_finish()
{
#ifndef _WIN32
    int ret = 0;
    int status = 0;
    int options = 0;
    do {
        ret = waitpid(m_pid, &status, options);
    } while (ret == -1 && errno == EINTR);
    REALM_ASSERT_RELEASE_EX(ret != -1, errno, m_pid, m_test_name, m_identifier);

    bool signaled_to_stop = WIFSIGNALED(status);
    REALM_ASSERT_RELEASE_EX(!signaled_to_stop, WTERMSIG(status), WCOREDUMP(status), m_pid, m_test_name, m_identifier);

    bool stopped = WIFSTOPPED(status);
    REALM_ASSERT_RELEASE_EX(!stopped, WSTOPSIG(status), m_pid, m_test_name, m_identifier);

    bool exited_normally = WIFEXITED(status);
    REALM_ASSERT_RELEASE_EX(exited_normally, m_pid, m_test_name, m_identifier);

    auto exit_status = WEXITSTATUS(status);
    REALM_ASSERT_RELEASE_EX(exit_status == 0, exit_status, m_pid, m_test_name, m_identifier);
    return status;
#else
    if (!is_parent()) {
        return 0;
    }
    REALM_ASSERT_EX(m_process.hProcess != 0, m_test_name, m_identifier);
    constexpr DWORD milliseconds_to_wait = 10 * 60 * 1000;
    auto status = WaitForSingleObject(m_process.hProcess, milliseconds_to_wait);
    if (status == WAIT_TIMEOUT) {
        REALM_ASSERT_EX(false, "process wait failed", m_test_name, m_identifier);
    }
    else if (status == WAIT_FAILED) {
        REALM_ASSERT_EX(false, util::format("process wait failed (%1)", GetLastError()), m_test_name, m_identifier);
    }
    REALM_ASSERT_EX(status == WAIT_OBJECT_0, status, m_test_name, m_identifier);
    return 0;
#endif
}

std::unique_ptr<SpawnedProcess> spawn_process(const std::string& test_name, const std::string& process_ident)
{
    std::unique_ptr<SpawnedProcess> process = std::make_unique<SpawnedProcess>(test_name, process_ident);
#ifndef _WIN32
    util::prepare_for_fork_in_parent();
    int pid = fork();
    REALM_ASSERT(pid >= 0);
    if (pid == 0) {
        util::post_fork_in_child();
    }
    process->set_pid(pid);
#else
    const char* str = getenv("REALM_CHILD_IDENT");
    if (!str) {
        STARTUPINFO si;
        PROCESS_INFORMATION pi;
        TCHAR program_name[MAX_PATH];
        ZeroMemory(&si, sizeof(si));
        si.cb = sizeof(si);
        ZeroMemory(&pi, sizeof(pi));
        auto success = GetModuleFileName(NULL, program_name, MAX_PATH);
        REALM_ASSERT_EX(success, util::format("GetModuleFileName failed (%1)", GetLastError()), test_name,
                        process_ident);
        std::string env = "REALM_FORKED=1";
        env.append("\0", 1);
        env.append(util::format("UNITTEST_FILTER=%1", test_name));
        env.append("\0", 1);
        env.append(util::format("REALM_CHILD_IDENT=%1", process_ident));
        env.append("\0\0", 2);
        if (!CreateProcess(program_name, // Application name
                           NULL,         // Command line
                           NULL,         // Process handle not inheritable
                           NULL,         // Thread handle not inheritable
                           FALSE,        // Set handle inheritance to FALSE
                           0,            // No creation flags, console of child goes to parent
                           env.data(),   // Environment block, ANSI
                           NULL,         // Use parent's starting directory
                           &si,          // Pointer to STARTUPINFO structure
                           &pi)          // Pointer to PROCESS_INFORMATION structure
        ) {
            REALM_ASSERT_EX(false, util::format("CreateProcess failed (%1).\n", GetLastError()), test_name,
                            process_ident);
        }
        process->set_pid(pi);
    }
#endif
    return process;
}

int64_t get_pid()
{
#ifdef _WIN32
    uint64_t pid = GetCurrentProcessId();
    return pid;
#else
    uint64_t pid = getpid();
    return pid;
#endif
}

} // namespace test_util
} // namespace realm
