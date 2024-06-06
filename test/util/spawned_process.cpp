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
#include <realm/util/backtrace.hpp>
#include <realm/util/file_mapper.hpp>

#include "test_path.hpp"

#ifndef _WIN32
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <csignal>
#include <iostream>
#include <sstream>

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
    const char* str = getenv("REALM_CHILD_IDENT");
    return str && str == m_identifier;
}

bool SpawnedProcess::is_parent()
{
    return !(getenv("REALM_CHILD_IDENT") || getenv("REALM_SPAWNED"));
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

static std::stringstream s_ss;
static void signal_handler(int signal)
{
    std::cout << "signal handler: " << signal << std::endl;
    util::Backtrace::capture().print(s_ss);
    std::cout << "trace: " << s_ss.str() << std::endl;
    exit(signal);
}

std::unique_ptr<SpawnedProcess> spawn_process(const std::string& test_name, const std::string& process_ident)
{
    std::unique_ptr<SpawnedProcess> process = std::make_unique<SpawnedProcess>(test_name, process_ident);
    const char* child_ident = getenv("REALM_CHILD_IDENT");
    if (child_ident) {
        std::signal(SIGSEGV, signal_handler);
        std::signal(SIGABRT, signal_handler);
        return process;
    }

    std::vector<std::string> env_vars = {"REALM_SPAWNED=1", util::format("UNITTEST_FILTER=%1", test_name),
                                         util::format("REALM_CHILD_IDENT=%1", process_ident)};
    if (auto value = getenv("UNITTEST_ENCRYPT_ALL")) {
        env_vars.push_back(util::format("UNITTEST_ENCRYPT_ALL=%1", value));
    }
    if (auto value = getenv("UNITTEST_ENABLE_SYNC_TO_DISK")) {
        env_vars.push_back(util::format("UNITTEST_ENABLE_SYNC_TO_DISK=%1", value));
    }
    if (getenv("TMPDIR")) {
        env_vars.push_back(util::format("TMPDIR=%1", getenv("TMPDIR")));
    }

#if REALM_ANDROID || REALM_IOS
    // posix_spawn() is unavailable on Android, and not permitted on iOS
    REALM_UNREACHABLE();
#elif defined(_WIN32)
    STARTUPINFO si;
    PROCESS_INFORMATION pi;
    TCHAR program_name[MAX_PATH];
    ZeroMemory(&si, sizeof(si));
    si.cb = sizeof(si);
    ZeroMemory(&pi, sizeof(pi));
    auto success = GetModuleFileName(NULL, program_name, MAX_PATH);
    REALM_ASSERT_EX(success, util::format("GetModuleFileName failed (%1)", GetLastError()), test_name, process_ident);
    std::string env;
    for (auto& var : env_vars) {
        env.append(var);
        env.append("\0", 1);
    }
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
#else
    pid_t pid_of_child;
    std::string name_of_exe = test_util::get_test_exe_name();
    // need to use the same test path as parent so that tests use the same realm paths
    std::string test_path_prefix = test_util::get_test_path_prefix();
#ifdef __linux__
    // process the path in case we want to run the tests from outside the bulding directory
    auto pos = name_of_exe.find_last_of('/');
    if (pos != std::string::npos)
        name_of_exe = test_path_prefix + "/" + name_of_exe.substr(pos + 1);
#endif
    REALM_ASSERT(name_of_exe.size());
    char* arg_v[] = {name_of_exe.data(), test_path_prefix.data(), nullptr};
    std::vector<char*> env_var_ptrs;
    env_var_ptrs.reserve(env_vars.size() + 1);
    std::transform(env_vars.begin(), env_vars.end(), std::back_inserter(env_var_ptrs), [](std::string& str) {
        return str.data();
    });
    env_var_ptrs.push_back(nullptr);
    int ret = posix_spawn(&pid_of_child, name_of_exe.data(), nullptr, nullptr, arg_v, env_var_ptrs.data());
    REALM_ASSERT_EX(ret == 0, ret);
    process->set_pid(pid_of_child);
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
