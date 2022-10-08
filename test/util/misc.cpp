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

#include "misc.hpp"

#include <realm/util/assert.hpp>

#ifndef _WIN32
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <realm/util/file_mapper.hpp>

namespace realm {
namespace test_util {

void replace_all(std::string& str, const std::string& from, const std::string& to)
{
    if (from.empty())
        return;
    size_t start_pos = 0;
    while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length(); // In case 'to' contains 'from', like replacing 'x' with 'yx'
    }
}

bool equal_without_cr(std::string s1, std::string s2)
{
    // Remove CR so that we can be compare strings platform independant

    replace_all(s1, "\r", "");
    replace_all(s2, "\r", "");
    return (s1 == s2);
}

// FIXME: we should implement these for windows as well.
int waitpid_checked(int pid, int options, const std::string& info)
{
#ifndef _WIN32
    int ret = 0;
    int status = 0;
    do {
        ret = waitpid(pid, &status, options);
    } while (ret == -1 && errno == EINTR);
    REALM_ASSERT_RELEASE_EX(ret != -1, errno, pid, info);

    bool signaled_to_stop = WIFSIGNALED(status);
    REALM_ASSERT_RELEASE_EX(!signaled_to_stop, WTERMSIG(status), WCOREDUMP(status), pid, info);

    bool stopped = WIFSTOPPED(status);
    REALM_ASSERT_RELEASE_EX(!stopped, WSTOPSIG(status), pid, info);

    bool exited_normally = WIFEXITED(status);
    REALM_ASSERT_RELEASE_EX(exited_normally, pid, info);

    auto exit_status = WEXITSTATUS(status);
    REALM_ASSERT_RELEASE_EX(exit_status == 0, exit_status, pid, info);
    return status;
#else
    constexpr bool not_supported_on_windows = false;
    REALM_ASSERT(not_supported_on_windows);
#endif
}

int fork_and_update_mappings()
{
#ifndef _WIN32
    util::prepare_for_fork_in_parent();
    int pid = fork();
    if (pid == 0) {
        util::post_fork_in_child();
    }
    return pid;
#else
    constexpr bool not_supported_on_windows = false;
    REALM_ASSERT(not_supported_on_windows);
#endif
}

} // namespace test_util
} // namespace realm
