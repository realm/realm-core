/*************************************************************************
 *
 * Copyright 2019 Realm Inc.
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

#include <realm/util/fifo_helper.hpp>

#include <fcntl.h>
#include <sstream>
#include <system_error>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef _WIN32
#include <Windows.h>
#endif

namespace realm {
namespace util {

void create_fifo(std::string path, const std::string tmp_dir)
{
#ifdef REALM_ANDROID
    // Upgrading apps on Android Huawai devices sometimes leave FIFO files with the wrong
    // file owners. This results in the Android sandbox preventing Realm from opening the
    // database file. To prevent this from happening we create all FIFO files with full permissions.
    // This should be safe as the app is already running inside a protected part of the filesystem,
    // so no outside users have access to the filesystem where these files are.
    // It does make storing Realms on external storage slightly more unsafe, but users already have
    // full access to modify or copy files from there, so there should be no change from a pratical
    // security standpoint.
    // See more here: https://github.com/realm/realm-java/issues/3972#issuecomment-313675948
    mode_t mode = 0600;
#else
    mode_t mode = 0600;
#endif

    // Create and open the named pipe
    int ret = mkfifo(path.c_str(), mode);
    if (ret == -1) {
        int err = errno;
        if (err == ENOTSUP || err == EACCES || err == EPERM || err == EINVAL) {
            // Filesystem doesn't support named pipes, so try putting it in tmp_dir instead
            // Hash collisions are okay here because they just result in doing
            // extra work, as opposed to correctness problems.
            std::ostringstream ss;
            ss << tmp_dir;
            ss << "realm_" << std::hash<std::string>()(path) << ".cv";
            path = ss.str();
            ret = mkfifo(path.c_str(), mode);
            err = errno;
        }

        // the fifo already existing isn't an error
        if (ret == -1 && err != EEXIST) {
            // Workaround for a mkfifo bug on Blackberry devices:
            // When the fifo already exists, mkfifo fails with error ENOSYS which is not correct.
            // In this case, we use stat to check if the path exists and it is a fifo.
            struct stat stat_buf;
            if (stat(path.c_str(), &stat_buf) == 0) {
                if ((stat_buf.st_mode & S_IFMT) != S_IFIFO) {
                    throw std::runtime_error(path + " exists and it is not a fifo.");
                }
            }
            else {
                throw std::system_error(err, std::system_category());
            }
        }
    }
}

} // namespace util
} // namespace realm

