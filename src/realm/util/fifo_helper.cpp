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

#include <sstream>
#include <system_error>
#include <sys/stat.h>

// FIFOs do not work on Windows.
namespace realm {
namespace util {

namespace {
void check_is_fifo(const std::string& path)
{
#ifndef _WIN32
    struct stat stat_buf;
    if (stat(path.c_str(), &stat_buf) == 0) {
        if ((stat_buf.st_mode & S_IFMT) != S_IFIFO) {
            throw std::runtime_error(path + " exists and it is not a fifo.");
        }
    }
#endif
}
} // Anonymous namespace

void create_fifo(std::string path)
{
#ifndef _WIN32
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
    mode_t mode = 0666;
#else
    mode_t mode = 0600;
#endif

    // Create and open the named pipe
    int ret = mkfifo(path.c_str(), mode);
    if (ret == -1) {
        int err = errno;
        // the fifo already existing isn't an error
        if (err != EEXIST) {
#ifdef __ANDROID__
            // Workaround for a mkfifo bug on Blackberry devices:
            // When the fifo already exists, mkfifo fails with error ENOSYS which is not correct.
            // In this case, we use stat to check if the path exists and it is a fifo.
            if (err == ENOSYS) {
                check_is_fifo(path);
            }
            else {
                throw std::system_error(err, std::system_category());
            }
#else
            throw std::system_error(err, std::system_category());
#endif
        }
        else {
            // If the file already exists, verify it is a FIFO
            return check_is_fifo(path);
        }
    }
#endif
}

bool try_create_fifo(const std::string& path)
{
    try {
        create_fifo(path);
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace util
} // namespace realm
