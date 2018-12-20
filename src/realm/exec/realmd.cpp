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

// Realm daemon (realmd) responsible for async commits

#include <realm/group_shared.hpp>
#include <unistd.h>
#include <iostream>

using namespace realm;

int main(int argc, char* argv[])
{

    // rudimentary check that a database name is provided as parameter.
    if (argc != 2) {
        fprintf(stderr, "ERROR: No database name provided\n");
        exit(3);
    }

    // Spawn daemon proces. Parent will exit causing the daemon to be
    // adopted by the init process. Ensures that the daemon won't become
    // a zombie, but be collected by the init process when it exits.
    // This is the second fork of the double-fork-idiom.
    int pid = fork();
    if (pid == 0) { // in daemon process:

#ifdef REALM_ENABLE_LOGFILE
        std::cerr << "Daemon starting" << std::endl;
#endif
        SharedGroup async_committer((SharedGroup::unattached_tag()));
        char* file = argv[1];
        using sgf = _impl::SharedGroupFriend;
        sgf::async_daemon_open(async_committer, file);
    }
    else if (pid > 0) {
        // in parent, fork was ok, so return succes
        _Exit(0);
    }
    else {
        // in parent, fork failed, so return error code
        return 2;
    }
}
