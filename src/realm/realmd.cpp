// Realm daemon (realmd) responsible for async commits

#include <realm/group_shared.hpp>
#include <unistd.h>
#include <iostream>

using namespace realm;
using namespace std;

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
        cerr << "Daemon starting" << endl;
#endif
        SharedGroup::unattached_tag tag;
        SharedGroup async_committer(tag);
        char* file = argv[1];
        async_committer.open(file, true, SharedGroup::durability_Async, true);

    } else if (pid > 0) { // in parent, fork was ok, so return succes

        _Exit(0);

    } else { // in parent, fork failed, so return error code

        return 2;
    }
}

