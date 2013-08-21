// Tightdb daemon (tightdbd) responsible for async commits

#include <tightdb/group_shared.hpp>
#include <unistd.h>

using namespace tightdb;

void exit_handler()
{
    fprintf(stderr, "Daemon exiting (exit_handler called)");
}

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

        atexit(exit_handler);
        fprintf(stderr, "Daemon starting\n");
        try {
            SharedGroup::unattached_tag tag;
            SharedGroup async_committer(tag);
            char* file = argv[1];
            async_committer.open(file, true, SharedGroup::durability_Async, true);
        } catch (...) {
            fprintf(stderr, "Daemon threw an exception");
        }

    } else if (pid > 0) { // in parent, fork was ok, so return succes

        exit(0);

    } else { // in parent, fork failed, so return error code

        exit(2);
    }
    // never come here
}

