// Tightdb daemon (tightdbd) responsible for async commits

#include <tightdb/group_shared.hpp>

using namespace tightdb;

int main(int argc, char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "ERROR: No database name provided\n");
        exit(3);
    }
    int pid = fork();
    if (pid == 0) {
        // in daemon process
        SharedGroup::unattached_tag tag;
        SharedGroup async_committer(tag);
        char* file = argv[1];
        async_committer.open(file, true, SharedGroup::durability_Async, true);
    } else if (pid > 0) {
        // in parent, fork was ok, so return succes
        exit(0);
    } else {
        // in parent, fork failed, so return error code
        exit(2);
    }
}

