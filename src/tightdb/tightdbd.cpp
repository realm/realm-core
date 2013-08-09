// Tightdb daemon (tightdbd) responsible for async commits

#include <tightdb/group_shared.hpp>

using namespace tightdb;

int main(int argc, char* argv[], char* envp[]) {
    if (argc != 2) {
        fprintf(stderr, "ERROR: No database name provided\n");
        exit(1);
    }
    SharedGroup::unattached_tag tag;
    SharedGroup async_committer(tag);
    char* file = argv[1];
    async_committer.open(file, true, SharedGroup::durability_Async, true);
}

