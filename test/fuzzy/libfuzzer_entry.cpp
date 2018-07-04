#include <realm/db.hpp>
#include <realm/history.hpp>

#include <ctime>
#include <cstdio>
#include <fstream>
#include <iostream>

#include "../fuzz_group.hpp"
#include "../util/test_path.hpp"

using namespace realm;
using namespace realm::util;

// This function is the entry point for libfuzzer, main is auto-generated
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* Data, size_t Size)
{
    if (Size == 0) {
        return 0;
    }
    realm::test_util::RealmPathInfo test_context{"libfuzzer_test"};
    SHARED_GROUP_TEST_PATH(path);
    disable_sync_to_disk();
    util::Optional<std::ostream&> log; // logging off
    std::string contents(reinterpret_cast<const char*>(Data), Size);
    parse_and_apply_instructions(contents, path, log);
    return 0; // Non-zero return values are reserved for future use.
}
