#include "realmbm.hpp"

#include "results.hpp"      // Results
#include "timer.hpp"        // Timer

using namespace realm;
using namespace realm::test_util;

WithSharedGroup::WithSharedGroup()
{
    std::string realm_path = "results.realm";
    this->sg.reset(new SharedGroup(realm_path, false,
        SharedGroup::durability_MemOnly, nullptr));
}
