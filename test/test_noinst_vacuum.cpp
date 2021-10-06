#include "test.hpp"

#include <realm/util/file.hpp>
#include <realm/sync/noinst/server/vacuum.hpp>

using namespace realm;
using namespace realm::_impl;

using Vacuum = _impl::Vacuum;

// The Realm "vacuum_no_history_type.realm" is a new Realm that has just been created without
// a history. It is vacuumed with a forced history_type of SyncServer.
TEST(Vacuum_HistoryType)
{
    util::Logger& logger = test_context.logger;
    TEST_DIR(dir);

    std::string origin_path = util::File::resolve("vacuum_no_history_type.realm", "resources");
    std::string target_path = util::File::resolve("vacuum_no_history_type.realm", dir);
    util::File::copy(origin_path, target_path);

    Vacuum::Options options;
    options.history_type = Replication::hist_SyncServer;
    options.bump_realm_version = true;
    Vacuum vacuum{logger, options};

    Vacuum::Results results = vacuum.vacuum(target_path);

    CHECK_EQUAL(results.before_size, 4096);
    CHECK_EQUAL(results.type_description, "Sync Server");
}
