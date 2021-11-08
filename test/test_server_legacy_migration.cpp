#include <realm/list.hpp>
#include <realm/table.hpp>
#include <realm/group.hpp>
#include <realm/sync/noinst/server/server_legacy_migration.hpp>
#include <realm/sync/noinst/server/server_history.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/history.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::sync;


// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment variable `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.


namespace {

class MyContext : public _impl::ServerHistory::Context {
public:
    std::mt19937_64& server_history_get_random() noexcept override final
    {
        return m_random;
    }

private:
    std::mt19937_64 m_random;
};

TEST(ServerLegacyMigration_ClientFileToCore6)
{
    std::string path = util::File::resolve("client_file_migration_core6.realm", "resources"); // Throws
    SHARED_GROUP_TEST_PATH(copy_path);
    util::File::copy(path, copy_path);

    std::unique_ptr<Replication> history = make_client_replication();
    // Upgrade not possible
    CHECK_THROW_ANY(DB::create(*history, copy_path));
}

} // unnamed namespace
