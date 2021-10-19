#include <stdexcept>
#include <set>
#include <random>

#include <realm/db.hpp>
#include <realm/util/file.hpp>
#include <realm/sync/noinst/server/server_history.hpp>
#include <realm/sync/noinst/server/server_legacy_migration.hpp>

using namespace realm;

namespace {

class HistoryContext : public _impl::ServerHistory::Context {
public:
    std::mt19937_64& server_history_get_random() noexcept override final
    {
        return m_random;
    }

private:
    std::mt19937_64 m_random;
};

inline bool ends_with(const std::string& str, const char* suffix) noexcept
{
    auto data = str.data();
    auto size = str.size();
    auto suffix_size = std::strlen(suffix);
    return suffix_size <= size && std::equal(data + size - suffix_size, data + size, suffix);
}

bool check_legacy_format_1(const Group& group)
{
    using gf = _impl::GroupFriend;
    const Allocator& alloc = gf::get_alloc(const_cast<Group&>(group));
    ref_type top_ref = gf::get_top_ref(const_cast<Group&>(group));
    _impl::History::version_type dummy_version = 0;
    int history_type = 0;
    int history_schema_version = 0;
    gf::get_version_and_history_info(alloc, top_ref, dummy_version, history_type, history_schema_version);
    bool good = (history_type == Replication::hist_SyncServer && history_schema_version == 0);
    return good;
}

bool try_migrate_file(const std::string original_path, const std::string& new_path)
{
    HistoryContext context; // Throws
    _impl::ServerHistory::DummyCompactionControl compaction_control;
    Group legacy_group{original_path}; // Throws
    if (check_legacy_format_1(legacy_group)) {
        _impl::ServerHistory new_history{context, compaction_control}; // Throws
        DBRef new_shared_group = DB::create(new_history, new_path);    // Throws
        return true;
    }
    else {
        // Assume that this failure is because the Realm file was already
        // migrated, but verify the assumption by opening it with the right
        // history type.
        _impl::ServerHistory history{context, compaction_control}; // Throws
        DBRef db = DB::create(history, original_path);             // Throws
        return false;
    }
}

void migrate_file_safely(const std::string& realm_file, const std::string& temp_file_1,
                         const std::string& temp_file_2, const std::string& backup_file)
{
    std::string lock_file = DB::get_core_file(realm_file, DB::CoreFileType::Lock); // Throws
    util::File lock{lock_file, util::File::mode_Write};                            // Throws
    lock.lock_exclusive();                                                         // Throws
    util::File::UnlockGuard ug{lock};
    util::File::copy(realm_file, temp_file_1);                              // Throws
    util::File::try_remove(temp_file_2);                                    // Throws
    bool migration_was_needed = try_migrate_file(temp_file_1, temp_file_2); // Throws
    if (migration_was_needed) {
        // Just-in-time backup of the original Realm file.
        util::File::copy(realm_file, backup_file); // Throws
        // Replace original Realm file with the migrated one.
        util::File::move(temp_file_2, realm_file); // Throws
    }
}

} // unnamed namespace


namespace realm {
namespace _impl {

void ensure_legacy_migration_1(const std::string& realms_dir, const std::string& migration_dir, util::Logger& logger)
{
    std::string completed_file = util::File::resolve("completed_1", migration_dir); // Throws
    if (!util::File::exists(completed_file)) {                                      // Throws
        util::try_make_dir(migration_dir);                                          // Throws

        // Find all the Realm files
        std::set<std::string> realm_dirs;
        std::set<std::string> realm_files;
        auto file_handler = [&](const std::string& file, const std::string& dir) {
            if (ends_with(file, ".realm")) {
                if (!dir.empty())
                    realm_dirs.insert(dir);                          // Throws
                std::string file_2 = util::File::resolve(file, dir); // Throws
                realm_files.insert(file_2);                          // Throws
            }
            return true; // Continue
        };
        util::File::for_each(realms_dir, file_handler); // Throws

        if (!realm_files.empty()) {
            logger.info("Migration required");
            logger.info("Found %1 Realm files in %2", realm_files.size(), realms_dir); // Throws

            // Ensure that we have a backup directory with a subdirectory
            // structure matching the one in the Realms directory.
            std::string backup_dir = util::File::resolve("backup_1", migration_dir); // Throws
            util::try_make_dir(backup_dir);                                          // Throws
            for (const auto& dir : realm_dirs) {
                std::string dir_2 = util::File::resolve(dir, backup_dir); // Throws
                util::try_make_dir(dir_2);                                // Throws
            }

            // Setup a directory for temporary files
            std::string temp_dir = util::File::resolve("temp_1", migration_dir); // Throws
            util::try_make_dir(temp_dir);                                        // Throws
            std::string temp_file_1 = util::File::resolve("1", temp_dir);        // Throws
            std::string temp_file_2 = util::File::resolve("2", temp_dir);        // Throws

            // Migrate the Realm files.
            std::size_t n = 1;
            for (const auto& file : realm_files) {
                logger.info("Migrating %1 (%2/%3)", file, n, realm_files.size());
                std::string realm_file = util::File::resolve(file, realms_dir);         // Throws
                std::string backup_file = util::File::resolve(file, backup_dir);        // Throws
                migrate_file_safely(realm_file, temp_file_1, temp_file_2, backup_file); // Throws
                ++n;
            }

            util::remove_dir_recursive(temp_dir); // Throws
            logger.info("Migration completed successfully");
        }
        util::File(completed_file, util::File::mode_Write); // Throws
    }
}

} // namespace _impl
} // namespace realm
