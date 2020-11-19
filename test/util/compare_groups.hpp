#ifndef REALM_TEST_UTIL_COMPARE_GROUPS_HPP
#define REALM_TEST_UTIL_COMPARE_GROUPS_HPP

#include <functional>

#include <realm/util/logger.hpp>
#include <realm/group.hpp>
#include <realm/table.hpp>

namespace realm {
namespace sync {
struct TableInfoCache;
} // namespace sync
namespace test_util {

bool compare_tables(const Table& table_1, const Table& table_2, util::Logger&);

bool compare_tables(const Table& table_1, const Table& table_2);

bool compare_groups(const Transaction& group_1, const Transaction& group_2);

bool compare_groups(const Transaction& group_1, const Transaction& group_2, util::Logger&);

bool compare_groups(const Transaction& group_1, const Transaction& group_2,
                    std::function<bool(StringData table_name)> filter_func, util::Logger&);


// Implementation

inline bool compare_groups(const Transaction& group_1, const Transaction& group_2, util::Logger& logger)
{
    return compare_groups(
        group_1, group_2,
        [](StringData) {
            return true;
        },
        logger);
}

} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_COMPARE_GROUPS_HPP
