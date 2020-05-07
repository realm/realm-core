#include "dump_changesets.hpp"

#include <locale>
#include <sstream>
#include <utility>

#include <realm/string_data.hpp>
#include <realm/util/file.hpp>

#include "unit_test.hpp"

namespace realm {
namespace test_util {

std::unique_ptr<TestDirNameGenerator> get_changeset_dump_dir_generator(const unit_test::TestContext& test_context,
                                                                       const char* env_var)
{
    auto dump_path = StringData{::getenv(env_var)};
    if (dump_path.size() == 0) {
        return nullptr;
    }

    std::ostringstream out;
    out.imbue(std::locale::classic());

    out << dump_path;
    util::try_make_dir(out.str());

    out << "/" << test_context.test_details.test_name;
    auto directory = out.str();
    util::try_make_dir(directory);

    return std::make_unique<TestDirNameGenerator>(std::move(directory));
}

} // namespace test_util
} // namespace realm
