#include <realm/util/features.h>
#include <realm/util/assert.hpp>
#include <realm/util/misc_errors.hpp>

using namespace realm::util;


namespace {

class misc_category: public std::error_category {
    const char* name() const noexcept override;
    std::string message(int) const override;
};

misc_category g_misc_category;

const char* misc_category::name() const noexcept
{
    return "tigthdb.misc";
}

std::string misc_category::message(int value) const
{
    switch (error::misc_errors(value)) {
        case error::unknown:
            return "Unknown error";
    }
    REALM_ASSERT(false);
    return std::string();
}

} // anonymous namespace


namespace realm {
namespace util {
namespace error {

std::error_code make_error_code(misc_errors err)
{
    return std::error_code(err, g_misc_category);
}

} // namespace error
} // namespace util
} // namespace realm
