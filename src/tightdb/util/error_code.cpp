#include <tightdb/util/assert.hpp>
#include <tightdb/util/error_code.hpp>

using namespace std;
using namespace tightdb::util;


namespace {

class misc_category: public error_category {
    string message(int) const TIGHTDB_OVERRIDE;
};

misc_category g_misc_category;

string misc_category::message(int value) const
{
    switch (error::misc_errors(value)) {
        case error::unknown:
            return "Unknown error";
    }
    TIGHTDB_ASSERT(false);
    return string();
}

} // anonymous namespace


namespace tightdb {
namespace util {
namespace error {

error_code make_error_code(misc_errors err)
{
    return error_code(err, g_misc_category);
}

} // namespace error
} // namespace util
} // namespace tightdb
