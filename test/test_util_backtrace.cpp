#include "test.hpp"

#include <realm/util/backtrace.hpp>
#include <realm/exceptions.hpp>
#include <realm/string_data.hpp>

using namespace realm;
using namespace realm::util;

REALM_NOINLINE void throw_logic_error(LogicError::ErrorKind kind)
{
    throw LogicError{kind};
}

TEST(Backtrace_LogicError)
{
    try {
        throw_logic_error(LogicError::string_too_big);
    }
    catch (const LogicError& err) {
        // arm requires -funwind-tables to make backtraces, and that increases binary size.
#if REALM_PLATFORM_APPLE || (defined(__linux__) && !REALM_ANDROID && !defined(__arm__))
        if (!CHECK(StringData{err.what()}.contains("throw_logic_error")))
            std::cout << err.what() << std::endl;

#endif
        LogicError copy = err;
        CHECK_EQUAL(StringData{copy.what()}, StringData{err.what()});
    }
}
