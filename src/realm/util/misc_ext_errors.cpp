#include <realm/util/assert.hpp>
#include <realm/util/misc_ext_errors.hpp>

using namespace realm;

util::MiscExtErrorCategory util::misc_ext_error_category;


const char* util::MiscExtErrorCategory::name() const noexcept
{
    return "realm.util.misc_ext";
}


std::string util::MiscExtErrorCategory::message(int value) const
{
    switch (MiscExtErrors(value)) {
        case MiscExtErrors::end_of_input:
            return "End of input";
        case MiscExtErrors::premature_end_of_input:
            return "Premature end of input";
        case MiscExtErrors::delim_not_found:
            return "Delimiter not found";
        case MiscExtErrors::operation_not_supported:
            return "Operation not supported";
    }
    REALM_ASSERT(false);
    return {};
}
