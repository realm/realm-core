#include <utility>

#include <realm/util/assert.hpp>
#include <realm/util/enum.hpp>


namespace realm {
namespace _impl {

EnumMapper::EnumMapper(const util::EnumAssoc* map, bool ignore_case)
{
    const std::ctype<char>& ctype = std::use_facet<std::ctype<char>>(std::locale::classic());
    const util::EnumAssoc* i = map;
    while (i->name) {
        std::string name = i->name;                       // Throws (copy)
        auto p_1 = value_to_name.emplace(i->value, name); // Throws
        bool was_inserted_1 = p_1.second;
        REALM_ASSERT(was_inserted_1);
        if (ignore_case)
            ctype.tolower(&*name.begin(), &*name.end());
        auto p_2 = name_to_value.emplace(name, i->value);
        bool was_inserted_2 = p_2.second;
        REALM_ASSERT(was_inserted_2);
        ++i;
    }
}


bool EnumMapper::parse(const std::string& string, int& value, bool ignore_case) const
{
    std::string string_2 = string; // Throws (copy)
    if (ignore_case) {
        const std::ctype<char>& ctype = std::use_facet<std::ctype<char>>(std::locale::classic());
        ctype.tolower(&*string_2.begin(), &*string_2.end());
    }
    auto i = name_to_value.find(string_2);
    if (i == name_to_value.end())
        return false;
    value = i->second;
    return true;
}

} // namespace _impl
} // namespace realm
