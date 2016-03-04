#include <realm/util/inspect.hpp>

#include <sstream>

namespace realm {
namespace util {

std::string inspect(const std::string& str)
{
    std::string result;
    result.reserve(str.size() + 2);
    result += "\"";
    result += str;
    result += "\"";
    return result;
}

template<>
std::string inspect(const void* ptr)
{
    std::stringstream ss;
    ss << ptr;
    return ss.str();
}

std::string inspect_pointer(const char* type_name, const void* ptr)
{
    std::stringstream ss;
    ss << "(" << type_name << "*)" << ptr;
    return ss.str();
}

} // namespace util
} // namespace realm
