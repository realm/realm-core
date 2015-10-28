#include "quote.hpp"

std::string realm::test_util::quote(const std::string& string)
{
    std::string quoted;
    for (char c: string) {
        if (c == '"' || c == '\\')
            quoted += '\\';
        quoted += c;
    }
    return quoted;
}
