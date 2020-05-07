#ifndef REALM_UTIL_DEMANGLE_HPP
#define REALM_UTIL_DEMANGLE_HPP

#include <typeinfo>
#include <string>

namespace realm {
namespace util {


/// Demangle the specified C++ ABI identifier.
///
/// See for example
/// http://gcc.gnu.org/onlinedocs/libstdc++/latest-doxygen/namespaceabi.html
std::string demangle(const std::string&);


/// Get the demangled name of the specified type.
template <class T>
inline std::string get_type_name()
{
    return demangle(typeid(T).name());
}


/// Get the demangled name of the type of the specified argument.
template <class T>
inline std::string get_type_name(const T& v)
{
    return demangle(typeid(v).name());
}


} // namespace util
} // namespace realm

#endif // REALM_UTIL_DEMANGLE_HPP
