#ifndef REALM_UTIL_OVERLOADED_HPP
#define REALM_UTIL_OVERLOADED_HPP

namespace realm::util {

// FIXME: Move this utility into Core.
template <class... Fs>
struct overloaded : Fs... {
    using Fs::operator()...;
};
template <class... Fs>
overloaded(Fs...)->overloaded<Fs...>;

} // namespace realm::util

#endif // REALM_UTIL_OVERLOADED_HPP
