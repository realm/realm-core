/*************************************************************************
 *
 * Copyright 2017 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_UTIL_OVERLOAD_HPP
#define REALM_UTIL_OVERLOAD_HPP

namespace realm::util {
template <class... Ts>
struct overload : Ts... {
    using Ts::operator()...;
#ifdef _MSC_VER
    // https://developercommunity.visualstudio.com/t/runtime-stack-corruption-using-stdvisit/346200
    // A bug in VC++'s Empty Base Optimization causes it to compute the wrong
    // size if both the type and the last base class have zero size. This
    // results in the stack pointer being adjusted incorrectly if the final
    // lambda passed to overload has no captures. Making overload non-zero size
    // prevents this.
    char dummy = 0;
#endif
};
template <class... Ts>
overload(Ts...) -> overload<Ts...>;
} // namespace realm::util

#endif // REALM_UTIL_OVERLOAD_HPP
