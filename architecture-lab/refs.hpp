/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
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

#ifndef __REFS_HPP__
#define __REFS_HPP__

#include <cstdint>

/* Typed references
 */
class DynType {};

template <typename T>
struct Ref {
    uint64_t r;
    void operator=(const Ref<T>& other) { r = other.r; }
    Ref(const Ref<T>& other) { r = other.r; }
    Ref() : r(0) {}
};

template <>
struct Ref<DynType> {
    uint64_t r;
    template <typename O>
    void operator=(const Ref<O>& other) { r = other.r; }
    template <typename O>
    Ref(const Ref<O>& other) { r = other.r; }
    Ref() : r(0) {}
    template <typename O>
    inline Ref<O> as() { Ref<O> res; res.r = r; return res; }
};

template <typename T>
static inline bool is_null(Ref<T> ref) { return ref.r == 0; }

#endif

