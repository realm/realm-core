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

#ifndef REALM_ARRAY_BACKLINK_HPP
#define REALM_ARRAY_BACKLINK_HPP

#include <realm/cluster.hpp>

namespace realm {
class ArrayBacklink : private Array {
public:
    using Array::Array;
    using Array::init_from_ref;
    using Array::set_parent;
    using Array::init_from_parent;
    using Array::update_parent;
    using Array::get_ref;
    using Array::truncate_and_destroy_children;

    static int64_t default_value(bool)
    {
        return 0;
    }

    void create()
    {
        Array::create(type_HasRefs);
    }

    void insert(size_t ndx, int64_t val)
    {
        Array::insert(ndx, val);
    }

    int64_t get(size_t ndx) const
    {
        return Array::get(ndx);
    }

    void add(int64_t val)
    {
        Array::add(val);
    }

    void erase(size_t ndx);
    void add(size_t ndx, Key key);
    void remove(size_t ndx, Key key);
    size_t get_backlink_count(size_t ndx) const;
    Key get_backlink(size_t ndx, size_t index) const;
};
}

#endif /* SRC_REALM_ARRAY_KEY_HPP_ */
