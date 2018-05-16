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

#include "compatibility.hpp"

using realm::DB;
using realm::DBOptions;

namespace compatibility {

DBOptions::Durability durability(RealmDurability level)
{
    switch (level) {
    case RealmDurability::Full:
        return DBOptions::Durability::Full;
    case RealmDurability::MemOnly:
        return DBOptions::Durability::MemOnly;
    case RealmDurability::Async:
        return DBOptions::Durability::Async;
    }
    REALM_ASSERT(false); // unhandled case
    return DBOptions::Durability::Full;
}

realm::DBRef create_new_shared_group(std::string path, RealmDurability level, const char* key)
{
    return DB::create(path, false, DBOptions(durability(level), key));
}

} // end namespace compatibility

