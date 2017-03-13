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

using realm::SharedGroup;

namespace compatibility {

SharedGroup::DurabilityLevel durability(RealmDurability level)
{
    switch (level) {
    case RealmDurability::Full:
        return SharedGroup::durability_Full;
    case RealmDurability::MemOnly:
        return SharedGroup::durability_MemOnly;
    case RealmDurability::Async:
        return SharedGroup::durability_Async;
    }
    REALM_ASSERT(false); // unhandled case
    return SharedGroup::durability_Full;
}

SharedGroup* create_new_shared_group(std::string path, RealmDurability level, const char* key)
{
    return new SharedGroup(path, false, durability(level), key);
}

} // namespace compatibility

