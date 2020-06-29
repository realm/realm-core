/*************************************************************************
 *
 * Copyright 2019 Realm Inc.
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

#ifndef REALM_DICTIONARY_HPP
#define REALM_DICTIONARY_HPP

#include <realm/collection.hpp>

namespace realm {

class DictionaryClusterTree;

class Dictionary : public CollectionBase {
public:
    Dictionary(const Obj& obj, ColKey col_key)
        : CollectionBase(obj, col_key)
    {
        if (m_obj) {
            init_from_parent();
        }
    }
    void insert(Mixed key, Mixed value)
    {
        if (Replication* repl = this->m_obj.get_replication()) {
            repl->dictionary_insert(*this, key, value);
        }
    }


private:
    bool init_from_parent() const override
    {
        return false;
    }
};
} // namespace realm

#endif // REALM_DICTIONARY_HPP
