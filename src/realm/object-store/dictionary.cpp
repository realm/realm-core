/////////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/object-store/dictionary.hpp>
#include <realm/table.hpp>

namespace realm::object_store {

Dictionary::Dictionary(std::shared_ptr<Realm> r, const Obj& parent_obj, ColKey col)
    : Collection(std::move(r), parent_obj, col)
    , m_dict(dynamic_cast<realm::Dictionary*>(m_coll_base.get()))
{
    REALM_ASSERT(m_dict);
}

Dictionary::Dictionary(std::shared_ptr<Realm> r, const realm::Dictionary& dict)
    : Collection(std::move(r), dict)
    , m_dict(dynamic_cast<realm::Dictionary*>(m_coll_base.get()))
{
    REALM_ASSERT(m_dict);
}

Dictionary::~Dictionary() {}

Obj Dictionary::get_object(StringData key) const
{
    auto k = m_dict->get(key).get<ObjKey>();
    return m_dict->get_target_table()->get_object(k);
}

} // namespace realm::object_store
