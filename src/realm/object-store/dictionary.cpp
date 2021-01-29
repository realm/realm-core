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
#include <realm/object-store/results.hpp>
#include <realm/table.hpp>

namespace realm::object_store {

Dictionary::Dictionary() noexcept
    : m_dict(nullptr)
{
}

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

Results Dictionary::snapshot() const
{
    return as_results().snapshot();
}

Results Dictionary::get_keys() const
{
    verify_attached();
    Results ret(m_realm, m_coll_base);
    ret.as_keys(true);
    return ret;
}

Results Dictionary::get_values() const
{
    return as_results();
}

class NotificationHandler {
public:
    NotificationHandler(realm::Dictionary& dict, Dictionary::CBFunc cb)
        : m_dict(dict)
        , m_cb(cb)
    {
    }
    void before(CollectionChangeSet const&) {}
    void after(CollectionChangeSet const& c)
    {
        DictionaryChangeSet changes;
        for (auto ndx : c.deletions.as_indexes()) {
            changes.deletions.push_back(ndx);
        }
        for (auto ndx : c.insertions.as_indexes()) {
            changes.insertions.push_back(m_dict.get_key(ndx));
        }
        for (auto ndx : c.modifications_new.as_indexes()) {
            changes.modifications.push_back(m_dict.get_key(ndx));
        }
        m_cb(changes, {});
    }
    void error(std::exception_ptr ptr)
    {
        m_cb({}, ptr);
    }

private:
    realm::Dictionary& m_dict;
    Dictionary::CBFunc m_cb;
};


NotificationToken Dictionary::add_key_based_notification_callback(CBFunc cb) &
{
    return add_notification_callback(NotificationHandler(*this->m_dict, cb));
}


Dictionary Dictionary::freeze(const std::shared_ptr<Realm>& frozen_realm) const
{
    auto frozen_collection = frozen_realm->import_copy_of(*m_dict);
    REALM_ASSERT(dynamic_cast<realm::Dictionary*>(frozen_collection.get()));
    realm::Dictionary* frozen_dict = static_cast<realm::Dictionary*>(frozen_collection.get());
    return Dictionary(frozen_realm, *frozen_dict);
}

} // namespace realm::object_store
