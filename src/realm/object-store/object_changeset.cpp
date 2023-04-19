////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
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

#include <realm/object-store/object_changeset.hpp>

using namespace realm;

void ObjectChangeSet::insertions_add(ObjKey obj)
{
    m_insertions.insert(obj);
}

void ObjectChangeSet::modifications_add(ObjKey obj, ColKey col)
{
    // don't report modifications on new objects
    if (m_insertions.find(obj) == m_insertions.end()) {
        m_modifications[obj].insert(col);
    }
}

void ObjectChangeSet::deletions_add(ObjKey obj)
{
    m_modifications.erase(obj);
    size_t num_inserts_removed = m_insertions.erase(obj);
    if (num_inserts_removed == 0) {
        m_deletions.insert(obj);
    }
}

bool ObjectChangeSet::insertions_remove(ObjKey obj)
{
    return m_insertions.erase(obj) > 0;
}

bool ObjectChangeSet::modifications_remove(ObjKey obj)
{
    return m_modifications.erase(obj) > 0;
}

bool ObjectChangeSet::deletions_remove(ObjKey obj)
{
    return m_deletions.erase(obj) > 0;
}

bool ObjectChangeSet::deletions_contains(ObjKey obj) const
{
    return m_deletions.count(obj) > 0;
}

bool ObjectChangeSet::insertions_contains(ObjKey obj) const
{
    return m_insertions.count(obj) > 0;
}

bool ObjectChangeSet::modifications_contains(ObjKey obj, const std::vector<ColKey>& filtered_column_keys) const
{
    // If there is no filter we just check if the object in question was changed which means its key (`obj`)
    // can be found within the `m_modifications`.
    if (filtered_column_keys.size() == 0) {
        return m_modifications.count(obj) > 0;
    }

    // If a filter is set but the `obj` is not contained within the `m_modifcations` at all we do not need to check
    // further.
    if (m_modifications.count(obj) == 0) {
        return false;
    }

    // If a filter was set we need to check if the changed column is part of this filter.
    const std::unordered_set<ColKey>& changed_columns_for_object = m_modifications.at(obj);
    for (const auto& column_key_in_filter : filtered_column_keys) {
        if (changed_columns_for_object.count(column_key_in_filter)) {
            return true;
        }
    }

    return false;
}

const ObjectChangeSet::ColumnSet* ObjectChangeSet::get_columns_modified(ObjKey obj) const
{
    auto it = m_modifications.find(obj);
    if (it == m_modifications.end()) {
        return nullptr;
    }
    return &it->second;
}

void ObjectChangeSet::merge(ObjectChangeSet&& other)
{
    if (other.empty())
        return;
    if (empty()) {
        *this = std::move(other);
        return;
    }

    verify();
    other.verify();

    // Drop any inserted-then-deleted rows, then merge in new insertions
    for (auto it = other.m_deletions.begin(); it != other.m_deletions.end();) {
        m_modifications.erase(*it);
        if (m_insertions.erase(*it)) {
            it = m_deletions.erase(it);
        }
        else {
            ++it;
        }
    }
    m_insertions.merge(std::move(other.m_insertions));
    m_deletions.merge(std::move(other.m_deletions));
    for (auto& obj : other.m_modifications) {
        m_modifications[obj.first].merge(std::move(obj.second));
    }

    verify();

    other = {};
}

void ObjectChangeSet::verify()
{
#ifdef REALM_DEBUG
    for (auto it = m_deletions.begin(); it != m_deletions.end(); ++it) {
        REALM_ASSERT(m_modifications.find(*it) == m_modifications.end());
        REALM_ASSERT(m_insertions.find(*it) == m_insertions.end());
    }
#endif
}
