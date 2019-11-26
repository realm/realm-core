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

#ifndef REALM_COLLECTION_CHANGE_BUILDER_HPP
#define REALM_COLLECTION_CHANGE_BUILDER_HPP

#include "collection_notifications.hpp"

#include <realm/util/optional.hpp>

#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace realm {
namespace _impl {

class ObjectChangeSet {
public:
    using ColKeyType = uint64_t;
    using ObjectKeyType = int64_t;
    using ObjectSet = std::unordered_set<ObjectKeyType>;
    using ObjectMapToColumnSet = std::unordered_map<ObjectKeyType, std::unordered_set<ColKeyType>>;

    ObjectChangeSet() = default;
    ObjectChangeSet(ObjectChangeSet const&) = default;
    ObjectChangeSet(ObjectChangeSet&&) = default;
    ObjectChangeSet& operator=(ObjectChangeSet const&) = default;
    ObjectChangeSet& operator=(ObjectChangeSet&&) = default;

    void insertions_add(ObjectKeyType obj);
    void modifications_add(ObjectKeyType obj, ColKeyType col = -1);
    void deletions_add(ObjectKeyType obj);
    void clear(size_t old_size);

    bool insertions_remove(ObjectKeyType obj);
    bool modifications_remove(ObjectKeyType obj);
    bool deletions_remove(ObjectKeyType obj);

    bool insertions_contains(ObjectKeyType obj) const;
    bool modifications_contains(ObjectKeyType obj) const;
    bool deletions_contains(ObjectKeyType obj) const;
    // if the specified object has not been modified, returns util::Optional::None
    // if the object has been modified, returns the begin and end const_iterator into a vector of columns
    using ColKeyIterator = std::unordered_set<ColKeyType>::const_iterator;
    util::Optional<std::pair<ColKeyIterator, ColKeyIterator>> get_columns_modified(ObjectKeyType obj) const;

    bool insertions_empty() const noexcept { return m_insertions.empty(); }
    bool modifications_empty() const noexcept { return m_modifications.empty(); }
    bool deletions_empty() const noexcept { return m_deletions.empty(); }

    size_t insertions_size() const noexcept { return m_insertions.size(); }
    size_t modifications_size() const noexcept { return m_modifications.size(); }
    size_t deletions_size() const noexcept { return m_deletions.size(); }

    bool empty() const noexcept
    {
        return m_deletions.empty() && m_insertions.empty() && m_modifications.empty() && !m_clear_did_occur;
    }

    void merge(ObjectChangeSet&& other);
    void verify();

private:
    ObjectSet m_deletions;
    ObjectSet m_insertions;
    ObjectMapToColumnSet m_modifications;
    bool m_clear_did_occur = false;
};

class CollectionChangeBuilder : public CollectionChangeSet {
public:
    CollectionChangeBuilder(CollectionChangeBuilder const&) = default;
    CollectionChangeBuilder(CollectionChangeBuilder&&) = default;
    CollectionChangeBuilder& operator=(CollectionChangeBuilder const&) = default;
    CollectionChangeBuilder& operator=(CollectionChangeBuilder&&) = default;

    CollectionChangeBuilder(IndexSet deletions = {},
                            IndexSet insertions = {},
                            IndexSet modification = {},
                            std::vector<Move> moves = {});

    // Calculate where rows need to be inserted or deleted from old_rows to turn
    // it into new_rows, and check all matching rows for modifications
    static CollectionChangeBuilder calculate(std::vector<int64_t> const& old_rows,
                                             std::vector<int64_t> const& new_rows,
                                             std::function<bool (size_t)> row_did_change,
                                             bool in_table_order=false);

    // generic operations {
    CollectionChangeSet finalize() &&;
    void merge(CollectionChangeBuilder&&);

    void insert(size_t ndx, size_t count=1, bool track_moves=true);
    void modify(size_t ndx, size_t col=-1);
    void erase(size_t ndx);
    void clear(size_t old_size);
    // }

    // operations only implemented for LinkList semantics {
    void clean_up_stale_moves();
    void move(size_t from, size_t to);
    // }

private:
    bool m_track_columns = true;

    template<typename Func>
    void for_each_col(Func&& f);

    void verify();
};
} // namespace _impl
} // namespace realm

#endif // REALM_COLLECTION_CHANGE_BUILDER_HPP
