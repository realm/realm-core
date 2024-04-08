/*************************************************************************
 *
 * Copyright 2023 Realm Inc.
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

#ifndef REALM_COLLECTION_PARENT_HPP
#define REALM_COLLECTION_PARENT_HPP

#include <realm/alloc.hpp>
#include <realm/mixed.hpp>
#include <realm/path.hpp>
#include <realm/table_ref.hpp>

namespace realm {

class Obj;
class Replication;
class CascadeState;
class BPlusTreeMixed;

class Collection;
class CollectionBase;
class CollectionList;
class LstBase;
class SetBase;
class Dictionary;

template <class T>
class Set;

template <class T>
class Lst;

using CollectionPtr = std::shared_ptr<Collection>;
using LstBasePtr = std::unique_ptr<LstBase>;
using SetBasePtr = std::unique_ptr<SetBase>;
using CollectionBasePtr = std::shared_ptr<CollectionBase>;
using ListMixedPtr = std::shared_ptr<Lst<Mixed>>;
using DictionaryPtr = std::shared_ptr<Dictionary>;
using SetMixedPtr = std::shared_ptr<Set<Mixed>>;

/// The status of an accessor after a call to `update_if_needed()`.
enum class UpdateStatus {
    /// The owning object or column no longer exist, and the accessor could
    /// not be updated. The accessor should be left in a detached state
    /// after this, and further calls to `update_if_needed()` are not
    /// guaranteed to reattach the accessor.
    Detached,

    /// The underlying data of the accessor was changed since the last call
    /// to `update_if_needed()`. The accessor is still valid.
    Updated,

    /// The underlying data of the accessor did not change since the last
    /// call to `update_if_needed()`, and the accessor is still valid in its
    /// current state.
    NoChange,
};

class CollectionParent : public std::enable_shared_from_this<CollectionParent> {
public:
    using Index = StableIndex;

    // Return the nesting level of the parent
    uint8_t get_level() const noexcept
    {
        return m_level;
    }
    void check_level() const;
    // Return the path to this object. The path is calculated from
    // the topmost Obj - which must be an Obj with a primary key.
    virtual FullPath get_path() const = 0;
    // Return path from owning object
    virtual Path get_short_path() const = 0;
    // Return column of owning property
    virtual ColKey get_col_key() const noexcept = 0;
    // Return path from owning object
    virtual StablePath get_stable_path() const = 0;
    // Add a translation of Index to PathElement
    virtual void add_index(Path& path, const Index& ndx) const = 0;
    // Return position of Index held by child
    virtual size_t find_index(const Index& ndx) const = 0;
    /// Get table of owning object
    virtual TableRef get_table() const noexcept = 0;
    // Reread the content version from the allocator. Called when a child makes
    // a write to mark the already up-to-date parent as still being up-to-date.
    virtual void update_content_version() const noexcept = 0;

    static LstBasePtr get_listbase_ptr(ColKey col_key, uint8_t level);
    static SetBasePtr get_setbase_ptr(ColKey col_key, uint8_t level);
    static CollectionBasePtr get_collection_ptr(ColKey col_key, uint8_t level);

    static int64_t generate_key(size_t sz);
    static void set_key(BPlusTreeMixed& tree, size_t index);

protected:
    friend class Collection;
    template <class>
    friend class CollectionBaseImpl;
    friend class CollectionList;

    static constexpr size_t s_max_level = 100;
    uint8_t m_level = 0;

    constexpr CollectionParent(uint8_t level = 0)
        : m_level(level)
    {
    }

    virtual ~CollectionParent();
    /// Update the accessor (and return `UpdateStatus::Detached` if the
    // collection is not initialized.
    virtual UpdateStatus update_if_needed() const = 0;
    /// Get owning object
    virtual const Obj& get_object() const noexcept = 0;
    /// Get the top ref from pareht
    virtual ref_type get_collection_ref(Index, CollectionType) const = 0;
    /// Check if we can possibly get a ref
    virtual bool check_collection_ref(Index, CollectionType) const noexcept
    {
        return true;
    }
    /// Set the top ref in parent
    virtual void set_collection_ref(Index, ref_type ref, CollectionType) = 0;

    /// Get the counter which is incremented whenever the root Obj is updated.
    virtual uint32_t parent_version() const noexcept = 0;
};

} // namespace realm

#endif
