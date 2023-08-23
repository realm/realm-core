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
#include <realm/table_ref.hpp>
#include <realm/path.hpp>
#include <realm/mixed.hpp>

#include <external/mpark/variant.hpp>

namespace realm {

class Obj;
class Replication;
class CascadeState;

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

/*
 * In order to detect stale collection objects (objects referring to entities that have
 * been deleted from the DB) nested directly in an Obj object, we need a structure that
 * both holds an index of the relevant column as well as a somewhat unique key. The key
 * is generated when the collection is assigned to the property and stored alongside the
 * ref of the collection. The stored key is regenerated/cleared when a new value is
 * assigned to the property.
 */
class ColIndex {
public:
    ColIndex()
    {
        value.col_index = 0x7fff;
    }
    ColIndex(ColKey col_key, int64_t key)
    {
        value.col_index = col_key.get_index().val;
        value.is_collection = col_key.is_collection();
        value.key = uint16_t(key);
    }
    ColKey::Idx get_index() const noexcept
    {
        return {unsigned(value.col_index)};
    }
    int64_t get_key() const noexcept
    {
        return int16_t(value.key);
    }
    bool is_collection() const noexcept
    {
        return value.is_collection;
    }

    bool operator==(const ColIndex& other) const noexcept
    {
        // Compare only index
        return value.col_index == other.value.col_index;
    }

private:
    struct {
        uint32_t col_index : 15;
        uint32_t is_collection : 1;
        uint32_t key : 16;
    } value;
};

static_assert(sizeof(ColIndex) == sizeof(uint32_t));

using StableIndex = mpark::variant<ColIndex, int64_t, std::string>;
using StablePath = std::vector<StableIndex>;

class CollectionParent : public std::enable_shared_from_this<CollectionParent> {
public:
    using Index = StableIndex;

    // Return the nesting level of the parent
    size_t get_level() const noexcept
    {
        return m_level;
    }
    void check_level() const;
    // Return the path to this object. The path is calculated from
    // the topmost Obj - which must be an Obj with a primary key.
    virtual FullPath get_path() const = 0;
    // Return path from owning object
    virtual Path get_short_path() const = 0;
    // Return path from owning object
    virtual StablePath get_stable_path() const = 0;
    // Add a translation of Index to PathElement
    virtual void add_index(Path& path, Index ndx) const = 0;
    /// Get table of owning object
    virtual TableRef get_table() const noexcept = 0;

protected:
    friend class Collection;
    template <class>
    friend class CollectionBaseImpl;
    friend class CollectionList;

#ifdef REALM_DEBUG
    static constexpr size_t s_max_level = 4;
#else
    static constexpr size_t s_max_level = 100;
#endif
    size_t m_level = 0;

    constexpr CollectionParent(size_t level = 0)
        : m_level(level)
    {
    }

    virtual ~CollectionParent();
    /// Update the accessor (and return `UpdateStatus::Detached` if the parent
    /// is no longer valid, rather than throwing an exception).
    virtual UpdateStatus update_if_needed_with_status() const noexcept = 0;
    /// Check if the storage version has changed and update if it has
    /// Return true if the object was updated
    virtual bool update_if_needed() const = 0;
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

    // Used when inserting a new link. You will not remove existing links in this process
    void set_backlink(ColKey col_key, ObjLink new_link) const;
    // Used when replacing a link, return true if CascadeState contains objects to remove
    bool replace_backlink(ColKey col_key, ObjLink old_link, ObjLink new_link, CascadeState& state) const;
    // Used when removing a backlink, return true if CascadeState contains objects to remove
    bool remove_backlink(ColKey col_key, ObjLink old_link, CascadeState& state) const;

    LstBasePtr get_listbase_ptr(ColKey col_key) const;
    SetBasePtr get_setbase_ptr(ColKey col_key) const;
    CollectionBasePtr get_collection_ptr(ColKey col_key) const;

    static int64_t generate_key(size_t sz);
};

} // namespace realm

#endif
