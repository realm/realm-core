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
#include <realm/keys.hpp>
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
using CollectionListPtr = std::shared_ptr<CollectionList>;
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

// Given an object as starting point, a collection can be identified by
// a sequence of PathElements. The first element should always be a
// column key. The next elements are either an index into a list or a key
// to an entry in a dictionary
struct PathElement {
    union {
        std::string string_val;
        int64_t int_val;
    };
    enum Type { column, key, index } m_type;

    PathElement()
        : int_val(-1)
        , m_type(Type::column)
    {
    }
    PathElement(ColKey col_key)
        : int_val(col_key.value)
        , m_type(Type::column)
    {
    }
    PathElement(int ndx)
        : int_val(ndx)
        , m_type(Type::index)
    {
        REALM_ASSERT(ndx >= 0);
    }
    PathElement(size_t ndx)
        : int_val(int64_t(ndx))
        , m_type(Type::index)
    {
    }
    PathElement(StringData str)
        : string_val(str)
        , m_type(Type::key)
    {
    }
    PathElement(const char* str)
        : string_val(str)
        , m_type(Type::key)
    {
    }
    PathElement(const std::string& str)
        : string_val(str)
        , m_type(Type::key)
    {
    }
    PathElement(const PathElement& other)
        : m_type(other.m_type)
    {
        if (other.m_type == Type::key) {
            new (&string_val) std::string(other.string_val);
        }
        else {
            int_val = other.int_val;
        }
    }

    PathElement(PathElement&& other) noexcept
        : m_type(other.m_type)
    {
        if (other.m_type == Type::key) {
            new (&string_val) std::string(std::move(other.string_val));
        }
        else {
            int_val = other.int_val;
        }
    }
    ~PathElement()
    {
        if (m_type == Type::key) {
            string_val.std::string::~string();
        }
    }

    bool is_col_key() const noexcept
    {
        return m_type == Type::column;
    }
    bool is_ndx() const noexcept
    {
        return m_type == Type::index;
    }
    bool is_key() const noexcept
    {
        return m_type == Type::key;
    }

    ColKey get_col_key() const noexcept
    {
        REALM_ASSERT(is_col_key());
        return ColKey(int_val);
    }
    size_t get_ndx() const noexcept
    {
        REALM_ASSERT(is_ndx());
        return size_t(int_val);
    }
    const std::string& get_key() const noexcept
    {
        REALM_ASSERT(is_key());
        return string_val;
    }

    PathElement& operator=(const PathElement& other) = delete;

    bool operator==(const PathElement& other) const
    {
        if (m_type == other.m_type) {
            return (m_type == Type::key) ? string_val == other.string_val : int_val == other.int_val;
        }
        return false;
    }
    bool operator==(const char* str) const
    {
        return (m_type == Type::key) ? string_val == str : false;
    }
    bool operator==(size_t i) const
    {
        return (m_type == Type::index) ? size_t(int_val) == i : false;
    }
    bool operator==(ColKey ck) const
    {
        return (m_type == Type::column) ? int_val == ck.value : false;
    }
};

using Path = std::vector<PathElement>;

std::ostream& operator<<(std::ostream& ostr, const PathElement& elem);
std::ostream& operator<<(std::ostream& ostr, const Path& path);

// Path from the group level.
struct FullPath {
    TableKey top_table;
    ObjKey top_objkey;
    Path path_from_top;
};

using StableIndex = mpark::variant<ColKey, int64_t, std::string>;
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
