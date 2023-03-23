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

using CollectionPtr = std::shared_ptr<Collection>;
using LstBasePtr = std::unique_ptr<LstBase>;
using SetBasePtr = std::unique_ptr<SetBase>;
using CollectionBasePtr = std::unique_ptr<CollectionBase>;
using CollectionListPtr = std::shared_ptr<CollectionList>;
using DictionaryPtr = std::unique_ptr<Dictionary>;

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


using PathElement = Mixed;
using Path = std::vector<PathElement>;

// Path from the group level.
struct FullPath {
    TableKey top_table;
    ObjKey top_objkey;
    Path path_from_top;
};

class CollectionParent {
public:
    using Index = mpark::variant<ColKey, int64_t, std::string>;

    // Return the nesting level of the parent
    virtual size_t get_level() const noexcept = 0;
    /// Get table of owning object
    virtual TableRef get_table() const noexcept = 0;

protected:
    template <class>
    friend class CollectionBaseImpl;
    friend class CollectionList;

    virtual ~CollectionParent();
    /// Update the accessor (and return `UpdateStatus::Detached` if the parent
    /// is no longer valid, rather than throwing an exception).
    virtual UpdateStatus update_if_needed_with_status() const = 0;
    /// Check if the storage version has changed and update if it has
    /// Return true if the object was updated
    virtual bool update_if_needed() const = 0;
    /// Get owning object
    virtual const Obj& get_object() const noexcept = 0;
    /// Get the top ref from pareht
    virtual ref_type get_collection_ref(Index) const noexcept = 0;
    /// Set the top ref from pareht
    virtual void set_collection_ref(Index, ref_type ref) = 0;

    // Used when inserting a new link. You will not remove existing links in this process
    void set_backlink(ColKey col_key, ObjLink new_link) const;
    // Used when replacing a link, return true if CascadeState contains objects to remove
    bool replace_backlink(ColKey col_key, ObjLink old_link, ObjLink new_link, CascadeState& state) const;
    // Used when removing a backlink, return true if CascadeState contains objects to remove
    bool remove_backlink(ColKey col_key, ObjLink old_link, CascadeState& state) const;

    LstBasePtr get_listbase_ptr(ColKey col_key) const;
    SetBasePtr get_setbase_ptr(ColKey col_key) const;
    DictionaryPtr get_dictionary_ptr(ColKey col_key) const;
    CollectionBasePtr get_collection_ptr(ColKey col_key) const;
};

// Used in Cluster when removing owning object
class DummyParent : public CollectionParent {
public:
    DummyParent(TableRef t, ref_type ref)
        : m_table(t)
        , m_ref(ref)
    {
    }
    size_t get_level() const noexcept final
    {
        return 0;
    }
    TableRef get_table() const noexcept final
    {
        return m_table;
    }

protected:
    TableRef m_table;
    ref_type m_ref;
    UpdateStatus update_if_needed_with_status() const final
    {
        return UpdateStatus::Updated;
    }
    bool update_if_needed() const final
    {
        return true;
    }
    const Obj& get_object() const noexcept final;
    ref_type get_collection_ref(Index) const noexcept final
    {
        return m_ref;
    }
    void set_collection_ref(Index, ref_type) {}
};

} // namespace realm

#endif
