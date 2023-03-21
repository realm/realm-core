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

#include <external/mpark/variant.hpp>

namespace realm {

class Obj;
class Replication;
class CascadeState;

class CollectionBase;
class CollectionList;
class LstBase;
class SetBase;
class Dictionary;

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

class CollectionParent {
public:
    using Index = mpark::variant<ColKey, int64_t, std::string>;

    virtual ~CollectionParent();
    virtual size_t get_level() const noexcept = 0;
    virtual Replication* get_replication() const = 0;
    virtual UpdateStatus update_if_needed_with_status() const = 0;
    virtual bool update_if_needed() const = 0;
    virtual int_fast64_t bump_content_version() = 0;
    virtual void bump_both_versions() = 0;
    virtual TableRef get_table() const noexcept = 0;
    virtual ColKey get_col_key() const noexcept
    {
        return {};
    }
    virtual const Obj& get_object() const noexcept = 0;
    virtual ref_type get_collection_ref(Index) const noexcept = 0;
    virtual void set_collection_ref(Index, ref_type ref) = 0;

    // Used when inserting a new link. You will not remove existing links in this process
    void set_backlink(ColKey col_key, ObjLink new_link) const;
    // Used when replacing a link, return true if CascadeState contains objects to remove
    bool replace_backlink(ColKey col_key, ObjLink old_link, ObjLink new_link, CascadeState& state) const;
    // Used when removing a backlink, return true if CascadeState contains objects to remove
    bool remove_backlink(ColKey col_key, ObjLink old_link, CascadeState& state) const;

protected:
    LstBasePtr get_listbase_ptr(ColKey col_key) const;
    SetBasePtr get_setbase_ptr(ColKey col_key) const;
    DictionaryPtr get_dictionary_ptr(ColKey col_key) const;
    CollectionBasePtr get_collection_ptr(ColKey col_key) const;
};

} // namespace realm

#endif
