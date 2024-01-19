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

#include <realm/collection_parent.hpp>
#include "realm/list.hpp"
#include "realm/set.hpp"
#include "realm/dictionary.hpp"
#include "realm/util/overload.hpp"

#include <random>
#include <mutex>

namespace realm {

std::ostream& operator<<(std::ostream& ostr, const PathElement& elem)
{
    if (elem.is_ndx()) {
        size_t ndx = elem.get_ndx();
        if (ndx == 0) {
            ostr << "FIRST";
        }
        else if (ndx == size_t(-1)) {
            ostr << "LAST";
        }
        else {
            ostr << elem.get_ndx();
        }
    }
    else if (elem.is_col_key()) {
        ostr << elem.get_col_key();
    }
    else if (elem.is_key()) {
        ostr << "'" << elem.get_key() << "'";
    }
    else if (elem.is_all()) {
        ostr << '*';
    }

    return ostr;
}

std::ostream& operator<<(std::ostream& ostr, const Path& path)
{
    for (auto& elem : path) {
        ostr << '[' << elem << ']';
    }
    return ostr;
}

bool StablePath::is_prefix_of(const StablePath& other) const noexcept
{
    if (size() > other.size())
        return false;

    auto it = other.begin();
    for (auto& p : *this) {
        if (!(p == *it))
            return false;
        ++it;
    }
    return true;
}

/***************************** CollectionParent ******************************/

CollectionParent::~CollectionParent() {}

void CollectionParent::check_level() const
{
    if (m_level + 1 > s_max_level) {
        throw LogicError(ErrorCodes::LimitExceeded, "Max nesting level reached");
    }
}
void CollectionParent::set_backlink(ColKey col_key, ObjLink new_link) const
{
    if (new_link && new_link.get_obj_key()) {
        auto t = get_table();
        auto target_table = t->get_parent_group()->get_table(new_link.get_table_key());
        ColKey backlink_col_key;
        auto type = col_key.get_type();
        if (type == col_type_TypedLink || type == col_type_Mixed || col_key.is_dictionary()) {
            // This may modify the target table
            backlink_col_key = target_table->find_or_add_backlink_column(col_key, t->get_key());
            // it is possible that this was a link to the same table and that adding a backlink column has
            // caused the need to update this object as well.
            update_if_needed();
        }
        else {
            backlink_col_key = t->get_opposite_column(col_key);
        }
        auto obj_key = new_link.get_obj_key();
        auto target_obj = obj_key.is_unresolved() ? target_table->try_get_tombstone(obj_key)
                                                  : target_table->try_get_object(obj_key);
        if (!target_obj) {
            throw InvalidArgument(ErrorCodes::KeyNotFound, "Target object not found");
        }
        target_obj.add_backlink(backlink_col_key, get_object().get_key());
    }
}

bool CollectionParent::replace_backlink(ColKey col_key, ObjLink old_link, ObjLink new_link, CascadeState& state) const
{
    bool recurse = remove_backlink(col_key, old_link, state);
    set_backlink(col_key, new_link);

    return recurse;
}

bool CollectionParent::remove_backlink(ColKey col_key, ObjLink old_link, CascadeState& state) const
{
    if (old_link && old_link.get_obj_key()) {
        auto t = get_table();
        REALM_ASSERT(t->valid_column(col_key));
        ObjKey old_key = old_link.get_obj_key();
        auto target_obj = t->get_parent_group()->get_object(old_link);
        TableRef target_table = target_obj.get_table();
        ColKey backlink_col_key;
        auto type = col_key.get_type();
        if (type == col_type_TypedLink || type == col_type_Mixed || col_key.is_dictionary()) {
            backlink_col_key = target_table->find_or_add_backlink_column(col_key, t->get_key());
        }
        else {
            backlink_col_key = t->get_opposite_column(col_key);
        }

        bool strong_links = target_table->is_embedded();
        bool is_unres = old_key.is_unresolved();

        bool last_removed = target_obj.remove_one_backlink(backlink_col_key, get_object().get_key()); // Throws
        if (is_unres) {
            if (last_removed) {
                // Check is there are more backlinks
                if (!target_obj.has_backlinks(false)) {
                    // Tombstones can be erased right away - there is no cascading effect
                    target_table->m_tombstones->erase(old_key, state);
                }
            }
        }
        else {
            return state.enqueue_for_cascade(target_obj, strong_links, last_removed);
        }
    }

    return false;
}

LstBasePtr CollectionParent::get_listbase_ptr(ColKey col_key) const
{
    auto table = get_table();
    auto attr = table->get_column_attr(col_key);
    REALM_ASSERT(attr.test(col_attr_List) || attr.test(col_attr_Nullable));
    bool nullable = attr.test(col_attr_Nullable);

    switch (table->get_column_type(col_key)) {
        case type_Int: {
            if (nullable)
                return std::make_unique<Lst<util::Optional<Int>>>(col_key);
            else
                return std::make_unique<Lst<Int>>(col_key);
        }
        case type_Bool: {
            if (nullable)
                return std::make_unique<Lst<util::Optional<Bool>>>(col_key);
            else
                return std::make_unique<Lst<Bool>>(col_key);
        }
        case type_Float: {
            if (nullable)
                return std::make_unique<Lst<util::Optional<Float>>>(col_key);
            else
                return std::make_unique<Lst<Float>>(col_key);
        }
        case type_Double: {
            if (nullable)
                return std::make_unique<Lst<util::Optional<Double>>>(col_key);
            else
                return std::make_unique<Lst<Double>>(col_key);
        }
        case type_String: {
            return std::make_unique<Lst<String>>(col_key);
        }
        case type_Binary: {
            return std::make_unique<Lst<Binary>>(col_key);
        }
        case type_Timestamp: {
            return std::make_unique<Lst<Timestamp>>(col_key);
        }
        case type_Decimal: {
            return std::make_unique<Lst<Decimal128>>(col_key);
        }
        case type_ObjectId: {
            if (nullable)
                return std::make_unique<Lst<util::Optional<ObjectId>>>(col_key);
            else
                return std::make_unique<Lst<ObjectId>>(col_key);
        }
        case type_UUID: {
            if (nullable)
                return std::make_unique<Lst<util::Optional<UUID>>>(col_key);
            else
                return std::make_unique<Lst<UUID>>(col_key);
        }
        case type_TypedLink: {
            return std::make_unique<Lst<ObjLink>>(col_key);
        }
        case type_Mixed: {
            return std::make_unique<Lst<Mixed>>(col_key, get_level() + 1);
        }
        case type_Link:
            return std::make_unique<LnkLst>(col_key);
    }
    REALM_TERMINATE("Unsupported column type");
}

SetBasePtr CollectionParent::get_setbase_ptr(ColKey col_key) const
{
    auto table = get_table();
    auto attr = table->get_column_attr(col_key);
    REALM_ASSERT(attr.test(col_attr_Set));
    bool nullable = attr.test(col_attr_Nullable);

    switch (table->get_column_type(col_key)) {
        case type_Int:
            if (nullable)
                return std::make_unique<Set<util::Optional<Int>>>(col_key);
            return std::make_unique<Set<Int>>(col_key);
        case type_Bool:
            if (nullable)
                return std::make_unique<Set<util::Optional<Bool>>>(col_key);
            return std::make_unique<Set<Bool>>(col_key);
        case type_Float:
            if (nullable)
                return std::make_unique<Set<util::Optional<Float>>>(col_key);
            return std::make_unique<Set<Float>>(col_key);
        case type_Double:
            if (nullable)
                return std::make_unique<Set<util::Optional<Double>>>(col_key);
            return std::make_unique<Set<Double>>(col_key);
        case type_String:
            return std::make_unique<Set<String>>(col_key);
        case type_Binary:
            return std::make_unique<Set<Binary>>(col_key);
        case type_Timestamp:
            return std::make_unique<Set<Timestamp>>(col_key);
        case type_Decimal:
            return std::make_unique<Set<Decimal128>>(col_key);
        case type_ObjectId:
            if (nullable)
                return std::make_unique<Set<util::Optional<ObjectId>>>(col_key);
            return std::make_unique<Set<ObjectId>>(col_key);
        case type_UUID:
            if (nullable)
                return std::make_unique<Set<util::Optional<UUID>>>(col_key);
            return std::make_unique<Set<UUID>>(col_key);
        case type_TypedLink:
            return std::make_unique<Set<ObjLink>>(col_key);
        case type_Mixed:
            return std::make_unique<Set<Mixed>>(col_key);
        case type_Link:
            return std::make_unique<LnkSet>(col_key);
    }
    REALM_TERMINATE("Unsupported column type.");
}

CollectionBasePtr CollectionParent::get_collection_ptr(ColKey col_key) const
{
    if (col_key.is_list()) {
        return get_listbase_ptr(col_key);
    }
    else if (col_key.is_set()) {
        return get_setbase_ptr(col_key);
    }
    else if (col_key.is_dictionary()) {
        return std::make_unique<Dictionary>(col_key, get_level() + 1);
    }
    return {};
}


int64_t CollectionParent::generate_key(size_t sz)
{
    static std::mt19937 gen32;
    static std::mutex mutex;

    int64_t key;
    const std::lock_guard<std::mutex> lock(mutex);
    do {
        if (sz < 0x10) {
            key = int8_t(gen32());
        }
        else if (sz < 0x1000) {
            key = int16_t(gen32());
        }
        else {
            key = int32_t(gen32());
        }
    } while (key == 0);

    return key;
}


} // namespace realm
