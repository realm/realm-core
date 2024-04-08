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
    return std::equal(begin(), end(), other.begin());
}

/***************************** CollectionParent ******************************/

CollectionParent::~CollectionParent() {}

void CollectionParent::check_level() const
{
    if (size_t(m_level) + 1 > s_max_level) {
        throw LogicError(ErrorCodes::LimitExceeded, "Max nesting level reached");
    }
}

template <typename Base, template <typename> typename Collection, typename LinkCol>
std::unique_ptr<Base> create_collection(ColKey col_key, uint8_t level)
{
    bool nullable = col_key.get_attrs().test(col_attr_Nullable);
    switch (col_key.get_type()) {
        case col_type_Int:
            if (nullable)
                return std::make_unique<Collection<util::Optional<Int>>>(col_key);
            return std::make_unique<Collection<Int>>(col_key);
        case col_type_Bool:
            if (nullable)
                return std::make_unique<Collection<util::Optional<Bool>>>(col_key);
            return std::make_unique<Collection<Bool>>(col_key);
        case col_type_Float:
            if (nullable)
                return std::make_unique<Collection<util::Optional<Float>>>(col_key);
            return std::make_unique<Collection<Float>>(col_key);
        case col_type_Double:
            if (nullable)
                return std::make_unique<Collection<util::Optional<Double>>>(col_key);
            return std::make_unique<Collection<Double>>(col_key);
        case col_type_String:
            return std::make_unique<Collection<String>>(col_key);
        case col_type_Binary:
            return std::make_unique<Collection<Binary>>(col_key);
        case col_type_Timestamp:
            return std::make_unique<Collection<Timestamp>>(col_key);
        case col_type_Decimal:
            return std::make_unique<Collection<Decimal128>>(col_key);
        case col_type_ObjectId:
            if (nullable)
                return std::make_unique<Collection<util::Optional<ObjectId>>>(col_key);
            return std::make_unique<Collection<ObjectId>>(col_key);
        case col_type_UUID:
            if (nullable)
                return std::make_unique<Collection<util::Optional<UUID>>>(col_key);
            return std::make_unique<Collection<UUID>>(col_key);
        case col_type_TypedLink:
            return std::make_unique<Collection<ObjLink>>(col_key);
        case col_type_Mixed:
            return std::make_unique<Collection<Mixed>>(col_key, level + 1);
        case col_type_Link:
            return std::make_unique<LinkCol>(col_key);
        default:
            REALM_TERMINATE("Unsupported column type.");
    }
}

LstBasePtr CollectionParent::get_listbase_ptr(ColKey col_key, uint8_t level)
{
    REALM_ASSERT(col_key.get_attrs().test(col_attr_List) || col_key.get_type() == col_type_Mixed);
    return create_collection<LstBase, Lst, LnkLst>(col_key, level);
}

SetBasePtr CollectionParent::get_setbase_ptr(ColKey col_key, uint8_t level)
{
    REALM_ASSERT(col_key.get_attrs().test(col_attr_Set));
    return create_collection<SetBase, Set, LnkSet>(col_key, level);
}

CollectionBasePtr CollectionParent::get_collection_ptr(ColKey col_key, uint8_t level)
{
    if (col_key.is_list()) {
        return get_listbase_ptr(col_key, level);
    }
    else if (col_key.is_set()) {
        return get_setbase_ptr(col_key, level);
    }
    else if (col_key.is_dictionary()) {
        return std::make_unique<Dictionary>(col_key, level + 1);
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

void CollectionParent::set_key(BPlusTreeMixed& tree, size_t index)
{
    int64_t key = generate_key(tree.size());
    while (tree.find_key(key) != realm::not_found) {
        key++;
    }
    tree.set_key(index, key);
}

} // namespace realm
