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

#ifndef REALM_PATH_HPP
#define REALM_PATH_HPP

#include <realm/keys.hpp>
#include <realm/string_data.hpp>
#include <realm/table_ref.hpp>

namespace realm {

class Mixed;
namespace util::serializer {
struct SerialisationState;
}

// Given an object as starting point, a collection can be identified by
// a sequence of PathElements. The first element should always be a
// column key. The next elements are either an index into a list or a key
// to an entry in a dictionary
struct PathElement {
    union {
        std::string string_val;
        int64_t int_val;
    };
    enum Type { column, key, index, all } m_type;
    struct AllTag {};

    PathElement()
        : int_val(-1)
        , m_type(Type::all)
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
    PathElement(AllTag)
        : int_val(0)
        , m_type(Type::all)
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
    bool is_all() const noexcept
    {
        return m_type == Type::all;
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

    PathElement& operator=(const PathElement& other)
    {
        if (is_key() && !other.is_key()) {
            string_val.std::string::~string();
        }
        if (other.is_key()) {
            if (is_key()) {
                string_val = other.string_val;
            }
            else {
                new (&string_val) std::string(other.string_val);
            }
        }
        else {
            int_val = other.int_val;
        }
        m_type = other.m_type;
        return *this;
    }

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

// A key wrapper to be used for sorting,
// In addition to column key, it supports index into collection.
// TODO: Implement sorting by indexed elements of an array. They should be similar to dictionary keys.
class ExtendedColumnKey {
public:
    ExtendedColumnKey(ColKey col)
        : m_colkey(col)
    {
    }
    ExtendedColumnKey(ColKey col, const PathElement& index)
        : m_colkey(col)
        , m_index(index)
    {
    }
    ExtendedColumnKey(const ExtendedColumnKey& other)
        : m_colkey(other.m_colkey)
        , m_index(other.m_index)
    {
    }
    operator ColKey() const
    {
        return m_colkey;
    }
    ExtendedColumnKey& operator=(const ExtendedColumnKey& rhs)
    {
        m_colkey = rhs.m_colkey;
        m_index = rhs.m_index;
        return *this;
    }

    void set_index(const PathElement& index)
    {
        m_index = index;
    }
    const PathElement& get_index() const
    {
        return m_index;
    }
    bool is_dictionary() const
    {
        return m_colkey.is_dictionary();
    }
    bool is_list() const
    {
        return m_colkey.is_list();
    }
    bool has_index() const
    {
        return !m_index.is_all();
    }
    ConstTableRef get_target_table(const Table* table) const;

    std::string get_description(const Table* table) const;
    std::string get_description(ConstTableRef table, util::serializer::SerialisationState& state) const;

    bool is_collection() const;
    ObjKey get_link_target(const Obj& obj) const;
    Mixed get_value(const Obj& obj) const;

private:
    ColKey m_colkey;
    PathElement m_index;
};

/*
 * In order to detect stale collection objects (objects referring to entities that have
 * been deleted from the DB), we need a structure that both holds a somewhat unique salt
 * and possibly an index of the relevant column. The salt is generated when the collection
 * is assigned to the property and stored alongside the ref of the collection. The stored
 * salt is regenerated/cleared when a new value is assigned to the property/collection
 * element.
 */
class StableIndex {
public:
    StableIndex() = default;
    StableIndex(ColKey col_key, int64_t salt)
    {
        m_col_index = col_key.get_index().val;
        m_is_collection = col_key.is_collection();
        m_is_column = true;
        m_salt = int32_t(salt);
    }
    StableIndex(int64_t salt)
    {
        m_salt = int32_t(salt);
    }
    int64_t get_salt() const
    {
        return m_salt;
    }
    ColKey::Idx get_index() const noexcept
    {
        return {unsigned(m_col_index)};
    }
    bool is_collection() const noexcept
    {
        return m_is_collection;
    }

    bool operator==(const StableIndex& other) const noexcept
    {
        return m_is_column ? m_col_index == other.m_col_index : m_salt == other.m_salt;
    }
    bool operator<(const StableIndex& other) const noexcept
    {
        return m_is_column ? m_col_index < other.m_col_index : m_salt < other.m_salt;
    }

private:
    bool m_is_column = false;
    bool m_is_collection = false;
    int16_t m_col_index = 0;
    int32_t m_salt = 0;
};

static_assert(sizeof(StableIndex) == 8);

class StablePath : public std::vector<StableIndex> {
public:
    bool is_prefix_of(const StablePath& other) const noexcept;
};

} // namespace realm

#endif /* REALM_PATH_HPP */
