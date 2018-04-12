/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
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

#ifndef REALM_BINDING_HPP
#define REALM_BINDING_HPP

#include <string>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>

#include <realm/row.hpp>
#include <realm/table.hpp>
#include <realm/group_shared.hpp>

namespace realm {
namespace binding {

#define REALM_OBJECT(reflect) \
    realm::binding::Anchor _m_realm_object_anchor; \
    static void _realm_object_reflection(realm::binding::Reflect& r) \
    { \
        reflect(r); \
    } \
    friend class realm::binding::ClassReflection


struct ClassSchemaInfo;

struct Anchor {
    Row m_row;
    const ClassSchemaInfo* m_class_schema_info;
};

struct PropertyInfo {
    std::string name;
    size_t id; // first property will be 0, then 1, ...
    DataType column_type;
    bool column_nullable;
    int32_t offset_from_anchor;
};

struct ClassInfo {
    std::string name;
    std::string table_name;

    std::map<std::string, PropertyInfo> properties;

    size_t m_property_id_counter = 0;
};

struct ClassSchemaInfo {
    size_t table_index;
    std::vector<size_t> property_column_indices;
};

template <class T> struct GetDataType;
template <> struct GetDataType<int> {
    static constexpr DataType value = type_Int;
    using CoreType = int_fast64_t;
};
template <> struct GetDataType<std::string> {
    static constexpr DataType value = type_String;
    using CoreType = StringData;
};
template <> struct GetDataType<StringData> {
    static constexpr DataType value = type_String;
    using CoreType = StringData;
};

template <class T> struct GetDataType<util::Optional<T>> {
    static constexpr DataType value = GetDataType<T>::value;
    using CoreType = typename GetDataType<T>::CoreType;
};

template <class T> struct GetNullable { static constexpr bool value = false; };
template <class T> struct GetNullable<util::Optional<T>> { static constexpr bool value = true; };


struct PropertyBase {
    int32_t m_offset_from_anchor = std::numeric_limits<int32_t>::min();
    uint32_t m_id = uint32_t(-1);

    Anchor* find_anchor()
    {
        char* p = reinterpret_cast<char*>(this);
        return reinterpret_cast<Anchor*>(p - m_offset_from_anchor);
    }

    const Anchor* find_anchor() const
    {
        const char* p = reinterpret_cast<const char*>(this);
        return reinterpret_cast<const Anchor*>(p - m_offset_from_anchor);
    }

    Row& get_row()
    {
        return find_anchor()->m_row;
    }

    size_t get_column_index() const
    {
        const Anchor* anchor = find_anchor();
        return anchor->m_class_schema_info->property_column_indices.at(m_id);
    }
};

template <class T>
struct Property : PropertyBase {
    Property() {}
    using CoreType = typename GetDataType<T>::CoreType;

    DataType get_type() const { return GetDataType<T>::value; }

    Property& operator=(T new_value)
    {
        REALM_ASSERT(get_row().is_attached());
        size_t col_ndx = get_column_index();
        get_row().template set<CoreType>(col_ndx, CoreType(new_value));
        return *this;
    }

    T get()
    {
        REALM_ASSERT(get_row().is_attached());
    }

    operator T()
    {
        return get();
    }
};

struct Reflect {
    ClassInfo& m_class_info;
    explicit Reflect(ClassInfo& class_info) : m_class_info(class_info) {}

    Reflect& name(std::string class_name)
    {
        table_name("class_" + class_name);
        m_class_info.name = std::move(class_name);
        return *this;
    }

    Reflect& table_name(std::string table_name)
    {
        m_class_info.table_name = std::move(table_name);
        return *this;
    }

    template <class T, class M>
    Reflect& bind_property(Property<M> T::*member, std::string name)
    {
        auto anchor_offset = offsetof(T, _m_realm_object_anchor); // FIXME: Valid in C++17

        // FIXME: Hack to find the offset of the member
        T tmp;
        Property<M>* m2 = &(tmp.*member);
        auto property_offset = reinterpret_cast<char*>(m2) - reinterpret_cast<char*>(&tmp);

        PropertyInfo info;
        info.name = name;
        info.column_type = GetDataType<M>::value;
        info.column_nullable = GetNullable<M>::value;
        info.offset_from_anchor = int32_t(property_offset) - int32_t(anchor_offset);
        info.id = m_class_info.m_property_id_counter++;
        m_class_info.properties[name] = std::move(info);
        return *this;
    }
};


class ClassReflection {
public:
    static ClassReflection& get() {
        static ClassReflection* singleton = new ClassReflection;
        return *singleton;
    }

    template <class T>
    ClassInfo& get_class_info() {
        // FIXME: Figure out how to do recursion when discovering link columns.
        std::lock_guard<std::mutex> l{m_mutex};
        auto key = std::type_index(typeid(T));
        auto it = m_class_infos.find(key);
        if (it == m_class_infos.end()) {
            it = m_class_infos.insert({key, std::make_unique<ClassInfo>()}).first;
            Reflect reflect{*it->second};
            T::_realm_object_reflection(reflect);
        }
        return *it->second;
    }

    const ClassSchemaInfo* get_class_schema_info(const ClassInfo* ci, WriteTransaction& tr)
    {
        std::lock_guard<std::mutex> l{m_mutex};
        // FIXME: Account for different schemas in different transactions
        auto it = m_class_schema_info.find(ci);
        if (it == m_class_schema_info.end()) {
            it = m_class_schema_info.insert({ci, std::make_unique<ClassSchemaInfo>()}).first;
            ClassSchemaInfo& csi = *it->second;
            csi.property_column_indices.resize(ci->properties.size());
            ConstTableRef table = tr.get_table(ci->table_name);
            csi.table_index = table->get_index_in_group();
            for (auto& pair: ci->properties) {
                csi.property_column_indices[pair.second.id] =
                    table->get_column_index(pair.first);
            }
        }
        return it->second.get();
    }
private:
    std::mutex m_mutex;
    std::unordered_map<std::type_index, std::unique_ptr<ClassInfo>> m_class_infos;
    std::unordered_map<const ClassInfo*, std::unique_ptr<ClassSchemaInfo>> m_class_schema_info;
};

template <class T>
ClassInfo& get_class_info() {
    return ClassReflection::get().get_class_info<T>();
}

struct Schema {
    std::vector<ClassInfo*> m_classes;

    template <class T>
    void add()
    {
        m_classes.push_back(&get_class_info<T>());
    }

    void auto_migrate(WriteTransaction& tr)
    {
        for (auto info: m_classes) {
            TableRef table = tr.get_or_add_table(info->table_name);
            for (auto& pair: info->properties) {
                auto& property = pair.second;
                // FIXME: Handle link columns
                size_t existing_column = table->get_column_index(property.name);
                if (existing_column == npos) {
                    table->add_column(property.column_type, property.name, property.column_nullable);
                }
                else {
                    if (table->get_column_type(existing_column) != property.column_type)
                        throw std::runtime_error(std::string("Column type mismatch: ") + property.name);
                    if (table->is_nullable(existing_column) != property.column_nullable)
                        throw std::runtime_error(std::string("Column nullability mismatch: ") + property.name);
                }
            }
        }
    }
};

inline void initialize_properties(const ClassInfo& ci, Anchor* anchor)
{
    char* anchor_ptr = reinterpret_cast<char*>(anchor);
    for (auto pair: ci.properties) {
        auto& property = pair.second;
        char* property_base_ptr = anchor_ptr + property.offset_from_anchor;
        PropertyBase* p = reinterpret_cast<PropertyBase*>(property_base_ptr);
        p->m_offset_from_anchor = property.offset_from_anchor;
        p->m_id = property.id;
    }
}

template <class T>
T create_object(WriteTransaction& tr)
{
    const ClassInfo& ci = get_class_info<T>();
    TableRef table = tr.get_table(ci.table_name);
    size_t row = table->add_empty_row();
    T object;
    object._m_realm_object_anchor.m_row = table->get(row);
    object._m_realm_object_anchor.m_class_schema_info = ClassReflection::get().get_class_schema_info(&ci, tr);
    initialize_properties(ci, &object._m_realm_object_anchor);
    return object;
}


} // namespace binding
} // namespace realm

#endif // REALM_BINDING_HPP
