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

#ifndef REALM_TEST_STRING_TYPES_HPP
#define REALM_TEST_STRING_TYPES_HPP

#include <realm/alloc.hpp>
#include <realm/column_string.hpp>
#include <realm/column_string_enum.hpp>

struct string_column {
    using ColumnTestType = realm::StringColumn;
    string_column(bool nullable = false)
    {
        m_ref = realm::StringColumn::create(realm::Allocator::get_default());
        m_col = new realm::StringColumn(realm::Allocator::get_default(), m_ref, nullable);
    }
    virtual ~string_column()
    {
        m_col->destroy();
        delete m_col;
        m_col = nullptr;
    }
    realm::StringColumn& get_column()
    {
        return *m_col;
    }
    realm::ref_type m_ref;
    realm::StringColumn* m_col;
    static bool is_nullable()
    {
        return false;
    }
    static bool is_enumerated()
    {
        return false;
    }
};

struct nullable_string_column : public string_column {
    nullable_string_column()
        : string_column(true)
    {
    }
    static bool is_nullable()
    {
        return true;
    }
    static bool is_enumerated()
    {
        return false;
    }
};

struct enum_column : public string_column {
    using ColumnTestType = realm::StringEnumColumn;
    enum_column(bool nullable = false) : string_column(nullable)
    {
        realm::ref_type ref, keys_ref;
        bool enforce = true;
        bool created = m_col->auto_enumerate(keys_ref, ref, enforce);
        REALM_ASSERT(created);
        m_enum_col = new realm::StringEnumColumn(realm::Allocator::get_default(), ref, keys_ref, nullable); // Throws
    }
    ~enum_column()
    {
        m_enum_col->destroy();
        delete m_enum_col;
        m_enum_col = nullptr;
    }
    realm::StringEnumColumn& get_column()
    {
        return *m_enum_col;
    }
    realm::StringEnumColumn* m_enum_col;
    static bool is_nullable()
    {
        return false;
    }
    static bool is_enumerated()
    {
        return true;
    }
};

struct nullable_enum_column : public enum_column {
    nullable_enum_column()
        : enum_column(true)
    {
    }
    static bool is_nullable()
    {
        return true;
    }
    static bool is_enumerated()
    {
        return true;
    }
};

#endif // REALM_TEST_STRING_TYPES_HPP
