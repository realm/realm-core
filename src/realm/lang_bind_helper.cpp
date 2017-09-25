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

#include <realm/column_table.hpp>
#include <realm/lang_bind_helper.hpp>

using namespace realm;


Table* LangBindHelper::get_subtable_ptr_during_insert(Table* t, size_t col_ndx, size_t row_ndx)
{
    REALM_ASSERT(col_ndx < t->get_column_count());
    SubtableColumn& subtables = t->get_column_table(col_ndx);
    REALM_ASSERT(row_ndx < subtables.size());
    TableRef subtab = subtables.get_subtable_tableref(row_ndx);
    return subtab.release();
}


const char* LangBindHelper::get_data_type_name(DataType type) noexcept
{
    switch (type) {
        case type_Int:
            return "int";
        case type_Bool:
            return "bool";
        case type_Float:
            return "float";
        case type_Double:
            return "double";
        case type_String:
            return "string";
        case type_Binary:
            return "binary";
        case type_OldDateTime:
            return "date"; // Deprecated
        case type_Timestamp:
            return "timestamp";
        case type_Table:
            return "table";
        case type_Mixed:
            return "mixed";
        case type_Link:
            return "link";
        case type_LinkList:
            return "linklist";
    }

    return "unknown";
}
