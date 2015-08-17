#include <realm/column_table.hpp>
#include <realm/lang_bind_helper.hpp>

using namespace realm;


Table* LangBindHelper::get_subtable_ptr_during_insert(Table* t, size_t column_index, size_t row_index)
{
    REALM_ASSERT(column_index < t->get_column_count());
    SubtableColumn& subtables =  t->get_column_table(column_index);
    REALM_ASSERT(row_index < subtables.size());
    Table* subtab = subtables.get_subtable_ptr(row_index);
    subtab->bind_ref();
    return subtab;
}


const char* LangBindHelper::get_data_type_name(DataType type) REALM_NOEXCEPT
{
    switch (type) {
        case type_Int:      return "int";
        case type_Bool:     return "bool";
        case type_Float:    return "float";
        case type_Double:   return "double";
        case type_String:   return "string";
        case type_Binary:   return "binary";
        case type_DateTime: return "date";
        case type_Table:    return "table";
        case type_Mixed:    return "mixed";
        case type_Link:     return "link";
        case type_LinkList: return "linklist";
    }
    REALM_ASSERT(false);
    return "int";
}
