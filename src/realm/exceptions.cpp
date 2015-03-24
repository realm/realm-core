#include <realm/exceptions.hpp>
#include <realm/version.hpp>

using namespace realm;

const char* LogicError::what() const REALM_NOEXCEPT_OR_NOTHROW
{
    switch (m_kind) {
        case LogicError::string_too_big:
            return "String too big";
        case LogicError::binary_too_big:
            return "Binary too big";
        case LogicError::table_name_too_long:
            return "Table name too long";
        case LogicError::column_name_too_long:
            return "Column name too long";
        case LogicError::table_index_out_of_range:
            return "Table index out of range";
        case LogicError::row_index_out_of_range:
            return "Row index out of range";
        case LogicError::column_index_out_of_range:
            return "Column index out of range";
        case LogicError::illegal_combination:
            return "Illegal combination";
        case LogicError::type_mismatch:
            return "Type mismatch";
        case LogicError::wrong_kind_of_table:
            return "Wrong kind of table";
        case LogicError::detached_accessor:
            return "Detached accessor";
        case LogicError::no_search_index:
            return "Column has no search index";
        case LogicError::no_primary_key:
            return "Table has no primary key";
        case LogicError::is_primary_key:
            return "Column is the primary key of the table";
        case LogicError::has_primary_key:
            return "Primary key already added";
        case LogicError::unique_constraint_violation:
            return "Unique constraint violation";
    }
    return "Unknown error";
}

