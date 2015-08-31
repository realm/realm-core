#include <realm/exceptions.hpp>
#include <realm/version.hpp>

using namespace realm;

const char* LogicError::what() const REALM_NOEXCEPT_OR_NOTHROW
{
    switch (m_kind) {
        case string_too_big:
            return "String too big";
        case binary_too_big:
            return "Binary too big";
        case table_name_too_long:
            return "Table name too long";
        case column_name_too_long:
            return "Column name too long";
        case table_index_out_of_range:
            return "Table index out of range";
        case row_index_out_of_range:
            return "Row index out of range";
        case column_index_out_of_range:
            return "Column index out of range";
        case link_index_out_of_range:
            return "Link index out of range";
        case bad_version:
            return "Bad version number";
        case illegal_type:
            return "Illegal data type";
        case illegal_combination:
            return "Illegal combination";
        case type_mismatch:
            return "Data type mismatch";
        case group_mismatch:
            return "Tables are in different groups";
        case wrong_kind_of_descriptor:
            return "Wrong kind of descriptor";
        case wrong_kind_of_table:
            return "Wrong kind of table";
        case detached_accessor:
            return "Detached accessor";
        case no_search_index:
            return "Column has no search index";
        case no_primary_key:
            return "Table has no primary key";
        case is_primary_key:
            return "Column is the primary key of the table";
        case has_primary_key:
            return "Primary key already added";
        case unique_constraint_violation:
            return "Unique constraint violation";
        case column_not_nullable:
            return "Attempted to insert null into non-nullable column";
        case wrong_group_state:
            return "Wrong state og group accessor (already attached, "
                "or managed by a SharedGroup object)";
        case wrong_transact_state:
            return "Wrong transactional state (no active transaction, wrong type of transaction, "
                "or transaction already in progress)";
    }
    return "Unknown error";
}
