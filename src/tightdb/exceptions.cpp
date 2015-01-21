#include <tightdb/exceptions.hpp>
#include <tightdb/version.hpp>

using namespace tightdb;

const char* LogicError::what() const TIGHTDB_NOEXCEPT_OR_NOTHROW
{
    switch (m_kind) {
        case LogicError::string_too_big:
            return TIGHTDB_VER_CHUNK " String too big";
        case LogicError::binary_too_big:
            return TIGHTDB_VER_CHUNK " Binary too big";
        case LogicError::table_name_too_long:
            return TIGHTDB_VER_CHUNK " Table name too long";
        case LogicError::column_name_too_long:
            return TIGHTDB_VER_CHUNK " Column name too long";
        case LogicError::table_index_out_of_range:
            return TIGHTDB_VER_CHUNK " Table index out of range";
        case LogicError::row_index_out_of_range:
            return TIGHTDB_VER_CHUNK " Row index out of range";
        case LogicError::column_index_out_of_range:
            return TIGHTDB_VER_CHUNK " Column index out of range";
        case LogicError::illegal_combination:
            return TIGHTDB_VER_CHUNK " Illegal combination";
        case LogicError::type_mismatch:
            return TIGHTDB_VER_CHUNK " Type mismatch";
        case LogicError::wrong_kind_of_table:
            return TIGHTDB_VER_CHUNK " Wrong kind of table";
        case LogicError::detached_accessor:
            return TIGHTDB_VER_CHUNK " Detached accessor";
        case LogicError::no_search_index:
            return TIGHTDB_VER_CHUNK " Column has no search index";
        case LogicError::no_primary_key:
            return TIGHTDB_VER_CHUNK " Table has no primary key";
        case LogicError::has_primary_key:
            return TIGHTDB_VER_CHUNK " Primary key already added";
        case LogicError::unique_constraint_violation:
            return TIGHTDB_VER_CHUNK " Unique constraint violation";
    }
    return TIGHTDB_VER_CHUNK " Unknown error";
}

