#include <tightdb/exceptions.hpp>

using namespace tightdb;


const char* const LogicError::string_too_big = "String too big";

const char* const LogicError::binary_too_big = "Binary too big";

const char* const LogicError::table_name_too_long = "Table name too long";

const char* const LogicError::column_name_too_long = "Column name too long";

const char* const LogicError::table_index_out_of_range = "Table index out of range";

const char* const LogicError::row_index_out_of_range = "Row index out of range";

const char* const LogicError::column_index_out_of_range = "Column index out of range";

const char* const LogicError::illegal_combination = "Illegal combination";

const char* const LogicError::type_mismatch = "Type mismatch";

const char* const LogicError::wrong_kind_of_table = "Wrong kind of table";

const char* const LogicError::detached_accessor = "Detached accessor";

const char* const LogicError::no_search_index = "Column has no search index";

const char* const LogicError::no_primary_key = "Table has no primary key";

const char* const LogicError::has_primary_key = "Primary key already added";

const char* const LogicError::unique_constraint_violation = "Unique constraint violation";
