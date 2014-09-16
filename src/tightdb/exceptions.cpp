#include <tightdb/exceptions.hpp>

using namespace tightdb;


const char* const LogicError::index_out_of_range = "Index out of range";

const char* const LogicError::illegal_combination = "Illegal combination";

const char* const LogicError::type_mismatch = "Type mismatch";

const char* const LogicError::wrong_kind_of_table = "Wrong kind of table";

const char* const LogicError::detached_accessor = "Detached accessor";

const char* const LogicError::immutable_data = "Immutable data";

const char* const LogicError::no_search_index = "Column has no search index";

const char* const LogicError::no_primary_key = "Table has no primary key";

const char* const LogicError::has_primary_key = "Primary key already added";
