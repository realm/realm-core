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

#include <realm/exceptions.hpp>
#include <realm/version.hpp>

using namespace realm;

// LCOV_EXCL_START (LogicError is not a part of the public API, so code may never
// rely on the contents of these strings, as they are deliberately unspecified.)
const char* LogicError::what() const noexcept
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
        case column_name_in_use:
            return "Column name must be unique";
        case invalid_column_name:
            return "Column name not found";
        case table_index_out_of_range:
            return "Table index out of range";
        case row_index_out_of_range:
            return "Row index out of range";
        case column_index_out_of_range:
            return "Column index out of range";
        case string_position_out_of_range:
            return "String position out of range";
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
        case target_row_index_out_of_range:
            return "Target table row index out of range";
        case no_search_index:
            return "Column has no search index";
        case unique_constraint_violation:
            return "Unique constraint violation";
        case column_not_nullable:
            return "Attempted to insert null into non-nullable column";
        case wrong_group_state:
            return "Wrong state or group accessor (already attached, "
                   "or managed by a SharedGroup object)";
        case wrong_transact_state:
            return "Wrong transactional state (no active transaction, wrong type of transaction, "
                   "or transaction already in progress)";
        case no_history:
            return "Continuous transaction through SharedGroup object without history information";
        case mixed_durability:
            return "Durability setting (as passed to the SharedGroup constructor) was "
                   "not consistent across the session";
        case mixed_history_type:
            return "History type (as specified by the Replication implementation passed to "
                "the SharedGroup constructor) was not consistent across the session";
        case mixed_history_schema_version:
            return "History schema version (as specified by the Replication implementation passed "
                "to the SharedGroup constructor) was not consistent across the session";
        case table_has_no_columns:
            return "Table has no columns";
        case column_does_not_exist:
            return "Column does not exist";
        case subtable_of_subtable_index:
            return "Search index on a subtable of a subtable is not yet supported";
    }
    return "Unknown error";
}
// LCOV_EXCL_STOP (LogicError messages)
