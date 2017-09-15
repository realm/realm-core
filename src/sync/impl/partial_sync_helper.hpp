////////////////////////////////////////////////////////////////////////////
//
// Copyright 2017 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef partial_sync_helper_hpp
#define partial_sync_helper_hpp

#include <string>
#include <unordered_map>

#include <realm/table_ref.hpp>

namespace realm {
    
class Realm;
class List;

using PartialSyncResultCallback = void(List results, std::exception_ptr error);

class PartialSyncHelper {
public:
    PartialSyncHelper(Realm*);

    // Register an object class and query for use with partial synchronization.
    // The callback will be called exactly once: upon either the successful completion
    // of the query, or upon its failure.
    // Bindings can take the `List` passed into the callback and construct a binding
    // level collection from it that can be used by the host application. They can
    // then observe the collection themselves if they wish to be notified about
    // further changes to it.
    void register_query(const std::string& object_class,
                        const std::string& query,
                        std::function<PartialSyncResultCallback>);

private:
    // The schema that describes the common properties on `__ResultSets`.
    struct Schema {
        size_t idx_matches_property;
        size_t idx_query;
        size_t idx_status;
        size_t idx_error_message;
    };

    Realm *m_parent_realm;
    PartialSyncHelper::Schema m_common_schema;
    TableRef m_result_sets_table;
    std::unordered_map<std::string, size_t> m_object_type_schema;

    // Register an object class (specified by its raw, user-facing class name) with the
    // partial sync system. If the class has not been added, it is added.
    // More specifically, if necessary the __ResultSets schema is modified to add a
    // property of type Array<class> named "<class>_matches".
    // Returns the column index of the "<class>_matches" property.
    size_t get_matches_column_idx_for_object_class(const std::string&);
};

}

#endif /* partial_sync_helper_hpp */
