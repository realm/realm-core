////////////////////////////////////////////////////////////////////////////
//
// Copyright 2023 Realm Inc.
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

#ifndef REALM_OS_CLASS_HPP
#define REALM_OS_CLASS_HPP

#include <realm/obj.hpp>
#include "realm/parser/keypath_mapping.hpp"
#include "realm/parser/query_parser.hpp"
#include <memory>

namespace realm {

class ObjectSchema;
class Realm;
class Query;

class Class {
public:
    Class(const std::shared_ptr<Realm>& r, StringData object_type);

    size_t size() const;
    TableKey get_key() const;
    ColKey get_column_key(StringData) const;
    Query get_query(const std::string& query_string, query_parser::Arguments& args,
                    const query_parser::KeyPathMapping& mapping) const;

    Obj create_object(Mixed primary_value = {});
    Obj get_object(Mixed primary_value);

private:
    friend class Results;

    std::shared_ptr<Realm> m_realm;
    const ObjectSchema* m_object_schema;
    TableRef m_table;
};

} // namespace realm

#endif // REALM_OS_CLASS_HPP
