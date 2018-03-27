////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
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

#include "parser_utils.hpp"

#include <sstream>
#include <typeinfo>

#include <realm/table.hpp>

namespace realm {
namespace util {

template <typename T>
const char* type_to_str()
{
    return typeid(T).name();
}
template <>
const char* type_to_str<bool>()
{
    return "Bool";
}
template <>
const char* type_to_str<Int>()
{
    return "Int";
}
template <>
const char* type_to_str<Float>()
{
    return "Float";
}
template <>
const char* type_to_str<Double>()
{
    return "Double";
}
template <>
const char* type_to_str<String>()
{
    return "String";
}
template <>
const char* type_to_str<Binary>()
{
    return "Binary";
}
template <>
const char* type_to_str<Timestamp>()
{
    return "Timestamp";
}
template <>
const char* type_to_str<Link>()
{
    return "Link";
}

const char* data_type_to_str(DataType type)
{
    switch (type) {
        case type_Int:
            return "Int";
        case type_Bool:
            return "Bool";
        case type_Float:
            return "Float";
        case type_Double:
            return "Double";
        case type_String:
            return "String";
        case type_Binary:
            return "Binary";
        case type_OldDateTime:
            return "DateTime";
        case type_Timestamp:
            return "Timestamp";
        case type_OldTable:
            return "Table";
        case type_OldMixed:
            return "Mixed";
        case type_Link:
            return "Link";
        case type_LinkList:
            return "LinkList";
    }
    return "type_Unknown"; // LCOV_EXCL_LINE
}

const char* collection_operator_to_str(parser::Expression::KeyPathOp op)
{
    switch (op) {
        case parser::Expression::KeyPathOp::None:
            return "NONE";
        case parser::Expression::KeyPathOp::Min:
            return "@min";
        case parser::Expression::KeyPathOp::Max:
            return "@max";
        case parser::Expression::KeyPathOp::Sum:
            return "@sum";
        case parser::Expression::KeyPathOp::Avg:
            return "@avg";
        case parser::Expression::KeyPathOp::SizeString:
            return "@size";
        case parser::Expression::KeyPathOp::SizeBinary:
            return "@size";
        case parser::Expression::KeyPathOp::Count:
            return "@count";
    }
    return "";
}

using KeyPath = std::vector<std::string>;

KeyPath key_path_from_string(const std::string &s) {
    std::stringstream ss(s);
    std::string item;
    KeyPath key_path;
    while (std::getline(ss, item, '.')) {
        key_path.push_back(item);
    }
    return key_path;
}

StringData get_printable_table_name(const Table& table)
{
    StringData name = table.get_name();
    // the "class_" prefix is an implementation detail of the object store that shouldn't be exposed to users
    static const std::string prefix = "class_";
    if (name.size() > prefix.size() && strncmp(name.data(), prefix.data(), prefix.size()) == 0) {
        name = StringData(name.data() + prefix.size(), name.size() - prefix.size());
    }
    return name;
}

} // namespace utils
} // namespace realm
