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

#include <realm/group.hpp>
#include <realm/dictionary.hpp>
#include <realm/list.hpp>
#include <realm/set.hpp>
#include <external/json/json.hpp>
#include "realm/util/base64.hpp"

namespace realm {

void Group::schema_to_json(std::ostream& out) const
{
    check_attached();

    out << "[" << std::endl;

    auto keys = get_table_keys();
    int sz = int(keys.size());
    for (int i = 0; i < sz; ++i) {
        auto key = keys[i];
        ConstTableRef table = get_table(key);

        table->schema_to_json(out);
        if (i < sz - 1)
            out << ",";
        out << std::endl;
    }

    out << "]" << std::endl;
}

void Group::to_json(std::ostream& out, JSONOutputMode output_mode) const
{
    check_attached();
    out << "{" << std::endl;

    auto keys = get_table_keys();
    bool first = true;
    for (size_t i = 0; i < keys.size(); ++i) {
        auto key = keys[i];
        ConstTableRef table = get_table(key);

        if (!table->is_embedded()) {
            if (!first)
                out << ",";
            out << "\"" << table->get_class_name() << "\"";
            out << ":";
            table->to_json(out, output_mode);
            out << std::endl;
            first = false;
        }
    }

    out << "}" << std::endl;
}

void Table::to_json(std::ostream& out, JSONOutputMode output_mode) const
{
    // Represent table as list of objects
    out << "[";
    bool first = true;

    for (auto& obj : *this) {
        if (first) {
            first = false;
        }
        else {
            out << ",";
        }
        obj.to_json(out, output_mode);
    }

    out << "]";
}

using Json = nlohmann::json;

template <typename T>
void Dictionary::insert_json(const std::string& key, const T& value)
{
    const Json& j = value;
    switch (j.type()) {
        case Json::value_t::null:
            insert(key, Mixed());
            break;
        case Json::value_t::string:
            insert(key, j.get<std::string>());
            break;
        case Json::value_t::boolean:
            insert(key, j.get<bool>());
            break;
        case Json::value_t::number_integer:
        case Json::value_t::number_unsigned:
            insert(key, j.get<int64_t>());
            break;
        case Json::value_t::number_float:
            insert(key, j.get<double>());
            break;
        case Json::value_t::object: {
            insert_collection(key, CollectionType::Dictionary);
            auto dict = get_dictionary(key);
            for (auto [k, v] : j.items()) {
                dict->insert_json(k, v);
            }
            break;
        }
        case Json::value_t::array: {
            insert_collection(key, CollectionType::List);
            auto list = get_list(key);
            for (auto&& elem : value) {
                list->add_json(elem);
            }
            break;
        }
        case Json::value_t::binary:
        case Json::value_t::discarded:
            REALM_TERMINATE("should never see discarded or binary");
    }
}

template <typename T>
void Lst<Mixed>::add_json(const T& value)
{
    const Json& j = value;
    size_t sz = size();
    switch (j.type()) {
        case Json::value_t::null:
            insert(sz, Mixed());
            break;
        case Json::value_t::string:
            insert(sz, j.get<std::string>());
            break;
        case Json::value_t::boolean:
            insert(sz, j.get<bool>());
            break;
        case Json::value_t::number_integer:
        case Json::value_t::number_unsigned:
            insert(sz, j.get<int64_t>());
            break;
        case Json::value_t::number_float:
            insert(sz, j.get<double>());
            break;
        case Json::value_t::object: {
            insert_collection(sz, CollectionType::Dictionary);
            auto dict = get_dictionary(sz);
            for (auto [k, v] : j.items()) {
                dict->insert_json(k, v);
            }
            break;
        }
        case Json::value_t::array: {
            insert_collection(sz, CollectionType::List);
            auto list = get_list(sz);
            for (auto&& elem : j) {
                list->add_json(elem);
            }
            break;
        }
        case Json::value_t::binary:
        case Json::value_t::discarded:
            REALM_TERMINATE("should never see discarded or binary");
    }
}

Obj& Obj::set_json(ColKey col_key, StringData json)
{
    auto j = Json::parse(std::string_view(json.data(), json.size()), nullptr, false);
    switch (j.type()) {
        case Json::value_t::null:
            set(col_key, Mixed());
            break;
        case Json::value_t::string:
            set(col_key, Mixed(j.get<std::string>()));
            break;
        case Json::value_t::boolean:
            set(col_key, Mixed(j.get<bool>()));
            break;
        case Json::value_t::number_integer:
        case Json::value_t::number_unsigned:
            set(col_key, Mixed(j.get<int64_t>()));
            break;
        case Json::value_t::number_float:
            set(col_key, Mixed(j.get<double>()));
            break;
        case Json::value_t::object: {
            set_collection(col_key, CollectionType::Dictionary);
            Dictionary dict(*this, col_key);
            for (auto [k, v] : j.items()) {
                dict.insert_json(k, v);
            }
            break;
        }
        case Json::value_t::array: {
            set_collection(col_key, CollectionType::List);
            Lst<Mixed> list(*this, col_key);
            list.clear();
            for (auto&& elem : j) {
                list.add_json(elem);
            }
        } break;
        case Json::value_t::binary:
            // Parser will never return binary
        case Json::value_t::discarded:
            throw InvalidArgument(ErrorCodes::MalformedJson, "Illegal json");
    }

    return *this;
}

void Obj::to_json(std::ostream& out, JSONOutputMode output_mode) const
{
    bool prefixComma = false;
    out << "{";
    if (output_mode == output_mode_json && !m_table->get_primary_key_column() && !m_table->is_embedded()) {
        prefixComma = true;
        out << "\"_key\":" << this->m_key.value;
    }

    auto col_keys = m_table->get_column_keys();
    for (auto ck : col_keys) {
        auto type = ck.get_type();

        if (prefixComma)
            out << ",";
        out << "\"" << m_table->get_column_name(ck) << "\":";
        prefixComma = true;

        TableRef target_table;
        ColKey pk_col_key;
        if (type == col_type_Link) {
            target_table = get_target_table(ck);
            pk_col_key = target_table->get_primary_key_column();
        }

        auto print_link = [&](const Mixed& val) {
            REALM_ASSERT(val.is_type(type_Link, type_TypedLink));
            TableRef tt = target_table;
            bool typed_link = false;
            if (!tt) {
                // It must be a typed link
                tt = m_table->get_parent_group()->get_table(val.get_link().get_table_key());
                typed_link = true;
            }
            auto obj_key = val.get<ObjKey>();
            std::string closing;

            if (tt->is_embedded()) {
                if (output_mode == output_mode_xjson_plus) {
                    out << "{ \"$embedded\": {";
                    out << "\"table\": \"" << tt->get_name() << "\", \"value\": ";
                    closing = "}}";
                }
                tt->get_object(obj_key).to_json(out, output_mode);
            }
            else {
                pk_col_key = tt->get_primary_key_column();
                if (output_mode == output_mode_xjson_plus || typed_link) {
                    out << "{ \"$link\": {";
                    out << "\"table\": \"" << tt->get_class_name() << "\", \"key\": ";
                    closing = "}}";
                }
                if (pk_col_key) {
                    tt->get_primary_key(obj_key).to_json(out, output_mode);
                }
                else {
                    out << obj_key.value;
                }
            }
            out << closing;
        };

        if (ck.is_collection()) {
            auto collection = get_collection_ptr(ck);
            collection->to_json(out, output_mode, print_link);
        }
        else {
            auto val = get_any(ck);
            if (!val.is_null()) {
                if (val.is_type(type_Link, type_TypedLink)) {
                    print_link(val);
                }
                else if (val.is_type(type_Dictionary)) {
                    DummyParent parent(m_table, val.get_ref());
                    Dictionary dict(parent, 0);
                    dict.to_json(out, output_mode, print_link);
                }
                else if (val.is_type(type_List)) {
                    DummyParent parent(m_table, val.get_ref());
                    Lst<Mixed> list(parent, 0);
                    list.to_json(out, output_mode, print_link);
                }
                else {
                    val.to_json(out, output_mode);
                }
            }
            else {
                out << "null";
            }
        }
    }
    out << "}";
}

namespace {
const char to_be_escaped[] = "\"\n\r\t\f\\\b";
const char encoding[] = "\"nrtf\\b";

template <class T>
inline void out_floats(std::ostream& out, T value)
{
    std::streamsize old = out.precision();
    out.precision(std::numeric_limits<T>::digits10 + 1);
    out << std::scientific << value;
    out.precision(old);
}

void out_string(std::ostream& out, std::string str)
{
    size_t p = str.find_first_of(to_be_escaped);
    while (p != std::string::npos) {
        char c = str[p];
        auto found = strchr(to_be_escaped, c);
        REALM_ASSERT(found);
        out << str.substr(0, p) << '\\' << encoding[found - to_be_escaped];
        str = str.substr(p + 1);
        p = str.find_first_of(to_be_escaped);
    }
    out << str;
}

void out_binary(std::ostream& out, BinaryData bin)
{
    std::string encode_buffer;
    encode_buffer.resize(util::base64_encoded_size(bin.size()));
    util::base64_encode(bin, encode_buffer);
    out << encode_buffer;
}
} // anonymous namespace


void Mixed::to_xjson(std::ostream& out) const noexcept
{
    switch (get_type()) {
        case type_Int:
            out << "{\"$numberLong\": \"";
            out << int_val;
            out << "\"}";
            break;
        case type_Bool:
            out << (bool_val ? "true" : "false");
            break;
        case type_Float:
            out << "{\"$numberDouble\": \"";
            out_floats<float>(out, float_val);
            out << "\"}";
            break;
        case type_Double:
            out << "{\"$numberDouble\": \"";
            out_floats<double>(out, double_val);
            out << "\"}";
            break;
        case type_String: {
            out << "\"";
            out_string(out, string_val);
            out << "\"";
            break;
        }
        case type_Binary: {
            out << "{\"$binary\": {\"base64\": \"";
            out_binary(out, binary_val);
            out << "\", \"subType\": \"00\"}}";
            break;
        }
        case type_Timestamp: {
            out << "{\"$date\": {\"$numberLong\": \"";
            int64_t timeMillis = date_val.get_seconds() * 1000 + date_val.get_nanoseconds() / 1000000;
            out << timeMillis;
            out << "\"}}";
            break;
        }
        case type_Decimal:
            out << "{\"$numberDecimal\": \"";
            out << decimal_val;
            out << "\"}";
            break;
        case type_ObjectId:
            out << "{\"$oid\": \"";
            out << id_val;
            out << "\"}";
            break;
        case type_UUID:
            out << "{\"$binary\": {\"base64\": \"";
            out << uuid_val.to_base64();
            out << "\", \"subType\": \"04\"}}";
            break;

        case type_TypedLink: {
            Mixed val(get<ObjLink>().get_obj_key());
            val.to_xjson(out);
            break;
        }
        case type_Link:
        case type_Mixed:
            break;
    }
}

void Mixed::to_xjson_plus(std::ostream& out) const noexcept
{

    // Special case for outputing a typedLink, otherwise just us out_mixed_xjson
    if (is_type(type_TypedLink)) {
        auto link = get<ObjLink>();
        out << "{ \"$link\": { \"table\": \"" << link.get_table_key() << "\", \"key\": ";
        Mixed val(link.get_obj_key());
        val.to_xjson(out);
        out << "}}";
        return;
    }

    to_xjson(out);
}

void Mixed::to_json(std::ostream& out, JSONOutputMode output_mode) const noexcept
{
    if (is_null()) {
        out << "null";
        return;
    }
    switch (output_mode) {
        case output_mode_xjson: {
            to_xjson(out);
            return;
        }
        case output_mode_xjson_plus: {
            to_xjson_plus(out);
            return;
        }
        case output_mode_json: {
            switch (get_type()) {
                case type_Int:
                    out << int_val;
                    break;
                case type_Bool:
                    out << (bool_val ? "true" : "false");
                    break;
                case type_Float:
                    out_floats<float>(out, float_val);
                    break;
                case type_Double:
                    out_floats<double>(out, double_val);
                    break;
                case type_String: {
                    out << "\"";
                    out_string(out, string_val);
                    out << "\"";
                    break;
                }
                case type_Binary: {
                    out << "\"";
                    out_binary(out, binary_val);
                    out << "\"";
                    break;
                }
                case type_Timestamp:
                    out << "\"";
                    out << date_val;
                    out << "\"";
                    break;
                case type_Decimal:
                    out << "\"";
                    out << decimal_val;
                    out << "\"";
                    break;
                case type_ObjectId:
                    out << "\"";
                    out << id_val;
                    out << "\"";
                    break;
                case type_UUID:
                    out << "\"";
                    out << uuid_val;
                    out << "\"";
                    break;
                case type_TypedLink:
                    out << "\"";
                    out << link_val;
                    out << "\"";
                    break;
                case type_Link:
                case type_Mixed:
                    break;
            }
        }
    }
}

} // namespace realm
