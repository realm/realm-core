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
            auto obj_key = val.get<ObjKey>();

            auto out_obj_key = [&] {
                if (pk_col_key) {
                    tt->get_primary_key(obj_key).to_json(out, output_mode);
                }
                else {
                    out << obj_key.value;
                }
            };
            if (!tt) {
                // It must be a typed link
                tt = m_table->get_parent_group()->get_table(val.get_link().get_table_key());
                pk_col_key = tt->get_primary_key_column();
                out << "{ \"$link\": {";

                out << "\"table\": \"" << tt->get_class_name() << "\", \"key\": ";
                out_obj_key();
                out << " }}";
            }
            else {
                if (tt->is_embedded()) {
                    if (output_mode == output_mode_xjson_plus) {
                        out << "{ \"$embedded\": ";
                    }
                    tt->get_object(obj_key).to_json(out, output_mode);
                    if (output_mode == output_mode_xjson_plus) {
                        out << "}";
                    }
                }
                else {
                    out_obj_key();
                }
            }
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
    if (std::isnan(value)) {
        out << "NaN";
    }
    else {
        std::streamsize old = out.precision(std::numeric_limits<T>::max_digits10);
        out << value;
        out.precision(old);
    }
}

void out_string(std::ostream& out, std::string_view str)
{
    for (auto c : str) {
        if (auto found = memchr(to_be_escaped, c, sizeof(to_be_escaped) - 1)) {
            out << '\\' << encoding[reinterpret_cast<const char*>(found) - to_be_escaped];
        }
        else if (unsigned(c) < 0x20) {
            // Other control characters
            char buffer[5];
            sprintf(buffer, "%04x", int(c));
            out << "\\u" << buffer;
        }
        else {
            out << c;
        }
    }
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
            out_string(out, std::string_view(string_val));
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
                    out_string(out, std::string_view(string_val));
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

std::string Mixed::to_json(JSONOutputMode output_mode) const noexcept
{
    std::ostringstream ostr;
    to_json(ostr, output_mode);
    return ostr.str();
}
} // namespace realm
