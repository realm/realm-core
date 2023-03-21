/*************************************************************************
 *
 * Copyright 2022 Realm Inc.
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

#include "fuzz_object.hpp"
#include "fuzz_logger.hpp"
#include "util.hpp"

#include <realm/group.hpp>
#include <realm/data_type.hpp>
#include <realm/util/optional.hpp>
#include <realm/object-store/shared_realm.hpp>
#include <realm.hpp>
#include <realm/index_string.hpp>

#include <utility>
#include <random>

using namespace realm;
using namespace realm::util;

// Max number of rows in a table. Overridden only by create_object() and only in the case where
// max_rows is not exceeded *prior* to executing add_empty_row.
const size_t max_rows = 100000;
const size_t add_empty_row_max = REALM_MAX_BPNODE_SIZE * REALM_MAX_BPNODE_SIZE + 1000;

unsigned char FuzzObject::get_next_token(State& s) const
{
    if (s.pos == s.str.size() || s.str.empty()) {
        throw EndOfFile{};
    }
    return s.str[s.pos++];
}

void FuzzObject::create_table(Group& group, FuzzLog& log)
{
    log << "FuzzObject::create_table();\n";
    std::string name = create_table_name();
    log << "group.add_table(\"" << name << "\");\n";
    group.add_table(name);
}

void FuzzObject::remove_table(Group& group, FuzzLog& log, State& s)
{
    log << "FuzzObject::remove_table();\n";
    TableKey table_key = group.get_table_keys()[get_next_token(s) % group.size()];
    log << "try { group.remove_table(" << table_key
        << "); }"
           " catch (const CrossTableLinkTarget&) { }\n";
    group.remove_table(table_key);
}

void FuzzObject::clear_table(Group& group, FuzzLog& log, State& s)
{
    log << "FuzzObject::clear_table();\n";
    TableKey table_key = group.get_table_keys()[get_next_token(s) % group.size()];
    log << "group.get_table(" << table_key << ")->clear();\n";
    group.get_table(table_key)->clear();
}

void FuzzObject::create_object(Group& group, FuzzLog& log, State& s)
{
    log << "FuzzObject::create_object();\n";
    TableKey table_key = group.get_table_keys()[get_next_token(s) % group.size()];
    size_t num_rows = get_next_token(s);
    if (group.get_table(table_key)->size() + num_rows < max_rows) {
        log << "{ std::vector<ObjKey> keys; wt->get_table(" << table_key << ")->create_objects("
            << num_rows % add_empty_row_max << ", keys); }\n";
        std::vector<ObjKey> keys;
        group.get_table(table_key)->create_objects(num_rows % add_empty_row_max, keys);
    }
}

void FuzzObject::add_column(Group& group, FuzzLog& log, State& s)
{
    log << "FuzzObject::add_column();\n";
    TableKey table_key = group.get_table_keys()[get_next_token(s) % group.size()];
    DataType type = get_type(get_next_token(s));
    std::string name = create_column_name(type);
    // Mixed cannot be nullable. For other types, chose nullability randomly
    bool nullable = (get_next_token(s) % 2 == 0);
    log << "group.get_table(" << table_key << ")->add_column(DataType(" << int(type) << "), \"" << name << "\", "
        << (nullable ? "true" : "false") << ");";
    auto col = group.get_table(table_key)->add_column(type, name, nullable);
    log << " // -> " << col << "\n";
}

void FuzzObject::remove_column(Group& group, FuzzLog& log, State& s)
{
    log << "FuzzObject::remove_column();\n";
    TableKey table_key = group.get_table_keys()[get_next_token(s) % group.size()];
    TableRef t = group.get_table(table_key);
    auto column_keys = t->get_column_keys();
    if (!column_keys.empty()) {
        ColKey col = column_keys[get_next_token(s) % column_keys.size()];
        log << "group.get_table(" << table_key << ")->remove_column(" << col << ");\n";
        t->remove_column(col);
    }
}

void FuzzObject::rename_column(Group& group, FuzzLog& log, State& s)
{
    log << "FuzzObject::rename_column();\n";
    TableKey table_key = group.get_table_keys()[get_next_token(s) % group.size()];
    TableRef t = group.get_table(table_key);
    auto column_keys = t->get_column_keys();
    if (!column_keys.empty()) {
        ColKey col = column_keys[get_next_token(s) % column_keys.size()];
        std::string name = create_column_name(t->get_column_type(col));
        log << "group.get_table(" << table_key << ")->rename_column(" << col << ", \"" << name << "\");\n";
        t->rename_column(col, name);
    }
}

void FuzzObject::add_search_index(Group& group, FuzzLog& log, State& s)
{
    log << "FuzzObject::add_search_index();\n";
    TableKey table_key = group.get_table_keys()[get_next_token(s) % group.size()];
    TableRef t = group.get_table(table_key);
    auto column_keys = t->get_column_keys();
    if (!column_keys.empty()) {
        ColKey col = column_keys[get_next_token(s) % column_keys.size()];
        bool supports_search_index = StringIndex::type_supported(t->get_column_type(col));

        if (supports_search_index) {
            log << "group.get_table(" << table_key << ")->add_search_index(" << col << ");\n";
            t->add_search_index(col);
        }
    }
}

void FuzzObject::remove_search_index(Group& group, FuzzLog& log, State& s)
{
    log << "FuzzObject::remove_search_index();\n";
    TableKey table_key = group.get_table_keys()[get_next_token(s) % group.size()];
    TableRef t = group.get_table(table_key);
    auto column_keys = t->get_column_keys();
    if (!column_keys.empty()) {
        ColKey col = column_keys[get_next_token(s) % column_keys.size()];
        // We don't need to check if the column is of a type that is indexable or if it has index on or off
        // because Realm will just do a no-op at worst (no exception or assert).
        log << "group.get_table(" << table_key << ")->remove_search_index(" << col << ");\n";
        t->remove_search_index(col);
    }
}

void FuzzObject::add_column_link(Group& group, FuzzLog& log, State& s)
{
    log << "FuzzObject::add_column_link();\n";
    TableKey table_key_1 = group.get_table_keys()[get_next_token(s) % group.size()];
    TableKey table_key_2 = group.get_table_keys()[get_next_token(s) % group.size()];
    TableRef t1 = group.get_table(table_key_1);
    TableRef t2 = group.get_table(table_key_2);
    std::string name = create_column_name(type_Link);
    log << "group.get_table(" << table_key_1 << ")->add_column_link(type_Link, \"" << name << "\", *group->get_table("
        << table_key_2 << "));";
    auto col = t1->add_column(*t2, name);
    log << " // -> " << col << "\n";
}

void FuzzObject::add_column_link_list(Group& group, FuzzLog& log, State& s)
{
    log << "FuzzObject::add_column_link_list();\n";
    TableKey table_key_1 = group.get_table_keys()[get_next_token(s) % group.size()];
    TableKey table_key_2 = group.get_table_keys()[get_next_token(s) % group.size()];
    TableRef t1 = group.get_table(table_key_1);
    TableRef t2 = group.get_table(table_key_2);
    std::string name = create_column_name(type_LinkList);
    log << "group.get_table(" << table_key_1 << ")->add_column_link(type_LinkList, \"" << name
        << "\", group.get_table(" << table_key_2 << "));";
    auto col = t1->add_column_list(*t2, name);
    log << " // -> " << col << "\n";
}

void FuzzObject::set_obj(Group& group, FuzzLog& log, State& s)
{
    log << "FuzzObject::set_obj();\n";
    TableKey table_key = group.get_table_keys()[get_next_token(s) % group.size()];
    TableRef t = group.get_table(table_key);
    auto all_col_keys = t->get_column_keys();
    if (!all_col_keys.empty() && t->size() > 0) {
        ColKey col = all_col_keys[get_next_token(s) % all_col_keys.size()];
        size_t row = get_next_token(s) % t->size();
        DataType type = t->get_column_type(col);
        Obj obj = t->get_object(row);
        log << "{\nObj obj = group.get_table(" << table_key << ")->get_object(" << row << ");\n";

        // With equal probability, either set to null or to a value
        if (get_next_token(s) % 2 == 0 && t->is_nullable(col)) {
            if (type == type_Link) {
                log << "obj.set(" << col << ", null_key);\n";
                obj.set(col, null_key);
            }
            else {
                log << "obj.set_null(" << col << ");\n";
                obj.set_null(col);
            }
        }
        else {
            if (type == type_String) {
                std::string str = create_string(get_next_token(s));
                log << "obj.set(" << col << ", \"" << str << "\");\n";
                obj.set(col, StringData(str));
            }
            else if (type == type_Binary) {
                std::string str = create_string(get_next_token(s));
                log << "obj.set<Binary>(" << col << ", BinaryData{\"" << str << "\", " << str.size() << "});\n";
                obj.set<Binary>(col, BinaryData(str));
            }
            else if (type == type_Int) {
                bool add_int = get_next_token(s) % 2 == 0;
                int64_t value = get_int64(s);
                if (add_int) {
                    log << "try { obj.add_int(" << col << ", " << value
                        << "); } catch (const LogicError& le) { CHECK(le.kind() == "
                           "LogicError::illegal_combination); }\n";
                    try {
                        obj.add_int(col, value);
                    }
                    catch (const LogicError& le) {
                        if (le.code() != ErrorCodes::IllegalOperation) {
                            throw;
                        }
                    }
                }
                else {
                    log << "obj.set<Int>(" << col << ", " << value << ");\n";
                    obj.set<Int>(col, value);
                }
            }
            else if (type == type_Bool) {
                bool value = get_next_token(s) % 2 == 0;
                log << "obj.set<Bool>(" << col << ", " << (value ? "true" : "false") << ");\n";
                obj.set<Bool>(col, value);
            }
            else if (type == type_Float) {
                float value = get_next_token(s);
                log << "obj.set<Float>(" << col << ", " << value << ");\n";
                obj.set<Float>(col, value);
            }
            else if (type == type_Double) {
                double value = get_next_token(s);
                log << "obj.set<double>(" << col << ", " << value << ");\n";
                obj.set<double>(col, value);
            }
            else if (type == type_Link) {
                TableRef target = t->get_link_target(col);
                if (target->size() > 0) {
                    ObjKey target_key = target->get_object(get_next_token(s) % target->size()).get_key();
                    log << "obj.set<Key>(" << col << ", " << target_key << ");\n";
                    obj.set(col, target_key);
                }
            }
            else if (type == type_LinkList) {
                TableRef target = t->get_link_target(col);
                if (target->size() > 0) {
                    LnkLst links = obj.get_linklist(col);
                    ObjKey target_key = target->get_object(get_next_token(s) % target->size()).get_key();
                    // either add or set, 50/50 probability
                    if (links.size() > 0 && get_next_token(s) > 128) {
                        size_t linklist_row = get_next_token(s) % links.size();
                        log << "obj.get_linklist(" << col << ")->set(" << linklist_row << ", " << target_key
                            << ");\n";
                        links.set(linklist_row, target_key);
                    }
                    else {
                        log << "obj.get_linklist(" << col << ")->add(" << target_key << ");\n";
                        links.add(target_key);
                    }
                }
            }
            else if (type == type_Timestamp) {
                std::pair<int64_t, int32_t> values = get_timestamp_values(s);
                Timestamp value{values.first, values.second};
                log << "obj.set(" << col << ", " << value << ");\n";
                obj.set(col, value);
            }
        }
        log << "}\n";
    }
    else {
        log << "table " << table_key << " has size = " << t->size()
            << " and get_column_keys size = " << all_col_keys.size() << "\n";
    }
}

void FuzzObject::remove_obj(Group& group, FuzzLog& log, State& s)
{
    log << "FuzzObject::remove_obj();\n";
    TableKey table_key = group.get_table_keys()[get_next_token(s) % group.size()];
    TableRef t = group.get_table(table_key);
    if (t->size() > 0) {
        ObjKey key = t->get_object(get_next_token(s) % t->size()).get_key();
        log << "group.get_table(" << table_key << ")->remove_object(" << key << ");\n";
        t->remove_object(key);
    }
}

void FuzzObject::remove_recursive(Group& group, FuzzLog& log, State& s)
{
    log << "FuzzObject::remove_recursive();\n";
    TableKey table_key = group.get_table_keys()[get_next_token(s) % group.size()];
    TableRef t = group.get_table(table_key);
    if (t->size() > 0) {
        ObjKey key = t->get_object(get_next_token(s) % t->size()).get_key();
        log << "group.get_table(" << table_key << ")->remove_object_recursive(" << key << ");\n";
        t->remove_object_recursive(key);
    }
}

void FuzzObject::enumerate_column(Group& group, FuzzLog& log, State& s)
{
    log << "FuzzObject::enumerate_column();\n";
    TableKey table_key = group.get_table_keys()[get_next_token(s) % group.size()];
    TableRef t = group.get_table(table_key);
    auto all_col_keys = t->get_column_keys();
    if (!all_col_keys.empty()) {
        size_t ndx = get_next_token(s) % all_col_keys.size();
        ColKey col = all_col_keys[ndx];
        log << "group.get_table(" << table_key << ")->enumerate_string_column(" << col << ");\n";
        group.get_table(table_key)->enumerate_string_column(col);
    }
}

void FuzzObject::get_all_column_names(Group& group, FuzzLog& log)
{
    log << "FuzzObject::get_all_column_names();\n";
    for (auto table_key : group.get_table_keys()) {
        TableRef t = group.get_table(table_key);
        auto all_col_keys = t->get_column_keys();
        for (auto col : all_col_keys) {
            StringData col_name = t->get_column_name(col);
            static_cast<void>(col_name);
        }
    }
}

void FuzzObject::commit(SharedRealm shared_realm, FuzzLog& log)
{
    log << "FuzzObject::commit();\n";
    log << "FuzzObject::commit() - shared_realm->is_in_transaction();\n";
    if (shared_realm->is_in_transaction()) {
        log << "FuzzObject::commit() - shared_realm->commit_transaction();\n";
        shared_realm->commit_transaction();
        auto& group = shared_realm->read_group();
        REALM_DO_IF_VERIFY(log, group.verify());
    }
}

void FuzzObject::rollback(SharedRealm shared_realm, Group& group, FuzzLog& log)
{
    log << "FuzzObject::rollback()\n";
    if (!shared_realm->is_in_async_transaction() && !shared_realm->is_in_transaction()) {
        shared_realm->begin_transaction();
        REALM_DO_IF_VERIFY(log, group.verify());
        log << "shared_realm->cancel_transaction();\n";
        shared_realm->cancel_transaction();
        REALM_DO_IF_VERIFY(log, shared_realm->read_group().verify());
    }
}

void FuzzObject::advance(realm::SharedRealm shared_realm, FuzzLog& log)
{
    log << "FuzzObject::advance();\n";
    shared_realm->notify();
}

void FuzzObject::close_and_reopen(SharedRealm& shared_realm, FuzzLog& log, const Realm::Config& config)
{
    log << "Open/close realm\n";
    shared_realm->close();
    shared_realm.reset();
    shared_realm = Realm::get_shared_realm(config);
    log << "Verify group after realm got reopened\n";
    auto& group = shared_realm->read_group();
    REALM_DO_IF_VERIFY(log, group.verify());
}

void FuzzObject::create_table_view(Group& group, FuzzLog& log, State& s, std::vector<TableView>& table_views)
{
    log << "FuzzObject::create_table_view();\n";
    TableKey table_key = group.get_table_keys()[get_next_token(s) % group.size()];
    TableRef t = group.get_table(table_key);
    log << "table_views.push_back(wt->get_table(" << table_key << ")->where().find_all());\n";
    TableView tv = t->where().find_all();
    table_views.push_back(tv);
}

void FuzzObject::check_null(Group& group, FuzzLog& log, State& s)
{
    log << "FuzzObject::check_null();\n";
    TableKey table_key = group.get_table_keys()[get_next_token(s) % group.size()];
    TableRef t = group.get_table(table_key);
    if (t->get_column_count() > 0 && t->size() > 0) {
        auto all_col_keys = t->get_column_keys();
        size_t ndx = get_next_token(s) % all_col_keys.size();
        ColKey col = all_col_keys[ndx];
        ObjKey key = t->get_object(get_int32(s) % t->size()).get_key();
        log << "group.get_table(" << table_key << ")->get_object(" << key << ").is_null(" << col << ");\n";
        bool res = t->get_object(key).is_null(col);
        static_cast<void>(res);
    }
}

DataType FuzzObject::get_type(unsigned char c) const
{
    DataType types[] = {type_Int, type_Bool, type_Float, type_Double, type_String, type_Binary, type_Timestamp};

    unsigned char mod = c % (sizeof(types) / sizeof(DataType));
    return types[mod];
}

const char* FuzzObject::get_encryption_key() const
{
#if REALM_ENABLE_ENCRYPTION
    return "1234567890123456789012345678901123456789012345678901234567890123";
#else
    return nullptr;
#endif
}

int64_t FuzzObject::get_int64(State& s) const
{
    int64_t v = 0;
    for (size_t t = 0; t < 8; t++) {
        unsigned char c = get_next_token(s);
        *(reinterpret_cast<signed char*>(&v) + t) = c;
    }
    return v;
}

int32_t FuzzObject::get_int32(State& s) const
{
    int32_t v = 0;
    for (size_t t = 0; t < 4; t++) {
        unsigned char c = get_next_token(s);
        *(reinterpret_cast<signed char*>(&v) + t) = c;
    }
    return v;
}

std::string FuzzObject::create_string(size_t length) const
{
    REALM_ASSERT_3(length, <, 256);
    static auto& chrs = "abcdefghijklmnopqrstuvwxyz"
                        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    thread_local static std::mt19937 rg{std::random_device{}()};
    thread_local static std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(chrs) - 2);
    std::string s;
    s.reserve(length);
    while (length--)
        s += chrs[pick(rg)];
    return s;
}

std::pair<int64_t, int32_t> FuzzObject::get_timestamp_values(State& s) const
{
    int64_t seconds = get_int64(s);
    int32_t nanoseconds = get_int32(s) % 1000000000;
    // Make sure the values form a sensible Timestamp
    const bool both_non_negative = seconds >= 0 && nanoseconds >= 0;
    const bool both_non_positive = seconds <= 0 && nanoseconds <= 0;
    const bool correct_timestamp = both_non_negative || both_non_positive;
    if (!correct_timestamp) {
        nanoseconds = -nanoseconds;
    }
    return {seconds, nanoseconds};
}

std::string FuzzObject::create_column_name(DataType t)
{
    std::string str;
    switch (t) {
        case type_Int:
            str = "int_";
            break;
        case type_Bool:
            str = "bool_";
            break;
        case type_Float:
            str = "float_";
            break;
        case type_Double:
            str = "double_";
            break;
        case type_String:
            str = "string_";
            break;
        case type_Binary:
            str = "binary_";
            break;
        case type_Timestamp:
            str = "date_";
            break;
        case type_Decimal:
            str = "decimal_";
            break;
        case type_ObjectId:
            str = "id_";
            break;
        case type_Link:
            str = "link_";
            break;
        case type_TypedLink:
            str = "typed_link_";
            break;
        case type_LinkList:
            str = "link_list_";
            break;
        case type_UUID:
            str = "uuid_";
            break;
        case type_Mixed:
            str = "any_";
            break;
    }
    return str + util::to_string(m_column_index++);
}

std::string FuzzObject::create_table_name()
{
    std::string str = "Table_";
    return str + util::to_string(m_table_index++);
}

std::string FuzzObject::get_current_time_stamp() const
{
    std::time_t t = std::time(nullptr);
    const int str_size = 100;
    char str_buffer[str_size] = {0};
    std::strftime(str_buffer, str_size, "%c", std::localtime(&t));
    return str_buffer;
}
