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
#include <realm/group.hpp>
#include <realm/data_type.hpp>
#include <realm/util/optional.hpp>
#include <realm/object-store/shared_realm.hpp>

DataType FuzzObject::get_type(unsigned char c)
{
    DataType types[] = {type_Int, type_Bool, type_Float, type_Double, type_String, type_Binary, type_Timestamp};

    unsigned char mod = c % (sizeof(types) / sizeof(DataType));
    return types[mod];
}

const char* get_encryption_key()
{
#if REALM_ENABLE_ENCRYPTION
    return "1234567890123456789012345678901123456789012345678901234567890123";
#else
    return nullptr;
#endif
}

int64_t get_int64(State& s)
{
    int64_t v = 0;
    for (size_t t = 0; t < 8; t++) {
        unsigned char c = get_next(s);
        *(reinterpret_cast<signed char*>(&v) + t) = c;
    }
    return v;
}

int32_t get_int32(State& s)
{
    int32_t v = 0;
    for (size_t t = 0; t < 4; t++) {
        unsigned char c = get_next(s);
        *(reinterpret_cast<signed char*>(&v) + t) = c;
    }
    return v;
}

std::string create_string(size_t length)
{
    REALM_ASSERT_3(length, <, 256);
    char buf[256] = {0};
    for (size_t i = 0; i < length; i++)
        buf[i] = 'a' + (rand() % 20);
    return std::string{buf, length};
}

std::pair<int64_t, int32_t> get_timestamp_values(State& s)
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

int table_index = 0;
int column_index = 0;

std::string create_column_name(DataType t)
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
    return str + util::to_string(column_index++);
}

std::string create_table_name()
{
    std::string str = "Table_";
    return str + util::to_string(table_index++);
}

std::string get_current_time_stamp()
{
    std::time_t t = std::time(nullptr);
    const int str_size = 100;
    char str_buffer[str_size] = {0};
    std::strftime(str_buffer, str_size, "%c", std::localtime(&t));
    return str_buffer;
}


void FuzzObject::create_table(Group& group, std::ostream* log)
{
    std::string name = create_table_name();
    if (log) {
        *log << "group.add_table(\"" << name << "\");\n";
    }
    group.add_table(name);
}

void FuzzObject::remove_table(Group& group, std::ostream* log)
{
    try {
        TableKey table_key = group.get_table_keys()[get_next(s) % group.size()];
        if (log) {
            *log << "try { wt->remove_table(" << table_key
                 << "); }"
                    " catch (const CrossTableLinkTarget&) { }\n";
        }
        group.remove_table(table_key);
    }
    catch (const CrossTableLinkTarget&) {
        if (log) {
            *log << "// Exception\n";
        }
    }
}

void FuzzObject::clear_table(Group& group, std::ostream* log, State& s)
{
    TableKey table_key = group.get_table_keys()[get_next(s) % group.size()];
    if (log) {
        *log << "wt->get_table(" << table_key << ")->clear();\n";
    }
    group.get_table(table_key)->clear();
}

void FuzzObject::create_object(Group& group, std::ostream* log, State& s)
{
    TableKey table_key = group.get_table_keys()[get_next(s) % group.size()];
    size_t num_rows = get_next(s);
    if (group.get_table(table_key)->size() + num_rows < max_rows) {
        if (log) {
            *log << "{ std::vector<ObjKey> keys; wt->get_table(" << table_key << ")->create_objects("
                 << num_rows % add_empty_row_max << ", keys); }\n";
        }
        std::vector<ObjKey> keys;
        group.get_table(table_key)->create_objects(num_rows % add_empty_row_max, keys);
    }
}

void FuzzObject::add_column(Group& group, std::ostream* log, State& s)
{
    TableKey table_key = group.get_table_keys()[get_next(s) % group.size()];
    DataType type = get_type(get_next(s));
    std::string name = create_column_name(type);
    // Mixed cannot be nullable. For other types, chose nullability randomly
    bool nullable = (get_next(s) % 2 == 0);
    if (log) {
        *log << "wt->get_table(" << table_key << ")->add_column(DataType(" << int(type) << "), \"" << name << "\", "
             << (nullable ? "true" : "false") << ");";
    }
    auto col = group.get_table(table_key)->add_column(type, name, nullable);
    if (log) {
        *log << " // -> " << col << "\n";
    }
}

void FuzzObject::remove_column(Group& group, std::ostream* log, State& s)
{
    TableKey table_key = group.get_table_keys()[get_next(s) % group.size()];
    TableRef t = group.get_table(table_key);
    auto column_keys = t->get_column_keys();
    if (!column_keys.empty()) {
        ColKey col = column_keys[get_next(s) % column_keys.size()];
        if (log) {
            *log << "wt->get_table(" << table_key << ")->remove_column(" << col << ");\n";
        }
        t->remove_column(col);
    }
}

void FuzzObject::rename_column(Group& group, std::ostream* log, State& s)
{
    TableKey table_key = group.get_table_keys()[get_next(s) % group.size()];
    TableRef t = group.get_table(table_key);
    auto column_keys = t->get_column_keys();
    if (!column_keys.empty()) {
        ColKey col = column_keys[get_next(s) % column_keys.size()];
        std::string name = create_column_name(t->get_column_type(col));
        if (log) {
            *log << "wt->get_table(" << table_key << ")->rename_column(" << col << ", \"" << name << "\");\n";
        }
        t->rename_column(col, name);
    }
}

void FuzzObject::add_search_index(Group& group, std::ostream* log, State& s)
{
    TableKey table_key = group.get_table_keys()[get_next(s) % group.size()];
    TableRef t = group.get_table(table_key);
    auto column_keys = t->get_column_keys();
    if (!column_keys.empty()) {
        ColKey col = column_keys[get_next(s) % column_keys.size()];
        bool supports_search_index = StringIndex::type_supported(t->get_column_type(col));

        if (supports_search_index) {
            if (log) {
                *log << "wt->get_table(" << table_key << ")->add_search_index(" << col << ");\n";
            }
            t->add_search_index(col);
        }
    }
}

void FuzzObject::remove_search_index(Group& group, std::ostream* log, State& s)
{
    TableKey table_key = group.get_table_keys()[get_next(s) % group.size()];
    TableRef t = group.get_table(table_key);
    auto column_keys = t->get_column_keys();
    if (!column_keys.empty()) {
        ColKey col = column_keys[get_next(s) % column_keys.size()];
        // We don't need to check if the column is of a type that is indexable or if it has index on or off
        // because Realm will just do a no-op at worst (no exception or assert).
        if (log) {
            *log << "wt->get_table(" << table_key << ")->remove_search_index(" << col << ");\n";
        }
        t->remove_search_index(col);
    }
}

void FuzzObject::add_column_link(Group& group, std::ostream* log, State& s)
{
    TableKey table_key_1 = group.get_table_keys()[get_next(s) % group.size()];
    TableKey table_key_2 = group.get_table_keys()[get_next(s) % group.size()];
    TableRef t1 = group.get_table(table_key_1);
    TableRef t2 = group.get_table(table_key_2);
    std::string name = create_column_name(type_Link);
    if (log) {
        *log << "wt->get_table(" << table_key_1 << ")->add_column_link(type_Link, \"" << name << "\", *wt->get_table("
             << table_key_2 << "));";
    }
    auto col = t1->add_column(*t2, name);
    if (log) {
        *log << " // -> " << col << "\n";
    }
}

void FuzzObject::add_column_link_list(Group& group, std::ostream* log, State& s)
{
    TableKey table_key_1 = group.get_table_keys()[get_next(s) % group.size()];
    TableKey table_key_2 = group.get_table_keys()[get_next(s) % group.size()];
    TableRef t1 = group.get_table(table_key_1);
    TableRef t2 = group.get_table(table_key_2);
    std::string name = create_column_name(type_LinkList);
    if (log) {
        *log << "wt->get_table(" << table_key_1 << ")->add_column_link(type_LinkList, \"" << name
             << "\", *wt->get_table(" << table_key_2 << "));";
    }
    auto col = t1->add_column_list(*t2, name);
    if (log) {
        *log << " // -> " << col << "\n";
    }
}

void FuzzObject::set_obj(Group& group, std::ostream* log, State& s)
{
    TableKey table_key = group.get_table_keys()[get_next(s) % group.size()];
    TableRef t = group.get_table(table_key);
    auto all_col_keys = t->get_column_keys();
    if (!all_col_keys.empty() && t->size() > 0) {
        ColKey col = all_col_keys[get_next(s) % all_col_keys.size()];
        size_t row = get_next(s) % t->size();
        DataType type = t->get_column_type(col);
        Obj obj = t->get_object(row);
        if (log) {
            *log << "{\nObj obj = wt->get_table(" << table_key << ")->get_object(" << row << ");\n";
        }

        // With equal probability, either set to null or to a value
        if (get_next(s) % 2 == 0 && t->is_nullable(col)) {
            if (type == type_Link) {
                if (log) {
                    *log << "obj.set(" << col << ", null_key);\n";
                }
                obj.set(col, null_key);
            }
            else {
                if (log) {
                    *log << "obj.set_null(" << col << ");\n";
                }
                obj.set_null(col);
            }
        }
        else {
            if (type == type_String) {
                std::string str = create_string(get_next(s));
                if (log) {
                    *log << "obj.set(" << col << ", \"" << str << "\");\n";
                }
                obj.set(col, StringData(str));
            }
            else if (type == type_Binary) {
                std::string str = create_string(get_next(s));
                if (log) {
                    *log << "obj.set<Binary>(" << col << ", BinaryData{\"" << str << "\", " << str.size() << "});\n";
                }
                obj.set<Binary>(col, BinaryData(str));
            }
            else if (type == type_Int) {
                bool add_int = get_next(s) % 2 == 0;
                int64_t value = get_int64(s);
                if (add_int) {
                    if (log) {
                        *log << "try { obj.add_int(" << col << ", " << value
                             << "); } catch (const LogicError& le) { CHECK(le.kind() == "
                                "LogicError::illegal_combination); }\n";
                    }
                    try {
                        obj.add_int(col, value);
                    }
                    catch (const LogicError& le) {
                        if (le.kind() != LogicError::illegal_combination) {
                            throw;
                        }
                    }
                }
                else {
                    if (log) {
                        *log << "obj.set<Int>(" << col << ", " << value << ");\n";
                    }
                    obj.set<Int>(col, value);
                }
            }
            else if (type == type_Bool) {
                bool value = get_next(s) % 2 == 0;
                if (log) {
                    *log << "obj.set<Bool>(" << col << ", " << (value ? "true" : "false") << ");\n";
                }
                obj.set<Bool>(col, value);
            }
            else if (type == type_Float) {
                float value = get_next(s);
                if (log) {
                    *log << "obj.set<Float>(" << col << ", " << value << ");\n";
                }
                obj.set<Float>(col, value);
            }
            else if (type == type_Double) {
                double value = get_next(s);
                if (log) {
                    *log << "obj.set<double>(" << col << ", " << value << ");\n";
                }
                obj.set<double>(col, value);
            }
            else if (type == type_Link) {
                TableRef target = t->get_link_target(col);
                if (target->size() > 0) {
                    ObjKey target_key = target->get_object(get_next(s) % target->size()).get_key();
                    if (log) {
                        *log << "obj.set<Key>(" << col << ", " << target_key << ");\n";
                    }
                    obj.set(col, target_key);
                }
            }
            else if (type == type_LinkList) {
                TableRef target = t->get_link_target(col);
                if (target->size() > 0) {
                    LnkLst links = obj.get_linklist(col);
                    ObjKey target_key = target->get_object(get_next(s) % target->size()).get_key();
                    // either add or set, 50/50 probability
                    if (links.size() > 0 && get_next(s) > 128) {
                        size_t linklist_row = get_next(s) % links.size();
                        if (log) {
                            *log << "obj.get_linklist(" << col << ")->set(" << linklist_row << ", " << target_key
                                 << ");\n";
                        }
                        links.set(linklist_row, target_key);
                    }
                    else {
                        if (log) {
                            *log << "obj.get_linklist(" << col << ")->add(" << target_key << ");\n";
                        }
                        links.add(target_key);
                    }
                }
            }
            else if (type == type_Timestamp) {
                std::pair<int64_t, int32_t> values = get_timestamp_values(s);
                Timestamp value{values.first, values.second};
                if (log) {
                    *log << "obj.set(" << col << ", " << value << ");\n";
                }
                obj.set(col, value);
            }
        }
        if (log) {
            *log << "}\n";
        }
    }
}

void FuzzObject::remove_obj(Group& group, std::ostream* log, State& s)
{
    TableKey table_key = group.get_table_keys()[get_next(s) % group.size()];
    TableRef t = group.get_table(table_key);
    if (t->size() > 0) {
        ObjKey key = t->get_object(get_next(s) % t->size()).get_key();
        if (log) {
            *log << "wt->get_table(" << table_key << ")->remove_object(" << key << ");\n";
        }
        t->remove_object(key);
    }
}

void FuzzObject::remove_recursive(Group& group, std::ostream* log, State& s)
{
    TableKey table_key = group.get_table_keys()[get_next(s) % group.size()];
    TableRef t = group.get_table(table_key);
    if (t->size() > 0) {
        ObjKey key = t->get_object(get_next(s) % t->size()).get_key();
        if (log) {
            *log << "wt->get_table(" << table_key << ")->remove_object_recursive(" << key << ");\n";
        }
        t->remove_object_recursive(key);
    }
}

void FuzzObject::enumerate_column(Group& group, std::ostream* log, State& s)
{
    TableKey table_key = group.get_table_keys()[get_next(s) % group.size()];
    TableRef t = group.get_table(table_key);
    auto all_col_keys = t->get_column_keys();
    if (!all_col_keys.empty()) {
        size_t ndx = get_next(s) % all_col_keys.size();
        ColKey col = all_col_keys[ndx];
        if (log) {
            *log << "wt->get_table(" << table_key << ")->enumerate_string_column(" << col << ");\n";
        }
        group.get_table(table_key)->enumerate_string_column(col);
    }
}

void FuzzObject::get_all_column_names(Group& group)
{
    // try to fuzz find this: https://github.com/realm/realm-core/issues/1769
    for (auto table_key : group.get_table_keys()) {
        TableRef t = group.get_table(table_key);
        auto all_col_keys = t->get_column_keys();
        for (auto col : all_col_keys) {
            StringData col_name = t->get_column_name(col);
            static_cast<void>(col_name);
        }
    }
}

void FuzzObject::commit(SharedRealm shared_realm, std::ostream* log)
{
    if (shared_realm->is_in_transaction()) {
        if (log) {
            *log << "shared_realm->commit_transaction();\n";
        }
        shared_realm->commit_transaction();
        REALM_DO_IF_VERIFY(log, shared_realm->read_group().verify());
    }
}

void FuzzObject::rollback(SharedRealm shared_realm, Group& group, std::ostream* log)
{
    if (log) {
        *log << "wt->rollback_and_continue_as_read();\n";
    }
    shared_realm->begin_transaction();
    REALM_DO_IF_VERIFY(log, group.verify());
    if (log) {
        *log << "wt->promote_to_write();\n";
    }
    shared_realm->cancel_transaction();
    REALM_DO_IF_VERIFY(log, shared_realm->read_group().verify());
}

void FuzzObject::advance(Group& group, std::ostream* log)
{
    if (log) {
        *log << "rt->advance_read();\n";
    }
    Transaction* tr = (Transaction*)(&group);
    tr->advance_read();
    REALM_DO_IF_VERIFY(log, tr->verify());
}

void FuzzObject::close_and_reopen(SharedRealm shared_realm, std::ostream* log, Realm::Config& config)
{
    if (log) {
        *log << "wt = nullptr;\n";
        *log << "rt = nullptr;\n";
        *log << "db->close();\n";
    }
    shared_realm->close();
    if (log) {
        *log << "db = DB::create(*hist, path, DBOptions(key));\n";
    }
    shared_realm = Realm::get_shared_realm(config);
    if (log) {
        *log << "wt = db_w->start_write();\n";
        *log << "rt = db->start_read();\n";
    }
    auto& group = shared_realm->read_group();
    REALM_DO_IF_VERIFY(log, group.verify());
}

void FuzzObject::create_table_view(Group& group, std::ostream* log, State& s, std::vector<TableView>& table_views)
{
    TableKey table_key = group.get_table_keys()[get_next(s) % group.size()];
    TableRef t = group.get_table(table_key);
    if (log) {
        *log << "table_views.push_back(wt->get_table(" << table_key << ")->where().find_all());\n";
    }
    TableView tv = t->where().find_all();
    table_views.push_back(tv);
}

void FuzzObject::check_null(Group& group, std::ostream* log, State& s)
{
    TableKey table_key = group.get_table_keys()[get_next(s) % group.size()];
    TableRef t = group.get_table(table_key);
    if (t->get_column_count() > 0 && t->size() > 0) {
        auto all_col_keys = t->get_column_keys();
        size_t ndx = get_next(s) % all_col_keys.size();
        ColKey col = all_col_keys[ndx];
        ObjKey key = t->get_object(get_int32(s) % t->size()).get_key();
        if (log) {
            *log << "wt->get_table(" << table_key << ")->get_object(" << key << ").is_null(" << col << ");\n";
        }
        bool res = t->get_object(key).is_null(col);
        static_cast<void>(res);
    }
}

void FuzzObject::async_write(SharedRealm shared_realm, std::ostream* log)
{
    if (log) {
        *log << "Async write \n";
    }
    if (!shared_realm->is_in_async_transaction() && !shared_realm->is_in_transaction()) {
        shared_realm->async_begin_transaction([&]() {
            shared_realm->async_commit_transaction([](std::exception_ptr) {});
        });
    }
}

void FuzzObject::async_cancel(SharedRealm shared_realm, Group& group, std::ostream* log, State& s)
{
    if (log) {
        *log << "Async cancel \n";
    }
    auto token = shared_realm->async_begin_transaction([&]() {
        TableKey table_key = group.get_table_keys()[get_next(s) % group.size()];
        size_t num_rows = get_next(s);
        if (group.get_table(table_key)->size() + num_rows < max_rows) {
            if (log) {
                *log << "{ std::vector<ObjKey> keys; wt->get_table(" << table_key << ")->create_objects("
                     << num_rows % add_empty_row_max << ", keys); }\n";
            }
            std::vector<ObjKey> keys;
            group.get_table(table_key)->create_objects(num_rows % add_empty_row_max, keys);
        }
    });
    shared_realm->async_cancel_transaction(token);
}