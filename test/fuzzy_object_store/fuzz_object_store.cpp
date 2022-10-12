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

#include "fuzz_object_store.hpp"

#include <realm.hpp>
#include <realm/index_string.hpp>

#include <ctime>
#include <cstdio>
#include <fstream>
#include <iostream>

#include "../test/util/test_path.hpp"
#include <realm/object-store/shared_realm.hpp>

using namespace realm;
using namespace realm::util;

#define TEST_FUZZ
// #ifdef TEST_FUZZ
//  Determines whether or not to run the shared group verify function
//  after each transaction. This will find errors earlier but is expensive.
#define REALM_VERIFY true

#if REALM_VERIFY
#define REALM_DO_IF_VERIFY(log, op)                                                                                  \
    do {                                                                                                             \
        if (log)                                                                                                     \
            *log << #op << ";\n";                                                                                    \
        op;                                                                                                          \
    } while (false)
#else
#define REALM_DO_IF_VERIFY(log, owner)                                                                               \
    do {                                                                                                             \
    } while (false)
#endif

namespace {

struct EndOfFile {};

enum INS {
    ADD_TABLE,
    REMOVE_TABLE,
    CREATE_OBJECT,
    RENAME_COLUMN,
    ADD_COLUMN,
    REMOVE_COLUMN,
    SET,
    REMOVE_OBJECT,
    REMOVE_RECURSIVE,
    ADD_COLUMN_LINK,
    ADD_COLUMN_LINK_LIST,
    CLEAR_TABLE,
    ADD_SEARCH_INDEX,
    REMOVE_SEARCH_INDEX,
    COMMIT,
    ROLLBACK,
    ADVANCE,
    MOVE_LAST_OVER,
    CLOSE_AND_REOPEN,
    GET_ALL_COLUMN_NAMES,
    CREATE_TABLE_VIEW,
    COMPACT,
    IS_NULL,
    ENUMERATE_COLUMN,

    ASYNC_WRITE,
    ASYNC_CANCEL,
    ASYNC_RUN,

    COUNT
};

DataType get_type(unsigned char c)
{
    DataType types[] = {type_Int, type_Bool, type_Float, type_Double, type_String, type_Binary, type_Timestamp};

    unsigned char mod = c % (sizeof(types) / sizeof(DataType));
    return types[mod];
}

struct State {
    std::string str;
    size_t pos;
};

unsigned char get_next(State& s)
{
    if (s.pos == s.str.size()) {
        throw EndOfFile{};
    }
    char byte = s.str[s.pos];
    s.pos++;
    return byte;
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

// You can use this variable to make a conditional breakpoint if you know that
// a problem occurs after a certain amount of iterations.
int iteration = 0;
} // anonymous namespace


void parse_and_apply_instructions_object_store(std::string& in, const std::string& path, std::ostream* log)
{


    const size_t add_empty_row_max = REALM_MAX_BPNODE_SIZE * REALM_MAX_BPNODE_SIZE + 1000;
    const size_t max_tables = REALM_MAX_BPNODE_SIZE * 10;

    // Max number of rows in a table. Overridden only by create_object() and only in the case where
    // max_rows is not exceeded *prior* to executing add_empty_row.
    const size_t max_rows = 100000;
    column_index = table_index = 0;

    State s;
    s.str = in;
    s.pos = 0;

    // const bool use_encryption = false;
    const bool use_encryption = get_next(s) % 2 == 0;

    struct TestConfig : public Realm::Config {
        TestConfig(std::string local_path, bool use_encryption)
        {
            disable_sync_to_disk();
            path = local_path;
            schema_version = 0;
            if (use_encryption) {
                const char* key = get_encryption_key();
                const char* i = key;
                while (*i != '\0') {
                    encryption_key.push_back(*i);
                    i++;
                }
            }
        }
    };
    TestConfig config{path, use_encryption};


    if (log) {
        *log << "// Test case generated in " REALM_VER_CHUNK " on " << get_current_time_stamp() << ".\n";
        *log << "// REALM_MAX_BPNODE_SIZE is " << REALM_MAX_BPNODE_SIZE << "\n";
        *log << "// ----------------------------------------------------------------------\n";
        std::string printable_key;
        if (!use_encryption) {
            printable_key = "nullptr";
        }
        else {
            printable_key = std::string("\"") + config.encryption_key.data() + "\"";
        }
        *log << "const char* key = " << printable_key << ";\n";
        *log << "\n";
    }
    auto shared_realm = Realm::get_shared_realm(config);
    auto& group = shared_realm->read_group();
    std::vector<TableView> table_views;
    try {
        for (;;) {
            char instr = get_next(s) % COUNT;
            iteration++;

            // This can help when debugging
            if (log) {
                *log << iteration << " ";
            }

            auto fetch_group = [shared_realm]() -> Group& {
                if (!shared_realm->is_in_transaction()) {
                    shared_realm->begin_transaction();
                }
                return shared_realm->read_group();
            };

            if (instr == ADD_TABLE && group.size() < max_tables) {

                std::string name = create_table_name();
                if (log) {
                    *log << "group.add_table(\"" << name << "\");\n";
                }
                auto& group = fetch_group();
                group.add_table(name);
            }
            else if (instr == REMOVE_TABLE && group.size() > 0) {

                auto& group = fetch_group();
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
            //            else if (instr == CLEAR_TABLE && group->size() > 0) {
            //                TableKey table_key = group->get_table_keys()[get_next(s) % group->size()];
            //                if (log) {
            //                    *log << "wt->get_table(" << table_key << ")->clear();\n";
            //                }
            //                group->get_table(table_key)->clear();
            //            }
            //            else if (instr == CREATE_OBJECT && group->size() > 0) {
            //                TableKey table_key = group->get_table_keys()[get_next(s) % group->size()];
            //                size_t num_rows = get_next(s);
            //                if (group->get_table(table_key)->size() + num_rows < max_rows) {
            //                    if (log) {
            //                        *log << "{ std::vector<ObjKey> keys; wt->get_table(" << table_key <<
            //                        ")->create_objects("
            //                             << num_rows % add_empty_row_max << ", keys); }\n";
            //                    }
            //                    std::vector<ObjKey> keys;
            //                    group->get_table(table_key)->create_objects(num_rows % add_empty_row_max, keys);
            //                }
            //            }
            //            else if (instr == ADD_COLUMN && group->size() > 0) {
            //                TableKey table_key = group->get_table_keys()[get_next(s) % group->size()];
            //                DataType type = get_type(get_next(s));
            //                std::string name = create_column_name(type);
            //                // Mixed cannot be nullable. For other types, chose nullability randomly
            //                bool nullable = (get_next(s) % 2 == 0);
            //                if (log) {
            //                    *log << "wt->get_table(" << table_key << ")->add_column(DataType(" << int(type) <<
            //                    "), \"" << name
            //                         << "\", " << (nullable ? "true" : "false") << ");";
            //                }
            //                auto col = group->get_table(table_key)->add_column(type, name, nullable);
            //                if (log) {
            //                    *log << " // -> " << col << "\n";
            //                }
            //            }
            //            else if (instr == REMOVE_COLUMN && group->size() > 0) {
            //                TableKey table_key = group->get_table_keys()[get_next(s) % group->size()];
            //                TableRef t = group->get_table(table_key);
            //                auto column_keys = t->get_column_keys();
            //                if (!column_keys.empty()) {
            //                    ColKey col = column_keys[get_next(s) % column_keys.size()];
            //                    if (log) {
            //                        *log << "wt->get_table(" << table_key << ")->remove_column(" << col << ");\n";
            //                    }
            //                    t->remove_column(col);
            //                }
            //            }
            //            else if (instr == RENAME_COLUMN && group->size() > 0) {
            //                TableKey table_key = group->get_table_keys()[get_next(s) % group->size()];
            //                TableRef t = group->get_table(table_key);
            //                auto column_keys = t->get_column_keys();
            //                if (!column_keys.empty()) {
            //                    ColKey col = column_keys[get_next(s) % column_keys.size()];
            //                    std::string name = create_column_name(t->get_column_type(col));
            //                    if (log) {
            //                        *log << "wt->get_table(" << table_key << ")->rename_column(" << col << ", \"" <<
            //                        name
            //                             << "\");\n";
            //                    }
            //                    t->rename_column(col, name);
            //                }
            //            }
            //            else if (instr == ADD_SEARCH_INDEX && group->size() > 0) {
            //                TableKey table_key = group->get_table_keys()[get_next(s) % group->size()];
            //                TableRef t = group->get_table(table_key);
            //                auto column_keys = t->get_column_keys();
            //                if (!column_keys.empty()) {
            //                    ColKey col = column_keys[get_next(s) % column_keys.size()];
            //                    bool supports_search_index = StringIndex::type_supported(t->get_column_type(col));
            //
            //                    if (supports_search_index) {
            //                        if (log) {
            //                            *log << "wt->get_table(" << table_key << ")->add_search_index(" << col <<
            //                            ");\n";
            //                        }
            //                        t->add_search_index(col);
            //                    }
            //                }
            //            }
            //            else if (instr == REMOVE_SEARCH_INDEX && group->size() > 0) {
            //                TableKey table_key = group->get_table_keys()[get_next(s) % group->size()];
            //                TableRef t = group->get_table(table_key);
            //                auto column_keys = t->get_column_keys();
            //                if (!column_keys.empty()) {
            //                    ColKey col = column_keys[get_next(s) % column_keys.size()];
            //                    // We don't need to check if the column is of a type that is indexable or if it has
            //                    index on or
            //                    // off
            //                    // because Realm will just do a no-op at worst (no exception or assert).
            //                    if (log) {
            //                        *log << "wt->get_table(" << table_key << ")->remove_search_index(" << col <<
            //                        ");\n";
            //                    }
            //                    t->remove_search_index(col);
            //                }
            //            }
            //            else if (instr == ADD_COLUMN_LINK && group->size() >= 1) {
            //                TableKey table_key_1 = group->get_table_keys()[get_next(s) % group->size()];
            //                TableKey table_key_2 = group->get_table_keys()[get_next(s) % group->size()];
            //                TableRef t1 = group->get_table(table_key_1);
            //                TableRef t2 = group->get_table(table_key_2);
            //                std::string name = create_column_name(type_Link);
            //                if (log) {
            //                    *log << "wt->get_table(" << table_key_1 << ")->add_column_link(type_Link, \"" <<
            //                    name
            //                         << "\", *wt->get_table(" << table_key_2 << "));";
            //                }
            //                auto col = t1->add_column(*t2, name);
            //                if (log) {
            //                    *log << " // -> " << col << "\n";
            //                }
            //            }
            //            else if (instr == ADD_COLUMN_LINK_LIST && group->size() >= 2) {
            //                TableKey table_key_1 = group->get_table_keys()[get_next(s) % group->size()];
            //                TableKey table_key_2 = group->get_table_keys()[get_next(s) % group->size()];
            //                TableRef t1 = group->get_table(table_key_1);
            //                TableRef t2 = group->get_table(table_key_2);
            //                std::string name = create_column_name(type_LinkList);
            //                if (log) {
            //                    *log << "wt->get_table(" << table_key_1 << ")->add_column_link(type_LinkList, \"" <<
            //                    name
            //                         << "\", *wt->get_table(" << table_key_2 << "));";
            //                }
            //                auto col = t1->add_column_list(*t2, name);
            //                if (log) {
            //                    *log << " // -> " << col << "\n";
            //                }
            //            }
            //            else if (instr == SET && group->size() > 0) {
            //                TableKey table_key = group->get_table_keys()[get_next(s) % group->size()];
            //                TableRef t = group->get_table(table_key);
            //                auto all_col_keys = t->get_column_keys();
            //                if (!all_col_keys.empty() && t->size() > 0) {
            //                    ColKey col = all_col_keys[get_next(s) % all_col_keys.size()];
            //                    size_t row = get_next(s) % t->size();
            //                    DataType type = t->get_column_type(col);
            //                    Obj obj = t->get_object(row);
            //                    if (log) {
            //                        *log << "{\nObj obj = wt->get_table(" << table_key << ")->get_object(" << row <<
            //                        ");\n";
            //                    }
            //
            //                    // With equal probability, either set to null or to a value
            //                    if (get_next(s) % 2 == 0 && t->is_nullable(col)) {
            //                        if (type == type_Link) {
            //                            if (log) {
            //                                *log << "obj.set(" << col << ", null_key);\n";
            //                            }
            //                            obj.set(col, null_key);
            //                        }
            //                        else {
            //                            if (log) {
            //                                *log << "obj.set_null(" << col << ");\n";
            //                            }
            //                            obj.set_null(col);
            //                        }
            //                    }
            //                    else {
            //                        if (type == type_String) {
            //                            std::string str = create_string(get_next(s));
            //                            if (log) {
            //                                *log << "obj.set(" << col << ", \"" << str << "\");\n";
            //                            }
            //                            obj.set(col, StringData(str));
            //                        }
            //                        else if (type == type_Binary) {
            //                            std::string str = create_string(get_next(s));
            //                            if (log) {
            //                                *log << "obj.set<Binary>(" << col << ", BinaryData{\"" << str << "\", "
            //                                << str.size()
            //                                     << "});\n";
            //                            }
            //                            obj.set<Binary>(col, BinaryData(str));
            //                        }
            //                        else if (type == type_Int) {
            //                            bool add_int = get_next(s) % 2 == 0;
            //                            int64_t value = get_int64(s);
            //                            if (add_int) {
            //                                if (log) {
            //                                    *log << "try { obj.add_int(" << col << ", " << value
            //                                         << "); } catch (const LogicError& le) { CHECK(le.kind() == "
            //                                            "LogicError::illegal_combination); }\n";
            //                                }
            //                                try {
            //                                    obj.add_int(col, value);
            //                                }
            //                                catch (const LogicError& le) {
            //                                    if (le.kind() != LogicError::illegal_combination) {
            //                                        throw;
            //                                    }
            //                                }
            //                            }
            //                            else {
            //                                if (log) {
            //                                    *log << "obj.set<Int>(" << col << ", " << value << ");\n";
            //                                }
            //                                obj.set<Int>(col, value);
            //                            }
            //                        }
            //                        else if (type == type_Bool) {
            //                            bool value = get_next(s) % 2 == 0;
            //                            if (log) {
            //                                *log << "obj.set<Bool>(" << col << ", " << (value ? "true" : "false") <<
            //                                ");\n";
            //                            }
            //                            obj.set<Bool>(col, value);
            //                        }
            //                        else if (type == type_Float) {
            //                            float value = get_next(s);
            //                            if (log) {
            //                                *log << "obj.set<Float>(" << col << ", " << value << ");\n";
            //                            }
            //                            obj.set<Float>(col, value);
            //                        }
            //                        else if (type == type_Double) {
            //                            double value = get_next(s);
            //                            if (log) {
            //                                *log << "obj.set<double>(" << col << ", " << value << ");\n";
            //                            }
            //                            obj.set<double>(col, value);
            //                        }
            //                        else if (type == type_Link) {
            //                            TableRef target = t->get_link_target(col);
            //                            if (target->size() > 0) {
            //                                ObjKey target_key = target->get_object(get_next(s) %
            //                                target->size()).get_key(); if (log) {
            //                                    *log << "obj.set<Key>(" << col << ", " << target_key << ");\n";
            //                                }
            //                                obj.set(col, target_key);
            //                            }
            //                        }
            //                        else if (type == type_LinkList) {
            //                            TableRef target = t->get_link_target(col);
            //                            if (target->size() > 0) {
            //                                LnkLst links = obj.get_linklist(col);
            //                                ObjKey target_key = target->get_object(get_next(s) %
            //                                target->size()).get_key();
            //                                // either add or set, 50/50 probability
            //                                if (links.size() > 0 && get_next(s) > 128) {
            //                                    size_t linklist_row = get_next(s) % links.size();
            //                                    if (log) {
            //                                        *log << "obj.get_linklist(" << col << ")->set(" << linklist_row
            //                                        << ", "
            //                                             << target_key << ");\n";
            //                                    }
            //                                    links.set(linklist_row, target_key);
            //                                }
            //                                else {
            //                                    if (log) {
            //                                        *log << "obj.get_linklist(" << col << ")->add(" << target_key <<
            //                                        ");\n";
            //                                    }
            //                                    links.add(target_key);
            //                                }
            //                            }
            //                        }
            //                        else if (type == type_Timestamp) {
            //                            std::pair<int64_t, int32_t> values = get_timestamp_values(s);
            //                            Timestamp value{values.first, values.second};
            //                            if (log) {
            //                                *log << "obj.set(" << col << ", " << value << ");\n";
            //                            }
            //                            obj.set(col, value);
            //                        }
            //                    }
            //                    if (log) {
            //                        *log << "}\n";
            //                    }
            //                }
            //            }
            //            else if (instr == REMOVE_OBJECT && group->size() > 0) {
            //                TableKey table_key = group->get_table_keys()[get_next(s) % group->size()];
            //                TableRef t = group->get_table(table_key);
            //                if (t->size() > 0) {
            //                    ObjKey key = t->get_object(get_next(s) % t->size()).get_key();
            //                    if (log) {
            //                        *log << "wt->get_table(" << table_key << ")->remove_object(" << key << ");\n";
            //                    }
            //                    t->remove_object(key);
            //                }
            //            }
            //            else if (instr == REMOVE_RECURSIVE && group->size() > 0) {
            //                TableKey table_key = group->get_table_keys()[get_next(s) % group->size()];
            //                TableRef t = group->get_table(table_key);
            //                if (t->size() > 0) {
            //                    ObjKey key = t->get_object(get_next(s) % t->size()).get_key();
            //                    if (log) {
            //                        *log << "wt->get_table(" << table_key << ")->remove_object_recursive(" << key <<
            //                        ");\n";
            //                    }
            //                    t->remove_object_recursive(key);
            //                }
            //            }
            //            else if (instr == ENUMERATE_COLUMN && group->size() > 0) {
            //                TableKey table_key = group->get_table_keys()[get_next(s) % group->size()];
            //                TableRef t = group->get_table(table_key);
            //                auto all_col_keys = t->get_column_keys();
            //                if (!all_col_keys.empty()) {
            //                    size_t ndx = get_next(s) % all_col_keys.size();
            //                    ColKey col = all_col_keys[ndx];
            //                    if (log) {
            //                        *log << "wt->get_table(" << table_key << ")->enumerate_string_column(" << col <<
            //                        ");\n";
            //                    }
            //                    group->get_table(table_key)->enumerate_string_column(col);
            //                }
            //            }
            else if (instr == COMMIT) {
                if (shared_realm->is_in_transaction()) {
                    if (log) {
                        *log << "shared_realm->commit_transaction();\n";
                    }
                    shared_realm->commit_transaction();
                    REALM_DO_IF_VERIFY(log, shared_realm->read_group().verify());
                }
            }
            //            else if (instr == ROLLBACK) {
            //                if (log) {
            //                    *log << "wt->rollback_and_continue_as_read();\n";
            //                }
            //                shared_realm->begin_transaction();
            //                REALM_DO_IF_VERIFY(log, group->verify());
            //                if (log) {
            //                    *log << "wt->promote_to_write();\n";
            //                }
            //                shared_realm->cancel_transaction();
            //                REALM_DO_IF_VERIFY(log, group->verify());
            //            }
            //            else if (instr == ADVANCE) {
            //                if (log) {
            //                    *log << "rt->advance_read();\n";
            //                }
            //                shared_realm->read_group().adva
            //                Transaction* tr = (Transaction*)group;
            //                tr->advance_read();
            //                REALM_DO_IF_VERIFY(log, tr->verify());
            //            }
            //            else if (instr == CLOSE_AND_REOPEN) {
            //                if (log) {
            //                    *log << "wt = nullptr;\n";
            //                    *log << "rt = nullptr;\n";
            //                    *log << "db->close();\n";
            //                }
            //                group = nullptr;
            //                shared_realm->close();
            //                if (log) {
            //                    *log << "db = DB::create(*hist, path, DBOptions(key));\n";
            //                }
            //                shared_realm = Realm::get_shared_realm(config);
            //                if (log) {
            //                    *log << "wt = db_w->start_write();\n";
            //                    *log << "rt = db->start_read();\n";
            //                }
            //                group = &shared_realm->read_group();
            //                REALM_DO_IF_VERIFY(log, group->verify());
            //            }
            //            else if (instr == GET_ALL_COLUMN_NAMES && group->size() > 0) {
            //                // try to fuzz find this: https://github.com/realm/realm-core/issues/1769
            //                for (auto table_key : group->get_table_keys()) {
            //                    TableRef t = group->get_table(table_key);
            //                    auto all_col_keys = t->get_column_keys();
            //                    for (auto col : all_col_keys) {
            //                        StringData col_name = t->get_column_name(col);
            //                        static_cast<void>(col_name);
            //                    }
            //                }
            //            }
            //            else if (instr == CREATE_TABLE_VIEW && group->size() > 0) {
            //                TableKey table_key = group->get_table_keys()[get_next(s) % group->size()];
            //                TableRef t = group->get_table(table_key);
            //                if (log) {
            //                    *log << "table_views.push_back(wt->get_table(" << table_key <<
            //                    ")->where().find_all());\n";
            //                }
            //                TableView tv = t->where().find_all();
            //                table_views.push_back(tv);
            //            }
            //            else if (instr == COMPACT) {
            //            }
            //            else if (instr == IS_NULL && group->size() > 0) {
            //                TableKey table_key = group->get_table_keys()[get_next(s) % group->size()];
            //                TableRef t = group->get_table(table_key);
            //                if (t->get_column_count() > 0 && t->size() > 0) {
            //                    auto all_col_keys = t->get_column_keys();
            //                    size_t ndx = get_next(s) % all_col_keys.size();
            //                    ColKey col = all_col_keys[ndx];
            //                    ObjKey key = t->get_object(get_int32(s) % t->size()).get_key();
            //                    if (log) {
            //                        *log << "wt->get_table(" << table_key << ")->get_object(" << key << ").is_null("
            //                        << col
            //                             << ");\n";
            //                    }
            //                    bool res = t->get_object(key).is_null(col);
            //                    static_cast<void>(res);
            //                }
            //            }
            //            else if(instr == ASYNC_WRITE && group->size() > 0) {
            //                if(log) {
            //                    *log << "Async write \n";
            //                }
            //                shared_realm->async_begin_transaction([&](){
            //                    shared_realm->async_commit_transaction([](std::exception_ptr){});
            //                });
            //            }
            //            else if(instr == ASYNC_CANCEL) {
            //                if(log) {
            //                    *log << "Async cancel \n";
            //                }
            //                auto token = shared_realm->async_begin_transaction([&](){
            //                    TableKey table_key = group->get_table_keys()[get_next(s) % group->size()];
            //                    size_t num_rows = get_next(s);
            //                    if (group->get_table(table_key)->size() + num_rows < max_rows) {
            //                        if (log) {
            //                            *log << "{ std::vector<ObjKey> keys; wt->get_table(" << table_key <<
            //                            ")->create_objects("
            //                            << num_rows % add_empty_row_max << ", keys); }\n";
            //                        }
            //                        std::vector<ObjKey> keys;
            //                        group->get_table(table_key)->create_objects(num_rows % add_empty_row_max, keys);
            //                    }
            //                });
            //                shared_realm->async_cancel_transaction(token);
            //            }
            //            else if(instr == ASYNC_RUN)
            //            {
            //                if(log) {
            //                    *log << "Async run \n";
            //                }
            //                auto& group = shared_realm->read_group();
            //                const int thread_count = 10;
            //                // Create first table in group
            //                {
            //                    shared_realm->begin_transaction();
            //                    auto t1 = group.add_table("test");
            //                    std::vector<ColKey> res;
            //                    res.push_back(t1->add_column(type_Int, "first"));
            //                    res.push_back(t1->add_column(type_Int, "second"));
            //                    res.push_back(t1->add_column(type_Bool, "third"));
            //                    res.push_back(t1->add_column(type_String, "fourth"));
            //                    for (int i = 0; i < thread_count; ++i)
            //                        t1->create_object(ObjKey(i)).set_all(0, 2, false, "test");
            //                    shared_realm->commit_transaction();
            //                }
            //
            //                Thread threads[thread_count];
            //
            //                auto writer = [&config](ObjKey key){
            //                    auto local_shared_realm = Realm::get_shared_realm(config);
            //                    auto& group = local_shared_realm->read_group();
            //                    for (size_t i = 0; i < 100; ++i) {
            //                        {
            //                            local_shared_realm->begin_transaction();
            //                            auto t1 = group.get_table("test");
            //                            auto cols = t1->get_column_keys();
            //                            t1->get_object(key).add_int(cols[0], 1);
            //                            t1->get_object(key).add_int(cols[1], 1);
            //                            t1->get_object(key).set_any(cols[2], true);
            //                            t1->get_object(key).set_any(cols[3], "String");
            //                            local_shared_realm->commit_transaction();
            //                        }
            //                        {
            //                            auto& group = local_shared_realm->read_group();
            //                            auto t = group.get_table("test");
            //                            auto cols = t->get_column_keys();
            //                            t->get_object(key).get<Int>(cols[0]);
            //                            t->get_object(key).get<Int>(cols[1]);
            //                            t->get_object(key).get<Bool>(cols[2]);
            //                            t->get_object(key).get<String>(cols[3]);
            //                        }
            //                    }
            //
            //                };
            //
            //                for (int i = 0; i < thread_count; ++i)
            //                    threads[i].start([&i,&writer] {
            //                        writer(ObjKey{i});
            //                    });
            //
            //                for (int i = 0; i < thread_count; ++i)
            //                    threads[i].join();
            //
            //                {
            //                    auto& group = shared_realm->read_group();
            //                    group.verify();
            //                    auto t = group.get_table("test");
            //                    auto col = t->get_column_keys()[0];
            //                    auto col1 = t->get_column_keys()[1];
            //                    auto col2 = t->get_column_keys()[2];
            //                    auto col3 = t->get_column_keys()[3];
            //
            //                    for (int i = 0; i < thread_count; ++i) {
            //                        t->get_object(ObjKey(i)).get<Int>(col);
            //                        t->get_object(ObjKey(i)).get<Int>(col1);
            //                        t->get_object(ObjKey(i)).get<Bool>(col2);
            //                        t->get_object(ObjKey(i)).get<String>(col3);
            //                    }
            //                }
            //            }
        }
    }
    catch (const EndOfFile&) {
    }
}


static void usage(const char* argv[])
{
    fprintf(stderr,
            "Usage: %s {FILE | --} [--log] [--name NAME] [--prefix PATH]\n"
            "Where FILE is a instruction file that will be replayed.\n"
            "Pass -- without argument to read filenames from stdin\n"
            "Pass --log to have code printed to stdout producing the same instructions.\n"
            "Pass --name NAME with distinct values when running on multiple threads,\n"
            "                 to make sure the test don't use the same Realm file\n"
            "Pass --prefix PATH to supply a path that should be prepended to all filenames\n"
            "                 read from stdin.\n",
            argv[0]);
    exit(1);
}

class Singleton {

public:
    std::string m_path;
    std::string m_contents;
    std::ofstream m_log;
    bool logging{false};

    static Singleton& init(int argc, const char* argv[])
    {
        static Singleton singleton{argc, argv};
        return singleton;
    }

private:
    Singleton(int argc, const char* argv[])
    {
        std::string name = "fuzz-test";
        std::string prefix = "./";
        bool file_names_from_stdin = false;

        realm::test_util::RealmPathInfo test_context{name};
        SHARED_GROUP_TEST_PATH(path);

        size_t file_arg = size_t(-1);
        for (size_t i = 1; i < size_t(argc); ++i) {
            std::string arg = argv[i];
            if (arg == "--log") {
                m_log.open("fuzz_log.txt");
                m_log << path.c_str() << std::endl;
                m_log << "Init realm " << std::endl;
                logging = true;
            }
            else if (arg == "--") {
                file_names_from_stdin = true;
            }
            else if (arg == "--prefix") {
                prefix = argv[++i];
            }
            else if (arg == "--name") {
                name = argv[++i];
            }
            else {
                file_arg = i;
            }
        }

        if (!file_names_from_stdin && file_arg == size_t(-1)) {
            usage(argv);
        }

        disable_sync_to_disk();

        std::ifstream in(argv[file_arg], std::ios::in | std::ios::binary);
        if (!in.is_open()) {
            std::cerr << "Could not open file for reading: " << argv[file_arg] << "\n";
            exit(1);
        }

        std::string contents((std::istreambuf_iterator<char>(in)), (std::istreambuf_iterator<char>()));

        m_path = path.c_str();
        m_contents.swap(contents);
    }
};

int run_fuzzy(int argc, const char* argv[])
{
    auto& instance = Singleton::init(argc, argv);
    if (instance.logging) {
        instance.m_log << "Going to fuzz this ... \n";
    }

    auto logger = instance.logging ? &instance.m_log : nullptr;
    parse_and_apply_instructions_object_store(instance.m_contents, instance.m_path, logger);
    return 0;
}


/*

 //    if (file_names_from_stdin) {
 //        std::string file_name;
 //
 //        std::cin >> file_name;
 //        while (std::cin) {
 //            std::ifstream in(prefix + file_name, std::ios::in | std::ios::binary);
 //            if (!in.is_open()) {
 //                std::cerr << "Could not open file for reading: " << (prefix + file_name) << std::endl;
 //            }
 //            else {
 //                std::cout << file_name << std::endl;
 //                realm::test_util::RealmPathInfo test_context{name};
 //                SHARED_GROUP_TEST_PATH(path);
 //
 //                std::string contents((std::istreambuf_iterator<char>(in)), (std::istreambuf_iterator<char>()));
 //                parse_and_apply_instructions(contents, path, log);
 //            }
 //
 //            std::cin >> file_name;
 //        }
 //    }
 //    else {

 //    auto path = "./fuzz_test.realm.lock";
 //        std::string contents((std::istreambuf_iterator<char>(in)), (std::istreambuf_iterator<char>()));
 //    log = &fs;

 //    }

     //int a = 1/0;

 */
