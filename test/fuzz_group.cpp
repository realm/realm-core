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

#include <realm.hpp>
#include <realm/history.hpp>
#include <realm/index_string.hpp>

#include <ctime>
#include <cstdio>
#include <fstream>
#include <iostream>

#include "util/test_path.hpp"

using namespace realm;
using namespace realm::util;

// DISABLE until it can handle stable keys for Tables.
#define TEST_FUZZ
#ifdef TEST_FUZZ
// Determines whether or not to run the shared group verify function
// after each transaction. This will find errors earlier but is expensive.
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

struct EndOfFile {
};

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

std::pair<int64_t, int32_t> get_timestamp_values(State& s) {
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

// returns random binary blob data in a string, logs to a variable called "blob" if logging is enabled
std::string construct_binary_payload(State& s, util::Optional<std::ostream&> log)
{
    size_t rand_char = get_next(s);
    size_t blob_size = static_cast<uint64_t>(get_int64(s)) % (ArrayBlob::max_binary_size + 1);
    std::string buffer(blob_size, static_cast<unsigned char>(rand_char));
    if (log) {
        *log << "std::string blob(" << blob_size << ", static_cast<unsigned char>(" << rand_char << "));\n";
    }
    return buffer;
}

namespace {
int table_index = 0;
int column_index = 0;
}

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
        case type_Link:
            str = "link_";
            break;
        case type_LinkList:
            str = "link_list_";
            break;
        case type_OldDateTime:
        case type_OldTable:
        case type_OldMixed:
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

void parse_and_apply_instructions(std::string& in, const std::string& path, util::Optional<std::ostream&> log)
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
    const char* encryption_key = use_encryption ? get_encryption_key() : nullptr;

    if (log) {
        *log << "// Test case generated in " REALM_VER_CHUNK " on " << get_current_time_stamp() << ".\n";
        *log << "// REALM_MAX_BPNODE_SIZE is " << REALM_MAX_BPNODE_SIZE << "\n";
        *log << "// ----------------------------------------------------------------------\n";
        std::string printable_key;
        if (encryption_key == nullptr) {
            printable_key = "nullptr";
        }
        else {
            printable_key = std::string("\"") + encryption_key + "\"";
        }

        *log << "SHARED_GROUP_TEST_PATH(path);\n";

        *log << "const char* key = " << printable_key << ";\n";
        *log << "std::unique_ptr<Replication> hist_r(make_in_realm_history(path));\n";
        *log << "std::unique_ptr<Replication> hist_w(make_in_realm_history(path));\n";

        *log << "DB db_w(*hist_w, DBOptions(key));\n";
        *log << "DB db_r(*hist_r, DBOptions(key));\n";
        *log << "auto wt = db_w.start_write();\n";
        *log << "auto rt = db_r.start_read();\n";
        *log << "std::vector<TableView> table_views;\n";
        *log << "std::vector<TableRef> subtable_refs;\n";

        *log << "\n";
    }

    std::unique_ptr<Replication> hist_r(make_in_realm_history(path));
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));

    DB db_r(*hist_r, DBOptions(encryption_key));
    DB db_w(*hist_w, DBOptions(encryption_key));
    auto wt = db_w.start_write();
    auto rt = db_r.start_read();
    std::vector<TableView> table_views;
    std::vector<TableRef> subtable_refs;

    try {

        for (;;) {
            char instr = get_next(s) % COUNT;

            if (instr == ADD_TABLE && wt->size() < max_tables) {
                std::string name = create_table_name();
                if (log) {
                    *log << "wt->add_table(\"" << name << "\");\n";
                }
                wt->add_table(name);
            }
            else if (instr == REMOVE_TABLE && wt->size() > 0) {
                TableKey table_key = wt->get_table_keys()[get_next(s) % wt->size()];
                if (log) {
                    *log << "try { wt->remove_table(" << table_key << "); }"
                                                                      " catch (const CrossTableLinkTarget&) { }\n";
                }
                try {
                    wt->remove_table(table_key);
                }
                catch (const CrossTableLinkTarget&) {
                    if (log) {
                        *log << "// Exception\n";
                    }
                }
            }
            else if (instr == CLEAR_TABLE && wt->size() > 0) {
                TableKey table_key = wt->get_table_keys()[get_next(s) % wt->size()];
                if (log) {
                    *log << "wt->get_table(" << table_key << ")->clear();\n";
                }
                wt->get_table(table_key)->clear();
            }
            else if (instr == CREATE_OBJECT && wt->size() > 0) {
                TableKey table_key = wt->get_table_keys()[get_next(s) % wt->size()];
                if (wt->get_table(table_key)->get_column_count() == 0) {
                    continue; // do not insert rows if there are no columns
                }
                size_t num_rows = get_next(s);
                if (wt->get_table(table_key)->size() + num_rows < max_rows) {
                    typedef _impl::TableFriend tf;
                    if (wt->get_table(table_key)->get_column_count() > 0 ||
                        tf::is_cross_table_link_target(*wt->get_table(table_key))) {
                        if (log) {
                            *log << "{ std::vector<ObjKey> keys; wt->get_table(" << table_key << ")->create_objects("
                                 << num_rows % add_empty_row_max << ", keys); }\n";
                        }
                        std::vector<ObjKey> keys;
                        wt->get_table(table_key)->create_objects(num_rows % add_empty_row_max, keys);
                    }
                }
            }
            else if (instr == ADD_COLUMN && wt->size() > 0) {
                TableKey table_key = wt->get_table_keys()[get_next(s) % wt->size()];
                DataType type = get_type(get_next(s));
                std::string name = create_column_name(type);
                // Mixed cannot be nullable. For other types, chose nullability randomly
                bool nullable = (get_next(s) % 2 == 0);
                if (log) {
                    *log << "wt->get_table(" << table_key << ")->add_column(DataType(" << int(type) << "), \"" << name
                         << "\", " << (nullable ? "true" : "false") << ");\n";
                }
                wt->get_table(table_key)->add_column(type, name, nullable);
            }
            else if (instr == REMOVE_COLUMN && wt->size() > 0) {
                TableKey table_key = wt->get_table_keys()[get_next(s) % wt->size()];
                TableRef t = wt->get_table(table_key);
                auto all_col_keys = t->get_col_keys();
                if (!all_col_keys.empty()) {
                    ColKey col = all_col_keys[get_next(s) % all_col_keys.size()];
                    if (log) {
                        *log << "wt->get_table(" << table_key << ")->remove_column(" << col << ");\n";
                    }
                    t->remove_column(col);
                }
            }
            else if (instr == RENAME_COLUMN && wt->size() > 0) {
                TableKey table_key = wt->get_table_keys()[get_next(s) % wt->size()];
                TableRef t = wt->get_table(table_key);
                auto all_col_keys = t->get_col_keys();
                if (!all_col_keys.empty()) {
                    ColKey col = all_col_keys[get_next(s) % all_col_keys.size()];
                    std::string name = create_column_name(t->get_column_type(col));
                    if (log) {
                        *log << "wt->get_table(" << table_key << ")->rename_column(" << col << ", \"" << name
                             << "\");\n";
                    }
                    t->rename_column(col, name);
                }
            }
            else if (instr == ADD_SEARCH_INDEX && wt->size() > 0) {
                TableKey table_key = wt->get_table_keys()[get_next(s) % wt->size()];
                TableRef t = wt->get_table(table_key);
                auto all_col_keys = t->get_col_keys();
                if (!all_col_keys.empty()) {
                    ColKey col = all_col_keys[get_next(s) % all_col_keys.size()];
                    bool supports_search_index = StringIndex::type_supported(t->get_column_type(col));

                    if (supports_search_index) {
                        if (log) {
                            *log << "wt->get_table(" << table_key << ")->add_search_index(" << col << ");\n";
                        }
                        t->add_search_index(col);
                    }
                }
            }
            else if (instr == REMOVE_SEARCH_INDEX && wt->size() > 0) {
                TableKey table_key = wt->get_table_keys()[get_next(s) % wt->size()];
                TableRef t = wt->get_table(table_key);
                auto all_col_keys = t->get_col_keys();
                if (!all_col_keys.empty()) {
                    ColKey col = all_col_keys[get_next(s) % all_col_keys.size()];
                    // We don't need to check if the column is of a type that is indexable or if it has index on or
                    // off
                    // because Realm will just do a no-op at worst (no exception or assert).
                    if (log) {
                        *log << "wt->get_table(" << table_key << ")->remove_search_index(" << col << ");\n";
                    }
                    t->remove_search_index(col);
                }
            }
            else if (instr == ADD_COLUMN_LINK && wt->size() >= 1) {
                TableKey table_key_1 = wt->get_table_keys()[get_next(s) % wt->size()];
                TableKey table_key_2 = wt->get_table_keys()[get_next(s) % wt->size()];
                TableRef t1 = wt->get_table(table_key_1);
                TableRef t2 = wt->get_table(table_key_2);
                std::string name = create_column_name(type_Link);
                if (log) {
                    *log << "wt->get_table(" << table_key_1 << ")->add_column_link(type_Link, \"" << name
                         << "\", *wt->get_table(" << table_key_2 << "));\n";
                }
                t1->add_column_link(type_Link, name, *t2);
            }
            else if (instr == ADD_COLUMN_LINK_LIST && wt->size() >= 2) {
                TableKey table_key_1 = wt->get_table_keys()[get_next(s) % wt->size()];
                TableKey table_key_2 = wt->get_table_keys()[get_next(s) % wt->size()];
                TableRef t1 = wt->get_table(table_key_1);
                TableRef t2 = wt->get_table(table_key_2);
                std::string name = create_column_name(type_LinkList);
                if (log) {
                    *log << "wt->get_table(" << table_key_1 << ")->add_column_link(type_LinkList, \"" << name
                         << "\", *wt->get_table(" << table_key_2 << "));\n";
                }
                t1->add_column_link(type_LinkList, name, *t2);
            }
            else if (instr == SET && wt->size() > 0) {
                TableKey table_key = wt->get_table_keys()[get_next(s) % wt->size()];
                TableRef t = wt->get_table(table_key);
                auto all_col_keys = t->get_col_keys();
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
                                *log << "obj.set<Binary>(" << col << ", BinaryData{\"" << str << "\", " << str.size()
                                     << "});\n";
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
                                LinkList links = obj.get_linklist(col);
                                ObjKey target_key = target->get_object(get_next(s) % target->size()).get_key();
                                // either add or set, 50/50 probability
                                if (links.size() > 0 && get_next(s) > 128) {
                                    size_t linklist_row = get_next(s) % links.size();
                                    if (log) {
                                        *log << "obj.get_linklist(" << col << ")->set(" << linklist_row << ", "
                                             << target_key << ");\n";
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
            else if (instr == REMOVE_OBJECT && wt->size() > 0) {
                TableKey table_key = wt->get_table_keys()[get_next(s) % wt->size()];
                TableRef t = wt->get_table(table_key);
                if (t->size() > 0) {
                    ObjKey key = t->get_object(get_next(s) % t->size()).get_key();
                    if (log) {
                        *log << "wt->get_table(" << table_key << ")->remove_object(" << key << ");\n";
                    }
                    t->remove_object(key);
                }
            }
            else if (instr == REMOVE_RECURSIVE && wt->size() > 0) {
                TableKey table_key = wt->get_table_keys()[get_next(s) % wt->size()];
                TableRef t = wt->get_table(table_key);
                if (t->size() > 0) {
                    ObjKey key = t->get_object(get_next(s) % t->size()).get_key();
                    if (log) {
                        *log << "wt->get_table(" << table_key << ")->remove_object_recursive(" << key << ");\n";
                    }
                    t->remove_object_recursive(key);
                }
            }
            else if (instr == ENUMERATE_COLUMN && wt->size() > 0) {
                TableKey table_key = wt->get_table_keys()[get_next(s) % wt->size()];
                TableRef t = wt->get_table(table_key);
                auto all_col_keys = t->get_col_keys();
                if (!all_col_keys.empty()) {
                    size_t ndx = get_next(s) % all_col_keys.size();
                    ColKey col = all_col_keys[ndx];
                    if (log) {
                        *log << "wt->get_table(" << table_key << ")->enumerate_string_column(" << col << ");\n";
                    }
                    wt->get_table(table_key)->enumerate_string_column(col);
                }
            }
            else if (instr == COMMIT) {
                if (log) {
                    *log << "wt->commit_and_continue_as_read();\n";
                }
                wt->commit_and_continue_as_read();
                REALM_DO_IF_VERIFY(log, wt->verify());
                if (log) {
                    *log << "wt->promote_to_write();\n";
                }
                wt->promote_to_write();
                REALM_DO_IF_VERIFY(log, wt->verify());
            }
            else if (instr == ROLLBACK) {
                if (log) {
                    *log << "wt->rollback_and_continue_as_read();\n";
                }
                wt->rollback_and_continue_as_read();
                REALM_DO_IF_VERIFY(log, wt->verify());
                if (log) {
                    *log << "wt->promote_to_write();\n";
                }
                wt->promote_to_write();
                REALM_DO_IF_VERIFY(log, wt->verify());
            }
            else if (instr == ADVANCE) {
                if (log) {
                    *log << "rt->advance_read();\n";
                }
                rt->advance_read();
                REALM_DO_IF_VERIFY(log, rt->verify());
            }
            else if (instr == CLOSE_AND_REOPEN) {
                bool read_group = get_next(s) % 2 == 0;
                if (read_group) {
                    if (log) {
                        *log << "db_r.close();\n";
                    }
                    db_r.close();
                    if (log) {
                        *log << "db_r.open(path, true, DBOptions(key));\n";
                    }
                    db_r.open(path, true, DBOptions(encryption_key));
                    if (log) {
                        *log << "rt = nullptr;\n";
                        *log << "rt = db_r.start_read();\n";
                    }
                    rt = nullptr;
                    rt = db_r.start_read();
                    REALM_DO_IF_VERIFY(log, rt->verify());
                }
                else {
                    if (log) {
                        *log << "wt = nullptr;\n";
                        *log << "db_w.close();\n";
                    }
                    wt = nullptr;
                    db_w.close();
                    if (log) {
                        *log << "db_w.open(path, true, DBOptions(key));\n";
                    }
                    db_w.open(path, true, DBOptions(encryption_key));
                    if (log) {
                        *log << "wt = db_w.start_write();\n";
                    }
                    wt = db_w.start_write();
                    REALM_DO_IF_VERIFY(log, wt->verify());
                }
            }
            else if (instr == GET_ALL_COLUMN_NAMES && wt->size() > 0) {
                // try to fuzz find this: https://github.com/realm/realm-core/issues/1769
                for (auto table_key : wt->get_table_keys()) {
                    TableRef t = wt->get_table(table_key);
                    auto all_col_keys = t->get_col_keys();
                    for (auto col : all_col_keys) {
                        StringData col_name = t->get_column_name(col);
                        static_cast<void>(col_name);
                    }
                }
            }
            else if (instr == CREATE_TABLE_VIEW && wt->size() > 0) {
                TableKey table_key = wt->get_table_keys()[get_next(s) % wt->size()];
                TableRef t = wt->get_table(table_key);
                if (log) {
                    *log << "table_views.push_back(wt->get_table(" << table_key << ")->where().find_all());\n";
                }
                TableView tv = t->where().find_all();
                table_views.push_back(tv);
            }
            else if (instr == COMPACT) {
                /*
                if (log) {
                    *log << "db_r.close();\n";
                }
                db_r.close();
                if (log) {
                    *log << "wt->commit();\n";
                }
                wt->commit();

                if (log) {
                    *log << "REALM_ASSERT_RELEASE(db_w.compact());\n";
                }
                REALM_ASSERT_RELEASE(db_w.compact());

                if (log) {
                    *log << "wt = db_w.start_write();\n";
                }
                wt = db_w.start_write();
                if (log) {
                    *log << "db_r.open(path, true, DBOptions(key));\n";
                }
                db_r.open(path, true, DBOptions(encryption_key));
                if (log) {
                    *log << "rt = db_r.start_read();\n";
                }
                rt = db_r.start_read();
                REALM_DO_IF_VERIFY(log, rt->verify());
                */
            }
            else if (instr == IS_NULL && rt->size() > 0) {
                TableKey table_key = rt->get_table_keys()[get_next(s) % rt->size()];
                TableRef t = rt->get_table(table_key);
                if (t->get_column_count() > 0 && t->size() > 0) {
                    auto all_col_keys = t->get_col_keys();
                    size_t ndx = get_next(s) % all_col_keys.size();
                    ColKey col = all_col_keys[ndx];
                    ObjKey key = t->get_object(get_int32(s) % t->size()).get_key();
                    if (log) {
                        *log << "g_r.get_table(" << table_key << ")->get_object(" << key << ").is_null(" << col
                             << ");\n";
                    }
                    bool res = t->get_object(key).is_null(col);
                    static_cast<void>(res);
                }
            }
        }
    }
    catch (const EndOfFile&) {
    }
}


void usage(const char* argv[])
{
    fprintf(stderr, "Usage: %s {FILE | --} [--log] [--name NAME] [--prefix PATH]\n"
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


int run_fuzzy(int argc, const char* argv[])
{
    util::Optional<std::ostream&> log;
    std::string name = "fuzz-test";
    std::string prefix = "./";
    bool file_names_from_stdin = false;

    size_t file_arg = size_t(-1);
    for (size_t i = 1; i < size_t(argc); ++i) {
        std::string arg = argv[i];
        if (arg == "--log") {
            log = util::some<std::ostream&>(std::cout);
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

    if (file_names_from_stdin) {
        std::string file_name;

        std::cin >> file_name;
        while (std::cin) {
            std::ifstream in(prefix + file_name, std::ios::in | std::ios::binary);
            if (!in.is_open()) {
                std::cerr << "Could not open file for reading: " << (prefix + file_name) << std::endl;
            }
            else {
                std::cout << file_name << std::endl;
                realm::test_util::RealmPathInfo test_context{name};
                SHARED_GROUP_TEST_PATH(path);

                std::string contents((std::istreambuf_iterator<char>(in)), (std::istreambuf_iterator<char>()));
                parse_and_apply_instructions(contents, path, log);
            }

            std::cin >> file_name;
        }
    }
    else {
        std::ifstream in(argv[file_arg], std::ios::in | std::ios::binary);
        if (!in.is_open()) {
            std::cerr << "Could not open file for reading: " << argv[file_arg] << "\n";
            exit(1);
        }

        realm::test_util::RealmPathInfo test_context{name};
        SHARED_GROUP_TEST_PATH(path);

        std::string contents((std::istreambuf_iterator<char>(in)), (std::istreambuf_iterator<char>()));
        parse_and_apply_instructions(contents, path, log);
    }

    return 0;
}
#else
int run_fuzzy(int, const char* [])
{
    return 0;
}
#endif
