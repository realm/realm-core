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

#include <realm/group_shared.hpp>
#include <realm/lang_bind_helper.hpp>
#include <realm/history.hpp>

#include <ctime>
#include <cstdio>
#include <fstream>
#include <iostream>

#include "util/test_path.hpp"

using namespace realm;
using namespace realm::util;

// DISABLE until it can handle stable keys for Tables.
#if 0
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

std::string create_string(size_t length)
{
    REALM_ASSERT_3(length, <, 256);
    char buf[256] = {0};
    for (size_t i = 0; i < length; i++)
        buf[i] = 'a' + (rand() % 20);
    return std::string{buf, length};
}

enum INS {
    ADD_TABLE,
    REMOVE_TABLE,
    CREATE_OBJECT,
    INSERT_COLUMN,
    RENAME_COLUMN,
    ADD_COLUMN,
    REMOVE_COLUMN,
    SET,
    REMOVE_OBJECT,
    REMOVE_RECURSIVE,
    ADD_COLUMN_LINK,
    ADD_COLUMN_LINK_LIST,
    CLEAR_TABLE,
    INSERT_COLUMN_LINK,
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
<<<<<<< HEAD
=======
    SWAP_ROWS,
    MOVE_ROWS,
    SET_UNIQUE,
>>>>>>> v5.0.1
    IS_NULL,
    OPTIMIZE_TABLE,

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

std::string create_column_name(State& s, DataType t)
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
    const size_t length = get_next(s) % 20 + 1;
    return str + create_string(length);
}

std::string create_table_name(State& s)
{
    const size_t length = get_next(s) % (Group::max_table_name_length + 1);
    return create_string(length);
}

std::string get_current_time_stamp()
{
    std::time_t t = std::time(nullptr);
    const int str_size = 100;
    char str_buffer[str_size] = {0};
    std::strftime(str_buffer, str_size, "%c", std::localtime(&t));
    return str_buffer;
}

// randomly choose a table and column which meets the requirements for set_unique.
std::pair<size_t, size_t> get_target_for_set_unique(const Group& g, State &s) {
    std::vector<std::pair<size_t, size_t>> candidates;
    for (size_t table_ndx = 0; table_ndx < g.size(); ++table_ndx) {

        // We are looking for a non-empty table
        ConstTableRef t = g.get_table(table_ndx);
        if (t->size() == 0) {
            continue;
        }

        for (size_t col_ndx = 0; col_ndx < t->get_column_count(); ++col_ndx) {
            // The column we want to set a unique value on must a have a search index
            if (!t->has_search_index(col_ndx)) {
                continue;
            }
            DataType type = t->get_column_type(col_ndx);
            if (type == type_String || type == type_Int) {
                candidates.push_back({table_ndx, col_ndx});
            }
        }
    }

    if (candidates.empty()) {
        return {g.size(), -1}; // not found
    } else if (candidates.size() == 1) {
        return candidates[0]; // don't bother consuming another input
    }

    unsigned char r = get_next(s) % candidates.size();
    return candidates[r];
}

void parse_and_apply_instructions(std::string& in, const std::string& path, util::Optional<std::ostream&> log)
{
    const size_t add_empty_row_max = REALM_MAX_BPNODE_SIZE * REALM_MAX_BPNODE_SIZE + 1000;
    const size_t max_tables = REALM_MAX_BPNODE_SIZE * 10;

    // Max number of rows in a table. Overridden only by create_object() and only in the case where
    // max_rows is not exceeded *prior* to executing add_empty_row.
    const size_t max_rows = 100000;

    try {
        State s;
        s.str = in;
        s.pos = 0;

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

            *log << "SharedGroup sg_r(*hist_r, SharedGroupOptions(key));\n";
            *log << "SharedGroup sg_w(*hist_w, SharedGroupOptions(key));\n";

            *log << "Group& g = const_cast<Group&>(sg_w.begin_write());\n";
            *log << "Group& g_r = const_cast<Group&>(sg_r.begin_read());\n";
            *log << "std::vector<TableView> table_views;\n";
            *log << "std::vector<TableRef> subtable_refs;\n";

            *log << "\n";
        }

        std::unique_ptr<Replication> hist_r(make_in_realm_history(path));
        std::unique_ptr<Replication> hist_w(make_in_realm_history(path));

        SharedGroup sg_r(*hist_r, SharedGroupOptions(encryption_key));
        SharedGroup sg_w(*hist_w, SharedGroupOptions(encryption_key));
        Group& g = const_cast<Group&>(sg_w.begin_write());
        Group& g_r = const_cast<Group&>(sg_r.begin_read());
        std::vector<TableView> table_views;
        std::vector<TableRef> subtable_refs;
        std::vector<std::vector<Key>> keys;

        g.set_cascade_notification_handler([&keys](const Group::CascadeNotification& notification) {
            size_t sz = notification.rows.size();
            for (size_t i = 0; i < sz; i++) {
                std::vector<Key>& vec(keys[notification.rows[i].table_ndx]);
                auto it = find(vec.begin(), vec.end(), notification.rows[i].key);
                vec.erase(it);
            }
        });

        for (;;) {
            char instr = get_next(s) % COUNT;

            if (instr == ADD_TABLE && g.size() < max_tables) {
                std::string name = create_table_name(s);
                if (log) {
                    *log << "try { g.add_table(\"" << name << "\"); }"
                                                              " catch (const TableNameInUse&) { }\n";
                }
                try {
                    g.add_table(name);
                    keys.push_back({});
                    REALM_ASSERT(keys.size() == g.size());
                }
                catch (const TableNameInUse&) {
                }
            }
            else if (instr == REMOVE_TABLE && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                if (log) {
                    *log << "try { g.remove_table(" << table_ndx << "); }"
                                                                    " catch (const CrossTableLinkTarget&) { }\n";
                }
                try {
                    g.remove_table(table_ndx);
                    keys.erase(keys.begin() + table_ndx);
                    REALM_ASSERT(keys.size() == g.size());
                }
                catch (const CrossTableLinkTarget&) {
                    REALM_ASSERT(keys.size() == g.size());
                    if (log) {
                        *log << "// Exception\n";
                    }
                }
            }
            else if (instr == CLEAR_TABLE && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                if (log) {
                    *log << "g.get_table(" << table_ndx << ")->clear();\n";
                }
                g.get_table(table_ndx)->clear();
                keys[table_ndx].clear();
            }
<<<<<<< HEAD
            else if (instr == CREATE_OBJECT && g.size() > 0) {
=======
            else if (instr == INSERT_ROW && g.size() > 0) {
>>>>>>> v5.0.1
                size_t table_ndx = get_next(s) % g.size();
                if (g.get_table(table_ndx)->get_column_count() == 0) {
                    continue; // do not insert rows if there are no columns
                }
                size_t num_rows = get_next(s);
                if (g.get_table(table_ndx)->size() + num_rows < max_rows) {
                    typedef _impl::TableFriend tf;
                    if (g.get_table(table_ndx)->get_column_count() > 0 ||
                        tf::is_cross_table_link_target(*g.get_table(table_ndx))) {
                        if (log) {
                            *log << "{ std::vector<Key> keys; g.get_table(" << table_ndx << ")->create_objects("
                                 << num_rows % add_empty_row_max << ", keys); }\n";
                        }
                        g.get_table(table_ndx)->create_objects(num_rows % add_empty_row_max, keys[table_ndx]);
                    }
                }
            }
            else if (instr == ADD_COLUMN && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                DataType type = get_type(get_next(s));
                std::string name = create_column_name(s, type);
                // Mixed cannot be nullable. For other types, chose nullability randomly
                bool nullable = (get_next(s) % 2 == 0);
                if (log) {
                    *log << "g.get_table(" << table_ndx << ")->add_column(DataType(" << int(type) << "), \"" << name
                         << "\", " << (nullable ? "true" : "false") << ");\n";
                }
                g.get_table(table_ndx)->add_column(type, name, nullable);
            }
            else if (instr == INSERT_COLUMN && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                size_t col_ndx = get_next(s) % (g.get_table(table_ndx)->get_column_count() + 1);
                DataType type = get_type(get_next(s));
                std::string name = create_column_name(s, type);
                bool nullable = (get_next(s) % 2 == 0);
                if (log) {
                    *log << "g.get_table(" << table_ndx << ")->insert_column(" << col_ndx << ", DataType("
                         << int(type) << "), \"" << name << "\", " << (nullable ? "true" : "false") << ");\n";
                }
                g.get_table(table_ndx)->insert_column(col_ndx, type, name, nullable);
            }
            else if (instr == REMOVE_COLUMN && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->get_column_count() > 0) {
                    size_t col_ndx = get_next(s) % t->get_column_count();
                    if (log) {
                        *log << "g.get_table(" << table_ndx << ")->remove_column(" << col_ndx << ");\n";
                    }
                    t->remove_column(col_ndx);
                }
            }
            else if (instr == RENAME_COLUMN && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->get_column_count() > 0) {
                    size_t col_ndx = get_next(s) % t->get_column_count();
                    std::string name = create_column_name(s, t->get_column_type(col_ndx));
                    if (log) {
                        *log << "g.get_table(" << table_ndx << ")->rename_column("
                             << col_ndx << ", \"" << name << "\");\n";
                    }
                    t->rename_column(col_ndx, name);
                }
            }
            else if (instr == ADD_SEARCH_INDEX && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->get_column_count() > 0) {
                    size_t col_ndx = get_next(s) % t->get_column_count();
                    bool supports_search_index = _impl::TableFriend::get_column(*t, col_ndx).supports_search_index();

                    if (supports_search_index) {
                        if (log) {
                            *log << "g.get_table(" << table_ndx << ")->add_search_index(" << col_ndx << ");\n";
                        }
                        t->add_search_index(col_ndx);
                    }
                }
            }
            else if (instr == REMOVE_SEARCH_INDEX && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->get_column_count() > 0) {
                    size_t col_ndx = get_next(s) % t->get_column_count();
                    // We don't need to check if the column is of a type that is indexable or if it has index on or
                    // off
                    // because Realm will just do a no-op at worst (no exception or assert).
                    if (log) {
                        *log << "g.get_table(" << table_ndx << ")->remove_search_index(" << col_ndx << ");\n";
                    }
                    t->remove_search_index(col_ndx);
                }
            }
            else if (instr == ADD_COLUMN_LINK && g.size() >= 1) {
                size_t table_ndx_1 = get_next(s) % g.size();
                size_t table_ndx_2 = get_next(s) % g.size();
                TableRef t1 = g.get_table(table_ndx_1);
                TableRef t2 = g.get_table(table_ndx_2);
                std::string name = create_column_name(s, type_Link);
                if (log) {
                    *log << "g.get_table(" << table_ndx_1 << ")->add_column_link(type_Link, \"" << name
                         << "\", *g.get_table(" << table_ndx_2 << "));\n";
                }
                t1->add_column_link(type_Link, name, *t2);
            }
            else if (instr == INSERT_COLUMN_LINK && g.size() >= 1) {
                size_t table_ndx_1 = get_next(s) % g.size();
                size_t table_ndx_2 = get_next(s) % g.size();
                size_t col_ndx = get_next(s) % (g.get_table(table_ndx_1)->get_column_count() + 1);
                TableRef t1 = g.get_table(table_ndx_1);
                TableRef t2 = g.get_table(table_ndx_2);
                std::string name = create_column_name(s, type_Link);
                if (log) {
                    *log << "g.get_table(" << table_ndx_1 << ")->insert_column_link(" << col_ndx << ", type_Link, \""
                         << name << "\", *g.get_table(" << table_ndx_2 << "));\n";
                }
                t1->insert_column_link(col_ndx, type_Link, name, *t2);
            }
            else if (instr == ADD_COLUMN_LINK_LIST && g.size() >= 2) {
                size_t table_ndx_1 = get_next(s) % g.size();
                size_t table_ndx_2 = get_next(s) % g.size();
                TableRef t1 = g.get_table(table_ndx_1);
                TableRef t2 = g.get_table(table_ndx_2);
                std::string name = create_column_name(s, type_LinkList);
                if (log) {
                    *log << "g.get_table(" << table_ndx_1 << ")->add_column_link(type_LinkList, \"" << name
                         << "\", *g.get_table(" << table_ndx_2 << "));\n";
                }
                t1->add_column_link(type_LinkList, name, *t2);
            }
            else if (instr == SET && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->get_column_count() > 0 && t->size() > 0) {
                    size_t col_ndx = get_next(s) % t->get_column_count();
                    Key key = keys[table_ndx][get_next(s) % t->size()];
                    DataType type = t->get_column_type(col_ndx);
                    Obj obj = t->get_object(key);
                    if (log) {
                        *log << "{\nObj obj = g.get_table(" << table_ndx << ")->get_object(" << key << ");\n";
                    }

                    // With equal probability, either set to null or to a value
                    if (get_next(s) % 2 == 0 && t->is_nullable(col_ndx)) {
                        if (type == type_Link) {
                            if (log) {
                                *log << "obj.set(" << col_ndx << ", null_key);\n";
                            }
                            obj.set(col_ndx, null_key);
                        }
                        else {
                            if (log) {
                                *log << "obj.set_null(" << col_ndx << ");\n";
                            }
                            obj.set_null(col_ndx);
                        }
                    }
                    else {
                        if (type == type_String) {
                            std::string str = create_string(get_next(s));
                            if (log) {
                                *log << "obj.set(" << col_ndx << ", \"" << str << "\");\n";
                            }
                            obj.set(col_ndx, StringData(str));
                        }
                        else if (type == type_Binary) {
                            /* FIXME
                            bool insert_big_blob = get_next(s) % 2 == 0;
                            if (insert_big_blob) {
                                if (log) {
                                    *log << "{\n\t";
                                }
                                std::string blob = construct_binary_payload(s, log);
                                if (log) {
                                    *log << "\tg.get_table(" << table_ndx << ")->set_binary_big(" << col_ndx << ", "
                                         << row_ndx << ", BinaryData(blob));\n}\n";
                                }
                                t->set_binary_big(col_ndx, row_ndx, BinaryData(blob));
                            }
                            else {
                            }
                            */
                            std::string str = create_string(get_next(s));
                            if (log) {
                                *log << "obj.set<Binary>(" << col_ndx << ", BinaryData{\"" << str << "\", "
                                     << str.size() << "});\n";
                            }
                            obj.set<Binary>(col_ndx, BinaryData(str));
                        }
                        else if (type == type_Int) {
                            bool add_int = get_next(s) % 2 == 0;
                            int64_t value = get_int64(s);
                            if (add_int) {
                                if (log) {
                                    *log << "try { obj.add_int(" << col_ndx << ", " << value
                                         << "); } catch (const LogicError& le) { CHECK(le.kind() == "
                                            "LogicError::illegal_combination); }\n";
                                }
                                try {
                                    obj.add_int(col_ndx, value);
                                }
                                catch (const LogicError& le) {
                                    if (le.kind() != LogicError::illegal_combination) {
                                        throw;
                                    }
                                }
                            }
                            else {
                                if (log) {
                                    *log << "obj.set<Int>(" << col_ndx << ", " << value << ");\n";
                                }
                                obj.set<Int>(col_ndx, value);
                            }

                        }
                        else if (type == type_Bool) {
                            bool value = get_next(s) % 2 == 0;
                            if (log) {
                                *log << "obj.set<Bool>(" << col_ndx << ", " << (value ? "true" : "false") << ");\n";
                            }
                            obj.set<Bool>(col_ndx, value);
                        }
                        else if (type == type_Float) {
                            float value = get_next(s);
                            if (log) {
                                *log << "obj.set<Float>(" << col_ndx << ", " << value << ");\n";
                            }
                            obj.set<Float>(col_ndx, value);
                        }
                        else if (type == type_Double) {
                            double value = get_next(s);
                            if (log) {
                                *log << "obj.set<double>(" << col_ndx << ", " << value << ");\n";
                            }
                            obj.set<double>(col_ndx, value);
                        }
                        else if (type == type_Link) {
                            TableRef target = t->get_link_target(col_ndx);
                            if (target->size() > 0) {
                                Key target_key = keys[target->get_index_in_group()][get_next(s) % target->size()];
                                if (log) {
                                    *log << "obj.set<Key>(" << col_ndx << ", " << target_key << ");\n";
                                }
                                obj.set<Key>(col_ndx, target_key);
                            }
                        }
                        else if (type == type_LinkList) {
                            TableRef target = t->get_link_target(col_ndx);
                            if (target->size() > 0) {
                                LinkList links = obj.get_linklist(col_ndx);
                                Key target_key = keys[target->get_index_in_group()][get_next(s) % target->size()];
                                // either add or set, 50/50 probability
                                if (links.size() > 0 && get_next(s) > 128) {
                                    size_t linklist_row = get_next(s) % links.size();
                                    if (log) {
                                        *log << "obj.get_linklist(" << col_ndx << ")->set(" << linklist_row << ", "
                                             << target_key << ");\n";
                                    }
                                    links.set(linklist_row, target_key);
                                }
                                else {
                                    if (log) {
                                        *log << "obj.get_linklist(" << col_ndx << ")->add(" << target_key << ");\n";
                                    }
                                    links.add(target_key);
                                }
                            }
                        }
                        else if (type == type_Timestamp) {
                            std::pair<int64_t, int32_t> values = get_timestamp_values(s);
                            Timestamp value{values.first, values.second};
                            if (log) {
                                *log << "obj.set(" << col_ndx << ", " << value << ");\n";
                            }
                            obj.set(col_ndx, value);
                        }
                    }
                    if (log) {
                        *log << "}\n";
                    }
                }
            }
            else if (instr == REMOVE_OBJECT && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->size() > 0) {
                    Key key = keys[table_ndx][get_next(s) % t->size()];
                    if (log) {
                        *log << "g.get_table(" << table_ndx << ")->remove_object(" << key << ");\n";
                    }
                    t->remove_object(key);
                }
            }
            else if (instr == REMOVE_RECURSIVE && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->size() > 0) {
                    Key key = keys[table_ndx][get_next(s) % t->size()];
                    if (log) {
                        *log << "g.get_table(" << table_ndx << ")->remove_object_recursive(" << key << ");\n";
                    }
                    t->remove_object_recursive(key);
                }
            }
            else if (instr == OPTIMIZE_TABLE && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                // Force creation of a string enum column
                if (log) {
                    *log << "g.get_table(" << table_ndx << ")->optimize(true);\n";
                }
                g.get_table(table_ndx)->optimize(true);
            }
            else if (instr == COMMIT) {
                if (log) {
                    *log << "LangBindHelper::commit_and_continue_as_read(sg_w);\n";
                }
                LangBindHelper::commit_and_continue_as_read(sg_w);
                REALM_DO_IF_VERIFY(log, g.verify());
                if (log) {
                    *log << "LangBindHelper::promote_to_write(sg_w);\n";
                }
                LangBindHelper::promote_to_write(sg_w);
                REALM_DO_IF_VERIFY(log, g.verify());
            }
            else if (instr == ROLLBACK) {
                if (log) {
                    *log << "LangBindHelper::rollback_and_continue_as_read(sg_w);\n";
                }
                LangBindHelper::rollback_and_continue_as_read(sg_w);
                REALM_DO_IF_VERIFY(log, g.verify());
                if (log) {
                    *log << "LangBindHelper::promote_to_write(sg_w);\n";
                }
                LangBindHelper::promote_to_write(sg_w);
                REALM_DO_IF_VERIFY(log, g.verify());
            }
            else if (instr == ADVANCE) {
                if (log) {
                    *log << "LangBindHelper::advance_read(sg_r);\n";
                }
                LangBindHelper::advance_read(sg_r);
                REALM_DO_IF_VERIFY(log, g_r.verify());
            }
            else if (instr == CLOSE_AND_REOPEN) {
                bool read_group = get_next(s) % 2 == 0;
                if (read_group) {
                    if (log) {
                        *log << "sg_r.close();\n";
                    }
                    sg_r.close();
                    if (log) {
                        *log << "sg_r.open(path, true, SharedGroupOptions(key));\n";
                    }
                    sg_r.open(path, true, SharedGroupOptions(encryption_key));
                    if (log) {
                        *log << "sg_r.begin_read();\n";
                    }
                    sg_r.begin_read();
                    REALM_DO_IF_VERIFY(log, g_r.verify());
                }
                else {
                    if (log) {
                        *log << "sg_w.close();\n";
                    }
                    sg_w.close();
                    if (log) {
                        *log << "sg_w.open(path, true, SharedGroupOptions(key));\n";
                    }
                    sg_w.open(path, true, SharedGroupOptions(encryption_key));
                    if (log) {
                        *log << "sg_w.begin_write();\n";
                    }
                    sg_w.begin_write();
                    REALM_DO_IF_VERIFY(log, g.verify());
                }
            }
            else if (instr == GET_ALL_COLUMN_NAMES && g.size() > 0) {
                // try to fuzz find this: https://github.com/realm/realm-core/issues/1769
                for (size_t table_ndx = 0; table_ndx < g.size(); ++table_ndx) {
                    TableRef t = g.get_table(table_ndx);
                    for (size_t col_ndx = 0; col_ndx < t->get_column_count(); ++col_ndx) {
                        StringData col_name = t->get_column_name(col_ndx);
                        static_cast<void>(col_name);
                    }
                }
            }
            else if (instr == CREATE_TABLE_VIEW && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (log) {
                    *log << "table_views.push_back(g.get_table(" << table_ndx << ")->where().find_all());\n";
                }
                TableView tv = t->where().find_all();
                table_views.push_back(tv);
            }
            else if (instr == COMPACT) {
                if (log) {
                    *log << "sg_r.close();\n";
                }
                sg_r.close();
                if (log) {
                    *log << "sg_w.commit();\n";
                }
                sg_w.commit();

                if (log) {
                    *log << "REALM_ASSERT_RELEASE(sg_w.compact());\n";
                }
                REALM_ASSERT_RELEASE(sg_w.compact());

                if (log) {
                    *log << "sg_w.begin_write();\n";
                }
                sg_w.begin_write();
                if (log) {
                    *log << "sg_r.open(path, true, SharedGroupOptions(key));\n";
                }
                sg_r.open(path, true, SharedGroupOptions(encryption_key));
                if (log) {
                    *log << "sg_r.begin_read();\n";
                }
                sg_r.begin_read();
                REALM_DO_IF_VERIFY(log, g_r.verify());
            }
            else if (instr == IS_NULL && g_r.size() > 0) {
                size_t table_ndx = get_next(s) % g_r.size();
                TableRef t = g_r.get_table(table_ndx);
                if (t->get_column_count() > 0 && t->size() > 0) {
                    size_t col_ndx = get_int32(s) % t->get_column_count();
                    Key key = keys[table_ndx][get_int32(s) % t->size()];
                    if (log) {
                        *log << "g_r.get_table(" << table_ndx << ")->get_object(Key(" << key.value << ")).is_null("
                             << col_ndx << ");\n";
                    }
                    bool res = t->get_object(key).is_null(col_ndx);
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
