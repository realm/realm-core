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
#include <realm/link_view.hpp>
#include <realm/lang_bind_helper.hpp>
#include <realm/history.hpp>

#include <ctime>
#include <cstdio>
#include <fstream>
#include <iostream>

#include "util/test_path.hpp"

using namespace realm;
using namespace realm::util;

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
    INSERT_TABLE,
    REMOVE_TABLE,
    INSERT_ROW,
    ADD_EMPTY_ROW,
    INSERT_COLUMN,
    RENAME_COLUMN,
    ADD_COLUMN,
    REMOVE_COLUMN,
    SET,
    REMOVE_ROW,
    MERGE_ROWS,
    ADD_COLUMN_LINK,
    ADD_COLUMN_LINK_LIST,
    CLEAR_TABLE,
    MOVE_TABLE,
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
    CREATE_SUBTABLE_VIEW,
    COMPACT,
    SWAP_ROWS,
    MOVE_COLUMN,
    SET_UNIQUE,
    IS_NULL,

    COUNT
};

DataType get_type(unsigned char c)
{
    DataType types[] = {type_Int,    type_Bool,  type_Float, type_Double,   type_String,
                        type_Binary, type_Table, type_Mixed, type_Timestamp};

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

Mixed construct_mixed(State& s, util::Optional<std::ostream&> log, std::string& buffer)
{
    // Mixed type has 8 constructors supporting different types.
    unsigned char type = get_next(s) % 8;

    switch (type) {
        default:
        case 0: {
            bool b = get_next(s) % 2;
            if (log) {
                *log << "Mixed mixed(" << (b ? "true" : "false") << ");\n";
            }
            return Mixed(b);
        }
        case 1: {
            int64_t value = get_int64(s);
            if (log) {
                *log << "Mixed mixed((int64_t)(" << value << "));\n";
            }
            return Mixed(value);
        }
        case 2: {
            float value = get_next(s);
            if (log) {
                *log << "Mixed mixed((float)(" << value << "));\n";
            }
            return Mixed(value);
        }
        case 3: {
            double value = get_next(s);
            if (log) {
                *log << "Mixed mixed((double)(" << value << "));\n";
            }
            return Mixed(value);
        }
        case 4: {
            buffer = create_string(get_next(s));
            if (log) {
                *log << "Mixed mixed(StringData(\"" << buffer << "\"));\n";
            }
            return Mixed(StringData(buffer));
        }
        case 5: {
            size_t rand_char = get_next(s);
            size_t blob_size = get_int64(s) % ArrayBlob::max_binary_size;
            buffer = std::string(blob_size, static_cast<unsigned char>(rand_char));
            if (log) {
                *log << "std::string blob(" << blob_size << ", static_cast<unsigned char>(" << rand_char << "));\n"
                     << "Mixed mixed(BinaryData(blob));\n";
            }
            return Mixed(BinaryData(buffer));
        }
        case 6: {
            int64_t time = get_int64(s);
            if (log) {
                *log << "Mixed mixed(OldDateTime(" << time << "));\n";
            }
            return Mixed(OldDateTime(time));
        }
        case 7: {
            std::pair<int64_t, int32_t> values = get_timestamp_values(s);
            if (log) {
                *log << "Mixed mixed(Timestamp{" << values.first << ", " << values.second << "});\n";
            }
            return Mixed(Timestamp{values.first, values.second});
        }
    }
}

std::string create_column_name(State& s)
{
    const size_t length = get_next(s) % (Descriptor::max_column_name_length + 1);
    return create_string(length);
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

    // Max number of rows in a table. Overridden only by add_empty_row_max() and only in the case where
    // max_rows is not exceeded *prior* to executing add_empty_row.
    const size_t max_rows = 100000;

    try {
        State s;
        s.str = in;
        s.pos = 0;

        const bool use_encryption = get_next(s) % 2 == 0;
        const char* key = use_encryption ? get_encryption_key() : nullptr;

        if (log) {
            *log << "// Test case generated in " REALM_VER_CHUNK " on " << get_current_time_stamp() << ".\n";
            *log << "// REALM_MAX_BPNODE_SIZE is " << REALM_MAX_BPNODE_SIZE << "\n";
            *log << "// ----------------------------------------------------------------------\n";
            std::string printable_key;
            if (key == nullptr) {
                printable_key = "nullptr";
            }
            else {
                printable_key = std::string("\"") + key + "\"";
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

        SharedGroup sg_r(*hist_r, SharedGroupOptions(key));
        SharedGroup sg_w(*hist_w, SharedGroupOptions(key));
        Group& g = const_cast<Group&>(sg_w.begin_write());
        Group& g_r = const_cast<Group&>(sg_r.begin_read());
        std::vector<TableView> table_views;
        std::vector<TableRef> subtable_refs;

        for (;;) {
            char instr = get_next(s) % COUNT;

            if (instr == ADD_TABLE && g.size() < max_tables) {
                std::string name = create_table_name(s);
                if (log) {
                    *log << "try { g.add_table(\"" << name << "\"); } catch (const TableNameInUse&) { }\n";
                }
                try {
                    g.add_table(name);
                }
                catch (const TableNameInUse&) {
                }
            }
            else if (instr == INSERT_TABLE && g.size() < max_tables) {
                size_t table_ndx = get_next(s) % (g.size() + 1);
                std::string name = create_table_name(s);
                if (log) {
                    *log << "try { g.insert_table(" << table_ndx << ", \"" << name
                         << "\"); } catch (const TableNameInUse&) { }\n";
                }
                try {
                    g.insert_table(table_ndx, name);
                }
                catch (const TableNameInUse&) {
                }
            }
            else if (instr == REMOVE_TABLE && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                if (log) {
                    *log << "try { g.remove_table(" << table_ndx << "); } catch (const CrossTableLinkTarget&) { }\n";
                }
                try {
                    g.remove_table(table_ndx);
                }
                catch (const CrossTableLinkTarget&) {
                }
            }
            else if (instr == CLEAR_TABLE && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                if (log) {
                    *log << "g.get_table(" << table_ndx << ")->clear();\n";
                }
                g.get_table(table_ndx)->clear();
            }
            else if (instr == MOVE_TABLE && g.size() >= 2) {
                size_t from_ndx = get_next(s) % g.size();
                size_t to_ndx = get_next(s) % g.size();
                if (from_ndx != to_ndx) {
                    if (log) {
                        *log << "g.move_table(" << from_ndx << ", " << to_ndx << ");\n";
                    }
                    g.move_table(from_ndx, to_ndx);
                }
            }
            else if (instr == INSERT_ROW && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                if (g.get_table(table_ndx)->get_column_count() == 0) {
                    continue; // do not insert rows if there are no columns
                }
                size_t row_ndx = get_next(s) % (g.get_table(table_ndx)->size() + 1);
                size_t num_rows = get_next(s);
                typedef _impl::TableFriend tf;
                if (g.get_table(table_ndx)->get_column_count() > 0 ||
                    tf::is_cross_table_link_target(*g.get_table(table_ndx))) {
                    if (log) {
                        *log << "g.get_table(" << table_ndx << ")->insert_empty_row(" << row_ndx << ", "
                             << num_rows % add_empty_row_max << ");\n";
                    }
                    g.get_table(table_ndx)->insert_empty_row(row_ndx, num_rows % add_empty_row_max);
                }
            }
            else if (instr == ADD_EMPTY_ROW && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                if (g.get_table(table_ndx)->get_column_count() == 0) {
                    continue; // do not add rows if there are no columns
                }
                size_t num_rows = get_next(s);
                if (g.get_table(table_ndx)->size() + num_rows < max_rows) {
                    typedef _impl::TableFriend tf;
                    if (g.get_table(table_ndx)->get_column_count() > 0 ||
                        tf::is_cross_table_link_target(*g.get_table(table_ndx))) {
                        if (log) {
                            *log << "g.get_table(" << table_ndx << ")->add_empty_row(" << num_rows % add_empty_row_max
                                 << ");\n";
                        }
                        g.get_table(table_ndx)->add_empty_row(num_rows % add_empty_row_max);
                    }
                }
            }
            else if (instr == ADD_COLUMN && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                DataType type = get_type(get_next(s));
                std::string name = create_column_name(s);
                // Mixed cannot be nullable. For other types, chose nullability randomly
                bool nullable = (type == type_Mixed) ? false : (get_next(s) % 2 == 0);
                if (type != type_Table) {
                    if (log) {
                        *log << "g.get_table(" << table_ndx << ")->add_column(DataType(" << int(type) << "), \""
                             << name << "\", " << (nullable ? "true" : "false") << ");\n";
                    }
                    g.get_table(table_ndx)->add_column(type, name, nullable);
                }
                else {
                    bool subnullable = (get_next(s) % 2 == 0);
                    if (log) {
                        *log << "{\n"
                             << "DescriptorRef subdescr;\n"
                             << "g.get_table(" << table_ndx << ")->add_column(type_Table, \"" << name << "\", "
                             << (nullable ? "true" : "false") << ", &subdescr);\n"
                             << "subdescr->add_column(type_Int, \"integers\", nullptr, "
                             << (subnullable ? "true" : "false") << ");\n"
                             << "}\n";
                    }
                    DescriptorRef subdescr;
                    g.get_table(table_ndx)->add_column(type, name, subnullable, &subdescr);
                    subdescr->add_column(type_Int, "integers", nullptr, subnullable);
                }
            }
            else if (instr == INSERT_COLUMN && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                size_t col_ndx = get_next(s) % (g.get_table(table_ndx)->get_column_count() + 1);
                DataType type = get_type(get_next(s));
                std::string name = create_column_name(s);
                bool nullable = (type == type_Mixed) ? false : (get_next(s) % 2 == 0);
                if (type != type_Table) {
                    if (log) {
                        *log << "g.get_table(" << table_ndx << ")->insert_column(" << col_ndx << ", DataType("
                             << int(type) << "), \"" << name << "\", " << (nullable ? "true" : "false") << ");\n";
                    }
                    g.get_table(table_ndx)->insert_column(col_ndx, type, name, nullable);
                }
                else {
                    bool subnullable = (get_next(s) % 2 == 0);
                    if (log) {
                        *log << "{\n"
                             << "DescriptorRef subdescr;\n"
                             << "g.get_table(" << table_ndx << ")->insert_column(" << col_ndx << ", type_Table, "
                             << "\"" << name << "\", " << (nullable ? "true" : "false") << ", &subdescr);\n"
                             << "subdescr->add_column(type_Int, \"integers\", nullptr, "
                             << (subnullable ? "true" : "false") << ");\n"
                             << "}\n";
                    }
                    DescriptorRef subdescr;
                    g.get_table(table_ndx)->insert_column(col_ndx, type, name, subnullable, &subdescr);
                    subdescr->add_column(type_Int, "integers", nullptr, subnullable);
                }
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
                    std::string name = create_column_name(s);
                    if (log) {
                        *log << "g.get_table(" << table_ndx << ")->rename_column("
                             << col_ndx << ", \"" << name << "\");\n";
                    }
                    t->rename_column(col_ndx, name);
                }
            }
            else if (instr == MOVE_COLUMN && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->get_column_count() > 1) {
                    // There's a chance that we randomly choose to move a column
                    // index with itself, but that's ok lets test that case too
                    size_t col_ndx1 = get_next(s) % t->get_column_count();
                    size_t col_ndx2 = get_next(s) % t->get_column_count();
                    if (log) {
                        *log << "_impl::TableFriend::move_column(*(g.get_table(" << table_ndx
                             << ")->get_descriptor()), " << col_ndx1 << ", " << col_ndx2 << ");\n";
                    }
                    _impl::TableFriend::move_column(*(t->get_descriptor()), col_ndx1, col_ndx2);
                }
            }
            else if (instr == ADD_SEARCH_INDEX && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->get_column_count() > 0) {
                    size_t col_ndx = get_next(s) % t->get_column_count();
                    DataType typ = t->get_column_type(col_ndx);

                    if (typ == type_Table) {
                        if (log) {
                            *log << "g.get_table(" << table_ndx << ")->get_subdescriptor(" << col_ndx
                                 << ")->add_search_index(0);\n";
                        }
                        t->get_subdescriptor(col_ndx)->add_search_index(0);
                    }
                    else {
                        bool supports_search_index =
                            _impl::TableFriend::get_column(*t, col_ndx).supports_search_index();

                        if (supports_search_index) {
                            if (log) {
                                *log << "g.get_table(" << table_ndx << ")->add_search_index(" << col_ndx << ");\n";
                            }
                            t->add_search_index(col_ndx);
                        }
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
                    DataType typ = t->get_column_type(col_ndx);

                    if (typ == type_Table) {
                        if (log) {
                            *log << "g.get_table(" << table_ndx << ")->get_subdescriptor(" << col_ndx
                                 << ")->remove_search_index(0);\n";
                        }
                        t->get_subdescriptor(col_ndx)->remove_search_index(0);
                    }
                    else {
                        if (log) {
                            *log << "g.get_table(" << table_ndx << ")->remove_search_index(" << col_ndx << ");\n";
                        }
                        t->remove_search_index(col_ndx);
                    }
                }
            }
            else if (instr == ADD_COLUMN_LINK && g.size() >= 1) {
                size_t table_ndx_1 = get_next(s) % g.size();
                size_t table_ndx_2 = get_next(s) % g.size();
                TableRef t1 = g.get_table(table_ndx_1);
                TableRef t2 = g.get_table(table_ndx_2);
                std::string name = create_column_name(s);
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
                std::string name = create_column_name(s);
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
                std::string name = create_column_name(s);
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
                    size_t row_ndx = get_next(s) % t->size();
                    DataType type = t->get_column_type(col_ndx);

                    // With equal probability, either set to null or to a value
                    if (get_next(s) % 2 == 0 && t->is_nullable(col_ndx)) {
                        if (type == type_Link) {
                            if (log) {
                                *log << "g.get_table(" << table_ndx << ")->nullify_link(" << col_ndx << ", "
                                     << row_ndx << ");\n";
                            }
                            t->nullify_link(col_ndx, row_ndx);
                        }
                        else {
                            if (log) {
                                *log << "g.get_table(" << table_ndx << ")->set_null(" << col_ndx << ", " << row_ndx
                                     << ");\n";
                            }
                            t->set_null(col_ndx, row_ndx);
                        }
                    }
                    else {
                        if (type == type_String) {
                            std::string str = create_string(get_next(s));
                            if (log) {
                                *log << "g.get_table(" << table_ndx << ")->set_string(" << col_ndx << ", " << row_ndx
                                     << ", \"" << str << "\");\n";
                            }
                            t->set_string(col_ndx, row_ndx, str);
                        }
                        else if (type == type_Binary) {
                            bool insert_big_blob = get_next(s) % 2 == 0;
                            if (insert_big_blob) {
                                size_t rand_char = get_next(s);
                                size_t blob_size = get_next(s) + ArrayBlob::max_binary_size;
                                std::string blob(blob_size, static_cast<unsigned char>(rand_char));
                                if (log) {
                                    *log << "{\n\tstd::string data(" << blob_size << ", static_cast<unsigned char>(" << rand_char << "));\n\t"
                                    << "g.get_table(" << table_ndx << ")->set_binary_big(" << col_ndx << ", " << row_ndx
                                    << ", BinaryData(data.data(), " << blob_size << "));\n}\n";
                                }
                                t->set_binary_big(col_ndx, row_ndx, BinaryData(blob.data(), blob_size));
                            }
                            else {
                                std::string str = create_string(get_next(s));
                                if (log) {
                                    *log << "g.get_table(" << table_ndx << ")->set_binary(" << col_ndx << ", " << row_ndx
                                    << ", BinaryData{\"" << str << "\", " << str.size() << "});\n";
                                }
                                t->set_binary(col_ndx, row_ndx, BinaryData(str));
                            }
                        }
                        else if (type == type_Int) {
                            bool add_int = get_next(s) % 2 == 0;
                            int64_t value = get_int64(s);
                            if (add_int) {
                                if (log) {
                                    *log << "try { g.get_table(" << table_ndx << ")->add_int(" << col_ndx
                                    << ", " << row_ndx << ", " << value
                                    << "); } catch (const LogicError& le) { CHECK(le.kind() == "
                                    "LogicError::illegal_combination); }\n";
                                }
                                try {
                                    t->add_int(col_ndx, row_ndx, value);
                                }
                                catch (const LogicError& le) {
                                    if (le.kind() != LogicError::illegal_combination) {
                                        throw;
                                    }
                                }
                            }
                            else {
                                if (log) {
                                    *log << "g.get_table(" << table_ndx << ")->set_int(" << col_ndx << ", " << row_ndx
                                    << ", " << value << ");\n";
                                }
                                t->set_int(col_ndx, row_ndx, get_next(s));
                            }

                        }
                        else if (type == type_Bool) {
                            bool value = get_next(s) % 2 == 0;
                            if (log) {
                                *log << "g.get_table(" << table_ndx << ")->set_bool(" << col_ndx << ", " << row_ndx
                                     << ", " << (value ? "true" : "false") << ");\n";
                            }
                            t->set_bool(col_ndx, row_ndx, value);
                        }
                        else if (type == type_Float) {
                            float value = get_next(s);
                            if (log) {
                                *log << "g.get_table(" << table_ndx << ")->set_float(" << col_ndx << ", " << row_ndx
                                     << ", " << value << ");\n";
                            }
                            t->set_float(col_ndx, row_ndx, value);
                        }
                        else if (type == type_Double) {
                            double value = get_next(s);
                            if (log) {
                                *log << "g.get_table(" << table_ndx << ")->set_double(" << col_ndx << ", " << row_ndx
                                     << ", " << value << ");\n";
                            }
                            t->set_double(col_ndx, row_ndx, value);
                        }
                        else if (type == type_Link) {
                            TableRef target = t->get_link_target(col_ndx);
                            if (target->size() > 0) {
                                size_t target_row = get_next(s) % target->size();
                                if (log) {
                                    *log << "g.get_table(" << table_ndx << ")->set_link(" << col_ndx << ", "
                                         << row_ndx << ", " << target_row << ");\n";
                                }
                                t->set_link(col_ndx, row_ndx, target_row);
                            }
                        }
                        else if (type == type_LinkList) {
                            TableRef target = t->get_link_target(col_ndx);
                            if (target->size() > 0) {
                                LinkViewRef links = t->get_linklist(col_ndx, row_ndx);
                                // either add or set, 50/50 probability
                                if (links->size() > 0 && get_next(s) > 128) {
                                    size_t linklist_row = get_next(s) % links->size();
                                    size_t target_link_ndx = get_next(s) % target->size();
                                    if (log) {
                                        *log << "g.get_table(" << table_ndx << ")->get_linklist(" << col_ndx << ", "
                                             << row_ndx << ")->set(" << linklist_row << ", " << target_link_ndx
                                             << ");\n";
                                    }
                                    links->set(linklist_row, target_link_ndx);
                                }
                                else {
                                    size_t target_link_ndx = get_next(s) % target->size();
                                    if (log) {
                                        *log << "g.get_table(" << table_ndx << ")->get_linklist(" << col_ndx << ", "
                                             << row_ndx << ")->add(" << target_link_ndx << ");\n";
                                    }
                                    links->add(target_link_ndx);
                                }
                            }
                        }
                        else if (type == type_Timestamp) {
                            std::pair<int64_t, int32_t> values = get_timestamp_values(s);
                            Timestamp value{values.first, values.second};
                            if (log) {
                                *log << "g.get_table(" << table_ndx << ")->set_timestamp(" << col_ndx << ", "
                                     << row_ndx << ", " << value << ");\n";
                            }
                            t->set_timestamp(col_ndx, row_ndx, value);
                        }
                        else if (type == type_Mixed) {
                            if (log) {
                                *log << "{\n";
                            }
                            std::string buffer;
                            Mixed mixed = construct_mixed(s, log, buffer);
                            if (log) {
                                *log << "g.get_table(" << table_ndx << ")->set_mixed(" << col_ndx << ", "
                                     << row_ndx << ", mixed);\n}\n";
                            }
                            t->set_mixed(col_ndx, row_ndx, mixed);
                        }
                        else if (type == type_Table) {
                            if (log) {
                                *log << "{\n"
                                     << "TableRef sub = g.get_table(" << table_ndx << ")->get_subtable(" << col_ndx
                                     << ", " << row_ndx << ");\n";
                            }
                            TableRef sub = t->get_subtable(col_ndx, row_ndx);
                            size_t sz = sub->size();
                            REALM_ASSERT(sz == t->get_subtable_size(col_ndx, row_ndx));
                            if (sz == 0 || get_next(s) % 4 == 0) {
                                // In 25 % of the cases assign all new values
                                int nb_values = get_next(s) % 10;
                                std::vector<int64_t> values;
                                for (int i = 0; i < nb_values; i++) {
                                    values.push_back(int(get_next(s)));
                                }
                                if (log) {
                                    *log << "sub->clear();\n"
                                         << "sub->add_empty_row(" << nb_values << ");\n";
                                    for (int i = 0; i < nb_values; i++) {
                                        *log << "sub->set_int(0, " << i << ", " << values[i] << ", false);\n";
                                    }
                                }
                                sub->clear();
                                sub->add_empty_row(nb_values);
                                for (int i = 0; i < nb_values; i++) {
                                    sub->set_int(0, i, values[i], false);
                                }
                            }
                            else {
                                size_t row = get_next(s) % sz;
                                int64_t value = get_int64(s);
                                if (log) {
                                    *log << "sub->set_int(0, " << row << ", " << value << ", false);\n";
                                }
                                sub->set_int(0, row, value, false);
                            }
                            if (log) {
                                *log << "subtable_refs.push_back(sub);\n}\n";
                            }
                            subtable_refs.push_back(sub);
                        }
                    }
                }
            }
            else if (instr == REMOVE_ROW && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->size() > 0) {
                    size_t row_ndx = get_next(s) % t->size();
                    if (log) {
                        *log << "g.get_table(" << table_ndx << ")->remove(" << row_ndx << ");\n";
                    }
                    t->remove(row_ndx);
                }
            }
            else if (instr == MERGE_ROWS && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->size() > 1) {
                    size_t row_ndx1 = get_next(s) % t->size();
                    size_t row_ndx2 = get_next(s) % t->size();
                    if (row_ndx1 == row_ndx2) {
                        row_ndx2 = (row_ndx2 + 1) % t->size();
                    }
                    // A restriction of merge_rows is that any linklists in the
                    // "to" row must be empty because merging lists is not defined.
                    for (size_t col_ndx = 0; col_ndx != t->get_column_count(); ++col_ndx) {
                        if (t->get_column_type(col_ndx) == DataType::type_LinkList) {
                            if (!t->get_linklist(col_ndx, row_ndx2)->is_empty()) {
                                if (log) {
                                    *log << "g.get_table(" << table_ndx << ")->get_linklist("
                                    << col_ndx << ", " << row_ndx2 << ")->clear();\n";
                                }
                                t->get_linklist(col_ndx, row_ndx2)->clear();
                            }
                        }
                    }
                    if (log) {
                        *log << "g.get_table(" << table_ndx << ")->merge_rows(" << row_ndx1 << ", " << row_ndx2 << ");\n";
                    }
                    t->merge_rows(row_ndx1, row_ndx2);
                }
            }
            else if (instr == MOVE_LAST_OVER && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->size() > 0) {
                    int32_t row_ndx = get_int32(s) % t->size();
                    if (log) {
                        *log << "g.get_table(" << table_ndx << ")->move_last_over(" << row_ndx << ");\n";
                    }
                    t->move_last_over(row_ndx);
                }
            }
            else if (instr == SWAP_ROWS && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->size() > 0) {
                    int32_t row_ndx1 = get_int32(s) % t->size();
                    int32_t row_ndx2 = get_int32(s) % t->size();
                    if (log) {
                        *log << "g.get_table(" << table_ndx << ")->swap_rows(" << row_ndx1 << ", " << row_ndx2
                             << ");\n";
                    }
                    t->swap_rows(row_ndx1, row_ndx2);
                }
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
                        *log << "sg_r.open(path);\n";
                    }
                    sg_r.open(path);
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
                        *log << "sg_w.open(path);\n";
                    }
                    sg_w.open(path);
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
            else if (instr == CREATE_SUBTABLE_VIEW && subtable_refs.size() > 0) {
                size_t idx = get_next(s) % subtable_refs.size();
                size_t sz = subtable_refs[idx]->size();
                if (subtable_refs[idx]->is_attached() && sz) {
                    size_t find_ndx = get_next(s) % sz;
                    if (log) {
                        *log << "{\n"
                             << "int64_t val = subtable_refs[" << idx << "]->get_int(0, " << find_ndx << ");\n"
                             << "TableView tv = subtable_refs[" << idx << "]->where().equal(0, val).find_all();\n"
                             << "table_views.push_back(tv);\n"
                             << "}\n";
                    }
                    int64_t val = subtable_refs[idx]->get_int(0, find_ndx);
                    TableView tv = subtable_refs[idx]->where().equal(0, val).find_all();
                    table_views.push_back(tv);
                }
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
                    *log << "sg_r.open(path);\n";
                }
                sg_r.open(path);
                if (log) {
                    *log << "sg_r.begin_read();\n";
                }
                sg_r.begin_read();
                REALM_DO_IF_VERIFY(log, g_r.verify());
            }
            else if (instr == SET_UNIQUE && g.size() > 0) {
                std::pair<size_t, size_t> target = get_target_for_set_unique(g, s);
                if (target.first < g.size()) {
                    size_t table_ndx = target.first;
                    size_t col_ndx = target.second;
                    TableRef t = g.get_table(table_ndx);

                    // Only integer and string columns are supported. We let the fuzzer choose to set either
                    // null or a value (depending also on the nullability of the column).
                    //
                    // for integer columns, that means we call either of
                    //  - set_null_unique
                    //  - set_int_unique
                    // while for string columns, both null and values are handled by
                    //  - set_string_unique
                    //
                    // Due to an additional limitation involving non-empty lists, a specific kind of LogicError
                    // may be thrown. This is handled for each case below and encoded as a CHECK in the generated
                    // C++ unit tests when logging is enabled. Other kinds / types of exception are not handled,
                    // but simply rethrown.

                    DataType type = t->get_column_type(col_ndx);
                    switch (type) {
                        case type_Int: {
                            size_t row_ndx = get_int32(s) % t->size();
                            bool set_null = t->is_nullable(col_ndx) ? get_next(s) % 2 == 0 : false;
                            if (set_null) {
                                if (log) {
                                    *log << "try { g.get_table(" << table_ndx << ")->set_null_unique(" << col_ndx
                                         << ", " << row_ndx
                                         << "); } catch (const LogicError& le) "
                                            "{ CHECK(le.kind() == LogicError::illegal_combination); }\n";
                                }
                                try {
                                    t->set_null_unique(col_ndx, row_ndx);
                                }
                                catch (const LogicError& le) {
                                    if (le.kind() != LogicError::illegal_combination) {
                                        throw;
                                    }
                                }
                            }
                            else {
                                int64_t value = get_int64(s);
                                if (log) {
                                    *log << "try { g.get_table(" << table_ndx << ")->set_int_unique(" << col_ndx
                                         << ", " << row_ndx << ", " << value
                                         << "); } catch (const LogicError& le) "
                                            "{ CHECK(le.kind() == LogicError::illegal_combination); }\n";
                                }
                                try {
                                    t->set_int_unique(col_ndx, row_ndx, value);
                                }
                                catch (const LogicError& le) {
                                    if (le.kind() != LogicError::illegal_combination) {
                                        throw;
                                    }
                                }
                            }
                            break;
                        }
                        case type_String: {
                            size_t row_ndx = get_int32(s) % t->size();
                            bool set_null = t->is_nullable(col_ndx) ? get_next(s) % 2 == 0 : false;
                            if (set_null) {
                                if (log) {
                                    *log << "try { g.get_table(" << table_ndx << ")->set_string_unique(" << col_ndx
                                         << ", " << row_ndx << ", null{}); } catch (const LogicError& le) "
                                            "{ CHECK(le.kind() == LogicError::illegal_combination); }\n";
                                }
                                try {
                                    t->set_string_unique(col_ndx, row_ndx, null{});
                                }
                                catch (const LogicError& le) {
                                    if (le.kind() != LogicError::illegal_combination) {
                                        throw;
                                    }
                                }
                            }
                            else {
                                std::string str = create_string(get_next(s));
                                if (log) {
                                    *log << "try { g.get_table(" << table_ndx << ")->set_string_unique(" << col_ndx
                                         << ", " << row_ndx << ", \"" << str << "\"); } catch (const LogicError& le) "
                                            "{ CHECK(le.kind() == LogicError::illegal_combination); }\n";
                                }
                                try {
                                    t->set_string_unique(col_ndx, row_ndx, str);
                                }
                                catch (const LogicError& le) {
                                    if (le.kind() != LogicError::illegal_combination) {
                                        throw;
                                    }
                                }
                            }
                            break;
                        }
                        default:
                            break;
                    }
                }
            }
            else if (instr == IS_NULL && g_r.size() > 0) {
                size_t table_ndx = get_next(s) % g_r.size();
                TableRef t = g_r.get_table(table_ndx);
                if (t->get_column_count() > 0 && t->size() > 0) {
                    size_t col_ndx = get_int32(s) % t->get_column_count();
                    size_t row_ndx = get_int32(s) % t->size();
                    if (log) {
                        *log << "g_r.get_table(" << table_ndx << ")->is_null(" << col_ndx << ", " << row_ndx
                             << ");\n";
                    }
                    bool res = t->is_null(col_ndx, row_ndx);
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
    fprintf(stderr, "Usage: %s FILE [--log] [--name NAME]\n"
                    "Where FILE is a instruction file that will be replayed.\n"
                    "Pass --log to have code printed to stdout producing the same instructions.\n"
                    "Pass --name NAME with distinct values when running on multiple threads,\n"
                    "                 to make sure the test don't use the same Realm file\n",
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
