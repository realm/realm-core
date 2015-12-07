#include <realm/group_shared.hpp>
#include <realm/link_view.hpp>
#include <realm/commit_log.hpp>
#include "../test.hpp"

#include <stdio.h>
#include <fstream>

using namespace realm;
using namespace realm::util;
using namespace std;

struct EndOfFile {};

std::string create_string(unsigned char byte)
{
    char buf[256] = {0};
    for (size_t i = 0; i < sizeof(buf); i++)
        buf[i] = 'a' + (rand() % 20);
    return std::string{buf, byte};
}

enum INS {  ADD_TABLE, INSERT_TABLE, REMOVE_TABLE, INSERT_ROW, ADD_EMPTY_ROW, INSERT_COLUMN,
            ADD_COLUMN, REMOVE_COLUMN, SET, REMOVE_ROW, ADD_COLUMN_LINK, ADD_COLUMN_LINK_LIST,
            CLEAR_TABLE, MOVE_TABLE, INSERT_COLUMN_LINK, ADD_SEARCH_INDEX, REMOVE_SEARCH_INDEX,

            COUNT};

DataType get_type(unsigned char c)
{
    DataType types[9] = {
        type_Int,
        type_Bool,
        type_Float,
        type_Double,
        type_String,
        type_Binary,
        type_DateTime,
        type_Table,
        type_Mixed
    };

    unsigned char mod = c % (sizeof(types) / sizeof(DataType));
    return types[mod];
}

struct State {
    std::string str;
    size_t pos;
};

unsigned char get_next(State s)
{
    if (s.pos == s.str.size()) {
        throw EndOfFile{};
    }
    char byte = s.str[s.pos];
    s.pos++;
    return byte;
}

int64_t get_int64(std::string& in, size_t& pos) {
    return 0;
}

void parse_and_apply_instructions(std::string& in, Group& g, util::Optional<std::ostream&> log)
{
    // Temporary limit due to bug in add_empty_row()
    size_t EMPTY_ROW_MAX = 2;

    try {
        State s;
        s.str = in;
        s.pos = 0;
 
        size_t pos = 0;
        for (;;) {
            char instr = get_next(s) % COUNT;

            if (instr == ADD_TABLE && g.size() < 1100) {
                auto name = create_string(get_next(s) % Group::max_table_name_length);
                if (log) {
                    *log << "g.add_table(\"" << name << "\");\n";
                }
                try {
                    g.add_table(name);
                }
                catch (const TableNameInUse&) {
                }
            }
            else if (instr == INSERT_TABLE && g.size() < 1100) {
                size_t s0 = get_next(s) % (g.size() + 1);
                string sd0 = create_string(get_next(s) % (Group::max_table_name_length - 10) + 5);
                if (log) {
                    *log << "g.insert_table(" << s0 << ", \"" << sd0 << "\");\n";
                }
                g.insert_table(s0, sd0);
            }
            else if (instr == REMOVE_TABLE && g.size() > 0) {
                size_t idx = get_next(s) % g.size();
                if (log) {
                    *log << "g.remove_table(" << idx << ");\n";
                }
                try {
                    g.remove_table(idx);
                }
                catch (const CrossTableLinkTarget&) {
                }
            }
            else if (instr == CLEAR_TABLE && g.size() > 0) {
                size_t idx = get_next(s) % g.size();
                if (log) {
                    *log << "g.get_table(" << idx << ")->clear();\n";
                }
                g.get_table(idx)->clear();
            }
            else if (instr == MOVE_TABLE && g.size() >= 2) {
                size_t t1 = get_next(s) % g.size();
                size_t t2 = get_next(s) % g.size();
                if (t1 != t2) {
                    if (log) {
                        *log << "g.move_table(" << t1 << ", " << t2 << ");\n";
                    }
                    g.move_table(t1, t2);
                }
            }
            else if (instr == INSERT_ROW && g.size() > 0) {
                size_t table_idx = get_next(s) % g.size();
                size_t row_ndx = get_next(s) % (g.get_table(table_idx)->size() + 1);
                size_t num_rows = get_next(s);
                if (log) {
                    *log << "g.get_table(" << table_idx << ")->insert_empty_row(" << row_ndx << ", " << num_rows % EMPTY_ROW_MAX << ");\n";
                }
                g.get_table(table_idx)->insert_empty_row(row_ndx, num_rows % EMPTY_ROW_MAX);
            }
            else if (instr == ADD_EMPTY_ROW && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                size_t num_rows = get_next(s);
                if (log) {
                    *log << "g.get_table(" << table_ndx << ")->add_empty_row(" << num_rows % EMPTY_ROW_MAX << ");\n";
                }
                g.get_table(table_ndx)->add_empty_row(num_rows % EMPTY_ROW_MAX);
            }
            else if (instr == ADD_COLUMN && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                DataType type = get_type(get_next(s));
                string name = create_string(get_next(s) % Group::max_table_name_length);
                // Mixed and Subtable cannot be nullable. For other types, chose nullability randomly
                bool nullable = (type == type_Mixed || type == type_Table) ? false : (get_next(s) % 2 == 0);
                if (log) {
                    *log << "g.get_table(" << table_ndx << ")->add_column(DataType(" << int(type) << "), \"" << name << "\"," << nullable << ");\n";
                }
                g.get_table(table_ndx)->add_column(type, name, nullable);
            }
            else if (instr == INSERT_COLUMN && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                size_t col_ndx = get_next(s) % (g.get_table(table_ndx)->get_column_count() + 1);
                DataType type = get_type(get_next(s));
                string name = create_string(get_next(s) % Group::max_table_name_length);
                bool nullable = (type == type_Mixed || type == type_Table) ? false : (get_next(s) % 2 == 0);
                if (log) {
                    *log << "g.get_table(" << table_ndx << ")->insert_column(" << col_ndx << ", DataType(" << int(type) << "), \"" << name << "\"," << nullable << ");\n";
                }
                g.get_table(table_ndx)->insert_column(col_ndx, type, name, nullable);
            }
            else if (instr == REMOVE_COLUMN && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->get_column_count() > 0) {
                    size_t col_ndx = get_next(s) % t->get_column_count();
                    if (log) {
                        *log << "TableRef t = g.get_table(" << table_ndx << "); t->remove_column(" << col_ndx << ");\n";
                    }
                    t->remove_column(col_ndx);
                }
            }
            else if (instr == ADD_SEARCH_INDEX && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->get_column_count() > 0) {
                    size_t col_ndx = get_next(s) % t->get_column_count();
                    DataType dt = t->get_column_type(col_ndx);
                    if (dt != type_Float && dt != type_Double && dt != type_Link && dt != type_LinkList && dt != type_Table && dt != type_Mixed && dt != type_Binary) {
                        t->add_search_index(col_ndx);
                        
                        if (log) {
                            *log << "TableRef t = g.get_table(" << table_ndx << "); t->remove_column(" << col_ndx << ");\n";
                        }
                    }
                }
            }
            else if (instr == REMOVE_SEARCH_INDEX && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->get_column_count() > 0) {
                    size_t col_ndx = get_next(s) % t->get_column_count();
                    // We don't need to check if the column is of a type that is indexable or if it has index on or off
                    // because Realm will just do a no-op at worst (no exception or assert).
                    t->remove_search_index(col_ndx);

                    if (log) {
                        *log << "TableRef t = g.get_table(" << table_ndx << "); t->remove_search_index(" << col_ndx << ");\n";
                    }
                }
            }
            else if (instr == ADD_COLUMN_LINK && g.size() >= 1) {
                size_t table_ndx_1 = get_next(s) % g.size();
                size_t table_ndx_2 = get_next(s) % g.size();
                TableRef t1 = g.get_table(table_ndx_1);
                TableRef t2 = g.get_table(table_ndx_2);
                string name = create_string(get_next(s) % Group::max_table_name_length);
                if (log) {
                    *log << "g.get_table(" << table_ndx_1 << ")->add_column_link(type_Link, \"" << name << "\", *g.get_table(" << table_ndx_2 << "));\n";
                }
                t1->add_column_link(type_Link, name, *t2);
            }
            else if (instr == INSERT_COLUMN_LINK && g.size() >= 1) {
                size_t table_ndx_1 = get_next(s) % g.size();
                size_t table_ndx_2 = get_next(s) % g.size();
                size_t col_ndx = get_next(s) % (g.get_table(table_ndx_1)->get_column_count() + 1);
                TableRef t1 = g.get_table(table_ndx_1);
                TableRef t2 = g.get_table(table_ndx_2);
                string name = create_string(get_next(s) % Group::max_table_name_length);
                if (log) {
                    *log << "g.get_table(" << table_ndx_1 << ")->insert_column_link(" << col_ndx << ", type_Link, \"" << name << "\", *g.get_table(" << table_ndx_2 << "));\n";
                }
                t1->insert_column_link(col_ndx, type_Link, name, *t2);
            }
            else if (instr == ADD_COLUMN_LINK_LIST && g.size() >= 2) {
                size_t table_ndx_1 = get_next(s) % g.size();
                size_t table_ndx_2 = get_next(s) % g.size();
                TableRef t1 = g.get_table(table_ndx_1);
                TableRef t2 = g.get_table(table_ndx_2);
                string name = create_string(get_next(s) % Group::max_table_name_length);
                if (log) {
                    *log << "g.get_table(" << table_ndx_1 << ")->add_column_link(type_LinkList, \"" << name << "\", *g.get_table(" << table_ndx_2 << "));\n";
                }
                t1->add_column_link(type_LinkList, name, *t2);
            }
            else if (instr == SET && g.size() > 0) {
                size_t table_ndx = get_next(s) % g.size();
                TableRef t = g.get_table(table_ndx);
                if (t->get_column_count() > 0 && t->size() > 0) {
                    size_t c = get_next(s) % t->get_column_count();
                    size_t r = get_next(s) % t->size();

                    // With equal probability, either set to null or to a value
                    if (get_next(s) % 2 == 0 && t->is_nullable(c)) {
                        t->set_null(c, r);
                        if (log) {
                            *log << "g.get_table(" << table_ndx << ")->set_null(" << c << ", " << r << ");\n";
                        }
                    }
                    else {
                        DataType d = t->get_column_type(c);
                        if (d == type_String) {
                            string str = create_string(get_next(s));
                            if (log) {
                                *log << "g.get_table(" << table_ndx << ")->set_string(" << c << ", " << r << ", \"" << str << "\");\n";
                            }
                            t->set_string(c, r, str);
                        }
                        else if (d == type_Binary) {
                            string str = create_string(get_next(s));
                            if (log) {
                                *log << "g.get_table(" << table_ndx << ")->set_binary(" << c << ", " << r << ", BinaryData{\"" << str << "\", " << str.size() << "});\n";
                            }
                            t->set_binary(c, r, BinaryData(str));
                        }
                        else if (d == type_Int) {
                            int64_t value = get_next(s);
                            if (log) {
                                *log << "g.get_table(" << table_ndx << ")->set_int(" << c << ", " << r << ", " << value << ");\n";
                            }
                            t->set_int(c, r, get_next(s));
                        }
                        else if (d == type_DateTime) {
                            DateTime value{ get_next(s) };
                            if (log) {
                                *log << "g.get_table(" << table_ndx << ")->set_int(" << c << ", " << r << ", " << value << ");\n";
                            }
                            t->set_datetime(c, r, value);
                        }
                        else if (d == type_Bool) {
                            bool value = get_next(s) % 2 == 0;
                            if (log) {
                                *log << "g.get_table(" << table_ndx << ")->set_bool(" << c << ", " << r << ", " << value << ");\n";
                            }
                            t->set_bool(c, r, value);
                        }
                        else if (d == type_Float) {
                            float value = get_next(s);
                            if (log) {
                                *log << "g.get_table(" << table_ndx << ")->set_float(" << c << ", " << r << ", " << value << ");\n";
                            }
                            t->set_float(c, r, value);
                        }
                        else if (d == type_Double) {
                            double value = get_next(s);
                            if (log) {
                                *log << "g.get_table(" << table_ndx << ")->set_double(" << c << ", " << r << ", " << value << ");\n";
                            }
                            t->set_double(c, r, value);
                        }
                        else if (d == type_Link) {
                            TableRef target = t->get_link_target(c);
                            if (target->size() > 0) {
                                size_t target_row = get_next(s) % target->size();
                                if (log) {
                                    *log << "g.get_table(" << table_ndx << ")->set_link(" << c << ", " << r << ", " << target_row << ");\n";
                                }
                                t->set_link(c, r, target_row);
                            }
                        }
                        else if (d == type_LinkList) {
                            TableRef target = t->get_link_target(c);
                            if (target->size() > 0) {
                                LinkViewRef links = t->get_linklist(c, r);

                                // either add or set, 50/50 probability
                                if (links->size() > 0 && get_next(s) > 128) {
                                    links->set(get_next(s) % links->size(), get_next(s) % target->size());
                                }
                                else {
                                    links->add(get_next(s) % target->size());
                                }
                            }
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
        }
    }
    catch (const EndOfFile&) {
    }
}

