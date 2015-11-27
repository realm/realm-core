#include <realm/group_shared.hpp>
#include <realm/link_view.hpp>
#include <realm/commit_log.hpp>
#include "../test.hpp"

#include <stdio.h>
#include <fstream>

using namespace realm;
using namespace realm::util;

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
            CLEAR_TABLE, MOVE_TABLE,

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

    unsigned char mod = c % 9;

    return types[mod];
}

char get_next(std::istream& is)
{
    if (is.eof()) {
        throw EndOfFile{};
    }
    char byte;
    is >> byte;
    return byte;
}

void parse_and_apply_instructions(std::istream& is, Group& g)
{
    while (!is.eof()) {
        char instr = get_next(is);

        if (instr % COUNT == ADD_TABLE && g.size() < 1100) {
            g.add_table(create_string(get_next(is) % Group::max_table_name_length));
        }
        else if (instr % COUNT == INSERT_TABLE && g.size() < 1100) {
            size_t s0 = get_next(is) % (g.size() + 1);
            StringData sd0 = create_string(get_next(is) % (Group::max_table_name_length - 10) + 5);

            // os << "g.insert_table(" << s0 << ", \"" << sd0 << "\");\n";
            g.insert_table(s0, sd0);
        }
        else if (instr % COUNT == REMOVE_TABLE && g.size() > 0) {
            g.remove_table(get_next(is) % g.size());
        }
        else if (instr % COUNT == CLEAR_TABLE && g.size() > 0) {
            TableRef t = g.get_table(get_next(is) % g.size());
            t->clear();
        }
        else if (instr % COUNT == MOVE_TABLE && g.size() >= 2) {
            size_t t1 = get_next(is) % g.size();
            size_t t2 = get_next(is) % g.size();
            if(t1 != t2)
                g.move_table(t1, t2);
        }
        else if (instr % COUNT == INSERT_ROW && g.size() > 0) {
            TableRef t = g.get_table(get_next(is) % g.size());
            t->insert_empty_row(get_next(is) % (t->size() + 1), get_next(is));
        }
        else if (instr % COUNT == ADD_EMPTY_ROW && g.size() > 0) {
            TableRef t = g.get_table(get_next(is) % g.size());
            t->add_empty_row(get_next(is));
        }
        else if (instr % COUNT == ADD_COLUMN && g.size() > 0) {
            TableRef t = g.get_table(get_next(is) % g.size());
            t->add_column(get_type(get_next(is)), create_string(get_next(is) % Group::max_table_name_length));
        }
        else if (instr % COUNT == INSERT_COLUMN && g.size() > 0) {
            size_t s3 = get_next(is) % g.size();
            TableRef t = g.get_table(s3);
            size_t s0 = get_next(is) % (t->get_column_count() + 1);
            DataType d0 = get_type(get_next(is));
            StringData sd0 = create_string(get_next(is) % Group::max_table_name_length);
            // os << "{TableRef t = g.get_table(" << s3 << ");\n";
            // os << "t->insert_column(" << s0 << ", DataType(" << d0 << "), \"" << sd0 << "\");}\n";
            t->insert_column(s0, d0, sd0);
        }
        else if (get_next(is) == REMOVE_COLUMN && g.size() > 0) {
            size_t s3 = get_next(is) % g.size();
            TableRef t = g.get_table(s3);
            if (t->get_column_count() > 0) {
                size_t s0 = get_next(is) % t->get_column_count();
                // os << "{TableRef t = g.get_table(" << s3 << ");\n";
                // os << "t->remove_column(" << s0 << ");}\n";
                t->remove_column(s0);
            }
        }
        else if (instr % COUNT == ADD_COLUMN_LINK && g.size() >= 1) {
            size_t s0 = get_next(is) % g.size();
            size_t s1 = get_next(is) % g.size();
            TableRef t1 = g.get_table(s0);
            TableRef t2 = g.get_table(s1);
            StringData sd0 = create_string(get_next(is) % Group::max_table_name_length);
            // os << "{TableRef t1 = g.get_table(" << s0 << ");\n";
            // os << "TableRef t2 = g.get_table(" << s1 << ");\n";
            // os << "t1->add_column_link(type_Link, \"" << sd0 << "\", *t2);}\n";
            t1->add_column_link(type_Link, sd0, *t2);
        }
        else if (instr % COUNT == ADD_COLUMN_LINK_LIST && g.size() >= 2) {
            TableRef t1 = g.get_table(get_next(is) % g.size());
            TableRef t2 = g.get_table(get_next(is) % g.size());
            t1->add_column_link(type_LinkList, create_string(get_next(is) % Group::max_table_name_length), *t2);
        }
        else if (instr % COUNT == SET && g.size() > 0) {
            TableRef t = g.get_table(get_next(is) % g.size());
            if (t->get_column_count() > 0 && t->size() > 0) {
                size_t c = get_next(is) % t->get_column_count();
                size_t r = get_next(is) % t->size();

                DataType d = t->get_column_type(c);

                if (d == type_String) {
                    t->set_string(c, r, create_string(get_next(is)));
                }
                else if (d == type_Binary) {
                    StringData sd = create_string(get_next(is));
                    t->set_binary(c, r, BinaryData(sd.data(), sd.size()));
                }
                else if (d == type_Int) {
                    t->set_int(c, r, get_next(is));
                }
                else if (d == type_DateTime) {
                    t->set_datetime(c, r, DateTime(get_next(is)));
                }
                else if (d == type_Bool) {
                    t->set_bool(c, r, get_next(is) % 2 == 0);
                }
                else if (d == type_Float) {
                    t->set_float(c, r, get_next(is));
                }
                else if (d == type_Double) {
                    t->set_double(c, r, get_next(is));
                }
                else if (d == type_Link) {
                    TableRef target = t->get_link_target(c);
                    if (target->size() > 0)
                        t->set_link(c, r, get_next(is) % target->size());
                }
                else if (d == type_LinkList) {
                    TableRef target = t->get_link_target(c);
                    if (target->size() > 0) {
                        LinkViewRef links = t->get_linklist(c, r);

/*
                        // either add or set, 50/50 probability
                        if (links->size() > 0 && get_next(is) > 128) {
                            links->set(get_next(is) % links->size(), get_next(is) % target->size());
                        }
                        else {
                            links->add(get_next(is) % target->size());
                        }
*/

                    }
                }
            }
        }
        else if (instr % COUNT == REMOVE_ROW && g.size() > 0) {
            TableRef t = g.get_table(get_next(is) % g.size());
            if (t->size() > 0) {
                t->remove(get_next(is) % t->size());
            }
        }
    }
}

int main(int argc, const char* argv[])
{
    if (argc == 1) {
        fprintf(stderr, "Usage: %s <LOGFILE>\n(where <LOGFILE> is a instruction file that will be replayed.)", argv[0]);
        exit(1);
    }

    std::ifstream in{argv[1]};
    if (!in.is_open()) {
        fprintf(stderr, "Could not open file for reading: %s\n", argv[1]);
        exit(1);
    }

    test_util::unit_test::TestDetails test_details;
    test_details.test_index = 0;
    test_details.suite_name = "FuzzyTest";
    test_details.test_name = "TransactLogApplier";
    test_details.file_name = __FILE__;
    test_details.line_number = __LINE__;

    Group group;

    try {
        parse_and_apply_instructions(in, group);
    }
    catch (const EndOfFile&) {
        return 0;
    }
    catch (const LogicError&) {
        return 0;
    }
    catch (const TableNameInUse&) {
        return 0;
    }
    catch (const NoSuchTable&) {
        return 0;
    }
    catch (const CrossTableLinkTarget&) {
        return 0;
    }
    catch (const DescriptorMismatch&) {
        return 0;
    }
    catch (const FileFormatUpgradeRequired&) {
        return 0;
    }

    return 0;
}
