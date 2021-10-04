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

#ifndef REALM_TEST_FUZZ_TESTER_HPP
#define REALM_TEST_FUZZ_TESTER_HPP

#include <realm/util/features.h>
#include <realm/list.hpp>
#include <realm/sync/transform.hpp>

#include "util/unit_test.hpp"
#include "util/quote.hpp"
#include "util/compare_groups.hpp"
#include "util/dump_changesets.hpp"
#include "peer.hpp"

#include <iostream>

namespace realm {
namespace test_util {

template <class L>
struct StreamableLambda {
    StreamableLambda(L&& l)
        : m_lambda(std::move(l))
    {
    }
    L m_lambda;
};

template <class L>
StreamableLambda<L> make_streamable_lambda(L&& lambda)
{
    return StreamableLambda<L>(std::move(lambda));
}

template <class OS, class L>
OS& operator<<(OS& os, StreamableLambda<L>&& sl)
{
    sl.m_lambda(os);
    return os;
}

template <class Source>
class FuzzTester {
public:
    FuzzTester(Source& source, bool trace)
        : m_source(source)
        , m_trace(trace)
    {
    }

    // FIXME: Eliminate the dependency on the unit_test namespace.
    void round(unit_test::TestContext&, std::string path_add_on = "");

private:
    static const int num_modifications_per_round = 256;
    static const int num_clients = 4;

    static const int modify_weight = 100;
    static const int upload_weight = 100;
    static const int download_weight = 100;


    static constexpr double group_to_table_level_transition_chance()
    {
        return 7.0 / 8;
    }

    static constexpr double table_to_array_level_transition_chance()
    {
        return 7.0 / 8;
    }

    static const int rename_table_weight = 0; // Rename table is destructive; not supported.
    static const int add_table_weight = 100;
    static const int erase_table_weight = 10;

    static const int insert_column_weight = 10;
    static const int insert_link_column_weight = 5;
    static const int insert_array_column_weight = 5;
    static const int erase_column_weight = 1;

    static const int update_row_weight = 80;
    static const int insert_row_weight = 100;
    static const int erase_row_weight = 80;

    static const int set_link_weight = 80;
    static const int insert_link_weight = 100;
    static const int remove_link_weight = 70;
    static const int move_link_weight = 50;
    static const int swap_links_weight = 50;
    static const int clear_link_list_weight = 1;

    static const int array_set_weight = 80;
    static const int array_insert_weight = 100;
    static const int array_remove_weight = 70;
    static const int array_move_weight = 50;
    static const int array_swap_weight = 0;
    static const int array_clear_weight = 1;


    template <class T>
    T draw_int(T min, T max)
    {
        return m_source.template draw_int<T>(min, max);
    }

    template <class T>
    T draw_int_mod(T mod)
    {
        return m_source.template draw_int_mod<T>(mod);
    }

    template <class T>
    T draw_int_max(T max)
    {
        return m_source.template draw_int_max<T>(max);
    }

    template <class T>
    T draw_float()
    {
        return m_source.template draw_float<T>();
    }

    bool draw_bool()
    {
        return m_source.draw_bool();
    }

    void rename_table(Peer& client)
    {
        static_cast<void>(client);
        REALM_ASSERT(false);
    }

    auto trace_client(Peer& client)
    {
        return make_streamable_lambda([&](std::ostream& os) {
            os << "client_" << client.local_file_ident;
        });
    }

    auto trace_selected_table(Peer& client)
    {
        return make_streamable_lambda([&](std::ostream& os) {
            os << trace_client(client) << "->selected_table";
        });
    }

    auto trace_selected_link_list(Peer& client)
    {
        return make_streamable_lambda([&](std::ostream& os) {
            os << trace_client(client) << "->selected_link_list";
        });
    }

    auto trace_selected_array(Peer& client)
    {
        return make_streamable_lambda([&](std::ostream& os) {
            os << trace_client(client) << "->selected_array";
        });
    }

    auto trace_selected_int_array(Peer& client)
    {
        return make_streamable_lambda([&](std::ostream& os) {
            os << "static_cast<Lst<int64_t>*>(" << trace_client(client) << "->selected_array.get())";
        });
    }

    auto trace_selected_string_array(Peer& client)
    {
        return make_streamable_lambda([&](std::ostream& os) {
            os << "static_cast<Lst<StringData>*>(" << trace_client(client) << "->selected_array.get())";
        });
    }

    void add_table(Peer& client)
    {
        char table_name[] = {'c', 'l', 'a', 's', 's', '_', 0, 0};
        table_name[6] = 'A' + draw_int_mod(6); // pick a random letter A-F

        if (client.group->get_table(table_name))
            return;

        if (table_name[6] % 2 == 1) {
            // Every other table has a PK column.
            bool is_string_pk = (table_name[6] == 'B');
            if (m_trace) {
                std::cerr << "sync::create_table_with_primary_key(*" << trace_client(client)
                          << "->"
                             "group, \""
                          << table_name << "\",";
                if (is_string_pk)
                    std::cerr << "type_String";
                else
                    std::cerr << "type_Int";
                std::cerr << ", \"pk\");\n";
            }
            client.group->add_table_with_primary_key(table_name, is_string_pk ? type_String : type_Int, "pk");
        }
        else {
            if (m_trace) {
                std::cerr << "sync::create_table(*" << trace_client(client)
                          << "->"
                             "group, \""
                          << table_name << "\");\n";
            }
            client.group->add_table(table_name);
        }
    }

    void erase_table(Peer& client)
    {
        size_t num_tables = count_classes(client);
        size_t table_ndx = draw_int_mod(num_tables);
        TableRef table = get_class(client, table_ndx);
        if (m_trace) {
            std::cerr << "sync::erase_table(*" << trace_client(client)
                      << "->"
                         "group, \""
                      << table->get_name() << "\");\n";
        }
        client.group->remove_table(table->get_name());
    }

    void clear_group(Peer& client)
    {
        if (m_trace) {
            std::cerr << trace_client(client)
                      << "->"
                         "group->clear();\n";
        }
        //        client.group->clear();
    }

    void insert_column(Peer& client)
    {
        // It is currently an error to request multiple columns with the same name
        // but with different types / nullability (there is no non-destructive way
        // to merge them).
        const char* column_names[] = {"a", "b", "c", "d"};
        const DataType column_types[] = {type_Int, type_Int, type_String, type_String};
        const bool column_nullable[] = {false, true, false, true};

        size_t which = draw_int_mod(4);
        const char* name = column_names[which];
        DataType type = column_types[which];
        bool nullable = column_nullable[which];

        TableRef table = client.selected_table;
        if (table->get_column_key(name))
            return;

        if (m_trace) {
            const char* type_name;
            if (type == type_Int) {
                type_name = "type_Int";
            }
            else if (type == type_String) {
                type_name = "type_String";
            }
            else {
                REALM_TERMINATE("Missing trace support for column type.");
            }

            std::cerr << trace_selected_table(client) << "->add_column(" << type_name << ", \"" << name << "\", "
                      << nullable << ");\n";
        }

        ColKey col_key = table->add_column(type, name, nullable);
        m_unstructured_columns.push_back(col_key);
    }

    void insert_link_column(Peer& client)
    {
        REALM_ASSERT(count_classes(client) > 1);

        const char* column_names[] = {"e", "f"};
        const DataType column_types[] = {type_Link, type_LinkList};

        size_t which = draw_int_max(1);
        const char* name = column_names[which];
        DataType type = column_types[which];

        TableRef table = client.selected_table;
        if (table->get_column_key(name))
            return;

        // Avoid divergent schemas by always creating links to table "A"
        TableKey link_target_table_key = client.group->find_table("A");
        if (!link_target_table_key)
            return;

        TableRef link_target_table = client.group->get_table(link_target_table_key);

        if (m_trace) {
            const char* type_name;
            if (type == type_Link) {
                type_name = "type_Link";
            }
            else if (type == type_LinkList) {
                type_name = "type_LinkList";
            }
            else {
                REALM_TERMINATE("Missing trace support for column type.");
            }
            std::cerr << trace_selected_table(client) << "->add_column_link(" << type_name << ", \"" << name
                      << "\", *client_" << client.local_file_ident << "->group->get_table(\"A\"));\n";
        }

        if (type == type_LinkList) {
            ColKey col_key = table->add_column_list(*link_target_table, name);
            m_link_list_columns.push_back(col_key);
        }
        else {
            ColKey col_key = table->add_column(*link_target_table, name);
            m_unstructured_columns.push_back(col_key);
        }
    }

    void insert_array_column(Peer& client)
    {
        REALM_ASSERT(count_classes(client) >= 1);

        const char* column_names[] = {"g", "h"};
        const DataType column_types[] = {type_Int, type_String};

        size_t which = draw_int_max(1);
        const char* name = column_names[which];
        DataType type = column_types[which];
        bool nullable = false;

        TableRef table = client.selected_table;
        if (table->get_column_key(name))
            return;

        if (m_trace) {
            const char* type_name;
            if (type == type_Int) {
                type_name = "type_Int";
            }
            else if (type == type_String) {
                type_name = "type_String";
            }
            else {
                REALM_TERMINATE("Missing trace support for column type.");
            }
            std::cerr << trace_selected_table(client) << "->add_column_list(" << type_name << ", \"" << name << "\", "
                      << nullable << ");\n";
        }

        ColKey col_key = table->add_column_list(type, name, nullable);
        m_array_columns.push_back(col_key);
    }

    void update_row(Peer& client)
    {
        REALM_ASSERT(!m_unstructured_columns.empty());
        size_t i = draw_int_mod(m_unstructured_columns.size());
        ColKey col_key = m_unstructured_columns[i];
        size_t num_rows = client.selected_table->size();
        size_t row_ndx = draw_int_mod(num_rows);
        ObjKey row_key = (client.selected_table->begin() + row_ndx)->get_key();
        DataType type = client.selected_table->get_column_type(col_key);
        bool nullable = client.selected_table->is_nullable(col_key);

        Obj obj = client.selected_table->get_object(row_key);

        if (type == type_Int) {
            int_fast64_t value = next_value();
            if (nullable && value % 7 == 0) {
                bool is_default = (value % 21 == 0);
                if (m_trace) {
                    std::cerr << trace_selected_table(client) << "->get_object(" << row_key << ").set_null("
                              << col_key << ", " << is_default << ");\n";
                }
                client.selected_table->get_object(row_key).set_null(col_key, is_default);
                return;
            }
            else {
                if (value % 3 == 0 && (!nullable || !obj.is_null(col_key))) {
                    if (m_trace) {
                        std::cerr << trace_selected_table(client) << "->get_object(" << row_key << ").add_int("
                                  << col_key << ", " << value << ");\n";
                    }
                    obj.add_int(col_key, value);
                }
                else {
                    bool is_default = (value % 13 == 0);
                    if (m_trace) {
                        std::cerr << trace_selected_table(client) << "->get_object(" << row_key << ").set(" << col_key
                                  << ", " << value << ", " << is_default << ");\n";
                    }
                    obj.set(col_key, value, is_default);
                }
                return;
            }
        }

        if (type == type_String) {
            int_fast64_t ival = next_value();

            if (nullable && ival % 7 == 0) {
                bool is_default = (ival % 21 == 0);
                if (m_trace) {
                    std::cerr << trace_selected_table(client) << "->get_object(" << row_key << ").set_null("
                              << col_key << ", " << is_default << ");\n";
                }
                client.selected_table->get_object(row_key).set_null(col_key, is_default);
                return;
            }
            else {
                std::stringstream ss;
                ss << ival;
                std::string value = ss.str();

                bool is_default = (ival % 13 == 0);
                if (m_trace) {
                    std::cerr << trace_selected_table(client) << "->get_object(" << row_key << ").set(" << col_key
                              << ", \"" << value << "\", " << is_default << ");\n";
                }
                obj.set(col_key, value, is_default);
                return;
            }
        }

        if (type == type_Link) {
            TableRef target_table = client.selected_table->get_link_target(col_key);
            size_t value = draw_int_mod(target_table->size() + 1);
            if (value == target_table->size()) {
                if (m_trace) {
                    std::cerr << trace_selected_table(client) << "->get_object(" << row_key << ").set_null("
                              << col_key << ";\n";
                }
                obj.set_null(col_key);
            }
            else {
                ObjKey target_key = (target_table->begin() + value)->get_key();
                if (m_trace) {
                    std::cerr << trace_selected_table(client) << "->get_object(" << row_key << ").set(" << col_key
                              << ", " << target_key << ");\n";
                }
                obj.set(col_key, target_key);
            }
            return;
        }
        REALM_ASSERT(false);
    }

    void insert_row(Peer& client)
    {
        ColKey pk_col_key = client.selected_table->get_column_key("pk");

        if (!pk_col_key) {
            if (m_trace) {
                std::cerr << trace_selected_table(client) << "->create_object();\n";
            }
            client.selected_table->create_object();
        }
        else {
            char string_buffer[2] = {0};
            bool is_string_pk = (client.selected_table->get_column_type(pk_col_key) == type_String);
            int_fast64_t pk_int = 0;
            StringData pk_string;
            if (is_string_pk) {
                string_buffer[0] = 'a' + draw_int_max(25); // "a" to "z"
                pk_string = StringData{string_buffer, 1};
            }
            else {
                pk_int = draw_int_max(10); // Low number to ensure some collisions
            }
            if (m_trace) {
                std::cerr << trace_selected_table(client) << "->create_object_with_primary_key(";
                if (is_string_pk)
                    std::cerr << "\"" << pk_string << "\"";
                else
                    std::cerr << pk_int;
                std::cerr << ");\n";
            }
            if (is_string_pk)
                client.selected_table->create_object_with_primary_key(pk_string);
            else
                client.selected_table->create_object_with_primary_key(pk_int);
        }
    }

    void move_last_row_over(Peer& client)
    {
        size_t num_rows = client.selected_table->size();
        size_t row_ndx = draw_int_mod(num_rows);
        ObjKey row_key = (client.selected_table->begin() + row_ndx)->get_key();
        if (m_trace) {
            std::cerr << trace_selected_table(client) << "->remove_object(" << row_key << ");\n";
        }
        client.selected_table->remove_object(row_key);
    }

    void set_link(Peer& client)
    {
        size_t num_links = client.selected_link_list->size();
        size_t link_ndx = draw_int_max(num_links - 1);
        auto target_table = client.selected_link_list->get_target_table();
        size_t num_target_rows = target_table->size();
        REALM_ASSERT(num_target_rows > 0);
        size_t target_row_ndx = draw_int_mod(num_target_rows);
        ObjKey target_row_key = (target_table->begin() + target_row_ndx)->get_key();
        if (m_trace) {
            std::cerr << trace_selected_link_list(client) << "->set(" << link_ndx << ", " << target_row_key << ");\n";
        }
        client.selected_link_list->set(link_ndx, target_row_key);
    }

    void insert_link(Peer& client)
    {
        size_t num_links = client.selected_link_list->size();
        size_t link_ndx = draw_int_max(num_links);
        auto target_table = client.selected_link_list->get_target_table();
        size_t num_target_rows = target_table->size();
        REALM_ASSERT(num_target_rows > 0);
        size_t target_row_ndx = draw_int_mod(num_target_rows);
        ObjKey target_row_key = (target_table->begin() + target_row_ndx)->get_key();
        if (m_trace) {
            std::cerr << trace_selected_link_list(client) << "->insert(" << link_ndx << ", " << target_row_key
                      << ");\n";
        }
        client.selected_link_list->insert(link_ndx, target_row_key);
    }

    void remove_link(Peer& client)
    {
        size_t num_links = client.selected_link_list->size();
        size_t link_ndx = draw_int_mod(num_links);
        if (m_trace) {
            std::cerr << trace_selected_link_list(client) << "->remove(" << link_ndx << ");\n";
        }
        client.selected_link_list->remove(link_ndx);
    }

    void move_link(Peer& client)
    {
        size_t num_links = client.selected_link_list->size();
        size_t from_link_ndx, to_link_ndx;
        for (;;) {
            from_link_ndx = draw_int_mod(num_links);
            to_link_ndx = draw_int_mod(num_links);
            if (from_link_ndx != to_link_ndx)
                break;
        }

        if (m_trace) {
            std::cerr << trace_selected_link_list(client) << "->move(" << from_link_ndx << ", " << to_link_ndx
                      << ");\n";
        }
        client.selected_link_list->move(from_link_ndx, to_link_ndx);
    }

    void clear_link_list(Peer& client)
    {
        if (m_trace) {
            std::cerr << trace_selected_link_list(client) << "->clear();\n";
        }
        client.selected_link_list->clear();
    }

    void array_set(Peer& client)
    {
        size_t num_elements = client.selected_array->size();
        DataType type = client.selected_array->get_table()->get_column_type(client.selected_array->get_col_key());
        size_t ndx = draw_int_max(num_elements - 1);
        if (type == type_Int) {
            int_fast64_t value = draw_int_max(1000);
            if (m_trace) {
                std::cerr << trace_selected_int_array(client) << "->set(" << ndx << ", " << value << ");\n";
            }
            static_cast<Lst<int64_t>*>(client.selected_array.get())->set(ndx, value);
        }
        else {
            StringData value = "abc";
            if (m_trace) {
                std::cerr << trace_selected_string_array(client) << "->set(" << ndx << ", \"" << value << "\");\n";
            }
            static_cast<Lst<StringData>*>(client.selected_array.get())->set(ndx, value);
        }
    }

    void array_insert(Peer& client)
    {
        size_t num_elements = client.selected_array->size();
        DataType type = client.selected_array->get_table()->get_column_type(client.selected_array->get_col_key());
        size_t ndx = draw_int_max(num_elements);
        if (type == type_Int) {
            if (m_trace) {
                std::cerr << trace_selected_int_array(client) << "->insert(" << ndx << ", 0);\n";
            }
            static_cast<Lst<int64_t>*>(client.selected_array.get())->insert(ndx, 0);
        }
        else if (type == type_String) {
            if (m_trace) {
                std::cerr << trace_selected_string_array(client) << "->insert(" << ndx << ", \"\");\n";
            }
            static_cast<Lst<StringData>*>(client.selected_array.get())->insert(ndx, "");
        }
    }

    void array_remove(Peer& client)
    {
        size_t num_elements = client.selected_array->size();
        size_t ndx = draw_int_max(num_elements - 1);
        if (m_trace) {
            std::cerr << "client_" << client.local_file_ident
                      << "->"
                         "selected_array->remove("
                      << ndx << ", " << ndx + 1 << ");\n";
        }
        client.selected_array->remove(ndx, ndx + 1);
    }

    void array_move(Peer& client)
    {
        size_t num_elements = client.selected_array->size();
        size_t from_ndx, to_ndx;
        for (;;) {
            from_ndx = draw_int_mod(num_elements);
            to_ndx = draw_int_mod(num_elements);
            if (from_ndx != to_ndx)
                break;
        }

        if (m_trace) {
            std::cerr << trace_selected_array(client) << "->move_row(" << from_ndx << ", " << to_ndx << ");\n";
        }
        client.selected_array->move(from_ndx, to_ndx);
    }

    void array_clear(Peer& client)
    {
        if (m_trace) {
            std::cerr << trace_selected_array(client) << "->clear();\n";
        }
        client.selected_array->clear();
    }

    using action_func_type = void (FuzzTester<Source>::*)(Peer&);
    using action_type = std::pair<int, action_func_type>; // First componenet is 'weight'

    Source& m_source;
    const bool m_trace;
    int_fast64_t m_current_value;
    std::vector<ColKey> m_unstructured_columns;
    std::vector<ColKey> m_link_list_columns;
    std::vector<ColKey> m_array_columns;

    int_fast64_t next_value()
    {
        return ++m_current_value;
    }

    void get_group_level_modify_actions(size_t num_classes, std::vector<action_type>& actions)
    {
        if (num_classes >= 1)
            actions.push_back(std::make_pair(rename_table_weight + 0, &FuzzTester<Source>::rename_table));
        if (true)
            actions.push_back(std::make_pair(add_table_weight + 0, &FuzzTester<Source>::add_table));
        if (num_classes >= 1)
            actions.push_back(std::make_pair(erase_table_weight + 0, &FuzzTester<Source>::erase_table));
    }

    void get_table_level_modify_actions(size_t num_classes, size_t num_cols, size_t num_rows,
                                        std::vector<action_type>& actions)
    {
        if (true)
            actions.push_back(std::make_pair(insert_column_weight + 0, &FuzzTester<Source>::insert_column));
        if (num_classes > 1)
            actions.push_back(std::make_pair(insert_link_column_weight + 0, &FuzzTester<Source>::insert_link_column));
        if (num_classes >= 1)
            actions.push_back(
                std::make_pair(insert_array_column_weight + 0, &FuzzTester<Source>::insert_array_column));
        if (num_rows >= 1 && !m_unstructured_columns.empty())
            actions.push_back(std::make_pair(update_row_weight + 0, &FuzzTester<Source>::update_row));
        if (num_cols >= 1)
            actions.push_back(std::make_pair(insert_row_weight + 0, &FuzzTester<Source>::insert_row));
        if (num_rows >= 1)
            actions.push_back(std::make_pair(erase_row_weight + 0, &FuzzTester<Source>::move_last_row_over));
    }

    void get_link_list_level_modify_actions(size_t num_links, std::vector<action_type>& actions)
    {
        if (num_links >= 1)
            actions.push_back(std::make_pair(set_link_weight + 0, &FuzzTester<Source>::set_link));
        if (true)
            actions.push_back(std::make_pair(insert_link_weight + 0, &FuzzTester<Source>::insert_link));
        if (num_links >= 1)
            actions.push_back(std::make_pair(remove_link_weight + 0, &FuzzTester<Source>::remove_link));
        if (num_links >= 2)
            actions.push_back(std::make_pair(move_link_weight + 0, &FuzzTester<Source>::move_link));
        if (true)
            actions.push_back(std::make_pair(clear_link_list_weight + 0, &FuzzTester<Source>::clear_link_list));
    }

    void get_array_level_modify_actions(size_t num_elements, std::vector<action_type>& actions)
    {
        if (num_elements >= 1)
            actions.push_back(std::make_pair(array_set_weight + 0, &FuzzTester<Source>::array_set));
        if (true)
            actions.push_back(std::make_pair(array_insert_weight + 0, &FuzzTester<Source>::array_insert));
        if (num_elements >= 1)
            actions.push_back(std::make_pair(array_remove_weight + 0, &FuzzTester<Source>::array_remove));
        // if (num_elements >= 2)
        //     actions.push_back(std::make_pair(array_move_weight+0, &FuzzTester<Source>::array_move));
        if (true)
            actions.push_back(std::make_pair(array_clear_weight + 0, &FuzzTester<Source>::array_clear));
    }

    size_t count_classes(Peer& client)
    {
        size_t count = 0;
        for (TableKey key : client.group->get_table_keys()) {
            if (client.group->get_table_name(key).begins_with("class_"))
                ++count;
        }
        return count;
    }

    TableRef get_class(Peer& client, size_t ndx)
    {
        size_t x = 0;
        for (TableKey key : client.group->get_table_keys()) {
            if (client.group->get_table_name(key).begins_with("class_")) {
                if (x == ndx)
                    return client.group->get_table(key);
                else
                    ++x;
            }
        }
        return TableRef{};
    }
};

template <class S>
void FuzzTester<S>::round(unit_test::TestContext& test_context, std::string path_add_on)
{
    m_current_value = 0;

    if (m_trace) {
        std::cerr << "auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);\n"
                  << "auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());\n";
    }
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get(), path_add_on);
    std::vector<std::unique_ptr<Peer>> clients(num_clients);
    for (int i = 0; i < num_clients; ++i) {
        using file_ident_type = Peer::file_ident_type;
        file_ident_type client_file_ident = 2 + i;
        if (m_trace) {
            std::cerr << "auto client_" << client_file_ident << " = Peer::create_client(test_context, "
                      << client_file_ident << ", changeset_dump_dir_gen.get());\n";
        }
        clients[i] = Peer::create_client(test_context, client_file_ident, changeset_dump_dir_gen.get(), path_add_on);
    }
    int pending_modifications = num_modifications_per_round;
    std::vector<int> pending_uploads(num_clients);   // One entry per client
    std::vector<int> pending_downloads(num_clients); // One entry per client
    std::vector<int> client_indexes;
    std::vector<action_type> actions;
    for (;;) {
        int client_index;
        bool can_modify = pending_modifications > 0;
        if (can_modify) {
            client_index = draw_int_mod(num_clients);
        }
        else {
            client_indexes.clear();
            for (int i = 0; i < num_clients; ++i) {
                if (pending_uploads[i] > 0 || pending_downloads[i] > 0)
                    client_indexes.push_back(i);
            }
            if (client_indexes.empty())
                break;
            client_index = client_indexes[draw_int_mod(client_indexes.size())];
        }
        Peer& client = *clients[client_index];
        if (m_source.chance(1, 2)) {
            int time;
            if (m_source.chance(1, 16)) {
                time = draw_int(-16, -1);
            }
            else {
                time = draw_int(1, 5);
            }
            if (m_trace) {
                std::cerr << "client_" << client.local_file_ident
                          << "->"
                             "history.advance_time("
                          << time << ");\n";
            }
            client.history.advance_time(time);
        }
        bool can_upload = pending_uploads[client_index] > 0;
        bool can_download = pending_downloads[client_index] > 0;
        long long accum_weights = 0;
        if (can_modify)
            accum_weights += modify_weight;
        if (can_upload)
            accum_weights += upload_weight;
        if (can_download)
            accum_weights += download_weight;
        REALM_ASSERT(accum_weights > 0);
        long long rest_weight = draw_int_mod(accum_weights);
        if (can_modify) {
            if (rest_weight < modify_weight) {
                actions.clear();
                if (m_trace) {
                    std::cerr << "client_" << client.local_file_ident
                              << "->"
                                 "start_transaction();\n";
                }
                client.start_transaction();
                size_t num_classes = count_classes(client);
                bool group_level =
                    num_classes == 0 || draw_float<double>() >= group_to_table_level_transition_chance();
                if (group_level) {
                    get_group_level_modify_actions(num_classes, actions);
                }
                else {
                    // Draw a table, but not the special "pk" table.
                    TableRef table = get_class(client, draw_int_mod<size_t>(num_classes));

                    if (m_trace && table != client.selected_table) {
                        std::cerr << trace_selected_table(client) << " = " << trace_client(client)
                                  << "->"
                                     "group->get_table(\""
                                  << table->get_name() << "\");\n";
                    }
                    client.selected_table = table;
                    m_unstructured_columns.clear();
                    m_link_list_columns.clear();
                    m_array_columns.clear();
                    size_t n = table->get_column_count();
                    for (ColKey key : table->get_column_keys()) {
                        if (table->get_column_name(key) == "pk")
                            continue; // don't make normal modifications to primary keys
                        DataType type = table->get_column_type(key);
                        if (type == type_LinkList) {
                            // Only consider LinkList columns that target tables
                            // with rows in them.
                            if (table->get_link_target(key)->size() != 0) {
                                m_link_list_columns.push_back(key);
                            }
                        }
                        else if (table->is_list(key)) {
                            m_array_columns.push_back(key);
                        }
                        else {
                            m_unstructured_columns.push_back(key);
                        }
                    }
                    size_t num_cols = table->get_column_count();
                    size_t num_rows = table->size();
                    bool table_level = num_rows == 0 || (m_link_list_columns.empty() && m_array_columns.empty()) ||
                                       draw_float<double>() >= table_to_array_level_transition_chance();
                    if (table_level) {
                        get_table_level_modify_actions(num_classes, num_cols, num_rows, actions);
                    }
                    else {
                        REALM_ASSERT(n > 0); // No columns implies no rows
                        size_t i = draw_int_mod<size_t>(m_link_list_columns.size() + m_array_columns.size());
                        ColKey col_key;
                        bool is_array;
                        if (i >= m_link_list_columns.size()) {
                            col_key = m_array_columns[i - m_link_list_columns.size()];
                            is_array = true;
                        }
                        else {
                            col_key = m_link_list_columns[i];
                            is_array = false;
                        }

                        size_t row_ndx = draw_int_mod<size_t>(num_rows);
                        ObjKey row_key = (table->begin() + row_ndx)->get_key();

                        if (is_array) {
                            DataType type = table->get_column_type(col_key);
                            if (type == type_Int) {
                                LstPtr<int64_t> array = table->get_object(row_key).get_list_ptr<int64_t>(col_key);
                                if (m_trace) {
                                    std::cerr << trace_selected_array(client) << " = " << trace_selected_table(client)
                                              << "->get_object(" << row_key << ").get_list_ptr<int64_t>(" << col_key
                                              << ");\n";
                                }
                                client.selected_array = std::move(array);
                            }
                            else if (type == type_String) {
                                LstPtr<StringData> array =
                                    table->get_object(row_key).get_list_ptr<StringData>(col_key);
                                if (m_trace) {
                                    std::cerr << trace_selected_array(client) << " = " << trace_selected_table(client)
                                              << "->get_object(" << row_key << ").get_list_ptr<StringData>("
                                              << col_key << ");\n";
                                }
                                client.selected_array = std::move(array);
                            }
                            else {
                                REALM_TERMINATE("Unsupported list type.");
                            }
                            size_t num_elements = client.selected_array->size();
                            get_array_level_modify_actions(num_elements, actions);
                        }
                        else {
                            LnkLstPtr link_list = table->get_object(row_key).get_linklist_ptr(col_key);
                            if (m_trace) {
                                std::cerr << trace_selected_link_list(client) << " = " << trace_selected_table(client)
                                          << "->get_object(" << row_key << ").get_linklist_ptr(" << col_key << ");\n";
                            }
                            client.selected_link_list = std::move(link_list);
                            size_t num_links = link_list->size();
                            get_link_list_level_modify_actions(num_links, actions);
                        }
                    }
                }
                long long accum_weights_2 = 0;
                for (int i = 0; i < int(actions.size()); ++i)
                    accum_weights_2 += actions[i].first;
                long long rest_weight_2 = draw_int_mod(accum_weights_2);
                action_func_type action_func = 0;
                for (int i = 0; i < int(actions.size()); ++i) {
                    int action_weight = actions[i].first;
                    if (rest_weight_2 < action_weight) {
                        action_func = actions[i].second;
                        break;
                    }
                    rest_weight_2 -= action_weight;
                }
                REALM_ASSERT(action_func);
                (this->*action_func)(client);
                if (m_trace) {
                    std::cerr << "client_" << client.local_file_ident
                              << "->"
                                 "commit();";
                }
                auto produced_version = client.commit();
                if (m_trace) {
                    std::cerr << " // changeset " << produced_version << '\n';
                }
                ++pending_uploads[client_index];
                --pending_modifications;
                continue;
            }
            rest_weight -= modify_weight;
        }
        if (can_upload) {
            if (rest_weight < upload_weight) {
                if (m_trace) {
                    std::cerr << "server->integrate_next_changeset_from"
                                 "(*client_"
                              << client.local_file_ident << ");\n";
                }
                bool identical_initial_schema_creating_transaction = server->integrate_next_changeset_from(client);
                --pending_uploads[client_index];
                for (int i = 0; i < num_clients; ++i) {
                    if (i != client_index)
                        ++pending_downloads[i];
                }
                if (m_trace && identical_initial_schema_creating_transaction) {
                    std::cerr << "// Special handling of identical initial "
                                 "schema-creating transaction occured\n";
                }
                continue;
            }
            rest_weight -= upload_weight;
        }
        if (can_download) {
            if (rest_weight < download_weight) {
                if (m_trace) {
                    std::cerr << "client_" << client.local_file_ident
                              << "->"
                                 "integrate_next_changeset_from(*server);\n";
                }
                client.integrate_next_changeset_from(*server);
                --pending_downloads[client_index];
                continue;
            }
            rest_weight -= download_weight;
        }
        REALM_ASSERT(false);
    }

    ReadTransaction rt_0(server->shared_group);
    for (int i = 0; i < num_clients; ++i) {
        ReadTransaction rt_1(clients[i]->shared_group);
        bool same = CHECK(compare_groups(rt_0, rt_1));
        if (!same) {
            std::cout << "Server" << std::endl;
            rt_0.get_group().to_json(std::cout);
            std::cout << "Client_" << clients[i]->local_file_ident << std::endl;
            rt_1.get_group().to_json(std::cout);
        }
        CHECK(same);
    }
}


} // namespace test_util
} // namespace realm

#endif // REALM_TEST_FUZZ_TESTER_HPP
