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
#include <fstream>

class Group;
class SharedRealm;
class FuzzObject {
    // list of realm operations we support in our fuzzer
public:
    void create_table(Group& group, std::ostream* log);
    void remove_table(Group& group, std::ostream* log);
    void clear_table(Group& group, std::ostream* log, State& s);
    void create_object(Group& group, std::ostream* log, State& s);
    void add_column(Group& group, std::ostream* log, State& s);
    void remove_column(Group& group, std::ostream* log, State& s);
    void rename_column(Group& group, std::ostream* log, State& s);
    void add_search_index(Group& group, std::ostream* log, State& s);
    void remove_search_index(Group& group, std::ostream* log, State& s);
    void add_column_link(Group& group, std::ostream* log, State& s);
    void add_column_link_list(Group& group, std::ostream* log, State& s);
    void set_obj(Group& group, std::ostream* log, State& s);
    void remove_obj(Group& group, std::ostream* log, State& s);
    void remove_recursive(Group& group, std::ostream* log, State& s);
    void enumerate_column(Group& group, std::ostream* log, State& s);
    void get_all_column_names(Group& group);
    void commit(SharedRealm shared_realm, std::ostream* log);
    void rollback(SharedRealm shared_realm, Group& group, std::ostream* log);
    void advance(Group& group, std::ostream* log);
    void close_and_reopen(SharedRealm shared_realm, std::ostream* log, Realm::Config& config);
    void create_table_view(Group& group, std::ostream* log, State& s, std::vector<TableView>& table_views);
    void check_null(Group& group, std::ostream* log, State& s);
    void async_write(SharedRealm shared_realm, std::ostream* log);
    void async_cancel(SharedRealm shared_realm, Group& group, std::ostream* log, State& s);

private:
    DataType get_type(unsigned char c)
};