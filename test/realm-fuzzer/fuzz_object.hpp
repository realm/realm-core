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
#ifndef FUZZ_OBJECT_HPP
#define FUZZ_OBJECT_HPP

#include "util.hpp"
#include <realm/table_view.hpp>
#include <realm/object-store/shared_realm.hpp>
#include <vector>
#include <fstream>

struct State;
class FuzzLog;
class FuzzObject {
    // list of realm operations we support in our fuzzer
public:
    void create_table(realm::Group& group, FuzzLog& log);
    void remove_table(realm::Group& group, FuzzLog& lo, State& sg);
    void clear_table(realm::Group& group, FuzzLog& log, State& s);
    void create_object(realm::Group& group, FuzzLog& log, State& s);
    void add_column(realm::Group& group, FuzzLog& log, State& s);
    void remove_column(realm::Group& group, FuzzLog& log, State& s);
    void rename_column(realm::Group& group, FuzzLog& log, State& s);
    void add_search_index(realm::Group& group, FuzzLog& log, State& s);
    void remove_search_index(realm::Group& group, FuzzLog& log, State& s);
    void add_column_link(realm::Group& group, FuzzLog& log, State& s);
    void add_column_link_list(realm::Group& group, FuzzLog& log, State& s);
    void set_obj(realm::Group& group, FuzzLog& log, State& s);
    void remove_obj(realm::Group& group, FuzzLog& log, State& s);
    void remove_recursive(realm::Group& group, FuzzLog& log, State& s);
    void enumerate_column(realm::Group& group, FuzzLog& log, State& s);
    void get_all_column_names(realm::Group& group, FuzzLog& log);
    void commit(realm::SharedRealm shared_realm, FuzzLog& log);
    void rollback(realm::SharedRealm shared_realm, realm::Group& group, FuzzLog& log);
    void advance(realm::SharedRealm shared_realm, FuzzLog& log);
    void close_and_reopen(realm::SharedRealm& shared_realm, FuzzLog& log, const realm::Realm::Config& config);
    void create_table_view(realm::Group& group, FuzzLog& log, State& s, std::vector<realm::TableView>& table_views);
    void check_null(realm::Group& group, FuzzLog& log, State& s);

    const char* get_encryption_key() const;
    std::string get_current_time_stamp() const;
    unsigned char get_next_token(State&) const;

private:
    realm::DataType get_type(unsigned char c) const;
    int64_t get_int64(State& s) const;
    int32_t get_int32(State& s) const;
    std::string create_string(size_t length) const;
    std::pair<int64_t, int32_t> get_timestamp_values(State& s) const;
    std::string create_column_name(realm::DataType t);
    std::string create_table_name();

    int m_table_index = 0;
    int m_column_index = 0;
};
#endif