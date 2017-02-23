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

#ifndef __OBJECT_HPP__
#define __OBJECT_HPP__

#include <string>

#include "uids.hpp"

// fwd decls
class Memory;
struct SnapshotImpl;
struct _Cluster;
struct _Table;


// type used to indicate that a field is a list of T
template<typename T> struct List {
};

// type used to indicate that a field is a string
struct String {
};

// helper for accessing a list
template<typename T>
struct ListAccessor;

struct Object {

    // singular entries
    template<typename T>
    T operator()(Field<T> f);

    template<typename T>
    void set(Field<T> f, T value);

    // overload for strings
    void set(Field<String> f, std::string value);
    std::string operator()(Field<String> f);

    // lists
    template<typename T>
    ListAccessor<T> operator()(Field<List<T>> f);

    // implementation:
    SnapshotImpl* ss;
    uint64_t versioning_count;
    Table t;
    Row r;
    _Table* table;
    _Cluster* cluster;
    uint8_t index;
    uint8_t size;
    bool is_writable;
};

struct TreeLeaf;

struct ObjectIterator {

    bool next_access();

    Object o;
    uint64_t tree_index;
    TreeLeaf* leaf;
};

template<typename T>
struct ListAccessor {
    Object o;
    Field<List<T>> f;

    uint64_t get_size();

    void set_size(uint64_t new_size);

    T rd(uint64_t index);

    void wr(uint64_t index, T value);

    // not sure about this one:
    template<typename TFunc>
    void for_each(uint64_t first, uint64_t limit, TFunc func);
};

// specializations for Table and Row fields
template<>
struct ListAccessor<Table> {
    ListAccessor<uint64_t> list;
    uint64_t get_size() { return list.get_size(); }
    void set_size(uint64_t new_size) { list.set_size(new_size); }
    Table rd(uint64_t index) { Table t; t.key = list.rd(index); return t; }
    void wr(uint64_t index, Table value) { list.wr(index, value.key); }
};

template<>
struct ListAccessor<Row> {
    ListAccessor<uint64_t> list;
    uint64_t get_size() { return list.get_size(); }
    void set_size(uint64_t new_size) { list.set_size(new_size); }
    Row rd(uint64_t index) { Row t; t.key = list.rd(index); return t; }
    void wr(uint64_t index, Row value) { list.wr(index, value.key); }
};


#endif
