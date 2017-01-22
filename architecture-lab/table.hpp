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

#ifndef __TABLE_HPP__
#define __TABLE_HPP__

#include <stdexcept>

#include "memory.hpp"
#include "cuckoo.hpp"
#include "uids.hpp"
#include "object.hpp"

struct NotFound {};

struct _Table {
    _Cuckoo cuckoo;
    uint16_t num_fields;
    char typeinfo[16]; // for now!
    static Ref<_Table> commit(Memory& mem, Ref<_Table> from);
    static Ref<_Table> cow(Memory& mem, Ref<_Table> from);
    void copied_to_file(Memory& mem);
    void copied_from_file(Memory& mem);

    // insert a default-initialized entry, top must have been cow'ed first
    void insert(Memory& mem, uint64_t key);

    void get_cluster(Memory& mem, uint64_t key, Object& o);
    void change_cluster(Memory& mem, uint64_t key, Object& o);

    bool find(Memory& mem, uint64_t key);

    void init(const char* typeinfo);

    bool first_access(Memory& mem, ObjectIterator& o);

    template<typename T>
    inline void check_field(int col) const;
};

template<typename T>
inline void _Table::check_field(int col) const
{
    throw std::runtime_error("Unsupported field type");
    //static_assert(false, "Unsupported field type");
}


template<>
inline void _Table::check_field<uint64_t>(int col) const {
    if (typeinfo[col] != 'u')
        throw std::runtime_error("Wrong field type");
}

template<>
inline void _Table::check_field<int64_t>(int col) const {
    if (typeinfo[col] != 'i')
        throw std::runtime_error("Wrong field type");
}

template<>
inline void _Table::check_field<float>(int col) const {
    if (typeinfo[col] != 'f')
        throw std::runtime_error("Wrong field type");
}

template<>
inline void _Table::check_field<double>(int col) const {
    if (typeinfo[col] != 'd')
        throw std::runtime_error("Wrong field type");
}

template<>
inline void _Table::check_field<Table>(int col) const {
    if (typeinfo[col] != 't')
        throw std::runtime_error("Wrong field type");
}

template<>
inline void _Table::check_field<Row>(int col) const {
    if (typeinfo[col] != 'r')
        throw std::runtime_error("Wrong field type");
}

template<>
inline void _Table::check_field<String>(int col) const {
    if (typeinfo[col] != 's')
        throw std::runtime_error("Wrong field type");
}

template<>
inline void _Table::check_field<List<uint64_t>>(int col) const {
    if (typeinfo[col] != 'U')
        throw std::runtime_error("Wrong field type");
}

template<>
inline void _Table::check_field<List<int64_t>>(int col) const {
    if (typeinfo[col] != 'I')
        throw std::runtime_error("Wrong field type");
}

template<>
inline void _Table::check_field<List<float>>(int col) const {
    if (typeinfo[col] != 'F')
        throw std::runtime_error("Wrong field type");
}

template<>
inline void _Table::check_field<List<double>>(int col) const {
    if (typeinfo[col] != 'D')
        throw std::runtime_error("Wrong field type");
}

template<>
inline void _Table::check_field<List<Table>>(int col) const {
    if (typeinfo[col] != 'T')
        throw std::runtime_error("Wrong field type");
}

template<>
inline void _Table::check_field<List<Row>>(int col) const {
    if (typeinfo[col] != 'R')
        throw std::runtime_error("Wrong field type");
}


#endif
