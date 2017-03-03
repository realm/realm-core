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


struct _FieldInfo {
    uint64_t key; // [48 bit random + 16 bit index leading here]
    // possibly add: uint16_t index (if we choose to compress clusters' top array)
    char type;
};


struct _Table {
    _Cuckoo cuckoo;
    uint16_t num_fields;
    _FieldInfo fields[1]; // actually [num_fields], but...

    static Ref<_Table> commit(Memory& mem, Ref<_Table> from);
    static Ref<_Table> cow(Memory& mem, Ref<_Table> from);
    void copied_to_file(Memory& mem);
    void copied_from_file(Memory& mem);
    static size_t get_allocation_size(uint16_t num_fields) { 
        return sizeof(_Table) + (num_fields-1) * sizeof(_FieldInfo);
    }

    // insert a default-initialized entry, top must have been cow'ed first
    void insert(Memory& mem, uint64_t key);

    void get_cluster(Memory& mem, uint64_t key, Object& o);
    void change_cluster(Memory& mem, uint64_t key, Object& o);

    bool find(Memory& mem, uint64_t key);

    static Ref<_Table> create(Memory& mem, const char* typeinfo);

    bool first_access(Memory& mem, ObjectIterator& o);

    template<typename T>
    Field<T> check_field(int col) const;
};

template<typename T> inline char get_type_encoding() {
    throw std::runtime_error("Unsupported field type");
}

template<> inline char get_type_encoding<uint64_t>() { return 'u'; }
template<> inline char get_type_encoding<int64_t>() { return 'i'; }
template<> inline char get_type_encoding<float>() { return 'f'; }
template<> inline char get_type_encoding<double>() { return 'd'; }
template<> inline char get_type_encoding<Table>() { return 't'; }
template<> inline char get_type_encoding<Row>() { return 'r'; }
template<> inline char get_type_encoding<String>() { return 's'; }

template<> inline char get_type_encoding<List<uint64_t>>() { return 'U'; }
template<> inline char get_type_encoding<List<int64_t>>() { return 'I'; }
template<> inline char get_type_encoding<List<float>>() { return 'F'; }
template<> inline char get_type_encoding<List<double>>() { return 'D'; }
template<> inline char get_type_encoding<List<Table>>() { return 'T'; }
template<> inline char get_type_encoding<List<Row>>() { return 'R'; }

template<typename T>
inline Field<T> _Table::check_field(int col) const
{
    if (col >= num_fields)
        throw std::runtime_error("Request for undefined field number");
    if (fields[col].type != get_type_encoding<T>())
        throw std::runtime_error("Wrong field type");
    return { fields[col].key };
}


#endif
