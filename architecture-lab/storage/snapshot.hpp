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

#ifndef __SNAPSHOT_HPP__
#define __SNAPSHOT_HPP__

#include <ostream>
#include "uids.hpp"
#include "object.hpp"

class Snapshot {
public:
    // low level table/table layout interface:
    virtual Table create_table(const char* typeinfo) = 0;
    virtual Table get_table_dir() = 0;
    virtual Table get_layout_dir() = 0;

    // fields and their definitions:
    // throws if the field type does not match the table runtime typeinfo for that field
    template<typename T>
    Field<T> get_field(Table t, int number) const; //  FIXME: field name instead of number

    // manipulate rows

    // throws if row already exists
    // thwows if table does not exist
    virtual void insert(Table t, Row r) = 0;
    virtual bool exists(Table t, Row r) const = 0;

    // throws if table or row does not exist
    virtual Object get(Table t, Row r) const = 0;
    virtual Object change(Table t, Row r) = 0;

    // setup iterator
    // Returns true if valid access found.
    virtual bool first_access(Table t, ObjectIterator&, uint64_t first_index) const = 0;
    virtual uint64_t get_universe_size(Table t) const = 0;

    template<typename TFunc>
    void for_each_partition(int partitions, int partition_number, Table t, TFunc func) const;

    template<typename TFunc>
    void for_each(Table t, TFunc func) const { for_each_partition(1, 0, t, func); }

    // statistics
    virtual void print_stat(std::ostream&) = 0;
protected:
    virtual ~Snapshot() {};
};


template<typename TFunc>
void Snapshot::for_each_partition(int partitions, int partition_number, Table t, TFunc func) const {
    ObjectIterator oi;
    uint64_t limit = get_universe_size(t);
    uint64_t start_index;
    uint64_t partition_size = (limit / partitions) & ~0x0FFUL;
    if (partition_size != 0) {
        start_index = partition_size * partition_number;
        if (partition_number < partitions - 1) { //not the last partition
            limit = start_index + partition_size;
        }
    }
    else {
        if (partition_number != 0) return; // we've only got one partition
        start_index = 0;
    }
    bool work_to_do = (start_index < limit) && first_access(t, oi, start_index);
    while (work_to_do) {
        func(oi.o);
        work_to_do = oi.next_access();
        if (!work_to_do) {
            uint64_t next_index = oi.tree_index + 256;
            work_to_do = (next_index < limit) && first_access(t, oi, next_index);
        }
    }
}


#endif
