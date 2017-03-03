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

#include "snapshot.hpp"
#include "memory.hpp"
#include "direct_map.hpp"
#include "table.hpp"


// in-db representation:
struct _Snapshot {
    _DirectMap<_DirectMapLeaf<Ref<_Table>>> tables;
    uint64_t version;
    Table table_directory;
    Table table_layouts;
    static Ref<_Snapshot> commit(Memory& mem, Ref<_Snapshot> from);
    static Ref<_Snapshot> cow(Memory& mem, Ref<_Snapshot> from);
    void init();
};

class SnapshotImpl : public Snapshot {

public:
    SnapshotImpl(Memory& mem, Ref<_Snapshot> top_ref, bool writable);

    // manipulate tables:
    Table create_table(const char* typeinfo);
    Table get_table_dir();
    Table get_layout_dir();

    // fields and their definitions
    template<typename T>
    Field<T> get_field(Table t, int number) const; // add field name instead of number

    // manipulate rows
    void insert(Table t, Row r);
    bool exists(Table t, Row r) const;

    // throws if table or row does not exist
    Object get(Table t, Row r) const;
    Object change(Table t, Row r);

    bool first_access(Table t, ObjectIterator&, uint64_t start_index) const;
    uint64_t get_universe_size(Table t) const;

    // statistics
    void print_stat(std::ostream&);

    // hooks for control, used by DbImpl.
    Ref<_Snapshot> commit();
    void cow();
    virtual ~SnapshotImpl() {};

    // hooks used by Object
    Memory& refresh(Object* o) const;
    Memory& change(Object* o);
private:
    Memory& mem;
    Ref<_Snapshot> m_top;
    _Snapshot* m_top_ptr;
    uint64_t versioning_counter;
    bool is_writable;
};
