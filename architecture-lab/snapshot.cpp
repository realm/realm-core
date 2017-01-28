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

#include <cassert>

#include "snapshot.hpp"
#include "snapshot_impl.hpp"
#include "table.hpp"
#include "memory.hpp"

SnapshotImpl::SnapshotImpl(Memory& mem, Ref<_Snapshot> top_ref, bool writable) : mem(mem) {
    m_top = top_ref;
    m_top_ptr = mem.txl(top_ref);
    versioning_counter = m_top_ptr->version;
    is_writable = writable;
}

void SnapshotImpl::print_stat(std::ostream& out) {
    out << "Footprint: " << mem.get_footprint() 
        << "  Recycled: " << mem.get_recycled() 
        << "  Freed: " << mem.get_freed() 
        << std::endl;
    uint64_t use = mem.get_footprint() - (mem.get_freed() - mem.get_recycled());
    out << "Freelists (heap frag): " << mem.get_freed() - mem.get_recycled()
        << "  In actual use: " << use 
        << std::endl;
}

void SnapshotImpl::cow() {
    m_top = _Snapshot::cow(mem, m_top);
    m_top_ptr = mem.txl(m_top);
}

Ref<_Snapshot> SnapshotImpl::commit() {
    assert(is_writable);
    versioning_counter++;
    m_top_ptr->version = versioning_counter;
    is_writable = false;
    return _Snapshot::commit(mem, m_top);
}

void _Snapshot::init() {
    tables.init(64);
}

Table SnapshotImpl::create_table(const char* name, const char* typeinfo) {
    // FIXME
    return create_anon_table(typeinfo);
}

Table SnapshotImpl::create_anon_table(const char* typeinfo) {
    assert(mem.is_writable(m_top));
    uint64_t key = m_top_ptr->tables.insert(mem);
    Ref<_Table>* table = m_top_ptr->tables.get_ref(mem, key);
    *table = _Table::create(mem, typeinfo);
    versioning_counter += 2;
    return {key};
}

Table SnapshotImpl::get_table(const char* name) const {
    return {0};
}

template<typename T>
Field<T> Snapshot::get_field(Table t, int number) const {
    const SnapshotImpl* real_this = static_cast<const SnapshotImpl*>(this);
    return real_this->get_field<T>(t, number);
}

// explicit instantiation
template Field<uint64_t> Snapshot::get_field<uint64_t>(Table, int) const;
template Field<int64_t> Snapshot::get_field<int64_t>(Table, int) const;
template Field<float> Snapshot::get_field<float>(Table, int) const;
template Field<double> Snapshot::get_field<double>(Table, int) const;
template Field<Table> Snapshot::get_field<Table>(Table, int) const;
template Field<Row> Snapshot::get_field<Row>(Table, int) const;
template Field<String> Snapshot::get_field<String>(Table, int) const;

template Field<List<uint64_t>> Snapshot::get_field<List<uint64_t>>(Table, int) const;
template Field<List<int64_t>> Snapshot::get_field<List<int64_t>>(Table, int) const;
template Field<List<float>> Snapshot::get_field<List<float>>(Table, int) const;
template Field<List<double>> Snapshot::get_field<List<double>>(Table, int) const;
template Field<List<Table>> Snapshot::get_field<List<Table>>(Table, int) const;

template<typename T>
Field<T> SnapshotImpl::get_field(Table t, int number) const {
    Ref<_Table> table = m_top_ptr->tables.get(mem, t.key);
    _Table* table_ptr = mem.txl(table);
    return table_ptr->check_field<T>(number);
}

void SnapshotImpl::insert(Table t, Row r) {
    assert(mem.is_writable(m_top));
    m_top_ptr->tables.cow_path(mem, t.key);
    Ref<_Table>* table = m_top_ptr->tables.get_ref(mem, t.key);
    *table = _Table::cow(mem, *table);
    _Table* table_ptr = mem.txl(*table);
    table_ptr->insert(mem, r.key);
}

bool SnapshotImpl::exists(Table t, Row r) const {
    Ref<_Table> table = m_top_ptr->tables.get(mem, t.key);
    _Table* table_ptr = mem.txl(table);
    return table_ptr->find(mem, r.key);
}

Object SnapshotImpl::get(Table t, Row r) const {
    Object res;
    res.ss = const_cast<SnapshotImpl*>(this);
    res.versioning_count = versioning_counter;
    res.t = t;
    res.r = r;
    Ref<_Table> table = m_top_ptr->tables.get(mem, t.key);
    _Table* table_ptr = mem.txl(table);
    res.table = table_ptr;
    table_ptr->get_cluster(mem, r.key, res);
    return res;
}

Object SnapshotImpl::change(Table t, Row r) {
    Object res;
    res.ss = this;
    assert(is_writable);
    versioning_counter++;
    res.versioning_count = versioning_counter;
    res.t = t;
    res.r = r;
    assert(mem.is_writable(m_top));
    m_top_ptr->tables.cow_path(mem, t.key);
    Ref<_Table>* table = m_top_ptr->tables.get_ref(mem, t.key);
    *table = _Table::cow(mem, *table);
    _Table* table_ptr = mem.txl(*table);
    res.table = table_ptr;
    table_ptr->change_cluster(mem, r.key, res);
    return res;
}

Memory& SnapshotImpl::change(Object* o) {
    if (!is_writable) {
        throw std::runtime_error("Attempt to change a const Snapshot");
    }
    if (!o->is_writable) {
        *o = change(o->t, o->r);
    }
    else if (o->versioning_count != versioning_counter) {
        // coming here, we know that the object was already writable,
        // so we do not need to trigger copy-on-write on it, just update it.
        *o = get(o->t, o->r);
        assert(o->is_writable);
    }
    return mem;
}

Memory& SnapshotImpl::refresh(Object* o) const {
    if (o->versioning_count != versioning_counter) {
        *o = get(o->t, o->r);
    }
    return mem;
}

Ref<_Snapshot> _Snapshot::cow(Memory& mem, Ref<_Snapshot> from) {
    if (!mem.is_writable(from)) {
        _Snapshot* to_ptr;
        Ref<_Snapshot> to = mem.alloc<_Snapshot>(to_ptr);
        _Snapshot* from_ptr = mem.txl(from);
        *to_ptr = *from_ptr;
        mem.free(from);
        return to;
    }
    return from;
}

Ref<_Snapshot> _Snapshot::commit(Memory& mem, Ref<_Snapshot> from) {
    if (mem.is_writable(from)) {
        _Snapshot* to_ptr;
        Ref<_Snapshot> to = mem.alloc_in_file<_Snapshot>(to_ptr);
        _Snapshot* from_ptr = mem.txl(from);
        *to_ptr = *from_ptr;
        mem.free(from);
        to_ptr->tables.copied_to_file(mem);
        return to;
    }
    return from;
}

bool SnapshotImpl::first_access(Table t, ObjectIterator& oi, uint64_t start_index) const {
    oi.o.ss = const_cast<SnapshotImpl*>(this);
    oi.o.versioning_count = versioning_counter;
    oi.o.t = t;
    oi.o.r = {0}; // set by 'first_access'
    Ref<_Table> table = m_top_ptr->tables.get(mem, t.key);
    _Table* table_ptr = mem.txl(table);
    oi.o.table = table_ptr;
    oi.tree_index = start_index;
    bool ok = table_ptr->first_access(mem, oi);
    return ok;
}

uint64_t SnapshotImpl::get_universe_size(Table t) const {
    Ref<_Table> table = m_top_ptr->tables.get(mem, t.key);
    _Table* table_ptr = mem.txl(table);
    return table_ptr->cuckoo.primary_tree.mask+1;
}
