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

#include <cstdint>
#include <cassert>
#include <cstring>

#include "table.hpp"
#include "cuckoo.hpp"
#include "payload.hpp"
#include "array.hpp"
#include "snapshot_impl.hpp"

using _String = _List<char>;

union ArrayReps {
    _Array<uint64_t> as_u;
    _Array<int64_t> as_i;
    _Array<float> as_f;
    _Array<double> as_d;
    _Array<_String> as_s;
    _Array<_List<uint64_t>> as_U;
    _Array<_List<int64_t>> as_I;
    _Array<_List<float>> as_F;
    _Array<_List<double>> as_D;
};

template<typename T> T get(Memory&, ArrayReps&, int);
template<> uint64_t get<uint64_t>(Memory& mem, ArrayReps& ar, int index) { 
    return ar.as_u.get(mem, index); 
}

template<> int64_t get<int64_t>(Memory& mem, ArrayReps& ar, int index) { 
    return ar.as_i.get(mem, index); 
}

template<> float get<float>(Memory& mem, ArrayReps& ar, int index) { 
    return ar.as_f.get(mem, index); 
}

template<> double get<double>(Memory& mem, ArrayReps& ar, int index) { 
    return ar.as_d.get(mem, index); 
}

template<> _String get<_String>(Memory& mem, ArrayReps& ar, int index) { 
    return ar.as_s.get(mem, index); 
}

template<> _List<uint64_t> get<_List<uint64_t>>(Memory& mem, ArrayReps& ar, int index) { 
    return ar.as_U.get(mem, index); 
}

template<> _List<int64_t> get<_List<int64_t>>(Memory& mem, ArrayReps& ar, int index) { 
    return ar.as_I.get(mem, index); 
}

template<> _List<float> get<_List<float>>(Memory& mem, ArrayReps& ar, int index) { 
    return ar.as_F.get(mem, index); 
}

template<> _List<double> get<_List<double>>(Memory& mem, ArrayReps& ar, int index) { 
    return ar.as_D.get(mem, index); 
}

template<typename T> void set(Memory&, ArrayReps&, int, T, int);
template<> void set(Memory& mem, ArrayReps& ar, int index, uint64_t val, int size) { 
    ar.as_u.set(mem, index, val, size); 
}

template<> void set(Memory& mem, ArrayReps& ar, int index, int64_t val, int size) { 
    ar.as_i.set(mem, index, val, size); 
}

template<> void set(Memory& mem, ArrayReps& ar, int index, float val, int size) { 
    ar.as_f.set(mem, index, val, size); 
}

template<> void set(Memory& mem, ArrayReps& ar, int index, double val, int size) { 
    ar.as_d.set(mem, index, val, size); 
}

template<> void set(Memory& mem, ArrayReps& ar, int index, _String val, int size) { 
    ar.as_s.set(mem, index, val, size); 
}

template<> void set(Memory& mem, ArrayReps& ar, int index, _List<uint64_t> val, int size) { 
    ar.as_U.set(mem, index, val, size); 
}

template<> void set(Memory& mem, ArrayReps& ar, int index, _List<int64_t> val, int size) { 
    ar.as_I.set(mem, index, val, size); 
}

template<> void set(Memory& mem, ArrayReps& ar, int index, _List<float> val, int size) { 
    ar.as_F.set(mem, index, val, size); 
}

template<> void set(Memory& mem, ArrayReps& ar, int index, _List<double> val, int size) { 
    ar.as_D.set(mem, index, val, size); 
}

union Reps {
    uint64_t as_u;
    int64_t as_i;
    float as_f;
    double as_d;
    _String as_s;
    _List<uint64_t> as_U;
    _List<int64_t> as_I;
    _List<float> as_F;
    _List<double> as_D;
    Reps() { as_u = 0; }
};

struct _Cluster { ArrayReps entries[1]; };

struct ClusterMgr : public PayloadMgr {
    ClusterMgr(Memory& mem, int num_fields, const _FieldInfo* field_info);
    ~ClusterMgr();
    virtual void cow(Ref<DynType>& payload, int old_capacity, int new_capacity);
    virtual void free(Ref<DynType> payload, int capacity);
    virtual void read_internalbuffer(Ref<DynType> payload, int from);
    virtual void write_internalbuffer(Ref<DynType>& payload, int to, int capacity);
    virtual void init_internalbuffer();
    virtual void swap_internalbuffer(Ref<DynType>& payload, int index, int capacity);
    virtual Ref<DynType> commit(Ref<DynType> from);
    Memory& mem;
    int num_fields;
    const _FieldInfo* field_info;
    Reps* values;
    Reps value_buffer[64];
};

ClusterMgr::ClusterMgr(Memory& mem, int num_fields, const _FieldInfo* field_info)
    : mem(mem), num_fields(num_fields), field_info(field_info) {
    if (num_fields <= 64) {
        values = value_buffer;
    }
    else {
        values = new Reps[num_fields];
    }
}

ClusterMgr::~ClusterMgr() {
    if (num_fields > 64) {
        delete[] values;
    }
}


void ClusterMgr::init_internalbuffer() {
    for (int j = 0; j < num_fields; ++j) values[j].as_u = 0;
}

void ClusterMgr::free(Ref<DynType> payload, int capacity) {
    if (!is_null(payload)) {
        Ref<_Cluster> cluster = payload.as<_Cluster>();
        _Cluster* cluster_ptr = mem.txl(cluster);
        for (int j=0; j < num_fields; j++) {
            switch(field_info[j].type) {
                case 't': 
                case 'r':
                case 'u': cluster_ptr->entries[j].as_u.free(mem); break;
                case 'i': cluster_ptr->entries[j].as_i.free(mem); break;
                case 'f': cluster_ptr->entries[j].as_f.free(mem); break;
                case 'd': cluster_ptr->entries[j].as_d.free(mem); break;
                case 's': cluster_ptr->entries[j].as_s.free(mem); break;
                case 'T':
                case 'R':
                case 'U': cluster_ptr->entries[j].as_U.free(mem); break;
                case 'I': cluster_ptr->entries[j].as_I.free(mem); break;
                case 'F': cluster_ptr->entries[j].as_F.free(mem); break;
                case 'D': cluster_ptr->entries[j].as_D.free(mem); break;
                default: throw std::runtime_error("Internal error, unsupported type specifier");
            }
        }
        mem.free(payload, num_fields * sizeof(uint64_t));
    }
}

// FIXME: if old_capacity/new_capacity is unused here, then get rid of them!
void ClusterMgr::cow(Ref<DynType>& payload, int old_capacity, int new_capacity) {
    if (!mem.is_writable(payload) || new_capacity != old_capacity) {
        _Cluster* payload_ptr;
        assert(new_capacity != 0);
        assert(old_capacity <= 256);
        Ref<_Cluster> new_payload = mem.alloc<_Cluster>(payload_ptr, num_fields * sizeof(uint64_t));
        Ref<_Cluster> old_payload = payload.as<_Cluster>();
        _Cluster* old_payload_ptr = mem.txl(old_payload);
        for (int k = 0; k < num_fields; ++k) {
            payload_ptr->entries[k] = old_payload_ptr->entries[k];
        }
        mem.free(old_payload, num_fields * sizeof(uint64_t));
        payload = new_payload;
    }
}

Ref<DynType> ClusterMgr::commit(Ref<DynType> from) {
    if (mem.is_writable(from)) {
        _Cluster* from_ptr = mem.txl(from.as<_Cluster>());
        _Cluster* to_ptr;
        Ref<_Cluster> to = mem.alloc_in_file<_Cluster>(to_ptr, num_fields * sizeof(uint64_t));
        for (int k = 0; k < num_fields; ++k) {
            switch(field_info[k].type) {
                case 't': 
                case 'r':
                case 'u': to_ptr->entries[k].as_u = _Array<uint64_t>::commit(mem, from_ptr->entries[k].as_u); break;
                case 'i': to_ptr->entries[k].as_i = _Array<int64_t>::commit(mem, from_ptr->entries[k].as_i); break;
                case 'f': to_ptr->entries[k].as_f = _Array<float>::commit(mem, from_ptr->entries[k].as_f); break;
                case 'd': to_ptr->entries[k].as_d = _Array<double>::commit(mem, from_ptr->entries[k].as_d); break;
                case 's': to_ptr->entries[k].as_s = _Array<_String>::commit(mem, from_ptr->entries[k].as_s); break;
                case 'T':
                case 'R':
                case 'U': to_ptr->entries[k].as_U = _Array<_List<uint64_t>>::commit(mem, from_ptr->entries[k].as_U); 
                    break;
                case 'I': to_ptr->entries[k].as_I = _Array<_List<int64_t>>::commit(mem, from_ptr->entries[k].as_I); 
                    break;
                case 'F': to_ptr->entries[k].as_F = _Array<_List<float>>::commit(mem, from_ptr->entries[k].as_F); 
                    break;
                case 'D': to_ptr->entries[k].as_D = _Array<_List<double>>::commit(mem, from_ptr->entries[k].as_D); 
                    break;
                default: throw std::runtime_error("Internal error, unsupported type specifier");
            }
        }
        mem.free(from, num_fields * sizeof(uint64_t));
        return to;
    }
    return from;
}

void ClusterMgr::read_internalbuffer(Ref<DynType> payload, int index) {
    Ref<_Cluster> p_ref = payload.as<_Cluster>();
    _Cluster* p_ptr = mem.txl(p_ref);
    for (int col = 0; col < num_fields; ++col) {
        switch (field_info[col].type) {
            case 't': 
            case 'r':
            case 'u': values[col].as_u = p_ptr->entries[col].as_u.get(mem, index); break;
            case 'i': values[col].as_i = p_ptr->entries[col].as_i.get(mem, index); break;
            case 'f': values[col].as_f = p_ptr->entries[col].as_f.get(mem, index); break;
            case 'd': values[col].as_d = p_ptr->entries[col].as_d.get(mem, index); break;
            case 's': values[col].as_s = p_ptr->entries[col].as_s.get(mem, index); break;
            case 'T':
            case 'R':
            case 'U': values[col].as_U = p_ptr->entries[col].as_U.get(mem, index); break;
            case 'I': values[col].as_I = p_ptr->entries[col].as_I.get(mem, index); break;
            case 'F': values[col].as_F = p_ptr->entries[col].as_F.get(mem, index); break;
            case 'D': values[col].as_D = p_ptr->entries[col].as_D.get(mem, index); break;
            default: throw std::runtime_error("Internal error, unsupported type specifier");
        }
    }
}

void ClusterMgr::write_internalbuffer(Ref<DynType>& payload, int index, int capacity) {
    assert(mem.is_writable(payload));
    Ref<_Cluster> p_ref = payload.as<_Cluster>();
    _Cluster* p_ptr = mem.txl(p_ref);
    for (int col = 0; col < num_fields; ++col) {
        switch (field_info[col].type) {
            case 't': 
            case 'r':
            case 'u': p_ptr->entries[col].as_u.set(mem, index, values[col].as_u, capacity); break;
            case 'i': p_ptr->entries[col].as_i.set(mem, index, values[col].as_i, capacity); break;
            case 'f': p_ptr->entries[col].as_f.set(mem, index, values[col].as_f, capacity); break;
            case 'd': p_ptr->entries[col].as_d.set(mem, index, values[col].as_d, capacity); break;
            case 's': p_ptr->entries[col].as_s.set(mem, index, values[col].as_s, capacity); break;
            case 'T':
            case 'R':
            case 'U': p_ptr->entries[col].as_U.set(mem, index, values[col].as_U, capacity); break;
            case 'I': p_ptr->entries[col].as_I.set(mem, index, values[col].as_I, capacity); break;
            case 'F': p_ptr->entries[col].as_F.set(mem, index, values[col].as_F, capacity); break;
            case 'D': p_ptr->entries[col].as_D.set(mem, index, values[col].as_D, capacity); break;
            default: throw std::runtime_error("Internal error, unsupported type specifier");
        }
    }
}

void ClusterMgr::swap_internalbuffer(Ref<DynType>& payload, int index, int capacity) {
    assert(mem.is_writable(payload));
    Ref<_Cluster> p_ref = payload.as<_Cluster>();
    _Cluster* p_ptr = mem.txl(p_ref);
    for (int col = 0; col < num_fields; ++col) {
        switch (field_info[col].type) {
            case 't': 
            case 'r':
            case 'u': {
                _Array<uint64_t>& array = p_ptr->entries[col].as_u;
                uint64_t tmp = array.get(mem, index);
                p_ptr->entries[col].as_u.set(mem, index, values[col].as_u, capacity);
                values[col].as_u = tmp;
                break;
            }
            case 'i': {
                _Array<int64_t>& array = p_ptr->entries[col].as_i;
                int64_t tmp = array.get(mem, index);
                p_ptr->entries[col].as_i.set(mem, index, values[col].as_i, capacity);
                values[col].as_i = tmp;
                break;
            }
            case 'f': {
                _Array<float>& array = p_ptr->entries[col].as_f;
                float tmp = array.get(mem, index);
                p_ptr->entries[col].as_f.set(mem, index, values[col].as_u, capacity);
                values[col].as_f = tmp;
                break;
            }
            case 'd': {
                _Array<double>& array = p_ptr->entries[col].as_d;
                double tmp = array.get(mem, index);
                p_ptr->entries[col].as_d.set(mem, index, values[col].as_d, capacity);
                values[col].as_d = tmp;
                break;
            }
            case 's': {
                _Array<_String>& array = p_ptr->entries[col].as_s;
                _String tmp = array.get(mem, index);
                p_ptr->entries[col].as_s.set(mem, index, values[col].as_s, capacity);
                values[col].as_s = tmp;
                break;
            }
            case 'T':
            case 'R':
            case 'U': {
                _Array<_List<uint64_t>>& array = p_ptr->entries[col].as_U;
                _List<uint64_t> tmp = array.get(mem, index);
                p_ptr->entries[col].as_U.set(mem, index, values[col].as_U, capacity);
                values[col].as_U = tmp;
                break;
            }
            case 'I': {
                _Array<_List<int64_t>>& array = p_ptr->entries[col].as_I;
                _List<int64_t> tmp = array.get(mem, index);
                p_ptr->entries[col].as_I.set(mem, index, values[col].as_I, capacity);
                values[col].as_I = tmp;
                break;
            }
            case 'F': {
                _Array<_List<float>>& array = p_ptr->entries[col].as_F;
                _List<float> tmp = array.get(mem, index);
                p_ptr->entries[col].as_F.set(mem, index, values[col].as_F, capacity);
                values[col].as_F = tmp;
                break;
            }
            case 'D': {
                _Array<_List<double>>& array = p_ptr->entries[col].as_D;
                _List<double> tmp = array.get(mem, index);
                p_ptr->entries[col].as_D.set(mem, index, values[col].as_D, capacity);
                values[col].as_D = tmp;
                break;
            }
            default: throw std::runtime_error("Internal error, unsupported type specifier");
        }
    }
}

Ref<_Table> _Table::cow(Memory& mem, Ref<_Table> from) {
    if (!mem.is_writable(from)) {
        _Table* to_ptr;
        _Table* from_ptr = mem.txl(from);
        Ref<_Table> to = mem.alloc<_Table>(to_ptr, get_allocation_size(from_ptr->num_fields));
        *to_ptr = *from_ptr;
        for (uint16_t j = 1; j < to_ptr->num_fields; ++j) {
            to_ptr->fields[j] = from_ptr->fields[j];
        }
        mem.free(from);
        to_ptr->copied_from_file(mem);
        return to;
    }
    return from;
}

void _Table::copied_from_file(Memory& mem) {} // could forward to cuckoo?

Ref<_Table> _Table::commit(Memory& mem, Ref<_Table> from) {
    if (mem.is_writable(from)) {
        _Table* to_ptr;
        _Table* from_ptr = mem.txl(from);
        Ref<_Table> to =
            mem.alloc_in_file<_Table>(to_ptr, get_allocation_size(from_ptr->num_fields));
        *to_ptr = *from_ptr;
        for (uint16_t j = 1; j < to_ptr->num_fields; ++j) {
            to_ptr->fields[j] = from_ptr->fields[j];
        }
        mem.free(from);
        ClusterMgr pm(mem,to_ptr->num_fields, to_ptr->fields);
        to_ptr->cuckoo.copied_to_file(mem, pm);
        return to;
    }
    return from;
}

void _Table::copied_to_file(Memory& mem) {
    ClusterMgr pm(mem, num_fields, fields);
    cuckoo.copied_to_file(mem, pm);
}

void _Table::insert(Memory& mem, uint64_t key) {
    ClusterMgr pm(mem, num_fields, fields);
    pm.init_internalbuffer();
    cuckoo.insert(mem, key << 1, pm);
}

void _Table::get_cluster(Memory& mem, uint64_t key, Object& o) {
    Ref<DynType> payload;
    int index;
    uint8_t size;
    if (cuckoo.find(mem, key, payload, index, size)) {
        Ref<_Cluster> pl = payload.as<_Cluster>();
        _Cluster* pl_ptr = mem.txl(pl);
        o.cluster = pl_ptr;
        o.index = index;
        o.size = size;
        o.is_writable = mem.is_writable(pl);
        return;
    }
    throw NotFound();
}

void _Table::change_cluster(Memory& mem, uint64_t key, Object& o) {
    ClusterMgr pm(mem, num_fields, fields);
    Ref<DynType> payload;
    int index;
    uint8_t size;
    bool res = cuckoo.find_and_cow_path(mem, pm, key, payload, index, size);
    if (!res) {
        throw NotFound();
    }
    assert(mem.is_writable(payload));
    Ref<_Cluster> pl = payload.as<_Cluster>();
    _Cluster* pl_ptr = mem.txl(pl);
    o.cluster = pl_ptr;
    o.index = index;
    o.size = size;
    o.is_writable = true;
}

bool _Table::find(Memory& mem, uint64_t key) {
    int dummy_index;
    uint8_t dummy_size;
    Ref<DynType> dummy_ref;
    return cuckoo.find(mem, key, dummy_ref, dummy_index, dummy_size);
}

Ref<_Table> _Table::create(Memory& mem, const char* t_info) {
    int num_fields = strlen(t_info);
    _Table* table_ptr;
    Ref<_Table> result = mem.alloc<_Table>(table_ptr, _Table::get_allocation_size(num_fields));
    table_ptr->num_fields = num_fields;
    for (int j = 0; j < num_fields; ++j) {
        table_ptr->fields[j].type = t_info[j];
        table_ptr->fields[j].key = (rand() << 16) | j; // add random number for upper 48 bits
    }
    table_ptr->cuckoo.init();
    return result;
}

bool _Table::first_access(Memory& mem, ObjectIterator& oi) {
    // idx -> leaf,payload,num_elems
    // leaf,elem -> key
    return cuckoo.first_access(mem, oi);
}




template<typename T>
void Object::set(Field<T> f, T value) {
    Memory& mem = ss->change(this);
    uint16_t idx = f.key;
    if (f.key != table->fields[idx].key)
        throw std::runtime_error("Stale or invalid field specifier");
    ::set<T>(mem, cluster->entries[idx], index, value, size);
}

template<>
void Object::set<Table>(Field<Table> f, Table value) {
    Memory& mem = ss->change(this);
    uint16_t idx = f.key;
    if (f.key != table->fields[idx].key)
        throw std::runtime_error("Stale or invalid field specifier");
    ::set<uint64_t>(mem, cluster->entries[idx], index, value.key, size);
}

template<>
void Object::set<Row>(Field<Row> f, Row value) {
    Memory& mem = ss->change(this);
    uint16_t idx = f.key;
    if (f.key != table->fields[idx].key)
        throw std::runtime_error("Stale or invalid field specifier");
    ::set<uint64_t>(mem, cluster->entries[idx], index, value.key, size);
}

void Object::set(Field<String> f, std::string value) {
    Memory& mem = ss->change(this);
    uint16_t idx = f.key;
    if (f.key != table->fields[idx].key)
        throw std::runtime_error("Stale or invalid field specifier");
    _String s = ::get<_String>(mem, cluster->entries[idx], index);
    uint64_t limit = value.size();
    s.set_size(mem, limit);
    for (uint64_t k = 0; k < limit; ++k) s.set(mem, k, value[k]);
    ::set<_String>(mem, cluster->entries[idx], index, s, size);
}

std::string Object::operator()(Field<String> f) {
    Memory& mem = ss->refresh(this);
    uint16_t idx = f.key;
    if (f.key != table->fields[idx].key)
        throw std::runtime_error("Stale or invalid field specifier");
    _String s = ::get<_String>(mem, cluster->entries[idx], index);
    std::string res;
    uint64_t limit = s.get_size();
    res.reserve(limit);
    for (uint64_t k = 0; k < limit; ++k) res.push_back(s.get(mem, k));
    return res;
}

template<typename T>
T Object::operator()(Field<T> f) {
    Memory& mem = ss->refresh(this);
    uint16_t idx = f.key;
    if (f.key != table->fields[idx].key)
        throw std::runtime_error("Stale or invalid field specifier");
    return ::get<T>(mem, cluster->entries[idx], index);
}

template<>
Table Object::operator()<Table>(Field<Table> f) {
    Memory& mem = ss->refresh(this);
    uint16_t idx = f.key;
    if (f.key != table->fields[idx].key)
        throw std::runtime_error("Stale or invalid field specifier");
    Table res;
    res.key = ::get<uint64_t>(mem, cluster->entries[idx], index);
    return res;
}

template<>
Row Object::operator()<Row>(Field<Row> f) {
    Memory& mem = ss->refresh(this);
    uint16_t idx = f.key;
    if (f.key != table->fields[idx].key)
        throw std::runtime_error("Stale or invalid field specifier");
    Row res;
    res.key = ::get<uint64_t>(mem, cluster->entries[idx], index);
    return res;
}

template<typename T>
ListAccessor<T> Object::operator()(Field<List<T>> f) {
    ListAccessor<T> res;
    res.o = *this;
    res.f = f;
    return res;
}

template<>
ListAccessor<Table> Object::operator()(Field<List<Table>> f) {
    ListAccessor<Table> res;
    Field<List<uint64_t>> f2;
    f2.key = f.key;
    res.list.o = *this;
    res.list.f = f2;
    return res;
}

template<>
ListAccessor<Row> Object::operator()(Field<List<Row>> f) {
    ListAccessor<Row> res;
    Field<List<uint64_t>> f2;
    f2.key = f.key;
    res.list.o = *this;
    res.list.f = f2;
    return res;
}

template<typename T>
uint64_t ListAccessor<T>::get_size() {
    Memory& mem = o.ss->refresh(&o);
    uint16_t idx = f.key;
    if (f.key != o.table->fields[idx].key)
        throw std::runtime_error("Stale or invalid field specifier");
    _List<T> list = ::get<_List<T>>(mem, o.cluster->entries[idx], o.index);
    return list.get_size();
}

template<typename T>
T ListAccessor<T>::rd(uint64_t index) {
    Memory& mem = o.ss->refresh(&o);
    uint16_t idx = f.key;
    if (f.key != o.table->fields[idx].key)
        throw std::runtime_error("Stale or invalid field specifier");
    _List<T> list = ::get<_List<T>>(mem, o.cluster->entries[idx], o.index);
    return list.get(mem, index);
}

template<typename T>
void ListAccessor<T>::set_size(uint64_t size) {
    Memory& mem = o.ss->change(&o);
    uint16_t idx = f.key;
    if (f.key != o.table->fields[idx].key)
        throw std::runtime_error("Stale or invalid field specifier");
    _List<T> list = ::get<_List<T>>(mem, o.cluster->entries[idx], o.index);
    list.set_size(mem, size);
    ::set<_List<T>>(mem, o.cluster->entries[idx], o.index, list, o.size);
}

template<typename T>
void ListAccessor<T>::wr(uint64_t index, T value) {
    Memory& mem = o.ss->change(&o);
    uint16_t idx = f.key;
    if (f.key != o.table->fields[idx].key)
        throw std::runtime_error("Stale or invalid field specifier");
    _List<T> list = ::get<_List<T>>(mem, o.cluster->entries[idx], o.index);
    list.set(mem, index, value);
    ::set<_List<T>>(mem, o.cluster->entries[idx], o.index, list, o.size);
}


// explicit instantiation
template void Object::set<uint64_t>(Field<uint64_t>, uint64_t);
template void Object::set<int64_t>(Field<int64_t>, int64_t);
template void Object::set<float>(Field<float>, float);
template void Object::set<double>(Field<double>, double);
template void Object::set<Table>(Field<Table>, Table);
template void Object::set<Row>(Field<Row>, Row);

template uint64_t Object::operator()<uint64_t>(Field<uint64_t>);
template int64_t Object::operator()<int64_t>(Field<int64_t>);
template float Object::operator()<float>(Field<float>);
template double Object::operator()<double>(Field<double>);
template Table Object::operator()<Table>(Field<Table>);
template Row Object::operator()<Row>(Field<Row>);

template ListAccessor<uint64_t> Object::operator()<uint64_t>(Field<List<uint64_t>>);
template ListAccessor<int64_t> Object::operator()<int64_t>(Field<List<int64_t>>);
template ListAccessor<float> Object::operator()<float>(Field<List<float>>);
template ListAccessor<double> Object::operator()<double>(Field<List<double>>);
template ListAccessor<Table> Object::operator()<Table>(Field<List<Table>>);
template ListAccessor<Row> Object::operator()<Row>(Field<List<Row>>);

template class ListAccessor<uint64_t>;
template class ListAccessor<int64_t>;
template class ListAccessor<float>;
template class ListAccessor<double>;
template class ListAccessor<Table>;
template class ListAccessor<Row>;
