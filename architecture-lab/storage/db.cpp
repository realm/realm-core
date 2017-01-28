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

#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <stdexcept>

#include "db.hpp"
#include "memory.hpp"
#include "snapshot_impl.hpp"
#include "hash.hpp"
struct _Header;

struct DbImp : public Db {

    DbImp(const char* fname);
    const Snapshot& open_snapshot();
    void release(const Snapshot&&);
    Snapshot& create_changes();
    void abort(Snapshot&&);
    void commit(Snapshot&&);

    Memory mem;
    const char* fname;
    int fd;
    char* zero_page;
    _Header* header;
};


Db& Db::create(const char* fname) {
    DbImp* res = new DbImp(fname);
    // fixme to do only once. And fixme to be portable across platforms.
    // Or possibly store inside file?
    init_hashes();
    return *res;
}

struct _Versions;
struct _Meta {
    Ref<_Versions> versions;
    uint64_t logical_file_size;
    uint64_t in_file_allocation_point;
};

struct _Header {
    char selector;
    _Meta meta[2];
};

struct _Versions {
    uint64_t first_version;
    uint64_t last_version;
    Ref<_Snapshot> versions[1];
};

DbImp::DbImp(const char* fname) : fname(fname) {
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    fd = open(fname, O_RDWR | O_CREAT | O_EXCL, mode);
    if (fd < 0) 
        throw std::runtime_error("unable to create db");
    int status = ftruncate(fd, 64 * 1024);
    if (status < 0)
        throw std::runtime_error("unable to create zero page");
    zero_page = reinterpret_cast<char*>( mmap(0, 64*1024, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0));
    if (zero_page == nullptr)
        throw std::runtime_error("unable to map zero page");
    header = reinterpret_cast<_Header*>(zero_page);
    header->selector = 0;
    header->meta[0].versions = Ref<_Versions>();
    header->meta[0].logical_file_size = 64 * 1024;
    header->meta[0].in_file_allocation_point = Memory::chunk_size;
}

const Snapshot& DbImp::open_snapshot() {
    _Meta* meta = header->meta + header->selector;
    if (is_null(meta->versions))
        throw std::runtime_error("no snapshot in database");
    // make sure memory mapping matches changes in file:
    mem.prepare_mapping(fd, meta->logical_file_size);
    _Versions* v_ptr = mem.txl(meta->versions);
    return *new SnapshotImpl(mem, v_ptr->versions[0], false);
}

Snapshot& DbImp::create_changes() {
    _Meta* meta = header->meta + header->selector;
    if (is_null(meta->versions)) {
        _Snapshot* ptr;
        Ref<_Snapshot> top = mem.alloc<_Snapshot>(ptr);
        ptr->init();
        return *new SnapshotImpl(mem, top, true);
    }
    _Versions* v_ptr = mem.txl(meta->versions);
    SnapshotImpl* res = new SnapshotImpl(mem, v_ptr->versions[0], true);
    // cow the snapshot in advance, so we don't need to check for it all the time
    res->cow();
    return *res;
}


void DbImp::release(const Snapshot&& s) {
    // ...
    mem.reset_freelists();
}

void DbImp::abort(Snapshot&& s) {
    //.,..
    mem.reset_freelists();
}

void DbImp::commit(Snapshot&& s) {
    _Meta* meta = header->meta + header->selector;
  
    mem.open_for_write(fd, meta->in_file_allocation_point);
    SnapshotImpl* impl = static_cast<SnapshotImpl*>(&s);
    Ref<_Snapshot> res = impl->commit();
    Ref<_Versions> old_version = meta->versions;
    _Versions* old_version_ptr = mem.txl(old_version);
    _Versions* new_version_ptr;
    Ref<_Versions> new_version = mem.alloc_in_file<_Versions>(new_version_ptr);
    *new_version_ptr = *old_version_ptr;
    mem.free(old_version);
    new_version_ptr->versions[0] = res;
    _Meta* new_meta = header->meta + (1 ^ header->selector);
    new_meta->versions = new_version;
    mem.finish_writing(new_meta->logical_file_size, new_meta->in_file_allocation_point); // implies sync
    msync(zero_page, 64*1024, MS_SYNC);
    header->selector = 1 ^ header->selector;
    msync(zero_page, 64*1024, MS_SYNC);
    delete impl;
    mem.reset_freelists();
}
