#include "group.hpp"
#include <assert.h>
#include <iostream>
#include <fstream>
#include "group_writer.hpp"

using namespace std;

namespace {

class MemoryOStream {
public:
    MemoryOStream(size_t size): m_pos(0), m_buffer(NULL)
    {
        m_buffer = (char*)malloc(size);
    }

    bool   is_valid() const {return m_buffer != NULL;}
    size_t getpos() const {return m_pos;}

    size_t write(const char* p, size_t n)
    {
        const size_t pos = m_pos;
        memcpy(m_buffer+m_pos, p, n);
        m_pos += n;
        return pos;
    }
    void seek(size_t pos) {m_pos = pos;}

    char* release_buffer()
    {
        char* tmp = m_buffer;
        m_buffer = NULL; // invalidate
        return tmp;
    }
private:
    size_t m_pos;
    char* m_buffer;
};

class FileOStream {
public:
    FileOStream(const char* filepath) : m_pos(0), m_file(NULL)
    {
        m_file = fopen(filepath, "wb");
    }

    ~FileOStream()
    {
        fclose(m_file);
    }

    bool is_valid() const {return m_file != NULL;}
    size_t getpos() const {return m_pos;}

    size_t write(const char* p, size_t n)
    {
        const size_t pos = m_pos;
        fwrite(p, 1, n, m_file);
        m_pos += n;
        return pos;
    }

    void seek(size_t pos)
    {
        fseek(m_file, pos, SEEK_SET);
    }
    
private:
    size_t m_pos;
    FILE*  m_file;
};

} // namespace


namespace tightdb {

Group::Group():
    m_top(COLUMN_HASREFS, NULL, 0, m_alloc), m_tables(m_alloc), m_tableNames(NULL, 0, m_alloc),
    m_freePositions(COLUMN_NORMAL, NULL, 0, m_alloc), m_freeLengths(COLUMN_NORMAL, NULL, 0, m_alloc), m_isValid(true)
{
    create();
}

Group::Group(const char* filename, bool readOnly):
    m_top(m_alloc), m_tables(m_alloc), m_tableNames(m_alloc), m_freePositions(m_alloc), m_freeLengths(m_alloc), m_isValid(false)
{
    assert(filename);

    // Memory map file
    m_isValid = m_alloc.SetShared(filename, readOnly);

    if (m_isValid) create_from_ref();
}

Group::Group(const char* buffer, size_t len):
    m_top(m_alloc), m_tables(m_alloc), m_tableNames(m_alloc), m_freePositions(m_alloc), m_freeLengths(m_alloc), m_isValid(false)
{
    assert(buffer);

    // Memory map file
    m_isValid = m_alloc.SetSharedBuffer(buffer, len);

    if (m_isValid) create_from_ref();
}

void Group::create()
{
    m_tables.SetType(COLUMN_HASREFS);
    
    m_top.Add(m_tableNames.GetRef());
    m_top.Add(m_tables.GetRef());
    m_top.Add(m_freePositions.GetRef());
    m_top.Add(m_freeLengths.GetRef());
    
    // Set parent info
    m_tableNames.SetParent(&m_top, 0);
    m_tables.SetParent(&m_top, 1);
    m_freePositions.SetParent(&m_top, 2);
    m_freeLengths.SetParent(&m_top, 3);
}

void Group::create_from_ref()
{
    // Get ref for table top array
    const size_t top_ref = m_alloc.GetTopRef();
    
    // Instantiate top arrays
    if (top_ref == 0) {
        m_top.SetType(COLUMN_HASREFS);
        m_tables.SetType(COLUMN_HASREFS);
        m_tableNames.SetType(COLUMN_NORMAL);
        m_freePositions.SetType(COLUMN_NORMAL);
        m_freeLengths.SetType(COLUMN_NORMAL);
        
        create();
        
        // Everything but header is free space
        m_freePositions.Add(8);
        m_freeLengths.Add(m_alloc.GetFileLen()-8);
    }
    else {
        m_top.UpdateRef(top_ref);
        assert(m_top.Size() >= 2);
        
        m_tableNames.UpdateRef(m_top.Get(0));
        m_tables.UpdateRef(m_top.Get(1));
        m_tableNames.SetParent(&m_top, 0);
        m_tables.SetParent(&m_top, 1);
        
        // Serialized files do not have free space markers
        if (m_top.Size() > 2) {
            m_freePositions.UpdateRef(m_top.Get(2));
            m_freeLengths.UpdateRef(m_top.Get(3));
            m_freePositions.SetParent(&m_top, 2);
            m_freeLengths.SetParent(&m_top, 3);
        }
        
        // Make room for pointers to cached tables
        const size_t count = m_tables.Size();
        for (size_t i = 0; i < count; ++i) {
            m_cachedtables.Add(0);
        }
    }
}

Group::~Group()
{
    for (size_t i = 0; i < m_tables.Size(); ++i) {
        Table* const t = reinterpret_cast<Table*>(m_cachedtables.Get(i));
        delete t;
    }
    m_cachedtables.Destroy();

    // Recursively deletes entire tree
    m_top.Destroy();
}

size_t Group::get_table_count() const
{
    return m_tableNames.Size();
}

const char* Group::get_table_name(size_t table_ndx) const
{
    assert(table_ndx < m_tableNames.Size());
    return m_tableNames.Get(table_ndx);
}

bool Group::has_table(const char* name) const
{
    const size_t n = m_tableNames.Find(name);
    return (n != (size_t)-1);
}

TableRef Group::get_table(const char* name)
{
    const size_t n = m_tableNames.Find(name);

    if (n == size_t(-1)) {
        // Create new table
        Table* const t = new Table(m_alloc);
        t->m_top.SetParent(this, m_tables.Size());

        m_tables.Add(t->m_top.GetRef());
        m_tableNames.Add(name);
        m_cachedtables.Add((intptr_t)t);

        return t->GetTableRef();
    }
    else {
        // Get table from cache if exists, else create
        return get_table(n).GetTableRef();
    }
}

Table& Group::get_table(size_t ndx)
{
    assert(ndx < m_tables.Size());

    // Get table from cache if exists, else create
    Table* t = reinterpret_cast<Table*>(m_cachedtables.Get(ndx));
    if (!t) {
        const size_t ref = m_tables.GetAsRef(ndx);
        t = new Table(m_alloc, ref, this, ndx);
        m_cachedtables.Set(ndx, intptr_t(t));
    }
    return *t;
}


bool Group::write(const char* filepath)
{
    assert(filepath);

    FileOStream out(filepath);
    if (!out.is_valid()) return false;

    write(out);

    return true;
}

char* Group::write_to_mem(size_t& len)
{
    // Get max possible size of buffer
    const size_t max_size = m_alloc.GetTotalSize();

    MemoryOStream out(max_size);
    if (!out.is_valid()) return NULL; // alloc failed

    len = write(out);
    return out.release_buffer();
}

bool Group::commit()
{
    if (!m_alloc.CanPersist()) return false;
    
    // If we have an empty db file, we can just serialize directly
    //if (m_alloc.GetTopRef() == 0) {}
    
    GroupWriter out(*this);
    if (!out.IsValid()) return false;
    
    // Recursively write all changed arrays to end of file
    out.Commit();
    
    return true;
}

size_t Group::get_free_space(size_t len, size_t& filesize, bool testOnly, bool ensureRest)
{
    if (ensureRest) ++len;
    
    // Do we have a free space we can reuse?
    for (size_t i = 0; i < m_freeLengths.Size(); ++i) {
        const size_t free_len = m_freeLengths.Get(i);
        if (len <= free_len) {
            const size_t location = m_freePositions.Get(i);
            if (testOnly) return location;
            if (ensureRest) --len;
            
            // Update free list
            const size_t rest = free_len - len;
            if (rest == 0) {
                m_freePositions.Delete(i);
                m_freeLengths.Delete(i);
            }
            else {
                m_freeLengths.Set(i, rest);
                m_freePositions.Set(i, location + len);
            }
            
            return location;
        }
    }
    
    // No free space, so we have to expand the file.
    // we always expand megabytes at a time, both for
    // performance and to avoid excess fragmentation
    const size_t old_filesize = filesize;
    const size_t needed_size = old_filesize + len;
    while (filesize < needed_size) {
        filesize += 1024*1024;
    }

#if !defined(_MSC_VER) // write persistence
    // Extend the file
    const int fd = m_alloc.GetFileDescriptor();
    lseek(fd, filesize-1, SEEK_SET);
    ::write(fd, "\0", 1);
#endif

    // Add new free space
    const size_t end  = old_filesize + len;
    const size_t rest = filesize - end;
    m_freePositions.Add(end);
    m_freeLengths.Add(rest);
    
    return old_filesize;
}

void Group::connect_free_space(bool doConnect)
{
    assert(m_top.Size() == 4);

    if (doConnect) {
        m_top.Set(2, m_freePositions.GetRef());
        m_top.Set(3, m_freeLengths.GetRef());
        m_freePositions.SetParent(&m_top, 2);
        m_freeLengths.SetParent(&m_top, 3);
    }
    else {
        m_top.Set(2, 0);
        m_top.Set(3, 0);
        m_freePositions.SetParent(NULL, 0);
        m_freeLengths.SetParent(NULL, 0);
        
    }
}

void Group::update_refs(size_t topRef)
{
    // Update top with the new (persistent) ref
    m_top.UpdateRef(topRef);
    
    // Now we can update it's child arrays
    m_tableNames.UpdateFromParent();
    //m_freePositions.UpdateFromParent();
    //m_freeLengths.UpdateFromParent();
    
    // if the tables have not been modfied we don't
    // need to update cached tables
    if (!m_tables.UpdateFromParent()) return;
    
    // Also update cached tables
    const size_t count = m_cachedtables.Size();
    for (size_t i = 0; i < count; ++i) {
        Table* const t = (Table*)m_cachedtables.Get(i);
        if (t) {
            t->UpdateFromParent();
        }
    }
}

#ifdef _DEBUG

void Group::verify()
{
    for (size_t i = 0; i < m_tables.Size(); ++i) {
        // Get table from cache if exists, else create
        Table* t = reinterpret_cast<Table*>(m_cachedtables.Get(i));
        if (!t) {
            const size_t ref = m_tables.GetAsRef(i);
            t = new Table(m_alloc, ref, this, i);
            m_cachedtables.Set(i, intptr_t(t));
        }
        t->verify();
    }
}

MemStats Group::stats()
{
    MemStats stats;
    m_top.Stats(stats);
    
    return stats;
}


void Group::print() const
{
    m_alloc.Print();
}

void Group::to_dot(std::ostream& out)
{
    out << "digraph G {" << endl;

    out << "subgraph cluster_group {" << endl;
    out << " label = \"Group\";" << endl;

    m_top.ToDot(out, "group_top");
    m_tableNames.ToDot(out, "table_names");
    m_tables.ToDot(out, "tables");

    // Tables
    for (size_t i = 0; i < m_tables.Size(); ++i) {
        const Table& table = get_table(i);
        const char* const name = get_table_name(i);
        table.ToDot(out, name);
    }

    out << "}" << endl;
    out << "}" << endl;
}

#endif //_DEBUG

} //namespace tightdb
