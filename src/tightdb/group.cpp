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
    m_freePositions(COLUMN_NORMAL, NULL, 0, m_alloc), m_freeLengths(COLUMN_NORMAL, NULL, 0, m_alloc), m_freeVersions(COLUMN_NORMAL, NULL, 0, m_alloc), m_persistMode(0), m_isValid(true)
{
    create();
}

Group::Group(const char* filename, int mode):
    m_top(m_alloc), m_tables(m_alloc), m_tableNames(m_alloc), m_freePositions(m_alloc), m_freeLengths(m_alloc), m_freeVersions(m_alloc), m_persistMode(mode), m_isValid(false)
{
    assert(filename);

    // Memory map file
    const bool readOnly = mode & GROUP_READONLY;
    m_isValid = m_alloc.SetShared(filename, readOnly);

    if (m_isValid) {
        // if we just created shared group, we have to wait with
        // actually creating it's datastructures until first write
        if (m_persistMode == GROUP_SHARED && m_alloc.GetTopRef() == 0)
            return;
        else
            create_from_ref();
    }
}

Group::Group(const char* buffer, size_t len):
    m_top(m_alloc), m_tables(m_alloc), m_tableNames(m_alloc), m_freePositions(m_alloc), m_freeLengths(m_alloc), m_freeVersions(m_alloc), m_persistMode(0), m_isValid(false)
{
    assert(buffer);

    // Memory map file
    m_isValid = m_alloc.SetSharedBuffer(buffer, len);

    if (m_isValid) create_from_ref();
}

void Group::create()
{
    m_tables.SetType(COLUMN_HASREFS);

    m_top.add(m_tableNames.GetRef());
    m_top.add(m_tables.GetRef());
    m_top.add(m_freePositions.GetRef());
    m_top.add(m_freeLengths.GetRef());

    // Set parent info
    m_tableNames.SetParent(&m_top, 0);
    m_tables.SetParent(&m_top, 1);
    m_freePositions.SetParent(&m_top, 2);
    m_freeLengths.SetParent(&m_top, 3);

    if (m_freeVersions.IsValid()) {
        m_top.add(m_freeVersions.GetRef());
        m_freeVersions.SetParent(&m_top, 4);
    }
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
        if (is_shared()) {
            m_freeVersions.SetType(COLUMN_NORMAL);
        }

        create();

        // Everything but header is free space
        m_freePositions.add(8);
        m_freeLengths.add(m_alloc.GetFileLen()-8);
        if (is_shared())
            m_freeVersions.add(0);
    }
    else {
        m_top.UpdateRef(top_ref);
        const size_t top_size = m_top.Size();
        assert(top_size >= 2);

        m_tableNames.UpdateRef(m_top.Get(0));
        m_tables.UpdateRef(m_top.Get(1));
        m_tableNames.SetParent(&m_top, 0);
        m_tables.SetParent(&m_top, 1);

        // Serialized files do not have free space markers
        // at all, and files that are not shared does not
        // need version info for free space.
        if (top_size >= 4) {
            m_freePositions.UpdateRef(m_top.Get(2));
            m_freeLengths.UpdateRef(m_top.Get(3));
            m_freePositions.SetParent(&m_top, 2);
            m_freeLengths.SetParent(&m_top, 3);
        }
        if (top_size == 5) {
            m_freeVersions.UpdateRef(m_top.Get(4));
            m_freeVersions.SetParent(&m_top, 4);
        }

        // Make room for pointers to cached tables
        const size_t count = m_tables.Size();
        for (size_t i = 0; i < count; ++i) {
            m_cachedtables.add(0);
        }
    }
}

void Group::init_shared() {
    if (m_freeVersions.IsValid()) {
        // If free space tracking is enabled
        // we just have to reset it
        m_freeVersions.SetAllToZero();
    }
    else {
        // Serialized files have no free space tracking
        // at all so we have to add the basic free lists
        if (m_top.Size() == 2) {
            m_freePositions.SetType(COLUMN_NORMAL);
            m_freeLengths.SetType(COLUMN_NORMAL);
            m_top.add(m_freePositions.GetRef());
            m_top.add(m_freeLengths.GetRef());
            m_freePositions.SetParent(&m_top, 2);
            m_freeLengths.SetParent(&m_top, 3);
        }

        // Files that have only been used in single thread
        // mode do not have version tracking for the free lists
        if (m_top.Size() == 4) {
            const size_t count = m_freePositions.Size();
            m_freeVersions.SetType(COLUMN_NORMAL);
            for (size_t i = 0; i < count; ++i) {
                m_freeVersions.add(0);
            }
            m_top.add(m_freeVersions.GetRef());
            m_freeVersions.SetParent(&m_top, 4);
        }
    }
}
    
void Group::reset_to_new()
{
    assert(m_alloc.GetTopRef() == 0);
    if (!m_top.IsValid()) return; // already in new state

    // A shared group that has just been created and not yet
    // written to does not have any internal structures yet
    // (we may have to re-create this after a rollback)

    // Clear old cached state
    const size_t count = m_cachedtables.Size();
    for (size_t i = 0; i < count; ++i) {
        Table* const t = reinterpret_cast<Table*>(m_cachedtables.Get(i));
        delete t;
    }
    m_cachedtables.Clear();

    m_top.Invalidate();
    m_tables.Invalidate();
    m_tableNames.Invalidate();
    m_freePositions.Invalidate();
    m_freeLengths.Invalidate();
    m_freeVersions.Invalidate();

    m_tableNames.SetParent(NULL, 0);
    m_tables.SetParent(NULL, 0);
    m_freePositions.SetParent(NULL, 0);
    m_freeLengths.SetParent(NULL, 0);
    m_freeVersions.SetParent(NULL, 0);
}

void Group::rollback()
{
    assert(is_shared());

    // Clear all changes made during transaction
    m_alloc.FreeAll();
}

Group::~Group()
{
    if (!m_top.IsValid()) return; // nothing to clean up in new state

    const size_t count = m_tables.Size();
    for (size_t i = 0; i < count; ++i) {
        Table* const t = reinterpret_cast<Table*>(m_cachedtables.Get(i));
        delete t;
    }
    m_cachedtables.Destroy();

    // Recursively deletes entire tree
    m_top.Destroy();
}

bool Group::is_empty() const
{
    if (!m_top.IsValid()) return true;

    return m_tableNames.is_empty();
}

size_t Group::get_table_count() const
{
    if (!m_top.IsValid()) return 0;

    return m_tableNames.Size();
}

const char* Group::get_table_name(size_t table_ndx) const
{
    assert(m_top.IsValid());
    assert(table_ndx < m_tableNames.Size());

    return m_tableNames.Get(table_ndx);
}

bool Group::has_table(const char* name) const
{
    if (!m_top.IsValid()) return false;

    const size_t n = m_tableNames.find_first(name);
    return (n != (size_t)-1);
}

Table* Group::get_table_ptr(const char* name)
{
    const size_t n = m_tableNames.find_first(name);

    if (n == size_t(-1)) {
        // Create new table
        Table* const t = new Table(m_alloc);
        t->m_top.SetParent(this, m_tables.Size());

        m_tables.add(t->m_top.GetRef());
        m_tableNames.add(name);
        m_cachedtables.add((intptr_t)t);

        return t;
    }
    else {
        // Get table from cache if exists, else create
        return &get_table(n);
    }
}

Table& Group::get_table(size_t ndx)
{
    assert(m_top.IsValid());
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
    assert(m_top.IsValid());

    FileOStream out(filepath);
    if (!out.is_valid()) return false;

    write(out);

    return true;
}

char* Group::write_to_mem(size_t& len)
{
    assert(m_top.IsValid());

    // Get max possible size of buffer
    const size_t max_size = m_alloc.GetTotalSize();

    MemoryOStream out(max_size);
    if (!out.is_valid()) return NULL; // alloc failed

    len = write(out);
    return out.release_buffer();
}

bool Group::commit()
{
    return commit(-1, -1);
}

bool Group::commit(size_t current_version, size_t readlock_version)
{
    assert(m_top.IsValid());
    assert(readlock_version <= current_version);

    if (!m_alloc.CanPersist()) return false;

    // If we have an empty db file, we can just serialize directly
    //if (m_alloc.GetTopRef() == 0) {}

    GroupWriter out(*this);
    if (!out.IsValid()) return false;

    if (is_shared()) {
        m_readlock_version = readlock_version;
        out.SetVersions(current_version, readlock_version);
    }

    // Recursively write all changed arrays to end of file
    out.Commit();

    return true;
}

void Group::update_refs(size_t topRef)
{
    // Update top with the new (persistent) ref
    m_top.UpdateRef(topRef);
    assert(m_top.Size() >= 2);

    // Now we can update it's child arrays
    m_tableNames.UpdateFromParent();

    // No free-info in serialized databases
    // and version info is only in shared,
    if (m_top.Size() >= 4) {
        m_freePositions.UpdateFromParent();
        m_freeLengths.UpdateFromParent();
    }
    else {
        m_freePositions.Invalidate();
        m_freeLengths.Invalidate();
    }
    if (m_top.Size() == 5) {
        m_freeVersions.UpdateFromParent();
    }
    else {
        m_freeVersions.Invalidate();
    }

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
    
void Group::update_from_shared(size_t top_ref, size_t len)
{
    if (top_ref == 0) return;              // just created
    if (top_ref == m_top.GetRef()) return; // already up-to-date
    
    // Update memory mapping if needed
    m_alloc.ReMap(len);
    
    // Update group arrays
    m_top.UpdateRef(top_ref);
    assert(m_top.Size() >= 2);
    const bool nameschanged = !m_tableNames.UpdateFromParent();
    m_tables.UpdateFromParent();
    if (m_top.Size() > 2) {
        m_freePositions.UpdateFromParent();
        m_freeLengths.UpdateFromParent();
        if (m_top.Size() > 4) {
            m_freeVersions.UpdateFromParent();
        }
    }
    
    // If the names of the the tables in the group has not
    // changed we know that it still contains the same tables
    // so we can reuse the cached versions
    if (nameschanged) {
        // Clear old cached state
        const size_t count = m_cachedtables.Size();
        for (size_t i = 0; i < count; ++i) {
            Table* const t = reinterpret_cast<Table*>(m_cachedtables.Get(i));
            delete t;
        }
        m_cachedtables.Clear();
    
        // Make room for new pointers to cached tables
        const size_t table_count = m_tables.Size();
        for (size_t i = 0; i < table_count; ++i) {
            m_cachedtables.add(0);
        }
    }
    else {
        // Update cached tables
        //TODO: account for changed spec
        const size_t count = m_cachedtables.Size();
        for (size_t i = 0; i < count; ++i) {
            Table* const t = (Table*)m_cachedtables.Get(i);
            if (t) {
                t->UpdateFromParent();
            }
        }
    }
}


#ifdef _DEBUG

void Group::Verify()
{
    for (size_t i = 0; i < m_tables.Size(); ++i) {
        // Get table from cache if exists, else create
        Table* t = reinterpret_cast<Table*>(m_cachedtables.Get(i));
        if (!t) {
            const size_t ref = m_tables.GetAsRef(i);
            t = new Table(m_alloc, ref, this, i);
            m_cachedtables.Set(i, intptr_t(t));
        }
        t->Verify();
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

void Group::print_free() const
{
    if (!m_freePositions.IsValid()) {
        printf("none\n");
        return;
    }
    const bool hasVersions = m_freeVersions.IsValid();

    const size_t count = m_freePositions.Size();
    for (size_t i = 0; i < count; ++i) {
        const size_t pos  = m_freePositions[i];
        const size_t size = m_freeLengths[i];
        printf("%d: %d %d", (int)i, (int)pos, (int)size);

        if (hasVersions) {
            const size_t version = m_freeVersions[i];
            printf(" %d", (int)version);
        }
        printf("\n");
    }
    printf("\n");
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
        table.to_dot(out, name);
    }

    out << "}" << endl;
    out << "}" << endl;
}

#include <sys/mman.h>

void Group::zero_free_space(size_t file_size, size_t readlock_version)
{
    if (!is_shared()) return;

#if !defined(_MSC_VER)
    const int fd = m_alloc.GetFileDescriptor();

    // Map to memory
    void* const p = mmap(0, file_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (p == (void*)-1) return;

    const size_t count = m_freePositions.Size();
    for (size_t i = 0; i < count; ++i) {
        const size_t v = m_freeVersions.Get(i);
        if (v >= m_readlock_version) continue;

        const size_t pos = m_freePositions.Get(i);
        const size_t len = m_freeLengths.Get(i);

        memset((char*)p+pos, 0, len);
    }

    munmap(p, file_size);

#endif
}

#endif //_DEBUG

} //namespace tightdb
