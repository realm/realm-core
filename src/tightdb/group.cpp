#include <new>
#include <iostream>
#include <fstream>

#include <tightdb/group_writer.hpp>
#include <tightdb/group.hpp>

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
        fseek(m_file, static_cast<long>(pos), SEEK_SET);
    }

private:
    size_t m_pos;
    FILE*  m_file;
};

} // anonymous namespace


namespace tightdb {

Group::Group():
    m_top(COLUMN_HASREFS, NULL, 0, m_alloc), m_tables(m_alloc), m_tableNames(NULL, 0, m_alloc),
    m_freePositions(COLUMN_NORMAL, NULL, 0, m_alloc),
    m_freeLengths(COLUMN_NORMAL, NULL, 0, m_alloc),
    m_freeVersions(COLUMN_NORMAL, NULL, 0, m_alloc), m_persistMode(0), m_isValid(true)
{
    create();
}

Group::Group(const char* filename, int mode):
    m_top(m_alloc), m_tables(m_alloc), m_tableNames(m_alloc), m_freePositions(m_alloc),
    m_freeLengths(m_alloc), m_freeVersions(m_alloc), m_persistMode(mode), m_isValid(false)
{
    TIGHTDB_ASSERT(filename);

    // With shared groups, we might want to start in invalid state
    // and then initialize later
    if (mode & GROUP_INVALID)
        return;

    create_from_file(filename, true);
}

Group::Group(const char* buffer, size_t len, bool take_ownership):
    m_top(m_alloc), m_tables(m_alloc), m_tableNames(m_alloc), m_freePositions(m_alloc),
    m_freeLengths(m_alloc), m_freeVersions(m_alloc), m_persistMode(0), m_isValid(false)
{
    TIGHTDB_ASSERT(buffer);

    // Memory map file
    m_isValid = m_alloc.SetSharedBuffer(buffer, len);

    if (m_isValid) {
        const size_t top_ref = m_alloc.GetTopRef();
        create_from_ref(top_ref);
    }
}

bool Group::create_from_file(const char* filename, bool doInit)
{
    TIGHTDB_ASSERT(!m_isValid);

    // Memory map file
    // This leaves the group ready, but in invalid state
    const bool readOnly = m_persistMode & GROUP_READONLY;
    const bool isValid = m_alloc.SetShared(filename, readOnly);

    if (isValid && doInit) {
        m_isValid = true;

        // if we just created shared group, we have to wait with
        // actually creating it's datastructures until first write
        if (m_persistMode == GROUP_SHARED && m_alloc.GetTopRef() == 0)
            return true;
        else {
            const size_t top_ref = m_alloc.GetTopRef();
            create_from_ref(top_ref);
        }
    }

    return isValid;
}

void Group::create()
{
    m_tables.SetType(COLUMN_HASREFS); // FIXME: Why is this not done in Group() like the rest of the arrays?

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

void Group::create_from_ref(size_t top_ref)
{
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
        m_freePositions.add(header_len);
        m_freeLengths.add(m_alloc.GetFileLen() - header_len);
        if (is_shared())
            m_freeVersions.add(0);
    }
    else {
        m_top.UpdateRef(top_ref);
        const size_t top_size = m_top.Size();
        TIGHTDB_ASSERT(top_size >= 2);

        const size_t n_ref = m_top.Get(0);
        const size_t t_ref = m_top.Get(1);
        m_tableNames.UpdateRef(n_ref);
        m_tables.UpdateRef(t_ref);
        m_tableNames.SetParent(&m_top, 0);
        m_tables.SetParent(&m_top, 1);

        // Serialized files do not have free space markers
        // at all, and files that are not shared does not
        // need version info for free space.
        if (top_size >= 4) {
            const size_t fp_ref = m_top.Get(2);
            const size_t fl_ref = m_top.Get(3);
            m_freePositions.UpdateRef(fp_ref);
            m_freeLengths.UpdateRef(fl_ref);
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
    TIGHTDB_ASSERT(m_alloc.GetTopRef() == 0);
    if (!m_top.IsValid()) {
        m_isValid = true;
        return; // already in new state
    }

    // A shared group that has just been created and not yet
    // written to does not have any internal structures yet
    // (we may have to re-create this after a rollback)

    clear_cache();

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
    TIGHTDB_ASSERT(is_shared());

    // FIXME: I (Kristian) had to add this to avoid double deallocation in ~Group(), but is this the right fix? Alexander?
    invalidate();

    // Clear all changes made during transaction
//    m_alloc.FreeAll(); FIXME: Not needed if we keep the call to invalidate() above.
}

Group::~Group()
{
    if (m_top.IsValid()) {
        clear_cache();

        // Recursively deletes entire tree
        m_top.Destroy();
    }

    m_cachedtables.Destroy();
}

void Group::invalidate()
{
    //TODO: Should only invalidate object wrappers and never
    // touch the unferlying data (that may no longer be valid)
    clear_cache();

    m_top.Invalidate();
    m_tables.Invalidate();
    m_tableNames.Invalidate();
    m_freePositions.Invalidate();
    m_freeLengths.Invalidate();
    m_freeVersions.Invalidate();

    // FIXME: I (Kristian) had to add these to avoid a problem when resurrecting the arrays in create_from_ref() (top_ref==0). The problem is that if the parent is left as non-null, then Array::Alloc() will attempt to update the parent array, but the parent array is still empty at that point. I don't, however, think this is a sufficiently good fix? Alexander?
    m_tables.SetParent(0,0);
    m_tableNames.SetParent(0,0);
    m_freePositions.SetParent(0,0);
    m_freeLengths.SetParent(0,0);
    m_freeVersions.SetParent(0,0);

    // Reads may allocate some temproary state that we have
    // to clean up
    // TODO: This is also done in commit(), fix to do it only once
    m_alloc.FreeAll();

    m_isValid = false;
}

bool Group::is_empty() const
{
    if (!m_top.IsValid()) return true;

    return m_tableNames.is_empty();
}

bool Group::in_inital_state() const
{
    return !m_top.IsValid();
}

size_t Group::get_table_count() const
{
    if (!m_top.IsValid()) return 0;

    return m_tableNames.Size();
}

const char* Group::get_table_name(size_t table_ndx) const
{
    TIGHTDB_ASSERT(m_top.IsValid());
    TIGHTDB_ASSERT(table_ndx < m_tableNames.Size());

    return m_tableNames.Get(table_ndx);
}

Table* Group::get_table_ptr(size_t ndx)
{
    TIGHTDB_ASSERT(m_top.IsValid());
    TIGHTDB_ASSERT(ndx < m_tables.Size());

    // Get table from cache if exists, else create
    Table* table = reinterpret_cast<Table*>(m_cachedtables.Get(ndx));
    if (!table) {
        const size_t ref = m_tables.GetAsRef(ndx);
        table = new (nothrow) Table(Table::RefCountTag(), m_alloc, ref, this, ndx);
        if (!table) {
        error:
            throw_error(ERROR_OUT_OF_MEMORY);
        }
        table->bind_ref(); // The group has shared ownership
        if (!m_cachedtables.Set(ndx, intptr_t(table))) { // FIXME: intptr_t is not guaranteed to exists, even in C++11
            table->unbind_ref();
            goto error;
        }
    }
    return table;
}

Table* Group::create_new_table(const char* name)
{
    const size_t ref = Table::create_empty_table(m_alloc);
    if (!ref) {
    error:
        throw_error(ERROR_OUT_OF_MEMORY);
    }
    if (!m_tables.add(ref)) goto error;
    if (!m_tableNames.add(name)) goto error;
    Table* const table =
        new (nothrow) Table(Table::RefCountTag(), m_alloc, ref, this, m_tables.Size()-1);
    if (!table) goto error;
    table->bind_ref(); // The group has shared ownership
    if (!m_cachedtables.add(intptr_t(table))) { // FIXME: intptr_t is not guaranteed to exists, even in C++11
        table->unbind_ref();
        goto error;
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    Replication* repl = m_alloc.get_replication();
    if (repl) {
        error_code err = repl->new_top_level_table(name);
        if (err) throw_error(err);
    }
#endif
    return table;
}


bool Group::write(const char* filepath)
{
    TIGHTDB_ASSERT(filepath);
    TIGHTDB_ASSERT(m_top.IsValid());

    FileOStream out(filepath);
    if (!out.is_valid()) return false;

    write(out);

    return true;
}

char* Group::write_to_mem(size_t& len)
{
    TIGHTDB_ASSERT(m_top.IsValid());

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
    TIGHTDB_ASSERT(m_top.IsValid());
    TIGHTDB_ASSERT(readlock_version <= current_version);

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
    const size_t top_pos = out.Commit();

    // If the group is persisiting in single-thread (un-shared) mode
    // we have to make sure that the group stays valid after commit
    if (!is_shared()) {
        // Recusively update refs in all active tables (columns, arrays..)
        update_refs(top_pos);
    }
    else {
        TIGHTDB_ASSERT(m_alloc.IsAllFree());
        invalidate();
        TIGHTDB_ASSERT(m_alloc.IsAllFree());
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif

    return true;
}

void Group::update_refs(size_t topRef)
{
    // Update top with the new (persistent) ref
    m_top.UpdateRef(topRef);
    TIGHTDB_ASSERT(m_top.Size() >= 2);

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
        Table* const t = reinterpret_cast<Table*>(m_cachedtables.Get(i));
        if (t) {
            t->UpdateFromParent();
        }
    }
}

void Group::update_from_shared(size_t top_ref, size_t len)
{
    TIGHTDB_ASSERT(top_ref < len);

    // Update memory mapping if needed
    const bool isRemapped = m_alloc.ReMap(len);

    // If our last look at the file was when it
    // was empty, we may have to re-create the group
    if (in_inital_state() || top_ref == 0) {
        if (top_ref == 0)
            reset_to_new();    // may have been a rollback
        create_from_ref(top_ref);
        return;
    }

    // If the top has not changed, everything is up-to-date
    if (!isRemapped && top_ref == m_top.GetRef()) return;

    // Update group arrays
    m_top.UpdateRef(top_ref);
    TIGHTDB_ASSERT(m_top.Size() >= 2);
    const bool nameschanged = !m_tableNames.UpdateFromParent();
    m_tables.UpdateFromParent();
    if (m_top.Size() > 2) {
        m_freePositions.UpdateFromParent();
        m_freeLengths.UpdateFromParent();
        if (m_top.Size() > 4) {
            m_freeVersions.UpdateFromParent();
        }
    }

    // If the names of the tables in the group has not changed we know
    // that it still contains the same tables so we can reuse the
    // cached versions
    if (nameschanged) {
        clear_cache();

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
            Table* const t = reinterpret_cast<Table*>(m_cachedtables.Get(i));
            if (t) {
                t->UpdateFromParent();
            }
        }
    }
}

bool Group::operator==(const Group& g) const
{
    const size_t n = get_table_count();
    if (n != g.get_table_count()) return false;
    for (size_t i=0; i<n; ++i) {
        const Table* t1 = get_table_ptr(i);
        const Table* t2 = g.get_table_ptr(i);
        if (*t1 != *t2) return false;
    }
    return true;
}

void Group::to_string(std::ostream& out) const
{
    const size_t count = get_table_count();

    // Calculate widths
    size_t name_width = 6;
    size_t rows_width = 4;
    for (size_t i = 0; i < count; ++i) {
        const char* const name = get_table_name(i);
        const size_t len = strlen(name);
        if (name_width < len) name_width = len;

        ConstTableRef table = get_table(name);
        const size_t row_count = table->size();
        if (rows_width < row_count) rows_width = row_count;
    }

    // Print header
    out << "   ";
    out.width(name_width);
    out << "tables" << "  ";
    out.width(rows_width);
    out << "rows\n";

    // Print tables
    for (size_t i = 0; i < count; ++i) {
        const char* const name = get_table_name(i);
        ConstTableRef table = get_table(name);
        const size_t row_count = table->size();

        out << i << "  ";
        out.width(name_width);
        out.setf(std::ostream::left, std::ostream::adjustfield);
        out << name;
        out << "  ";
        out.width(rows_width);
        out.unsetf(std::ostream::adjustfield);
        out << row_count << std::endl;
    }
}

#ifdef TIGHTDB_DEBUG

void Group::Verify() const
{
    if (!is_valid()) return;

    // The file may have been created but not yet used
    // (so no structure has been initialized)
    const bool isShared = m_persistMode & GROUP_SHARED;
    if (isShared && m_alloc.GetTopRef() == 0 && !m_top.IsValid()) {
        TIGHTDB_ASSERT(!m_tables.IsValid());
        return;
    }

    // Verify free lists
    if (m_freePositions.IsValid()) {
        TIGHTDB_ASSERT(m_freeLengths.IsValid());

        const size_t count_p = m_freePositions.Size();
        const size_t count_l = m_freeLengths.Size();
        TIGHTDB_ASSERT(count_p == count_l);

        if (is_shared()) {
            TIGHTDB_ASSERT(m_freeVersions.IsValid());
            TIGHTDB_ASSERT(count_p == m_freeVersions.Size());
        }

        if (count_p) {
            // Check for alignment
            for (size_t i = 0; i < count_p; ++i) {
                const size_t p = m_freePositions.Get(i);
                const size_t l = m_freeLengths.Get(i);
                TIGHTDB_ASSERT((p & 0x7) == 0); // 64bit alignment
                TIGHTDB_ASSERT((l & 0x7) == 0); // 64bit alignment
            }

            const size_t filelen = m_alloc.GetFileLen();

            // Segments should be ordered and without overlap
            for (size_t i = 0; i < count_p-1; ++i) {
                const size_t pos1 = m_freePositions.Get(i);
                const size_t pos2 = m_freePositions.Get(i+1);
                TIGHTDB_ASSERT(pos1 < pos2);

                const size_t len1 = m_freeLengths.Get(i);
                TIGHTDB_ASSERT(len1 != 0);
                TIGHTDB_ASSERT(len1 < filelen);

                const size_t end = pos1 + len1;
                TIGHTDB_ASSERT(end <= pos2);
            }

            const size_t lastlen = m_freeLengths.back();
            TIGHTDB_ASSERT(lastlen != 0 && lastlen <= filelen);

            const size_t end = m_freePositions.back() + lastlen;
            TIGHTDB_ASSERT(end <= filelen);
        }
    }

    // Verify tables
    for (size_t i = 0; i < m_tables.Size(); ++i) {
        get_table_ptr(i)->Verify();
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

void Group::to_dot(std::ostream& out) const
{
    out << "digraph G {" << endl;

    out << "subgraph cluster_group {" << endl;
    out << " label = \"Group\";" << endl;

    m_top.ToDot(out, "group_top");
    m_tableNames.ToDot(out, "table_names");
    m_tables.ToDot(out, "tables");

    // Tables
    for (size_t i = 0; i < m_tables.Size(); ++i) {
        const Table* table = get_table_ptr(i);
        const char* const name = get_table_name(i);
        table->to_dot(out, name);
    }

    out << "}" << endl;
    out << "}" << endl;
}

void Group::to_dot() const
{
    to_dot(std::cerr);
}

#if !defined(_MSC_VER)
#include <sys/mman.h>
#endif

void Group::zero_free_space(size_t file_size, size_t readlock_version)
{
    static_cast<void>(readlock_version); // FIXME: Why is this parameter not used?

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

#endif // TIGHTDB_DEBUG

} //namespace tightdb
