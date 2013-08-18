#include <cerrno>
#include <new>
#include <algorithm>
#include <iostream>
#include <iomanip>
#include <fstream>

#include <tightdb/terminate.hpp>
#include <tightdb/group_writer.hpp>
#include <tightdb/group.hpp>
#include <tightdb/utilities.hpp>

using namespace std;
using namespace tightdb;

namespace {

class Initialization {
public:
    Initialization()
    {
        tightdb::cpuid_init();
    }
};

Initialization initialization;

class MemoryOStream {
public:
    MemoryOStream(size_t buffer_size): m_pos(0)
    {
        m_buffer = static_cast<char*>(malloc(buffer_size));
        if (!m_buffer) throw bad_alloc();
    }

    ~MemoryOStream()
    {
        free(m_buffer);
    }

    size_t getpos() const {return m_pos;}

    size_t write(const char* p, size_t n)
    {
        const size_t pos = m_pos;
        copy(p, p+n, m_buffer+m_pos);
        m_pos += n;
        return pos;
    }
    void seek(size_t pos) {m_pos = pos;}

    char* release_buffer() TIGHTDB_NOEXCEPT
    {
        char* const buffer = m_buffer;
        m_buffer = 0;
        return buffer;
    }

private:
    size_t m_pos;
    char* m_buffer;
};

class FileOStream {
public:
    FileOStream(const string& path): m_pos(0), m_streambuf(&m_file), m_out(&m_streambuf)
    {
        m_file.open(path, File::access_ReadWrite, File::create_Must, 0);
    }

    size_t getpos() const { return m_pos; }

    size_t write(const char* data, size_t size)
    {
        size_t size_0 = size;

        // Handle the case where 'size_t' has a larger range than 'streamsize'
        streamsize max_streamsize = numeric_limits<streamsize>::max();
        size_t max_put = numeric_limits<size_t>::max();
        if (int_less_than(max_streamsize, max_put))
            max_put = size_t(max_streamsize);
        while (max_put < size) {
            m_out.write(data, max_put);
            data += max_put;
            size -= max_put;
        }

        m_out.write(data, size);

        size_t pos = m_pos;
        if (int_add_with_overflow_detect(m_pos, size_0))
            throw runtime_error("File size overflow");
        return pos;
    }

    void seek(size_t pos)
    {
        streamsize pos2 = 0;
        if (int_cast_with_overflow_detect(pos, pos2))
            throw std::runtime_error("Seek position overflow");
        m_out.seekp(pos2);
    }

private:
    size_t m_pos;
    File m_file;
    File::Streambuf m_streambuf;
    ostream m_out;
};

} // anonymous namespace


namespace tightdb {

void Group::create_from_file(const string& filename, OpenMode mode, bool do_init)
{
    // Memory map file
    // This leaves the group ready, but in invalid state
    bool read_only = mode == mode_ReadOnly;
    bool no_create = mode == mode_ReadWriteNoCreate;
    m_alloc.attach_file(filename, m_is_shared, read_only, no_create);

    if (!do_init)  return;

    ref_type top_ref = m_alloc.get_top_ref();

    // if we just created shared group, we have to wait with
    // actually creating it's datastructures until first write
    if (m_is_shared && top_ref == 0) return;

    create_from_ref(top_ref); // FIXME: Throws and leaves the Group in peril
}

// Create a new memory structure and attach this group instance to it.
void Group::create()
{
    m_tables.set_type(Array::type_HasRefs); // FIXME: Why is this not done in Group() like the rest of the arrays?

    m_top.add(m_tableNames.get_ref());
    m_top.add(m_tables.get_ref());
    m_top.add(m_freePositions.get_ref());
    m_top.add(m_freeLengths.get_ref());

    // Set parent info
    m_tableNames.set_parent(&m_top, 0);
    m_tables.set_parent(&m_top, 1);
    m_freePositions.set_parent(&m_top, 2);
    m_freeLengths.set_parent(&m_top, 3);

    if (m_freeVersions.IsValid()) {
        m_top.add(m_freeVersions.get_ref());
        m_freeVersions.set_parent(&m_top, 4);
    }
}

// Attach this group instance to a preexisting memory structure.
void Group::create_from_ref(ref_type top_ref)
{
    // Instantiate top arrays
    if (top_ref == 0) {
        m_top.set_type(Array::type_HasRefs);
        m_tables.set_type(Array::type_HasRefs);
        m_tableNames.set_type(Array::type_Normal);
        m_freePositions.set_type(Array::type_Normal);
        m_freeLengths.set_type(Array::type_Normal);
        if (m_is_shared) {
            m_freeVersions.set_type(Array::type_Normal);
        }

        create();

        // Everything but header is free space
        m_freePositions.add(sizeof SlabAlloc::default_header);
        m_freeLengths.add(m_alloc.get_base_size() - sizeof SlabAlloc::default_header);
        if (m_is_shared)
            m_freeVersions.add(0);
    }
    else {
        m_top.update_ref(top_ref);
        const size_t top_size = m_top.size();
        TIGHTDB_ASSERT(top_size >= 2);

        const size_t n_ref = m_top.get_as_ref(0);
        const size_t t_ref = m_top.get_as_ref(1);
        m_tableNames.update_ref(n_ref);
        m_tables.update_ref(t_ref);
        m_tableNames.set_parent(&m_top, 0);
        m_tables.set_parent(&m_top, 1);

        // Serialized files do not have free space markers
        // at all, and files that are not shared does not
        // need version info for free space.
        if (top_size >= 4) {
            const size_t fp_ref = m_top.get_as_ref(2);
            const size_t fl_ref = m_top.get_as_ref(3);
            m_freePositions.update_ref(fp_ref);
            m_freeLengths.update_ref(fl_ref);
            m_freePositions.set_parent(&m_top, 2);
            m_freeLengths.set_parent(&m_top, 3);
        }
        if (top_size == 5) {
            m_freeVersions.update_ref(m_top.get_as_ref(4));
            m_freeVersions.set_parent(&m_top, 4);
        }

        // Make room for pointers to cached tables
        size_t count = m_tables.size();
        for (size_t i = 0; i < count; ++i) {
            m_cachedtables.add(0);
        }
    }
}

void Group::init_shared()
{
    if (m_freeVersions.IsValid()) {
        // If free space tracking is enabled
        // we just have to reset it
        m_freeVersions.SetAllToZero();
    }
    else {
        // Serialized files have no free space tracking
        // at all so we have to add the basic free lists
        if (m_top.size() == 2) {
            m_freePositions.set_type(Array::type_Normal);
            m_freeLengths.set_type(Array::type_Normal);
            m_top.add(m_freePositions.get_ref());
            m_top.add(m_freeLengths.get_ref());
            m_freePositions.set_parent(&m_top, 2);
            m_freeLengths.set_parent(&m_top, 3);
        }

        // Files that have only been used in single thread
        // mode do not have version tracking for the free lists
        if (m_top.size() == 4) {
            const size_t count = m_freePositions.size();
            m_freeVersions.set_type(Array::type_Normal);
            for (size_t i = 0; i < count; ++i) {
                m_freeVersions.add(0);
            }
            m_top.add(m_freeVersions.get_ref());
            m_freeVersions.set_parent(&m_top, 4);
        }
    }
}

void Group::reset_to_new()
{
    TIGHTDB_ASSERT(m_alloc.get_top_ref() == 0);
    if (!m_top.IsValid()) {
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

    m_tableNames.set_parent(0, 0);
    m_tables.set_parent(0, 0);
    m_freePositions.set_parent(0, 0);
    m_freeLengths.set_parent(0, 0);
    m_freeVersions.set_parent(0, 0);
}

void Group::rollback()
{
    TIGHTDB_ASSERT(m_is_shared);

    // FIXME: I (Kristian) had to add this to avoid double deallocation in ~Group(), but is this the right fix? Alexander?
    invalidate();

    // Clear all changes made during transaction
//    m_alloc.free_all(); FIXME: Not needed if we keep the call to invalidate() above.
}

Group::~Group()
{
    if (m_top.IsValid()) {
        clear_cache();

        // Recursively deletes entire tree
        m_top.destroy();
    }

    m_cachedtables.destroy();
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

    // FIXME: I (Kristian) had to add these to avoid a problem when resurrecting the arrays in create_from_ref() (top_ref==0). The problem is that if the parent is left as non-null, then Array::alloc() will attempt to update the parent array, but the parent array is still empty at that point. I don't, however, think this is a sufficiently good fix? Alexander?
    m_tables.set_parent(0,0);
    m_tableNames.set_parent(0,0);
    m_freePositions.set_parent(0,0);
    m_freeLengths.set_parent(0,0);
    m_freeVersions.set_parent(0,0);

    // Reads may allocate some temproary state that we have
    // to clean up
    // TODO: This is also done in commit(), fix to do it only once
    m_alloc.free_all();
}

Table* Group::get_table_ptr(size_t ndx)
{
    TIGHTDB_ASSERT(m_top.IsValid());
    TIGHTDB_ASSERT(ndx < m_tables.size());

    // Get table from cache if exists, else create
    Table* table = reinterpret_cast<Table*>(m_cachedtables.get(ndx));
    if (!table) {
        const size_t ref = m_tables.get_as_ref(ndx);
        Table::UnbindGuard t(new Table(Table::RefCountTag(), m_alloc, ref, this, ndx)); // Throws
        t->bind_ref(); // Increase reference count to 1
        m_cachedtables.set(ndx, intptr_t(t.get())); // FIXME: intptr_t is not guaranteed to exists, even in C++11
        // This group shares ownership of the table, so leave
        // reference count at 1.
        table = t.release();
    }
    return table;
}

Table* Group::create_new_table(StringData name)
{
    ref_type ref = Table::create_empty_table(m_alloc); // Throws
    m_tables.add(ref);
    m_tableNames.add(name);
    Table::UnbindGuard table(new Table(Table::RefCountTag(), m_alloc,
                                       ref, this, m_tables.size()-1)); // Throws
    table->bind_ref(); // Increase reference count to 1
    m_cachedtables.add(intptr_t(table.get())); // FIXME: intptr_t is not guaranteed to exists, even in C++11

#ifdef TIGHTDB_ENABLE_REPLICATION
    Replication* repl = m_alloc.get_replication();
    if (repl) repl->new_top_level_table(name); // Throws
#endif

    // This group shares ownership of the table, so leave reference
    // count at 1.
    return table.release();
}


void Group::write(const string& path) const
{
    TIGHTDB_ASSERT(m_top.IsValid());

    FileOStream out(path);
    write_to_stream(out);
}

BinaryData Group::write_to_mem() const
{
    TIGHTDB_ASSERT(m_top.IsValid());

    // Get max possible size of buffer
    size_t max_size = m_alloc.get_total_size();

    MemoryOStream out(max_size);
    size_t size = write_to_stream(out);

    char* data = out.release_buffer();
    return BinaryData(data, size);
}

// NOTE: This method must not modify *this if m_shared is false.
size_t Group::commit(size_t current_version, size_t readlock_version, bool persist)
{
    TIGHTDB_ASSERT(m_top.IsValid());
    TIGHTDB_ASSERT(readlock_version <= current_version);

    // FIXME: Under what circumstances can this even happen????
    // FIXME: What about when a user owned read-only buffer is attached?
    if (!m_alloc.is_attached()) throw runtime_error("Cannot persist");

    // If we have an empty db file, we can just serialize directly
    //if (m_alloc.get_top_ref() == 0) {}

    GroupWriter out(*this, persist);

    if (m_is_shared) {
        m_readlock_version = readlock_version;
        out.SetVersions(current_version, readlock_version);
    }

    // Recursively write all changed arrays to end of file
    const size_t top_pos = out.commit();

    // If the group is persisiting in single-thread (un-shared) mode
    // we have to make sure that the group stays valid after commit
    if (!m_is_shared) {
        // Recusively update refs in all active tables (columns, arrays..)
        update_refs(top_pos);

#ifdef TIGHTDB_DEBUG
        Verify();
#endif
    }
    else {
        TIGHTDB_ASSERT(m_alloc.is_all_free());
        invalidate();
        TIGHTDB_ASSERT(m_alloc.is_all_free());
    }

    return top_pos;
}

void Group::update_refs(ref_type top_ref)
{
    // Update top with the new (persistent) ref
    m_top.update_ref(top_ref);
    TIGHTDB_ASSERT(m_top.size() >= 2);

    // Now we can update it's child arrays
    m_tableNames.update_from_parent();

    // No free-info in serialized databases
    // and version info is only in shared,
    if (m_top.size() >= 4) {
        m_freePositions.update_from_parent();
        m_freeLengths.update_from_parent();
    }
    else {
        m_freePositions.Invalidate();
        m_freeLengths.Invalidate();
    }
    if (m_top.size() == 5) {
        m_freeVersions.update_from_parent();
    }
    else {
        m_freeVersions.Invalidate();
    }

    // if the tables have not been modfied we don't
    // need to update cached tables
    if (!m_tables.update_from_parent()) return;

    // Also update cached tables
    size_t n = m_cachedtables.size();
    for (size_t i = 0; i < n; ++i) {
        Table* const t = reinterpret_cast<Table*>(m_cachedtables.get(i));
        if (t) {
            t->update_from_parent();
        }
    }
}

void Group::update_from_shared(ref_type top_ref, size_t len)
{
    TIGHTDB_ASSERT(top_ref < len);

    // Update memory mapping if needed
    bool is_remapped = m_alloc.remap(len);

    // If our last look at the file was when it
    // was empty, we may have to re-create the group
    if (in_initial_state() || top_ref == 0) {
        if (top_ref == 0)
            reset_to_new();    // may have been a rollback
        create_from_ref(top_ref);
        return;
    }

    // If the top has not changed, everything is up-to-date
    if (!is_remapped && top_ref == m_top.get_ref()) return;

    // Update group arrays
    m_top.update_ref(top_ref);
    TIGHTDB_ASSERT(m_top.size() >= 2);
    bool names_changed = !m_tableNames.update_from_parent();
    m_tables.update_from_parent();
    if (m_top.size() > 2) {
        m_freePositions.update_from_parent();
        m_freeLengths.update_from_parent();
        if (m_top.size() > 4) {
            m_freeVersions.update_from_parent();
        }
    }

    // If the names of the tables in the group has not changed we know
    // that it still contains the same tables so we can reuse the
    // cached versions
    if (names_changed) {
        clear_cache();

        // Make room for new pointers to cached tables
        size_t n = m_tables.size();
        for (size_t i = 0; i < n; ++i) {
            m_cachedtables.add(0);
        }
    }
    else {
        // Update cached tables
        //TODO: account for changed spec
        size_t n = m_cachedtables.size();
        for (size_t i = 0; i < n; ++i) {
            if (Table* t = reinterpret_cast<Table*>(m_cachedtables.get(i))) {
                t->update_from_parent();
            }
        }
    }
}

bool Group::operator==(const Group& g) const
{
    size_t n = size();
    if (n != g.size()) return false;
    for (size_t i=0; i<n; ++i) {
        const Table* t1 = get_table_ptr(i);
        const Table* t2 = g.get_table_ptr(i);
        if (*t1 != *t2) return false;
    }
    return true;
}

void Group::to_string(ostream& out) const
{
    // Calculate widths
    size_t index_width = 4;
    size_t name_width = 10;
    size_t rows_width = 6;
    size_t count = size();
    for (size_t i = 0; i < count; ++i) {
        StringData name = get_table_name(i);
        if (name_width < name.size()) {
            name_width = name.size();
        }

        ConstTableRef table = get_table(name);
        size_t row_count = table->size();
        if (rows_width < row_count) { // FIXME: should be the number of digits in row_count: floor(log10(row_count+1))
            rows_width = row_count;
        }
    }


    // Print header
    out << setw(index_width+1) << left << " ";
    out << setw(name_width+1)  << left << "tables";
    out << setw(rows_width)    << left << "rows"    << endl;

    // Print tables
    for (size_t i = 0; i < count; ++i) {
        StringData name = get_table_name(i);
        ConstTableRef table = get_table(name);
        size_t row_count = table->size();

        out << setw(index_width) << right << i           << " ";
        out << setw(name_width)  << left  << name.data() << " ";
        out << setw(rows_width)  << left  << row_count   << endl;
    }
}

#ifdef TIGHTDB_DEBUG

void Group::Verify() const
{
    // The file may have been created but not yet used
    // (so no structure has been initialized)
    if (m_is_shared && m_alloc.get_top_ref() == 0 && !m_top.IsValid()) {
        TIGHTDB_ASSERT(!m_tables.IsValid());
        return;
    }

    // Verify free lists
    if (m_freePositions.IsValid()) {
        TIGHTDB_ASSERT(m_freeLengths.IsValid());

        size_t count_p = m_freePositions.size();
        size_t count_l = m_freeLengths.size();
        TIGHTDB_ASSERT(count_p == count_l);

        if (m_is_shared) {
            TIGHTDB_ASSERT(m_freeVersions.IsValid());
            TIGHTDB_ASSERT(count_p == m_freeVersions.size());
        }

        if (count_p) {
            // Check for alignment
            for (size_t i = 0; i < count_p; ++i) {
                size_t p = to_size_t(m_freePositions.get(i));
                size_t l = to_size_t(m_freeLengths.get(i));
                TIGHTDB_ASSERT((p & 0x7) == 0); // 64bit alignment
                TIGHTDB_ASSERT((l & 0x7) == 0); // 64bit alignment
            }

            size_t filelen = m_alloc.get_base_size();

            // Segments should be ordered and without overlap
            for (size_t i = 0; i < count_p-1; ++i) {
                size_t pos1 = to_size_t(m_freePositions.get(i));
                size_t pos2 = to_size_t(m_freePositions.get(i+1));
                TIGHTDB_ASSERT(pos1 < pos2);

                size_t len1 = to_size_t(m_freeLengths.get(i));
                TIGHTDB_ASSERT(len1 != 0);
                TIGHTDB_ASSERT(len1 < filelen);

                size_t end = pos1 + len1;
                TIGHTDB_ASSERT(end <= pos2);
            }

            size_t lastlen = to_size_t(m_freeLengths.back());
            TIGHTDB_ASSERT(lastlen != 0 && lastlen <= filelen);

            size_t end = to_size_t(m_freePositions.back() + lastlen);
            TIGHTDB_ASSERT(end <= filelen);
        }
    }

    // Verify tables
    for (size_t i = 0; i < m_tables.size(); ++i) {
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
    m_alloc.print();
}

void Group::print_free() const
{
    if (!m_freePositions.IsValid()) {
        printf("none\n");
        return;
    }
    bool has_versions = m_freeVersions.IsValid();

    size_t count = m_freePositions.size();
    for (size_t i = 0; i < count; ++i) {
        size_t pos  = to_size_t(m_freePositions[i]);
        size_t size = to_size_t(m_freeLengths[i]);
        printf("%d: %d %d", int(i), int(pos), int(size));

        if (has_versions) {
            size_t version = to_size_t(m_freeVersions[i]);
            printf(" %d", int(version));
        }
        printf("\n");
    }
    printf("\n");
}

void Group::to_dot(ostream& out) const
{
    out << "digraph G {" << endl;

    out << "subgraph cluster_group {" << endl;
    out << " label = \"Group\";" << endl;

    m_top.to_dot(out, "group_top");
    m_tableNames.to_dot(out, "table_names");
    m_tables.to_dot(out, "tables");

    // Tables
    for (size_t i = 0; i < m_tables.size(); ++i) {
        const Table* table = get_table_ptr(i);
        StringData name = get_table_name(i);
        table->to_dot(out, name);
    }

    out << "}" << endl;
    out << "}" << endl;
}

void Group::to_dot() const
{
    to_dot(cerr);
}

void Group::zero_free_space(size_t file_size, size_t readlock_version)
{
    static_cast<void>(readlock_version); // FIXME: Why is this parameter not used?

    if (!m_is_shared) return;

    File::Map<char> map(m_alloc.m_file, File::access_ReadWrite, file_size);

    size_t count = m_freePositions.size();
    for (size_t i = 0; i < count; ++i) {
        size_t v = to_size_t(m_freeVersions.get(i)); // todo, remove assizet when 64 bit
        if (v >= m_readlock_version) continue;

        size_t pos = to_size_t(m_freePositions.get(i));
        size_t len = to_size_t(m_freeLengths.get(i));

        char* p = map.get_addr() + pos;
        fill(p, p+len, 0);
    }
}

#endif // TIGHTDB_DEBUG

} //namespace tightdb
