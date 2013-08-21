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



void Group::open(const string& file_path, OpenMode mode)
{
    TIGHTDB_ASSERT(!is_attached());
    bool is_shared = false;
    bool read_only = mode == mode_ReadOnly;
    bool no_create = mode == mode_ReadWriteNoCreate;
    m_alloc.attach_file(file_path, is_shared, read_only, no_create);
    ref_type top_ref = m_alloc.get_top_ref();
    // FIXME: Must detach file from allocator if the following
    // throws. Unfortunatly we may already have created a file. If we
    // cannot avoid that, we must document the fact that a failed open
    // may still have the effect of creating the file.
    if (top_ref == 0) {
        // Attaching to a newly created file
        create(); // Throws
    }
    else {
        // Attaching to a pre-existing database
        init_from_ref(top_ref); // Throws
    }
}


void Group::open(BinaryData buffer, bool take_ownership)
{
    TIGHTDB_ASSERT(!is_attached());
    TIGHTDB_ASSERT(buffer.data());
    m_alloc.attach_buffer(const_cast<char*>(buffer.data()), buffer.size(), take_ownership);
    ref_type top_ref = m_alloc.get_top_ref();
    // FIXME: Must detach allocator if the following fails.
    if (top_ref == 0) {
        create(); // Throws
    }
    else {
        init_from_ref(top_ref); // Throws
    }
}


void Group::create()
{
    m_top.create(Array::type_HasRefs);
    m_tables.create(Array::type_HasRefs);
    m_table_names.create();
    m_free_positions.create(Array::type_Normal);
    m_free_lengths.create(Array::type_Normal);

    m_top.add(m_table_names.get_ref());
    m_top.add(m_tables.get_ref());
    m_top.add(m_free_positions.get_ref());
    m_top.add(m_free_lengths.get_ref());

    // We may have been attached to a newly created file, that is, to
    // a file consisting only of a default header and possibly some
    // free space. In that case, we must add as free space, the size
    // of the file minus its header.
    if (m_alloc.is_attached()) {
        size_t free = m_alloc.get_baseline() - sizeof SlabAlloc::default_header;
        if (free > 0) {
            m_free_positions.add(sizeof SlabAlloc::default_header);
            m_free_lengths.add(free);
        }
    }
}


void Group::init_from_ref(ref_type top_ref)
{
    m_top.init_from_ref(top_ref);
    size_t top_size = m_top.size();
    TIGHTDB_ASSERT(top_size >= 2);

    size_t names_ref = m_top.get_as_ref(0);
    size_t tables_ref = m_top.get_as_ref(1);
    m_table_names.init_from_ref(names_ref);
    m_tables.init_from_ref(tables_ref);

    // File created by Group::write() do not have free space markers
    // at all, and files that are not shared does not need version
    // info for free space.
    if (top_size > 2) {
        TIGHTDB_ASSERT(top_size >= 4);
        size_t fp_ref = m_top.get_as_ref(2);
        size_t fl_ref = m_top.get_as_ref(3);
        m_free_positions.init_from_ref(fp_ref);
        m_free_lengths.init_from_ref(fl_ref);

        if (m_is_shared && top_size > 4)
            m_free_versions.init_from_ref(m_top.get_as_ref(4));
    }

    // Make room for pointers to cached tables
    size_t n = m_tables.size();
    for (size_t i = 0; i < n; ++i) {
        m_cached_tables.add(0); // Throws
    }
}


void Group::init_shared()
{
    if (m_free_versions.is_attached()) {
        // If free space tracking is enabled
        // we just have to reset it
        m_free_versions.set_all_to_zero();
    }
    else {
        // Serialized files have no free space tracking
        // at all so we have to add the basic free lists
        if (m_top.size() == 2) {
            // FIXME: There is a risk that these are already
            // allocated, and that would cause a leak. This could
            // happen if an earlier commit attempt failed.
            TIGHTDB_ASSERT(!m_free_positions.is_attached());
            TIGHTDB_ASSERT(!m_free_lengths.is_attached());
            m_free_positions.create(Array::type_Normal);
            m_free_lengths.create(Array::type_Normal);
            m_top.add(m_free_positions.get_ref());
            m_top.add(m_free_lengths.get_ref());
        }

        // Files that have only been used in single thread
        // mode do not have version tracking for the free lists
        if (m_top.size() == 4) {
            // FIXME: There is a risk that this one is already
            // allocated, and that would cause a leak. This could
            // happen if an earlier commit attempt failed.
            TIGHTDB_ASSERT(!m_free_versions.is_attached());
            m_free_versions.create(Array::type_Normal);
            size_t n = m_free_positions.size();
            for (size_t i = 0; i < n; ++i)
                m_free_versions.add(0);
            m_top.add(m_free_versions.get_ref());
        }
    }
}


Group::~Group()
{
    if (m_top.is_attached()) {
        clear_cache();

        // Recursively deletes entire tree
        m_top.destroy();
    }

    m_cached_tables.destroy();
}


void Group::invalidate()
{
    // TODO: Should only invalidate object wrappers and never touch
    // the underlying data (that may no longer be valid)
    clear_cache();

    m_top.detach();
    m_tables.detach();
    m_table_names.detach();
    m_free_positions.detach();
    m_free_lengths.detach();
    m_free_versions.detach();

    // Reads may allocate some temproary state that we have
    // to clean up
    m_alloc.free_all();
}


Table* Group::get_table_ptr(size_t ndx)
{
    TIGHTDB_ASSERT(m_top.is_attached());
    TIGHTDB_ASSERT(ndx < m_tables.size());

    // Get table from cache if exists, else create
    Table* table = reinterpret_cast<Table*>(m_cached_tables.get(ndx));
    if (!table) {
        const size_t ref = m_tables.get_as_ref(ndx);
        Table::UnbindGuard t(new Table(Table::RefCountTag(), m_alloc, ref, this, ndx)); // Throws
        t->bind_ref(); // Increase reference count to 1
        m_cached_tables.set(ndx, intptr_t(t.get())); // FIXME: intptr_t is not guaranteed to exists, even in C++11
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
    m_table_names.add(name);
    Table::UnbindGuard table(new Table(Table::RefCountTag(), m_alloc,
                                       ref, this, m_tables.size()-1)); // Throws
    table->bind_ref(); // Increase reference count to 1
    m_cached_tables.add(intptr_t(table.get())); // FIXME: intptr_t is not guaranteed to exists, even in C++11

#ifdef TIGHTDB_ENABLE_REPLICATION
    Replication* repl = m_alloc.get_replication();
    if (repl)
        repl->new_top_level_table(name); // Throws
#endif

    // This group shares ownership of the table, so leave reference
    // count at 1.
    return table.release();
}


void Group::write(const string& path) const
{
    TIGHTDB_ASSERT(m_top.is_attached());

    FileOStream out(path);
    write_to_stream(out);
}


BinaryData Group::write_to_mem() const
{
    TIGHTDB_ASSERT(m_top.is_attached());

    // Get max possible size of buffer
    size_t max_size = m_alloc.get_total_size();

    MemoryOStream out(max_size);
    size_t size = write_to_stream(out);

    char* data = out.release_buffer();
    return BinaryData(data, size);
}


void Group::commit()
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_top.is_attached());

    // GroupWriter::commit() needs free-space tracking information, so
    // if the attached database does not contain it, we must add it
    // now. Empty (newly created) database files and database files
    // created by Group::write() do not have free-space tracking
    // information.
    if (m_free_positions.is_attached()) {
        TIGHTDB_ASSERT(m_top.size() == 4 || m_top.size() == 5);
        if (m_top.size() > 4) {
            // Delete free-list version information
            Array::destroy(m_top.get_as_ref(4), m_top.get_alloc());
            m_top.erase(4);
        }
    }
    else {
        TIGHTDB_ASSERT(m_top.size() == 2);
        m_free_positions.create(Array::type_Normal);
        m_free_lengths.create(Array::type_Normal);
        m_top.add(m_free_positions.get_ref());
        m_top.add(m_free_lengths.get_ref());
    }

    GroupWriter out(*this);

    // Recursively write all changed arrays to the database file
    bool do_sync = true;
    ref_type top_ref = out.commit(do_sync);

    // Since the group is persisiting in single-thread (un-shared)
    // mode we have to make sure that the group stays valid after
    // commit

    // Mark all managed space (beyond the atatched file) as free
    m_alloc.free_all();

    size_t old_baseline = m_alloc.get_baseline();

    // Remap file if it has grown
    size_t new_file_size = out.get_file_size();
    TIGHTDB_ASSERT(new_file_size >= m_alloc.get_baseline());
    if (new_file_size > m_alloc.get_baseline()) {
        if (m_alloc.remap(new_file_size)) {
            // The file was mapped to a new address, so all array
            // accessors must be updated.
            old_baseline = 0;
        }
    }

    // Recusively update refs in all active tables (columns, arrays..)
    update_refs(top_ref, old_baseline);

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}


void Group::update_refs(ref_type top_ref, size_t old_baseline)
{
    TIGHTDB_ASSERT(!m_free_versions.is_attached());

    // After Group::commit() we will always have free space tracking
    // info.
    TIGHTDB_ASSERT(m_top.size() == 4 || m_top.size() == 5);

    // Array nodes that a part of the previous version of the database
    // will not be overwritte by Group::commit(). This is necessary
    // for robustness in the face of abrupt termination of the
    // process. It also means that we can be sure that an array
    // remains unchanged across a commit if the new ref is equal to
    // the old ref and the ref is below the previous basline.

    if (top_ref < old_baseline && m_top.get_ref() == top_ref)
        return;

    m_top.init_from_ref(top_ref);

    // Now we can update it's child arrays
    m_table_names.update_from_parent(old_baseline);
    m_free_positions.update_from_parent(old_baseline);
    m_free_lengths.update_from_parent(old_baseline);

    // If m_tables has not been modfied we don't
    // need to update attached table accessors
    if (!m_tables.update_from_parent(old_baseline))
        return;

    // Update all attached table accessors including those attached to
    // subtables.
    size_t n = m_cached_tables.size();
    for (size_t i = 0; i < n; ++i) {
        Table* t = reinterpret_cast<Table*>(m_cached_tables.get(i));
        if (t) {
            t->update_from_parent(old_baseline);
        }
    }
}


void Group::update_from_shared(ref_type new_top_ref, size_t new_file_size)
{
    TIGHTDB_ASSERT(new_top_ref < new_file_size);
    TIGHTDB_ASSERT(!m_top.is_attached());

    // Update memory mapping if database file has grown
    TIGHTDB_ASSERT(new_file_size >= m_alloc.get_baseline());
    if (new_file_size > m_alloc.get_baseline())
        m_alloc.remap(new_file_size);

    // If our last look at the file was when it
    // was empty, we may have to re-create the group
    if (new_top_ref == 0) {
        create(); // Throws

        m_free_versions.create(Array::type_Normal);
        m_top.add(m_free_versions.get_ref());
        m_free_versions.add(0);
        return;
    }

    init_from_ref(new_top_ref); // Throws
}


bool Group::operator==(const Group& g) const
{
    size_t n = size();
    if (n != g.size())
        return false;
    for (size_t i=0; i<n; ++i) {
        const Table* t1 = get_table_ptr(i);
        const Table* t2 = g.get_table_ptr(i);
        if (*t1 != *t2)
            return false;
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
    TIGHTDB_ASSERT(m_top.is_attached() || !is_attached());

    if (!m_top.is_attached())
        return;

    // Verify free lists
    if (m_free_positions.is_attached()) {
        TIGHTDB_ASSERT(m_free_lengths.is_attached());

        size_t n = m_free_positions.size();
        TIGHTDB_ASSERT(n == m_free_lengths.size());

        if (m_free_versions.is_attached())
            TIGHTDB_ASSERT(n == m_free_versions.size());

        // FIXME: What we really need here is the "logical" size of
        // the file and not the real size. The real size may have
        // changed without the free space information having been
        // adjusted accordingly. This can happen, for example, if
        // commit() fails before writing the new top-ref, but after
        // having extended the file size. We currently do not have a
        // concept of a logical file size, but if provided, it would
        // have to be stored as part of a database version such that
        // it is updated atomically together with the rest of the
        // contents of the version.
        size_t file_size = m_alloc.is_attached() ? m_alloc.get_baseline() : 0;

        size_t prev_end = 0;
        for (size_t i = 0; i < n; ++i) {
            size_t pos  = to_size_t(m_free_positions.get(i));
            size_t size = to_size_t(m_free_lengths.get(i));

            TIGHTDB_ASSERT(pos < file_size);
            TIGHTDB_ASSERT(size > 0);
            TIGHTDB_ASSERT(pos + size <= file_size);
            TIGHTDB_ASSERT(prev_end <= pos);

            TIGHTDB_ASSERT(pos  % 8 == 0); // 8-byte alignment
            TIGHTDB_ASSERT(size % 8 == 0); // 8-byte alignment

            prev_end = pos + size;
        }
    }

    // Verify tables
    {
        size_t n = m_tables.size();
        for (size_t i = 0; i < n; ++i)
            get_table_ptr(i)->Verify();
    }
}


MemStats Group::stats()
{
    MemStats stats;
    m_top.stats(stats);

    return stats;
}


void Group::print() const
{
    m_alloc.print();
}


void Group::print_free() const
{
    if (!m_free_positions.is_attached()) {
        printf("none\n");
        return;
    }
    bool has_versions = m_free_versions.is_attached();

    size_t count = m_free_positions.size();
    for (size_t i = 0; i < count; ++i) {
        size_t pos  = to_size_t(m_free_positions[i]);
        size_t size = to_size_t(m_free_lengths[i]);
        printf("%d: %d %d", int(i), int(pos), int(size));

        if (has_versions) {
            size_t version = to_size_t(m_free_versions[i]);
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
    m_table_names.to_dot(out, "table_names");
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


void Group::to_dot(const char* file_path) const
{
    ofstream out(file_path);
    to_dot(out);
}


void Group::zero_free_space(size_t file_size, size_t readlock_version)
{
    static_cast<void>(readlock_version); // FIXME: Why is this parameter not used?

    if (!m_is_shared)
        return;

    File::Map<char> map(m_alloc.m_file, File::access_ReadWrite, file_size);

    size_t count = m_free_positions.size();
    for (size_t i = 0; i < count; ++i) {
        size_t v = to_size_t(m_free_versions.get(i)); // todo, remove assizet when 64 bit
        if (v >= m_readlock_version)
            continue;

        size_t pos = to_size_t(m_free_positions.get(i));
        size_t len = to_size_t(m_free_lengths.get(i));

        char* p = map.get_addr() + pos;
        fill(p, p+len, 0);
    }
}

#endif // TIGHTDB_DEBUG
