#include <tightdb/spec.hpp>

#ifdef TIGHTDB_ENABLE_REPLICATION
#include <tightdb/replication.hpp>
#endif

using namespace std;

namespace tightdb {

Spec::~Spec()
{
#ifdef TIGHTDB_ENABLE_REPLICATION
    Replication* repl = m_specSet.get_alloc().get_replication();
    if (repl) repl->on_spec_destroyed(this);
#endif
}

void Spec::init_from_ref(size_t ref, ArrayParent* parent, size_t ndx_in_parent)
{
    m_specSet.update_ref(ref);
    m_specSet.set_parent(parent, ndx_in_parent);
    TIGHTDB_ASSERT(m_specSet.size() == 2 || m_specSet.size() == 3);

    m_spec.update_ref(m_specSet.get_as_ref(0));
    m_spec.set_parent(&m_specSet, 0);
    m_names.update_ref(m_specSet.get_as_ref(1));
    m_names.set_parent(&m_specSet, 1);

    // SubSpecs array is only there when there are subtables
    if (m_specSet.size() == 3) {
        m_subSpecs.update_ref(m_specSet.get_as_ref(2));
        m_subSpecs.set_parent(&m_specSet, 2);
    }
}

void Spec::destroy()
{
    m_specSet.Destroy();
}

size_t Spec::get_ref() const
{
    return m_specSet.get_ref();
}

void Spec::update_ref(size_t ref, ArrayParent* parent, size_t pndx)
{
    init_from_ref(ref, parent, pndx);
}

void Spec::set_parent(ArrayParent* parent, size_t pndx)
{
    m_specSet.set_parent(parent, pndx);
}

bool Spec::update_from_parent()
{
    if (m_specSet.UpdateFromParent()) {
        m_spec.UpdateFromParent();
        m_names.UpdateFromParent();
        if (m_specSet.size() == 3) {
            m_subSpecs.UpdateFromParent();
        }
        return true;
    }
    else return false;
}

size_t Spec::add_column(DataType type, StringData name, ColumnType attr)
{
    m_names.add(name);
    m_spec.add(type);

    // We can set column attribute on creation
    // TODO: add to replication log
    if (attr != col_attr_None) {
        const size_t column_ndx = m_names.size()-1;
        set_column_attr(column_ndx, attr);
    }

    if (type == type_Table) {
        // SubSpecs array is only there when there are subtables
        if (m_specSet.size() == 2) {
            m_subSpecs.set_type(Array::coldef_HasRefs);
            //m_subSpecs.set_type((ColumnDef)4);
            //return;
            m_specSet.add(m_subSpecs.get_ref());
            m_subSpecs.set_parent(&m_specSet, 2);
        }

        Allocator& alloc = m_specSet.get_alloc();

        // Create spec for new subtable
        Array spec(Array::coldef_Normal, NULL, 0, alloc);
        ArrayString names(NULL, 0, alloc);
        Array spec_set(Array::coldef_HasRefs, NULL, 0, alloc);
        spec_set.add(spec.get_ref());
        spec_set.add(names.get_ref());

        // Add to list of subspecs
        const size_t ref = spec_set.get_ref();
        m_subSpecs.add(ref);
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    Replication* repl = m_spec_set.get_alloc().get_replication();
    if (repl) repl->add_column(m_table, this, type, name); // Throws
#endif

    return (m_names.size()-1); // column_ndx
}

size_t Spec::add_subcolumn(const vector<size_t>& column_path, DataType type, StringData name)
{
    TIGHTDB_ASSERT(!column_path.empty());
    return do_add_subcolumn(column_path, 0, type, name);
}

size_t Spec::do_add_subcolumn(const vector<size_t>& column_ids, size_t pos, DataType type, StringData name)
{
    const size_t column_ndx = column_ids[pos];
    Spec subspec = get_subtable_spec(column_ndx);

    if (pos == column_ids.size()-1) {
        return subspec.add_column(type, name);
    }
    else {
        return subspec.do_add_subcolumn(column_ids, pos+1, type, name);
    }
}

Spec Spec::add_subtable_column(StringData name)
{
    const size_t column_ndx = m_names.size();
    add_column(type_Table, name);

    return get_subtable_spec(column_ndx);
}

void Spec::rename_column(size_t column_ndx, StringData newname)
{
    TIGHTDB_ASSERT(column_ndx < m_spec.size());

    //TODO: Verify that new name is valid

    m_names.set(column_ndx, newname);
}

void Spec::rename_column(const vector<size_t>& column_ids, StringData name)
{
    do_rename_column(column_ids, 0, name);
}

void Spec::do_rename_column(const vector<size_t>& column_ids, size_t pos, StringData name)
{
    const size_t column_ndx = column_ids[pos];

    if (pos == column_ids.size()-1) {
        rename_column(column_ndx, name);
    }
    else {
        Spec subspec = get_subtable_spec(column_ndx);
        subspec.do_rename_column(column_ids, pos+1, name);
    }
}

void Spec::remove_column(size_t column_ndx)
{
    TIGHTDB_ASSERT(column_ndx < m_spec.size());

    const size_t type_ndx = get_column_type_pos(column_ndx);

    // If the column is a subtable column, we have to delete
    // the subspec(s) as well
    const ColumnType type = ColumnType(m_spec.get(type_ndx));
    if (type == col_type_Table) {
        const size_t subspec_ndx = get_subspec_ndx(column_ndx);
        const size_t subspec_ref = m_subSpecs.get_as_ref(subspec_ndx);

        Array subspec_top(subspec_ref, NULL, 0, m_specSet.get_alloc());
        subspec_top.Destroy(); // recursively delete entire subspec
        m_subSpecs.Delete(subspec_ndx);
    }

    // Delete the actual name and type entries
    m_names.erase(column_ndx);
    m_spec.Delete(type_ndx);

    // If there are an attribute, we have to delete that as well
    if (type_ndx > 0) {
        const ColumnType type_prefix = ColumnType(m_spec.get(type_ndx-1));
        if (type_prefix >= col_attr_Indexed)
            m_spec.Delete(type_ndx-1);
    }
}

void Spec::remove_column(const vector<size_t>& column_ids) {
    do_remove_column(column_ids, 0);
}

void Spec::do_remove_column(const vector<size_t>& column_ids, size_t pos)
{
    const size_t column_ndx = column_ids[pos];

    if (pos == column_ids.size()-1) {
        remove_column(column_ndx);
    }
    else {
        Spec subspec = get_subtable_spec(column_ndx);
        subspec.do_remove_column(column_ids, pos+1);
    }
}

Spec Spec::get_subtable_spec(size_t column_ndx)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(get_column_type(column_ndx) == type_Table);

    const size_t subspec_ndx = get_subspec_ndx(column_ndx);

    Allocator& alloc = m_specSet.get_alloc();
    const size_t ref = m_subSpecs.get_as_ref(subspec_ndx);

    return Spec(m_table, alloc, ref, &m_subSpecs, subspec_ndx);
}

const Spec Spec::get_subtable_spec(size_t column_ndx) const
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(get_column_type(column_ndx) == type_Table);

    const size_t subspec_ndx = get_subspec_ndx(column_ndx);

    Allocator& alloc = m_specSet.get_alloc();
    const size_t ref = m_subSpecs.get_as_ref(subspec_ndx);

    return Spec(m_table, alloc, ref, NULL, 0);
}

size_t Spec::get_subspec_ndx(size_t column_ndx) const
{
    const size_t type_ndx = get_column_type_pos(column_ndx);

    // The subspec array only keep info for subtables
    // so we need to count up to it's position
    size_t pos = 0;
    for (size_t i = 0; i < type_ndx; ++i) {
        if (ColumnType(m_spec.get(i)) == col_type_Table) ++pos;
    }
    return pos;
}

size_t Spec::get_subspec_ref(size_t subspec_ndx) const
{
    TIGHTDB_ASSERT(subspec_ndx < m_subSpecs.size());

    // Note that this addresses subspecs directly, indexing
    // by number of sub-table columns
    return m_subSpecs.get_as_ref(subspec_ndx);
}

size_t Spec::get_type_attr_count() const
{
    return m_spec.size();
}

ColumnType Spec::get_type_attr(size_t ndx) const
{
    return ColumnType(m_spec.get(ndx));
}

size_t Spec::get_column_count() const TIGHTDB_NOEXCEPT
{
    return m_names.size();
}

size_t Spec::get_column_type_pos(size_t column_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());

    // Column types are optionally prefixed by attribute
    // so to get the position of the type we have to ignore
    // the attributes before it.
    size_t i = 0;
    size_t type_ndx = 0;
    for (; type_ndx < column_ndx; ++i) {
        ColumnType type = ColumnType(m_spec.get(i));
        if (type >= col_attr_Indexed) continue; // ignore attributes
        ++type_ndx;
    }
    return i;
}

ColumnType Spec::get_real_column_type(size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < get_column_count());

    ColumnType type;
    size_t column_ndx = 0;
    for (size_t i = 0; column_ndx <= ndx; ++i) {
        type = ColumnType(m_spec.get(i));
        if (type >= col_attr_Indexed) continue; // ignore attributes
        ++column_ndx;
    }

    return type;
}

DataType Spec::get_column_type(size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < get_column_count());

    const ColumnType type = get_real_column_type(ndx);

    // Hide internal types
    if (type == col_type_StringEnum) return type_String;

    return DataType(type);
}

void Spec::set_column_type(size_t column_ndx, ColumnType type)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());

    size_t type_ndx = 0;
    size_t column_count = 0;
    const size_t count = m_spec.size();

    for (;type_ndx < count; ++type_ndx) {
        ColumnType t = ColumnType(m_spec.get(type_ndx));
        if (t >= col_attr_Indexed) continue; // ignore attributes
        if (column_count == column_ndx) break;
        ++column_count;
    }

    // At this point we only support upgrading to string enum
    TIGHTDB_ASSERT(ColumnType(m_spec.get(type_ndx)) == col_type_String);
    TIGHTDB_ASSERT(type == col_type_StringEnum);

    m_spec.set(type_ndx, type);
}

ColumnType Spec::get_column_attr(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < get_column_count());

    size_t column_ndx = 0;

    // The attribute is an optional prefix for the type
    for (size_t i = 0; column_ndx <= ndx; ++i) {
        ColumnType type = ColumnType(m_spec.get(i));
        if (type >= col_attr_Indexed) {
            if (column_ndx == ndx) return type;
        }
        else ++column_ndx;
    }

    return col_attr_None;
}

void Spec::set_column_attr(size_t ndx, ColumnType attr)
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    TIGHTDB_ASSERT(attr >= col_attr_Indexed);

    size_t column_ndx = 0;

    for (size_t i = 0; column_ndx <= ndx; ++i) {
        const ColumnType type = ColumnType(m_spec.get(i));
        if (type >= col_attr_Indexed) {
            if (column_ndx == ndx) {
                // if column already has an attr, we replace it
                if (attr == col_attr_None) m_spec.Delete(i);
                else m_spec.set(i, attr);
                return;
            }
        }
        else {
            if (column_ndx == ndx) {
                // prefix type with attr
                m_spec.Insert(i, attr);
                return;
            }
            ++column_ndx;
        }
    }
}

StringData Spec::get_column_name(size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    return m_names.get(ndx);
}

size_t Spec::get_column_index(StringData name) const
{
    return m_names.find_first(name);
}

#ifdef TIGHTDB_ENABLE_REPLICATION
size_t* Spec::record_subspec_path(const Array* root_subspecs, size_t* begin,
                                  size_t* end) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(begin < end);
    const Array* spec_set = &m_specSet;
    for (;;) {
        const size_t subspec_ndx = spec_set->GetParentNdx();
        *begin++ = subspec_ndx;
        const Array* const parent_subspecs = static_cast<const Array*>(spec_set->GetParent());
        if (parent_subspecs == root_subspecs) break;
        if (begin == end) return 0; // Error, not enough space in buffer
        spec_set = static_cast<const Array*>(parent_subspecs->GetParent());
    }
    return begin;
}
#endif // TIGHTDB_ENABLE_REPLICATION

bool Spec::operator==(const Spec& spec) const
{
    if (!m_spec.Compare(spec.m_spec)) return false;
    if (!m_names.Compare(spec.m_names)) return false;
    return true;
}


#ifdef TIGHTDB_DEBUG

void Spec::Verify() const
{
    size_t column_count = get_column_count();
    TIGHTDB_ASSERT(column_count == m_names.size());
    TIGHTDB_ASSERT(column_count <= m_spec.size());
}

void Spec::to_dot(ostream& out, StringData) const
{
    const size_t ref = m_specSet.get_ref();

    out << "subgraph cluster_specset" << ref << " {" << endl;
    out << " label = \"specset\";" << endl;

    m_specSet.ToDot(out);
    m_spec.ToDot(out, "spec");
    m_names.ToDot(out, "names");
    if (m_subSpecs.IsValid()) {
        m_subSpecs.ToDot(out, "subspecs");

        const size_t count = m_subSpecs.size();
        Allocator& alloc = m_specSet.get_alloc();

        // Write out subspecs
        for (size_t i = 0; i < count; ++i) {
            const size_t ref = m_subSpecs.get_as_ref(i);
            const Spec s(m_table, alloc, ref, NULL, 0);

            s.to_dot(out);
        }
    }

    out << "}" << endl;
}

#endif // TIGHTDB_DEBUG


} //namespace tightdb
