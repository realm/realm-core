#include <tightdb/spec.hpp>

#ifdef TIGHTDB_ENABLE_REPLICATION
#include <tightdb/replication.hpp>
#endif

using namespace std;
using namespace tightdb;


Spec::~Spec() TIGHTDB_NOEXCEPT
{
#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = m_top.get_alloc().get_replication())
        repl->on_spec_destroyed(this);
#endif
}

void Spec::init_from_ref(ref_type ref, ArrayParent* parent, size_t ndx_in_parent) TIGHTDB_NOEXCEPT
{
    m_top.init_from_ref(ref);
    m_top.set_parent(parent, ndx_in_parent);
    TIGHTDB_ASSERT(m_top.size() >= 3 && m_top.size() <= 5);

    m_spec.init_from_ref(m_top.get_as_ref(0));
    m_spec.set_parent(&m_top, 0);
    m_names.init_from_ref(m_top.get_as_ref(1));
    m_names.set_parent(&m_top, 1);
    m_attr.init_from_ref(m_top.get_as_ref(2));
    m_attr.set_parent(&m_top, 2);

    // SubSpecs array is only there and valid when there are subtables
    // if there are enumkey, but no subtables yet it will be a zero-ref
    if (m_top.size() >= 4) {
        ref_type ref = m_top.get_as_ref(3);
        if (ref) {
            m_subspecs.init_from_ref(m_top.get_as_ref(3));
            m_subspecs.set_parent(&m_top, 3);
        }
    }

    // Enumkeys array is only there when there are StringEnum columns
    if (m_top.size() == 5) {
        m_enumkeys.init_from_ref(m_top.get_as_ref(4));
        m_enumkeys.set_parent(&m_top, 4);
    }
}

void Spec::destroy() TIGHTDB_NOEXCEPT
{
    m_top.destroy();
}

ref_type Spec::get_ref() const TIGHTDB_NOEXCEPT
{
    return m_top.get_ref();
}

void Spec::set_parent(ArrayParent* parent, size_t ndx_in_parent) TIGHTDB_NOEXCEPT
{
    m_top.set_parent(parent, ndx_in_parent);
}

void Spec::update_from_parent(size_t old_baseline) TIGHTDB_NOEXCEPT
{
    if (!m_top.update_from_parent(old_baseline))
        return;

    m_spec.update_from_parent(old_baseline);
    m_names.update_from_parent(old_baseline);
    m_attr.update_from_parent(old_baseline);

    if (m_top.size() > 3) {
        m_subspecs.update_from_parent(old_baseline);
    }

    if (m_top.size() > 4) {
        m_enumkeys.update_from_parent(old_baseline);
    }
}

size_t Spec::add_column(DataType type, StringData name, ColumnAttr attr)
{
    m_names.add(name);
    m_spec.add(type);
    m_attr.add(attr); // TODO: add to replication log

    if (type == type_Table) {
        // SubSpecs array is only there when there are subtables
        if (!m_subspecs.is_attached()) {
            m_subspecs.create(Array::type_HasRefs);
            if (m_top.size() == 3)
                m_top.add(m_subspecs.get_ref());
            else
                m_top.set(3, m_subspecs.get_ref());
            m_subspecs.set_parent(&m_top, 3);
        }

        Allocator& alloc = m_top.get_alloc();

        // Create spec for new subtable
        Array spec(Array::type_Normal, 0, 0, alloc);
        ArrayString names(0, 0, alloc);
        Array attr(Array::type_Normal, 0, 0, alloc);
        Array spec_set(Array::type_HasRefs, 0, 0, alloc);
        spec_set.add(spec.get_ref());
        spec_set.add(names.get_ref());
        spec_set.add(attr.get_ref());

        // Add to list of subspecs
        ref_type ref = spec_set.get_ref();
        m_subspecs.add(ref);
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = m_top.get_alloc().get_replication())
        repl->add_column(m_table, this, type, name); // Throws
#endif

    return m_names.size() - 1; // column_ndx
}

size_t Spec::add_subcolumn(const vector<size_t>& column_path, DataType type, StringData name)
{
    if (column_path.empty())
        return add_column(type, name);
    else
        return do_add_subcolumn(column_path, 0, type, name);
}

size_t Spec::do_add_subcolumn(const vector<size_t>& column_ids, size_t pos, DataType type, StringData name)
{
    size_t column_ndx = column_ids[pos];
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
    size_t column_ndx = m_names.size();
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
    size_t column_ndx = column_ids[pos];

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

    // If the column is a subtable column, we have to delete
    // the subspec(s) as well
    ColumnType type = ColumnType(m_spec.get(column_ndx));
    if (type == col_type_Table) {
        size_t subspec_ndx = get_subspec_ndx(column_ndx);
        ref_type subspec_ref = m_subspecs.get_as_ref(subspec_ndx);

        Array subspec_top(subspec_ref, 0, 0, m_top.get_alloc());
        subspec_top.destroy(); // recursively delete entire subspec
        m_subspecs.erase(subspec_ndx);
    }
    else if (type == col_type_StringEnum) {
        // Enum columns do also have a separate key list
        size_t keys_ndx = get_enumkeys_ndx(column_ndx);
        ref_type keys_ref = m_enumkeys.get_as_ref(keys_ndx);

        Array keys_top(keys_ref, 0, 0, m_top.get_alloc());
        keys_top.destroy();
        m_enumkeys.erase(keys_ndx);
    }

    // Delete the actual name and type entries
    m_names.erase(column_ndx);
    m_spec.erase(column_ndx);
    m_attr.erase(column_ndx);
}

void Spec::remove_column(const vector<size_t>& column_ids)
{
    do_remove_column(column_ids, 0);
}

void Spec::do_remove_column(const vector<size_t>& column_ids, size_t pos)
{
    size_t column_ndx = column_ids[pos];

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

    size_t subspec_ndx = get_subspec_ndx(column_ndx);

    Allocator& alloc = m_top.get_alloc();
    ref_type ref = m_subspecs.get_as_ref(subspec_ndx);

    return Spec(m_table, alloc, ref, &m_subspecs, subspec_ndx);
}

const Spec Spec::get_subtable_spec(size_t column_ndx) const
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(get_column_type(column_ndx) == type_Table);

    size_t subspec_ndx = get_subspec_ndx(column_ndx);

    Allocator& alloc = m_top.get_alloc();
    ref_type ref = m_subspecs.get_as_ref(subspec_ndx);

    return Spec(m_table, alloc, ref, 0, 0);
}

size_t Spec::get_subspec_ndx(size_t column_ndx) const
{
    // The subspec array only keep info for subtables
    // so we need to count up to it's position
    size_t pos = 0;
    for (size_t i = 0; i < column_ndx; ++i) {
        if (ColumnType(m_spec.get(i)) == col_type_Table)
            ++pos;
    }
    return pos;
}

ref_type Spec::get_subspec_ref(size_t subspec_ndx) const
{
    TIGHTDB_ASSERT(subspec_ndx < m_subspecs.size());

    // Note that this addresses subspecs directly, indexing
    // by number of sub-table columns
    return m_subspecs.get_as_ref(subspec_ndx);
}

void Spec::upgrade_string_to_enum(size_t column_ndx, ref_type keys_ref,
                                  ArrayParent*& keys_parent, size_t& keys_ndx)
{
    TIGHTDB_ASSERT(get_column_type(column_ndx) == type_String);

    // Create the enumkeys list if needed
    if (!m_enumkeys.is_attached()) {
        m_enumkeys.create(Array::type_HasRefs);
        if (m_top.size() == 3)
            m_top.add(0); // no subtables
        if (m_top.size() == 4)
            m_top.add(m_enumkeys.get_ref());
        else
            m_top.set(4, m_enumkeys.get_ref());
        m_enumkeys.set_parent(&m_top, 4);
    }

    // Insert the new key list
    size_t ins_pos = get_enumkeys_ndx(column_ndx);
    m_enumkeys.insert(ins_pos, keys_ref);

    set_column_type(column_ndx, col_type_StringEnum);

    // Return parent info
    keys_parent = &m_enumkeys;
    keys_ndx    = ins_pos;
}

size_t Spec::get_enumkeys_ndx(size_t column_ndx) const
{
    // The enumkeys array only keep info for stringEnum columns
    // so we need to count up to it's position
    size_t pos = 0;
    for (size_t i = 0; i < column_ndx; ++i) {
        if (ColumnType(m_spec.get(i)) == col_type_StringEnum)
            ++pos;
    }
    return pos;
}

ref_type Spec::get_enumkeys_ref(size_t column_ndx, ArrayParent** keys_parent, size_t* keys_ndx)
{
    size_t enumkeys_ndx = get_enumkeys_ndx(column_ndx);

    // We may also need to return parent info
    if (keys_parent)
        *keys_parent = &m_enumkeys;
    if (keys_ndx)
        *keys_ndx = enumkeys_ndx;

    return m_enumkeys.get_as_ref(enumkeys_ndx);
}

size_t Spec::get_column_count() const TIGHTDB_NOEXCEPT
{
    return m_names.size();
}

ColumnType Spec::get_real_column_type(size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    return ColumnType(m_spec.get(ndx));
}

DataType Spec::get_column_type(size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < get_column_count());

    ColumnType type = get_real_column_type(ndx);

    // Hide internal types
    if (type == col_type_StringEnum)
        return type_String;

    return DataType(type);
}

void Spec::set_column_type(size_t column_ndx, ColumnType type)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());

    // At this point we only support upgrading to string enum
    TIGHTDB_ASSERT(ColumnType(m_spec.get(column_ndx)) == col_type_String);
    TIGHTDB_ASSERT(type == col_type_StringEnum);

    m_spec.set(column_ndx, type);
}

ColumnAttr Spec::get_column_attr(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    return ColumnAttr(m_attr.get(ndx));
}

void Spec::set_column_attr(size_t column_ndx, ColumnAttr attr)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());

    // At this point we only allow one attr at a time
    // so setting it will overwrite existing. In the future
    // we will allow combinations.
    m_attr.set(column_ndx, attr);
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

size_t Spec::get_column_pos(size_t column_ndx) const
{
    // If there are indexed columns, the indexes also takes
    // up space in the list of columns refs (m_columns in table)
    // so we need to be able to get the adjusted position

    size_t offset = 0;
    for (size_t i = 0; i < column_ndx; ++i) {
        if (m_attr.get(i) == col_attr_Indexed) {
            ++offset;
        }
    }
    return column_ndx + offset;
}


void Spec::get_column_info(size_t column_ndx, ColumnInfo& info) const
{
    info.m_column_ref_ndx = get_column_pos(column_ndx);
    info.m_has_index = (get_column_attr(column_ndx) & col_attr_Indexed) != 0;
}


void Spec::get_subcolumn_info(const vector<size_t>& column_path, size_t column_path_ndx,
                              ColumnInfo& info) const
{
    TIGHTDB_ASSERT(1 <= column_path.size());
    TIGHTDB_ASSERT(column_path_ndx <= column_path.size() - 1);
    size_t column_ndx = column_path[column_path_ndx];
    bool is_last = column_path.size() <= column_path_ndx + 1;
    if (is_last) {
        get_column_info(column_ndx, info);
        return;
    }

    size_t subspec_ndx = get_subspec_ndx(column_ndx);
    ref_type subspec_ref = get_subspec_ref(subspec_ndx);
    Spec subspec(0, m_top.get_alloc(), subspec_ref, 0, 0);
    subspec.get_subcolumn_info(column_path, column_path_ndx+1, info);
}


#ifdef TIGHTDB_ENABLE_REPLICATION
size_t* Spec::record_subspec_path(const Array* root_subspecs, size_t* begin,
                                  size_t* end) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(begin < end);
    const Array* spec_set = &m_top;
    for (;;) {
        size_t subspec_ndx = spec_set->get_ndx_in_parent();
        *begin++ = subspec_ndx;
        const Array* parent_subspecs = static_cast<const Array*>(spec_set->get_parent());
        if (parent_subspecs == root_subspecs)
            break;
        if (begin == end)
            return 0; // Error, not enough space in buffer
        spec_set = static_cast<const Array*>(parent_subspecs->get_parent());
    }
    return begin;
}
#endif // TIGHTDB_ENABLE_REPLICATION

bool Spec::operator==(const Spec& spec) const
{
    if (!m_spec.compare_int(spec.m_spec))
        return false;
    if (!m_names.compare_string(spec.m_names))
        return false;
    return true;
}


#ifdef TIGHTDB_DEBUG

void Spec::Verify() const
{
    size_t column_count = get_column_count();
    TIGHTDB_ASSERT(column_count == m_names.size());
    TIGHTDB_ASSERT(column_count == m_spec.size());
    TIGHTDB_ASSERT(column_count == m_attr.size());

    TIGHTDB_ASSERT(m_spec.get_ref() == m_top.get_as_ref(0));
    TIGHTDB_ASSERT(m_names.get_ref() == m_top.get_as_ref(1));
    TIGHTDB_ASSERT(m_attr.get_ref() == m_top.get_as_ref(2));
}

void Spec::to_dot(ostream& out, StringData) const
{
    ref_type ref = m_top.get_ref();

    out << "subgraph cluster_specset" << ref << " {" << endl;
    out << " label = \"specset\";" << endl;

    m_top.to_dot(out);
    m_spec.to_dot(out, "spec");
    m_names.to_dot(out, "names");
    if (m_subspecs.is_attached()) {
        m_subspecs.to_dot(out, "subspecs");

        Allocator& alloc = m_top.get_alloc();

        // Write out subspecs
        size_t count = m_subspecs.size();
        for (size_t i = 0; i < count; ++i) {
            ref_type ref = m_subspecs.get_as_ref(i);
            Spec s(m_table, alloc, ref, const_cast<Array*>(&m_subspecs), i);
            s.to_dot(out);
        }
    }

    out << "}" << endl;
}

#endif // TIGHTDB_DEBUG
