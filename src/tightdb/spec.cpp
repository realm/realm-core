#include <tightdb/impl/destroy_guard.hpp>
#include <tightdb/spec.hpp>

#ifdef TIGHTDB_ENABLE_REPLICATION
#  include <tightdb/replication.hpp>
#endif

using namespace std;
using namespace tightdb;


Spec::~Spec() TIGHTDB_NOEXCEPT
{
#ifdef TIGHTDB_ENABLE_REPLICATION
    if (m_top.is_attached()) {
        if (Replication* repl = m_top.get_alloc().get_replication())
            repl->on_spec_destroyed(this);
    }
#endif
}

void Spec::init(ref_type ref, ArrayParent* parent, size_t ndx_in_parent) TIGHTDB_NOEXCEPT
{
    m_top.init_from_ref(ref);
    m_top.set_parent(parent, ndx_in_parent);
    size_t top_size = m_top.size();
    TIGHTDB_ASSERT(top_size >= 3 && top_size <= 5);

    m_spec.init_from_ref(m_top.get_as_ref(0));
    m_spec.set_parent(&m_top, 0);
    m_names.init_from_ref(m_top.get_as_ref(1));
    m_names.set_parent(&m_top, 1);
    m_attr.init_from_ref(m_top.get_as_ref(2));
    m_attr.set_parent(&m_top, 2);

    // SubSpecs array is only there and valid when there are subtables
    // if there are enumkey, but no subtables yet it will be a zero-ref
    if (top_size >= 4) {
        if (ref_type ref = m_top.get_as_ref(3)) {
            m_subspecs.init_from_ref(ref);
            m_subspecs.set_parent(&m_top, 3);
        }
    }

    // Enumkeys array is only there when there are StringEnum columns
    if (top_size >= 5) {
        m_enumkeys.init_from_ref(m_top.get_as_ref(4));
        m_enumkeys.set_parent(&m_top, 4);
    }
}

void Spec::update_from_parent(size_t old_baseline) TIGHTDB_NOEXCEPT
{
    if (!m_top.update_from_parent(old_baseline))
        return;

    m_spec.update_from_parent(old_baseline);
    m_names.update_from_parent(old_baseline);
    m_attr.update_from_parent(old_baseline);

    if (m_top.size() > 3)
        m_subspecs.update_from_parent(old_baseline);

    if (m_top.size() > 4)
        m_enumkeys.update_from_parent(old_baseline);
}

ref_type Spec::create_empty_spec(Allocator& alloc)
{
    // The 'spec_set' contains the specification (types and names) of
    // all columns and sub-tables
    Array spec_set(alloc);
    spec_set.create(Array::type_HasRefs); // Throws
    try {
        size_t size = 0;
        int_fast64_t value = 0;
        // One type for each column
        ref_type types_ref = Array::create_array(Array::type_Normal, size, value, alloc); // Throws
        try {
            int_fast64_t v = types_ref; // FIXME: Dangerous case: unsigned -> signed
            spec_set.add(v); // Throws
        }
        catch (...) {
            Array::destroy(types_ref, alloc);
            throw;
        }
        // One name for each column
        ref_type names_ref = ArrayString::create_array(size, alloc); // Throws
        try {
            int_fast64_t v = names_ref; // FIXME: Dangerous case: unsigned -> signed
            spec_set.add(v); // Throws
        }
        catch (...) {
            Array::destroy(names_ref, alloc);
            throw;
        }
        // One attrib set for each column
        ref_type attribs_ref = ArrayString::create_array(size, alloc); // Throws
        try {
            int_fast64_t v = attribs_ref; // FIXME: Dangerous case: unsigned -> signed
            spec_set.add(v); // Throws
        }
        catch (...) {
            Array::destroy(attribs_ref, alloc);
            throw;
        }
    }
    catch (...) {
        spec_set.destroy();
        throw;
    }
    return spec_set.get_ref();
}

void Spec::insert_column(size_t column_ndx, DataType type, StringData name, ColumnAttr attr)
{
    TIGHTDB_ASSERT(column_ndx <= m_spec.size());

    m_names.insert(column_ndx, name); // Throws
    m_spec.insert(column_ndx, type); // Throws
    // FIXME: So far, attributes are never reported to the replication system
    m_attr.insert(column_ndx, attr); // Throws

    if (type == type_Table) {
        Allocator& alloc = m_top.get_alloc();
        // `m_subspecs` array is only present when the spec contains a subtable column
        if (!m_subspecs.is_attached()) {
            ref_type subspecs_ref = Array::create_empty_array(Array::type_HasRefs, alloc); // Throws
            _impl::RefDestroyGuard dg(subspecs_ref, alloc);
            if (m_top.size() == 3) {
                m_top.add(subspecs_ref); // Throws
            }
            else {
                m_top.set(3, subspecs_ref); // Throws
            }
            m_subspecs.init_from_ref(subspecs_ref);
            m_subspecs.set_parent(&m_top, 3);
            dg.release();
        }

        // Add a new empty spec to `m_subspecs`
        {
            ref_type subspec_ref = create_empty_spec(alloc); // Throws
            _impl::RefDestroyGuard dg(subspec_ref, alloc);
            size_t subspec_ndx = column_ndx == get_column_count() ?
                get_num_subspecs() : get_subspec_ndx(column_ndx);
            m_subspecs.insert(subspec_ndx, subspec_ref); // Throws
            dg.release();
        }
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
        m_subspecs.erase(subspec_ndx); // Throws
    }
    else if (type == col_type_StringEnum) {
        // Enum columns do also have a separate key list
        size_t keys_ndx = get_enumkeys_ndx(column_ndx);
        ref_type keys_ref = m_enumkeys.get_as_ref(keys_ndx);

        Array keys_top(keys_ref, 0, 0, m_top.get_alloc());
        keys_top.destroy();
        m_enumkeys.erase(keys_ndx); // Throws
    }

    // Delete the actual name and type entries
    m_names.erase(column_ndx); // Throws
    m_spec.erase(column_ndx);  // Throws
    m_attr.erase(column_ndx);  // Throws
}

size_t Spec::get_subspec_ndx(size_t column_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(get_column_type(column_ndx) == type_Table);

    // The m_subspecs array only keep info for subtables so we need to
    // count up to it's position
    size_t subspec_ndx = 0;
    for (size_t i = 0; i != column_ndx; ++i) {
        if (ColumnType(m_spec.get(i)) == col_type_Table)
            ++subspec_ndx;
    }
    return subspec_ndx;
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
        if (m_top.size() == 4) {
            m_top.add(m_enumkeys.get_ref());
        }
        else {
            m_top.set(4, m_enumkeys.get_ref());
        }
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

size_t Spec::get_enumkeys_ndx(size_t column_ndx) const TIGHTDB_NOEXCEPT
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

DataType Spec::get_column_type(size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < get_column_count());

    ColumnType type = get_real_column_type(ndx);

    // Hide internal types
    if (type == col_type_StringEnum)
        return type_String;

    return DataType(type);
}

size_t Spec::get_column_pos(size_t column_ndx) const
{
    // If there are indexed columns, the indexes also takes
    // up space in the list of columns refs (m_columns in table)
    // so we need to be able to get the adjusted position

    size_t offset = 0;
    for (size_t i = 0; i < column_ndx; ++i) {
        if (m_attr.get(i) == col_attr_Indexed)
            ++offset;
    }
    return column_ndx + offset;
}


void Spec::get_column_info(size_t column_ndx, ColumnInfo& info) const TIGHTDB_NOEXCEPT
{
    info.m_column_ref_ndx = get_column_pos(column_ndx);
    info.m_has_index = (get_column_attr(column_ndx) & col_attr_Indexed) != 0;
}


size_t* Spec::record_subspec_path(const Array& root_subspecs, size_t* begin,
                                  size_t* end) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(begin < end);
    const Array* spec_set = &m_top;
    size_t* i = begin;
    for (;;) {
        size_t subspec_ndx = spec_set->get_ndx_in_parent();
        *i++ = subspec_ndx;
        const Array* parent_subspecs = static_cast<const Array*>(spec_set->get_parent());
        if (parent_subspecs == &root_subspecs)
            break;
        if (i == end)
            return 0; // Error, not enough space in buffer
        spec_set = static_cast<const Array*>(parent_subspecs->get_parent());
    }
    return i;
}


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
            Spec s(alloc, ref, const_cast<Array*>(&m_subspecs), i);
            s.to_dot(out);
        }
    }

    out << "}" << endl;
}

#endif // TIGHTDB_DEBUG
