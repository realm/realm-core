#include <realm/impl/destroy_guard.hpp>
#include <realm/spec.hpp>

#ifdef REALM_ENABLE_REPLICATION
#  include <realm/replication.hpp>
#endif

using namespace std;
using namespace realm;


Spec::~Spec() REALM_NOEXCEPT
{
#ifdef REALM_ENABLE_REPLICATION
    if (m_top.is_attached()) {
        if (Replication* repl = m_top.get_alloc().get_replication())
            repl->on_spec_destroyed(this);
    }
#endif
}


void Spec::init(MemRef mem) REALM_NOEXCEPT
{
    m_top.init_from_mem(mem);
    size_t top_size = m_top.size();
    REALM_ASSERT(top_size >= 3 && top_size <= 5);

    m_types.init_from_ref(m_top.get_as_ref(0));
    m_types.set_parent(&m_top, 0);
    m_names.init_from_ref(m_top.get_as_ref(1));
    m_names.set_parent(&m_top, 1);
    m_attr.init_from_ref(m_top.get_as_ref(2));
    m_attr.set_parent(&m_top, 2);

    // Subspecs array is only there and valid when there are subtables
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


void Spec::update_from_parent(size_t old_baseline) REALM_NOEXCEPT
{
    if (!m_top.update_from_parent(old_baseline))
        return;

    m_types.update_from_parent(old_baseline);
    m_names.update_from_parent(old_baseline);
    m_attr.update_from_parent(old_baseline);

    if (m_top.size() > 3)
        m_subspecs.update_from_parent(old_baseline);

    if (m_top.size() > 4)
        m_enumkeys.update_from_parent(old_baseline);
}


MemRef Spec::create_empty_spec(Allocator& alloc)
{
    // The 'spec_set' contains the specification (types and names) of
    // all columns and sub-tables
    Array spec_set(alloc);
    _impl::DeepArrayDestroyGuard dg(&spec_set);
    spec_set.create(Array::type_HasRefs); // Throws

    _impl::DeepArrayRefDestroyGuard dg_2(alloc);
    {
        // One type for each column
        bool context_flag = false;
        MemRef mem = Array::create_empty_array(Array::type_Normal, context_flag, alloc); // Throws
        dg_2.reset(mem.m_ref);
        int_fast64_t v(mem.m_ref); // FIXME: Dangerous case: unsigned -> signed
        spec_set.add(v); // Throws
        dg_2.release();
    }
    {
        size_t size = 0;
        // One name for each column
        MemRef mem = ArrayString::create_array(size, alloc); // Throws
        dg_2.reset(mem.m_ref);
        int_fast64_t v = mem.m_ref; // FIXME: Dangerous case: unsigned -> signed
        spec_set.add(v); // Throws
        dg_2.release();
    }
    {
        // One attrib set for each column
        bool context_flag = false;
        MemRef mem = Array::create_empty_array(Array::type_Normal, context_flag, alloc); // Throws
        dg_2.reset(mem.m_ref);
        int_fast64_t v = mem.m_ref; // FIXME: Dangerous case: unsigned -> signed
        spec_set.add(v); // Throws
        dg_2.release();
    }

    dg.release();
    return spec_set.get_mem();
}


void Spec::insert_column(size_t column_ndx, ColumnType type, StringData name, ColumnAttr attr)
{
    REALM_ASSERT(column_ndx <= m_types.size());

    // Backlinks should always be appended to end
    REALM_ASSERT(column_ndx == m_types.size() || type != col_type_BackLink);

    if (type != col_type_BackLink) // backlinks do not have names
        m_names.insert(column_ndx, name); // Throws
    m_types.insert(column_ndx, type); // Throws
    // FIXME: So far, attributes are never reported to the replication system
    m_attr.insert(column_ndx, attr); // Throws

    bool has_subspec = type == col_type_Table || type == col_type_Link ||
        type == col_type_LinkList || type == col_type_BackLink;
    if (has_subspec) {
        Allocator& alloc = m_top.get_alloc();
        // `m_subspecs` array is only present when the spec contains a subtable column
        if (!m_subspecs.is_attached()) {
            bool context_flag = false;
            MemRef subspecs_mem =
                Array::create_empty_array(Array::type_HasRefs, context_flag, alloc); // Throws
            _impl::DeepArrayRefDestroyGuard dg(subspecs_mem.m_ref, alloc);
            if (m_top.size() == 3) {
                int_fast64_t v(subspecs_mem.m_ref); // FIXME: Dangerous cast (unsigned -> signed)
                m_top.add(v); // Throws
            }
            else {
                int_fast64_t v(subspecs_mem.m_ref); // FIXME: Dangerous cast (unsigned -> signed)
                m_top.set(3, v); // Throws
            }
            m_subspecs.init_from_ref(subspecs_mem.m_ref);
            m_subspecs.set_parent(&m_top, 3);
            dg.release();
        }

        if (type == col_type_Table) {
            // Add a new empty spec to `m_subspecs`
            MemRef subspec_mem = create_empty_spec(alloc); // Throws
            _impl::DeepArrayRefDestroyGuard dg(subspec_mem.m_ref, alloc);
            size_t subspec_ndx = get_subspec_ndx(column_ndx);
            int_fast64_t v(subspec_mem.m_ref); // FIXME: Dangerous cast (unsigned -> signed)
            m_subspecs.insert(subspec_ndx, v); // Throws
            dg.release();
        }
        else if (type == col_type_Link || type == col_type_LinkList) {
            // Store group-level table index of target table. When we set the
            // target it will be as a tagged integer (low bit set) Since we
            // don't know it yet we just store zero (null ref).
            size_t subspec_ndx = get_subspec_ndx(column_ndx);
            m_subspecs.insert(subspec_ndx, 0); // Throws
        }
        else if (type == col_type_BackLink) {
            // Store group-level table index of origin table and index of origin
            // column. When we set the target it will be as a tagged integer
            // (low bit set) Since we don't know it yet we just store zero (null
            // ref).
            size_t subspec_ndx = get_subspec_ndx(column_ndx);
            m_subspecs.insert(subspec_ndx, 0); // Throws
            m_subspecs.insert(subspec_ndx, 1); // Throws
        }
    }
}


void Spec::erase_column(size_t column_ndx)
{
    REALM_ASSERT(column_ndx < m_types.size());
    typedef _impl::TableFriend tf;

    // If the column is a subtable column, we have to delete
    // the subspec(s) as well
    ColumnType type = ColumnType(m_types.get(column_ndx));
    if (type == col_type_Table) {
        size_t subspec_ndx = get_subspec_ndx(column_ndx);
        ref_type subspec_ref = m_subspecs.get_as_ref(subspec_ndx);

        Array subspec_top(m_top.get_alloc());
        subspec_top.init_from_ref(subspec_ref);
        subspec_top.destroy_deep(); // recursively delete entire subspec
        m_subspecs.erase(subspec_ndx); // Throws
    }
    else if (tf::is_link_type(type)) {
        size_t subspec_ndx = get_subspec_ndx(column_ndx);
        m_subspecs.erase(subspec_ndx); // origin table index  : Throws
    }
    else if (type == col_type_BackLink) {
        size_t subspec_ndx = get_subspec_ndx(column_ndx);
        m_subspecs.erase(subspec_ndx); // origin table index  : Throws
        m_subspecs.erase(subspec_ndx); // origin column index : Throws
    }
    else if (type == col_type_StringEnum) {
        // Enum columns do also have a separate key list
        size_t keys_ndx = get_enumkeys_ndx(column_ndx);
        ref_type keys_ref = m_enumkeys.get_as_ref(keys_ndx);

        Array keys_top(m_top.get_alloc());
        keys_top.init_from_ref(keys_ref);
        keys_top.destroy_deep();
        m_enumkeys.erase(keys_ndx); // Throws
    }

    // Delete the actual name and type entries
    REALM_ASSERT((column_ndx >= m_names.size()) == (type == col_type_BackLink));
    if (type != col_type_BackLink)
        m_names.erase(column_ndx); // Throws
    m_types.erase(column_ndx);  // Throws
    m_attr.erase(column_ndx);  // Throws
}


size_t Spec::get_subspec_ndx(size_t column_ndx) const REALM_NOEXCEPT
{
    REALM_ASSERT(column_ndx <= get_column_count());
    REALM_ASSERT(column_ndx == get_column_count() ||
                   get_column_type(column_ndx) == col_type_Table    ||
                   get_column_type(column_ndx) == col_type_Link     ||
                   get_column_type(column_ndx) == col_type_LinkList ||
                   get_column_type(column_ndx) == col_type_BackLink );

    // The m_subspecs array only keep info for subtables so we need to
    // count up to it's position
    size_t subspec_ndx = 0;
    for (size_t i = 0; i != column_ndx; ++i) {
        ColumnType type = ColumnType(m_types.get(i));
        if (type == col_type_Table || type == col_type_Link || type == col_type_LinkList) {
            ++subspec_ndx;
        }
        else if (type == col_type_BackLink) {
            subspec_ndx += 2; // table and column refs
        }
    }
    return subspec_ndx;
}


void Spec::upgrade_string_to_enum(size_t column_ndx, ref_type keys_ref,
                                  ArrayParent*& keys_parent, size_t& keys_ndx)
{
    REALM_ASSERT(get_column_type(column_ndx) == col_type_String);

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


size_t Spec::get_enumkeys_ndx(size_t column_ndx) const REALM_NOEXCEPT
{
    // The enumkeys array only keep info for stringEnum columns
    // so we need to count up to it's position
    size_t enumkeys_ndx = 0;
    for (size_t i = 0; i < column_ndx; ++i) {
        if (ColumnType(m_types.get(i)) == col_type_StringEnum)
            ++enumkeys_ndx;
    }
    return enumkeys_ndx;
}


ref_type Spec::get_enumkeys_ref(size_t column_ndx, ArrayParent** keys_parent,
                                size_t* keys_ndx) REALM_NOEXCEPT
{
    size_t enumkeys_ndx = get_enumkeys_ndx(column_ndx);

    // We may also need to return parent info
    if (keys_parent)
        *keys_parent = &m_enumkeys;
    if (keys_ndx)
        *keys_ndx = enumkeys_ndx;

    return m_enumkeys.get_as_ref(enumkeys_ndx);
}


size_t Spec::get_opposite_link_table_ndx(size_t column_ndx) const REALM_NOEXCEPT
{
    REALM_ASSERT(column_ndx < get_column_count());
    REALM_ASSERT(get_column_type(column_ndx) == col_type_Link ||
                   get_column_type(column_ndx) == col_type_LinkList ||
                   get_column_type(column_ndx) == col_type_BackLink);

    // Group-level index of opposite table is stored as tagged int in the
    // subspecs array
    size_t subspec_ndx = get_subspec_ndx(column_ndx);
    int64_t tagged_value = m_subspecs.get(subspec_ndx);
    REALM_ASSERT(tagged_value != 0); // can't retrieve it if never set

    uint64_t table_ref = uint64_t(tagged_value) >> 1;
    return table_ref;
}


void Spec::set_opposite_link_table_ndx(size_t column_ndx, size_t table_ndx)
{
    REALM_ASSERT(column_ndx < get_column_count());
    REALM_ASSERT(get_column_type(column_ndx) == col_type_Link ||
                   get_column_type(column_ndx) == col_type_LinkList ||
                   get_column_type(column_ndx) == col_type_BackLink);

    // position of target table is stored as tagged int
    size_t tagged_ndx = (table_ndx << 1) + 1;

    size_t subspec_ndx = get_subspec_ndx(column_ndx);
    m_subspecs.set(subspec_ndx, tagged_ndx); // Throws
}


void Spec::set_backlink_origin_column(size_t backlink_col_ndx, size_t origin_col_ndx)
{
    REALM_ASSERT(backlink_col_ndx < get_column_count());
    REALM_ASSERT(get_column_type(backlink_col_ndx) == col_type_BackLink);

    // position of target table is stored as tagged int
    size_t tagged_ndx = (origin_col_ndx << 1) + 1;

    size_t subspec_ndx = get_subspec_ndx(backlink_col_ndx);
    m_subspecs.set(subspec_ndx+1, tagged_ndx); // Throws
}


size_t Spec::get_origin_column_ndx(size_t backlink_col_ndx) const REALM_NOEXCEPT
{
    REALM_ASSERT(backlink_col_ndx < get_column_count());
    REALM_ASSERT(get_column_type(backlink_col_ndx) == col_type_BackLink);

    // Origin column is stored as second tagged int in the subspecs array
    size_t subspec_ndx = get_subspec_ndx(backlink_col_ndx);
    int64_t tagged_value = m_subspecs.get(subspec_ndx+1);
    REALM_ASSERT(tagged_value != 0); // can't retrieve it if never set

    size_t origin_col_ndx = size_t(uint64_t(tagged_value) >> 1);
    return origin_col_ndx;
}


size_t Spec::find_backlink_column(size_t origin_table_ndx, size_t origin_col_ndx) const
    REALM_NOEXCEPT
{
    size_t backlinks_column_start = m_names.size();
    size_t backlinks_start = get_subspec_ndx(backlinks_column_start);
    size_t count = m_subspecs.size();

    int64_t tagged_table_ndx = (origin_table_ndx << 1) + 1;
    int64_t tagged_column_ndx = (origin_col_ndx << 1) + 1;

    for (size_t i = backlinks_start; i < count; i += 2) {
        if (m_subspecs.get(i)   == tagged_table_ndx &&
            m_subspecs.get(i+1) == tagged_column_ndx)
        {
            size_t pos = (i - backlinks_start) / 2;
            return backlinks_column_start + pos;
        }
    }

    REALM_ASSERT(false);
    return not_found;
}


DataType Spec::get_public_column_type(size_t ndx) const REALM_NOEXCEPT
{
    REALM_ASSERT(ndx < get_column_count());

    ColumnType type = get_column_type(ndx);

    // Hide internal types
    if (type == col_type_StringEnum)
        return type_String;

    return DataType(type);
}


size_t Spec::get_column_ndx_in_parent(size_t column_ndx) const
{
    // If there are indexed columns, the indexes also takes
    // up space in the list of columns refs (m_columns in table)
    // so we need to be able to get the adjusted position

    size_t offset = 0;
    for (size_t i = 0; i < column_ndx; ++i) {
        if ((m_attr.get(i) & col_attr_Indexed) != 0)
            ++offset;
    }
    return column_ndx + offset;
}


void Spec::get_column_info(size_t column_ndx, ColumnInfo& info) const REALM_NOEXCEPT
{
    info.m_column_ref_ndx = get_column_ndx_in_parent(column_ndx);
    info.m_has_search_index = (get_column_attr(column_ndx) & col_attr_Indexed) != 0;
}


bool Spec::operator==(const Spec& spec) const REALM_NOEXCEPT
{
    if (!m_types.compare_int(spec.m_types))
        return false;
    if (!m_names.compare_string(spec.m_names))
        return false;
    return true;
}


#ifdef REALM_DEBUG

void Spec::Verify() const
{
    REALM_ASSERT(m_names.size() == get_public_column_count());
    REALM_ASSERT(m_types.size()  == get_column_count());
    REALM_ASSERT(m_attr.size()  == get_column_count());

    REALM_ASSERT(m_types.get_ref()  == m_top.get_as_ref(0));
    REALM_ASSERT(m_names.get_ref() == m_top.get_as_ref(1));
    REALM_ASSERT(m_attr.get_ref()  == m_top.get_as_ref(2));
}


void Spec::to_dot(ostream& out, StringData) const
{
    ref_type ref = m_top.get_ref();

    out << "subgraph cluster_specset" << ref << " {" << endl;
    out << " label = \"specset\";" << endl;

    m_top.to_dot(out);
    m_types.to_dot(out, "types");
    m_names.to_dot(out, "names");

    size_t num_cols = m_types.size();
    bool have_subspecs = false;
    for (size_t i = 0; i < num_cols; ++i) {
        ColumnType type = ColumnType(m_types.get(i));
        if (type == col_type_Table) {
            have_subspecs = true;
            break;
        }
    }

    if (have_subspecs) {
        REALM_ASSERT(m_subspecs.is_attached());
        m_subspecs.to_dot(out, "subspecs");

        Allocator& alloc = m_top.get_alloc();

        // Write out subspecs
        for (size_t i = 0; i < num_cols; ++i) {
            ColumnType type = ColumnType(m_types.get(i));
            if (type != col_type_Table)
                continue;
            size_t subspec_ndx = get_subspec_ndx(i);
            ref_type ref = m_subspecs.get_as_ref(subspec_ndx);
            MemRef mem(ref, alloc);
            Spec subspec(alloc);
            subspec.init(mem);
            subspec.set_parent(const_cast<Array*>(&m_subspecs), i);
            subspec.to_dot(out);
        }
    }

    out << "}" << endl;
}

#endif // REALM_DEBUG
