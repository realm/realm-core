#include <tightdb/util/unique_ptr.hpp>
#include <tightdb/descriptor.hpp>

using namespace tightdb;
using namespace tightdb::util;


void Descriptor::insert_column(size_t column_ndx, DataType type, StringData name)
{
    TIGHTDB_ASSERT(is_attached());
    _impl::TableFriend::insert_column(*this, column_ndx, type, name); // Throws

    // Adjust the column indexes of subdescriptor accessors at higher
    // column indexes.
    typedef subdesc_map::iterator iter;
    iter end = m_subdesc_map.end();
    for (iter i = m_subdesc_map.begin(); i != end; ++i) {
        if (i->m_column_ndx >= column_ndx)
            ++i->m_column_ndx;
    }
}


void Descriptor::remove_column(size_t column_ndx)
{
    TIGHTDB_ASSERT(is_attached());
    _impl::TableFriend::remove_column(*this, column_ndx); // Throws

    // If it exists, remove and detach the subdescriptor accessor
    // associated with the removed column. Also adjust the column
    // indexes of subdescriptor accessors at higher column indexes.
    typedef subdesc_map::iterator iter;
    iter end = m_subdesc_map.end();
    iter erase = end;
    for (iter i = m_subdesc_map.begin(); i != end; ++i) {
        if (i->m_column_ndx == column_ndx) {
            // Must hold a reliable reference count while detaching
            DescriptorRef desc(i->m_subdesc);
            desc->detach();
            erase = i;
        }
        else if (i->m_column_ndx > column_ndx) {
            --i->m_column_ndx; // Account for the removed column
        }
    }
    if (erase != end)
        m_subdesc_map.erase(erase);
}


DescriptorRef Descriptor::get_subdescriptor(size_t column_ndx)
{
    TIGHTDB_ASSERT(is_attached());

    DescriptorRef subdesc;

    // Reuse the the descriptor accessor if it is already in the map
    typedef subdesc_map::iterator iter;
    iter end = m_subdesc_map.end();
    for (iter i = m_subdesc_map.begin(); i != end; ++i) {
        if (i->m_column_ndx == column_ndx) {
            subdesc.reset(i->m_subdesc);
            goto out;
        }
    }

    // Create a new descriptor accessor
    {
        SubspecRef subspec_ref = m_spec->get_subtable_spec(column_ndx);
        UniquePtr<Spec> subspec(new Spec(subspec_ref)); // Throws
        subdesc.reset(new Descriptor); // Throws
        m_subdesc_map.push_back(subdesc_entry(column_ndx, subdesc.get())); // Throws
        subdesc->attach(m_root_table.get(), this, subspec.get());
        subspec.release();
    }

  out:
    return move(subdesc);
}


Descriptor::~Descriptor() TIGHTDB_NOEXCEPT
{
    if (!is_attached())
        return;
    if (m_parent) {
        delete m_spec;
        m_parent->remove_subdesc_entry(this);
        m_parent.reset();
    }
    else {
        _impl::TableFriend::clear_desc_ptr(*m_root_table);
    }
    m_root_table.reset();
}


void Descriptor::detach() TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    detach_subdesc_accessors();
    if (m_parent) {
        delete m_spec;
        m_parent.reset();
    }
    m_root_table.reset();
}


void Descriptor::detach_subdesc_accessors() TIGHTDB_NOEXCEPT
{
    if (!m_subdesc_map.empty()) {
        typedef subdesc_map::const_iterator iter;
        iter end = m_subdesc_map.end();
        for (iter i = m_subdesc_map.begin(); i != end; ++i) {
            // Must hold a reliable reference count while detaching
            DescriptorRef desc(i->m_subdesc);
            desc->detach();
        }
        m_subdesc_map.clear();
    }
}


void Descriptor::remove_subdesc_entry(Descriptor* subdesc) const TIGHTDB_NOEXCEPT
{
    typedef subdesc_map::iterator iter;
    iter end = m_subdesc_map.end();
    for (iter i = m_subdesc_map.begin(); i != end; ++i) {
        if (i->m_subdesc == subdesc) {
            m_subdesc_map.erase(i);
            return;
        }
    }
    TIGHTDB_ASSERT(false);
}


size_t* Descriptor::record_subdesc_path(size_t* begin, size_t* end) const TIGHTDB_NOEXCEPT
{
    size_t* begin_2 = end;
    const Descriptor* desc = this;
    for (;;) {
        if (desc->is_root())
            return begin_2;
        if (TIGHTDB_UNLIKELY(begin_2 == begin))
            return 0; // Not enough space in path buffer
        const Descriptor* parent = desc->m_parent.get();
        size_t column_ndx = not_found;
        typedef subdesc_map::iterator iter;
        iter end = parent->m_subdesc_map.end();
        for (iter i = parent->m_subdesc_map.begin(); i != end; ++i) {
            if (i->m_subdesc == desc) {
                column_ndx = i->m_column_ndx;
                break;
            }
        }
        TIGHTDB_ASSERT(column_ndx != not_found);
        *--begin_2 = column_ndx;
        desc = parent;
    }
}
