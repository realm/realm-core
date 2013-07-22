/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_COLUMN_STRING_ENUM_HPP
#define TIGHTDB_COLUMN_STRING_ENUM_HPP

#include <tightdb/column_string.hpp>

namespace tightdb {

// Pre-declarations
class StringIndex;

class ColumnStringEnum: public Column {
public:
    ColumnStringEnum(ref_type keys, ref_type values, ArrayParent* = 0,
                     std::size_t ndx_in_parent = 0, Allocator& = Allocator::get_default());
    ~ColumnStringEnum();
    void destroy() TIGHTDB_OVERRIDE;

    StringData get(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    void add(StringData value);
    void set(std::size_t ndx, StringData value);
    void insert(std::size_t ndx, StringData value);
    void erase(std::size_t ndx) TIGHTDB_OVERRIDE;
    void clear() TIGHTDB_OVERRIDE;

    using Column::move_last_over;
    using Column::add;
    using Column::insert;

    std::size_t count(StringData value) const;
    size_t find_first(StringData value, std::size_t begin=0, std::size_t end=-1) const;
    void find_all(Array& res, StringData value, std::size_t begin=0, std::size_t end=-1) const;
    FindRes find_all_indexref(StringData value, std::size_t& dst) const;

    std::size_t count(std::size_t key_index) const;
    std::size_t find_first(std::size_t key_index, std::size_t begin=0, std::size_t end=-1) const;
    void find_all(Array& res, std::size_t key_index, std::size_t begin=0, std::size_t end=-1) const;

    void UpdateParentNdx(int diff);
    void UpdateFromParent();

    // Index
    bool HasIndex() const { return m_index != 0; }
    const StringIndex& GetIndex() const { return *m_index; }
    StringIndex& CreateIndex();
    void SetIndexRef(ref_type, ArrayParent*, std::size_t ndx_in_parent);
    void ReuseIndex(StringIndex&);
    void RemoveIndex() { m_index = 0; }

    // Compare two string columns for equality
    bool compare(const AdaptiveStringColumn&) const;
    bool compare(const ColumnStringEnum&) const;

    const Array* get_enum_root_array() const TIGHTDB_NOEXCEPT { return m_keys.get_root_array(); }

#ifdef TIGHTDB_DEBUG
    void Verify() const; // Must be upper case to avoid conflict with macro in ObjC
    void to_dot(std::ostream&, StringData title) const;
#endif

    std::size_t GetKeyNdx(StringData value) const;
    std::size_t GetKeyNdxOrAdd(StringData value);

private:
    // Member variables
    AdaptiveStringColumn m_keys;
    StringIndex* m_index;
};





// Implementation:

inline StringData ColumnStringEnum::get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < Column::size());
    std::size_t key_ndx = Column::get_as_ref(ndx);
    return m_keys.get(key_ndx);
}


} // namespace tightdb

#endif // TIGHTDB_COLUMN_STRING_ENUM_HPP
