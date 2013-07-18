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
#ifndef TIGHTDB_INDEX_HPP
#define TIGHTDB_INDEX_HPP

#include <tightdb/column.hpp>

namespace tightdb {

class Index: public Column {
public:
    Index();
    explicit Index(Array::Type, Array* = 0, std::size_t ndx_in_parent = 0);
    explicit Index(ref_type ref);
    Index(ref_type ref, Array* parent, std::size_t ndx_in_parent);

    bool is_empty() const;

    void BuildIndex(const Column& c);

    void insert(size_t ndx, int64_t value, bool is_last=false);
    void erase(size_t ndx, int64_t value, bool is_last=false);
    void set(size_t ndx, int64_t old_value, int64_t new_value);

    using Column::insert;
    using Column::erase;

    size_t find_first(int64_t value) const;
    bool find_all(Column& result, int64_t value) const;
    bool FindAllRange(Column& result, int64_t start, int64_t end) const;

#ifdef TIGHTDB_DEBUG
    void Verify() const;
#endif // TIGHTDB_DEBUG

protected:
    // B-Tree functions
    NodeChange DoInsert(size_t ndx, int64_t value);
    bool DoDelete(size_t ndx, int64_t value);

    // Node functions
    void NodeAdd(size_t ref);

    void UpdateRefs(size_t pos, int diff);

    void LeafInsert(size_t ref, int64_t value);

    int64_t MaxValue() const;
    size_t MaxRef() const;
};

} // namespace tightdb

#endif // TIGHTDB_INDEX_HPP
