/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/
#ifndef REALM_HANDOVER_DEFS
#define REALM_HANDOVER_DEFS

#include <memory>

namespace realm {

enum class ConstSourcePayload { Copy, Stay };
enum class MutableSourcePayload { Move };

struct TableViewHandoverPatch;

struct TableHandoverPatch {
    size_t m_table_num;
};

struct LinkViewHandoverPatch {
    std::unique_ptr<TableHandoverPatch> m_table;
    size_t m_col_num;
    size_t m_row_ndx;
};

struct QueryHandoverPatch {
    std::unique_ptr<TableHandoverPatch> m_table;
    std::unique_ptr<TableViewHandoverPatch> table_view_data;
    std::unique_ptr<LinkViewHandoverPatch> link_view_data;
    ~QueryHandoverPatch() {};
};

struct TableViewHandoverPatch {
    std::unique_ptr<TableHandoverPatch> m_table;
    std::unique_ptr<TableHandoverPatch> linked_table;
    size_t linked_column;
    size_t linked_row;
    bool was_in_sync;
    QueryHandoverPatch query_patch;
    std::unique_ptr<LinkViewHandoverPatch> linkview_patch;
    ~TableViewHandoverPatch() { }
};


struct RowBaseHandoverPatch {
    std::unique_ptr<TableHandoverPatch> m_table;
    size_t row_ndx;
};


} // end namespace Realm

#endif
