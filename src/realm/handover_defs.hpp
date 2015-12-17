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

struct TableView_Handover_patch;

struct Table_Handover_patch {
    size_t m_table_num;
};

struct LinkView_Handover_patch {
    std::unique_ptr<Table_Handover_patch> m_table;
    size_t m_col_num;
    size_t m_row_ndx;
};

struct Query_Handover_patch {
    std::unique_ptr<Table_Handover_patch> m_table;
    std::unique_ptr<TableView_Handover_patch> table_view_data;
    std::unique_ptr<LinkView_Handover_patch> link_view_data;
    ~Query_Handover_patch() {};
};

struct TableView_Handover_patch {
    std::unique_ptr<Table_Handover_patch> m_table;
    std::unique_ptr<Table_Handover_patch> linked_table;
    size_t linked_column;
    size_t linked_row;
    bool was_in_sync;
    Query_Handover_patch query_patch;
    std::unique_ptr<LinkView_Handover_patch> linkview_patch;
    ~TableView_Handover_patch() { }
};


struct RowBase_Handover_patch {
    std::unique_ptr<Table_Handover_patch> m_table;
    size_t row_ndx;
};


} // end namespace Realm

#endif
