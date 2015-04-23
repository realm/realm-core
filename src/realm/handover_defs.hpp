/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] Realm Inc
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

namespace realm {

enum class ConstSourcePayload { Copy, Stay };
enum class MutableSourcePayload { Move };

struct TableView_Handover_patch;

struct LinkView_Handover_patch {
    std::size_t m_table_num;
    std::size_t m_col_num;
    std::size_t m_row_ndx;
};

struct Query_Handover_patch {
    std::size_t m_table_num;
    bool m_has_table;
    TableView_Handover_patch* table_view_data;
    LinkView_Handover_patch* link_view_data;
};

struct TableView_Handover_patch {
    std::size_t table_num;
    Query_Handover_patch query_patch;
    LinkView_Handover_patch* linkview_patch;
};

struct RowBase_Handover_patch {
    std::size_t table_num;
    std::size_t row_ndx;
};


} // end namespace Realm

#endif
