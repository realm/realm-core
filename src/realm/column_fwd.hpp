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
#ifndef REALM_COLUMN_FWD_HPP
#define REALM_COLUMN_FWD_HPP

#include <cstdint>

namespace realm {

class ColumnBase;
class AdaptiveStringColumn;
class ColumnStringEnum;
class ColumnBinary;
class ColumnTable;
class ColumnMixed;
class ColumnLink;
class ColumnLinkList;
template <class T, bool Nullable = false> class TColumn;
template<class T> class BasicColumn;

/// FIXME: Rename Column to IntegerColumn.
using Column = TColumn<std::int64_t>;
using ColumnIntNull = TColumn<std::int64_t, true>;
using ColumnDouble = BasicColumn<double>;
using ColumnFloat = BasicColumn<float>;

} // namespace realm

#endif // REALM_COLUMN_FWD_HPP
