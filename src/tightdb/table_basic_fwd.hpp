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
#ifndef REALM_TABLE_BASIC_FWD_HPP
#define REALM_TABLE_BASIC_FWD_HPP

namespace realm {


template<class Spec> class BasicTable;

template<class T> struct IsBasicTable { static const bool value = false; };
template<class Spec> struct IsBasicTable<BasicTable<Spec> > { static const bool value = true; };
template<class Spec> struct IsBasicTable<const BasicTable<Spec> > { static const bool value = true; };


} // namespace realm

#endif // REALM_TABLE_BASIC_FWD_HPP
