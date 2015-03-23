/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
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
#ifndef REALM_TEST_UTIL_CHECK_LOGIC_ERROR_HPP
#define REALM_TEST_UTIL_CHECK_LOGIC_ERROR_HPP

#include <tightdb/exceptions.hpp>

#include "unit_test.hpp"

#define CHECK_LOGIC_ERROR(expr, error_kind) \
    CHECK_THROW_EX(expr, realm::LogicError, e.kind() == error_kind)

#endif // REALM_TEST_UTIL_CHECK_LOGIC_ERROR_HPP
