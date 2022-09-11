//////////////////////////////////////////////////////////////////////////////
////
//// Copyright 2016 Realm Inc.
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
//// http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
////
//////////////////////////////////////////////////////////////////////////////
//
//#ifndef CLIENT_RESET_TEST_UTILS_HPP
//#define CLIENT_RESET_TEST_UTILS_HPP
//
//#include <realm/object-store/sync/app.hpp>
//#include <realm/object-store/sync/generic_network_transport.hpp>
//#include <realm/object-store/sync/impl/sync_file.hpp>
//#include <realm/object-store/sync/impl/sync_metadata.hpp>
//#include <realm/object-store/sync/sync_session.hpp>
//
//#include <realm/util/functional.hpp>
//#include <realm/util/function_ref.hpp>
//
//#include "../event_loop.hpp"
//#include "../test_file.hpp"
//#include "../test_utils.hpp"
//#include "sync_test_utils.hpp"
//#include "common_utils.hpp"
//
//#if REALM_ENABLE_SYNC
//
//#include <realm/sync/config.hpp>
//#include <realm/object-store/sync/sync_manager.hpp>
//
//#include <realm/sync/client.hpp>
//#include <realm/sync/noinst/server/server.hpp>
//
//#endif // REALM_ENABLE_SYNC
//
//// disable the tests that rely on having baas available on the network
//// but allow opt-in by building with REALM_ENABLE_AUTH_TESTS=1
//#ifndef REALM_ENABLE_AUTH_TESTS
//#define REALM_ENABLE_AUTH_TESTS 0
//#endif
//
//#ifndef TEST_ENABLE_SYNC_LOGGING
//#define TEST_ENABLE_SYNC_LOGGING 0 // change to 1 to enable trace-level logging
//#endif
//
//#ifndef TEST_ENABLE_SYNC_LOGGING_LEVEL
//#if TEST_ENABLE_SYNC_LOGGING
//#define TEST_ENABLE_SYNC_LOGGING_LEVEL all
//#else
//#define TEST_ENABLE_SYNC_LOGGING_LEVEL off
//#endif // TEST_ENABLE_SYNC_LOGGING
//#endif // TEST_ENABLE_SYNC_LOGGING_LEVEL
//
//namespace realm {
//
//#if REALM_ENABLE_SYNC
//
//
//#endif // REALM_ENABLE_SYNC
//
//namespace reset_utils {
//
//
//
//} // namespace reset_utils
//
//} // namespace realm
//
//using namespace realm;
//namespace {
//TableRef get_table(Realm& realm, StringData object_type);
//}
//
//
//#endif // CLIENT_RESET_TEST_UTILS_HPP
