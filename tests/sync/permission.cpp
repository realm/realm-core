////////////////////////////////////////////////////////////////////////////
//
// Copyright 2017 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include "sync_test_utils.hpp"

#include "sync/sync_permission.hpp"

using namespace realm;

TEST_CASE("`Permission` class", "[sync]") {

	SECTION("paths_are_equivalent() properly returns true") {
		// Identical paths and identical users for tilde-paths.
		CHECK(Permission::paths_are_equivalent("/~/foo", "/~/foo", "user1", "user1"));
		// Identical paths for non-tilde paths.
		CHECK(Permission::paths_are_equivalent("/user2/foo", "/user2/foo", "user1", "user1"));
		CHECK(Permission::paths_are_equivalent("/user2/foo", "/user2/foo", "user1", "user2"));
		// First path can be turned into second path.
		CHECK(Permission::paths_are_equivalent("/~/foo", "/user1/foo", "user1", "user2"));
		// Second path can be turned into first path.
		CHECK(Permission::paths_are_equivalent("/user1/foo", "/~/foo", "user2", "user1"));
	}

	SECTION("paths_are_equivalent() properly returns false") {
		// Different tilde-paths.
		CHECK(!Permission::paths_are_equivalent("/~/foo", "/~/bar", "user1", "user1"));
		// Different non-tilde paths.
		CHECK(!Permission::paths_are_equivalent("/user1/foo", "/user2/bar", "user1", "user1"));
		// Identical paths and different users for tilde-paths.
		CHECK(!Permission::paths_are_equivalent("/~/foo", "/~/foo", "user1", "user2"));
		// First path cannot be turned into second path.
		CHECK(!Permission::paths_are_equivalent("/~/foo", "/user1/foo", "user2", "user2"));
		// Second path cannot be turned into first path.
		CHECK(!Permission::paths_are_equivalent("/user1/foo", "/~/foo", "user2", "user2"));
	}
}
