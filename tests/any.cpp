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

#include "catch.hpp"

#include "util/any.hpp"

using namespace realm;

TEST_CASE("util::Any basic API") {
	SECTION("copy constructor works") {
		util::Any first_any(15);
		util::Any second_any(first_any);
		REQUIRE(util::any_cast<int>(first_any) == util::any_cast<int>(second_any));
	}

	SECTION("move constructor works") {
		const int value = 15;
		util::Any first_any(15);
		util::Any second_any(std::move(first_any));
		REQUIRE(!first_any.has_value());
		REQUIRE(second_any.has_value());
		REQUIRE(util::any_cast<int>(second_any) == value);
	}

	SECTION("copy assignment works") {
		auto first_any = util::Any(15);
		auto second_any = util::Any(first_any);
		REQUIRE(util::any_cast<int>(first_any) == util::any_cast<int>(second_any));
	}

	SECTION("move assignment works") {
		const int value = 15;
		util::Any first_any(15);
		auto second_any = std::move(first_any);
		REQUIRE(!first_any.has_value());
		REQUIRE(second_any.has_value());
		REQUIRE(util::any_cast<int>(second_any) == value);
	}

	SECTION("reset works") {
		auto bool_any = util::Any(false);
		REQUIRE(bool_any.has_value());
		bool_any.reset();
		REQUIRE(!bool_any.has_value());
	}

	SECTION("swap works") {
		const int first_value = 15;
		const bool second_value = false;
		auto first_any = util::Any(first_value);
		auto second_any = util::Any(second_value);
		first_any.swap(second_any);
		REQUIRE(util::any_cast<int>(second_any) == first_value);
		REQUIRE(util::any_cast<bool>(first_any) == second_value);
	}
}

TEST_CASE("util::Any wrapping types") {
	SECTION("works with bools") {
		const bool bool_value = true;
		auto bool_any = util::Any(bool_value);
		REQUIRE(util::any_cast<bool>(bool_any) == bool_value);
	}

	SECTION("works with longs") {
		const long long_value = 31415927;
		auto long_any = util::Any(long_value);
		REQUIRE(util::any_cast<long>(long_any) == long_value);
	}

	SECTION("works with strings") {
		const std::string str_value = "util::Any is a replacement for the 'any' type in C++17";
		auto str_any = util::Any(str_value);
		REQUIRE(util::any_cast<std::string>(str_any) == str_value);
	}

	SECTION("works with shared pointers") {
		const std::shared_ptr<bool> ptr_value = std::make_shared<bool>(true);
		auto ptr_any = util::Any(ptr_value);
		REQUIRE(util::any_cast<std::shared_ptr<bool>>(ptr_any) == ptr_value);
	}

	SECTION("throws on type error") {
		const std::string str_value = "util::Any is a replacement for the 'any' type in C++17";
		auto str_any = util::Any(str_value);
		REQUIRE_THROWS(util::any_cast<bool>(str_any));
	}

	SECTION("throws on emptiness") {
		util::Any any(true);
		any.reset();
		REQUIRE_THROWS(util::any_cast<bool>(any));
	}
}
