/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include "testsettings.hpp"

#include <optional>
#include <realm/util/optional.hpp>

#include "test.hpp"

using namespace realm::util;

static_assert(is_optional<std::optional<int>>::value, "");
static_assert(!is_optional<int>::value, "");
static_assert(std::is_same_v<RemoveOptional<std::optional<int>>::type, int>, "");
static_assert(std::is_same_v<RemoveOptional<int>::type, int>, "");
