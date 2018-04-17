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

#include <cstring>
#include <typeinfo>
#include <limits>
#include <vector>
#include <map>
#include <sstream>
#include <fstream>
#include <iostream>

#include <unistd.h>
#include <sys/wait.h>

#include <realm.hpp>
#include <realm/impl/destroy_guard.hpp>
#include <realm/impl/simulated_failure.hpp>
#include <realm/column_string.hpp>
#include <realm/column_string_enum.hpp>
#include <realm/column_mixed.hpp>
#include <realm/array_blobs_small.hpp>
#include <realm/array_string_long.hpp>
#include <realm/lang_bind_helper.hpp>
#include <realm/replication.hpp>
#include <realm/commit_log.hpp>

#include "../test.hpp"
#include "../util/demangle.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
using namespace realm::_impl;
