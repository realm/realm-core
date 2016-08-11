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
#ifdef TEST_DESTRUCTOR_THREAD_SAFETY

#include <realm.hpp>
#include <realm/util/features.h>
#include <memory>

#include "test.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
using unit_test::TestContext;

ONLY(ThreadSafety_TableView) {
    std::vector<std::shared_ptr<TableView>> ptrs;
    Mutex mutex;
    test_util::ThreadWrapper thread;

    thread.start([&mutex, &ptrs] {
            while (true) {
                LockGuard lock(mutex);
                ptrs.clear();
            }
        });

    while (true) {
        Group group;

        TableRef table = group.add_table("table");
        table->add_column(type_Int, "int");
        auto table_view = std::make_shared<TableView>(table->where().find_all());
        {
            LockGuard lock(mutex);
            ptrs.push_back(table_view);
        }
    }
}

#endif

