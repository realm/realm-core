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

#include <cstdlib>
#include <algorithm>
#include <memory>
#include <iostream>

#include <realm.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/shared_realm.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>
#include "../../util/timer.hpp"
#include "../../util/random.hpp"
//#include "../../util/unit_test.hpp"
#include "../../test.hpp"
#include "../../test_table_helper.hpp"
using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
// using unit_test::TestContext;

#ifdef REALM_CLUSTER_IF
using OrderVec = std::vector<ObjKey>;
#else
using OrderVec = std::vector<size_t>;
#endif


enum step_type { DIRECT, INDEXED, PK };
int main()
{
    auto run_steps = [&](int num_steps, int step_size, step_type st) {
        TestPathGuard guard("benchmark-insertion.realm");
        std::string path(guard);

        Realm::Config config;
        config.cache = true;
        config.path = path;
        config.schema_version = 1;
        if (st == INDEXED) {
            config.schema = Schema{
                {"object", {{"value", PropertyType::String, Property::IsPrimary{false}, Property::IsIndexed{true}}}},
            };
        }
        else if (st == PK) {
            config.schema = Schema{
                {"object", {{"value", PropertyType::String, Property::IsPrimary{true}}}},
            };
        }
        else {
            config.schema = Schema{
                {"object", {{"value", PropertyType::String}}},
            };
        }
        auto realm = Realm::get_shared_realm(config);
        {
            realm->begin_transaction();
            auto t = realm->read_group().add_table("object");
            auto col = t->add_column(type_String, "value");
            if (st == INDEXED) {
                t->add_search_index(col);
            }
            if (st == PK) {
                t->set_primary_key_column(col);
            }
            realm->commit_transaction();
        }

        std::cout << "Run with type " << st << " format " << num_steps << " x " << step_size << std::endl;
        auto start = std::chrono::steady_clock::now();
        for (int j = 0; j < num_steps * step_size; j += step_size) {
            CppContext d(realm);
            realm->begin_transaction();
            for (int i = j; i < j + step_size; ++i) {
                std::string s(std::to_string(i));
                util::Any v = AnyDict{{"value", s}};
                Object::create(d, realm, *realm->schema().find("object"), v, CreatePolicy::ForceCreate);
            }
            realm->commit_transaction();
            auto end = std::chrono::steady_clock::now();
            auto diff = end - start;
            auto print_diff = std::chrono::duration_cast<std::chrono::milliseconds>(diff);
            std::cout << j << " " << print_diff.count() << std::endl;
        }
    };
    auto run_type = [&](step_type st) {
        std::cout << "Run for type " << st << std::endl;
        run_steps(10, 1000000, st);
        run_steps(30, 333333, st);
        run_steps(100, 100000, st);
        run_steps(300, 33333, st);
        run_steps(1000, 10000, st);
        run_steps(3000, 3333, st);
        run_steps(10000, 1000, st);
    };
    run_type(DIRECT);
    run_type(INDEXED);
    run_type(PK);
}
