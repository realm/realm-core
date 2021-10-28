#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <utility>
#include <memory>
#include <initializer_list>
#include <string>
#include <vector>
#include <map>
#include <sstream>
#include <iostream>
#include <fstream>

#include <realm/util/features.h>
#include <realm/binary_data.hpp>
#include <realm/db.hpp>
#include <realm/replication.hpp>
#include <realm/list.hpp>
#include <realm/sync/transform.hpp>

#include "test.hpp"
#include "util/quote.hpp"

#include "peer.hpp"
#include "util/compare_groups.hpp"
#include "util/dump_changesets.hpp"

using namespace realm;
using namespace realm::sync;
using namespace realm::test_util;
using unit_test::TestContext;

// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment varible `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.


TEST(Array_Example)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    auto create_schema = [](WriteTransaction& tr) {
        TableRef foobar = tr.add_table("class_foobar");
        foobar->add_column(type_Int, "foo");
        foobar->add_column_list(type_Int, "bar");
    };

    client_1->create_schema(create_schema);
    client_2->create_schema(create_schema);

    client_1->transaction([](Peer& p) {
        Obj obj = p.table("class_foobar")->create_object();
        auto bar = p.table("class_foobar")->get_column_key("bar");

        auto foo = p.table("class_foobar")->get_column_key("foo");
        p.table("class_foobar")->begin()->set(foo, 1);

        auto array = obj.get_list<int64_t>(bar);
        array.add(123);
        array.add(124);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_2->transaction([](Peer& p) {
        // sync::create_object(*p.group, *p.table("foobar"));
        auto bar = p.table("class_foobar")->get_column_key("bar");

        auto foo = p.table("class_foobar")->get_column_key("foo");
        p.table("class_foobar")->begin()->set(foo, 2);

        auto array = p.table("class_foobar")->begin()->get_list<int64_t>(bar);
        array.add(456);
        array.add(457);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    {
        ReadTransaction read_client_1(client_1->shared_group);
        CHECK(compare_groups(read_server, read_client_1));
    }
    {
        ReadTransaction read_client_2(client_2->shared_group);
        CHECK(compare_groups(read_server, read_client_2));
    }

    std::vector<int64_t> values1, values2;

    client_1->transaction([&values1](Peer& p) {
        auto bar = p.table("class_foobar")->get_column_key("bar");
        auto array = p.table("class_foobar")->begin()->get_list<int64_t>(bar);

        for (size_t i = 0; i < array.size(); i++)
            values1.push_back(array.get(i));
    });

    client_2->transaction([&values2](Peer& p) {
        auto bar = p.table("class_foobar")->get_column_key("bar");
        auto array = p.table("class_foobar")->begin()->get_list<int64_t>(bar);

        for (size_t i = 0; i < array.size(); i++)
            values2.push_back(array.get(i));
    });

    CHECK(values1 == values2);
}
