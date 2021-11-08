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
#include <realm/set.hpp>
#include <realm/sync/transform.hpp>

#include "test.hpp"
#include "util/quote.hpp"

#include "peer.hpp"
#include "fuzz_tester.hpp" // Transform_Randomized
#include "util/compare_groups.hpp"
#include "util/dump_changesets.hpp"

extern unsigned int unit_test_random_seed;

namespace {

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


TEST(Transform_OneClient)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());

    client->create_schema([](WriteTransaction& tr) {
        TableRef t = tr.get_or_add_table("class_foo");
        t->add_column(type_Int, "i");
    });
    synchronize(server.get(), {client.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client(client->shared_group);
    CHECK(compare_groups(read_server, read_client));
}


TEST(Transform_TwoClients)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    auto create_schema = [](WriteTransaction& tr) {
        TableRef foo = tr.get_or_add_table("class_foo");
        foo->add_column(type_Int, "i");
    };

    client_1->create_schema(create_schema);
    client_2->create_schema(create_schema);

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
}

TEST(Transform_AddTableInOrder)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    client_1->create_schema([](WriteTransaction& tr) {
        tr.get_or_add_table("class_foo");
        tr.get_or_add_table("class_bar");
    });

    client_2->create_schema([](WriteTransaction& tr) {
        tr.get_or_add_table("class_foo");
        tr.get_or_add_table("class_bar");
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
}

TEST(Transform_AddTableOutOfOrder)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    client_1->create_schema([](WriteTransaction& tr) {
        tr.get_or_add_table("class_foo");
        tr.get_or_add_table("class_bar");
    });

    client_2->create_schema([](WriteTransaction& tr) {
        tr.get_or_add_table("class_bar");
        tr.get_or_add_table("class_foo");
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
}

TEST(Transform_AddColumnsInOrder)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    client_1->create_schema([](WriteTransaction& tr) {
        TableRef foo = tr.get_or_add_table("class_foo");
        foo->add_column(type_Int, "foo_col");
        foo->add_column(type_String, "foo_col2");
        TableRef bar = tr.get_or_add_table("class_bar");
        bar->add_column(type_String, "bar_col");
    });

    client_2->create_schema([](WriteTransaction& tr) {
        TableRef foo = tr.get_or_add_table("class_foo");
        foo->add_column(type_Int, "foo_col");
        foo->add_column(type_String, "foo_col2");
        TableRef bar = tr.get_or_add_table("class_bar");
        bar->add_column(type_String, "bar_col");
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
}

TEST(Transform_AddColumnsOutOfOrder)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    client_1->create_schema([](WriteTransaction& tr) {
        TableRef bar = tr.get_or_add_table("class_bar");
        bar->add_column(type_String, "bar_col");
        TableRef foo = tr.get_or_add_table("class_foo");
        foo->add_column(type_Int, "foo_int");
        foo->add_column(type_String, "foo_string");
    });

    client_2->create_schema([](WriteTransaction& tr) {
        TableRef foo = tr.get_or_add_table("class_foo");
        foo->add_column(type_String, "foo_string");
        foo->add_column(type_Int, "foo_int");
        TableRef bar = tr.get_or_add_table("class_bar");
        bar->add_column(type_String, "bar_col");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2));
}

TEST(Transform_LinkListSet_vs_MoveLastOver)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());
    auto create_schema = [](WriteTransaction& transaction) {
        TableRef foo = transaction.get_or_add_table("class_foo");
        foo->add_column(type_Int, "i");
        TableRef bar = transaction.get_or_add_table("class_bar");
        bar->add_column_list(*foo, "ll");
    };
    client_1->create_schema(create_schema);
    client_2->create_schema(create_schema);

    client_1->transaction([](Peer& p) {
        p.table("class_foo")->create_object();
        ObjKey foo1 = p.table("class_foo")->create_object().get_key();
        auto ll = p.table("class_bar")->create_object().get_linklist("ll");
        ll.insert(0, foo1);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_2->transaction([](Peer& p) {
        ObjKey foo0 = p.table("class_foo")->begin()->get_key();
        auto ll = p.table("class_bar")->begin()->get_linklist("ll");
        ll.set(0, foo0);
    });

    client_1->transaction([](Peer& p) {
        p.table("class_foo")->remove_object(p.table("class_foo")->begin() + 0);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2));
}

TEST(Transform_LinkListInsert_vs_MoveLastOver)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());
    auto create_schema = [](WriteTransaction& transaction) {
        TableRef foo = transaction.get_or_add_table("class_foo");
        foo->add_column(type_Int, "i");
        TableRef bar = transaction.get_or_add_table("class_bar");
        bar->add_column_list(*foo, "ll");
    };
    client_1->create_schema(create_schema);
    client_2->create_schema(create_schema);

    client_1->transaction([](Peer& p) {
        p.table("class_foo")->create_object();
        p.table("class_foo")->create_object();
        p.table("class_bar")->create_object();
        auto ll = p.table("class_bar")->begin()->get_linklist("ll");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_2->transaction([](Peer& p) {
        auto ll = p.table("class_bar")->begin()->get_linklist("ll");
        TableRef target = p.table("class_foo");
        ll.insert(0, target->begin()->get_key());
    });

    client_1->transaction([](Peer& p) {
        p.table("class_foo")->remove_object(p.table("class_foo")->begin() + 0);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_client_1, read_client_2));
}


TEST(Transform_Experiment)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    auto schema = [](WriteTransaction& tr) {
        TableRef t = tr.get_or_add_table("class_t");
        TableRef t2 = tr.get_or_add_table("class_t2");
        t2->add_column(type_Int, "i");
        t->add_column_list(*t2, "ll");
    };

    client_1->create_schema(schema);
    client_2->create_schema(schema);

    client_1->transaction([&](Peer& c1) {
        TableRef t = c1.table("class_t");
        TableRef t2 = c1.table("class_t2");
        t->create_object();
        t2->create_object();
        t2->create_object();
        (t->begin() + 0)->get_linklist("ll").add(t2->begin()->get_key());
        (t->begin() + 0)->get_linklist("ll").add(t2->begin()->get_key());
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->transaction([&](Peer& c1) {
        TableRef t = c1.table("class_t");
        TableRef t2 = c1.table("class_t2");
        t2->remove_object(t2->begin() + 1);
        (t->begin() + 0)->get_linklist("ll").add(t2->begin()->get_key());
    });

    client_2->transaction([&](Peer& c2) {
        TableRef t = c2.table("class_t");
        TableRef t2 = c2.table("class_t2");
        (t->begin() + 0)->get_linklist("ll").set(1, (t2->begin() + 1)->get_key());
    });


    synchronize(server.get(), {client_1.get(), client_2.get()});
    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2));

    CHECK_EQUAL(read_server.get_table("class_t")->size(), 1);
    CHECK_EQUAL(read_server.get_table("class_t")->begin()->get_linklist("ll").size(), 2);
}

TEST(Transform_SelectLinkList)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    auto schema = [](WriteTransaction& tr) {
        TableRef t = tr.get_or_add_table("class_t");
        TableRef t2 = tr.get_or_add_table("class_t2");
        t2->add_column(type_Int, "i");
        t->add_column_list(*t2, "ll");
    };

    client_1->create_schema(schema);
    client_2->create_schema(schema);

    client_1->transaction([&](Peer& c1) {
        c1.table("class_t2")->create_object();
        c1.table("class_t")->create_object();
        c1.table("class_t")->create_object();
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->transaction([&](Peer& c1) {
        TableRef t = c1.table("class_t");
        TableRef t2 = c1.table("class_t2");
        (t->begin() + 1)->get_linklist("ll").add(t2->begin()->get_key());
        t->remove_object(t->begin() + 0);
    });

    client_2->transaction([&](Peer& c2) {
        TableRef t = c2.table("class_t");
        TableRef t2 = c2.table("class_t2");
        (t->begin() + 1)->get_linklist("ll").add(t2->begin()->get_key());
    });


    synchronize(server.get(), {client_1.get(), client_2.get()});
    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2));

    CHECK_EQUAL(read_server.get_table("class_t")->size(), 1);
    CHECK_EQUAL(read_server.get_table("class_t")->begin()->get_linklist("ll").size(), 2);
}


TEST(Transform_InsertRows)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    auto schema = [](WriteTransaction& tr) {
        TableRef t = tr.get_or_add_table("class_t");
        t->add_column(type_Int, "i");
    };

    client_1->create_schema(schema);
    client_2->create_schema(schema);
    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->start_transaction();
    client_1->table("class_t")->create_object();
    client_1->table("class_t")->begin()->set("i", 123);
    client_1->commit();

    client_2->start_transaction();
    client_2->table("class_t")->create_object();
    client_2->table("class_t")->begin()->set("i", 456);
    client_2->commit();

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2));
}

TEST(Transform_AdjustSetLinkPayload)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    auto schema = [](WriteTransaction& tr) {
        TableRef t = tr.get_or_add_table("class_t");
        t->add_column(type_Int, "i");
        TableRef l = tr.get_or_add_table("class_l");
        l->add_column(*t, "l");
    };

    client_1->create_schema(schema);
    client_2->create_schema(schema);
    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->transaction([](Peer& client_1) {
        TableRef t = client_1.table("class_t");
        TableRef l = client_1.table("class_l");
        t->create_object();
        t->begin()->set("i", 123);
        l->create_object();
        l->begin()->set("l", t->begin()->get_key());
    });

    client_2->transaction([](Peer& client_2) {
        TableRef t = client_2.table("class_t");
        TableRef l = client_2.table("class_l");
        t->create_object();
        t->begin()->set("i", 456);
        client_2.table("class_l")->create_object();
        l->begin()->set("l", t->begin()->get_key());
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2));

    {
        ConstTableRef t = read_client_1.get_table("class_t");
        ConstTableRef l = read_client_1.get_table("class_l");
        ObjKey link0 = l->begin()->get<ObjKey>("l");
        ObjKey link1 = (l->begin() + 1)->get<ObjKey>("l");
        CHECK_EQUAL(123, t->get_object(link0).get<int64_t>("i"));
        CHECK_EQUAL(456, t->get_object(link1).get<int64_t>("i"));
    }
}

TEST(Transform_AdjustLinkListSetPayload)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    auto schema = [](WriteTransaction& tr) {
        TableRef t = tr.get_or_add_table("class_t");
        t->add_column(type_Int, "i");
        TableRef l = tr.get_or_add_table("class_ll");
        l->add_column_list(*t, "ll");
    };

    client_1->create_schema(schema);
    client_2->create_schema(schema);
    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->transaction([](Peer& client_1) {
        client_1.table("class_t")->create_object();
        client_1.table("class_t")->begin()->set("i", 123);
        client_1.table("class_ll")->create_object();
        LnkLst ll = client_1.table("class_ll")->begin()->get_linklist("ll");
        ll.add((ll.get_target_table()->begin() + 0)->get_key());
    });

    client_2->transaction([](Peer& client_2) {
        client_2.table("class_t")->create_object();
        client_2.table("class_t")->begin()->set("i", 456);
        client_2.table("class_ll")->create_object();
        LnkLst ll = client_2.table("class_ll")->begin()->get_linklist("ll");
        ll.add((ll.get_target_table()->begin() + 0)->get_key());
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2));

    ConstTableRef client_1_table_link = read_client_1.get_table("class_ll");
    LnkLst ll = (client_1_table_link->begin() + 0)->get_linklist("ll");
    CHECK_EQUAL(123, ll.get_object(0).get<int64_t>("i"));
    CHECK_EQUAL(456, (client_1_table_link->begin() + 1)->get_linklist("ll").get_object(0).get<int64_t>("i"));
}

TEST(Transform_MergeInsertSetAndErase)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    auto schema = [](WriteTransaction& tr) {
        TableRef t = tr.get_or_add_table("class_t");
        t->add_column(type_Int, "i");
    };

    client_1->create_schema(schema);
    client_2->create_schema(schema);

    client_1->transaction([](Peer& client_1) {
        client_1.table("class_t")->create_object();
        client_1.table("class_t")->create_object();
        client_1.table("class_t")->begin()->set("i", 123);
        client_1.table("class_t")->get_object(1).set("i", 456);
    });
    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->transaction([](Peer& client_1) {
        client_1.table("class_t")->create_object();
        client_1.table("class_t")->get_object(2).set("i", 789);
    });

    client_2->transaction([](Peer& client_2) {
        TableRef t;
        client_2.table("class_t")->remove_object(client_2.table("class_t")->begin());
    });
    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2));

    {
        ConstTableRef t = read_client_1.get_table("class_t");
        CHECK_EQUAL(2, t->size());
    }
}


TEST(Transform_MergeSetLinkAndMoveLastOver)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    auto schema = [](WriteTransaction& tr) {
        TableRef t = tr.get_or_add_table("class_t");
        t->add_column(type_Int, "i");
        TableRef l = tr.get_or_add_table("class_l");
        l->add_column(*t, "l");
    };

    client_1->create_schema(schema);
    client_2->create_schema(schema);
    client_1->transaction([](Peer& client_1) {
        client_1.table("class_t")->create_object().set("i", 123);
    });
    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->transaction([](Peer& client_1) {
        auto k = client_1.table("class_t")->begin()->get_key();
        client_1.table("class_l")->create_object().set("l", k);
    });

    client_2->transaction([](Peer& client_2) {
        client_2.table("class_t")->begin()->remove();
    });
    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2));

    {
        ConstTableRef t = read_client_1.get_table("class_t");
        CHECK_EQUAL(0, t->size());
        ConstTableRef l = read_client_1.get_table("class_l");
        CHECK_EQUAL(1, l->size());
        ObjKey target_row = l->begin()->get<ObjKey>("l");
        CHECK_NOT(target_row);
    }
}


TEST(Transform_MergeSetDefault)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    auto schema = [](WriteTransaction& tr) {
        TableRef t = tr.get_group().add_table_with_primary_key("class_t", type_Int, "i");
        t->add_column(type_Int, "j");
    };

    client_1->create_schema(schema);
    client_2->create_schema(schema);
    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->transaction([&](Peer& client_1) {
        TableRef t = client_1.table("class_t");
        t->create_object_with_primary_key(123);
        bool is_default = false;
        t->begin()->set("j", 456, is_default);
    });

    // SetDefault at later timestamp.
    client_2->history.advance_time(100);

    client_2->transaction([&](Peer& client_2) {
        TableRef t = client_2.table("class_t");
        t->create_object_with_primary_key(123);
        bool is_default = true;
        t->begin()->set("j", 789, is_default);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});


    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2));

    ConstTableRef t = read_client_1.get_table("class_t");
    CHECK_EQUAL(t->size(), 1);
    // Check that the later SetDefault did not overwrite the Set instruction.
    CHECK_EQUAL(t->begin()->get<Int>("j"), 456);
}

TEST(Transform_MergeLinkListsWithPrimaryKeys)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    auto schema = [](WriteTransaction& tr) {
        TableRef t = tr.get_group().add_table_with_primary_key("class_t", type_Int, "i");
        TableRef t2 = tr.get_or_add_table("class_t2");
        t->add_column(type_String, "s");
        t->add_column_list(*t2, "ll");
        t2->add_column(type_Int, "i2");
    };

    client_1->create_schema(schema);
    client_2->create_schema(schema);
    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->transaction([](Peer& client_1) {
        TableRef t = client_1.table("class_t");
        TableRef t2 = client_1.table("class_t2");
        t->create_object_with_primary_key(123);
        t->begin()->set("s", "a");
        t2->create_object();
        t2->begin()->set("i2", 1);
        (t->begin() + 0)->get_linklist("ll").add(t2->begin()->get_key());
    });

    client_2->history.advance_time(10);

    client_2->transaction([](Peer& client_2) {
        TableRef t = client_2.table("class_t");
        TableRef t2 = client_2.table("class_t2");
        t->create_object_with_primary_key(123);
        t->begin()->set("s", "bb");
        t2->create_object();
        t2->begin()->set("i2", 2);
        auto ll = (t->begin() + 0)->get_linklist("ll");
        ll.add(ll.get_target_table()->begin()->get_key());
        ll.add(ll.get_target_table()->begin()->get_key());
    });

    client_1->history.advance_time(20);

    client_1->transaction([](Peer& client_1) {
        TableRef t = client_1.table("class_t");
        TableRef t2 = client_1.table("class_t2");
        auto k = t2->create_object().set("i2", 3).get_key();
        (t->begin() + 0)->get_linklist("ll").add(k);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2));

    ConstTableRef t = read_client_1.get_table("class_t");
    CHECK_EQUAL(t->size(), 1);
    CHECK_EQUAL(t->begin()->get<StringData>("s"), "bb");
    LnkLst lv = (t->begin() + 0)->get_linklist("ll");
    CHECK_EQUAL(lv.size(), 4);
    CHECK_EQUAL(lv.get_object(0).get<Int>("i2"), 1);
    CHECK_EQUAL(lv.get_object(1).get<Int>("i2"), 2);
    CHECK_EQUAL(lv.get_object(3).get<Int>("i2"), 3);
}

TEST(Transform_AddInteger)
{

    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    auto schema = [](WriteTransaction& tr) {
        TableRef t1 = tr.get_or_add_table("class_t");
        t1->add_column(type_Int, "i");
    };

    client_1->create_schema(schema);
    client_2->create_schema(schema);

    client_1->transaction([](Peer& client_1) {
        client_1.table("class_t")->create_object();
    });
    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->transaction([](Peer& client_1) {
        client_1.table("class_t")->begin()->add_int("i", 5);
    });
    client_2->transaction([](Peer& client_2) {
        client_2.table("class_t")->begin()->add_int("i", 4);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    {
        ReadTransaction read_server(server->shared_group);
        ReadTransaction read_client_1(client_1->shared_group);
        ReadTransaction read_client_2(client_2->shared_group);
        CHECK_EQUAL(read_server.get_table("class_t")->begin()->get<Int>("i"), 9);
        CHECK(compare_groups(read_server, read_client_1));
        CHECK(compare_groups(read_server, read_client_2));
    }

    client_2->history.advance_time(0);
    client_2->transaction([](Peer& client_2) {
        client_2.table("class_t")->begin()->add_int("i", 2); // This ends up being discarded
    });

    client_1->history.advance_time(10);
    client_1->transaction([](Peer& client_1) {
        client_1.table("class_t")->begin()->set("i", 100);
    });

    client_2->history.advance_time(20);
    client_2->transaction([](Peer& client_2) {
        client_2.table("class_t")->begin()->add_int("i", 3); // This comes after the set on client_1, so comes in
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});
    {
        ReadTransaction read_server(server->shared_group);
        ReadTransaction read_client_1(client_1->shared_group);
        ReadTransaction read_client_2(client_2->shared_group);
        CHECK_EQUAL(read_server.get_table("class_t")->begin()->get<Int>("i"), 103);
        CHECK(compare_groups(read_server, read_client_1));
        CHECK(compare_groups(read_server, read_client_2));
    }
}

TEST(Transform_AddIntegerSetNull)
{

    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    auto schema = [](WriteTransaction& tr) {
        TableRef t1 = tr.get_or_add_table("class_t");
        bool nullable = true;
        t1->add_column(type_Int, "i", nullable);
    };

    client_1->create_schema(schema);
    client_2->create_schema(schema);

    client_1->transaction([](Peer& client_1) {
        client_1.table("class_t")->create_object();
    });
    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->transaction([](Peer& client_1) {
        client_1.table("class_t")->begin()->set("i", 0);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_2->history.advance_time(0);
    client_2->transaction([](Peer& client_2) {
        client_2.table("class_t")->begin()->add_int("i", 2); // This ends up being discarded
    });

    client_1->history.advance_time(10);
    client_1->transaction([](Peer& client_1) {
        client_1.table("class_t")->begin()->set_null("i");
    });

    client_2->history.advance_time(20);
    client_2->transaction([](Peer& client_2) {
        client_2.table("class_t")->begin()->add_int("i", 3); // This ends up being discarded
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});
    {
        ReadTransaction read_server(server->shared_group);
        ReadTransaction read_client_1(client_1->shared_group);
        ReadTransaction read_client_2(client_2->shared_group);
        CHECK(read_server.get_table("class_t")->begin()->is_null("i"));
        CHECK(compare_groups(read_server, read_client_1));
        CHECK(compare_groups(read_server, read_client_2));
    }
}


TEST(Transform_EraseSelectedLinkView)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    auto init = [](WriteTransaction& tr) {
        TableRef origin = tr.get_or_add_table("class_origin");
        TableRef target = tr.get_or_add_table("class_target");
        origin->add_column_list(*target, "ll");
        target->add_column(type_Int, "");
        origin->create_object();
        origin->create_object();
        target->create_object();
        target->create_object();
        target->create_object();
        target->create_object();
        target->create_object();
        target->create_object();
        LnkLst link_list = (origin->begin() + 1)->get_linklist("ll");
        link_list.add(target->begin()->get_key());
        link_list.add((target->begin() + 1)->get_key());
    };

    client_1->create_schema(init);
    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    auto transact_1 = [](WriteTransaction& tr) {
        TableRef origin = tr.get_table("class_origin");
        LnkLst link_list = (origin->begin() + 1)->get_linklist("ll");
        auto target_table = link_list.get_target_table();
        link_list.set(0, target_table->get_object(2).get_key()); // Select the link list of the 2nd row
        origin->remove_object(origin->begin() + 0);     // Move that link list
        if (link_list.size() > 1) {
            link_list.set(1, target_table->get_object(3).get_key()); // Now modify it again
        }
    };
    auto transact_2 = [](WriteTransaction& tr) {
        TableRef origin = tr.get_table("class_origin");
        LnkLst link_list = (origin->begin() + 1)->get_linklist("ll");
        auto target_table = link_list.get_target_table();
        if (link_list.size() > 1) {
            link_list.set(0, target_table->get_object(4).get_key()); // Select the link list of the 2nd row
            link_list.set(1, target_table->get_object(5).get_key()); // Now modify it again
        }
    };

    client_1->create_schema(transact_1);
    client_2->create_schema(transact_2);
    synchronize(server.get(), {client_1.get(), client_2.get()});

    {
        ReadTransaction read_server(server->shared_group);
        ReadTransaction read_client_1(client_1->shared_group);
        ReadTransaction read_client_2(client_2->shared_group);
        CHECK(compare_groups(read_server, read_client_1));
        CHECK(compare_groups(read_server, read_client_2));

        ConstTableRef origin = read_server.get_table("class_origin");
        ConstTableRef target = read_server.get_table("class_target");
        CHECK_EQUAL(1, origin->size());
        LnkLst link_list = (origin->begin() + 0)->get_linklist("ll");
        CHECK_EQUAL(2, link_list.size());
        CHECK_EQUAL((target->begin() + 4)->get_key(), link_list.get(0));
        CHECK_EQUAL((target->begin() + 5)->get_key(), link_list.get(1));
    }
}


TEST(Transform_Randomized)
{
    const char* trace_p = ::getenv("UNITTEST_RANDOMIZED_TRACE");
    bool trace = trace_p && (StringData{trace_p} != "no");

    // FIXME: Unfortunately these rounds are terribly slow, presumable due to
    // sync-to-disk. Can we use "in memory" mode too boost them?
    int num_major_rounds = 100;
    int num_minor_rounds = 1;

    Random random(unit_test_random_seed); // Seed from slow global generator
    FuzzTester<Random> randomized(random, trace);

    for (int major_round = 0; major_round < num_major_rounds; ++major_round) {
        for (int minor_round = 0; minor_round < num_minor_rounds; ++minor_round) {
            if (trace)
                std::cerr << "---------------\n";
            randomized.round(test_context);
        }
        if (trace)
            std::cerr << "Round " << (major_round + 1) << "\n";
    }
}

namespace {

void integrate_changesets(Peer* peer_to, Peer* peer_from)
{
    size_t n = peer_to->count_outstanding_changesets_from(*peer_from);
    for (size_t i = 0; i < n; ++i)
        peer_to->integrate_next_changeset_from(*peer_from);
}


/// time_two_clients times the integration of change sets between two clients and a server.
/// The two clients create the same schema indepedently at start up and sync with the server.
/// The schema contains one table if same_table is true, and two tables if same_table is false.
/// The tables are given one integer column each. The clients insert nrows_1 and nrows_2 empty rows respectively in
/// their table. If fill_rows is true, the clients insert a value in each row.
/// If \a one_change_set is true, the clients insert all rows within one transaction. Otherwise
/// each row in inserted in its own transaction which will lead to one change set for each instruction.
/// The synchronization between the clients and the server progresses in steps:
/// The server integrates the change sets from client_1.
/// The server integrates the change sets from client_2. This is one of the two slow processes.
/// Client_1 integrates the new change sets from the server.
/// Client_2 integates the change sets from the server. This is the second slow process.
/// The function returns the tuple of durations (duration_server, duration_client_1, duration_client_2).
/// Durations are measured in milliseconds.
std::tuple<double, double, double> timer_two_clients(TestContext& test_context, const std::string path_add_on,
                                                     int nrows_1, int nrows_2, bool same_table, bool fill_rows,
                                                     bool one_change_set)
{
    std::string table_name_1 = "class_table_name_1";
    std::string table_name_2 = same_table ? table_name_1 : "class_table_name_2";

    // We don't bother dumping the changesets generated by the performance tests because they aren't
    // exercising any complex behavior of the merge rules.
    auto server = Peer::create_server(test_context, nullptr, path_add_on);
    auto client_1 = Peer::create_client(test_context, 2, nullptr, path_add_on);
    auto client_2 = Peer::create_client(test_context, 3, nullptr, path_add_on);

    client_1->create_schema([&](WriteTransaction& tr) {
        TableRef table = tr.get_or_add_table(table_name_1);
        table->add_column(type_Int, "int column");
    });

    client_2->create_schema([&](WriteTransaction& tr) {
        TableRef table = tr.get_or_add_table(table_name_2);
        table->add_column(type_Int, "int column");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    if (one_change_set)
        client_1->start_transaction();
    for (int i = 0; i < nrows_1; ++i) {
        if (!one_change_set)
            client_1->start_transaction();
        Obj obj = client_1->table(table_name_1)->create_object();
        if (fill_rows)
            obj.set("int column", 10 * i + 1);
        if (!one_change_set)
            client_1->commit();
    }
    if (one_change_set)
        client_1->commit();

    integrate_changesets(server.get(), client_1.get());

    if (one_change_set)
        client_2->start_transaction();
    for (int i = 0; i < nrows_2; ++i) {
        if (!one_change_set)
            client_2->start_transaction();
        Obj obj = client_2->table(table_name_2)->create_object();
        if (fill_rows)
            obj.set("int column", 10 * i + 2);
        if (!one_change_set)
            client_2->commit();
    }
    if (one_change_set)
        client_2->commit();

    // Timing the server integrating instructions from client_2.
    // This integration can suffer from the quadratic problem.
    auto time_start_server = std::chrono::high_resolution_clock::now();
    integrate_changesets(server.get(), client_2.get());
    auto time_end_server = std::chrono::high_resolution_clock::now();
    auto duration_server =
        std::chrono::duration_cast<std::chrono::milliseconds>(time_end_server - time_start_server).count();

    // Timing client_1 integrating change sets from the server.
    // This integration never suffers from the quadratic problem.
    auto time_start_client_1 = std::chrono::high_resolution_clock::now();
    integrate_changesets(client_1.get(), server.get());
    auto time_end_client_1 = std::chrono::high_resolution_clock::now();
    auto duration_client_1 =
        std::chrono::duration_cast<std::chrono::milliseconds>(time_end_client_1 - time_start_client_1).count();

    // Timing client_1 integrating instructions from the server.
    // This integration can suffer from the quadratic problem.
    // In cases where the quadratic factor dominates the timing, this duration
    // is expected to be similar to the duration od the server above.
    auto time_start_client_2 = std::chrono::high_resolution_clock::now();
    integrate_changesets(client_2.get(), server.get());
    auto time_end_client_2 = std::chrono::high_resolution_clock::now();
    auto duration_client_2 =
        std::chrono::duration_cast<std::chrono::milliseconds>(time_end_client_2 - time_start_client_2).count();


    // Check that the server and clients are synchronized
    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2));

    return std::make_tuple(double(duration_server), double(duration_client_1), double(duration_client_2));
}


/// timer_multi_clients handles \a nclients clients and one server.
/// First, the clients create the same schema and synchronizes it with the server.
/// Each of the clients insert \a nrows empty rows in their Realm.
/// Next everything is synced across all peers.
/// The return value is the duration of the server computation in milliseconds.
double timer_multi_clients(TestContext& test_context, const std::string path_add_on, int nclients, int nrows)
{
    std::string table_name = "class_table_name";

    // We don't bother dumping the changesets generated by the performance tests because they aren't
    // exercising any complex behavior of the merge rules.
    auto server = Peer::create_server(test_context, nullptr, path_add_on);
    std::vector<std::unique_ptr<Peer>> clients;
    for (int i = 0; i < nclients; ++i)
        clients.push_back(Peer::create_client(test_context, i + 2, nullptr, path_add_on));

    for (int i = 0; i < nclients; ++i) {
        clients[i]->create_schema([&](WriteTransaction& tr) {
            TableRef table = tr.get_or_add_table(table_name);
            table->add_column(type_Int, "int column");
        });
        integrate_changesets(server.get(), clients[i].get());
    }

    for (int i = 0; i < nclients; ++i) {
        integrate_changesets(clients[i].get(), server.get());
    }

    // Fill the clients with nrows empty rows
    for (int i = 0; i < nclients; ++i) {
        std::unique_ptr<Peer>& client = clients[i];
        client->start_transaction();
        for (int i = 0; i < nrows; ++i)
            client->table(table_name)->create_object();
        client->commit();
    }

    auto time_start = std::chrono::high_resolution_clock::now();

    // The server integrates all change sets from the clients.
    for (int i = 0; i < nclients; ++i)
        integrate_changesets(server.get(), clients[i].get());

    auto time_end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count();

    // The clients integrate the change sets from the server.
    // Each client obtains the change sets from all other clients
    for (int i = 0; i < nclients; ++i)
        integrate_changesets(clients[i].get(), server.get());

    // Check that the server and clients are synchronized
    ReadTransaction read_server(server->shared_group);
    for (int i = 0; i < nclients; ++i) {
        ReadTransaction read_client(clients[i]->shared_group);
        CHECK(compare_groups(read_server, read_client));
    }

    return double(duration);
}


/// Timing of the server integrating change sets.
/// The server is first populated with \a n_change_sets_server change sets
/// where each change set consists of \a n_instr_server instructions.
/// A client generates \a n_change_sets_client change sets each containing
/// \a n_instr_client instructions.
/// All instructions are insert_empty_row in the same table.
/// The funtions returns the time it take the server to integrate the incoming
/// \a n_change_sets_client change sets.
/// The incoming change sets are causally independent of the ones residing on the server.
double timer_integrate_change_sets(TestContext& test_context, const std::string path_add_on,
                                   uint_fast64_t n_change_sets_server, uint_fast64_t n_instr_server,
                                   uint_fast64_t n_change_sets_client, uint_fast64_t n_instr_client)
{
    std::string table_name = "class_table_name";

    // We don't bother dumping the changesets generated by the performance tests because they aren't
    // exercising any complex behavior of the merge rules.
    auto server = Peer::create_server(test_context, nullptr, path_add_on);
    auto client_1 = Peer::create_client(test_context, 2, nullptr, path_add_on);
    auto client_2 = Peer::create_client(test_context, 3, nullptr, path_add_on);

    client_1->create_schema([&](WriteTransaction& tr) {
        TableRef table = tr.get_or_add_table(table_name);
        table->add_column(type_Int, "int column");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    for (uint_fast64_t i = 0; i < n_change_sets_server; ++i) {
        client_1->start_transaction();
        for (uint_fast64_t j = 0; j < n_instr_server; ++j) {
            client_1->table(table_name)->create_object();
        }
        client_1->commit();
    }

    integrate_changesets(server.get(), client_1.get());

    for (uint_fast64_t i = 0; i < n_change_sets_client; ++i) {
        client_2->start_transaction();
        for (uint_fast64_t j = 0; j < n_instr_client; ++j) {
            client_2->table(table_name)->create_object();
        }
        client_2->commit();
    }

    auto time_start_server = std::chrono::high_resolution_clock::now();
    integrate_changesets(server.get(), client_2.get());
    auto time_end_server = std::chrono::high_resolution_clock::now();
    auto duration_server =
        std::chrono::duration_cast<std::chrono::milliseconds>(time_end_server - time_start_server).count();

    return double(duration_server);
}


void run_timer_two_clients(TestContext& test_context, std::string title, bool same_table, bool fill_rows,
                           bool one_change_set,
                           int max_single,  // The maximum number of rows
                           int max_product, // The maximum of a product of rows
                           std::ostream& out)
{
    out << std::endl << title << std::endl;
    out << "nrows_1\tnrows_2\tduration server\tduration client 1\tduration client 2" << std::endl;

    for (int nrows_1 = 1; nrows_1 <= max_single; nrows_1 *= 10) {
        for (int nrows_2 = 1; nrows_2 <= max_single && nrows_1 * nrows_2 <= max_product; nrows_2 *= 10) {
            std::cout << nrows_1 << ", " << nrows_2 << std::endl;
            std::string path_add_on = title + "_" + std::to_string(nrows_1) + "_" + std::to_string(nrows_2);
            double duration_server, duration_client_1, duration_client_2;
            std::tie(duration_server, duration_client_1, duration_client_2) =
                timer_two_clients(test_context, path_add_on, nrows_1, nrows_2, same_table, fill_rows, one_change_set);
            out << nrows_1 << "\t" << nrows_2 << "\t" << duration_server << "\t" << duration_client_1 << "\t"
                << duration_client_2 << std::endl;
        }
    }

    out << std::endl << std::endl;
}


void run_timer_two_clients_different_tables_empty_rows_one_change_set(TestContext& test_context, std::ostream& out)
{
    std::string title = "Two clients, different tables, empty rows, one change set";
    int max_single = int(1e6);
    int max_product = int(1e9);
    bool same_table = false;
    bool fill_rows = false;
    bool one_change_set = true;
    run_timer_two_clients(test_context, title, same_table, fill_rows, one_change_set, max_single, max_product, out);
}


void run_timer_two_clients_different_tables_empty_rows_many_change_sets(TestContext& test_context, std::ostream& out)
{
    std::string title = "Two clients, different tables, empty rows, many change sets";
    int max_single = int(1e5);
    int max_product = int(1e8);
    bool same_table = false;
    bool fill_rows = false;
    bool one_change_set = false;
    run_timer_two_clients(test_context, title, same_table, fill_rows, one_change_set, max_single, max_product, out);
}


void run_timer_two_clients_same_table_filled_rows_one_change_set(TestContext& test_context, std::ostream& out)
{
    std::string title = "Two clients, same table, filled rows, one change set";
    int max_single = int(1e6);
    int max_product = int(1e8);
    bool same_table = true;
    bool fill_rows = true;
    bool one_change_set = true;
    run_timer_two_clients(test_context, title, same_table, fill_rows, one_change_set, max_single, max_product, out);
}


void run_timer_two_clients_same_table_filled_rows_many_change_sets(TestContext& test_context, std::ostream& out)
{
    std::string title = "Two clients, same table, filled rows, many change sets";
    int max_single = int(1e5);
    int max_product = int(1e8);
    bool same_table = true;
    bool fill_rows = true;
    bool one_change_set = false;
    run_timer_two_clients(test_context, title, same_table, fill_rows, one_change_set, max_single, max_product, out);
}


void run_timer_many_clients_same_table_empty_rows(TestContext& test_context, std::ostream& out)
{
    out << "Many clients, same table, empty rows" << std::endl;
    out << "nclients\tnrows\tduration" << std::endl;

    int max_clients = 16;
    uint_fast64_t max_product = uint_fast64_t(1e11);
    for (int nclients = 1; nclients <= max_clients; nclients *= 2) {
        for (uint_fast64_t nrows = 1; nclients * nclients * nrows * nrows <= max_product; nrows *= 10) {
            std::string path_add_on = "many_clients_" + std::to_string(nclients) + "_" + std::to_string(nrows);
            double duration = timer_multi_clients(test_context, path_add_on, nclients, int(nrows));
            out << nclients << "\t" << nrows << "\t" << duration << std::endl;
        }
    }

    out << std::endl << std::endl;
}


void report_integrate_change_sets(TestContext& test_context, uint_fast64_t n_change_sets_server,
                                  uint_fast64_t n_instr_server, uint_fast64_t n_change_sets_client,
                                  uint_fast64_t n_instr_client, std::ostream& out)
{
    std::string path_add_on = "integrate_change_sets_" + std::to_string(n_change_sets_server) + "_" +
                              std::to_string(n_instr_server) + "_" + std::to_string(n_change_sets_client) + "_" +
                              std::to_string(n_instr_client);

    double duration = timer_integrate_change_sets(test_context, path_add_on, n_change_sets_server, n_instr_server,
                                                  n_change_sets_client, n_instr_client);

    uint_fast64_t n_merges = n_change_sets_server * n_instr_server * n_change_sets_client * n_instr_client;

    out << n_change_sets_server << "\t" << n_instr_server << "\t";
    out << n_change_sets_client << "\t" << n_instr_client << "\t";
    out << duration << "\t" << (n_merges / duration) << "\t" << (n_change_sets_client / duration) << std::endl;
}

/// This function can be used interactively to generate output for various combinations of parameters to
/// the report_integrate_change_sets function.
void run_timer_integrate_change_sets(TestContext& test_context, std::ostream& out)
{
    out << "integrate change sets of variable number of instructions" << std::endl;
    out << "n_change_sets_server\tn_instr_server\tn_change_sets_client\tn_instr_client\tduration in ms\t";
    out << "number of merges per ms\tnumber of integrated change sets per ms" << std::endl;

    uint_fast64_t n_change_sets_server = 1;
    uint_fast64_t n_instr_server = 1;
    uint_fast64_t n_change_sets_client = 1;
    uint_fast64_t n_instr_client = 1;

    //    for (n_change_sets_client = 1; n_change_sets_client < 1e6; n_change_sets_client *= 10)
    //        report_integrate_change_sets(test_context, n_change_sets_server, n_instr_server, n_change_sets_client,
    //        n_instr_client, out);

    //    for (n_instr_client = 1; n_change_sets_client * n_instr_client <= 1e7; n_instr_client *= 10)
    //        report_integrate_change_sets(test_context, n_change_sets_server, n_instr_server, n_change_sets_client,
    //        n_instr_client, out);

    //    for (n_change_sets_server = 1; n_change_sets_server < 1e5; n_change_sets_server *= 2)
    //        report_integrate_change_sets(test_context, n_change_sets_server, n_instr_server, n_change_sets_client,
    //        n_instr_client, out);

    for (n_instr_server = 100; n_instr_server <= uint_fast64_t(1e8); n_instr_server *= 10) {
        n_instr_client = uint_fast64_t(1e8) / n_instr_server;
        report_integrate_change_sets(test_context, n_change_sets_server, n_instr_server, n_change_sets_client,
                                     n_instr_client, out);
    }

    out << std::endl << std::endl;
}


void run_all_timers(TestContext& test_context, const std::string path)
{
    std::ofstream out{path, std::ios_base::app};

    run_timer_two_clients_different_tables_empty_rows_one_change_set(test_context, out);
    run_timer_two_clients_different_tables_empty_rows_many_change_sets(test_context, out);
    run_timer_two_clients_same_table_filled_rows_one_change_set(test_context, out);
    run_timer_two_clients_same_table_filled_rows_many_change_sets(test_context, out);
    run_timer_many_clients_same_table_empty_rows(test_context, out);
    run_timer_integrate_change_sets(test_context, out);

    out << std::endl;
}


} // End anonymous namespace


// This TEST is a benchmark that is placed here because it needs the machinery from this file.
// This benchmark should be moved when and if Sync gets a formal benchmarking system.
// This TEST should be disabled in normal unit testing.
// FIXME: Move this benchmark to a benchmark suite.
TEST(TransformTimer)
{
    const std::string path_of_performance_csv_file = "../../sync_performance_numbers.csv";

    // This should normally be false to avoid running the performance benchmark at every run of unit tests.
    const bool should_performance_test_be_run = false;

    if (should_performance_test_be_run)
        run_all_timers(test_context, path_of_performance_csv_file);
}


TEST(Transform_ErrorCase_LinkListDoubleMerge)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    client_1->transaction([](Peer& c) {
        TableRef a = c.group->add_table_with_primary_key("class_a", type_Int, "pk");
        TableRef b = c.group->add_table_with_primary_key("class_b", type_Int, "pk");
        a->add_column_list(*b, "ll");
        Obj a_obj = a->create_object_with_primary_key(123);
        Obj b_obj = b->create_object_with_primary_key(456);
        a_obj.get_linklist("ll").add(b_obj.get_key());
    });

    client_2->transaction([](Peer& c) {
        TableRef a = c.group->add_table_with_primary_key("class_a", type_Int, "pk");
        TableRef b = c.group->add_table_with_primary_key("class_b", type_Int, "pk");
        a->add_column_list(*b, "ll");
        Obj a_obj = a->create_object_with_primary_key(123);
        Obj b_obj = b->create_object_with_primary_key(456);
        a_obj.get_linklist("ll").add(b_obj.get_key());
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});
    ReadTransaction rt_0(server->shared_group);
    ReadTransaction rt_1(client_1->shared_group);
    ReadTransaction rt_2(client_2->shared_group);
    CHECK(compare_groups(rt_0, rt_1));
    CHECK(compare_groups(rt_0, rt_2));
    CHECK_EQUAL(rt_1.get_table("class_a")->begin()->get_linklist("ll").size(), 2);
}

TEST(Transform_ArrayInsert_EraseObject)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());
    ObjKey k0, k1;

    client_1->transaction([&k0, &k1](Peer& c) {
        TableRef source = c.group->add_table("class_source");
        TableRef target = c.group->add_table("class_target");
        source->add_column_list(*target, "ll");
        source->create_object();
        k0 = target->create_object().get_key();
        k1 = target->create_object().get_key();
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->transaction([k0, k1](Peer& c) {
        TableRef source = c.table("class_source");
        REALM_ASSERT(source);
        TableRef target = c.table("class_target");
        REALM_ASSERT(target);
        auto ll = source->begin()->get_linklist(source->get_column_key("ll"));
        ll.add(k0);
        ll.add(k1);
    });

    synchronize(server.get(), {client_1.get()});

    client_2->transaction([](Peer& c) {
        TableRef target = c.table("class_target");
        REALM_ASSERT(target);
        target->begin()->remove();
    });

    client_2.get()->integrate_next_changeset_from(*server);
}


TEST(Transform_ArrayClearVsArrayClear_TimestampBased)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());
    ColKey col_ints;

    // Create baseline
    client_1->transaction([&](Peer& c) {
        TableRef table = c.group->add_table("class_table");
        col_ints = table->add_column_list(type_Int, "ints");
        auto obj = table->create_object();
        auto ints = obj.get_list<int64_t>("ints");
        ints.insert(0, 1);
        ints.insert(1, 2);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    // Clear the list and insert new values on two clients. The client with the
    // higher timestamp should win, and its elements should survive.

    client_1->transaction([&](Peer& c) {
        TableRef table = c.group->get_table("class_table");
        auto obj = *table->begin();
        auto ints = obj.get_list<int64_t>("ints");
        ints.clear();
        ints.insert(0, 3);
        ints.insert(1, 4);
    });

    client_2->history.advance_time(1);

    client_2->transaction([&](Peer& c) {
        TableRef table = c.group->get_table("class_table");
        auto obj = *table->begin();
        auto ints = obj.get_list<int64_t>("ints");
        ints.clear();
        ints.insert(0, 5);
        ints.insert(1, 6);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction rt_0(server->shared_group);
    ReadTransaction rt_1(client_1->shared_group);
    ReadTransaction rt_2(client_2->shared_group);
    CHECK(compare_groups(rt_0, rt_1));
    CHECK(compare_groups(rt_0, rt_2));
    auto table = rt_0.get_table("class_table");
    auto obj = *table->begin();
    auto ints = obj.get_list<int64_t>(col_ints);
    CHECK_EQUAL(ints.size(), 2);
    CHECK_EQUAL(ints[0], 5);
    CHECK_EQUAL(ints[1], 6);
}

TEST(Transform_CreateEraseCreateSequencePreservesObject)
{
    // If two clients independently create an object, then erase the object, and
    // then recreate it, we want to preserve the object creation with the higher
    // timestamp.
    //
    // The previous behavior was that whoever had the most EraseObject
    // instructions "won".

    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    Associativity assoc{test_context, 2, changeset_dump_dir_gen.get()};
    assoc.for_each_permutation([&](auto& it) {
        auto server = &*it.server;
        auto client_1 = &*it.clients[0];
        auto client_2 = &*it.clients[1];

        // Disable history compaction to be certain that create-erase-create
        // cycles are not eliminated.
        server->history.set_disable_compaction(true);
        client_1->history.set_disable_compaction(true);
        client_2->history.set_disable_compaction(true);

        // Create baseline
        client_1->transaction([&](Peer& c) {
            auto table = c.group->add_table_with_primary_key("class_table", type_Int, "pk");
            table->add_column(type_Int, "int");
            auto obj = table->create_object_with_primary_key(123);
            obj.set<int64_t>("int", 0);
        });

        it.sync_all();

        // Create a Create-Erase-Create cycle.
        client_1->transaction([&](Peer& c) {
            TableRef table = c.group->get_table("class_table");
            auto obj = *table->begin();
            obj.remove();
            obj = table->create_object_with_primary_key(123);
            obj.set<int64_t>("int", 1);
            obj.remove();
            obj = table->create_object_with_primary_key(123);
            obj.set<int64_t>("int", 11);
        });

        client_2->history.advance_time(1);
        client_2->transaction([&](Peer& c) {
            TableRef table = c.group->get_table("class_table");
            auto obj = *table->begin();
            obj.remove();
            obj = table->create_object_with_primary_key(123);
            obj.set<int64_t>("int", 2);
        });

        it.sync_all();

        ReadTransaction rt_0(server->shared_group);
        auto table = rt_0.get_table("class_table");
        CHECK_EQUAL(table->size(), 1);
        auto obj = *table->begin();
        CHECK_EQUAL(obj.get<int64_t>("pk"), 123);
        CHECK_EQUAL(obj.get<int64_t>("int"), 2);
    });
}

TEST(Transform_AddIntegerSurvivesSetNull)
{
    // An AddInteger instruction merged with a Set(null) instruction with a
    // lower timestamp should not discard the AddInteger instruction. The
    // implication is that if a new Set(non-null) occurs "in between" the
    // Set(null) and the AddInteger instruction, ordered by timestamp, the
    // addition survives.

    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    Associativity assoc{test_context, 3, changeset_dump_dir_gen.get()};
    assoc.for_each_permutation([&](auto& it) {
        auto server = &*it.server;
        auto client_1 = &*it.clients[0];
        auto client_2 = &*it.clients[1];
        auto client_3 = &*it.clients[2];

        // Create baseline
        client_1->transaction([&](Peer& c) {
            auto table = c.group->add_table_with_primary_key("class_table", type_Int, "pk");
            const bool nullable = true;
            table->add_column(type_Int, "int", nullable);
            auto obj = table->create_object_with_primary_key(0);
            obj.set<int64_t>("int", 0);
        });

        it.sync_all();

        // At t0, set the field to NULL.
        client_1->transaction([&](Peer& c) {
            auto table = c.group->get_table("class_table");
            auto obj = *table->begin();
            CHECK(!obj.is_null("int"));
            CHECK_EQUAL(obj.get<util::Optional<int64_t>>("int"), 0);
            obj.set_null("int");
        });

        // At t2, increment the integer.
        client_2->history.advance_time(2);
        client_2->transaction([&](Peer& c) {
            auto table = c.group->get_table("class_table");
            auto obj = *table->begin();
            CHECK(!obj.is_null("int"));
            CHECK_EQUAL(obj.get<util::Optional<int64_t>>("int"), 0);
            obj.add_int("int", 1);
        });

        // Synchronize client_1 and client_2. The value should be NULL
        // afterwards. Note: Not using sync_all(), because we want the change
        // from client_3 to not be causally dependent on the state at this
        // point.
        synchronize(server, {client_1, client_2});

        {
            ReadTransaction rt{client_2->shared_group};
            auto table = rt.get_table("class_table");
            auto obj = *table->begin();
            CHECK(obj.is_null("int"));
        }

        // At t1, set the field to 10. Since the timeline is interleaved, the
        // final value should be 11, because the AddInteger from above should be
        // forward-ported on top of this value.
        client_3->history.advance_time(1);
        client_3->transaction([&](Peer& c) {
            auto table = c.group->get_table("class_table");
            auto obj = *table->begin();
            CHECK_EQUAL(obj.get<util::Optional<int64_t>>("int"), 0);
            obj.set<int64_t>("int", 10);
        });

        it.sync_all();

        {
            ReadTransaction rt_0{server->shared_group};
            auto table = rt_0.get_table("class_table");
            auto obj = *table->begin();
            CHECK_EQUAL(obj.get<util::Optional<int64_t>>("int"), 11);
        }
    });
}

TEST(Transform_AddIntegerSurvivesSetDefault)
{
    // Set(default) should behave as-if it occurred at the beginning of time.

    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    Associativity assoc{test_context, 3, changeset_dump_dir_gen.get()};
    assoc.for_each_permutation([&](auto& it) {
        auto server = &*it.server;
        auto client_1 = &*it.clients[0];
        auto client_2 = &*it.clients[1];
        auto client_3 = &*it.clients[2];

        // Create baseline
        client_1->transaction([&](Peer& c) {
            auto table = c.group->add_table_with_primary_key("class_table", type_Int, "pk");
            table->add_column(type_Int, "int");
            auto obj = table->create_object_with_primary_key(0);
        });

        it.sync_all();

        // At t1, set value explicitly.
        client_1->history.advance_time(1);
        client_1->transaction([&](Peer& c) {
            auto table = c.group->get_table("class_table");
            auto obj = *table->begin();
            obj.set("int", 1);
        });

        // At t2, increment value.
        client_2->history.advance_time(2);
        client_2->transaction([&](Peer& c) {
            auto table = c.group->get_table("class_table");
            auto obj = *table->begin();
            obj.add_int("int", 1);
        });

        // At t3, set default value.
        client_3->history.advance_time(3);
        client_3->transaction([&](Peer& c) {
            auto table = c.group->get_table("class_table");
            auto obj = *table->begin();
            const bool is_default = true;
            obj.set("int", 10, is_default);
        });

        it.sync_all();

        ReadTransaction rt_0(server->shared_group);
        auto table = rt_0.get_table("class_table");
        auto obj = *table->begin();
        // Expected outcome: The SetDefault instruction has no effect, so the result should be 2.
        CHECK_EQUAL(obj.get<int64_t>("int"), 2);
    });
}

TEST(Transform_AddIntegerSurvivesSetDefault_NoRegularSets)
{
    // Set(default) should behave as-if it occurred at the beginning of time.

    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    Associativity assoc{test_context, 3, changeset_dump_dir_gen.get()};
    assoc.for_each_permutation([&](auto& it) {
        auto server = &*it.server;
        auto client_1 = &*it.clients[0];
        auto client_2 = &*it.clients[1];
        auto client_3 = &*it.clients[2];

        // Create baseline
        client_1->transaction([&](Peer& c) {
            auto table = c.group->add_table_with_primary_key("class_table", type_Int, "pk");
            table->add_column(type_Int, "int");
            auto obj = table->create_object_with_primary_key(0);
        });

        it.sync_all();

        // At t1, set value explicitly.
        client_1->history.advance_time(1);
        client_1->transaction([&](Peer& c) {
            auto table = c.group->get_table("class_table");
            auto obj = *table->begin();
            const bool is_default = true;
            obj.set("int", 1, is_default);
        });

        // At t2, set a new default value.
        client_2->history.advance_time(2);
        client_2->transaction([&](Peer& c) {
            auto table = c.group->get_table("class_table");
            auto obj = *table->begin();
            const bool is_default = true;
            obj.set("int", 10, is_default);
        });

        // At t3, add something based on the default value.
        client_3->history.advance_time(3);
        client_3->transaction([&](Peer& c) {
            auto table = c.group->get_table("class_table");
            auto obj = *table->begin();
            obj.add_int("int", 1);
        });

        it.sync_all();

        ReadTransaction rt_0(server->shared_group);

        auto table = rt_0.get_table("class_table");
        auto obj = *table->begin();
        // Expected outcome: The AddInteger instruction should be rebased on top of the latest SetDefault instruction.
        CHECK_EQUAL(obj.get<int64_t>("int"), 11);
    });
}

TEST(Transform_DanglingLinks)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    Associativity assoc{test_context, 2, changeset_dump_dir_gen.get()};
    assoc.for_each_permutation([&](auto& it) {
        auto server = &*it.server;
        auto client_1 = &*it.clients[0];
        auto client_2 = &*it.clients[1];

        // Create baseline
        client_1->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.add_table_with_primary_key("class_table", type_Int, "pk");
            auto table2 = tr.add_table_with_primary_key("class_table2", type_Int, "pk");
            table->add_column_list(*table2, "links");
            auto obj = table->create_object_with_primary_key(0);
            auto obj2 = table2->create_object_with_primary_key(0);
            obj.get_linklist("links").insert(0, obj2.get_key());
        });

        it.sync_all();

        // Client 1 removes the target object.
        client_1->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.get_table("class_table");
            auto table2 = tr.get_table("class_table2");
            auto obj2 = table2->get_object_with_primary_key(0);
            obj2.remove();

            auto obj = table->get_object_with_primary_key(0);
            auto links = obj.get_linklist("links");
            CHECK_EQUAL(links.size(), 0);

            // Check that backlinks were eagerly removed
            auto keys = obj.get_list<ObjKey>("links");
            CHECK_EQUAL(keys.size(), 0);
        });

        // Client 2 adds a new link to the object.
        client_2->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.get_table("class_table");
            auto table2 = tr.get_table("class_table2");
            auto obj = table->get_object_with_primary_key(0);
            auto obj2 = table2->get_object_with_primary_key(0);
            auto links = obj.get_linklist("links");
            links.insert(1, obj2.get_key());
            CHECK_EQUAL(links.size(), 2);

            auto keys = obj.get_list<ObjKey>("links");
            CHECK_EQUAL(keys.size(), 2);
        });

        it.sync_all();

        ReadTransaction rt_0{server->shared_group};
        auto table = rt_0.get_table("class_table");
        auto table2 = rt_0.get_table("class_table2");
        CHECK_EQUAL(table2->size(), 0); // The object ended up being deleted

        auto objkey = table->find_primary_key(0);
        auto obj = table->get_object(objkey);

        // The "virtual" list should seem empty.
        auto links = obj.get_linklist("links");
        CHECK_EQUAL(links.size(), 0);

        // ... But the real list should contain 1 tombstone.
        auto keys = obj.get_list<ObjKey>(table->get_column_key("links"));
        CHECK_EQUAL(keys.size(), 1);
    });
}

TEST(Transform_Dictionary)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    Associativity assoc{test_context, 2, changeset_dump_dir_gen.get()};
    assoc.for_each_permutation([&](auto& it) {
        auto server = &*it.server;
        auto client_1 = &*it.clients[0];
        auto client_2 = &*it.clients[1];

        // Create baseline
        client_1->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
            table->add_column_dictionary(type_Mixed, "dict");
            table->create_object_with_primary_key(0);
            table->create_object_with_primary_key(1);
        });

        it.sync_all();

        // Populate dictionary on both sides.
        client_1->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.get_table("class_Table");
            auto obj0 = table->get_object_with_primary_key(0);
            auto obj1 = table->get_object_with_primary_key(1);
            auto dict0 = obj0.get_dictionary("dict");
            auto dict1 = obj1.get_dictionary("dict");

            dict0.insert("a", 123);
            dict0.insert("b", "Hello");
            dict0.insert("c", 45.0);

            dict1.insert("a", 456);
        });

        // Since client_2 has a higher peer ID, it should win this conflict.
        client_2->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.get_table("class_Table");
            auto obj0 = table->get_object_with_primary_key(0);
            auto obj1 = table->get_object_with_primary_key(1);
            auto dict0 = obj0.get_dictionary("dict");
            auto dict1 = obj1.get_dictionary("dict");

            dict0.insert("b", "Hello, World!");
            dict0.insert("d", true);

            dict1.insert("b", 789.f);
        });

        it.sync_all();

        ReadTransaction rt{server->shared_group};
        auto table = rt.get_table("class_Table");
        CHECK(table);
        auto obj0 = table->get_object_with_primary_key(0);
        auto obj1 = table->get_object_with_primary_key(1);
        auto dict0 = obj0.get_dictionary("dict");
        auto dict1 = obj1.get_dictionary("dict");

        CHECK_EQUAL(dict0.size(), 4);
        CHECK_EQUAL(dict0.get("a"), Mixed{123});
        CHECK_EQUAL(dict0.get("b"), Mixed{"Hello, World!"});
        CHECK_EQUAL(dict0.get("c"), Mixed{45.0});
        CHECK_EQUAL(dict0.get("d"), Mixed{true});

        CHECK_EQUAL(dict1.size(), 2);
        CHECK_EQUAL(dict1.get("a"), 456);
        CHECK_EQUAL(dict1.get("b"), 789.f);
    });
}

TEST(Transform_Set)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    Associativity assoc{test_context, 2, changeset_dump_dir_gen.get()};
    assoc.for_each_permutation([&](auto& it) {
        auto server = &*it.server;
        auto client_1 = &*it.clients[0];
        auto client_2 = &*it.clients[1];

        // Create baseline
        client_1->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
            table->add_column_set(type_Mixed, "set");
            table->create_object_with_primary_key(0);
        });

        it.sync_all();

        // Populate set on both sides.
        client_1->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.get_table("class_Table");
            auto obj = table->get_object_with_primary_key(0);
            auto set = obj.get_set<Mixed>("set");
            set.insert(999);
            set.insert("Hello");
            set.insert(123.f);
        });
        client_2->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.get_table("class_Table");
            auto obj = table->get_object_with_primary_key(0);
            auto set = obj.get_set<Mixed>("set");
            set.insert(999);
            set.insert("World");
            set.insert(456.f);

            // Erase an element from the set. Since client_2 has higher peer ID,
            // it should win the conflict.
            set.erase(999);
            set.insert(999);
            set.erase(999);
        });

        it.sync_all();

        ReadTransaction rt{server->shared_group};
        auto table = rt.get_table("class_Table");
        auto obj = table->get_object_with_primary_key(0);
        auto set = obj.get_set<Mixed>("set");
        CHECK_EQUAL(set.size(), 4);
        CHECK_NOT_EQUAL(set.find("Hello"), realm::npos);
        CHECK_NOT_EQUAL(set.find(123.f), realm::npos);
        CHECK_NOT_EQUAL(set.find("World"), realm::npos);
        CHECK_NOT_EQUAL(set.find(456.f), realm::npos);
        CHECK_EQUAL(set.find(999), realm::npos);
    });
}

TEST(Transform_ArrayEraseVsArrayErase)
{
    // This test case recreates the problem that the above test exposes
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_3 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());
    auto client_4 = Peer::create_client(test_context, 4, changeset_dump_dir_gen.get());
    auto client_5 = Peer::create_client(test_context, 5, changeset_dump_dir_gen.get());

    client_3->create_schema([](WriteTransaction& tr) {
        auto t = tr.get_group().add_table_with_primary_key("class_A", type_Int, "pk");
        t->add_column_list(type_String, "h");
        t->create_object_with_primary_key(5);
    });

    synchronize(server.get(), {client_3.get(), client_4.get(), client_5.get()});

    client_5->transaction([](Peer& p) {
        Obj obj = *p.table("class_A")->begin();
        auto ll = p.table("class_A")->begin()->get_list<String>("h");
        ll.insert(0, "5abc");
    });

    client_4->transaction([](Peer& p) {
        Obj obj = *p.table("class_A")->begin();
        auto ll = p.table("class_A")->begin()->get_list<String>("h");
        ll.insert(0, "4abc");
    });

    server->integrate_next_changeset_from(*client_5);
    server->integrate_next_changeset_from(*client_4);

    client_3->transaction([](Peer& p) {
        Obj obj = *p.table("class_A")->begin();
        auto ll = p.table("class_A")->begin()->get_list<String>("h");
        ll.insert(0, "3abc");
    });

    client_5->transaction([](Peer& p) {
        Obj obj = *p.table("class_A")->begin();
        auto ll = p.table("class_A")->begin()->get_list<String>("h");
        ll.insert(0, "5def");
    });

    server->integrate_next_changeset_from(*client_3);
    server->integrate_next_changeset_from(*client_5);

    client_4->transaction([](Peer& p) {
        Obj obj = *p.table("class_A")->begin();
        auto ll = p.table("class_A")->begin()->get_list<String>("h");
        ll.remove(0);
    });

    client_5->transaction([](Peer& p) {
        Obj obj = *p.table("class_A")->begin();
        auto ll = p.table("class_A")->begin()->get_list<String>("h");
        ll.remove(0);
    });

    server->integrate_next_changeset_from(*client_4);
    server->integrate_next_changeset_from(*client_5);
}

TEST(Transform_RSYNC_143)
{
    // Divergence between Create-Set-Erase and Create.

    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    Associativity assoc{test_context, 2, changeset_dump_dir_gen.get()};
    assoc.for_each_permutation([&](auto& it) {
        auto server = &*it.server;
        auto client_1 = &*it.clients[0];
        auto client_2 = &*it.clients[1];

        // Create baseline
        client_1->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
            table->add_column(type_Int, "int");
        });

        it.sync_all();

        client_1->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.get_table("class_Table");
            auto obj = table->create_object_with_primary_key(123);
            obj.set("int", 500);
            obj.remove();
        });

        client_2->history.advance_time(1);
        client_2->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.get_table("class_Table");
            table->create_object_with_primary_key(123);
        });

        it.sync_all();

        ReadTransaction rt{server->shared_group};
        auto table = rt.get_table("class_Table");
        CHECK_EQUAL(table->size(), 0);
    });
}

TEST(Transform_RSYNC_143_Fallout)
{
    // Divergence between Create-Set-Erase and Create.

    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    Associativity assoc{test_context, 2, changeset_dump_dir_gen.get()};
    assoc.for_each_permutation([&](auto& it) {
        auto server = &*it.server;
        auto client_1 = &*it.clients[0];
        auto client_2 = &*it.clients[1];

        // Create baseline
        client_1->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
            table->add_column(type_Int, "int");
        });

        it.sync_all();

        client_1->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.get_table("class_Table");
            auto obj = table->create_object_with_primary_key(123);
            obj.set("int", 500);
        });

        it.sync_all();

        client_1->history.advance_time(1);
        client_1->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.get_table("class_Table");
            auto obj = table->get_object_with_primary_key(123);
            obj.remove();
        });

        client_2->history.advance_time(1);
        client_2->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.get_table("class_Table");
            auto obj = table->create_object_with_primary_key(123);
            obj.set("int", 900);
            obj.remove();
        });

        it.sync_all();

        static_cast<void>(server);
    });
}

TEST(Transform_SetInsert_Clear_same_path)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_3 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    Mixed pk(1);
    client_2->transaction([&](Peer& c) {
        auto& tr = *c.group;
        auto table = tr.add_table_with_primary_key("class_Table", type_Int, "_id");
        auto embedded_table = tr.add_embedded_table("class_Embedded");
        auto link_col_key = table->add_column_list(*embedded_table, "embedded");
        auto set_col_key = embedded_table->add_column_set(type_Int, "set");
        auto obj = table->create_object_with_primary_key(pk);
        auto embedded_obj = obj.get_linklist(link_col_key).create_and_insert_linked_object(0);
        auto set = embedded_obj.get_set<Int>(set_col_key);
        set.insert(1);
    });

    synchronize(server.get(), {client_2.get(), client_3.get()});

    client_2->transaction([&](Peer& c) {
        auto& tr = *c.group;
        auto table = tr.get_table("class_Table");
        auto embedded_table = tr.get_table("class_Embedded");
        auto set = embedded_table->get_object(table->get_object_with_primary_key(pk).get_linklist("embedded").get(0))
                       .get_set<Int>("set");
        set.clear();
        set.insert(1);
    });

    client_3->transaction([&](Peer& c) {
        auto& tr = *c.group;
        auto table = tr.get_table("class_Table");
        auto embedded_table = tr.get_table("class_Embedded");
        auto set = embedded_table->get_object(table->get_object_with_primary_key(pk).get_linklist("embedded").get(0))
                       .get_set<Int>("set");
        set.insert(2);
    });

    server->integrate_next_changeset_from(*client_2);
    server->integrate_next_changeset_from(*client_3);

    {
        ReadTransaction check_tr(server->shared_group);
        auto table = check_tr.get_table("class_Table");
        auto embedded_table = check_tr.get_table("class_Embedded");
        auto set = embedded_table->get_object(table->get_object_with_primary_key(pk).get_linklist("embedded").get(0))
                       .get_set<Int>("set");
        CHECK_EQUAL(set.size(), size_t(1));
        CHECK_NOT_EQUAL(set.find(1), size_t(-1));
        CHECK_EQUAL(set.find(2), size_t(-1));
    }
}

TEST(Transform_SetInsert_Clear_different_paths)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_3 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    Mixed pk(1);
    // Create baseline
    client_2->transaction([&](Peer& c) {
        auto& tr = *c.group;
        auto table = tr.add_table_with_primary_key("class_Table", type_Int, "_id");
        auto embedded_table = tr.add_embedded_table("class_Embedded");
        auto link_col_key = table->add_column_list(*embedded_table, "embedded");
        auto set_col_key = embedded_table->add_column_set(type_Int, "set");
        auto obj = table->create_object_with_primary_key(pk);
        for (size_t i = 0; i < 2; ++i) {
            auto embedded_obj = obj.get_linklist(link_col_key).create_and_insert_linked_object(i);
            auto set = embedded_obj.get_set<Int>(set_col_key);
            set.insert(1);
            set.insert(2);
        }
    });

    synchronize(server.get(), {client_2.get(), client_3.get()});

    client_2->transaction([&](Peer& c) {
        auto& tr = *c.group;
        auto table = tr.get_table("class_Table");
        auto embedded_table = tr.get_table("class_Embedded");
        auto set = embedded_table->get_object(table->get_object_with_primary_key(pk).get_linklist("embedded").get(0))
                       .get_set<Int>("set");
        set.clear();
        set.insert(1);
    });

    client_3->transaction([&](Peer& c) {
        auto& tr = *c.group;
        auto table = tr.get_table("class_Table");
        auto embedded_table = tr.get_table("class_Embedded");
        auto set = embedded_table->get_object(table->get_object_with_primary_key(pk).get_linklist("embedded").get(1))
                       .get_set<Int>("set");
        set.insert(3);
    });

    server->integrate_next_changeset_from(*client_2);
    server->integrate_next_changeset_from(*client_3);

    {
        ReadTransaction check_tr(server->shared_group);
        auto table = check_tr.get_table("class_Table");
        auto embedded_table = check_tr.get_table("class_Embedded");
        auto set_1 =
            embedded_table->get_object(table->get_object_with_primary_key(pk).get_linklist("embedded").get(0))
                .get_set<Int>("set");
        auto set_2 =
            embedded_table->get_object(table->get_object_with_primary_key(pk).get_linklist("embedded").get(1))
                .get_set<Int>("set");
        CHECK_NOT_EQUAL(set_1.find(1), size_t(-1));
        CHECK_EQUAL(set_1.find(2), size_t(-1));
        CHECK_EQUAL(set_2.size(), size_t(3));
    }
}

TEST(Transform_SetErase_Clear_same_path)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_3 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    Mixed pk(1);
    client_2->transaction([&](Peer& c) {
        auto& tr = *c.group;
        auto table = tr.add_table_with_primary_key("class_Table", type_Int, "_id");
        auto embedded_table = tr.add_embedded_table("class_Embedded");
        auto link_col_key = table->add_column_list(*embedded_table, "embedded");
        auto set_col_key = embedded_table->add_column_set(type_Int, "set");
        auto obj = table->create_object_with_primary_key(pk);
        auto embedded_obj = obj.get_linklist(link_col_key).create_and_insert_linked_object(0);
        auto set = embedded_obj.get_set<Int>(set_col_key);
        set.insert(1);
        set.insert(2);
    });

    synchronize(server.get(), {client_2.get(), client_3.get()});

    client_2->transaction([&](Peer& c) {
        auto& tr = *c.group;
        auto table = tr.get_table("class_Table");
        auto embedded_table = tr.get_table("class_Embedded");
        auto set = embedded_table->get_object(table->get_object_with_primary_key(pk).get_linklist("embedded").get(0))
                       .get_set<Int>("set");
        CHECK_EQUAL(set.size(), size_t(2));
        set.clear();
        set.insert(2);
    });

    client_3->transaction([&](Peer& c) {
        auto& tr = *c.group;
        auto table = tr.get_table("class_Table");
        auto embedded_table = tr.get_table("class_Embedded");
        auto set = embedded_table->get_object(table->get_object_with_primary_key(pk).get_linklist("embedded").get(0))
                       .get_set<Int>("set");
        auto [size, erased] = set.erase(2);
        CHECK_EQUAL(size, 1);
        CHECK(erased);
    });

    server->integrate_next_changeset_from(*client_2);
    server->integrate_next_changeset_from(*client_3);

    {
        ReadTransaction check_tr(server->shared_group);
        auto table = check_tr.get_table("class_Table");
        auto embedded_table = check_tr.get_table("class_Embedded");
        auto set = embedded_table->get_object(table->get_object_with_primary_key(pk).get_linklist("embedded").get(0))
                       .get_set<Int>("set");
        CHECK_EQUAL(set.size(), size_t(1));
        CHECK_NOT_EQUAL(set.find(2), size_t(-1));
        CHECK_EQUAL(set.find(1), size_t(-1));
    }
}

TEST(Transform_SetErase_Clear_different_paths)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_3 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    Mixed pk(1);
    client_2->transaction([&](Peer& c) {
        auto& tr = *c.group;
        auto table = tr.add_table_with_primary_key("class_Table", type_Int, "_id");
        auto embedded_table = tr.add_embedded_table("class_Embedded");
        auto link_col_key = table->add_column_list(*embedded_table, "embedded");
        auto set_col_key = embedded_table->add_column_set(type_Int, "set");
        auto obj = table->create_object_with_primary_key(pk);
        for (size_t i = 0; i < 2; ++i) {
            auto embedded_obj = obj.get_linklist(link_col_key).create_and_insert_linked_object(i);
            auto set = embedded_obj.get_set<Int>(set_col_key);
            set.insert(1);
            set.insert(2);
        }
    });

    synchronize(server.get(), {client_2.get(), client_3.get()});

    client_2->transaction([&](Peer& c) {
        auto& tr = *c.group;
        auto table = tr.get_table("class_Table");
        auto embedded_table = tr.get_table("class_Embedded");
        auto set = embedded_table->get_object(table->get_object_with_primary_key(pk).get_linklist("embedded").get(0))
                       .get_set<Int>("set");
        CHECK_EQUAL(set.size(), size_t(2));
        set.clear();
    });

    client_3->transaction([&](Peer& c) {
        auto& tr = *c.group;
        auto table = tr.get_table("class_Table");
        auto embedded_table = tr.get_table("class_Embedded");
        auto set = embedded_table->get_object(table->get_object_with_primary_key(pk).get_linklist("embedded").get(1))
                       .get_set<Int>("set");
        auto erased = set.erase(1).second;
        CHECK(erased);
    });

    server->integrate_next_changeset_from(*client_2);
    server->integrate_next_changeset_from(*client_3);

    {
        ReadTransaction check_tr(server->shared_group);
        auto table = check_tr.get_table("class_Table");
        auto embedded_table = check_tr.get_table("class_Embedded");
        auto set_1 =
            embedded_table->get_object(table->get_object_with_primary_key(pk).get_linklist("embedded").get(0))
                .get_set<Int>("set");
        auto set_2 =
            embedded_table->get_object(table->get_object_with_primary_key(pk).get_linklist("embedded").get(1))
                .get_set<Int>("set");
        CHECK_EQUAL(set_1.size(), size_t(0));
        CHECK_EQUAL(set_2.size(), size_t(1));
        CHECK_EQUAL(set_2.find(1), size_t(-1));
        CHECK_NOT_EQUAL(set_2.find(2), size_t(-1));
    }
}

TEST(Transform_ArrayClearVersusClearRegression)
{
    // This test is automatically generated by fuzz testing, and would produce a
    // crash due to a failure to maintain the `prior_size` field of ArrayClear.

    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_3 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());
    client_2->start_transaction();
    client_2->group->get_or_add_table("class_F");
    client_2->commit(); // changeset 2
    client_2->history.advance_time(5);
    client_2->start_transaction();
    client_2->selected_table = client_2->group->get_table("class_F");
    client_2->selected_table->add_column_list(type_Int, "g", 0);
    client_2->commit(); // changeset 3
    client_3->start_transaction();
    client_3->group->get_or_add_table("class_F");
    client_3->commit(); // changeset 2
    client_2->history.advance_time(2);
    server->integrate_next_changeset_from(*client_2);
    client_2->history.advance_time(3);
    client_2->start_transaction();
    client_2->selected_table = client_2->group->get_table("class_F");
    client_2->selected_table->create_object();
    client_2->commit(); // changeset 4
    client_3->integrate_next_changeset_from(*server);
    client_2->start_transaction();
    client_2->selected_table = client_2->group->get_table("class_F");
    client_2->selected_array =
        client_2->selected_table->get_object(ObjKey(0)).get_list_ptr<int64_t>(ColKey(134217728));
    client_2->selected_array->clear();
    client_2->commit(); // changeset 5
    client_3->start_transaction();
    client_3->selected_table = client_3->group->get_table("class_F");
    client_3->selected_table->add_column(type_String, "c", 0);
    client_3->commit(); // changeset 4
    server->integrate_next_changeset_from(*client_2);
    server->integrate_next_changeset_from(*client_2);
    server->integrate_next_changeset_from(*client_3);
    client_3->history.advance_time(-5);
    server->integrate_next_changeset_from(*client_3);
    client_3->history.advance_time(4);
    client_3->integrate_next_changeset_from(*server);
    client_3->integrate_next_changeset_from(*server);
    client_3->start_transaction();
    client_3->selected_table = client_3->group->get_table("class_F");
    client_3->selected_array =
        client_3->selected_table->get_object(ObjKey(512)).get_list_ptr<int64_t>(ColKey(134217729));
    client_3->selected_array->clear();
    client_3->commit(); // changeset 7
    server->integrate_next_changeset_from(*client_2);
    client_2->history.advance_time(2);
    client_2->start_transaction();
    client_2->group->add_table_with_primary_key("class_C", type_Int, "pk");
    client_2->commit(); // changeset 6
    client_3->history.advance_time(1);
    server->integrate_next_changeset_from(*client_3);
    client_2->history.advance_time(1);
    client_2->integrate_next_changeset_from(*server);
    client_2->history.advance_time(3);
    client_2->start_transaction();
    client_2->selected_table = client_2->group->get_table("class_C");
    client_2->selected_table->create_object_with_primary_key(3);
    client_2->commit(); // changeset 8
    client_2->start_transaction();
    client_2->selected_table = client_2->group->get_table("class_C");
    client_2->selected_table->create_object_with_primary_key(6);
    client_2->commit(); // changeset 9
    server->integrate_next_changeset_from(*client_2);
    client_2->integrate_next_changeset_from(*server);
    client_2->history.advance_time(-14);
    server->integrate_next_changeset_from(*client_2);
    client_3->start_transaction();
    client_3->selected_table = client_3->group->get_table("class_F");
    client_3->selected_array =
        client_3->selected_table->get_object(ObjKey(512)).get_list_ptr<int64_t>(ColKey(134217729));
    static_cast<Lst<int64_t>*>(client_3->selected_array.get())->insert(0, 0);
    client_3->commit(); // changeset 8
    client_2->history.advance_time(1);
    client_2->start_transaction();
    client_2->selected_table = client_2->group->get_table("class_F");
    client_2->selected_array =
        client_2->selected_table->get_object(ObjKey(0)).get_list_ptr<int64_t>(ColKey(134217728));
    static_cast<Lst<int64_t>*>(client_2->selected_array.get())->insert(0, 0);
    client_2->commit(); // changeset 11
    client_3->start_transaction();
    client_3->selected_table = client_3->group->get_table("class_F");
    client_3->selected_array =
        client_3->selected_table->get_object(ObjKey(512)).get_list_ptr<int64_t>(ColKey(134217729));
    static_cast<Lst<int64_t>*>(client_3->selected_array.get())->set(0, 430);
    client_3->commit(); // changeset 9
    client_3->start_transaction();
    client_3->selected_table = client_3->group->get_table("class_F");
    client_3->selected_array =
        client_3->selected_table->get_object(ObjKey(512)).get_list_ptr<int64_t>(ColKey(134217729));
    static_cast<Lst<int64_t>*>(client_3->selected_array.get())->insert(1, 0);
    client_3->commit(); // changeset 10
    client_2->history.advance_time(1);
    client_2->start_transaction();
    client_2->selected_table = client_2->group->get_table("class_C");
    client_2->selected_table->add_column(type_Int, "b", 1);
    client_2->commit(); // changeset 12
    client_3->history.advance_time(2);
    client_3->integrate_next_changeset_from(*server);
    client_3->history.advance_time(2);
    server->integrate_next_changeset_from(*client_3);
    client_2->integrate_next_changeset_from(*server);
    client_2->history.advance_time(1);
    client_2->integrate_next_changeset_from(*server);
    client_2->history.advance_time(2);
    server->integrate_next_changeset_from(*client_2);
    client_2->start_transaction();
    client_2->selected_table = client_2->group->get_table("class_F");
    client_2->selected_array =
        client_2->selected_table->get_object(ObjKey(0)).get_list_ptr<int64_t>(ColKey(134217728));
    client_2->selected_array->clear();
    client_2->commit(); // changeset 15
    client_3->history.advance_time(4);
    client_3->start_transaction();
    client_3->selected_table = client_3->group->get_table("class_F");
    client_3->selected_array =
        client_3->selected_table->get_object(ObjKey(512)).get_list_ptr<int64_t>(ColKey(134217729));
    client_3->selected_array->clear();
    client_3->commit(); // changeset 12
    client_3->start_transaction();
    client_3->selected_table = client_3->group->get_table("class_F");
    client_3->selected_array =
        client_3->selected_table->get_object(ObjKey(512)).get_list_ptr<int64_t>(ColKey(134217729));
    client_3->selected_array->clear();
    client_3->commit(); // changeset 13
    client_3->history.advance_time(5);
    client_3->integrate_next_changeset_from(*server);
    client_3->history.advance_time(4);
    server->integrate_next_changeset_from(*client_3);
    client_3->history.advance_time(1);
    client_3->integrate_next_changeset_from(*server);
    client_3->history.advance_time(4);
    server->integrate_next_changeset_from(*client_3);
    client_2->history.advance_time(4);
    server->integrate_next_changeset_from(*client_2);
    client_3->history.advance_time(4);
    client_3->start_transaction();
    client_3->selected_table = client_3->group->get_table("class_C");
    client_3->selected_table->create_object_with_primary_key(4);
    client_3->commit(); // changeset 16
    client_2->history.advance_time(5);
    client_2->integrate_next_changeset_from(*server);
    client_3->history.advance_time(1);
    client_3->start_transaction();
    client_3->selected_table = client_3->group->get_table("class_F");
    client_3->selected_table->get_object(ObjKey(512)).set(ColKey(131072), "1", 0);
    client_3->commit(); // changeset 17
    client_2->integrate_next_changeset_from(*server);
    client_3->start_transaction();
    client_3->selected_table = client_3->group->get_table("class_F");
    client_3->selected_array =
        client_3->selected_table->get_object(ObjKey(512)).get_list_ptr<int64_t>(ColKey(134217729));
    client_3->selected_array->clear();
    client_3->commit(); // changeset 18
    client_2->history.advance_time(4);
    client_2->start_transaction();
    client_2->selected_table = client_2->group->get_table("class_F");
    client_2->selected_array =
        client_2->selected_table->get_object(ObjKey(0)).get_list_ptr<int64_t>(ColKey(134217728));
    static_cast<Lst<int64_t>*>(client_2->selected_array.get())->insert(0, 0);
    client_2->commit(); // changeset 18
    server->integrate_next_changeset_from(*client_2);
    client_2->start_transaction();
    client_2->group->add_table_with_primary_key("class_E", type_Int, "pk");
    client_2->commit(); // changeset 19
    client_3->start_transaction();
    client_3->selected_table = client_3->group->get_table("class_F");
    client_3->selected_array =
        client_3->selected_table->get_object(ObjKey(512)).get_list_ptr<int64_t>(ColKey(134217729));
    client_3->selected_array->clear();
    client_3->commit(); // changeset 19
    client_3->history.advance_time(1);
    client_3->start_transaction();
    client_3->selected_table = client_3->group->get_table("class_F");
    client_3->selected_table->get_object(ObjKey(512)).set(ColKey(131072), "2", 0);
    client_3->commit(); // changeset 20
    client_2->history.advance_time(5);
    client_2->start_transaction();
    client_2->selected_table = client_2->group->get_table("class_F");
    client_2->selected_array =
        client_2->selected_table->get_object(ObjKey(0)).get_list_ptr<int64_t>(ColKey(134217728));
    client_2->selected_array->clear();
    client_2->commit(); // changeset 20
    server->integrate_next_changeset_from(*client_2);
    server->integrate_next_changeset_from(*client_2);
    client_3->start_transaction();
    client_3->selected_table = client_3->group->get_table("class_C");
    client_3->selected_table->create_object_with_primary_key(9);
    client_3->commit(); // changeset 21
    server->integrate_next_changeset_from(*client_2);
    client_2->start_transaction();
    client_2->selected_table = client_2->group->get_table("class_F");
    client_2->selected_array =
        client_2->selected_table->get_object(ObjKey(0)).get_list_ptr<int64_t>(ColKey(134217728));
    static_cast<Lst<int64_t>*>(client_2->selected_array.get())->insert(0, 0);
    client_2->commit(); // changeset 21
    client_3->history.advance_time(5);
    client_3->integrate_next_changeset_from(*server);
    client_2->start_transaction();
    client_2->selected_table = client_2->group->get_table("class_F");
    client_2->selected_array =
        client_2->selected_table->get_object(ObjKey(0)).get_list_ptr<int64_t>(ColKey(134217728));
    static_cast<Lst<int64_t>*>(client_2->selected_array.get())->insert(0, 0);
    client_2->commit(); // changeset 22
    client_3->history.advance_time(2);
    server->integrate_next_changeset_from(*client_3);
    client_2->history.advance_time(2);
    server->integrate_next_changeset_from(*client_2);
}

} // unnamed namespace
