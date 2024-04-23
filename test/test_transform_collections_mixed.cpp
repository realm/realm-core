#include "peer.hpp"
#include "util/dump_changesets.hpp"

using namespace realm;
using namespace realm::test_util;

struct TransformTestHarness {
    enum ConflictOrdering { ClientOneBeforeTwo, ClientTwoBeforeOne, SameTime };

    template <typename Func>
    explicit TransformTestHarness(unit_test::TestContext& test_context, std::optional<ConflictOrdering> ordering,
                                  Func&& baseline_func)
        : TransformTestHarness(test_context)
    {
        client_1->transaction([&](Peer& c) {
            auto& tr = *c.group;
            TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
            auto col_any = table->add_column(type_Mixed, "any");
            auto obj = table->create_object_with_primary_key(1);
            baseline_func(obj, col_any);
        });

        synchronize(server.get(), {client_1.get(), client_2.get()});

        if (!ordering)
            return;

        switch (*ordering) {
            case SameTime:
                break;
            case ClientOneBeforeTwo:
                client_1->history.set_time(1);
                client_2->history.set_time(2);
                break;
            case ClientTwoBeforeOne:
                client_2->history.set_time(1);
                client_1->history.set_time(2);
                break;
        }
    }

    explicit TransformTestHarness(unit_test::TestContext& test_context)
        : test_context(test_context)
        , changeset_dump_dir_gen(get_changeset_dump_dir_generator(test_context))
        , server(Peer::create_server(test_context, changeset_dump_dir_gen.get()))
        , client_1(Peer::create_client(test_context, 2, changeset_dump_dir_gen.get()))
        , client_2(Peer::create_client(test_context, 3, changeset_dump_dir_gen.get()))
    {
    }

    template <typename Func>
    void transaction(const std::unique_ptr<Peer>& p, Func&& func, std::optional<int> timestamp = {})
    {
        if (timestamp) {
            p->history.set_time(*timestamp);
        }
        p->transaction([&](Peer& p) {
            auto col_any = p.table("class_Table")->get_column_key("any");
            auto obj = p.table("class_Table")->get_object_with_primary_key(1);
            func(obj, col_any);
        });
    }

    template <typename Func>
    void check_merge_result(Func&& func)
    {
        synchronize(server.get(), {client_1.get(), client_2.get()});

        auto read_server = server->shared_group->start_read();
        auto read_client_1 = client_1->shared_group->start_read();
        auto read_client_2 = client_2->shared_group->start_read();

        CHECK(compare_groups(*read_server, *read_client_1));
        CHECK(compare_groups(*read_server, *read_client_2, *test_context.logger));
        auto table = read_server->get_table("class_Table");
        auto col_any = table->get_column_key("any");
        func(table->get_object_with_primary_key(Mixed{1}), col_any);
    }

    unit_test::TestContext& test_context;
    std::unique_ptr<TestDirNameGenerator> changeset_dump_dir_gen;
    std::unique_ptr<Peer> server;
    std::unique_ptr<Peer> client_1;
    std::unique_ptr<Peer> client_2;
};

// Test merging instructions at different level of nesting.

TEST(Transform_CreateArrayVsArrayInsert)
{
    // Baseline: set property 'any' to Dictionary
    // {id: 1, any: {}}
    TransformTestHarness h(test_context, TransformTestHarness::SameTime, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    // Client 1 sets property 'any' from Dictionary to List
    // {id: 1, any: []}
    h.client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
    });

    // Client 2 sets property 'any' from Dictionary to List and inserts one integer in the list
    // {id: 1, any: [42]}
    h.client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(42);
    });

    // Result: Both changes are accepted - both clients are setting the same type (idempotent),
    // and the insert is on the same list
    // {id: 1, any: [42]}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>(col_any);
        CHECK_EQUAL(list->size(), 1);
        CHECK_EQUAL(list->get(0), 42);
    });
}

TEST(Transform_Nested_CreateArrayVsArrayInsert)
{
    // Baseline: set 'any.A.0' to List and insert one integer in the list
    // {id: 1, any: {{"A": [[42]]}}}
    TransformTestHarness h(test_context, TransformTestHarness::SameTime, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::List);
        auto list2 = list->get_list(0);
        list2->insert(0, 42);
    });

    // Client 2 sets 'any.A.0.0' from integer to List
    // {id: 1, any: {{"A": [[[]]]}}}
    h.transaction(h.client_2, [&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A", 0});
        list->set_collection(0, CollectionType::List);
    });

    synchronize(h.server.get(), {h.client_2.get()});

    // Client 1 sets 'any.A.0.0' from integer to List
    // {id: 1, any: {{"A": [[[]]]}}}
    h.transaction(h.client_1, [&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A", 0});
        list->set_collection(0, CollectionType::List);
    });

    // Client 2 inserts one integer in list at 'any.A.0.0'
    // {id: 1, any: {{"A": [[[42]]]}}}
    h.transaction(h.client_2, [&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A", 0, 0});
        list->add(42);
    });

    // Result: Both changes are accepted - both clients are setting the same type (idempotent),
    // and the insert is on the same type
    // {id: 1, any: {{"A": [[[42]]]}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A", 0, 0});
        CHECK_EQUAL(list->size(), 1);
        CHECK_EQUAL(list->get(0), 42);
    });
}

TEST(Transform_CreateArrayVsDictionaryInsert)
{
    // Baseline: set property 'any' to Dictionary
    // {id: 1, any: {}}
    TransformTestHarness h(test_context, TransformTestHarness::SameTime, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    // Client 1 sets property 'any' from Dictionary to List
    // {id: 1, any: []}
    h.transaction(h.client_1, [&](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
    });

    // Client 2 inserts one integer in dictionary at 'any'
    // {id: 1, any: {{"key": 42}}}
    h.transaction(h.client_2, [&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert("key", 42);
    });

    // Result: Client 1 wins because its update is higher up in the path
    // {id: 1, any: []}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK(obj.get_list_ptr<Mixed>(col_any)->is_empty());
    });
}

TEST(Transform_Nested_CreateArrayVsDictionaryInsert)
{
    // Baseline: set 'any.A.0.B' to Dictionary
    // {id: 1, any: {{"A": [{{"B": {}}}]}}}
    TransformTestHarness h(test_context, TransformTestHarness::SameTime, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert_collection("B", CollectionType::Dictionary);
    });

    // Client 1 sets 'any.A.0.B' from Dictionary to List
    // {id: 1, any: {{"A": [{{"B": []}}]}}}
    h.transaction(h.client_1, [&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
    });

    // Client 2 inserts one integer in dictionary at 'any.A.0.B'
    // {id: 1, any: {{"A": [{{"B": {{"key": 42}}}}]}}}
    h.transaction(h.client_2, [&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0, "B"});
        dict->insert("key", 42);
    });

    // Result: Client 1 wins because its update is higher up in the path
    // {id: 1, any: {{"A": [{{"B": []}}]}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK(obj.get_list_ptr<Mixed>({col_any, "A", 0, "B"})->is_empty());
    });
}

TEST(Transform_CreateDictionaryVsDictionaryInsert)
{
    // Baseline: set property 'any' to List
    // {id: 1, any: []}
    TransformTestHarness h(test_context, TransformTestHarness::SameTime, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
    });

    auto set_nested_dictionary = [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
    };

    // Client 2 sets property 'any' from List to Dictionary
    // {id: 1, any: []}
    h.transaction(h.client_2, set_nested_dictionary);

    synchronize(h.server.get(), {h.client_2.get()});

    // Client 1 sets property 'any' from List to Dictionary
    // {id: 1, any: []}
    h.transaction(h.client_1, set_nested_dictionary);

    // Client 2 inserts one integer in dictionary at 'any'
    // {id: 1, any: {{"key": 42}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key", 42);
    });

    // Result: Both changes are accepted - both clients are setting the same type (idempotent),
    // and the insert is on the same type
    // {id: 1, any: {{"key": 42}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        CHECK_EQUAL(dict.size(), 1);
        CHECK_EQUAL(dict.get("key"), 42);
    });
}

TEST(Transform_Nested_CreateDictionaryVsDictionaryInsert)
{
    // Baseline: set 'any.A.0.B' to List
    // {id: 1, any: {{"A": [{{"B": []}}]}}}
    TransformTestHarness h(test_context, TransformTestHarness::SameTime, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert_collection("B", CollectionType::List);
    });

    auto set_nested_dictionary = [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
    };

    // Client 2 sets 'any.A.0.B' from List to Dictionary
    // {id: 1, any: {{"A": [{{"B": {}}}]}}}
    h.transaction(h.client_2, set_nested_dictionary);

    synchronize(h.server.get(), {h.client_2.get()});

    // Client 1 sets 'any.A.0.B' from List to Dictionary
    // {id: 1, any: {{"A": [{{"B": {}}}]}}}
    h.transaction(h.client_1, set_nested_dictionary);

    // Client 2 inserts one integer in dictionary at 'any.A.0.B'
    // {id: 1, any: {{"A": [{{"B": {{"key": 42}}}}]}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0, "B"});
        dict->insert("key", 42);
    });

    // Result: Both changes are accepted - both clients are setting the same type (idempotent),
    // and the insert is on the same type
    // {id: 1, any: {{"A": [{{"B": {{"key": 42}}}}]}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0, "B"});
        CHECK_EQUAL(dict->size(), 1);
        CHECK_EQUAL(dict->get("key"), 42);
    });
}

TEST(Transform_CreateDictionaryVsArrayInsert)
{
    // Baseline: set property 'any' to List
    // {id: 1, any: []}
    TransformTestHarness h(test_context, TransformTestHarness::SameTime, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
    });

    // Client 2 sets property 'any' from List to Dictionary
    // {id: 1, any: {}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    // Client 2 inserts one integer in list at 'any'
    // {id: 1, any: [42]}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.add(42);
    });

    // Result: Client 1 wins because its update is higher up in the path
    // {id: 1, any: {}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK(obj.get_dictionary(col_any).is_empty());
    });
}

TEST(Transform_Nested_CreateDictionaryVsArrayInsert)
{
    // Baseline: set 'any.A.0.B' to List
    // {id: 1, any: {{"A": [{{"B": []}}]}}}
    TransformTestHarness h(test_context, TransformTestHarness::SameTime, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert_collection("B", CollectionType::List);
    });

    // Client 1 sets 'any.A.0.B' from List to Dictionary
    // {id: 1, any: {{"A": [{{"B": {}}}]}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
    });

    // Client 2 inserts one integer in list at 'any.A.0.B'
    // {id: 1, any: {{"A": [{{"B": [42]}}]}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A", 0, "B"});
        list->add(42);
    });

    // Result: Client 1 wins because its update is higher up in the path
    // {id: 1, any: {{"A": [{{"B": {}}}]}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK(obj.get_dictionary_ptr({col_any, "A", 0, "B"})->is_empty());
    });
}

TEST(Transform_ArrayInsertVsUpdateString)
{
    // Baseline: set property 'any' to List and insert two integers in the list
    // {id: 1, any: [1, 2]}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(1);
        list.add(2);
    });

    // Client 1 inserts one integer in the list at 'any'
    // {id: 1, any: [1, 2, 3]}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.add(3);
    });

    // Client 2 sets property 'any' from List to a string value
    // {id: 1, any: "value"}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set(col_any, Mixed{"value"});
    });

    // Client 2 wins because its update is higher up in the path
    // {id: 1, any: "value"}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK_EQUAL(obj.get_any(col_any), "value");
    });
}

TEST(Transform_ClearArrayVsDictionaryInsert)
{
    // Baseline: set property 'any' to List
    // {id: 1, any: []}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
    });

    // Client 1 inserts two integers in the list at 'any', clears the list, and
    // inserts one integer in the list
    // {id: 1, any: [3]}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.add(1);
        list.add(2);
        list.clear();
        list.add(3);
    });

    // Client 2 sets property 'any' from List to Dictionary and inserts one integer in the dictionary (after Client
    // 1's changes)
    // {id: 1, any: {{"key1": 42}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 42);
    });

    // Result: Client 2 wins because Update comes after Clear
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        CHECK(dict.size() == 1);
        CHECK(dict.get("key1") == 42);
    });
}

// Test merging instructions at same level of nesting (both on Mixed properties and nested collections).

TEST(Transform_CreateArrayBeforeUpdateInt)
{
    // Baseline: property 'any' is not set to any type
    // {id: 1, any: null}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj, ColKey) {});

    // Client 1 sets property 'any' to List
    // {id: 1, any: []}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
    });

    // Client 2 sets property 'any' to an integer value (after Client 1's change)
    // {id: 1, any: 42}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_any(col_any, 42);
    });

    // Result: Client 2 wins because its change has a higher timestamp
    // {id: 1, any: 42}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK_EQUAL(obj.get_any(col_any), 42);
    });
}

TEST(Transform_CreateArrayAfterUpdateInt)
{
    // Baseline: property 'any' is not set to any type
    // {id: 1, any: null}
    TransformTestHarness h(test_context, TransformTestHarness::ClientTwoBeforeOne, [](Obj, ColKey) {});

    // Client 1 sets property 'any' to List
    // {id: 1, any: []}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
    });

    // Client 2 sets property 'any' to an integer value (before Client 1's change)
    // {id: 1, any: 42}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_any(col_any, 42);
    });

    // Result: Client 1 wins because its change has a higher timestamp
    // {id: 1, any: []}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK(obj.get_list<Mixed>(col_any).is_empty());
    });
}

TEST(Transform_Nested_CreateArrayBeforeUpdateInt)
{
    // Baseline: set 'any.A.0' as Dictionary and insert one string in the dictionary
    // {id: 1, any: {{"A": [{{"B": "some value"}}]}}}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert("B", "some value");
    });

    // Client 1 sets 'any.A.0.B' from string to List
    // {id: 1, any: {{"A": [{{"B": []}}]}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
    });

    // Client 2 sets 'any.A.0.B' from string to integer (after Client 1's change)
    // {id: 1, any: {{"A": [{{"B": 42}}]}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert("B", 42);
    });

    // Result: Client 2 wins because its change has a higher timestamp
    // {id: 1, any: {{"A": [{{"B": 42}}]}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        CHECK_EQUAL(dict->size(), 1);
        CHECK_EQUAL(dict->get("B"), 42);
    });
}

TEST(Transform_CreateDictionaryBeforeUpdateInt)
{
    // Baseline: property 'any' is not set to any type
    // {id: 1, any: null}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj, ColKey) {});

    // Client 1 sets property 'any' to Dictionary
    // {id: 1, any: {}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    // Client 2 sets property 'any' to an integer value (after Client 1's change)
    // {id: 1, any: 42}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_any(col_any, 42);
    });

    // Result: Client 2 wins because its change has a higher timestamp
    // {id: 1, any: 42}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK_EQUAL(obj.get_any(col_any), 42);
    });
}

TEST(Transform_CreateDictionaryAfterUpdateInt)
{
    // Baseline: property 'any' is not set to any type
    // {id: 1, any: null}
    TransformTestHarness h(test_context, TransformTestHarness::ClientTwoBeforeOne, [](Obj, ColKey) {});

    // Client 1 sets property 'any' to Dictionary
    // {id: 1, any: {}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    // Client 2 sets property 'any' to an integer value (before Client 1's change)
    // {id: 1, any: 42}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_any(col_any, 42);
    });

    // Result: Client 1 wins because its change has a higher timestamp
    // {id: 1, any: {}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK(obj.get_dictionary(col_any).is_empty());
    });
}

TEST(Transform_Nested_CreateDictionaryAfterUpdateInt)
{
    // Baseline: set 'any.A.0' as Dictionary and insert one string in the dictionary
    // {id: 1, any: {{"A": [{{"B": "some value"}}]}}}
    TransformTestHarness h(test_context, TransformTestHarness::ClientTwoBeforeOne, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert("B", "some value");
    });

    // Client 1 sets 'any.A.0.B' from string to Dictionary
    // {id: 1, any: {{"A": [{{"B": {}}}]}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
    });

    // Client 2 sets 'any.A.0.B' from string to integer (before Client 1's change)
    // {id: 1, any: {{"A": [{{"B": 42}}]}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert("B", 42);
    });

    // Result: Client 1 wins because its change has a higher timestamp
    // {id: 1, any: {{"A": [{{"B": {}}}]}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK(obj.get_dictionary_ptr({col_any, "A", 0, "B"})->is_empty());
    });
}

TEST(Transform_MergeArrays)
{
    // Baseline: property 'any' is not set to any type
    // {id: 1, any: null}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj, ColKey) {});

    // Client 1 sets property 'any' to List and inserts two strings in the list
    // {id: 1, any: ["a", "b"]}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, "a");
        list.insert(1, "b");
    });

    // Client 2 sets property 'any' to List and inserts two strings in the list
    // {id: 1, any: ["c", "d"]}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, "c");
        list.insert(1, "d");
    });

    // Result: Both changes are accepted and the lists are merged - both clients are setting the same type
    // (idempotent), and inserts are all in the same list
    // {id: 1, any: ["a", "b", "c", "d"]}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>(col_any);
        CHECK_EQUAL(list->size(), 4);
        CHECK_EQUAL(list->get(0), "a");
        CHECK_EQUAL(list->get(1), "b");
        CHECK_EQUAL(list->get(2), "c");
        CHECK_EQUAL(list->get(3), "d");
    });
}

TEST(Transform_Nested_MergeArrays)
{
    // Baseline: set 'any.A.0.B' to Dictionary
    // {id: 1, any: {{"A": [{{"B": {}}}]}}}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert_collection("B", CollectionType::Dictionary);
    });

    // Client 1 sets 'any.A.0.B' from Dictionary to List and inserts two strings in the list
    // {id: 1, any: {{"A": [{{"B": ["a", "b"]}}]}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
        auto list = dict->get_list("B");
        list->insert(0, "a");
        list->insert(1, "b");
    });

    // Client 2 sets 'any.A.0.B' from Dictionary to List and inserts two strings in the list
    // {id: 1, any: {{"A": [{{"B": ["c", "d"]}}]}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
        auto list = dict->get_list("B");
        list->insert(0, "c");
        list->insert(1, "d");
    });

    // Result: Both changes are accepted and the lists are merged - both clients are setting the same type
    // (idempotent), and inserts are all in the same list
    // {id: 1, any: {{"A": [{{"B": ["a", "b", "c", "d"]}}]}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A", 0, "B"});
        CHECK_EQUAL(list->size(), 4);
        CHECK_EQUAL(list->get(0), "a");
        CHECK_EQUAL(list->get(1), "b");
        CHECK_EQUAL(list->get(2), "c");
        CHECK_EQUAL(list->get(3), "d");
    });
}

TEST(Transform_Nested_MergeArrays_CorrectOrder)
{
    // Baseline: set 'any.A.0.B' to Dictionary
    // {id: 1, any: {{"A": [{{"B": {}}}]}}}
    TransformTestHarness h(test_context, TransformTestHarness::ClientTwoBeforeOne, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert_collection("B", CollectionType::Dictionary);
    });

    // Client 1 sets 'any.A.0.B' from Dictionary to List and inserts two strings in the list
    // {id: 1, any: {{"A": [{{"B": ["a", "b"]}}]}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
        auto list = dict->get_list("B");
        list->insert(0, "a");
        list->insert(1, "b");
    });

    // Client 2 sets 'any.A.0.B' from Dictionary to List and inserts two strings in the list (before Client 1's
    // changes)
    // {id: 1, any: {{"A": [{{"B": ["c", "d"]}}]}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
        auto list = dict->get_list("B");
        list->insert(0, "c");
        list->insert(1, "d");
    });

    // Result: Both changes are accepted and the lists are merged - both clients are setting the same type
    // (idempotent), and inserts are all in the same list (but Client 2's changes come first)
    // {id: 1, any: {{"A": [{{"B": ["c", "d", "a", "b"]}}]}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A", 0, "B"});
        CHECK_EQUAL(list->size(), 4);
        CHECK_EQUAL(list->get(0), "c");
        CHECK_EQUAL(list->get(1), "d");
        CHECK_EQUAL(list->get(2), "a");
        CHECK_EQUAL(list->get(3), "b");
    });
}

TEST(Transform_MergeDictionaries)
{
    // Baseline: property 'any' is not set to any type
    // {id: 1, any: null}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj, ColKey) {});

    // Client 1 sets property 'any' to Dictionary and inserts two strings in the dictionary
    // {id: 1, any: {{"key1": "a"}, {"key2": "b"}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto list = obj.get_dictionary(col_any);
        list.insert("key1", "a");
        list.insert("key2", "b");
    });

    // Client 1 sets property 'any' to Dictionary and inserts two strings in the dictionary
    // {id: 1, any: {{"key2": "y"}, {"key3": "z"}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto list = obj.get_dictionary(col_any);
        list.insert("key2", "y");
        list.insert("key3", "z");
    });

    // Result: Both changes are accepted and the dictionaries are merged - both clients are setting the same type
    // (idempotent), and inserts are all in the same dictionary. Client 2 overwrites the value of key "key2" because
    // its change has a higher timestamp.
    // {id: 1, any: {{"key1": "a"}, {"key2": "y"}, {"key3": "z"}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        CHECK_EQUAL(dict.size(), 3);
        CHECK_EQUAL(dict.get("key1"), "a");
        CHECK_EQUAL(dict.get("key2"), "y");
        CHECK_EQUAL(dict.get("key3"), "z");
    });
}

TEST(Transform_Nested_MergeDictionaries)
{
    // Baseline: set 'any.A.0.B' to List
    // {id: 1, any: {{"A": [{{"B": []}}]}}}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert_collection("B", CollectionType::List);
    });

    // Client 1 sets 'any.A.0.B' from List to Dictionary and inserts two strings in the dictionary
    // {id: 1, any: {{"A": [{{"B": {{"key1": "a"}, {"key2": "b"}}}}]}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
        auto dict2 = dict->get_dictionary("B");
        dict2->insert("key1", "a");
        dict2->insert("key2", "b");
    });

    // Client 1 sets 'any.A.0.B' from List to Dictionary and inserts two strings in the dictionary
    // {id: 1, any: {{"A": [{{"B": {{"key2": "y"}, {"key3": "z"}}}}]}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
        auto dict2 = dict->get_dictionary("B");
        dict2->insert("key2", "y");
        dict2->insert("key3", "z");
    });

    // Result: Both changes are accepted and the dictionaries are merged - both clients are setting the same type
    // (idempotent), and inserts are all in the same dictionary. Client 2 overwrites the value of key "key2" because
    // its change has a higher timestamp.
    // {id: 1, any: {{"A": [{{"B": {{"key1": "a"}, {"key2": "y"}, {"key3": "z"}}}}]}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0, "B"});
        CHECK_EQUAL(dict->size(), 3);
        CHECK_EQUAL(dict->get("key1"), "a");
        CHECK_EQUAL(dict->get("key2"), "y");
        CHECK_EQUAL(dict->get("key3"), "z");
    });
}

TEST(Transform_CreateArrayAfterCreateDictionary)
{
    // Baseline: property 'any' is not set to any type
    // {id: 1, any: null}
    TransformTestHarness h(test_context, TransformTestHarness::ClientTwoBeforeOne, [](Obj, ColKey) {});

    // Client 1 sets property 'any' to List and inserts two strings in the list
    // {id: 1, any: ["a", "b"]}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, "a");
        list.insert(1, "b");
    });

    // Client 2 sets property 'any' to Dictionary and inserts two strings in the dictionary (before Client 1's change)
    // {id: 1, any: {{"key1": "a"}, {"key2": "b"}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", "a");
        dict.insert("key2", "b");
    });

    // Result: Client 1 wins because its change has a higher timestamp
    // {id: 1, any: ["a", "b"]}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>(col_any);
        CHECK_EQUAL(list->size(), 2);
        CHECK_EQUAL(list->get(0), "a");
        CHECK_EQUAL(list->get(1), "b");
    });
}

TEST(Transform_CreateArrayBeforeCreateDictionary)
{
    // Baseline: property 'any' is not set to any type
    // {id: 1, any: null}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj, ColKey) {});

    // Client 1 sets property 'any' to List and inserts two strings in the list
    // {id: 1, any: ["a", "b"]}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, "a");
        list.insert(1, "b");
    });

    // Client 2 sets property 'any' to Dictionary and inserts two strings in the dictionary (after Client 1's change)
    // {id: 1, any: {{"key1": "a"}, {"key2": "b"}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", "a");
        dict.insert("key2", "b");
    });

    // Result: Client 2 wins because its change has a higher timestamp
    // {id: 1, any: {{"key1": "a"}, {"key2": "b"}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr(col_any);
        CHECK_EQUAL(dict->size(), 2);
        CHECK_EQUAL(dict->get("key1"), "a");
        CHECK_EQUAL(dict->get("key2"), "b");
    });
}

TEST(Transform_Nested_CreateArrayAfterCreateDictionary)
{
    // Baseline: set 'any.A.0' to Dictionary
    // {id: 1, any: {{"A": [{}]}}}
    TransformTestHarness h(test_context, TransformTestHarness::ClientTwoBeforeOne, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
    });

    // Client 1 inserts a List in dictionary at 'any.A.0' and then inserts two strings in the list
    // {id: 1, any: {{"A": [{{"B": ["a", "b"]}}]}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
        auto list = dict->get_list("B");
        list->insert(0, "a");
        list->insert(1, "b");
    });

    // Client 2 inserts a Dictionary in dictionary at 'any.A.0' and then inserts two strings in the dictionary (before
    // Client 1's changes)
    // {id: 1, any: {{"A": [{{"B": {{"key1": "a", {"key2": "b"}}}}}]}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
        auto dict2 = dict->get_dictionary("B");
        dict2->insert("key1", "a");
        dict2->insert("key2", "b");
    });

    // Result: Client 1 wins because its change has a higher timestamp
    // {id: 1, any: {{"A": [{{"B": ["a", "b"]}}]}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A", 0, "B"});
        CHECK_EQUAL(list->size(), 2);
        CHECK_EQUAL(list->get(0), "a");
        CHECK_EQUAL(list->get(1), "b");
    });
}

TEST(Transform_Nested_ClearArrayVsUpdateString)
{
    // Baseline: set 'any.A' to List and insert two integers in the list
    // {id: 1, any: {{"A": [1, 2]}}}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->add(1);
        list->add(2);
    });

    // Client 1 clears the list at 'any.A' and inserts one integer in the list
    // {id: 1, any: {{"A": [3]}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A"});
        list->clear();
        list->add(3);
    });

    // Client 2 sets 'any.A' from List to a string value (after Client 1's changes)
    // {id: 1, any: {{"A", "some value"}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert("A", "some value");
    });

    // Result: Client 2 wins because Update comes after Clear
    // {id: 1, any: {{"A", "some value"}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr(col_any);
        CHECK_EQUAL(dict->size(), 1);
        CHECK_EQUAL(dict->get("A"), "some value");
    });
}

TEST(Transform_ClearArrayVsCreateArray)
{
    // Baseline: set property 'any' to Dictionary and insert two integers in the dictionary
    // {id: 1, any: {{"key1": 1}, {"key2": 2}}}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 1);
        dict.insert("key2", 2);
    });

    // Client 1 sets property 'any' from Dictionary to List, inserts one integer in the list, clears the list, and
    // inserts one integer in the list
    // {id: 1, any: [2]}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(1);
        list.clear();
        list.add(2);
    });

    // Client 2 sets property 'any' from Dictionary to List and inserts one integer in the list
    // {id: 1, any: [4]}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(4);
    });

    // Result: Client 1 wins - Clear wins against the insertion from Client 2
    // {id: 1, any: [2]}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>(col_any);
        CHECK_EQUAL(list->size(), 1);
        CHECK_EQUAL(list->get(0), 2);
    });
}

TEST(Transform_ClearArrayInsideArrayVsCreateArray)
{
    // Baseline: set 'any.0' to Dictionary and insert two integers in the dictionary
    // {id: 1, any: [{{"key1": 1}, {"key2": 2}}]}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert_collection(0, CollectionType::Dictionary);
        auto dict = list.get_dictionary(0);
        dict->insert("key1", 1);
        dict->insert("key2", 2);
    });

    // Client 1 sets 'any.0' from Dictionary to List, inserts one integer in the list, clears the list, and inserts
    // one integer in the list
    // {id: 1, any: [2]}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::List);
        auto list2 = list.get_list(0);
        list2->add(1);
        list2->clear();
        list2->add(2);
    });

    // Client 2 sets 'any.0' from Dictionary to List and inserts one integer in the list
    // {id: 1, any: [4]}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::List);
        auto list2 = list.get_list(0);
        list2->add(4);
    });

    // Result: Client 1 wins - Clear wins against the insertion from Client 2
    // {id: 1, any: [2]}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, 0});
        CHECK_EQUAL(list->size(), 1);
        CHECK_EQUAL(list->get(0), 2);
    });
}

TEST(Transform_ClearArrayInsideDictionaryVsCreateArray)
{
    // Baseline: set 'any.A' to Dictionary and insert two integers in the dictionary
    // {id: 1, any: {{"A": {{{"key1": 1}, {"key2": 2}}}}}}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::Dictionary);
        auto dict2 = dict.get_dictionary("A");
        dict2->insert("key1", 1);
        dict2->insert("key2", 2);
    });

    // Client 1 sets 'any.A' from Dictionary to List, inserts one integer in the list, clears the list, and inserts
    // one integer in the list
    // {id: 1, any: {{"A": [2]}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::List);
        auto list = dict.get_list("A");
        list->add(1);
        list->clear();
        list->add(2);
    });

    // Client 2 sets 'any.A' from Dictionary to List and inserts one integer in the list
    // {id: 1, any: {{"A": [4]}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::List);
        auto list = dict.get_list("A");
        list->add(4);
    });

    // Result: Client 1 wins - Clear wins against the insertion from Client 2
    // {id: 1, any: {{"A": [2]}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A"});
        CHECK_EQUAL(list->size(), 1);
        CHECK_EQUAL(list->get(0), 2);
    });
}

TEST(Transform_ClearArrayVsCreateDictionary)
{
    // Baseline: set property 'any' to List
    // {id: 1, any: []}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
    });

    // Client 1 inserts two integers in list at 'any', clears the list, and inserts one integer in the list
    // {id: 1, any: [3]}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.add(1);
        list.add(2);
        list.clear();
        list.add(3);
    });

    // Client 2 sets property 'any' from List to Dictionary and inserts one integer in the dictionary
    // {id: 1, any: {{"key1": 42}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 42);
    });

    // Result: Client 2 wins because Update comes after Clear
    // {id: 1, any: {"key1": 42}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        CHECK(dict.size() == 1);
        CHECK(dict.get("key1") == 42);
    });
}

TEST(Transform_ClearArrayInsideArrayVsCreateDictionary)
{
    // Baseline: set property 'any' to List and insert one integer in the list
    // {id: 1, any: [42]}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, 42);
    });

    // Client 1 sets 'any.0' from integer to List, inserts one integer in the list, clears the list, and inserts one
    // integer in the list
    // {id: 1, any: [[2]]}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::List);
        auto list2 = list.get_list(0);
        list2->add(1);
        list2->clear();
        list2->add(2);
    });

    // Client 2 sets 'any.0' from integer to Dictionary and inserts one string in the dictionary (after Client 1's
    // changes)
    // {id: 1, any: [{{"key1": "some value"}}]}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::Dictionary);
        auto dict = list.get_dictionary(0);
        dict->insert("key1", "some value");
    });

    // Result: Client 2 wins because Update comes after Clear
    // {id: 1, any: [{"key1": "some value"}]}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, 0});
        CHECK(dict->size() == 1);
        CHECK(dict->get("key1") == "some value");
    });
}

TEST(Transform_ClearArrayInsideDictionaryVsCreateDictionary)
{
    // Baseline: set property 'any' to Dictionary and insert one string in the dictionary
    // {id: 1, any: {{"A": "some value"}}}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("A", "some value");
    });

    // Client 1 sets 'any.A' from string to List, inserts one integer in the list, clears the list, and inserts one
    // integer in the list
    // {id: 1, any: {{"A": [2]}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::List);
        auto list = dict.get_list("A");
        list->add(1);
        list->clear();
        list->add(2);
    });

    // Client 2 sets 'any.A' from string to Dictionary and inserts one string in the dictionary (after Client 1's
    // changes)
    // {id: 1, any: {{"A": {{"key1": "some value"}}}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::Dictionary);
        auto dict2 = dict.get_dictionary("A");
        dict2->insert("key1", "some other value");
    });

    // Result: Client 2 wins because Update comes after Clear
    // {id: 1, any: {{"A": {"key1": "some other value"}}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A"});
        CHECK(dict->size() == 1);
        CHECK(dict->get("key1") == "some other value");
    });
}

TEST(Transform_ClearDictionaryVsCreateArray)
{
    // Baseline: set property 'any' to Dictionary and insert two integers in the dictionary
    // {id: 1, any: {{"key1": 1}, {"key2": 2}}}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 1);
        dict.insert("key2", 2);
    });

    // Client 1 insert one integer in dictionary at 'any', clears the dictionary, and inserts one integer in the
    // dictionary
    // {id: 1, any: {{"key4": 4}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key3", 3);
        dict.clear();
        dict.insert("key4", 4);
    });

    // Client 2 sets property 'any' from Dictionary to List and inserts one integer in the list
    // {id: 1, any: [1]}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(1);
    });

    // Result: Client 2 wins because Update comes after Clear
    // {id: 1, any: [1]}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>(col_any);
        CHECK(list->size() == 1);
        CHECK(list->get(0) == 1);
    });
}

TEST(Transform_ClearDictionaryInsideArrayVsCreateArray)
{
    // Baseline: set 'any.0' to Dictionary and insert two integers in the dictionary
    // {id: 1, any: [{{"key1": 1}, {"key2": 2}}]}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert_collection(0, CollectionType::Dictionary);
        auto dict = list.get_dictionary(0);
        dict->insert("key1", 1);
        dict->insert("key2", 2);
    });

    // Client 1 inserts one integer in dictionary at 'any.0', clears it, and insert one integer in the dictionary
    // {id: 1, any: [{{"key4": 4}}]}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, 0});
        dict->insert("key3", 3);
        dict->clear();
        dict->insert("key4", 4);
    });

    // Client 2 sets 'any.0' from Dictionary to List and inserts one integer in the list
    // {id: 1, any: [[4]]}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::List);
        auto list2 = list.get_list(0);
        list2->add(4);
    });

    // Result: Client 2 wins because Update comes after Clear
    // {id: 1, any: [[4]]}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, 0});
        CHECK(list->size() == 1);
        CHECK(list->get(0) == 4);
    });
}

TEST(Transform_ClearDictionaryInsideDictionaryVsCreateArray)
{
    // Baseline: set 'any.A' to Dictionary and insert two integers in the dictionary
    // {id: 1, any: {{"A": {{"key1": 1}, {"key2": 2}}}}}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::Dictionary);
        auto dict2 = dict.get_dictionary("A");
        dict2->insert("key1", 1);
        dict2->insert("key2", 2);
    });

    // Client 1 inserts one integer in dictionary at 'any.A', clear the dictionary, and inserts one integer in the
    // dictionary
    // {id: 1, any: {{"A": {{"key4": 4}}}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A"});
        dict->insert("key3", 3);
        dict->clear();
        dict->insert("key4", 4);
    });

    // Client 2 sets 'any.A' from Dictionary to List and inserts one integer in the list
    // {id: 1, any: {{"A": [4]}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::List);
        auto list = dict.get_list("A");
        list->add(4);
    });

    // Result: Client 2 wins because Update comes after Clear
    // {id: 1, any: {{"A": [4]}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A"});
        CHECK(list->size() == 1);
        CHECK(list->get(0) == 4);
    });
}

TEST(Transform_ClearDictionaryVsCreateDictionary)
{
    // Baseline: set property 'any' to List
    // {id: 1, any: []}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
    });

    // Client 1 sets property 'any' from List to Dictionary, inserts one integer in the dictionary, clears the
    // dictionary, and inserts one integer in the dictionary
    // {id: 1, any: {{"key2": 2}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 1);
        dict.clear();
        dict.insert("key2", 2);
    });

    // Client 2 sets property 'any' from List to Dictionary and inserts one integer in the dictionary
    // {id: 1, any: {{"key3": 3}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key3", 3);
    });

    // Result: Client 1 wins - Clear wins against the insertion from Client 2
    // {id: 1, any: {{"key2": 2}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        CHECK_EQUAL(dict.size(), 1);
        CHECK_EQUAL(dict.get("key2"), 2);
    });
}

TEST(Transform_ClearDictionaryInsideArrayVsCreateDictionary)
{
    // Baseline: set property 'any' to List and insert one integer in the list
    // {id: 1, any: [42]}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, 42);
    });

    // Client 1 sets 'any.0' from integer to Dictionary, inserts one integer in the dictionary, clears the
    // dictionary, and inserts one integer in the dictionary
    // {id: 1, any: [{{"key2": 2}}]}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.insert_collection(0, CollectionType::Dictionary);
        auto dict = list.get_dictionary(0);
        dict->insert("key1", 1);
        dict->clear();
        dict->insert("key2", 2);
    });

    // Client 2 sets 'any.0' from integer to Dictionary and inserts one integer in the dictionary
    // {id: 1, any: [{{"key3": 3}}]}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::Dictionary);
        auto dict = list.get_dictionary(0);
        dict->insert("key3", 3);
    });

    // Result: Client 1 wins - Clear wins against the insertion from Client 2
    // {id: 1, any: [{{"key2": 2}}]}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, 0});
        CHECK_EQUAL(dict->size(), 1);
        CHECK_EQUAL(dict->get("key2"), 2);
    });
}

TEST(Transform_ClearDictionaryInsideDictionaryVsCreateDictionary)
{
    // Baseline: set property 'any' to Dictionary and insert one string in the dictionary
    // {id: 1, any: {{"A": "some value"}}}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("A", "some value");
    });

    // Client 1 sets 'any.A' from string to Dictionary, inserts one integer in the dictionary, clears the dictionary,
    // and inserts one integer in the dictionary
    // {id: 1, any: {{"A": {{"key2": 2}}}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::Dictionary);
        auto dict2 = dict.get_dictionary("A");
        dict2->insert("key1", 1);
        dict2->clear();
        dict2->insert("key2", 2);
    });

    // Client 2 sets 'any.A' from string to Dictionary and inserts one integer in the dictionary
    // {id: 1, any: {{"A": {{"key3": 3}}}}}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::Dictionary);
        auto dict2 = dict.get_dictionary("A");
        dict2->insert("key3", 3);
    });

    // Result: Client 1 wins - Clear wins against the insertion from Client 2
    // {id: 1, any: {{"A": {{"key2": 2}}}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A"});
        CHECK_EQUAL(dict->size(), 1);
        CHECK_EQUAL(dict->get("key2"), 2);
    });
}

TEST(Transform_UpdateClearVsUpdateClear)
{
    std::vector<int> timestamps{1, 2, 3, 4};

    do {
        auto t1 = timestamps[0];
        auto t2 = timestamps[1];
        auto t3 = timestamps[2];
        auto t4 = timestamps[3];

        if (t1 > t2 || t3 > t4)
            continue;

        // Baseline: property 'any' is not set to any type
        // {id: 1, any: null}
        TransformTestHarness h(test_context, {}, [](Obj, ColKey) {});

        // Client 1 sets property 'any' to List and inserts one integer in the list
        // {id: 1, any: [1]}
        h.transaction(
            h.client_1,
            [](Obj obj, ColKey col_any) {
                obj.set_collection(col_any, CollectionType::List);
                auto l = obj.get_list<Mixed>(col_any);
                l.add(1);
            },
            t1);

        // Client 1 clears the list at property 'any'
        // {id: 1, any: []}
        h.transaction(
            h.client_1,
            [](Obj obj, ColKey col_any) {
                auto l = obj.get_list<Mixed>(col_any);
                l.clear();
            },
            t2);

        // Client 2 sets property 'any' to List and inserts one integer in the list
        // {id: 1, any: [2]}
        h.transaction(
            h.client_2,
            [](Obj obj, ColKey col_any) {
                obj.set_collection(col_any, CollectionType::List);
                auto l = obj.get_list<Mixed>(col_any);
                l.add(2);
            },
            t3);

        // Client 2 clears the list at property 'any'
        // {id: 1, any: []}
        h.transaction(
            h.client_2,
            [](Obj obj, ColKey col_any) {
                auto l = obj.get_list<Mixed>(col_any);
                l.clear();
            },
            t4);

        // Check clients converge.
        h.check_merge_result([&](Obj, ColKey) {});

    } while (std::next_permutation(timestamps.begin(), timestamps.end()));
}

TEST(Transform_UpdateClearVsUpdateClear_DifferentTypes)
{
    std::vector<int> timestamps{1, 2, 3, 4};

    do {
        auto t1 = timestamps[0];
        auto t2 = timestamps[1];
        auto t3 = timestamps[2];
        auto t4 = timestamps[3];

        if (t1 > t2 || t3 > t4)
            continue;

        // Baseline: property 'any' is not set to any type
        // {id: 1, any: null}
        TransformTestHarness h(test_context, {}, [](Obj, ColKey) {});

        // Client 1 sets property 'any' to List and inserts one integer in the list
        // {id: 1, any: [1]}
        h.transaction(
            h.client_1,
            [](Obj obj, ColKey col_any) {
                obj.set_collection(col_any, CollectionType::List);
                auto l = obj.get_list<Mixed>(col_any);
                l.add(42);
            },
            t1);

        // Client 1 clears the list at property 'any'
        // {id: 1, any: []}
        h.transaction(
            h.client_1,
            [](Obj obj, ColKey col_any) {
                auto l = obj.get_list<Mixed>(col_any);
                l.clear();
            },
            t2);

        // Client 2 sets property 'any' to Dictionary and inserts one integer in the dictionary
        // {id: 1, any: {{"key": 42}}}
        h.transaction(
            h.client_2,
            [](Obj obj, ColKey col_any) {
                obj.set_collection(col_any, CollectionType::Dictionary);
                auto d = obj.get_dictionary(col_any);
                d.insert("key", 42);
            },
            t3);

        // Client 2 clears the dictionary at property 'any'
        // {id: 1, any: {}}
        h.transaction(
            h.client_2,
            [](Obj obj, ColKey col_any) {
                auto d = obj.get_dictionary(col_any);
                d.clear();
            },
            t4);

        // Check clients converge.
        h.check_merge_result([&](Obj, ColKey) {});

    } while (std::next_permutation(timestamps.begin(), timestamps.end()));
}

TEST(Transform_UpdateClearVsAddInteger)
{
    // Baseline: set property 'any' to integer value
    // {id: 1, any: 42}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_any(col_any, 42);
    });

    // Client 1 sets property 'any' from integer to Dictionary, inserts one integer in the dictionary and clears the
    // dictionary, inserts one more integer in the dictionary
    // {id: 1, any: {{"key2": 2}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 1);
        dict.clear();
        dict.insert("key2", 2);
    });

    // Client 2 adds integer to property 'any'
    // {id: 1, any: 43}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.add_int(col_any, 1);
    });

    // Result: Client 1 wins - Update wins against AddInteger from Client 2
    // {id: 1, any: {{"key2": 2}}}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        CHECK_EQUAL(dict.size(), 1);
        CHECK_EQUAL(dict.get("key2"), 2);
    });
}

TEST(Transform_ClearVsUpdateAddInteger)
{
    // Baseline: set property 'any' to Dictionary and insert one integer in the dictionary
    // {id: 1, any: {{"key1": 1}}}
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 1);
    });

    // Client 1 clears the dictionary at property 'any' and adds one integer in the dictionary
    // {id: 1, any: {{"key2": 2}}}
    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.clear();
        dict.insert("key2", 2);
    });

    // Client 2 sets property 'any' from Dictionary to integer value and adds another integer to it
    // {id: 1, any: 43}
    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_any(col_any, 42);
        obj.add_int(col_any, 1);
    });

    // Result: Client 2 wins - Update wins against Clear from Client 2
    // {id: 1, any: 43}
    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK_EQUAL(obj.get<Mixed>(col_any), 43);
    });
}

TEST(Transform_UpdateClearVsUpdateAddInteger)
{
    std::vector<int> timestamps{1, 2, 3, 4};

    do {
        auto t1 = timestamps[0];
        auto t2 = timestamps[1];
        auto t3 = timestamps[2];
        auto t4 = timestamps[3];

        if (t1 > t2 || t3 > t4)
            continue;

        // Baseline: set property 'any' to List
        // {id: 1, any: []}
        TransformTestHarness h(test_context, {}, [](Obj obj, ColKey col_any) {
            obj.set_collection(col_any, CollectionType::List);
        });

        // Client 1 sets property 'any' from List to Dictionary and inserts one integer in the dictionary
        // {id: 1, any: {{"key1": 1}}}
        h.transaction(
            h.client_1,
            [](Obj obj, ColKey col_any) {
                obj.set_collection(col_any, CollectionType::Dictionary);
                auto dict = obj.get_dictionary(col_any);
                dict.insert("key1", 1);
            },
            t1);

        // Client 1 clears the dictionary at property 'any' and inserts one integer in the dictionary
        // {id: 1, any: {{"key2": 2}}}
        h.transaction(
            h.client_1,
            [](Obj obj, ColKey col_any) {
                auto dict = obj.get_dictionary(col_any);
                dict.clear();
                dict.insert("key2", 2);
            },
            t2);

        // Client 2 sets property 'any' from Dictionary to integer value
        // {id: 1, any: 42}
        h.transaction(
            h.client_2,
            [](Obj obj, ColKey col_any) {
                obj.set_any(col_any, 42);
            },
            t3);

        // Client 2 adds integer value to property 'any'
        // {id: 1, any: 43}
        h.transaction(
            h.client_2,
            [](Obj obj, ColKey col_any) {
                obj.add_int(col_any, 1);
            },
            t4);

        // Check clients converge.
        h.check_merge_result([&](Obj, ColKey) {});

    } while (std::next_permutation(timestamps.begin(), timestamps.end()));
}
