#include "peer.hpp"
#include "util/dump_changesets.hpp"

using namespace realm;
using namespace realm::test_util;

struct TransformTestHarness {
    enum ConflictOrdering { ClientOneBeforeTwo, ClientTwoBeforeOne, SameTime };

    template <typename Func>
    explicit TransformTestHarness(unit_test::TestContext& test_context, ConflictOrdering ordering,
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

        switch (ordering) {
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
    void transaction(const std::unique_ptr<Peer>& p, Func&& func)
    {
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
    TransformTestHarness h(test_context, TransformTestHarness::SameTime, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    h.client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
    });

    h.client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(42);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>(col_any);
        CHECK_EQUAL(list->size(), 1);
        CHECK_EQUAL(list->get(0), 42);
    });
}

TEST(Transform_Nested_CreateArrayVsArrayInsert)
{
    TransformTestHarness h(test_context, TransformTestHarness::SameTime, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::List);
        auto list2 = list->get_list(0);
        list2->insert(0, 42);
    });

    h.transaction(h.client_2, [&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A", 0});
        list->set_collection(0, CollectionType::List);
    });

    synchronize(h.server.get(), {h.client_2.get()});

    h.transaction(h.client_1, [&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A", 0});
        list->set_collection(0, CollectionType::List);
    });

    h.transaction(h.client_2, [&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A", 0, 0});
        list->add(42);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK_EQUAL(obj.get_list_ptr<Mixed>({col_any, "A", 0, 0})->get(0), 42);
    });
}

TEST(Transform_CreateArrayVsDictionaryInsert)
{
    TransformTestHarness h(test_context, TransformTestHarness::SameTime, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    h.transaction(h.client_1, [&](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
    });

    h.transaction(h.client_2, [&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert("key", 42);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK(obj.get_list_ptr<Mixed>(col_any)->is_empty());
    });
}

TEST(Transform_Nested_CreateArrayVsDictionaryInsert)
{
    TransformTestHarness h(test_context, TransformTestHarness::SameTime, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert_collection("B", CollectionType::Dictionary);
    });

    h.transaction(h.client_1, [&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
    });

    h.transaction(h.client_2, [&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0, "B"});
        dict->insert("key", 42);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK(obj.get_list_ptr<Mixed>({col_any, "A", 0, "B"})->is_empty());
    });
}

TEST(Transform_CreateDictionaryVsDictionaryInsert)
{
    TransformTestHarness h(test_context, TransformTestHarness::SameTime, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
    });

    auto set_nested_dictionary = [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
    };

    h.transaction(h.client_2, set_nested_dictionary);

    synchronize(h.server.get(), {h.client_2.get()});

    h.transaction(h.client_1, set_nested_dictionary);

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key", 42);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK_EQUAL(obj.get_dictionary(col_any).get("key"), 42);
    });
}

TEST(Transform_Nested_CreateDictionaryVsDictionaryInsert)
{
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

    h.transaction(h.client_2, set_nested_dictionary);

    synchronize(h.server.get(), {h.client_2.get()});

    h.transaction(h.client_1, set_nested_dictionary);

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0, "B"});
        dict->insert("key", 42);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK_EQUAL(obj.get_dictionary_ptr({col_any, "A", 0, "B"})->get("key"), 42);
    });
}

TEST(Transform_CreateDictionaryVsArrayInsert)
{
    TransformTestHarness h(test_context, TransformTestHarness::SameTime, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.add(42);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK(obj.get_dictionary(col_any).is_empty());
    });
}

TEST(Transform_Nested_CreateDictionaryVsArrayInsert)
{
    TransformTestHarness h(test_context, TransformTestHarness::SameTime, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert_collection("B", CollectionType::List);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A", 0, "B"});
        list->add(42);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK(obj.get_dictionary_ptr({col_any, "A", 0, "B"})->is_empty());
    });
}

TEST(Transform_ArrayInsertVsUpdateString)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(1);
        list.add(2);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.add(3);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set(col_any, Mixed{"value"});
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK_EQUAL(obj.get_any(col_any), "value");
    });
}

TEST(Transform_ClearArrayVsDictionaryInsert)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(1);
        list.add(2);
        list.clear();
        list.add(3);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 42);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>(col_any);
        CHECK_EQUAL(list->size(), 1);
        CHECK_EQUAL(list->get(0), 3);
    });
}

// Test merging instructions at same level of nesting (both on Mixed properties and nested collections).

TEST(Transform_CreateArrayBeforeUpdateInt)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj, ColKey) {});

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_any(col_any, 42);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK_EQUAL(obj.get_any(col_any), 42);
    });
}

TEST(Transform_CreateArrayAfterUpdateInt)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientTwoBeforeOne, [](Obj, ColKey) {});

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_any(col_any, 42);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK(obj.get_list<Mixed>(col_any).is_empty());
    });
}

TEST(Transform_Nested_CreateArrayBeforeUpdateInt)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert("B", "some value");
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert("B", 42);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK_EQUAL(obj.get_dictionary_ptr({col_any, "A", 0})->get("B"), 42);
    });
}

TEST(Transform_CreateDictionaryBeforeUpdateInt)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj, ColKey) {});

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_any(col_any, 42);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK_EQUAL(obj.get_any(col_any), 42);
    });
}

TEST(Transform_CreateDictionaryAfterUpdateInt)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientTwoBeforeOne, [](Obj, ColKey) {});

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_any(col_any, 42);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK(obj.get_dictionary(col_any).is_empty());
    });
}

TEST(Transform_Nested_CreateDictionaryAfterUpdateInt)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientTwoBeforeOne, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert("B", "some value");
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert("B", 42);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        CHECK(obj.get_dictionary_ptr({col_any, "A", 0, "B"})->is_empty());
    });
}

TEST(Transform_MergeArrays)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj, ColKey) {});

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, "a");
        list.insert(1, "b");
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, "c");
        list.insert(1, "d");
    });

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
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert_collection("B", CollectionType::Dictionary);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
        auto list = dict->get_list("B");
        list->insert(0, "a");
        list->insert(1, "b");
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
        auto list = dict->get_list("B");
        list->insert(0, "c");
        list->insert(1, "d");
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A", 0, "B"});
        CHECK_EQUAL(list->size(), 4);
        CHECK_EQUAL(list->get(0), "a");
        CHECK_EQUAL(list->get(1), "b");
        CHECK_EQUAL(list->get(2), "c");
        CHECK_EQUAL(list->get(3), "d");
    });
}

TEST(Transform_MergeDictionaries)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj, ColKey) {});

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto list = obj.get_dictionary(col_any);
        list.insert("key1", "a");
        list.insert("key2", "b");
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto list = obj.get_dictionary(col_any);
        list.insert("key2", "y");
        list.insert("key3", "z");
    });

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
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert_collection("B", CollectionType::List);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
        auto dict2 = dict->get_dictionary("B");
        dict2->insert("key1", "a");
        dict2->insert("key2", "b");
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
        auto dict2 = dict->get_dictionary("B");
        dict2->insert("key2", "y");
        dict2->insert("key3", "z");
    });

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
    TransformTestHarness h(test_context, TransformTestHarness::ClientTwoBeforeOne, [](Obj, ColKey) {});

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, "a");
        list.insert(1, "b");
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", "a");
        dict.insert("key2", "b");
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>(col_any);
        CHECK_EQUAL(list->size(), 2);
        CHECK_EQUAL(list->get(0), "a");
        CHECK_EQUAL(list->get(1), "b");
    });
}

TEST(Transform_CreateArrayBeforeCreateDictionary)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj, ColKey) {});

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, "a");
        list.insert(1, "b");
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", "a");
        dict.insert("key2", "b");
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr(col_any);
        CHECK_EQUAL(dict->size(), 2);
        CHECK_EQUAL(dict->get("key1"), "a");
        CHECK_EQUAL(dict->get("key2"), "b");
    });
}

TEST(Transform_Nested_CreateArrayAfterCreateDictionary)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientTwoBeforeOne, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
        auto list = dict->get_list("B");
        list->insert(0, "a");
        list->insert(1, "b");
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
        auto dict2 = dict->get_dictionary("B");
        dict2->insert("key1", "a");
        dict2->insert("key2", "b");
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A", 0, "B"});
        CHECK_EQUAL(list->size(), 2);
        CHECK_EQUAL(list->get(0), "a");
        CHECK_EQUAL(list->get(1), "b");
    });
}

TEST(Transform_Nested_ClearArrayVsUpdateString)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->add(1);
        list->add(2);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A"});
        list->clear();
        list->add(3);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert("A", "some value");
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr(col_any);
        CHECK_EQUAL(dict->size(), 1);
        CHECK_EQUAL(dict->get("A"), "some value");
    });
}

TEST(Transform_ClearArrayVsCreateArray)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 1);
        dict.insert("key2", 2);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(1);
        list.clear();
        list.add(2);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(4);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>(col_any);
        CHECK_EQUAL(list->size(), 1);
        CHECK_EQUAL(list->get(0), 2);
    });
}

TEST(Transform_ClearArrayInsideArrayVsCreateArray)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert_collection(0, CollectionType::Dictionary);
        auto dict = list.get_dictionary(0);
        dict->insert("key1", 1);
        dict->insert("key2", 2);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::List);
        auto list2 = list.get_list(0);
        list2->add(1);
        list2->clear();
        list2->add(2);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::List);
        auto list2 = list.get_list(0);
        list2->add(4);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, 0});
        CHECK_EQUAL(list->size(), 1);
        CHECK_EQUAL(list->get(0), 2);
    });
}

TEST(Transform_ClearArrayInsideDictionaryVsCreateArray)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::Dictionary);
        auto dict2 = dict.get_dictionary("A");
        dict2->insert("key1", 1);
        dict2->insert("key2", 2);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::List);
        auto list = dict.get_list("A");
        list->add(1);
        list->clear();
        list->add(2);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::List);
        auto list = dict.get_list("A");
        list->add(4);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A"});
        CHECK_EQUAL(list->size(), 1);
        CHECK_EQUAL(list->get(0), 2);
    });
}

TEST(Transform_ClearArrayVsCreateDictionary)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.add(1);
        list.add(2);
        list.clear();
        list.add(3);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 42);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        CHECK(dict.is_empty());
    });
}

TEST(Transform_ClearArrayInsideArrayVsCreateDictionary)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, 42);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::List);
        auto list2 = list.get_list(0);
        list2->add(1);
        list2->clear();
        list2->add(2);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::Dictionary);
        auto dict = list.get_dictionary(0);
        dict->insert("key1", "some value");
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, 0});
        CHECK(dict->is_empty());
    });
}

TEST(Transform_ClearArrayInsideDictionaryVsCreateDictionary)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("A", "some value");
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::List);
        auto list = dict.get_list("A");
        list->add(1);
        list->clear();
        list->add(2);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::Dictionary);
        auto dict2 = dict.get_dictionary("A");
        dict2->insert("key1", "some other value");
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A"});
        CHECK(dict->is_empty());
    });
}

TEST(Transform_ClearDictionaryVsCreateArray)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 1);
        dict.insert("key2", 2);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key3", 3);
        dict.clear();
        dict.insert("key4", 4);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(1);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>(col_any);
        CHECK(list->is_empty());
    });
}

TEST(Transform_ClearDictionaryInsideArrayVsCreateArray)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert_collection(0, CollectionType::Dictionary);
        auto dict = list.get_dictionary(0);
        dict->insert("key1", 1);
        dict->insert("key2", 2);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, 0});
        dict->insert("key3", 3);
        dict->clear();
        dict->insert("key4", 4);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::List);
        auto list2 = list.get_list(0);
        list2->add(4);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, 0});
        CHECK(list->is_empty());
    });
}

TEST(Transform_ClearDictionaryInsideDictionaryVsCreateArray)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::Dictionary);
        auto dict2 = dict.get_dictionary("A");
        dict2->insert("key1", 1);
        dict2->insert("key2", 2);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A"});
        dict->insert("key3", 3);
        dict->clear();
        dict->insert("key4", 4);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::List);
        auto list = dict.get_list("A");
        list->add(4);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto list = obj.get_list_ptr<Mixed>({col_any, "A"});
        CHECK(list->is_empty());
    });
}

TEST(Transform_ClearDictionaryVsCreateDictionary)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 1);
        dict.clear();
        dict.insert("key2", 2);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key3", 3);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        CHECK_EQUAL(dict.size(), 1);
        CHECK_EQUAL(dict.get("key2"), 2);
    });
}

TEST(Transform_ClearDictionaryInsideArrayVsCreateDictionary)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, 42);
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.insert_collection(0, CollectionType::Dictionary);
        auto dict = list.get_dictionary(0);
        dict->insert("key1", 1);
        dict->clear();
        dict->insert("key2", 2);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::Dictionary);
        auto dict = list.get_dictionary(0);
        dict->insert("key3", 3);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, 0});
        CHECK_EQUAL(dict->size(), 1);
        CHECK_EQUAL(dict->get("key2"), 2);
    });
}

TEST(Transform_ClearDictionaryInsideDictionaryVsCreateDictionary)
{
    TransformTestHarness h(test_context, TransformTestHarness::ClientOneBeforeTwo, [](Obj obj, ColKey col_any) {
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("A", "some value");
    });

    h.transaction(h.client_1, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::Dictionary);
        auto dict2 = dict.get_dictionary("A");
        dict2->insert("key1", 1);
        dict2->clear();
        dict2->insert("key2", 2);
    });

    h.transaction(h.client_2, [](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::Dictionary);
        auto dict2 = dict.get_dictionary("A");
        dict2->insert("key3", 3);
    });

    h.check_merge_result([&](Obj obj, ColKey col_any) {
        auto dict = obj.get_dictionary_ptr({col_any, "A"});
        CHECK_EQUAL(dict->size(), 1);
        CHECK_EQUAL(dict->get("key2"), 2);
    });
}
