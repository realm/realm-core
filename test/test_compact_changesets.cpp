#include "test.hpp"
#include "util/random.hpp"

#include <realm/sync/noinst/compact_changesets.hpp>
#include <realm/sync/changeset_encoder.hpp>

using namespace realm;
using namespace realm::sync;
using namespace realm::_impl;

namespace {
struct InstructionBuilder : InstructionHandler {
    using Instruction = realm::sync::Instruction;

    explicit InstructionBuilder(Changeset& log)
        : m_log(log)
    {
    }
    Changeset& m_log;

    void operator()(const Instruction& instr) final
    {
        m_log.push_back(instr);
    }

    StringBufferRange add_string_range(StringData string) final
    {
        return m_log.append_string(string);
    }

    void set_intern_string(uint32_t index, StringBufferRange range) final
    {
        m_log.interned_strings()[index] = range;
    }

    InternString intern_string(StringData string)
    {
        return m_log.intern_string(string);
    }
};
} // unnamed namespace

// FIXME: Compaction is disabled since path-based instructions.
TEST_IF(CompactChangesets_RedundantSets, false)
{
    using Instruction = realm::sync::Instruction;
    Changeset changeset;
    InstructionBuilder push(changeset);

    auto table = changeset.intern_string("Test");

    Instruction::Update set1;
    set1.table = table;
    set1.object = GlobalKey{1, 1};
    set1.field = changeset.intern_string("foo");
    set1.value = Instruction::Payload(int64_t(123));
    push(set1);

    Instruction::Update set2;
    set2.table = table;
    set2.object = GlobalKey{1, 1};
    set2.field = changeset.intern_string("foo");
    set2.value = Instruction::Payload(int64_t(345));
    push(set2);

    Instruction::Update set3;
    set3.table = table;
    set3.object = GlobalKey{1, 1};
    set3.field = changeset.intern_string("foo");
    set3.value = Instruction::Payload(int64_t(123));
    push(set3);

    CHECK_EQUAL(changeset.size(), 4);

    compact_changesets(&changeset, 1);

    CHECK_EQUAL(changeset.size(), 2);
}

// FIXME: Compaction is disabled since path-based instructions.
TEST_IF(CompactChangesets_DiscardsCreateErasePair, false)
{
    using Instruction = realm::sync::Instruction;
    Changeset changeset;
    InstructionBuilder push(changeset);

    auto table = changeset.intern_string("Test");

    Instruction::CreateObject create_object;
    create_object.table = table;
    create_object.object = GlobalKey{1, 1};
    push(create_object);

    Instruction::Update set;
    set.table = table;
    set.object = GlobalKey{1, 1};
    set.field = changeset.intern_string("foo");
    set.value = Instruction::Payload{int64_t(123)};
    push(set);

    Instruction::EraseObject erase_object;
    erase_object.table = table;
    erase_object.object = GlobalKey{1, 1};
    push(erase_object);

    CHECK_EQUAL(changeset.size(), 4);

    compact_changesets(&changeset, 1);

    CHECK_EQUAL(changeset.size(), 1);
}

// FIXME: Compaction is disabled since path-based instructions.
TEST_IF(CompactChangesets_LinksRescueObjects, false)
{
    using Instruction = realm::sync::Instruction;
    Changeset changeset;
    InstructionBuilder push(changeset);

    auto table = changeset.intern_string("Test");
    auto other = changeset.intern_string("Other");

    Instruction::CreateObject create_object;
    create_object.table = table;
    create_object.object = GlobalKey{1, 1};
    push(create_object);

    Instruction::Update set;
    set.table = table;
    set.field = changeset.intern_string("foo");
    set.object = GlobalKey{1, 1};
    set.value = Instruction::Payload{int64_t(123)};
    push(set);

    Instruction::ArrayInsert link_list_insert;
    link_list_insert.table = other;
    link_list_insert.object = GlobalKey{1, 2};
    link_list_insert.field = changeset.intern_string("field");
    link_list_insert.prior_size = 0;
    link_list_insert.path.push_back(0);
    link_list_insert.value = Instruction::Payload(Instruction::Payload::Link{table, GlobalKey{1, 1}});
    push(link_list_insert);

    // slightly unrealistic; this would always be preceded by a LinkListErase
    // (nullify) instruction, but whatever.
    // FIXME: ... until dangling links are implemented.
    Instruction::EraseObject erase_object;
    erase_object.table = table;
    erase_object.object = GlobalKey{1, 1};
    push(erase_object);

    CHECK_EQUAL(changeset.size(), 8);

    compact_changesets(&changeset, 1);

    CHECK_EQUAL(changeset.size(), 7);
}

// FIXME: Compaction is disabled since path-based instructions.
TEST_IF(CompactChangesets_EliminateSubgraphs, false)
{
    using Instruction = realm::sync::Instruction;
    Changeset changeset;
    InstructionBuilder push(changeset);

    auto table = changeset.intern_string("Test");

    Instruction::CreateObject create_object;
    create_object.table = table;
    create_object.object = GlobalKey{1, 1};
    push(create_object);

    Instruction::CreateObject create_object_2;
    create_object_2.table = table;
    create_object_2.object = GlobalKey{1, 2};
    push(create_object_2);

    // Create a link from {1, 1} to {1, 2}
    Instruction::ArrayInsert link_list_insert;
    link_list_insert.table = table;
    link_list_insert.object = GlobalKey{1, 1};
    link_list_insert.field = changeset.intern_string("field");
    link_list_insert.prior_size = 0;
    link_list_insert.path.push_back(0);
    link_list_insert.value = Instruction::Payload(Instruction::Payload::Link{table, GlobalKey{1, 2}});
    push(link_list_insert);

    // slightly unrealistic; this would always be preceded by a LinkListErase
    // (nullify) instruction, but whatever.
    Instruction::EraseObject erase_object;
    erase_object.table = table;
    erase_object.object = GlobalKey{1, 1};
    push(erase_object);

    Instruction::EraseObject erase_object_2;
    erase_object_2.table = table;
    erase_object_2.object = GlobalKey{1, 2};
    push(erase_object_2);

    CHECK_EQUAL(changeset.size(), 7);

    compact_changesets(&changeset, 1);

    CHECK_EQUAL(changeset.size(), 1); // Only the SelectTable remains
}


// FIXME: Compaction is disabled since path-based instructions.
TEST_IF(CompactChangesets_EraseRecreate, false)
{
    using Instruction = realm::sync::Instruction;
    Changeset changeset;
    InstructionBuilder push(changeset);

    auto table = changeset.intern_string("Test");
    auto field = changeset.intern_string("foo");

    Instruction::CreateObject create_1;
    create_1.table = table;
    create_1.object = GlobalKey{1, 1};
    push(create_1);

    Instruction::Update set_1;
    set_1.table = table;
    set_1.object = GlobalKey{1, 1};
    set_1.field = field;
    set_1.value = Instruction::Payload{int64_t(123)};
    push(set_1);

    Instruction::EraseObject erase;
    erase.table = table;
    erase.object = GlobalKey{1, 1};
    push(erase);

    Instruction::CreateObject create_2;
    create_2.table = table;
    create_2.object = GlobalKey{1, 1};
    push(create_2);

    Instruction::Update set_2;
    set_2.table = table;
    set_2.object = GlobalKey{1, 1};
    set_2.field = field;
    set_2.value = Instruction::Payload{int64_t(123)};
    push(set_2);

    CHECK_EQUAL(changeset.size(), 6);

    compact_changesets(&changeset, 1);

    CHECK_EQUAL(changeset.size(), 3); // Only the first Set instruction should be removed
}


#if 0
TEST(CompactChangesets_PrimaryKeysRescueObjects)
{
    using Instruction = realm::sync::Instruction;
    Changeset changeset;
    InstructionBuilder push(changeset);

    push(Instruction::SelectTable{changeset.intern_string("Test")});

    Instruction::CreateObject create_object;
    create_object.has_primary_key = true;
    create_object.payload = Instruction::Payload{int64_t(123)};
    create_object.object = object_id_for_primary_key(123);
    push(create_object);


    Instruction::Update set;
    set.field = changeset.intern_string("foo");
    set.object = object_id_for_primary_key(123);
    set.payload = Instruction::Payload{int64_t(123)};
    push(set);

    Instruction::EraseObject erase_object;
    erase_object.object = object_id_for_primary_key(123);
    push(erase_object);

    CHECK_EQUAL(changeset.size(), 4);

    compact_changesets(&changeset, 1);

    CHECK_EQUAL(changeset.size(), 3);
}

namespace {

GlobalKey make_object_id(test_util::Random& random)
{
    return GlobalKey{random.draw_int<uint64_t>(1, 3), random.draw_int<uint64_t>(1, 5)};
}

void select_table(StringData table, InstructionBuilder& builder, StringData& selected_table)
{
    using Instruction = realm::sync::Instruction;
    if (selected_table != table) {
        builder(Instruction::SelectTable{builder.intern_string(table)});
        selected_table = table;
    }
}
} // unnamed namespace

TEST_IF(CompactChangesets_Measure, false)
{
    using Instruction = realm::sync::Instruction;
    using B = InstructionBuilder;
    using R = test_util::Random;

    static const size_t changeset_size = 10000;

    auto generate_large_changeset = [](Changeset& changeset) {
        InstructionBuilder builder(changeset);

        // Assuming schema:
        //
        // class Foo {
        //   string foo;
        //   int bar;
        // }
        //
        // class Bar {
        //   Link<Foo> foo;
        //   int bar;
        // }


        auto create_foo = [](B& builder, R& random, StringData& selected_table) {
            select_table("class_Foo", builder, selected_table);
            Instruction::CreateObject create_object;
            create_object.object = make_object_id(random);
            create_object.has_primary_key = false;
            builder(create_object);
        };

        auto create_bar = [](B& builder, R& random, StringData& selected_table) {
            select_table("class_Bar", builder, selected_table);
            Instruction::CreateObject create_object;
            create_object.object = make_object_id(random);
            create_object.has_primary_key = false;
            builder(create_object);
        };

        auto erase_foo = [](B& builder, R& random, StringData& selected_table) {
            select_table("class_Foo", builder, selected_table);
            Instruction::EraseObject erase_object;
            erase_object.object = make_object_id(random);
            builder(erase_object);
        };

        auto erase_bar = [](B& builder, R& random, StringData& selected_table) {
            select_table("class_Bar", builder, selected_table);
            Instruction::EraseObject erase_object;
            erase_object.object = make_object_id(random);
            builder(erase_object);
        };

        auto set_foo_foo = [](B& builder, R& random, StringData& selected_table) {
            select_table("class_Foo", builder, selected_table);
            Instruction::Update set;
            set.object = make_object_id(random);
            set.field = builder.intern_string("foo");
            set.payload = Instruction::Payload(random.draw_int<int64_t>(0, 10));
            builder(set);
        };

        auto set_foo_bar = [](B& builder, R& random, StringData& selected_table) {
            select_table("class_Foo", builder, selected_table);
            Instruction::Update set;
            set.object = make_object_id(random);
            set.field = builder.intern_string("bar");
            set.payload = Instruction::Payload(builder.add_string_range(
                "VERY LONG STRING VERY LONG STRING VERY LONG STRING VERY LONG STRING VERY LONG STRING VERY LONG "
                "STRING VERY LONG STRING VERY LONG STRING VERY LONG STRING VERY LONG STRING VERY LONG STRING VERY "
                "LONG STRING VERY LONG STRING VERY LONG STRING VERY LONG STRING VERY LONG STRING"));
            builder(set);
        };

        auto set_bar_foo = [](B& builder, R& random, StringData& selected_table) {
            select_table("class_Bar", builder, selected_table);
            Instruction::Update set;
            set.object = make_object_id(random);
            set.field = builder.intern_string("foo");
            Instruction::Payload::Link link;
            link.target_table = builder.intern_string("class_Foo");
            link.target = make_object_id(random);
            set.payload = Instruction::Payload(link);
            builder(set);
        };

        auto set_bar_bar = [](B& builder, R& random, StringData& selected_table) {
            select_table("class_Bar", builder, selected_table);
            Instruction::Update set;
            set.object = make_object_id(random);
            set.field = builder.intern_string("bar");
            set.payload = Instruction::Payload(random.draw_int<int64_t>(0, 10));
            builder(set);
        };

        static const size_t num_actions = 8;
        void (*actions[num_actions])(InstructionBuilder&, test_util::Random&, StringData&) = {
            create_foo, create_bar, erase_foo, erase_bar, set_foo_foo, set_foo_bar, set_bar_foo, set_bar_bar,
        };

        test_util::Random random;
        StringData selected_table;

        random.seed(123);

        for (size_t i = 0; i < changeset_size; ++i) {
            size_t action_ndx = random.draw_int_mod<size_t>(num_actions);
            auto action = actions[action_ndx];
            action(builder, random, selected_table);
        }
    };

    Changeset changeset;
    generate_large_changeset(changeset);
    size_t num_instructions_before = changeset.size();

    util::AppendBuffer<char> encoded_uncompacted;
    encode_changeset(changeset, encoded_uncompacted);

    std::cout << "Encoded, uncompacted: " << encoded_uncompacted.size() << " bytes\n";

    compact_changesets(&changeset, 1);
    size_t num_instructions_after = changeset.size();

    util::AppendBuffer<char> encoded_compacted;
    encode_changeset(changeset, encoded_compacted);

    std::cout << "Encoded, compacted:   " << encoded_compacted.size() << " bytes\n";
    std::cout << "# instructions discarded: " << num_instructions_before - num_instructions_after << "\n";
    std::cout << "\n";
}

#endif
