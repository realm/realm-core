#include <realm/sync/noinst/compact_changesets.hpp>
#include <realm/sync/noinst/changeset_index.hpp>

#include <realm/util/metered/map.hpp>
#include <realm/util/metered/set.hpp>

using namespace realm;
using namespace realm::sync;

namespace {

// FIXME: Implement changeset compaction for embedded objects.
#if 0
struct ChangesetCompactor {
    using GlobalID = _impl::ChangesetIndex::GlobalID;
    using RangeMap = _impl::ChangesetIndex::Ranges;
    using InstructionPosition = std::pair<Changeset*, Changeset::iterator>;


    struct ObjectInfo {
        // The instruction that creates the object.
        util::Optional<InstructionPosition> create_instruction;

        // All instructions touching the object, not including Create/Erase.
        // This also does not include instructions that *target* the object.
        RangeMap instructions;

        // For dead objects, link list instructions can generally be removed.
        // Due to merge semantics, when a target object is erased, there must
        // be a preceding link list erase instruction.  However if the link
        // list is cleared and all other link list operations are removed, the
        // compacted changeset is guaranteed to be mergable. ArraySwap and
        // ArrayMove operations can be removed without problems.
        //
        // The approach taken for link lists of dead objects is to remove all
        // ArraySwap and ArrayMove instructions, replace the first instruction
        // of type ArraySet, ArrayInsert, ArrayErase or ArrayClear with an
        // ArrayClear instruction, and remove all the remaining the
        // instructions of this type. Of course, if the first instruction
        // already is an ArrayClear the substitution is a no-op.
        //
        // The std::map field_to_link_list_op stores, for each field(column),
        // the position of the first instruction of type ArraySet, ArrayInsert,
        // ArrayErase or ArrayClear. The previous SelectField instruction and
        // the prior size are also stored. This map is needed to substitute a
        // set of link list operations with an ArrayClear instruction further
        // down.
        struct LinkListOp {
            InstructionPosition select_field_instr;
            InstructionPosition link_list_instr;
            uint32_t prior_size;
        };
        util::metered::map<StringData, LinkListOp> field_to_link_list_op;

        // If the object is referenced by any alive object, it will become a
        // ghost instead of dead. The object will also be a ghost if it has a
        // primary key.
        // FIXME: Define std::hash for StringData, GlobalID (waiting for Core to
        // give us an implementation of std::hash for StringData).
        util::metered::map<StringData, util::metered::set<GlobalKey>> referenced_by;
    };

    // Ordering guaranteed; Can use lower_bound() to find all objects belonging
    // to a particular table.
    util::metered::map<StringData, util::metered::map<GlobalKey, ObjectInfo>> m_objects;
    util::metered::vector<Changeset*> m_changesets;

    void add_changeset(Changeset&);
    void add_instruction_at(RangeMap&, Changeset&, Changeset::iterator);
    void compact();
    void compact_dead_object(ObjectInfo&, InstructionPosition erase_instruction);
    void compact_live_object(ObjectInfo&);
    void remove_redundant_selects();
};

void ChangesetCompactor::add_instruction_at(RangeMap& ranges, Changeset& changeset,
                                            Changeset::iterator pos)
{
    _impl::ChangesetIndex::add_instruction_at(ranges, changeset, pos); // Throws
}

void ChangesetCompactor::add_changeset(Changeset& changeset)
{
    m_changesets.push_back(&changeset);

    StringData selected_table;
    StringData selected_link_target_table;
    GlobalKey selected_container;
    StringData selected_field;
    InstructionPosition select_field_pos;

    for (auto it = changeset.begin(); it != changeset.end(); ++it) {
        auto instr = *it;
        if (!instr)
            continue;

        switch (instr->type) {
            case Instruction::Type::SelectTable: {
                auto& select_table = instr->get_as<Instruction::SelectTable>();
                selected_table = changeset.get_string(select_table.table);
                break;
            }
            case Instruction::Type::SelectField: {
                auto& select_container = instr->get_as<Instruction::SelectField>();
                selected_container = select_container.object;
                selected_field = changeset.get_string(select_container.field);
                select_field_pos = InstructionPosition {&changeset, it};
                selected_link_target_table = changeset.get_string(select_container.link_target_table);
                auto& info = m_objects[selected_table][selected_container]; // Throws
                add_instruction_at(info.instructions, changeset, it); // Throws
                break;
            }
            case Instruction::Type::AddTable: break;
            case Instruction::Type::EraseTable: break;
            case Instruction::Type::CreateObject: {
                auto& create_object = instr->get_as<Instruction::CreateObject>();
                GlobalKey object_id = create_object.object;
                auto& info = m_objects[selected_table][object_id]; // Throws
                info.create_instruction = InstructionPosition{&changeset, it};
                break;
            }
            case Instruction::Type::EraseObject: {
                auto& erase_object = instr->get_as<Instruction::EraseObject>();
                GlobalKey object_id = erase_object.object;
                auto it2 = m_objects.find(selected_table);
                if (it2 != m_objects.end()) {
                    auto it3 = it2->second.find(object_id);
                    if (it3 != it2->second.end()) {
                        compact_dead_object(it3->second, InstructionPosition{ &changeset, it }); // Throws
                        it2->second.erase(it3);
                    }
                }
                break;
            }
            case Instruction::Type::Set: {
                auto& set = instr->get_as<Instruction::Update>();
                auto& info = m_objects[selected_table][set.object]; // Throws
                if (set.payload.type == type_Link) {
                    StringData link_target_table = changeset.get_string(set.payload.data.link.target_table);
                    auto& link_target_info = m_objects[link_target_table][set.payload.data.link.target]; // Throws
                    link_target_info.referenced_by[selected_table].insert(set.object); // Throws
                }
                add_instruction_at(info.instructions, changeset, it); // Throws
                break;
            }
            case Instruction::Type::AddInteger: {
                auto& add_integer = instr->get_as<Instruction::AddInteger>();
                auto& info = m_objects[selected_table][add_integer.object]; // Throws
                add_instruction_at(info.instructions, changeset, it); // Throws
                break;
            }
            case Instruction::Type::AddColumn: break;
            case Instruction::Type::EraseColumn: break;
            case Instruction::Type::ArraySet: {
                auto& container_set = instr->get_as<Instruction::ArraySet>();
                auto& info = m_objects[selected_table][selected_container]; // Throws
                if (container_set.payload.type == type_Link) {
                    StringData link_target_table = changeset.get_string(container_set.payload.data.link.target_table);
                    REALM_ASSERT(link_target_table == selected_link_target_table);
                    // selected_link_target_table is used.
                    GlobalKey link_target = container_set.payload.data.link.target;
                    auto& target_info = m_objects[selected_link_target_table][link_target]; // Throws
                    target_info.referenced_by[selected_table].insert(selected_container); // Throws
                    ObjectInfo::LinkListOp& link_list_op = info.field_to_link_list_op[selected_field]; // Throws
                    if (!link_list_op.link_list_instr.first)
                        link_list_op = {select_field_pos, InstructionPosition{&changeset, it}, container_set.prior_size};
                }
                add_instruction_at(info.instructions, changeset, it); // Throws
                break;
            }
            case Instruction::Type::ArrayInsert: {
                auto& container_insert = instr->get_as<Instruction::ArrayInsert>();
                auto& info = m_objects[selected_table][selected_container];
                if (container_insert.payload.type == type_Link) {
                    // selected_link_target_table is used.
                    // Realms created by sync version 2.1.0 or newer has a class_ prefix on the property
                    // container_insert.payload.data.link.target_table. By using selected_link_target_table
                    // the link target table is correct for both old and new Realms.
                    GlobalKey link_target = container_insert.payload.data.link.target;
                    auto& target_info = m_objects[selected_link_target_table][link_target]; // Throws
                    target_info.referenced_by[selected_table].insert(selected_container); // Throws
                    ObjectInfo::LinkListOp& link_list_op = info.field_to_link_list_op[selected_field]; // Throws
                    if (!link_list_op.link_list_instr.first)
                        link_list_op = {select_field_pos, InstructionPosition{&changeset, it}, container_insert.prior_size};
                }
                add_instruction_at(info.instructions, changeset, it); // Throws
                break;
            }
            case Instruction::Type::ArrayMove:
            case Instruction::Type::ArrayErase: {
                auto& info = m_objects[selected_table][selected_container]; // Throws
                add_instruction_at(info.instructions, changeset, it); // Throws
                ObjectInfo::LinkListOp& link_list_op = info.field_to_link_list_op[selected_field]; // Throws
                if (!link_list_op.link_list_instr.first)
                    link_list_op = {select_field_pos, InstructionPosition{&changeset, it},
                       instr->get_as<Instruction::ArrayErase>().prior_size};
                break;
            }
            case Instruction::Type::ArrayClear: {
                auto& info = m_objects[selected_table][selected_container]; // Throws
                add_instruction_at(info.instructions, changeset, it); // Throws
                ObjectInfo::LinkListOp& link_list_op = info.field_to_link_list_op[selected_field]; // Throws
                if (!link_list_op.link_list_instr.first)
                    link_list_op = {select_field_pos, InstructionPosition{&changeset, it},
                       instr->get_as<Instruction::ArrayClear>().prior_size};
                break;
            }
        }
    }
}

void ChangesetCompactor::compact()
{
    for (auto& pair : m_objects) {
        for (auto& pair2 : pair.second) {
            compact_live_object(pair2.second);
        }
    }

    remove_redundant_selects();
}


void ChangesetCompactor::compact_dead_object(ObjectInfo& info, InstructionPosition erase_instruction)
{
    // "Ghost" objects are objects that we know will be deleted in the
    // end, but for which there are other references (such as link list
    // instructions), necessitating that the objects are actually
    // created. But all instructions populating the ghost object can
    // still be discarded, meaning that a reference from a ghost object cannot
    // produce more ghosts.
    bool is_ghost = false;
    if (info.create_instruction) {
        if (info.create_instruction->second->get_as<Instruction::CreateObject>().has_primary_key) {
            // If the object has a primary key, it must be considered a
            // ghost, because the corresponding EraseObject instruction
            // will have an effect in case of a primary key conflict.
            is_ghost = true;
        }
        else {
            for (auto& pair: info.referenced_by) {
                auto oit = m_objects.find(pair.first);
                if (oit != m_objects.end()) {
                    for (auto& object_id : pair.second) {
                        auto oit2 = oit->second.find(object_id);
                        if (oit2 != oit->second.end()) {
                            // Object is referenced by a surviving object, so
                            // mark it as a ghost.
                            is_ghost = true;
                            break;
                        }
                    }
                }
            }
        }
    }

    // Link list operations are only replaced with ArrayClear if the create
    // instruction is kept.
    bool maybe_link_list_substitution = is_ghost || !info.create_instruction;

    // Discard all instructions touching the object, other than
    // CreateObject and the instruction that deleted the object.
    // A link list clear will also be kept or substituted if needed to
    // remove other link list operations.
    for (auto& changeset_ranges : info.instructions) {
        for (auto& range: changeset_ranges.second) {
            REALM_ASSERT(range.begin.m_pos == 0);
            REALM_ASSERT(range.end.m_pos == 0);
            for (auto iter = range.begin; iter < range.end;) {
                auto instr = *iter;
                REALM_ASSERT(instr);
                bool select_field_must_be_kept = false;
                bool link_list_instr_must_be_substituted = false;
                uint32_t prior_size = 0;
                if (maybe_link_list_substitution) {
                    for (const auto& pair: info.field_to_link_list_op) {
                        if (instr == *pair.second.link_list_instr.second) {
                            link_list_instr_must_be_substituted = true;
                            prior_size = pair.second.prior_size;
                            break;
                        }
                        if (instr == *pair.second.select_field_instr.second) {
                            select_field_must_be_kept = true;
                            break;
                        }
                    }
                }
                REALM_ASSERT(!select_field_must_be_kept || !link_list_instr_must_be_substituted);
                if (link_list_instr_must_be_substituted) {
                    Instruction::Type type = instr->type;
                    REALM_ASSERT(type == Instruction::Type::ArraySet
                                 || type == Instruction::Type::ArrayInsert
                                 || type == Instruction::Type::ArrayErase
                                 || type == Instruction::Type::ArrayClear);
                    if (instr->type != Instruction::Type::ArrayClear) {
                        // Substitution takes place.
                        instr->type = Instruction::Type::ArrayClear;
                        instr->get_as<Instruction::ArrayClear>().prior_size = prior_size;
                    }
                    ++iter;
                }
                else if (select_field_must_be_kept) {
                    REALM_ASSERT(instr->type == Instruction::Type::SelectField);
                    ++iter;
                }
                else {
                    iter = changeset_ranges.first->erase_stable(iter);
                }
            }
        }
    }

    if (!is_ghost) {
        if (info.create_instruction) {
            // We created the object, so we can safely discard the CreateObject
            // instruction.
            info.create_instruction->first->erase_stable(info.create_instruction->second);

            // The object might have been erased by a ClearTable, in which
            // case the deleting instruction should not be discarded.
            if (erase_instruction.second->type == Instruction::Type::EraseObject) {
                erase_instruction.first->erase_stable(erase_instruction.second);
            }
        }
    }
}

void ChangesetCompactor::compact_live_object(ObjectInfo& info)
{
    // Look for redundant Set instructions, discard them.

    util::metered::map<StringData, std::pair<Changeset*, Changeset::iterator>> last_set_instructions;

    for (auto& changeset_ranges : info.instructions) {
        auto& changeset = *changeset_ranges.first;
        for (auto& range : changeset_ranges.second) {
            for (auto iter = range.begin; iter != range.end; ++iter) {
                auto instr = *iter;
                REALM_ASSERT(instr);
                Instruction::FieldInstructionBase* field_instr = nullptr;

                if (instr->type == Instruction::Type::Set) {
                    // If a previous Set instruction existed for this field, discard it
                    // and record the position of this instruction instead.
                    auto& set = instr->get_as<Instruction::Update>();
                    StringData field = changeset.get_string(set.field);
                    auto it = last_set_instructions.find(field);

                    if (it != last_set_instructions.end() && it->second.first->origin_timestamp <= changeset.origin_timestamp) {
                        it->second.first->erase_stable(it->second.second);
                        it->second = {&changeset, iter};
                    }
                    else {
                        last_set_instructions[field] = {&changeset, iter};
                    }
                }
                else if (instr->type == Instruction::Type::AddInteger) {
                    field_instr = &instr->get_as<Instruction::AddInteger>();
                }

                if (field_instr != nullptr) {
                    // A non-Set field instruction was encountered, which requires the
                    // previous Set instruction to be preserved.
                    StringData field = changeset.get_string(field_instr->field);
                    last_set_instructions.erase(field);
                }
            }
        }
    }
}

void ChangesetCompactor::remove_redundant_selects() {
    // This removes sequences of SelectTable and SelectField instructions,
    // except the last. Select instructions have no effect if followed
    // immediately by another Select instruction at the same level.

    for (Changeset* changeset : m_changesets) {
        util::Optional<Changeset::iterator> previous_select_table;
        util::Optional<Changeset::iterator> previous_select_field;

        for (auto it = changeset->begin(); it != changeset->end(); ++it) {
            auto instr = *it;
            if (!instr)
                continue;

            switch (instr->type) {
                case Instruction::Type::SelectTable: {
                    if (previous_select_table) {
                        changeset->erase_stable(*previous_select_table);
                    }
                    if (previous_select_field) {
                        changeset->erase_stable(*previous_select_field);
                        previous_select_field = util::none;
                    }
                    previous_select_table = it;
                    break;
                }
                case Instruction::Type::SelectField: {
                    if (previous_select_field) {
                        changeset->erase_stable(*previous_select_field);
                    }
                    previous_select_field = it;
                    break;
                }
                default: {
                    previous_select_table = util::none;
                    previous_select_field = util::none;
                    break;
                }
            }
        }
    }
}

#endif

} // unnamed namespace

void realm::_impl::compact_changesets(Changeset*, size_t)
{
    // FIXME: Implement changeset compaction for embedded objects.
    return;

#if 0
    ChangesetCompactor compactor;

    for (size_t i = 0; i < num_changesets; ++i) {
        compactor.add_changeset(changesets[i]); // Throws
    }

    compactor.compact(); // Throws
#endif
}
