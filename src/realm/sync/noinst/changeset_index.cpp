#include <realm/sync/noinst/changeset_index.hpp>
#include <realm/sync/object_id.hpp>

#include <iterator> // std::distance, std::advance

using namespace realm::sync;

namespace realm {
namespace _impl {

#if REALM_DEBUG
static bool compare_ranges(const Changeset::Range& left, const Changeset::Range& right)
{
    return left.begin < right.begin;
}

template <class InputIterator>
static bool check_ranges(InputIterator begin, InputIterator end)
{
    if (!std::is_sorted(begin, end, compare_ranges))
        return false;

    // Check that there are no overlaps
    if (begin != end) {
        auto last_end = begin->end;
        for (auto i = begin + 1; i != end; ++i) {
            if (last_end > i->begin)
                return false;
            last_end = i->end;
        }
    }
    return true;
}

static bool check_ranges(const ChangesetIndex::Ranges& ranges)
{
    for (auto& pair : ranges) {
        if (!check_ranges(pair.second.begin(), pair.second.end()))
            return false;
    }
    return true;
}
#endif // REALM_DEBUG


void ChangesetIndex::clear() noexcept
{
    m_object_instructions.clear();
    m_schema_instructions.clear();
    m_conflict_groups_owner.clear();
    m_num_conflict_groups = 0;
}


void ChangesetIndex::scan_changeset(Changeset& changeset)
{
    if (m_contains_destructive_schema_changes)
        return;

#if defined(REALM_DEBUG) // LCOV_EXCL_START
    for (auto& confict_group : m_conflict_groups_owner) {
        // Check that add_changeset() has not been called yet.
        REALM_ASSERT(confict_group.ranges.empty());
    }
#endif // REALM_DEBUG LCOV_EXCL_STOP

    using Instruction = realm::sync::Instruction;

    for (auto it = changeset.begin(); it != changeset.end(); ++it) {
        if (!*it)
            continue;

        const auto& instr = **it;

        if (auto add_table_instr = instr.get_if<Instruction::AddTable>()) {
            auto& p = *add_table_instr;
            schema_conflict_group(changeset.get_string(p.table));
        }
        else if (instr.get_if<Instruction::EraseTable>()) {
            m_contains_destructive_schema_changes = true;
            clear();
            return;
        }
        else if (auto add_column_instr = instr.get_if<Instruction::AddColumn>()) {
            auto& p = *add_column_instr;
            StringData table_name = changeset.get_string(p.table);
            ConflictGroup& cg = schema_conflict_group(table_name);
            if (p.type == Instruction::Payload::Type::Link) {
                StringData target_table = changeset.get_string(p.link_target_table);
                ConflictGroup& cg2 = schema_conflict_group(target_table);
                merge_conflict_groups(cg, cg2);
            }
        }
        else if (instr.get_if<Instruction::EraseColumn>()) {
            m_contains_destructive_schema_changes = true;
            clear();
            return;
        }
        else {
            GlobalID ids[2];
            size_t num_ids = get_object_ids_in_instruction(changeset, instr, ids, 2);
            REALM_ASSERT(num_ids >= 1);
            REALM_ASSERT(num_ids <= 2);

            ConflictGroup& cg = object_conflict_group(ids[0]);
            for (size_t i = 1; i < num_ids; ++i) {
                ConflictGroup& cg2 = object_conflict_group(ids[1]);
                merge_conflict_groups(cg, cg2);
            }
        }
    }
}


void ChangesetIndex::merge_conflict_groups(ConflictGroup& into, ConflictGroup& from)
{
    if (&into == &from)
        return;

    if (from.size > into.size) {
        // This is an optimization. The time it takes to merge two conflict
        // groups is proportional to the size of the incoming group (in number
        // of objects touched). If the incoming group is larger, merge the other
        // way.
        merge_conflict_groups(from, into);
        return;
    }

    REALM_ASSERT(into.ranges.empty());
    REALM_ASSERT(from.ranges.empty());

    for (auto& class_name : from.schemas) {
        m_schema_instructions[class_name] = &into;
    }
    into.schemas.insert(into.schemas.end(), from.schemas.begin(), from.schemas.end());

    for (auto& pair : from.objects) {
        auto& objset = into.objects[pair.first];
        auto& objinstr = m_object_instructions[pair.first];
        for (auto& object : pair.second) {
            objinstr[object] = &into;
        }
        objset.insert(objset.end(), pair.second.begin(), pair.second.end());
    }
    into.size += from.size;

    m_conflict_groups_owner.erase(from.self_it);
    --m_num_conflict_groups;
}


void ChangesetIndex::add_changeset(Changeset& log)
{
    if (!log.empty())
        m_everything[&log] = util::metered::vector<Changeset::Range>(1, Changeset::Range{log.begin(), log.end()});

    if (m_contains_destructive_schema_changes)
        return; // Just add to everything.

    using Instruction = realm::sync::Instruction;

    // Iterate over all instructions (skipping tombstones), and add them to the
    // index.
    for (auto it = log.begin(); it != log.end(); ++it) {
        if (!*it)
            continue;

        const auto& instr = **it;

        if (auto add_table_instr = instr.get_if<Instruction::AddTable>()) {
            StringData table = log.get_string(add_table_instr->table);
            auto& cg = schema_conflict_group(table);
            add_instruction_at(cg.ranges, log, it);
        }
        else if (instr.get_if<Instruction::EraseTable>()) {
            REALM_TERMINATE("Call scan_changeset() before add_changeset().");
        }
        else if (auto add_column_instr = instr.get_if<Instruction::AddColumn>()) {
            auto& p = *add_column_instr;
            StringData table = log.get_string(p.table);
            auto& cg = schema_conflict_group(table);
            if (p.type == Instruction::Payload::Type::Link) {
                REALM_ASSERT(&cg == &schema_conflict_group(log.get_string(p.link_target_table)));
            }
            add_instruction_at(cg.ranges, log, it);
        }
        else if (instr.get_if<Instruction::EraseColumn>()) {
            REALM_TERMINATE("Call scan_changeset() before add_changeset().");
        }
        else {
            GlobalID ids[2];
            size_t num_ids = get_object_ids_in_instruction(log, instr, ids, 2);
            REALM_ASSERT(num_ids >= 1);
            REALM_ASSERT(num_ids <= 2);

            auto& cg = object_conflict_group(ids[0]);
            for (size_t i = 1; i < num_ids; ++i) {
                REALM_ASSERT(&cg == &object_conflict_group(ids[i]));
            }
            add_instruction_at(cg.ranges, log, it);
        }
    }
}

size_t get_object_ids_in_instruction(const Changeset& changeset, const sync::Instruction& instr,
                                     ChangesetIndex::GlobalID* ids, size_t max_num_ids)
{
    REALM_ASSERT_RELEASE(max_num_ids >= 2);

    using Instruction = realm::sync::Instruction;

    if (auto obj_instr = instr.get_if<Instruction::ObjectInstruction>()) {
        ids[0] = {changeset.get_string(obj_instr->table), changeset.get_key(obj_instr->object)};

        if (auto set_instr = instr.get_if<Instruction::Update>()) {
            auto& p = *set_instr;
            if (p.value.type == Instruction::Payload::Type::Link) {
                ids[1] = {changeset.get_string(p.value.data.link.target_table),
                          changeset.get_key(p.value.data.link.target)};
                return 2;
            }
        }
        else if (auto insert_instr = instr.get_if<Instruction::ArrayInsert>()) {
            auto& p = *insert_instr;
            if (p.value.type == Instruction::Payload::Type::Link) {
                ids[1] = {changeset.get_string(p.value.data.link.target_table),
                          changeset.get_key(p.value.data.link.target)};
                return 2;
            }
        }

        return 1;
    }

    return 0;
}

auto ChangesetIndex::get_schema_changes_for_class(StringData class_name) const -> const Ranges*
{
    return const_cast<ChangesetIndex*>(this)->get_schema_changes_for_class(class_name);
}

auto ChangesetIndex::get_schema_changes_for_class(StringData class_name) -> Ranges*
{
    if (m_contains_destructive_schema_changes)
        return &m_everything;
    auto it = m_schema_instructions.find(class_name);
    if (it == m_schema_instructions.end())
        return &m_empty;
    return &it->second->ranges;
}

auto ChangesetIndex::get_modifications_for_object(GlobalID id) -> Ranges*
{
    if (m_contains_destructive_schema_changes)
        return &m_everything;
    auto it = m_object_instructions.find(id.table_name);
    if (it == m_object_instructions.end())
        return &m_empty;

    auto& object_instructions = it->second;
    auto it2 = object_instructions.find(id.object_id);
    if (it2 == object_instructions.end())
        return &m_empty;
    return &it2->second->ranges;
}

auto ChangesetIndex::get_modifications_for_object(GlobalID id) const -> const Ranges*
{
    return const_cast<ChangesetIndex*>(this)->get_modifications_for_object(id);
}

auto ChangesetIndex::schema_conflict_group(StringData class_name) -> ConflictGroup&
{
    auto& conflict_group = m_schema_instructions[class_name];
    if (conflict_group == nullptr) {
        m_conflict_groups_owner.emplace_back();
        ++m_num_conflict_groups;
        auto new_cg = std::prev(m_conflict_groups_owner.end());
        new_cg->schemas.push_back(class_name);
        new_cg->size = 1;
        new_cg->self_it = new_cg;
        conflict_group = &*new_cg;
    }
    return *conflict_group;
}

auto ChangesetIndex::object_conflict_group(const GlobalID& object_id) -> ConflictGroup&
{
    auto& objects_for_table = m_object_instructions[object_id.table_name];
    auto& conflict_group = objects_for_table[object_id.object_id];
    if (conflict_group == nullptr) {
        m_conflict_groups_owner.emplace_back();
        ++m_num_conflict_groups;
        auto new_cg = std::prev(m_conflict_groups_owner.end());
        new_cg->objects[object_id.table_name].push_back(object_id.object_id);
        new_cg->size = 1;
        new_cg->self_it = new_cg;
        conflict_group = &*new_cg;
    }
    return *conflict_group;
}

auto ChangesetIndex::erase_instruction(RangeIterator pos) -> RangeIterator
{
    Changeset* changeset = pos.m_outer->first;
    pos.check();
    auto new_pos = pos;
    new_pos.m_pos = changeset->erase_stable(pos.m_pos);

    if (new_pos.m_pos >= new_pos.m_inner->end) {
        // erased the last instruction in the range, move to the next range.
        REALM_ASSERT(new_pos.m_inner < new_pos.m_outer->second.end());
        ++new_pos.m_inner;
        if (new_pos.m_inner == new_pos.m_outer->second.end()) {
            REALM_ASSERT(new_pos.m_outer != new_pos.m_ranges->end());
            ++new_pos.m_outer;
            if (new_pos.m_outer == new_pos.m_ranges->end()) {
                return RangeIterator{new_pos.m_ranges, RangeIterator::end_tag{}}; // avoid new_pos.check()
            }
            else {
                new_pos.m_inner = new_pos.m_outer->second.begin();
                REALM_ASSERT(new_pos.m_inner != new_pos.m_outer->second.end());
                new_pos.m_pos = new_pos.m_inner->begin;
            }
        }
        else {
            new_pos.m_pos = new_pos.m_inner->begin;
            REALM_ASSERT(new_pos.m_pos != new_pos.m_inner->end); // empty ranges not allowed
        }
    }
    new_pos.check();
    return new_pos;
}

#if REALM_DEBUG
std::ostream& operator<<(std::ostream& os, GlobalID gid)
{
    return os << gid.table_name << "/" << format_pk(gid.object_id);
}

void ChangesetIndex::print(std::ostream& os) const
{
    // FIXME: TODO
    static_cast<void>(os);

    auto print_ranges = [&](const auto& subjects, const Ranges& ranges) {
        os << "[";
        for (auto it = subjects.begin(); it != subjects.end();) {
            os << *it;
            auto next = it;
            ++next;
            if (next != subjects.end())
                os << ", ";
            it = next;
        }
        os << "]: ";
        for (auto it = ranges.begin(); it != ranges.end();) {
            os << "Changeset" << std::dec << it->first->version << "(";
            for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
                auto offset = std::distance(it->first->begin(), it2->begin);
                auto length = std::distance(it2->begin, it2->end);
                os << "[" << std::dec << offset << "+" << length << "]";
                if (it2 + 1 != it->second.end())
                    os << ", ";
            }
            os << ")";
            auto next = it;
            ++next;
            if (next != ranges.end())
                os << ", ";
            it = next;
        }
    };

    std::map<Ranges*, std::set<StringData>> schema_modifications;
    std::map<Ranges*, std::set<GlobalID>> object_modifications;

    for (auto& pair : m_schema_instructions) {
        schema_modifications[&pair.second->ranges].insert(pair.first);
    }

    for (auto& pair : m_object_instructions) {
        for (auto& pair2 : pair.second) {
            object_modifications[&pair2.second->ranges].insert(GlobalID{pair.first, pair2.first});
        }
    }

    if (schema_modifications.size()) {
        os << "SCHEMA MODIFICATIONS:\n";
        for (const auto& pair : schema_modifications) {
            print_ranges(pair.second, *pair.first);
            os << "\n";
        }
        os << "\n";
    }

    if (object_modifications.size()) {
        os << "OBJECT MODIFICATIONS:\n";
        for (const auto& pair : object_modifications) {
            print_ranges(pair.second, *pair.first);
            os << "\n";
        }
        os << "\n";
    }
}

void ChangesetIndex::verify() const
{
    REALM_ASSERT(m_num_conflict_groups == m_conflict_groups_owner.size());

    // Verify that there are no stray pointers.
    for (auto& pair : m_object_instructions) {
        for (auto& pair2 : pair.second) {
            REALM_ASSERT(&*pair2.second->self_it == pair2.second);
            REALM_ASSERT(std::any_of(m_conflict_groups_owner.begin(), m_conflict_groups_owner.end(), [&](auto& cg) {
                return &cg == pair2.second;
            }));
        }
    }

    for (auto& pair : m_schema_instructions) {
        REALM_ASSERT(&*pair.second->self_it == pair.second);
        REALM_ASSERT(std::any_of(m_conflict_groups_owner.begin(), m_conflict_groups_owner.end(), [&](auto& cg) {
            return &cg == pair.second;
        }));
    }

    // Collect all changesets
    std::set<Changeset*> changesets;
    for (auto& cg : m_conflict_groups_owner) {
        check_ranges(cg.ranges);

        for (auto& ranges : cg.ranges) {
            changesets.insert(ranges.first);
        }
    }

    // Run through all instructions in each changeset and check that
    // instructions are correctly covered by the index.
    for (auto changeset : changesets) {
        auto& log = *changeset;

        using Instruction = realm::sync::Instruction;

        // Iterate over all instructions (skipping tombstones), and verify that
        // the index covers any objects mentioned in that instruction.
        for (auto it = log.begin(); it != log.end(); ++it) {
            if (!*it)
                continue;

            const auto& instr = **it;

            if (auto add_table_instr = instr.get_if<Instruction::AddTable>()) {
                StringData table = log.get_string(add_table_instr->table);
                auto ranges = *get_schema_changes_for_class(table);
                REALM_ASSERT(ranges_cover(ranges, log, it));
            }
            else if (auto erase_table_instr = instr.get_if<Instruction::EraseTable>()) {
                StringData table = log.get_string(erase_table_instr->table);
                auto ranges = *get_schema_changes_for_class(table);
                REALM_ASSERT(ranges_cover(ranges, log, it));
            }
            else if (auto add_column_instr = instr.get_if<Instruction::AddColumn>()) {
                StringData table = log.get_string(add_column_instr->table);
                auto ranges = *get_schema_changes_for_class(table);
                REALM_ASSERT(ranges_cover(ranges, log, it));
            }
            else if (auto erase_column_instr = instr.get_if<Instruction::EraseColumn>()) {
                StringData table = log.get_string(erase_column_instr->table);
                auto ranges = *get_schema_changes_for_class(table);
                REALM_ASSERT(ranges_cover(ranges, log, it));
            }
            else {
                GlobalID ids[2];
                size_t num_ids = get_object_ids_in_instruction(log, instr, ids, 2);
                REALM_ASSERT(num_ids >= 1);
                REALM_ASSERT(num_ids <= 2);
                auto& ranges_first = *get_modifications_for_object(ids[0]);

                for (size_t i = 0; i < num_ids; ++i) {
                    auto& ranges = *get_modifications_for_object(ids[i]);
                    REALM_ASSERT(&ranges == &ranges_first);
                    REALM_ASSERT(ranges_cover(ranges, log, it));
                }
            }
        }
    }
}

bool ChangesetIndex::ranges_cover(const Ranges& ranges, Changeset& log, Changeset::const_iterator it) const
{
    auto outer = ranges.find(&log);
    if (outer == ranges.end())
        return false;
    for (auto& range : outer->second) {
        if (it >= range.begin && it < range.end)
            return true;
    }
    return false;
}

#endif // REALM_DEBUG

void ChangesetIndex::add_instruction_at(Ranges& ranges, Changeset& changeset, Changeset::iterator pos)
{
    auto& ranges_for_changeset = ranges[&changeset];

    REALM_ASSERT(pos != changeset.end());
    auto next = pos;
    ++next;

    Changeset::Range incoming{pos, next};

    auto cmp = [](const Changeset::Range& range_a, const Changeset::Range& range_b) {
        return range_a.begin < range_b.begin;
    };

    auto it = std::lower_bound(ranges_for_changeset.begin(), ranges_for_changeset.end(), incoming, cmp);

    it = ranges_for_changeset.insert(it, incoming);
    if (it != ranges_for_changeset.begin())
        --it;

    // Merge adjacent overlapping ranges
    while (it + 1 != ranges_for_changeset.end()) {
        auto next_it = it + 1;
        if (it->end >= next_it->begin) {
            it->end = std::max(it->end, next_it->end);
            next_it = ranges_for_changeset.erase(next_it);
            it = next_it - 1;
        }
        else {
            ++it;
        }
    }
}


} // namespace _impl
} // namespace realm
