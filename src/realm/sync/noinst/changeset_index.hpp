
#ifndef REALM_NOINST_CHANGESET_INDEX_HPP
#define REALM_NOINST_CHANGESET_INDEX_HPP

#include <deque>
#include <list>
#include <map>

#include <realm/sync/changeset.hpp>

namespace realm {
namespace _impl {

/// The ChangesetIndex is responsible for keeping track of exactly which
/// instructions touch which objects. It does this by recording ranges of
/// instructions in changesets, such that the merge algorithm can do with
/// just merging the "relevant" instructions. Due to the semantics of link
/// nullification, instruction ranges for objects that have ever been
/// "connected" by a link instruction must be joined together. In other words,
/// if two objects are connected by a link instruction in a changeset, all
/// instructions pertaining to both objects will be merged with any instruction
/// that touches either.
struct ChangesetIndex {
    using Changeset = realm::sync::Changeset;
    using GlobalID = realm::sync::GlobalID;
    using PrimaryKey = realm::sync::PrimaryKey;

    struct CompareChangesetPointersByVersion {
        bool operator()(const Changeset* a, const Changeset* b) const noexcept
        {
            if (a->version == b->version) {
                return a->transform_sequence < b->transform_sequence;
            }
            return a->version < b->version;
        }
    };

    // This is always sorted by (changeset->version, range->begin).
    using Ranges = std::map<Changeset*, std::vector<Changeset::Range>, CompareChangesetPointersByVersion>;

    /// Scan changeset to discover objects connected by link instructions,
    /// classes connected by link columns, and destructive schema changes.
    ///
    /// Note: This function must be called before calling `add_changeset()`, and
    /// it must be called for both the changesets added to the index (incoming
    /// changesets) and reciprocal changesets.
    void scan_changeset(Changeset& changeset);

    /// Add instructions from \a changeset to the index.
    ///
    /// Note: It is an error to add the same changeset more than once.
    void add_changeset(Changeset& changeset);

    //@{
    /// Returns ranges for every schema change that mentions any of the class
    /// names.
    /// Includes SelectTable instructions for column modifications.
    ///
    /// NOTE: The non-const version does not modify the index, but returns a
    /// Ranges object that may iterated over in a non-const fashion (such as by
    /// the OT merge algorithm).
    Ranges* get_schema_changes_for_class(StringData class_name);
    const Ranges* get_schema_changes_for_class(StringData class_name) const;
    //@}

    //@{
    /// Returns ranges for every instruction touching the objects.
    /// This includes schema changes for each object's class, and object
    /// modifications to other objects that link to these objects.
    ///
    /// NOTE: The non-const version does not modify the index, but returns a
    /// Ranges object that may iterated over in a non-const fashion (such as by
    /// the OT merge algorithm).
    Ranges* get_modifications_for_object(GlobalID id);
    const Ranges* get_modifications_for_object(GlobalID id) const;
    //@}

    //@{
    /// Returns the ranges for all instructions added to the index.
    ///
    /// NOTE: The non-const version does not modify the index, but returns a
    /// Ranges object that may iterated over in a non-const fashion (such as by
    /// the OT merge algorithm).
    Ranges* get_everything()
    {
        return &m_everything;
    }
    const Ranges* get_everything() const
    {
        return &m_everything;
    }
    //@}

    size_t get_num_conflict_groups() const noexcept
    {
        return m_num_conflict_groups;
    }

    struct RangeIterator;

    RangeIterator erase_instruction(RangeIterator);

#if REALM_DEBUG
    void print(std::ostream&) const;
    void verify() const;
    bool ranges_cover(const Ranges&, Changeset&, Changeset::const_iterator) const;
#endif // REALM_DEBUG

    // If ndx is inside or one-beyond the last range in `ranges`, that range is
    // expanded.  Otherwise, a new range is appended beginning at pos.
    static void add_instruction_at(Ranges&, Changeset&, Changeset::iterator pos);

private:
    struct ConflictGroup {
        Ranges ranges;
        std::map<StringData, std::vector<PrimaryKey>> objects;
        std::vector<StringData> schemas;
        size_t size = 0;
        std::list<ConflictGroup>::iterator self_it;
    };
    std::map<StringData, std::map<PrimaryKey, ConflictGroup*>> m_object_instructions;
    std::map<StringData, ConflictGroup*> m_schema_instructions;
    std::list<ConflictGroup> m_conflict_groups_owner;
    size_t m_num_conflict_groups = 0; // must be kept in sync with m_conflict_groups_owner

    void clear() noexcept;

    Ranges m_empty;
    Ranges m_everything;
    bool m_contains_destructive_schema_changes = false;

    ConflictGroup& schema_conflict_group(StringData class_name);
    ConflictGroup& object_conflict_group(const GlobalID& object_id);

    // Merge \a from into \a into, and delete \a from.
    void merge_conflict_groups(ConflictGroup& into, ConflictGroup& from);
};


/// Collapse and compact adjacent and overlapping ranges.
void compact_ranges(ChangesetIndex::Ranges& ranges, bool is_sorted = false);

bool is_schema_change(const sync::Instruction&) noexcept;
bool is_container_instruction(const sync::Instruction&) noexcept;

/// Extract any object references from the instruction and place them in the
/// buffer at \a ids, until \a max_ids references are found.
///
/// \returns the number of object IDs found. At the time of writing, this cannot
/// surpass 2.
size_t get_object_ids_in_instruction(const sync::Changeset&, const sync::Instruction&, ChangesetIndex::GlobalID* ids,
                                     size_t max_ids);

/// The RangeIterator is used to iterate over instructions in a set of ranges.
///
/// `Changeset::Ranges` is a list of ranges of instructions. This iterator hides
/// the indirection, and simply iterates over all the instructions covered by
/// the ranges provided to the constructor.
///
/// The `RangeIterator` is composed of the `ChangesetIndex::Ranges::iterator`
/// and a `Changeset::iterator`.
struct ChangesetIndex::RangeIterator {
    using pointer_type = sync::Instruction*;
    using reference_type = sync::Instruction*;

    RangeIterator() {}

    /// Create an iterator representing the beginning.
    explicit RangeIterator(ChangesetIndex::Ranges* ranges) noexcept
        : m_ranges(ranges)
        , m_outer(ranges->begin())
    {
        if (m_outer != ranges->end()) {
            m_inner = m_outer->second.begin();
            m_pos = m_inner->begin;
            REALM_ASSERT(m_pos != m_inner->end); // empty ranges not allowed!
            check();
        }
    }

    struct end_tag {};
    /// Create an iterator representing the end.
    RangeIterator(ChangesetIndex::Ranges* ranges, end_tag) noexcept
        : m_ranges(ranges)
        , m_outer(ranges->end())
    {
    }

    void check() const noexcept
    {
        REALM_ASSERT_DEBUG(m_ranges);
        REALM_ASSERT_DEBUG(m_outer != m_ranges->end());
        REALM_ASSERT_DEBUG(m_inner >= m_outer->second.begin());
        REALM_ASSERT_DEBUG(m_inner < m_outer->second.end());
        REALM_ASSERT_DEBUG(m_pos >= m_inner->begin);
        REALM_ASSERT_DEBUG(m_pos < m_inner->end);
        REALM_ASSERT_DEBUG(m_pos.m_inner >= m_outer->first->begin().m_inner);
        REALM_ASSERT_DEBUG(m_pos.m_inner < m_outer->first->end().m_inner);
    }

    /// Go to the next instruction in the range. If there are no more
    /// instructions in the range, go to the next ranges.
    RangeIterator& operator++() noexcept
    {
        REALM_ASSERT_DEBUG(m_outer != m_ranges->end());

        ++m_pos;
        if (REALM_UNLIKELY(m_pos == m_inner->end)) {
            // Slow path
            inc_inner();
        }
        return *this;
    }

    RangeIterator operator++(int) noexcept
    {
        auto copy = *this;
        ++(*this);
        return copy;
    }

    RangeIterator& operator+=(size_t diff) noexcept
    {
        for (size_t i = 0; i < diff; ++i) {
            ++(*this);
        }
        return *this;
    }

    inline reference_type operator*() const noexcept
    {
        check();
        return *m_pos;
    }

    pointer_type operator->() const noexcept
    {
        check();
        return m_pos.operator->();
    }

    bool operator==(const RangeIterator& other) const noexcept
    {
        REALM_ASSERT(m_ranges == other.m_ranges);
        if (m_outer == other.m_outer) {
            if (m_outer != m_ranges->end()) {
                if (m_inner == other.m_inner) {
                    return m_pos == other.m_pos;
                }
                return false;
            }
            return true;
        }
        return false;
    }

    bool operator!=(const RangeIterator& other) const noexcept
    {
        return !(*this == other);
    }

    // FIXME: Quadruply nested iterators. This is madness.
    ChangesetIndex::Ranges* m_ranges = nullptr;
    ChangesetIndex::Ranges::iterator m_outer;
    ChangesetIndex::Ranges::mapped_type::iterator m_inner;
    Changeset::iterator m_pos;

private:
    void inc_inner() noexcept
    {
        ++m_inner;
        if (m_inner == m_outer->second.end()) {
            ++m_outer;
            if (m_outer == m_ranges->end()) {
                *this = RangeIterator{m_ranges, end_tag{}};
                return; // avoid check()
            }
            m_inner = m_outer->second.begin();
        }
        m_pos = m_inner->begin;
        REALM_ASSERT(m_pos < m_inner->end); // empty ranges not allowed
        check();
    }
};


// Implementation:

inline bool is_schema_change(const sync::Instruction& instr) noexcept
{
    using Instruction = realm::sync::Instruction;
    return instr.get_if<Instruction::ObjectInstruction>() == nullptr;
}

inline bool is_container_instruction(const sync::Instruction& instr) noexcept
{
    using Instruction = realm::sync::Instruction;
    auto& v = instr.m_instr;

    return mpark::holds_alternative<Instruction::ArrayInsert>(v) ||
           mpark::holds_alternative<Instruction::ArrayMove>(v) ||
           mpark::holds_alternative<Instruction::ArrayErase>(v) || mpark::holds_alternative<Instruction::Clear>(v);
}

} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_CHANGESET_INDEX_HPP
