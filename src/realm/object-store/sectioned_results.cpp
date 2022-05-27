////////////////////////////////////////////////////////////////////////////
//
// Copyright 2022 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/object-store/results.hpp>
#include <realm/object-store/sectioned_results.hpp>
#include <realm/object-store/thread_safe_reference.hpp>

namespace realm {

static SectionedResults::SectionKeyFunc builtin_comparison(Results& results, Results::SectionedResultsOperator op,
                                                           util::Optional<StringData> prop_name)
{
    switch (op) {
        case Results::SectionedResultsOperator::FirstLetter:
            if (results.get_type() == PropertyType::Object) {
                auto col_key = results.get_table()->get_column_key(*prop_name);
                return [col_key](Mixed value, SharedRealm realm) {
                    auto link = value.get_link();
                    auto v = realm->read_group()
                                 .get_table(link.get_table_key())
                                 ->get_object(link.get_obj_key())
                                 .get<StringData>(col_key);
                    return v.size() > 0 ? v.prefix(1) : "";
                };
            }
            else {
                return [](Mixed value, SharedRealm) {
                    auto v = value.get_string();
                    return v.size() > 0 ? v.prefix(1) : "";
                };
            }
        default:
            throw std::logic_error("Builtin section algorithm not implemented.");
    }
}

struct SectionedResultsNotificationHandler {
public:
    // Cocoa requires specific information on when a section
    // needs to be added or removed;
    struct SectionChangeInfo {
        IndexSet sections_to_insert;
        IndexSet sections_to_delete;
    };

    SectionedResultsNotificationHandler(SectionedResults& sectioned_results, SectionedResultsNotificatonCallback cb,
                                        util::Optional<size_t> section_filter = util::none)
        : m_cb(std::move(cb))
        , m_sectioned_results(sectioned_results)
        , m_prev_sections(m_sectioned_results.m_sections)
        , m_prev_row_to_index_path(m_sectioned_results.m_row_to_index_path)
        , m_section_filter(section_filter)
    {
    }

    void before(CollectionChangeSet const&) {}
    void after(CollectionChangeSet const& c)
    {
        auto convert_indices = [&](const IndexSet& indices,
                                   const std::vector<std::pair<size_t, size_t>>& rows_to_index_path) {
            std::map<size_t, IndexSet> ret;
            for (auto index : indices.as_indexes()) {
                auto& index_path = rows_to_index_path[index];
                ret[index_path.first].add(index_path.second);
            }
            return ret;
        };

        m_sectioned_results.calculate_sections_if_required();
        auto insertions = convert_indices(c.insertions, m_sectioned_results.m_row_to_index_path);
        auto modifications = convert_indices(c.modifications_new, m_sectioned_results.m_row_to_index_path);
        auto deletions = convert_indices(c.deletions, m_prev_row_to_index_path);
        auto section_changes = calculate_sections_to_insert_and_delete(insertions, deletions, modifications);

        bool should_notify = true;
        if (m_section_filter) {
            bool has_insertions = insertions.count(*m_section_filter) != 0;
            bool has_modifications = modifications.count(*m_section_filter) != 0;
            bool has_deletions = deletions.count(*m_section_filter) != 0;
            should_notify = has_insertions || has_modifications || has_deletions;
        }
        if (should_notify) {
            m_cb(SectionedResultsChangeSet{insertions, modifications, deletions, section_changes.sections_to_insert,
                                           section_changes.sections_to_delete},
                 {});
        }

        REALM_ASSERT(m_sectioned_results.m_results.is_valid());

        m_prev_row_to_index_path = m_sectioned_results.m_row_to_index_path;
        m_prev_sections = m_sectioned_results.m_sections;
    }
    void error(std::exception_ptr ptr)
    {
        m_cb({}, ptr);
    }

    /*
     Produces an IndexSet of which section indexes have been removed and which section indexes
     are to be inserted.
     */
    SectionChangeInfo calculate_sections_to_insert_and_delete(std::map<size_t, IndexSet>& insertions,
                                                              std::map<size_t, IndexSet>& deletions,
                                                              std::map<size_t, IndexSet>& modifications)
    {
        IndexSet sections_to_insert, sections_to_remove;
        std::map<size_t, size_t> new_sections, old_sections;

        for (auto& n : m_sectioned_results.m_sections) {
            new_sections[n.hash] = n.index;
        }
        for (auto& n : m_prev_sections) {
            old_sections[n.hash] = n.index;
        }

        auto diff = [](const std::map<size_t, size_t>& a, const std::map<size_t, size_t>& b, IndexSet& out) {
            for (auto& [hash, index] : b) {
                if (a.find(hash) == a.end()) {
                    out.add(b.at(hash));
                }
            }
        };

        diff(old_sections, new_sections, sections_to_insert);
        diff(new_sections, old_sections, sections_to_remove);

        /*
         This struct allows us to do bookkeeping on the changesets that the collection change checker produced.
         The need for this is that sometimes the changechecker may report a modification when we expect an insertion
         or deletion and we need to compensate for any missing insertions or deletions. It also helps compensate for
         any missing insertions / deletions when moving elements from from section to another.

         Example:

         ------------
         |A:        |
         |   apples |
         |   arizona|
         ------------
         |M:        |
         |   melon  |
         |   motor  |

         Delete 'apples' which is [section: 0, index: 0]
         Delete 'arizona' which is [section: 0, index: 1]

         Insert 'banana' which will be [section: 0, index: 0]

         Here is what the change checker will produce:
         Insertions: [empty], Deletions: [[section: 0, index: 1]], Modifications: [[section: 0, index: 0]]

         While this is correct for a nonsectioned collection, this doesn't prove valid when updating a UI
         using sectioned results.

         What we actually need:
         Insertions: [[section: 0, index: 0]], Deletions: [[section: 0, index: 0], [section: 0, index: 1]],
         Modifications: [empty] The bookkeeper will modify the insertions, deletions & modifications IndexSet's to
         something that is logically correct for the SDK's UI.

         */
        struct ChangeBookkeeper {
            ChangeBookkeeper(size_t current_indice_count, size_t insertions_since_last_comparison,
                             size_t deletions_since_last_comparison, size_t change_insertion_count,
                             size_t change_deletion_count, bool is_new_section, bool is_deleted_section,
                             util::Optional<IndexSet&> insertions, IndexSet& deletions, IndexSet& modifications)
                : m_current_indice_count(current_indice_count)
                , m_insertions_since_last_comparison(insertions_since_last_comparison)
                , m_deletions_since_last_comparison(deletions_since_last_comparison)
                , m_change_insertion_count(change_insertion_count)
                , m_change_deletion_count(change_deletion_count)
                , m_is_new_section(is_new_section)
                , m_is_deleted_section(is_deleted_section)
                , m_insertions(insertions)
                , m_deletions(deletions)
                , m_modifications(modifications)
            {
            }

        public:
            void validate()
            {
                if (m_insertions)
                    validate(*m_insertions, m_is_new_section, m_insertions_since_last_comparison,
                             m_change_insertion_count);
                validate(m_deletions, m_is_deleted_section, m_deletions_since_last_comparison,
                         m_change_deletion_count);
                validate_modifications();
            }

        private:
            void validate(IndexSet& indexes, bool is_new_or_deleted_section, size_t& changes_since_last_comparison,
                          size_t& change_count)
            {
                if (changes_since_last_comparison == change_count && !is_new_or_deleted_section) {
                    // The change checker has produced the correct result.
                    // No further action is required.
                    return;
                }
                if (is_new_or_deleted_section) {
                    // If a section is new or recently deleted just
                    // insert or remove all indexes for the count of
                    // the objects in the section.
                    indexes.set(m_current_indice_count);
                    return;
                }
                // The change checker had issues producing the indexes we need.
                if (changes_since_last_comparison > change_count) {
                    /*
                     We are missing changes from insertions or deletions.
                     Example of why we might hit this scenario:
                     - We deleted a section and this current section will take the place
                       of those old objects. The changechecker will likely only report a modification
                       if the changes line up to replace the old section (due to sorting), if that happens we cannot
                     report modifications back to the SDK, we need to properly produce a changeset that removes the
                     modifications and in its place return insertions or deletions.
                     */
                    indexes.set(changes_since_last_comparison);
                    change_count = indexes.count();
                }
                else if (change_count > changes_since_last_comparison) {
                    /*
                     We are adding too many changes from insertions or deletions.
                     Dump the extra indicies to modifications instead.
                     Example of why we might hit this scenario:
                     A change can be reported in both insertions and deletions for the same section & index
                     ------------
                     |A:        |
                     |  apricot |
                     ------------
                     |B:        |
                     |  banana  |
                     ------------
                     |C:        |
                     |  calender|

                     Modify 'apricot' to 'zebra'
                     Modify 'banana' to 'any'
                     Modify 'calender' to 'goat'

                     Change check will produce:
                     deletions: [[section: 0, index: 0]],
                     insertions: [[section: 2, index: 0]],
                     modifications: [[section: 1, index: 0], [section: 2, index: 0]]

                     The issue here is that section 'A' will still exist after
                     this update and there is a deletion at [section: 0, index: 0]
                     with no compensating insertion.
                     As the actual count of objects will stay the same in the section
                     move the extra deletion to the modifications IndexSet instead.
                     */
                    IndexSet indexes_to_remove;
                    for (auto index : indexes.as_indexes()) {
                        m_modifications.add(index);
                        indexes_to_remove.add(index);
                        change_count--;
                        if (change_count == changes_since_last_comparison)
                            break;
                    }
                    indexes.remove(indexes_to_remove);
                }
                REALM_ASSERT(changes_since_last_comparison == change_count);
            }

            void validate_modifications()
            {
                // Cocoa will throw an error if an index is
                // deleted and reloaded in the same table view update block.
                for (auto m : m_deletions.as_indexes()) {
                    m_modifications.remove(m);
                }

                if (m_is_deleted_section)
                    return;

                for (auto m : m_insertions->as_indexes()) {
                    m_modifications.remove(m);
                }
            }

            size_t m_current_indice_count;
            size_t m_insertions_since_last_comparison;
            size_t m_deletions_since_last_comparison;
            size_t m_change_insertion_count;
            size_t m_change_deletion_count;

            bool m_is_new_section;
            bool m_is_deleted_section;

            util::Optional<IndexSet&> m_insertions;
            IndexSet& m_deletions;
            IndexSet& m_modifications;
        };

        for (auto& section : m_sectioned_results.m_sections) {
            auto it = old_sections.find(section.hash);
            if (it == old_sections.end()) {
                auto& i = insertions[section.index];
                auto& d = deletions[section.index];
                auto& m = modifications[section.index];

                // This is a new section
                auto c = ChangeBookkeeper(section.indices.size(), 0, 0, i.count(), 0, true, false, i, d, m);
                c.validate();
            }
            else {
                // This section previously existed
                size_t old_index = it->second;
                // WIN32 requires a cast to `uint64_t` otherwise `SIZE_MAX` will be the value
                // if the resulting subtraction yields a negative number.
                int64_t diff = (uint64_t)m_sectioned_results.m_sections[section.index].indices.size() -
                               (uint64_t)m_prev_sections[old_index].indices.size();

                auto& i = insertions[section.index];
                auto& d = deletions[old_index];
                auto& m = modifications[section.index];
                auto insertions_since_last_comparison = diff > 0 ? (size_t)diff : 0;
                auto deletions_since_last_comparison = diff < 0 ? (size_t)std::abs(diff) : 0;

                auto c =
                    ChangeBookkeeper(section.indices.size(), insertions_since_last_comparison,
                                     deletions_since_last_comparison, i.count(), d.count(), false, false, i, d, m);
                c.validate();
            }
        }

        // Validate changesets for deleted sections.
        for (auto old_index : sections_to_remove.as_indexes()) {
            auto& d = deletions[old_index];
            auto& m = modifications[old_index];

            auto c = ChangeBookkeeper(m_prev_sections[old_index].indices.size(), 0, 0, 0, d.count(), false, true,
                                      util::none, d, m);
            c.validate();
        }

        // Remove empty entries in each change set.
        auto remove_empty_changes = [](std::map<size_t, IndexSet>& change) {
            std::vector<size_t> indexes_to_remove;
            for (auto& [section, indexes] : change) {
                if (indexes.empty()) {
                    indexes_to_remove.push_back(section);
                }
            }
            for (auto i : indexes_to_remove) {
                change.erase(i);
            }
        };

        remove_empty_changes(insertions);
        remove_empty_changes(deletions);
        remove_empty_changes(modifications);

        return {sections_to_insert, sections_to_remove};
    }

private:
    SectionedResultsNotificatonCallback m_cb;
    SectionedResults& m_sectioned_results;
    std::vector<SectionRange> m_prev_sections;
    std::vector<std::pair<size_t, size_t>> m_prev_row_to_index_path;
    util::Optional<size_t> m_section_filter;
};

Mixed ResultsSection::operator[](size_t idx) const
{
    m_parent->calculate_sections_if_required();
    return m_parent->m_results.get_any(m_parent->m_sections[m_index].indices[idx]);
}

Mixed ResultsSection::key()
{
    return m_parent->m_sections[m_index].key;
}

template <typename Context>
auto ResultsSection::get(Context& ctx, size_t row_ndx)
{
    m_parent->calculate_sections_if_required();
    return m_parent->m_results.get(ctx, m_parent->m_sections[m_index].indices[row_ndx]);
}

size_t ResultsSection::size()
{
    m_parent->calculate_sections_if_required();
    REALM_ASSERT(m_parent->m_sections.size() > m_index);
    auto range = m_parent->m_sections[m_index];
    return range.indices.size();
}

NotificationToken ResultsSection::add_notification_callback(SectionedResultsNotificatonCallback callback,
                                                            KeyPathArray key_path_array) &
{
    return m_parent->add_notification_callback_for_section(m_index, std::move(callback), key_path_array);
}

SectionedResults::SectionedResults(Results results, SectionKeyFunc section_key_func)
    : m_results(results)
    , m_callback(std::move(section_key_func))
{
    calculate_sections_if_required(true);
}

SectionedResults::SectionedResults(Results results, Results::SectionedResultsOperator op,
                                   util::Optional<StringData> prop_name)
    : m_results(results)
    , m_callback(builtin_comparison(results, op, prop_name))
{
    calculate_sections_if_required(true);
}

void SectionedResults::calculate_sections_if_required(bool force_update)
{
    if (m_results.m_update_policy == Results::UpdatePolicy::Never)
        return;
    else if (!m_results.has_changed() && !force_update)
        return;

    {
        util::CheckedUniqueLock lock(m_results.m_mutex);
        m_results.ensure_up_to_date();
    }

    calculate_sections();
}

// This method will run in the following scenarios:
// - SectionedResults is freshly created from Results
// - The underlying Table in the Results collection has changed
void SectionedResults::calculate_sections()
{
    auto r = m_results.snapshot();
    size_t size = r.size();
    m_sections.clear();
    m_row_to_index_path.clear();
    std::map<Mixed, size_t> key_to_section_index;
    m_row_to_index_path.resize(size);

    for (size_t i = 0; i < size; ++i) {
        Mixed key = m_callback(r.get_any(i), r.get_realm());
        // Disallow links as section keys. It would be uncommon to use them to begin with
        // and if the object acting as the key was deleted bad things would happen.
        if (!key.is_null() && key.get_type() == type_Link) {
            throw std::logic_error("Links are not supported as section keys.");
        }
        auto it = key_to_section_index.find(key);
        if (it == key_to_section_index.end()) {
            auto idx = m_sections.size();
            SectionRange section;
            section.key = key;
            section.index = idx;
            section.indices.push_back(i);
            section.hash = key.hash();
            m_sections.push_back(section);
            it = key_to_section_index.insert({key, m_sections.size() - 1}).first;
            m_row_to_index_path[i] = {it->second, section.indices.size() - 1};
        }
        else {
            auto& section = m_sections[it->second];
            section.indices.push_back(i);
            m_row_to_index_path[i] = {it->second, section.indices.size() - 1};
        }
    }
}

size_t SectionedResults::size()
{
    calculate_sections_if_required();
    return m_sections.size();
}

ResultsSection SectionedResults::operator[](size_t idx)
{
    return ResultsSection(this, idx);
}

NotificationToken SectionedResults::add_notification_callback(SectionedResultsNotificatonCallback callback,
                                                              KeyPathArray key_path_array) &
{
    return m_results.add_notification_callback(SectionedResultsNotificationHandler(*this, std::move(callback)),
                                               std::move(key_path_array));
}

NotificationToken SectionedResults::add_notification_callback_for_section(
    size_t section_index, SectionedResultsNotificatonCallback callback, KeyPathArray key_path_array)
{
    return m_results.add_notification_callback(
        SectionedResultsNotificationHandler(*this, std::move(callback), section_index), std::move(key_path_array));
}

realm::ThreadSafeReference SectionedResults::thread_safe_reference()
{
    return m_results;
}

SectionedResults SectionedResults::snapshot()
{
    calculate_sections_if_required();
    // m_callback will never be run when using a snapshot so we do
    // not need to set it.
    SectionedResults sr;
    sr.m_results = m_results.snapshot();
    sr.m_sections = m_sections;
    return sr;
}

bool SectionedResults::is_valid() const
{
    return m_results.is_valid();
}

} // namespace realm
