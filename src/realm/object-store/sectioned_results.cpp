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

#include <set>

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
    // needs to be added, removed, or reloaded when data changes
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
        auto modifications = convert_indices(c.modifications, m_sectioned_results.m_row_to_index_path);
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

    SectionChangeInfo calculate_sections_to_insert_and_delete(std::map<size_t, IndexSet>& insertions,
                                                              std::map<size_t, IndexSet>& deletions,
                                                              std::map<size_t, IndexSet>& modifications)
    {
        std::map<size_t, size_t> new_section_indexes;
        std::map<size_t, size_t> old_section_indexes;

        std::vector<size_t> new_section_hashes, old_section_hashes;
        for (auto& n : m_sectioned_results.m_sections) {
            new_section_indexes[n.hash] = n.index;
            new_section_hashes.push_back(n.hash);
        }
        for (auto& n : m_prev_sections) {
            old_section_indexes[n.hash] = n.index;
            old_section_hashes.push_back(n.hash);
        }

        IndexSet sections_to_insert;
        IndexSet sections_to_remove;

        auto diff = [](const std::vector<size_t>& a, const std::vector<size_t>& b,
                       std::map<size_t, size_t>& hash_lookup, IndexSet& out) {
            for (auto& hash : b) {
                if (std::find(a.begin(), a.end(), hash) == a.end()) {
                    out.add(hash_lookup[hash]);
                }
            }
        };

        diff(old_section_hashes, new_section_hashes, new_section_indexes, sections_to_insert);
        diff(new_section_hashes, old_section_hashes, old_section_indexes, sections_to_remove);

        /*
         This struct allows us to do bookkeeping on the changesets that the collection change checker produced.
         The need for this is that sometimes the changechecker may report a modification when we expect an insertion
         or deletion and we need to compensate for any missing insertions or deletions. It also helps compensate for
         any missing insertions / deletions when moving elements from from section to another.
         */
        struct Changes {
            size_t index;
            size_t current_indice_count;

            size_t insertions_since_last_comparison;
            size_t deletions_since_last_comparison;

            size_t change_insertion_count;
            size_t change_deletion_count;
            size_t change_modification_count;

            bool is_new_section;
            bool is_deleted_section;

            util::Optional<IndexSet&> insertions;
            util::Optional<IndexSet&> deletions;
            util::Optional<IndexSet&> modifications;

            void validate_insertions()
            {
                if (insertions_since_last_comparison == change_insertion_count && !is_new_section) {
                    // All looks good
                    return;
                }

                if (is_new_section) {
                    insertions->set(current_indice_count);
                    change_insertion_count = insertions->count();
                    REALM_ASSERT(current_indice_count == change_insertion_count);
                    return;
                }

                // The change checker had issues producing the insertions we need.
                if (insertions_since_last_comparison > change_insertion_count) {
                    // We are missing insertions
                    // The issue with this approach is that we
                    // lose precision on what indexes we actually need to insert
                    insertions->set(insertions_since_last_comparison);
                    change_insertion_count = insertions->count();
                }
                else if (change_insertion_count > insertions_since_last_comparison) {
                    // We are adding too many insertions
                    // dump the extra indicies to modifications instead
                    // ensure we dont go out of range
                    IndexSet indexes_to_remove;
                    for (auto index : insertions->as_indexes()) {
                        modifications->add(index);
                        indexes_to_remove.add(index);
                        change_insertion_count--;
                        if (change_insertion_count == insertions_since_last_comparison)
                            break;
                    }
                    insertions->remove(indexes_to_remove);
                }
                REALM_ASSERT(change_insertion_count == insertions->count());
            }

            void validate_deletions()
            {
                if (deletions_since_last_comparison == change_deletion_count && !is_deleted_section) {
                    // All looks good
                    return;
                }
                if (is_deleted_section) {
                    deletions->set(current_indice_count);
                    change_deletion_count = deletions->count();
                    REALM_ASSERT(current_indice_count == change_deletion_count);
                    return;
                }
                // The change checker had issues producing the deletions we need.
                if (deletions_since_last_comparison > change_deletion_count) {
                    // We are missing deletions
                    // Can we use anything from modifications?
                    deletions->set(deletions_since_last_comparison);
                    change_deletion_count = deletions->count();
                }
                else if (change_deletion_count > deletions_since_last_comparison) {
                    // We are adding too many deletions
                    // dump the extra indicies to modifications instead
                    // ensure we dont go out of range
                    IndexSet indexes_to_remove;
                    for (auto index : deletions->as_indexes()) {
                        modifications->add(index);
                        indexes_to_remove.add(index);
                        change_deletion_count--;
                        if (change_deletion_count == deletions_since_last_comparison)
                            break;
                    }
                    deletions->remove(indexes_to_remove);
                }
                REALM_ASSERT(deletions_since_last_comparison == change_deletion_count);
            }

            void validate_modifications()
            {
                if (is_deleted_section)
                    return;
                for (auto m : deletions->as_indexes()) {
                    modifications->remove(m);
                }

                for (auto m : insertions->as_indexes()) {
                    modifications->remove(m);
                }
            }

            void validate()
            {
                validate_insertions();
                validate_deletions();
                validate_modifications();
            }
        };

        for (auto& section : m_sectioned_results.m_sections) {
            auto it = find(old_section_hashes.begin(), old_section_hashes.end(), section.hash);
            if (it == old_section_hashes.end()) {
                auto& i = insertions[section.index];
                auto& d = deletions[section.index];
                auto& m = modifications[section.index];

                // This is a new section
                Changes c = {.index = section.index,
                             .current_indice_count = section.indices.size(),
                             .insertions_since_last_comparison = 0,
                             .deletions_since_last_comparison = 0,
                             .change_insertion_count = i.count(),
                             .change_deletion_count = 0,
                             .change_modification_count = m.count(),
                             .is_new_section = true,
                             .is_deleted_section = false,
                             .insertions = i,
                             .deletions = d,
                             .modifications = m};
                c.validate();
            }
            else {
                // This section previously existed
                size_t old_index = it - old_section_hashes.begin();
                int64_t diff = m_sectioned_results.m_sections[section.index].indices.size() -
                               m_prev_sections[old_index].indices.size();

                auto& i = insertions[section.index];
                auto& d = deletions[old_index];
                auto& m = modifications[section.index];

                Changes c = {.index = section.index,
                             .current_indice_count = section.indices.size(),
                             .insertions_since_last_comparison = diff > 0 ? (size_t)diff : 0,
                             .deletions_since_last_comparison = diff < 0 ? (size_t)abs(diff) : 0,
                             .change_insertion_count = i.count(),
                             .change_deletion_count = d.count(),
                             .change_modification_count = m.count(),
                             .is_new_section = false,
                             .is_deleted_section = false,
                             .insertions = i,
                             .deletions = d,
                             .modifications = m};
                c.validate();
            }
        }

        for (auto old_index : sections_to_remove.as_indexes()) {
            auto& d = deletions[old_index];
            Changes c = {.index = old_index,
                         .current_indice_count = m_prev_sections[old_index].indices.size(),
                         .insertions_since_last_comparison = 0,
                         .deletions_since_last_comparison = 0,
                         .change_insertion_count = 0,
                         .change_deletion_count = d.count(),
                         .change_modification_count = 0,
                         .is_new_section = false,
                         .is_deleted_section = true,
                         .insertions = util::none,
                         .deletions = d,
                         .modifications = util::none};
            c.validate();
        }

        // Cocoa will throw an error if an index is
        // deleted and reloaded in the same table view update block
        for (auto& [section, indexes] : deletions) {
            modifications[section].remove(indexes);
            if (modifications[section].empty()) {
                modifications.erase(section);
            }
        }

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

    auto desc = r.get_descriptor_ordering();
    if (desc.will_apply_sort()) {
        const SortDescriptor* sort_desc = static_cast<const SortDescriptor*>(desc[0]);
        auto is_asc = sort_desc->is_ascending(0);
        if (is_asc) {
            bool is_ascending = *is_asc;
            std::sort(m_sections.begin(), m_sections.end(),
                      [&is_ascending](const SectionRange& a, const SectionRange& b) {
                          return is_ascending ? (a.key < b.key) : (a.key > b.key);
                      });
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
                                               key_path_array);
}

NotificationToken SectionedResults::add_notification_callback_for_section(
    size_t section_index, SectionedResultsNotificatonCallback callback, KeyPathArray key_path_array)
{
    return m_results.add_notification_callback(
        SectionedResultsNotificationHandler(*this, std::move(callback), section_index), key_path_array);
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

} // namespace realm
