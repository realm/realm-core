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
                    auto v = realm->read_group().get_object(link).get<StringData>(col_key);
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
    SectionedResultsNotificationHandler(SectionedResults& sectioned_results, SectionedResultsNotificatonCallback cb,
                                        util::Optional<size_t> section_filter = util::none)
        : m_cb(std::move(cb))
        , m_sectioned_results(sectioned_results)
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

        auto converted_insertions = convert_indices(c.insertions, m_sectioned_results.m_row_to_index_path);
        auto converted_modifications = convert_indices(c.modifications, m_prev_row_to_index_path);
        auto converted_modifications_new =
            convert_indices(c.modifications_new, m_sectioned_results.m_row_to_index_path);
        auto converted_deletions = convert_indices(c.deletions, m_prev_row_to_index_path);

        std::map<size_t, IndexSet> modifications_to_keep, modifications_to_keep_new;
        auto section_changes = calculate_sections_to_insert_and_delete();

        for (auto [section_old, indexes_old] : converted_modifications) {
            auto it = m_sectioned_results.m_prev_section_index_to_key.find(section_old);
            if (it == m_sectioned_results.m_prev_section_index_to_key.end()) {
                throw std::logic_error("Section Key for given index does not exist.");
            }
            auto section_identifier = m_sectioned_results.m_section_key_to_index_lookup.find(it->second);
            // This section still exists.
            if (section_identifier != m_sectioned_results.m_section_key_to_index_lookup.end()) {
                auto old_indexes = indexes_old.as_indexes();
                auto new_indexes = converted_modifications_new[section_identifier->second].as_indexes();
                std::vector<size_t> out_indexes;
                std::set_intersection(old_indexes.begin(), old_indexes.end(), new_indexes.begin(), new_indexes.end(),
                                      std::back_inserter(out_indexes));
                auto& old_modifications = converted_modifications[section_old];
                // These are the indexes which are still in the
                // same position as they were in the old collection.
                for (auto& i : out_indexes) {
                    modifications_to_keep[section_old].add(i);
                    modifications_to_keep_new[section_identifier->second].add(i);
                    // Anything remaining in converted_modifications will be added to deletions.
                    old_modifications.remove(i);
                    // Anything remaining in converted_modifications_new will be added to insertions.
                    converted_modifications_new[section_identifier->second].remove(i);
                }
                if (!old_modifications.empty())
                    converted_deletions[section_old].add(converted_modifications[section_old]);
            }
        }

        for (auto [section, indexes] : converted_modifications_new) {
            if (!indexes.empty())
                converted_insertions[section].add(indexes);
        }

        // Cocoa only requires the index of the deleted sections to remove all deleted rows.
        // There is no need to pass back each individual deletion IndexPath.
        for (auto section : section_changes.second.as_indexes()) {
            converted_deletions.erase(section);
        }

        converted_modifications = modifications_to_keep;
        converted_modifications_new = modifications_to_keep_new;

        if (m_section_filter) {
            bool should_notify = true;

            std::map<size_t, IndexSet> filtered_insertions;
            std::map<size_t, IndexSet> filtered_modifications;
            std::map<size_t, IndexSet> filtered_deletions;

            bool has_insertions = converted_insertions.count(*m_section_filter) != 0;
            if (has_insertions) {
                filtered_insertions[*m_section_filter] = converted_insertions[*m_section_filter];
            }
            bool has_modifications = converted_modifications.count(*m_section_filter) != 0;
            if (has_modifications) {
                filtered_modifications[*m_section_filter] = converted_modifications[*m_section_filter];
            }

            bool has_deletions = converted_deletions.count(*m_section_filter) != 0;
            if (has_deletions) {
                filtered_deletions[*m_section_filter] = converted_deletions[*m_section_filter];
            }

            IndexSet filtered_sections_to_insert;
            IndexSet filtered_sections_to_delete;

            if (section_changes.first.contains(*m_section_filter))
                filtered_sections_to_insert.add(*m_section_filter);
            if (section_changes.second.contains(*m_section_filter))
                filtered_sections_to_delete.add(*m_section_filter);

            should_notify = has_insertions || has_modifications || has_deletions ||
                            !filtered_sections_to_insert.empty() || !filtered_sections_to_delete.empty();

            if (should_notify) {
                m_cb(SectionedResultsChangeSet{filtered_insertions, filtered_modifications, filtered_deletions,
                                               filtered_sections_to_insert, filtered_sections_to_delete},
                     {});
            }
        }
        else {
            m_cb(SectionedResultsChangeSet{converted_insertions, converted_modifications, converted_deletions,
                                           section_changes.first, section_changes.second},
                 {});
        }

        REALM_ASSERT(m_sectioned_results.m_results.is_valid());
        m_prev_row_to_index_path = m_sectioned_results.m_row_to_index_path;
    }
    void error(std::exception_ptr ptr)
    {
        m_cb({}, ptr);
    }

    std::pair<IndexSet, IndexSet> calculate_sections_to_insert_and_delete()
    {
        IndexSet sections_to_insert, sections_to_remove;
        auto diff = [](const std::map<Mixed, size_t>& a, const std::map<Mixed, size_t>& b, IndexSet& out) {
            for (auto& [key, index] : b) {
                if (a.find(key) == a.end()) {
                    out.add(index);
                }
            }
        };

        diff(m_sectioned_results.m_prev_sections, m_sectioned_results.m_section_key_to_index_lookup,
             sections_to_insert);
        diff(m_sectioned_results.m_section_key_to_index_lookup, m_sectioned_results.m_prev_sections,
             sections_to_remove);
        return {sections_to_insert, sections_to_remove};
    }

private:
    SectionedResultsNotificatonCallback m_cb;
    SectionedResults& m_sectioned_results;
    std::vector<std::pair<size_t, size_t>> m_prev_row_to_index_path;
    util::Optional<size_t> m_section_filter;
};

Mixed ResultsSection::operator[](size_t idx) const
{
    m_parent->calculate_sections_if_required();
    REALM_ASSERT(idx < m_parent->m_sections[m_index].indices.size());
    return m_parent->m_results.get_any(m_parent->m_sections[m_index].indices[idx]);
}

Mixed ResultsSection::key()
{
    return m_parent->m_sections[m_index].key;
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

realm::ThreadSafeReference ResultsSection::thread_safe_reference()
{
    return m_parent->thread_safe_reference();
}

bool ResultsSection::is_valid() const
{
    return m_parent->is_valid();
}

SectionedResults::SectionedResults(Results results, SectionKeyFunc section_key_func)
    : m_results(results)
    , m_callback(std::move(section_key_func))
{
}

SectionedResults::SectionedResults(Results results, Results::SectionedResultsOperator op,
                                   util::Optional<StringData> prop_name)
    : m_results(results)
    , m_callback(builtin_comparison(results, op, prop_name))
{
}

void SectionedResults::calculate_sections_if_required()
{
    if (m_results.m_update_policy == Results::UpdatePolicy::Never)
        return;
    else if (!m_results.has_changed() && has_performed_initial_evalutation)
        return;

    {
        util::CheckedUniqueLock lock(m_results.m_mutex);
        m_results.ensure_up_to_date();
    }

    calculate_sections();
    if (!has_performed_initial_evalutation)
        has_performed_initial_evalutation = true;
}

template <typename StringType>
std::unique_ptr<util::Optional<std::string>> create_buffered_key(Mixed& key, StringType value)
{
    if (value.size() == 0) {
        return std::make_unique<util::Optional<std::string>>(util::none);
    }
    auto v = util::Optional<std::string>(std::string(value.data(), value.size()));
    auto ptr = std::make_unique<util::Optional<std::string>>(v);
    key = **ptr;
    return ptr;
}

// This method will run in the following scenarios:
// - SectionedResults is performing its initial evaluation.
// - The underlying Table in the Results collection has changed
void SectionedResults::calculate_sections()
{
    m_prev_sections.clear();
    m_prev_section_index_to_key.clear();
    for (auto& s : m_sections) {
        m_prev_sections[s.key] = s.index;
        m_prev_section_index_to_key[s.index] = s.key;
    }

    size_t size = m_results.size();
    m_sections.clear();
    m_section_key_to_index_lookup.clear();
    m_row_to_index_path.clear();
    std::map<Mixed, size_t> key_to_section_index;
    m_row_to_index_path.resize(size);
    bool uses_buffer = false;
    size_t next_buffer_size = 0;

    for (size_t i = 0; i < size; ++i) {
        Mixed key = m_callback(m_results.get_any(i), m_results.get_realm());
        // Disallow links as section keys. It would be uncommon to use them to begin with
        // and if the object acting as the key was deleted bad things would happen.
        if (!key.is_null() && key.get_type() == type_Link) {
            throw std::logic_error("Links are not supported as section keys.");
        }

        auto it = key_to_section_index.find(key);
        if (it == key_to_section_index.end()) {
            util::Optional<std::unique_ptr<util::Optional<std::string>>> buffered_key;
            if (!key.is_null() && (key.get_type() == type_String || key.get_type() == type_Binary)) {
                auto ptr = (key.get_type() == type_String) ? create_buffered_key(key, key.get_string())
                                                           : create_buffered_key(key, key.get_binary());
                buffered_key = std::move(ptr);
                uses_buffer = true;
            }

            auto idx = m_sections.size();
            Section section;
            section.key = key;
            section.index = idx;
            section.indices.push_back(i);
            m_sections.push_back(section);
            it = key_to_section_index.insert({key, m_sections.size() - 1}).first;
            m_row_to_index_path[i] = {it->second, section.indices.size() - 1};
            m_section_key_to_index_lookup[key] = idx;

            if (uses_buffer && buffered_key) {
                m_str_data_buffers.push_back(std::move(*buffered_key));
                next_buffer_size++;
            }
        }
        else {
            auto& section = m_sections[it->second];
            section.indices.push_back(i);
            m_row_to_index_path[i] = {it->second, section.indices.size() - 1};
        }
    }
    if (uses_buffer) {
        auto buffers_to_purge = m_str_data_buffers.size() - (m_prev_str_data_buffer_size + next_buffer_size);
        m_str_data_buffers.erase(m_str_data_buffers.begin(), m_str_data_buffers.begin() + buffers_to_purge);
        m_prev_str_data_buffer_size = next_buffer_size;
    }
}

size_t SectionedResults::size()
{
    calculate_sections_if_required();
    return m_sections.size();
}

ResultsSection SectionedResults::operator[](size_t idx)
{
    REALM_ASSERT(idx < size());
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
    sr.has_performed_initial_evalutation = true;
    return sr;
}

bool SectionedResults::is_valid() const
{
    return m_results.is_valid();
}

bool SectionedResults::is_frozen() const
{
    return m_results.is_frozen();
}

} // namespace realm
