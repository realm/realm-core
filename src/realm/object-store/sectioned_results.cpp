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
                                        util::Optional<Mixed> section_filter = util::none)
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

        util::CheckedUniqueLock lock(m_sectioned_results.m_mutex);

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
            REALM_ASSERT(it != m_sectioned_results.m_prev_section_index_to_key.end());
            auto section_identifier = m_sectioned_results.m_sections.find(it->second);
            // This section still exists.
            if (section_identifier != m_sectioned_results.m_sections.end()) {
                auto old_indexes = indexes_old.as_indexes();
                auto new_indexes = converted_modifications_new[section_identifier->second.index].as_indexes();
                std::vector<size_t> out_indexes;
                std::set_intersection(old_indexes.begin(), old_indexes.end(), new_indexes.begin(), new_indexes.end(),
                                      std::back_inserter(out_indexes));
                auto& old_modifications = converted_modifications[section_old];
                // These are the indexes which are still in the
                // same position as they were in the old collection.
                for (auto& i : out_indexes) {
                    modifications_to_keep[section_old].add(i);
                    modifications_to_keep_new[section_identifier->second.index].add(i);
                    // Anything remaining in converted_modifications will be added to deletions.
                    old_modifications.remove(i);
                    // Anything remaining in converted_modifications_new will be added to insertions.
                    converted_modifications_new[section_identifier->second.index].remove(i);
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
            std::map<size_t, IndexSet> filtered_insertions, filtered_modifications, filtered_deletions;

            auto current_section = m_sectioned_results.m_sections.find(*m_section_filter);

            auto previous_index_of_section_key =
                m_sectioned_results.m_previous_key_to_index_lookup.find(*m_section_filter);
            auto current_key_exists = current_section != m_sectioned_results.m_sections.end();
            auto previous_key_exists =
                previous_index_of_section_key != m_sectioned_results.m_previous_key_to_index_lookup.end();

            bool has_insertions =
                current_key_exists && converted_insertions.count(current_section->second.index) != 0;
            if (has_insertions) {
                filtered_insertions[current_section->second.index] =
                    converted_insertions[current_section->second.index];
            }
            bool has_modifications =
                previous_key_exists && converted_modifications.count(previous_index_of_section_key->second) != 0;
            if (has_modifications) {
                filtered_modifications[previous_index_of_section_key->second] =
                    converted_modifications[previous_index_of_section_key->second];
            }

            bool has_deletions =
                previous_key_exists && converted_deletions.count(previous_index_of_section_key->second) != 0;
            if (has_deletions) {
                filtered_deletions[previous_index_of_section_key->second] =
                    converted_deletions[previous_index_of_section_key->second];
            }

            IndexSet filtered_sections_to_insert, filtered_sections_to_delete;

            if (current_key_exists) {
                if (section_changes.first.contains(current_section->second.index))
                    filtered_sections_to_insert.add(current_section->second.index);
            }

            if (previous_key_exists) {
                if (section_changes.second.contains(previous_index_of_section_key->second))
                    filtered_sections_to_delete.add(previous_index_of_section_key->second);
            }

            bool should_notify = has_insertions || has_modifications || has_deletions ||
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

    std::pair<IndexSet, IndexSet> calculate_sections_to_insert_and_delete() REQUIRES(m_sectioned_results.m_mutex)
    {
        IndexSet sections_to_insert, sections_to_remove;

        for (auto& [key, section] : m_sectioned_results.m_sections) {
            if (m_sectioned_results.m_previous_key_to_index_lookup.find(key) ==
                m_sectioned_results.m_previous_key_to_index_lookup.end()) {
                sections_to_insert.add(section.index);
            }
        }

        for (auto& [key, index] : m_sectioned_results.m_previous_key_to_index_lookup) {
            if (m_sectioned_results.m_sections.find(key) == m_sectioned_results.m_sections.end()) {
                sections_to_remove.add(index);
            }
        }

        return {sections_to_insert, sections_to_remove};
    }

private:
    SectionedResultsNotificatonCallback m_cb;
    SectionedResults& m_sectioned_results;
    std::vector<std::pair<size_t, size_t>> m_prev_row_to_index_path;
    // When set change notifications will be filtered to only deliver
    // change indices refering to the supplied section key.
    util::Optional<Mixed> m_section_filter;
};

template <typename StringType>
void create_buffered_key(Mixed& key, util::Optional<std::string>& buffer, StringType value)
{
    if (value.size() == 0) {
        key = StringType("", 0);
    }
    else {
        buffer = std::string(value.data(), value.size());
        key = Mixed(buffer);
    }
}

ResultsSection::ResultsSection()
    : m_parent(nullptr)
    , m_key(Mixed())
    , m_key_buffer(util::none)
{
}

ResultsSection& ResultsSection::operator=(ResultsSection&& other)
{
    std::swap(m_parent, other.m_parent);
    if (other.m_key_buffer) {
        std::swap(m_key_buffer, other.m_key_buffer);
        if (other.m_key.is_type(type_String)) {
            m_key = StringData(m_key_buffer->data(), m_key_buffer->size());
        }
        else if (other.m_key.is_type(type_String)) {
            m_key = BinaryData(m_key_buffer->data(), m_key_buffer->size());
        }
        else {
            REALM_UNREACHABLE();
        }
    }
    else {
        std::swap(m_key, other.m_key);
    }
    return *this;
}

ResultsSection::ResultsSection(SectionedResults* parent, Mixed key)
    : m_parent(parent)
{
    // Give the ResultsSection its own copy of the string data
    // to counter the event that the m_previous_str_buffers, m_current_str_buffers
    // no longer hold a reference to the data.
    if (key.is_type(type_String, type_Binary)) {
        key.is_type(type_String) ? create_buffered_key(m_key, m_key_buffer, key.get_string())
                                 : create_buffered_key(m_key, m_key_buffer, key.get_binary());
    }
    else {
        m_key = key;
    }
}

bool ResultsSection::is_valid() const
{
    util::CheckedUniqueLock lock(m_parent->m_mutex);
    m_parent->calculate_sections_if_required();
    auto it = m_parent->m_sections.find(m_key);
    return it != m_parent->m_sections.end() && m_parent->is_valid();
}

Mixed ResultsSection::operator[](size_t idx) const
{
    util::CheckedUniqueLock lock(m_parent->m_mutex);
    m_parent->calculate_sections_if_required();
    auto it = m_parent->m_sections.find(m_key);
    REALM_ASSERT(it != m_parent->m_sections.end());
    REALM_ASSERT(m_parent->is_valid() && idx < it->second.indices.size());
    return m_parent->m_results.get_any(it->second.indices[idx]);
}

Mixed ResultsSection::key()
{
    REALM_ASSERT(is_valid());
    return m_key;
}

size_t ResultsSection::index()
{
    util::CheckedUniqueLock lock(m_parent->m_mutex);
    m_parent->calculate_sections_if_required();
    auto it = m_parent->m_sections.find(m_key);
    REALM_ASSERT(m_parent->is_valid() && it != m_parent->m_sections.end());
    return it->second.index;
}

size_t ResultsSection::size()
{
    util::CheckedUniqueLock lock(m_parent->m_mutex);
    m_parent->calculate_sections_if_required();
    auto it = m_parent->m_sections.find(m_key);
    REALM_ASSERT(m_parent->is_valid() && it != m_parent->m_sections.end());
    return it->second.indices.size();
}

NotificationToken ResultsSection::add_notification_callback(SectionedResultsNotificatonCallback callback,
                                                            KeyPathArray key_path_array) &
{
    return m_parent->add_notification_callback_for_section(m_key, std::move(callback), key_path_array);
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
    else if ((!m_results.has_changed() || m_results.is_frozen()) && has_performed_initial_evalutation)
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
void create_buffered_key(Mixed& key, std::list<std::string>& buffer, StringType value)
{
    if (value.size() == 0) {
        key = StringType("", 0);
    }
    else {
        key = buffer.emplace_back(value.data(), value.size());
    }
}

// This method will run in the following scenarios:
// - SectionedResults is performing its initial evaluation.
// - The underlying Table in the Results collection has changed
void SectionedResults::calculate_sections()
{
    m_previous_str_buffers.clear();
    m_previous_str_buffers.swap(m_current_str_buffers);
    m_previous_key_to_index_lookup.clear();
    m_prev_section_index_to_key.clear();
    m_current_section_index_to_key_lookup.clear();
    for (auto& [key, section] : m_sections) {
        m_previous_key_to_index_lookup[key] = section.index;
        m_prev_section_index_to_key[section.index] = section.key;
    }

    m_sections.clear();
    m_row_to_index_path.clear();
    size_t size = m_results.size();
    m_row_to_index_path.resize(size);

    for (size_t i = 0; i < size; ++i) {
        Mixed key = m_callback(m_results.get_any(i), m_results.get_realm());
        // Disallow links as section keys. It would be uncommon to use them to begin with
        // and if the object acting as the key was deleted bad things would happen.
        if (key.is_type(type_Link, type_TypedLink)) {
            throw std::logic_error("Links are not supported as section keys.");
        }

        auto it = m_sections.find(key);
        if (it == m_sections.end()) {
            if (!key.is_null() && key.is_type(type_String, type_Binary)) {
                (key.get_type() == type_String) ? create_buffered_key(key, m_current_str_buffers, key.get_string())
                                                : create_buffered_key(key, m_current_str_buffers, key.get_binary());
            }

            auto idx = m_sections.size();
            Section section;
            section.key = key;
            section.index = idx;
            section.indices.push_back(i);
            m_sections[key] = section;
            m_row_to_index_path[i] = {idx, section.indices.size() - 1};
            m_current_section_index_to_key_lookup[idx] = key;
        }
        else {
            auto& section = it->second;
            section.indices.push_back(i);
            m_row_to_index_path[i] = {section.index, section.indices.size() - 1};
        }
    }
}

size_t SectionedResults::size()
{
    util::CheckedUniqueLock lock(m_mutex);
    REALM_ASSERT(is_valid());
    calculate_sections_if_required();
    return m_sections.size();
}

ResultsSection SectionedResults::operator[](size_t idx)
{
    auto s = size();
    REALM_ASSERT(idx < s);
    util::CheckedUniqueLock lock(m_mutex);
    auto it = m_current_section_index_to_key_lookup.find(idx);
    REALM_ASSERT(it != m_current_section_index_to_key_lookup.end());
    auto& section = m_sections[it->second];
    return ResultsSection(this, section.key);
}

ResultsSection SectionedResults::operator[](Mixed key)
{
    util::CheckedUniqueLock lock(m_mutex);
    REALM_ASSERT(is_valid());
    calculate_sections_if_required();
    auto it = m_sections.find(key);
    if (it == m_sections.end()) {
        throw std::logic_error("Key does not exist for any sections.");
    }
    return ResultsSection(this, it->second.key);
}

NotificationToken SectionedResults::add_notification_callback(SectionedResultsNotificatonCallback callback,
                                                              KeyPathArray key_path_array) &
{
    return m_results.add_notification_callback(SectionedResultsNotificationHandler(*this, std::move(callback)),
                                               std::move(key_path_array));
}

NotificationToken SectionedResults::add_notification_callback_for_section(
    Mixed section_key, SectionedResultsNotificatonCallback callback, KeyPathArray key_path_array)
{
    return m_results.add_notification_callback(
        SectionedResultsNotificationHandler(*this, std::move(callback), section_key), std::move(key_path_array));
}

SectionedResults SectionedResults::copy(Results&& results)
{
    util::CheckedUniqueLock lock(m_mutex);
    calculate_sections_if_required();
    // m_callback will never be run when using frozen results so we do
    // not need to set it.
    std::list<std::string> str_buffers;
    std::map<Mixed, Section> sections;
    std::unordered_map<size_t, Mixed> current_section_index_to_key_lookup;

    for (auto& [key, section] : m_sections) {
        Mixed new_key;
        if (key.is_type(type_String, type_Binary)) {
            key.is_type(type_String) ? create_buffered_key(new_key, str_buffers, key.get_string())
                                     : create_buffered_key(new_key, str_buffers, key.get_binary());
        }
        else {
            new_key = key;
        }
        Section new_section;
        new_section.index = section.index;
        new_section.key = new_key;
        new_section.indices = section.indices;
        sections[Mixed(new_key)] = new_section;
        current_section_index_to_key_lookup[section.index] = new_key;
    }
    return SectionedResults(std::move(results), std::move(sections), std::move(current_section_index_to_key_lookup),
                            std::move(str_buffers));
}

SectionedResults SectionedResults::snapshot()
{
    return copy(m_results.snapshot());
}

SectionedResults SectionedResults::freeze(std::shared_ptr<Realm> const& frozen_realm)
{
    return copy(m_results.freeze(frozen_realm));
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
