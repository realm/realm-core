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

#include <realm/object-store/sectioned_results.hpp>

#include <realm/exceptions.hpp>

namespace realm {

static SectionedResults::SectionKeyFunc builtin_comparison(Results& results, Results::SectionedResultsOperator op,
                                                           StringData prop_name)
{
    switch (op) {
        case Results::SectionedResultsOperator::FirstLetter:
            if (results.get_type() == PropertyType::Object) {
                auto col_key = results.get_table()->get_column_key(prop_name);
                return [col_key](Mixed value, const SharedRealm& realm) {
                    auto link = value.get_link();
                    auto v = realm->read_group().get_object(link).get<StringData>(col_key);
                    return v.size() > 0 ? v.prefix(1) : "";
                };
            }
            else {
                return [](Mixed value, const SharedRealm&) {
                    auto v = value.get_string();
                    return v.size() > 0 ? v.prefix(1) : "";
                };
            }
        default:
            throw LogicError(ErrorCodes::IllegalOperation, "Builtin section algorithm not implemented.");
    }
}

namespace {
template <typename T>
T& at(std::vector<T>& vec, size_t index)
{
    if (index >= vec.size()) {
        if (vec.capacity() <= index)
            vec.reserve(index * 2 + 1);
        vec.resize(index + 1);
    }
    return vec[index];
}

struct IndexSetAdder {
    IndexSet* set;
    IndexSetAdder& operator=(size_t value)
    {
        set->add(value);
        return *this;
    }
    IndexSetAdder& operator++()
    {
        return *this;
    }
    IndexSetAdder& operator++(int)
    {
        return *this;
    }
    IndexSetAdder& operator*()
    {
        return *this;
    }
};
} // anonymous namespace

struct SectionedResultsNotificationHandler {
public:
    SectionedResultsNotificationHandler(SectionedResults& sectioned_results,
                                        SectionedResultsNotificationCallback&& cb,
                                        util::Optional<Mixed> section_filter = util::none)
        : m_cb(std::move(cb))
        , m_sectioned_results(sectioned_results)
        , m_prev_row_to_index_path(m_sectioned_results.m_row_to_index_path)
        , m_section_filter(section_filter)
    {
    }

    void operator()(CollectionChangeSet const& c)
    {
        util::CheckedUniqueLock lock(m_sectioned_results.m_mutex);

        m_sectioned_results.calculate_sections_if_required();
        section_initial_changes(c);
        m_prev_row_to_index_path = m_sectioned_results.m_row_to_index_path;

        // Add source to target[i], expanding target if needed
        auto add = [](auto& source, auto& target, size_t i) {
            if (source.empty())
                return;
            if (i >= target.size())
                target.resize(i + 1);
            target[i].add(source);
        };

        // Modifications to rows in the unsectioned results may result in rows
        // moving between sections, which need to be reported as a delete+insert
        // instead. We don't have enough information at this point to produce a
        // correct minimal diff, so we err on the side of producing deletes and
        // inserts for everything that isn't marked as modified in both the old
        // and new versions.

        // Looping backwards here ensures that we have to resize the output
        // arrays at most once, as we encounter the back element that needs to
        // be present first.
        for (size_t i = m_change.modifications.size(); i > 0; --i) {
            auto& indexes_old = m_change.modifications[i - 1];
            auto key = m_sectioned_results.m_previous_index_to_key[i - 1];
            auto it = m_sectioned_results.m_current_key_to_index.find(key);
            if (it == m_sectioned_results.m_current_key_to_index.end()) {
                // Section was removed due to all of the rows being moved to
                // other sections. No need to report the individual rows as deleted.
                indexes_old.clear();
                continue;
            }
            size_t old_section = i - 1;
            size_t new_section = it->second;

            // Extract the intersection of the two sets
            IndexSet still_present;
            if (new_section < m_new_modifications.size()) {
                auto old_indexes = indexes_old.as_indexes();
                auto new_indexes = m_new_modifications[new_section].as_indexes();
                std::set_intersection(old_indexes.begin(), old_indexes.end(), new_indexes.begin(), new_indexes.end(),
                                      IndexSetAdder{&still_present});
                m_new_modifications[new_section].remove(still_present);
                m_change.modifications[old_section].remove(still_present);
            }

            // Anything in old modifications but not new gets added to deletions
            add(m_change.modifications[old_section], m_change.deletions, old_section);

            // Any positions marked as modified in both the old and new collections
            // stay marked as modified.
            m_change.modifications[old_section] = std::move(still_present);
        }

        // Anything remaining in new_modifications is now an insertion. This is
        // once again a reverse loop to ensure we only resize once.
        for (size_t i = m_new_modifications.size(); i > 0; --i)
            add(m_new_modifications[i - 1], m_change.insertions, i - 1);

        // The modifications array may now be longer than needed. This isn't
        // strictly needed but makes writing tests awkward.
        while (!m_change.modifications.empty() && m_change.modifications.back().empty())
            m_change.modifications.pop_back();

        // If we have a section filter we might have been called when there were
        // no changes to the section we care about, in which case we should skip
        // calling the callback unless it's the initial notification
        if (m_section_filter) {
            bool no_changes = m_change.insertions.empty() && m_change.deletions.empty() &&
                              m_change.modifications.empty() && m_change.sections_to_insert.empty() &&
                              m_change.sections_to_delete.empty();
            if (m_section_filter_should_deliver_initial_notification)
                m_section_filter_should_deliver_initial_notification = false;
            else if (no_changes)
                return;
        }

        m_cb(m_change);
    }

private:
    SectionedResultsNotificationCallback m_cb;
    SectionedResults& m_sectioned_results;
    std::vector<std::pair<size_t, size_t>> m_prev_row_to_index_path;
    SectionedResultsChangeSet m_change;
    std::vector<IndexSet> m_new_modifications;
    // When set change notifications will be filtered to only deliver
    // change indices referring to the supplied section key.
    util::Optional<Mixed> m_section_filter;
    bool m_section_filter_should_deliver_initial_notification = true;

    // Group the changes in the changeset by the section
    void section_initial_changes(CollectionChangeSet const& c) REQUIRES(m_sectioned_results.m_mutex)
    {
        m_change.insertions.clear();
        m_change.modifications.clear();
        m_change.deletions.clear();
        m_change.sections_to_insert.clear();
        m_change.sections_to_delete.clear();
        m_new_modifications.clear();

        // If we have a section filter, just check if it was added or removed
        // and for changes within that specific section
        if (m_section_filter) {
            auto get_index = [&](auto& map) -> size_t {
                if (auto it = map.find(*m_section_filter); it != map.end())
                    return it->second;
                return npos;
            };

            size_t old_index = get_index(m_sectioned_results.m_previous_key_to_index);
            size_t new_index = get_index(m_sectioned_results.m_current_key_to_index);
            if (old_index == npos && new_index == npos)
                return;
            if (old_index == npos && new_index != npos)
                m_change.sections_to_insert.add(new_index);
            else if (old_index != npos && new_index == npos)
                m_change.sections_to_delete.add(old_index);

            auto populate = [](auto& src, auto& mapping, size_t filter, auto& dst) {
                for (auto index : src.as_indexes()) {
                    auto [section, row] = mapping[index];
                    if (section == filter)
                        at(dst, section).add(row);
                }
            };
            populate(c.insertions, m_sectioned_results.m_row_to_index_path, new_index, m_change.insertions);
            populate(c.modifications, m_prev_row_to_index_path, old_index, m_change.modifications);
            populate(c.modifications_new, m_sectioned_results.m_row_to_index_path, new_index, m_new_modifications);

            // Only report deletions inside the section if it still exists
            if (new_index != npos) {
                populate(c.deletions, m_prev_row_to_index_path, old_index, m_change.deletions);
            }
            return;
        }

        // Symmetrical diff of new and old sections
        for (auto& section : m_sectioned_results.m_sections) {
            if (!m_sectioned_results.m_previous_key_to_index.count(section.key)) {
                m_change.sections_to_insert.add(section.index);
            }
        }
        for (auto& [key, index] : m_sectioned_results.m_previous_key_to_index) {
            if (!m_sectioned_results.m_current_key_to_index.count(key)) {
                m_change.sections_to_delete.add(index);
            }
        }

        // Group the change indexes by section
        for (auto index : c.insertions.as_indexes()) {
            auto [section, row] = m_sectioned_results.m_row_to_index_path[index];
            at(m_change.insertions, section).add(row);
        }
        for (auto index : c.modifications.as_indexes()) {
            auto [section, row] = m_prev_row_to_index_path[index];
            at(m_change.modifications, section).add(row);
        }
        for (auto index : c.modifications_new.as_indexes()) {
            auto [section, row] = m_sectioned_results.m_row_to_index_path[index];
            at(m_new_modifications, section).add(row);
        }
        for (auto index : c.deletions.as_indexes()) {
            auto [section, row] = m_prev_row_to_index_path[index];
            // If the section has been deleted that's the only information we
            // need and we can skip reporting the rows inside the section
            if (!m_change.sections_to_delete.contains(section))
                at(m_change.deletions, section).add(row);
        }
    }
};

namespace {
template <typename StringType>
void create_buffered_key(Mixed& key, std::unique_ptr<char[]>& buffer, StringType value)
{
    if (value.size() == 0) {
        key = StringType("", 0);
    }
    else {
        buffer = std::make_unique<char[]>(value.size());
        std::strncpy(buffer.get(), value.data(), value.size());
        key = StringType(buffer.get(), value.size());
    }
}

template <typename StringType>
void create_buffered_key(Mixed& key, std::list<std::string>& buffer, StringType value)
{
    if (value.size() == 0) {
        key = StringType("", 0);
    }
    else {
        key = StringType(buffer.emplace_back(value.data(), value.size()).data(), value.size());
    }
}

template <typename Buffer>
void create_buffered_key(Mixed& key, Buffer& buffer)
{
    if (key.is_null())
        return;
    if (key.is_type(type_String))
        create_buffered_key(key, buffer, key.get_string());
    else if (key.is_type(type_Binary))
        create_buffered_key(key, buffer, key.get_binary());
}

} // anonymous namespace

ResultsSection::ResultsSection(SectionedResults* parent, Mixed key)
    : m_parent(parent)
    , m_key(key)
{
    // Give the ResultsSection its own copy of the string data
    // to counter the event that the m_previous_str_buffers, m_current_str_buffers
    // no longer hold a reference to the data.
    create_buffered_key(m_key, m_key_buffer);
}

bool ResultsSection::is_valid() const
{
    return get_if_valid();
}

Section* ResultsSection::get_if_valid() const
{
    if (!m_parent->is_valid())
        return nullptr;
    util::CheckedUniqueLock lock(m_parent->m_mutex);
    // See if we need to recalculate the sections before
    // searching for the key.
    m_parent->calculate_sections_if_required();
    auto it = m_parent->m_current_key_to_index.find(m_key);
    if (it == m_parent->m_current_key_to_index.end())
        return nullptr;
    return &m_parent->m_sections[it->second];
}

Section* ResultsSection::get_section() const
{
    Section* section = get_if_valid();
    if (!section)
        throw StaleAccessor("Access to invalidated Results objects");
    return section;
}

Mixed ResultsSection::operator[](size_t idx) const
{
    Section* section = get_section();
    auto size = section->indices.size();
    if (idx >= size)
        throw OutOfBounds("ResultsSection[]", idx, size);
    return m_parent->m_results.get_any(section->indices[idx]);
}

Mixed ResultsSection::key()
{
    if (!is_valid())
        throw StaleAccessor("Access to invalidated Results objects");
    return m_key;
}

size_t ResultsSection::index()
{
    return get_section()->index;
}

size_t ResultsSection::size()
{
    return get_section()->indices.size();
}

NotificationToken ResultsSection::add_notification_callback(SectionedResultsNotificationCallback&& callback,
                                                            std::optional<KeyPathArray> key_path_array) &
{
    return m_parent->add_notification_callback_for_section(m_key, std::move(callback), key_path_array);
}

SectionedResults::SectionedResults(Results results, SectionKeyFunc section_key_func)
    : m_results(results)
    , m_callback(std::move(section_key_func))
{
}

SectionedResults::SectionedResults(Results results, Results::SectionedResultsOperator op, StringData prop_name)
    : m_results(results)
    , m_callback(builtin_comparison(results, op, prop_name))
{
}

void SectionedResults::calculate_sections_if_required()
{
    if (m_results.m_update_policy == Results::UpdatePolicy::Never)
        return;
    if ((m_results.is_frozen() || !m_results.has_changed()) && m_has_performed_initial_evaluation)
        return;

    {
        util::CheckedUniqueLock lock(m_results.m_mutex);
        m_results.ensure_up_to_date();
    }

    calculate_sections();
}

// This method will run in the following scenarios:
// - SectionedResults is performing its initial evaluation.
// - The underlying Table in the Results collection has changed
void SectionedResults::calculate_sections()
{
    m_previous_str_buffers.clear();
    m_previous_str_buffers.swap(m_current_str_buffers);
    m_previous_key_to_index.clear();
    m_previous_key_to_index.swap(m_current_key_to_index);
    m_previous_index_to_key.clear();
    for (auto& section : m_sections) {
        m_previous_index_to_key.push_back(section.key);
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
            throw InvalidArgument("Links are not supported as section keys.");
        }

        auto it = m_current_key_to_index.find(key);
        if (it == m_current_key_to_index.end()) {
            create_buffered_key(key, m_current_str_buffers);
            auto idx = m_sections.size();
            m_sections.push_back(Section{idx, key, {i}});
            m_current_key_to_index[key] = idx;
            m_row_to_index_path[i] = {idx, 0};
        }
        else {
            auto& section = m_sections[it->second];
            section.indices.push_back(i);
            m_row_to_index_path[i] = {section.index, section.indices.size() - 1};
        }
    }
    if (!m_has_performed_initial_evaluation) {
        REALM_ASSERT_EX(m_previous_key_to_index.size() == 0, m_previous_key_to_index.size());
        REALM_ASSERT_EX(m_previous_index_to_key.size() == 0, m_previous_index_to_key.size());
        m_previous_key_to_index = m_current_key_to_index;
        for (auto& section : m_sections) {
            m_previous_index_to_key.push_back(section.key);
        }
    }
    m_has_performed_initial_evaluation = true;
}

size_t SectionedResults::size()
{
    util::CheckedUniqueLock lock(m_mutex);
    check_valid();
    calculate_sections_if_required();
    return m_sections.size();
}

ResultsSection SectionedResults::operator[](size_t idx)
{
    auto s = size();
    if (idx >= s)
        throw OutOfBounds("SectionedResults[]", idx, s);
    util::CheckedUniqueLock lock(m_mutex);
    auto& section = m_sections[idx];
    return ResultsSection(this, section.key);
}

ResultsSection SectionedResults::operator[](Mixed key)
{
    util::CheckedUniqueLock lock(m_mutex);
    check_valid();
    calculate_sections_if_required();
    if (!m_current_key_to_index.count(key)) {
        throw InvalidArgument(util::format("Section key %1 not found.", key));
    }
    return ResultsSection(this, key);
}

NotificationToken SectionedResults::add_notification_callback(SectionedResultsNotificationCallback&& callback,
                                                              std::optional<KeyPathArray> key_path_array) &
{
    return m_results.add_notification_callback(SectionedResultsNotificationHandler(*this, std::move(callback)),
                                               std::move(key_path_array));
}

NotificationToken SectionedResults::add_notification_callback_for_section(
    Mixed section_key, SectionedResultsNotificationCallback&& callback, std::optional<KeyPathArray> key_path_array)
{
    return m_results.add_notification_callback(
        SectionedResultsNotificationHandler(*this, std::move(callback), section_key), std::move(key_path_array));
}

// Thread-safety analysis doesn't work when creating a different instance of the
// same type
SectionedResults SectionedResults::copy(Results&& results) NO_THREAD_SAFETY_ANALYSIS
{
    util::CheckedUniqueLock lock(m_mutex);
    calculate_sections_if_required();
    // m_callback will never be run when using frozen results so we do
    // not need to set it.
    SectionedResults ret;
    ret.m_has_performed_initial_evaluation = true;
    ret.m_results = std::move(results);
    ret.m_sections = m_sections;
    for (auto& section : ret.m_sections) {
        create_buffered_key(section.key, ret.m_current_str_buffers);
        ret.m_current_key_to_index[section.key] = section.index;
    }
    return ret;
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

void SectionedResults::check_valid() const
{
    m_results.validate_read();
}

bool SectionedResults::is_frozen() const
{
    return m_results.is_frozen();
}

void SectionedResults::reset_section_callback(SectionKeyFunc section_callback)
{
    util::CheckedUniqueLock lock(m_mutex);
    m_callback = std::move(section_callback);
    m_has_performed_initial_evaluation = false;
    m_sections.clear();
    m_previous_index_to_key.clear();
    m_current_key_to_index.clear();
    m_previous_key_to_index.clear();
    m_row_to_index_path.clear();
}
} // namespace realm
