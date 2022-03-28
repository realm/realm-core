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

static std::vector<SectionRange> calculate_sections(Results& results,
                                                    const SectionedResults::ComparisonFunc& callback) {

    auto sections = std::map<Mixed, SectionRange>();
    auto offset_ranges = std::vector<SectionRange>();
    // Take a snapshot in case the underlying results change while
    // the calculation is being performed.
    auto snapshot = results.snapshot();
    const size_t size = snapshot.size();
    size_t current_section = 0;
    for (size_t i = 0; i < size; ++i) {
        auto section_key = callback(snapshot.get_any(i));
        if (sections.find(section_key) == sections.end()) {
            SectionRange section;
            section.index = current_section;
            section.key = section_key;
            section.indices.push_back(i);
            sections[section_key] = section;
            current_section++;
        } else {
            sections[section_key].indices.push_back(i);
        }
    }

    transform(sections.begin(),
              sections.end(),
              back_inserter(offset_ranges), [](const std::map<Mixed, SectionRange>::value_type& val) { return val.second; });
    return offset_ranges;
}

static SectionedResults::ComparisonFunc builtin_comparison(Results& results,
                                                           util::Optional<StringData> prop_name,
                                                           Results::SectionedResultsOperator op)
{
    switch (op) {
        case Results::SectionedResultsOperator::FirstLetter:
            if (results.get_type() == PropertyType::Object) {
                return [r = results.get_realm(), p = *prop_name](Mixed value) {
                    auto obj = Object(r, value.get_link());
                    auto v = obj.get_column_value<StringData>(p);
                    return v.size() > 1 ? v.prefix(1) : "";
                };
            } else {
                return [r = results.get_realm()](Mixed value) {
                    auto v = value.get_string();
                    return v.size() > 1 ? v.prefix(1) : "";
                };
            }
    }
}

static SectionRange section_for_index(std::vector<SectionRange> offsets, size_t index) {
    for (auto offset : offsets) {
        auto is_match = [index](size_t idx) { return index == idx; };
        auto result = std::find_if(offset.indices.begin(), offset.indices.end(), is_match);
        if (std::find(offset.indices.begin(), offset.indices.end(), index) != offset.indices.end())
        {
            return offset;
        }
    }
    throw std::logic_error("Section for given index not found.");
}

struct SectionedResultsNotificationHandler {
public:
    SectionedResultsNotificationHandler(SectionedResults& sectioned_results,
                                        SectionedResultsNotificatonCallback cb,
                                        util::Optional<size_t> section_filter = util::none)
    : m_sectioned_results(sectioned_results)
    , m_cb(std::move(cb))
    , m_prev_rt(static_cast<Transaction*>(sectioned_results.m_results.get_table()->get_parent_group())->duplicate())
    , m_section_filter(section_filter)
    {
        auto table_view = m_sectioned_results.m_results.get_tableview();
        std::unique_ptr<TableView> prev_table_view = m_prev_rt->import_copy_of(table_view, PayloadPolicy::Copy);
        m_prev_results = Results(m_sectioned_results.m_results.get_realm(), *prev_table_view);
    }

    void before(CollectionChangeSet const& c) {}
    void after(CollectionChangeSet const& c)
    {
        std::map<size_t, std::vector<size_t>> deletions = {};
        if (!c.deletions.empty()) {
            auto offsets = calculate_sections(m_prev_results, m_sectioned_results.m_callback);
            deletions = convert_indicies(offsets, c.deletions.as_indexes());
        }

        m_sectioned_results.calculate_sections_if_required();
        auto insertions = convert_indicies(m_sectioned_results.m_offset_ranges, c.insertions.as_indexes());
        auto modifications = convert_indicies(m_sectioned_results.m_offset_ranges, c.modifications.as_indexes());

        bool should_notify = true;
        if (m_section_filter) {
            bool has_insertions = insertions.count(*m_section_filter) != 0;
            bool has_modifications = modifications.count(*m_section_filter) != 0;
            bool has_deletions = deletions.count(*m_section_filter) != 0;
            should_notify = has_insertions || has_modifications || has_deletions;
        }
        if (should_notify) {
            m_cb(SectionedResultsChangeSet {
                insertions,
                modifications,
                deletions
            }, {});
        }

        auto current_tr = static_cast<Transaction*>(m_sectioned_results.m_results.get_table()->get_parent_group());
        m_prev_rt->advance_read(current_tr->get_version_of_current_transaction());
    }
    void error(std::exception_ptr ptr)
    {
        m_cb({}, ptr);
    }

    std::map<size_t, std::vector<size_t>> convert_indicies(std::vector<SectionRange>& offsets,
                                                           IndexSet::IndexIteratableAdaptor indicies) {
        auto modified_sections = std::map<size_t, std::vector<size_t>>();
        for (auto i : indicies) {
            auto range = section_for_index(offsets, i);
            auto it = std::find(range.indices.begin(), range.indices.end(), i);
            if (it != range.indices.end()) {
                auto index = std::distance(range.indices.begin(), it);
                modified_sections[range.index].push_back(index);
            }
        }
        return modified_sections;
    }
private:
    SectionedResultsNotificatonCallback m_cb;
    SectionedResults& m_sectioned_results;

    TransactionRef m_prev_rt;
    Results m_prev_results;
    util::Optional<size_t> m_section_filter;
};

Mixed ResultsSection::operator[](size_t idx) const
{
    m_parent->calculate_sections_if_required();
    return m_parent->m_results.get_any(m_parent->m_offset_ranges[m_index].indices[idx]);
}

Mixed ResultsSection::key()
{
    return m_parent->m_offset_ranges[m_index].key;
}

template <typename Context>
auto ResultsSection::get(Context& ctx, size_t row_ndx)
{
    return this->m_parent->m_results.get(ctx, m_parent->m_offset_ranges[m_index].indices[row_ndx]);
}

size_t ResultsSection::size()
{
    m_parent->calculate_sections_if_required();
    auto range = m_parent->m_offset_ranges[m_index];
    return range.indices.size();
}

NotificationToken ResultsSection::add_notification_callback(SectionedResultsNotificatonCallback callback,
                                                            KeyPathArray key_path_array) &
{
    return m_parent->add_notification_callback_for_section(m_index, std::move(callback), key_path_array);
}

SectionedResults::SectionedResults(Results results,
                                   ComparisonFunc comparison_func):
m_results(results),
m_callback(std::move(comparison_func)) {
    m_offset_ranges = calculate_sections(m_results, m_callback);
};

SectionedResults::SectionedResults(Results results,
                                   util::Optional<StringData> prop_name,
                                   Results::SectionedResultsOperator op):
m_results(results),
m_callback(builtin_comparison(results, prop_name, op)) {
    m_offset_ranges = calculate_sections(m_results, m_callback);
};

void SectionedResults::calculate_sections_if_required(Results::EvaluateMode mode) {
    if (!m_results.ensure_up_to_date(mode)) {
        m_offset_ranges = calculate_sections(m_results, m_callback);
    }
}

size_t SectionedResults::size()
{
    calculate_sections_if_required();
    return m_offset_ranges.size();
}

ResultsSection SectionedResults::operator[](size_t idx) {
    return ResultsSection(this, idx);
}

NotificationToken SectionedResults::add_notification_callback(SectionedResultsNotificatonCallback callback,
                                                              KeyPathArray key_path_array) &
{
    return m_results.add_notification_callback(SectionedResultsNotificationHandler(*this, std::move(callback)), key_path_array);
}

NotificationToken SectionedResults::add_notification_callback_for_section(size_t section_index,
                                                                          SectionedResultsNotificatonCallback callback,
                                                                          KeyPathArray key_path_array)
{
    return m_results.add_notification_callback(SectionedResultsNotificationHandler(*this, std::move(callback), section_index), key_path_array);
}

} // namespace realm
