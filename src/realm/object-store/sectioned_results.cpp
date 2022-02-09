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

struct SectionedResultsNotificationHandler {
public:
    SectionedResultsNotificationHandler(SectionedResults& sectioned_results, SectionedResults::CBFunc cb)
    : m_sectioned_results(sectioned_results)
    , m_cb(std::move(cb))
    {
    }

    void before(CollectionChangeSet const& c) {}
    void after(CollectionChangeSet const& c)
    {
        m_sectioned_results.calculate_sections_if_required();
        std::map<size_t, std::set<size_t>> insertions = convert_indicies(c.insertions.as_indexes());
        std::map<size_t, std::set<size_t>> modifications = convert_indicies(c.modifications.as_indexes());
        std::map<size_t, std::set<size_t>> deletions = convert_indicies(c.deletions.as_indexes());
        m_cb(SectionedResultsChangeSet { insertions, modifications, deletions }, {});
    }
    void error(std::exception_ptr ptr)
    {
        m_cb({}, ptr);
    }

    std::map<size_t, std::set<size_t>> convert_indicies(IndexSet::IndexIteratableAdaptor indicies) {
        std::map<size_t, std::set<size_t>> modified_sections = std::map<size_t, std::set<size_t>>();
        for (auto i : indicies) {
            auto range = m_sectioned_results.section_for_index(i);
            modified_sections[range.index].insert(i - range.begin);
        }
        return modified_sections;
    }
private:
    SectionedResults::CBFunc m_cb;
    SectionedResults& m_sectioned_results;
};

Mixed ResultsSection::operator[](size_t idx) const
{
    m_parent->calculate_sections_if_required();
    return m_parent->m_results.get_any(m_parent->m_offset_ranges[m_index].begin + idx);
}

template <typename Context>
auto ResultsSection::get(Context& ctx, size_t row_ndx)
{
    return this->m_parent->m_results.get(ctx, m_parent->m_offset_ranges[m_index].begin + row_ndx);
}

size_t ResultsSection::size()
{
    m_parent->calculate_sections_if_required();
    auto range = m_parent->m_offset_ranges[m_index];
    return (range.end + 1) - range.begin;
}

void SectionedResults::calculate_sections() {
    m_offset_ranges = {};
    const size_t size = m_results.size();
    auto indicies = std::vector<size_t>();
    size_t current_section = 0;
    for (size_t i = 0; i < size; ++i) {
        if (i + 1 == size) {
            m_offset_ranges.back().end = i;
            break;
        }
        if (i == 0) {
            m_offset_ranges.push_back({current_section, 0, 0});
            current_section++;
        }
        m_offset_ranges.back().end = i;

        if (!m_callback(m_results.get_any(i), m_results.get_any(i + 1))) {
            m_offset_ranges.push_back({current_section, i + 1, 0});
            current_section++;
        }
    }
}

void SectionedResults::calculate_sections_if_required(Results::EvaluateMode mode) {
    if (!m_results.ensure_up_to_date(mode)) {
        calculate_sections();
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

SectionRange SectionedResults::section_for_index(size_t index) {
    auto is_match = [index](SectionRange& section) { return index >= section.begin && index <= section.end; };
    auto result = std::find_if(m_offset_ranges.begin(), m_offset_ranges.end(), is_match);
    return *result;
}

NotificationToken SectionedResults::add_notification_callback(util::UniqueFunction<void(SectionedResultsChangeSet, std::exception_ptr)> callback,
                                                              KeyPathArray key_path_array) &
{
    return m_results.add_notification_callback(SectionedResultsNotificationHandler(*this, std::move(callback)), key_path_array);
}

} // namespace realm
