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

#ifndef SECTIONED_RESULTS_HPP
#define SECTIONED_RESULTS_HPP

#include <realm/util/functional.hpp>

namespace realm {
class Mixed;
class Results;
class SectionedResults;
struct SectionRange;
struct SectionedResultsChangeSet;

using SectionedResultsNotificatonCallback = util::UniqueFunction<void(SectionedResultsChangeSet, std::exception_ptr)>;

/**
 * An instance of ResultsSection gives access to elements in the underlying collection that belong to a given section.
 *
 * A ResultsSection is only valid as long as it's `SectionedResults` parent stays alive.
 *
 * You can register a notification callback for changes to elements in a specific `ResultsSection`.
 */
class ResultsSection {
public:
    ResultsSection() = default;

    /// Retrieve an element from the section for a given index.
    Mixed operator[](size_t idx) const;

    template <typename Context>
    auto get(Context&, size_t index);

    /// The key identifying this section.
    Mixed key();

    /// The total count of elements in this section.
    size_t size();

    /**
     * Create an async query from this ResultsSection
     * The query will be run on a background thread and delivered to the callback,
     * and then rerun after each commit (if needed) and redelivered if it changed
     *
     * @param callback The function to execute when a insertions, modification or deletion in this `ResultsSection`
     * was detected.
     * @param key_path_array A filter that can be applied to make sure the `SectionedResultsNotificatonCallback` is
     * only executed when the property in the filter is changed but not otherwise.
     *
     * @return A `NotificationToken` that is used to identify this callback. This token can be used to remove the
     * callback via `remove_callback`.
     */
    NotificationToken add_notification_callback(SectionedResultsNotificatonCallback callback,
                                                KeyPathArray key_path_array = {}) &;

private:
    friend class SectionedResults;
    ResultsSection(SectionedResults* parent, size_t index)
        : m_parent(parent)
        , m_index(index)
    {
    }

    SectionedResults* m_parent;
    size_t m_index;
};

class SectionedResults {
public:
    SectionedResults() = default;

    using ComparisonFunc = util::UniqueFunction<Mixed(Mixed value, SharedRealm realm)>;

    ResultsSection operator[](size_t idx);
    /// The total amount of Sections.
    size_t size();

    /**
     * Create an async query from this SectionedResults
     * The query will be run on a background thread and delivered to the callback,
     * and then rerun after each commit (if needed) and redelivered if it changed
     *
     * @param callback The function to execute when a insertions, modification or deletion in this `SectionedResults`
     * was detected.
     * @param key_path_array A filter that can be applied to make sure the `SectionedResultsNotificatonCallback` is
     * only executed when the property in the filter is changed but not otherwise.
     *
     * @return A `NotificationToken` that is used to identify this callback. This token can be used to remove the
     * callback via `remove_callback`.
     */
    NotificationToken add_notification_callback(SectionedResultsNotificatonCallback callback,
                                                KeyPathArray key_path_array = {}) &;

    realm::ThreadSafeReference thread_safe_reference();

private:
    friend class Results;
    /// SectionedResults should not be created directly and should only be instantiated from `Results`.
    SectionedResults(Results results, ComparisonFunc comparison_func);
    SectionedResults(Results results, Results::SectionedResultsOperator op, util::Optional<StringData> prop_name);
    uint_fast64_t get_content_version();

    friend struct SectionedResultsNotificationHandler;
    void calculate_sections_if_required();

    NotificationToken add_notification_callback_for_section(size_t section_index,
                                                            SectionedResultsNotificatonCallback callback,
                                                            KeyPathArray key_path_array = {});

    friend class realm::ResultsSection;
    Results m_results;
    std::vector<SectionRange> m_offset_ranges;
    ComparisonFunc m_callback;
    uint_fast64_t m_previous_content_version;
};

struct SectionedResultsChangeSet {
    // Sections and indices in the _new_ collection which are new insertions
    std::map<size_t, std::vector<size_t>> insertions;
    // Sections and indices of objects in the _old_ collection which were modified
    std::map<size_t, std::vector<size_t>> modifications;
    // Sections and indices which were removed from the _old_ collection
    std::map<size_t, std::vector<size_t>> deletions;
};

/// For internal use only. Used to track the indicies for a given section.
struct SectionRange {
    size_t index;
    Mixed key;
    std::vector<size_t> indices;
};

} // namespace realm


#endif /* SECTIONED_RESULTS_HPP */
