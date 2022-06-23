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

#ifndef REALM_SECTIONED_RESULTS_HPP
#define REALM_SECTIONED_RESULTS_HPP

#include <realm/util/functional.hpp>

namespace realm {
class Mixed;
class Results;
class SectionedResults;
struct Section;
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

    realm::ThreadSafeReference thread_safe_reference();
    bool is_valid() const;

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

    using SectionKeyFunc = util::UniqueFunction<Mixed(Mixed value, SharedRealm realm)>;

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
    /// Return a new instance SectionedResults that uses a snapshot of the underlying `Results`.
    /// The section key callback parameter will never be invoked.
    SectionedResults snapshot();

    bool is_valid() const;
    bool is_frozen() const;

private:
    friend class Results;
    /// SectionedResults should not be created directly and should only be instantiated from `Results`.
    SectionedResults(Results results, SectionKeyFunc section_key_func);
    SectionedResults(Results results, Results::SectionedResultsOperator op, util::Optional<StringData> prop_name);
    uint_fast64_t get_content_version();

    friend struct SectionedResultsNotificationHandler;
    void calculate_sections_if_required();
    void calculate_sections();
    bool has_performed_initial_evalutation = false;
    NotificationToken add_notification_callback_for_section(size_t section_index,
                                                            SectionedResultsNotificatonCallback callback,
                                                            KeyPathArray key_path_array = {});

    friend class realm::ResultsSection;
    Results m_results;
    SectionKeyFunc m_callback;
    std::vector<Section> m_sections;
    // Stores the Key, Section Index of the previous section
    // so we can efficiently calculate the collection change set.
    std::map<Mixed, size_t> m_prev_sections;
    // Returns the index of the previous section from its key.
    std::map<size_t, Mixed> m_prev_section_index_to_key;
    // Returns the index of a section from its key.
    std::map<Mixed, size_t> m_section_key_to_index_lookup;

    // Key: Original index in Results, Value: <section_index, index_in_section>
    // Pass the index of the object from the underlying `Results`,
    // this will give a pair with the section index of the object, and the position of the object in that section.
    // This is used for parsing the indices in CollectionChangeSet to section indices.
    std::vector<std::pair<size_t, size_t>> m_row_to_index_path;
    // Binary representable types require a buffer to hold deep
    // copies of the values for the lifetime of the sectioned results.
    // This is due to the fact that such values can reference the memory address of the value in the realm.
    // We can not rely on that because it would not produce stable keys.
    // So we perform a deep copy to produce stable key values that will not change if the realm is modified.
    // The buffer will purge keys that are no longer used in the case that the `calculate_sections` method runs.
    std::deque<std::unique_ptr<util::Optional<std::string>>> m_str_data_buffers;
    size_t m_prev_str_data_buffer_size = 0;
};

struct SectionedResultsChangeSet {
    /// Sections and indices in the _new_ collection which are new insertions
    std::map<size_t, IndexSet> insertions;
    /// Sections and indices of objects in the _old_ collection which were modified
    std::map<size_t, IndexSet> modifications;
    /// Sections and indices which were removed from the _old_ collection
    std::map<size_t, IndexSet> deletions;
    /// Indexes of sections which are newly inserted.
    IndexSet sections_to_insert;
    /// Indexes of sections which are deleted from the _old_ collection.
    IndexSet sections_to_delete;
};

/// For internal use only. Used to track the indices for a given section.
struct Section {
    size_t index;
    Mixed key;
    std::vector<size_t> indices;
};

} // namespace realm


#endif /* REALM_SECTIONED_RESULTS_HPP */
