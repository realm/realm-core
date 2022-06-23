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
struct SectionedResultsChangeSet;

/// For internal use only. Used to track the indices for a given section.
struct Section {
    Section() = default;
    size_t index = 0;
    Mixed key;
    std::vector<size_t> indices;
};

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
    ResultsSection()
        : m_parent(nullptr)
        , m_key(Mixed())
    {
    }

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

    bool is_valid() const;

private:
    friend class SectionedResults;
    ResultsSection(SectionedResults* parent, Mixed key)
        : m_parent(parent)
        , m_key(key)
    {
    }

    SectionedResults* m_parent;
    Mixed m_key;
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
    NotificationToken add_notification_callback_for_section(Mixed section_key,
                                                            SectionedResultsNotificatonCallback callback,
                                                            KeyPathArray key_path_array = {});

    friend class realm::ResultsSection;
    Results m_results;
    SectionKeyFunc m_callback;
    std::unordered_map<Mixed, Section> m_sections;

    // Stores the Key, Section Index of the previous section
    // so we can efficiently calculate the collection change set.
    std::unordered_map<Mixed, size_t> m_previous_key_to_index_lookup;
    // Returns the key of the previous section from its index.
    std::unordered_map<size_t, Mixed> m_prev_section_index_to_key;
    // Returns the key of the current section from its index.
    std::vector<Mixed> m_ordered_section_keys;

    // By passing the index of the object from the underlying `Results`,
    // this will give a pair with the section index of the object, and the position of the object in that section.
    // This is used for parsing the indices in CollectionChangeSet to section indices.
    std::vector<std::pair<size_t, size_t>> m_row_to_index_path;
    // BinaryData & StringData types require a buffer to hold deep
    // copies of the key values for the lifetime of the sectioned results.
    // This is due to the fact that such values can reference the memory address of the value in the realm.
    // We can not rely on that because it would not produce stable keys.
    // So we perform a deep copy to produce stable key values that will not change if the realm is modified.
    // The buffer will purge keys that are no longer used in the case that the `calculate_sections` method runs.
    // We need to hold the string value in a shared_ptr so that if multiple notification handlers are attached
    // to this `SectionedResults` the copy of the key will not be discarded during `calculate_sections` as each
    // notification handler will hold a reference to the shared_ptr.
    std::unordered_map<Mixed, std::shared_ptr<std::string>> m_previous_str_buffers, m_current_str_buffers;
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

} // namespace realm


#endif /* REALM_SECTIONED_RESULTS_HPP */
