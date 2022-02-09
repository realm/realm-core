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
class Results;
class SectionedResults;
struct SectionedResultsChangeSet;
class Mixed;

struct SectionRange {
    size_t index;
    size_t begin;
    size_t end;
};

class ResultsSection {
public:

    ResultsSection(SectionedResults* parent, size_t index):
    m_parent(parent),
    m_index(index) { };

    Mixed operator[](size_t idx) const;

    template <typename Context>
    auto get(Context&, size_t index);

    size_t size();

private:
    SectionedResults* m_parent;
    size_t m_index;
};


class SectionedResults {
public:

    SectionedResults(Results results,
                     util::UniqueFunction<bool(Mixed first, Mixed second)> comparison_func):
    m_results(results),
    m_callback(std::move(comparison_func)) {
        calculate_sections();
    };

    ResultsSection operator[](size_t idx);

    using CBFunc = util::UniqueFunction<void(SectionedResultsChangeSet, std::exception_ptr)>;
    NotificationToken add_notification_callback(CBFunc callback,
                                                KeyPathArray key_path_array = {}) &;

    SectionRange section_for_index(size_t index);

    size_t size();

private:

    void calculate_sections();
    friend class SectionedResultsNotificationHandler;
    void calculate_sections_if_required(Results::EvaluateMode mode = Results::EvaluateMode::Count);

    friend class realm::ResultsSection;
    Results m_results;
    std::vector<SectionRange> m_offset_ranges;
    util::UniqueFunction<bool(Mixed first, Mixed second)> m_callback;
};

struct SectionedResultsChangeSet {
    // Sections and indices which were removed from the _old_ collection
    std::map<size_t, std::set<size_t>> deletions;
    // Sections and indices in the _new_ collection which are new insertions
    std::map<size_t, std::set<size_t>> insertions;
    // Sections and indices of objects in the _old_ collection which were modified
    std::map<size_t, std::set<size_t>> modifications;
};


} // namespace realm


#endif /* SECTIONED_RESULTS_HPP */
