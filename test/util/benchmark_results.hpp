/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_TEST_UTIL_BENCHMARK_RESULTS_HPP
#define REALM_TEST_UTIL_BENCHMARK_RESULTS_HPP

#include <vector>
#include <map>
#include <string>

namespace realm {
namespace test_util {


class BenchmarkResults {
public:
    BenchmarkResults(int max_lead_text_width, const char* results_file_stem = "results");
    ~BenchmarkResults();

    enum ChangeType {
        change_Percent,
        change_DropFactor,
        change_RiseFactor,
    };

    /// Use submit_single() when you know there is only going to be a single datapoint.
    void submit_single(const char* ident, const char* lead_text, double seconds, ChangeType = change_Percent);

    /// Use submit() when there are multiple data points, and call finish() when you are done.
    void submit(const char* ident, double seconds);
    void finish(const std::string& ident, const std::string& lead_text, ChangeType = change_Percent);

private:
    int m_max_lead_text_width;
    std::string m_results_file_stem;

    struct Result {
        Result();
        double min;
        double max;
        double total;
        double stddev;
        double median;
        size_t rep;

        double avg() const;
    };

    struct Measurement {
        std::vector<double> samples;

        Result finish() const;
    };

    typedef std::map<std::string, Measurement> Measurements;
    Measurements m_measurements;
    typedef std::map<std::string, Result> BaselineResults;
    BaselineResults m_baseline_results;

    void try_load_baseline_results();
    void save_results();
};


// Implementation:

inline BenchmarkResults::BenchmarkResults(int max_lead_text_width, const char* results_file_stem)
    : m_max_lead_text_width(max_lead_text_width)
    , m_results_file_stem(results_file_stem)
{
    try_load_baseline_results();
}

inline BenchmarkResults::~BenchmarkResults()
{
    if (!m_measurements.empty())
        save_results();
}


} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_BENCHMARK_RESULTS_HPP
